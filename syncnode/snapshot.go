// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package syncnode

import (
	"context"
	"math"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/util/loadutil"
)

// failureRateScanTimeout caps the synchronous task-store scan that
// computeRecentFailureRate performs. The active task list is small
// (capped by MaxConcurrentTasks + MaxQueueSize) so 1s is generous; a
// slow store should degrade to "no recent data" rather than stall the
// refresh goroutine.
const failureRateScanTimeout = 1 * time.Second

// recentFailureWindow is the rolling window over which the load-score
// failure-rate input is computed (design.md §6.3.1). 5 minutes matches
// the dispatcher's tolerance for transient post-deploy failures while
// still surfacing a degraded node within roughly one heartbeat cycle.
const recentFailureWindow = 5 * time.Minute

// snapshotCacheRefresh is how often the background ticker re-scans the
// task + rule stores to refresh the cached heartbeat-input gauges. The
// heartbeat tick runs every 10s but two multi-millisecond BoltDB scans
// per tick can compound under load — 30s is a comfortable upper bound
// for failure-rate freshness (the master only uses it as an input to a
// soft load score) and divorces the I/O from the hot path.
const snapshotCacheRefresh = 30 * time.Second

// snapshotCache holds the expensive heartbeat-input gauges computed on
// a background ticker. Snapshot() reads atomic-protected fields with no
// locking on the hot path. updatedAt is exposed for diagnostics only.
type snapshotCache struct {
	failureRate atomic.Value // float64; nil-load before first refresh
	rules       atomic.Pointer[[]proto.SyncRuleAdvert]
	updatedAt   atomic.Int64 // unix nanos of last successful refresh

	// System gauges sampled by the refresh goroutine and read by Snapshot().
	// Float64 bits are stored via math.Float64bits / math.Float64frombits.
	cpuPercent    atomic.Uint64 // CPU utilisation [0,100]
	memPercent    atomic.Uint64 // memory used % [0,100]
	memTotalMB    atomic.Uint64 // total physical memory in MiB
	cpuCores      atomic.Int64  // logical CPU count (cgroup-aware via runtime.NumCPU)
	bandwidthMBps atomic.Uint64 // egress MB/s over the last refresh window

	// Delta state for bandwidth and CPU sampling. Accessed by the single
	// refresh goroutine only — no concurrent access, no atomics needed.
	prevBytesAt    int64   // unix nanos when prevBytesCount was sampled
	prevBytesCount int64   // registry TotalBytesObserved at prevBytesAt
	prevCPUAt      int64   // unix nanos of previous cgroup CPU usage reading
	prevCPUUsage   int64   // cgroup CPU usage in µs at prevCPUAt
	cgroupCPUCores float64 // container CPU quota in cores (0 = unlimited)
}

// Snapshot satisfies HeartbeatSnapshotProvider. The MasterClient calls it
// once per heartbeat tick — the implementation MUST be cheap (no I/O on
// the hot path). Status / Result / NodeID / Addr / NodeVersion are filled
// by the client; we only fill the dynamic gauges.
//
// Expensive inputs (LastTaskFailureRate, Rules) are served from
// snapshotCache, which is refreshed every snapshotCacheRefresh by a
// dedicated goroutine started in doStart. Pre-cache (e.g. unit tests
// that construct a bare *SyncNode) returns zero values for those gauges.
func (s *SyncNode) Snapshot() proto.SyncNodeHeartbeatResponse {
	resp := proto.SyncNodeHeartbeatResponse{
		UptimeSeconds:  int64(time.Since(startedAt).Seconds()),
		ReloadFailures: reloadFailuresTotal.Load(),
	}
	if s.executor != nil {
		resp.RunningTasks = int64(s.executor.RunningCount())
	}
	// FIX C: surface the Runner's queued-task count so master's
	// heartbeat-derived load + dashboards see a real number instead of
	// the fixed zero we shipped before the concurrency gate.
	if s.runner != nil {
		resp.QueuedTasks = int64(s.runner.QueueLen())
	}
	// P2-6: master is the cron authority; this node has no local
	// scheduler. ScheduledRules stays at its zero value.
	if s.boltDB != nil {
		resp.BoltDBHealthy = s.boltDB.Health() == nil
	}

	// Load-score inputs (§6.3.1). Read concurrency caps under cfgMu so
	// SIGHUP-driven cfg swap can't race with a heartbeat tick.
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	if cfg != nil {
		resp.MaxConcurrentTasks = cfg.Concurrency.MaxConcurrentTasks
		resp.BandwidthMBpsLimit = float64(cfg.Concurrency.BandwidthLimitMBps)
	}

	// Cached gauges. Nil cache (pre-doStart / bare-construction tests)
	// returns zero values — matches the prior synchronous-scan behaviour
	// before any data was written.
	if s.snapshotCache != nil {
		if v := s.snapshotCache.failureRate.Load(); v != nil {
			if fr, ok := v.(float64); ok {
				resp.LastTaskFailureRate = fr
			}
		}
		// FIX #4: advertise the per-rule AggregateBandwidthLimitMBps so
		// master's SyncQuotaCalculator can fan the cluster cap across
		// active nodes (§12.4.1 / P1-8). Empty / 0-cap rules are still
		// emitted with their ID so master can clear stale caps.
		if r := s.snapshotCache.rules.Load(); r != nil {
			resp.Rules = *r
		}
		resp.CPUPercent = math.Float64frombits(s.snapshotCache.cpuPercent.Load())
		resp.MemPercent = math.Float64frombits(s.snapshotCache.memPercent.Load())
		resp.MemTotalMB = s.snapshotCache.memTotalMB.Load()
		resp.CPUCores = int(s.snapshotCache.cpuCores.Load())
		resp.BandwidthMBps = math.Float64frombits(s.snapshotCache.bandwidthMBps.Load())
	}

	// Include per-task in-flight progress so master can update the ledger
	// each heartbeat without waiting for task terminal.
	if s.executor != nil {
		snaps := s.executor.RunningSnapshots()
		if len(snaps) > 0 {
			reports := make([]proto.SyncTaskProgressReport, 0, len(snaps))
			for id, p := range snaps {
				reports = append(reports, proto.SyncTaskProgressReport{
					TaskID: id,
					Progress: proto.TaskTerminalProgress{
						FilesTotal:           p.FilesTotal,
						FilesDone:            p.FilesDone,
						FilesSkipped:         p.FilesSkipped,
						FilesFailed:          p.FilesFailed,
						BytesTotal:           p.BytesTotal,
						BytesDone:            p.BytesDone,
						BytesSkipped:         p.BytesSkipped,
						ThroughputMBps:       p.ThroughputMBps,
						CurrentBandwidthMBps: p.CurrentBandwidthMBps,
					},
				})
			}
			resp.TaskReports = reports
		}
	}

	return resp
}

// startSnapshotCacheLoop fires every snapshotCacheRefresh seconds, scans
// the task + rule stores, and writes the new values into the cache. The
// first refresh happens immediately so Snapshot() doesn't return zero
// values during the brief startup window between doStart and the first
// tick. Started AFTER initStateStore + initExecutorAndRunner +
// bootstrapRulesFromConfig so the stores it reads are wired and
// populated; stopped by closing stopC.
func (s *SyncNode) startSnapshotCacheLoop() {
	if s.snapshotCache == nil {
		s.snapshotCache = &snapshotCache{}
	}
	// Immediate seed so the first heartbeat after doStart returns real
	// values instead of zeros.
	s.refreshSnapshotCache()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(snapshotCacheRefresh)
		defer t.Stop()
		for {
			select {
			case <-s.stopC:
				return
			case <-t.C:
				s.refreshSnapshotCache()
			}
		}
	}()
}

// refreshSnapshotCache performs the actual I/O — one task-store List for
// the failure-rate gauge and one rule-store List for the advert vector —
// and stores the results atomically. Errors during the scan degrade
// gracefully: prior cached values stay in place (we only Store on
// success-shaped output, but List returning err already maps to 0 /
// empty inside the compute helpers).
func (s *SyncNode) refreshSnapshotCache() {
	if s.snapshotCache == nil {
		return
	}
	fr := s.computeRecentFailureRate(recentFailureWindow)
	s.snapshotCache.failureRate.Store(fr)
	rs := s.advertiseRules()
	s.snapshotCache.rules.Store(&rs)

	// CPU utilisation. interval=0 returns differential since the last
	// gopsutil call (non-blocking); safe to call every 30s.
	//
	// Cgroup-aware path: sample GetContainerCPUUsageMicros() at two points
	// and compute (delta_µs / elapsed_µs) / containerCores * 100.
	// Falls back to gopsutil host-level when not in a container or when the
	// cgroup files are unavailable (macOS, bare-metal without cgroup limits).
	cgroupCores, cgroupCoresErr := loadutil.GetContainerCPUCores()
	if cgroupCoresErr == nil && cgroupCores > 0 {
		// Container has a CPU limit — use cgroup-based sampling.
		s.snapshotCache.cgroupCPUCores = cgroupCores
		s.snapshotCache.cpuCores.Store(int64(math.Round(cgroupCores)))
		if usage, err := loadutil.GetContainerCPUUsageMicros(); err == nil {
			now := time.Now().UnixNano()
			prev := s.snapshotCache.prevCPUUsage
			prevAt := s.snapshotCache.prevCPUAt
			if prevAt > 0 && now > prevAt && usage >= prev {
				elapsedMicros := float64(now-prevAt) / 1000.0
				cpuPct := (float64(usage-prev) / elapsedMicros) / cgroupCores * 100
				if cpuPct < 0 {
					cpuPct = 0
				} else if cpuPct > 100 {
					cpuPct = 100
				}
				s.snapshotCache.cpuPercent.Store(math.Float64bits(cpuPct))
			}
			s.snapshotCache.prevCPUUsage = usage
			s.snapshotCache.prevCPUAt = now
		}
	} else {
		// No container CPU limit or cgroup unavailable — fall back to host.
		s.snapshotCache.cgroupCPUCores = 0
		s.snapshotCache.cpuCores.Store(int64(runtime.NumCPU()))
		if cpu, err := loadutil.GetCpuUtilPercent(0); err == nil {
			s.snapshotCache.cpuPercent.Store(math.Float64bits(cpu))
		}
	}

	// Memory. Use cgroup container limit when available; fall back to host.
	limitBytes, limitErr := loadutil.GetContainerMemoryLimitBytes()
	if limitErr == nil && limitBytes > 0 {
		s.snapshotCache.memTotalMB.Store(limitBytes / (1024 * 1024))
		if usageBytes, err := loadutil.GetContainerMemoryUsageBytes(); err == nil {
			pct := float64(usageBytes) / float64(limitBytes) * 100
			if pct < 0 {
				pct = 0
			} else if pct > 100 {
				pct = 100
			}
			s.snapshotCache.memPercent.Store(math.Float64bits(pct))
		}
	} else {
		if mem, err := loadutil.GetMemoryUsedPercent(); err == nil {
			s.snapshotCache.memPercent.Store(math.Float64bits(mem))
		}
		if totalBytes, err := loadutil.GetTotalMemory(); err == nil {
			s.snapshotCache.memTotalMB.Store(totalBytes / (1024 * 1024))
		}
	}

	// Egress bandwidth: derive MB/s from two consecutive byte-count readings.
	// prevBytesAt / prevBytesCount are single-goroutine fields.
	if s.rateLimits != nil {
		now := time.Now().UnixNano()
		cur := s.rateLimits.TotalBytesObserved()
		if s.snapshotCache.prevBytesAt > 0 && now > s.snapshotCache.prevBytesAt {
			dt := float64(now-s.snapshotCache.prevBytesAt) / float64(time.Second)
			db := float64(cur - s.snapshotCache.prevBytesCount)
			mbps := (db / (1024 * 1024)) / dt
			if mbps < 0 {
				mbps = 0
			}
			s.snapshotCache.bandwidthMBps.Store(math.Float64bits(mbps))
		}
		s.snapshotCache.prevBytesCount = cur
		s.snapshotCache.prevBytesAt = now
	}

	s.snapshotCache.updatedAt.Store(time.Now().UnixNano())
}

// advertiseRules snapshots the (ID, AggregateBandwidthLimitMBps) pairs
// for every active rule in the local store. Cheap — the rule list is
// bounded and the store's List is in-memory or BoltDB-cached. Errors fall
// back to an empty slice so the heartbeat still flies. Called only from
// the snapshot cache refresh loop post-fix (NOT from Snapshot directly).
func (s *SyncNode) advertiseRules() []proto.SyncRuleAdvert {
	if s.ruleStore == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), failureRateScanTimeout)
	defer cancel()
	all, err := s.ruleStore.List(ctx)
	if err != nil {
		return nil
	}
	out := make([]proto.SyncRuleAdvert, 0, len(all))
	for _, r := range all {
		// Only advertise active rules — paused / degraded shouldn't
		// consume a share of the cluster quota.
		if r.State != rules.StateActive {
			continue
		}
		out = append(out, proto.SyncRuleAdvert{
			ID:                          r.ID(),
			AggregateBandwidthLimitMBps: r.Config.AggregateBandwidthLimitMBps,
		})
	}
	return out
}

// computeRecentFailureRate returns failed/total over the last `window` of
// terminal task records, clamped to [0, 1]. Returns 0 when:
//   - the task store is unset (early in startup);
//   - the store errors (treated as "no signal");
//   - no terminal records fall inside the window.
//
// Called only from the snapshot cache refresh loop post-fix (NOT from
// Snapshot directly).
func (s *SyncNode) computeRecentFailureRate(window time.Duration) float64 {
	if s.taskStore == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), failureRateScanTimeout)
	defer cancel()
	recs, err := s.taskStore.List(ctx, "")
	if err != nil {
		return 0
	}
	cutoff := time.Now().Add(-window)
	var total, failed int
	for _, r := range recs {
		// Terminal records only — exclude in-flight runs (DoneAt zero).
		if r.DoneAt.IsZero() {
			continue
		}
		if r.DoneAt.Before(cutoff) {
			continue
		}
		total++
		if r.Status == executor.StatusFailed {
			failed++
		}
	}
	if total == 0 {
		return 0
	}
	rate := float64(failed) / float64(total)
	if rate < 0 {
		return 0
	}
	if rate > 1 {
		return 1
	}
	return rate
}
