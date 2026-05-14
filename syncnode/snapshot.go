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
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
)

// failureRateScanTimeout caps the synchronous task-store scan that
// computeRecentFailureRate performs on every heartbeat. The active task
// list is small (capped by MaxConcurrentTasks + MaxQueueSize) so 1s is
// generous; a slow store should degrade to "no recent data" rather than
// stall the heartbeat goroutine.
const failureRateScanTimeout = 1 * time.Second

// recentFailureWindow is the rolling window over which the load-score
// failure-rate input is computed (design.md §6.3.1). 5 minutes matches
// the dispatcher's tolerance for transient post-deploy failures while
// still surfacing a degraded node within roughly one heartbeat cycle.
const recentFailureWindow = 5 * time.Minute

// Snapshot satisfies HeartbeatSnapshotProvider. The MasterClient calls it
// once per heartbeat tick — the implementation MUST be cheap (no I/O on
// the hot path). Status / Result / NodeID / Addr / NodeVersion are filled
// by the client; we only fill the dynamic gauges.
func (s *SyncNode) Snapshot() proto.SyncNodeHeartbeatResponse {
	resp := proto.SyncNodeHeartbeatResponse{
		UptimeSeconds:  int64(time.Since(startedAt).Seconds()),
		ReloadFailures: reloadFailuresTotal.Load(),
	}
	if s.executor != nil {
		resp.RunningTasks = int64(s.executor.RunningCount())
	}
	if s.scheduler != nil {
		resp.ScheduledRules = s.scheduler.RegisteredCount()
	}
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
	resp.LastTaskFailureRate = s.computeRecentFailureRate(recentFailureWindow)

	// FIX #4: advertise the per-rule AggregateBandwidthLimitMBps so
	// master's SyncQuotaCalculator can fan the cluster cap across active
	// nodes (§12.4.1 / P1-8). Empty / 0-cap rules are still emitted with
	// their ID so master can clear stale caps; the master side treats 0
	// as "no cluster ceiling".
	resp.Rules = s.advertiseRules()

	return resp
}

// advertiseRules snapshots the (ID, AggregateBandwidthLimitMBps) pairs
// for every active rule in the local store. Cheap — the rule list is
// bounded and the store's List is in-memory or BoltDB-cached. Errors fall
// back to an empty slice so the heartbeat still flies.
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
// Implementation is synchronous on the heartbeat hot path. The active
// task list is bounded by ConcurrencyConfig + queue, so a List scan is
// cheap; if profiling shows otherwise, this can move to a ticker-cached
// gauge without changing the wire shape.
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
