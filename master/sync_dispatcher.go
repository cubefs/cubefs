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

package master

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

// -----------------------------------------------------------------------
// SyncDispatcher (Phase P1-1 / P1-2 / P1-5)
//
// Architecture: PASSIVE OBSERVER of Cluster.syncNodes.
//
// The dispatcher does NOT keep its own copy of node runtime state. On
// every read (LoadScore / Candidates) it pulls the fresh snapshot off
// the *SyncNode entries that sync_node_task.go (B-2) maintains via the
// heartbeat-response handler. This avoids a second source of truth and
// keeps sync_node.go / sync_node_task.go untouched.
//
// What the dispatcher OWNS:
//   - taskOwner   : taskID → owner addr (ownership ledger; P1-4 failover
//                   reads/mutates this on node death).
//   - lastDispatchAt : per-node round-robin tiebreaker timestamp.
// -----------------------------------------------------------------------

// dispatcherStaleness is how long a heartbeat can be silent before the
// node is considered ineligible for new dispatch. 30s matches the P1-1
// acceptance window ("kill a syncnode → its entry disappears within
// 30s").
const dispatcherStaleness = 30 * time.Second

// defaultMaxConcurrentTasks is the divisor used when the heartbeat
// snapshot doesn't carry an explicit MaxConcurrentTasks (which it
// currently doesn't — design.md §4.1 defaults a syncnode to 8 parallel
// tasks). When the upstream proto adds the field, swap this for the
// per-node value.
const defaultMaxConcurrentTasks = 8

// ErrNoCandidates is returned by Dispatch when no live syncnode is
// eligible (empty fleet, all stale, all bolt-unhealthy, or all
// saturated).
var ErrNoCandidates = errors.New("syncdispatcher: no eligible syncnode candidate")

// syncDispatcherSource is the minimum interface the dispatcher needs
// against the cluster. Defined locally so unit tests can supply a stub
// without dragging in the full *Cluster scaffolding.
type syncDispatcherSource interface {
	rangeSyncNodes(func(addr string, sn *SyncNode) bool)
}

// clusterSyncNodeSource adapts *Cluster to syncDispatcherSource.
type clusterSyncNodeSource struct {
	c *Cluster
}

func (s *clusterSyncNodeSource) rangeSyncNodes(visit func(addr string, sn *SyncNode) bool) {
	s.c.syncNodes.Range(func(k, v interface{}) bool {
		addr, _ := k.(string)
		sn, _ := v.(*SyncNode)
		if sn == nil {
			return true
		}
		return visit(addr, sn)
	})
}

// SyncDispatcher schedules syncnode tasks across the registered fleet
// using the load score from design.md §6.3.1.
type SyncDispatcher struct {
	mu             sync.RWMutex
	source         syncDispatcherSource
	taskOwner      map[string]string              // taskID → owner addr
	ownedByAddr    map[string]map[string]struct{} // addr → set of taskIDs
	lastDispatchAt map[string]time.Time           // addr → last dispatch wallclock

	// now is the wallclock function; tests override it.
	now func() time.Time
}

// NewSyncDispatcher constructs a dispatcher bound to the supplied
// Cluster. The dispatcher reads SyncNode runtime fields directly from
// c.syncNodes on every call.
func NewSyncDispatcher(c *Cluster) *SyncDispatcher {
	return newSyncDispatcherFromSource(&clusterSyncNodeSource{c: c})
}

// newSyncDispatcherFromSource is the test seam: tests inject a stub
// source instead of a real *Cluster.
func newSyncDispatcherFromSource(source syncDispatcherSource) *SyncDispatcher {
	return &SyncDispatcher{
		source:         source,
		taskOwner:      make(map[string]string),
		ownedByAddr:    make(map[string]map[string]struct{}),
		lastDispatchAt: make(map[string]time.Time),
		now:            time.Now,
	}
}

// computeLoadScore is the pure load-score function from §6.3.1.
//
//	load = 0.4*(RunningTasks / MaxConcurrentTasks)
//	     + 0.3*(BandwidthMBpsUsed / BandwidthMBpsLimit)
//	     + 0.2*(CPUPercent / 100)
//	     + 0.1*(LastTaskFailureRate)
//
// We only have RunningTasks, BandwidthMBps, CPUPercent, BoltDBHealthy
// in the heartbeat snapshot. Where a divisor is unavailable we treat
// utilization as 0 (the node is assumed under capacity); per
// design.md §4.1 default we use 8 as the MaxConcurrentTasks divisor.
// LastTaskFailureRate isn't exported on SyncNode yet — counted as 0
// for now (TODO P1-4: thread failure-rate signal from
// handleSyncNodeTaskResponse).
//
// Returns +Inf when:
//   - sn is nil
//   - heartbeat is older than dispatcherStaleness
//   - BoltDBHealthy=false (node can't durably claim work)
func computeLoadScore(sn *SyncNode, now time.Time) float64 {
	if sn == nil {
		return math.Inf(1)
	}
	sn.RLock()
	defer sn.RUnlock()

	if !sn.IsActive {
		return math.Inf(1)
	}
	if now.Sub(sn.ReportTime) > dispatcherStaleness {
		return math.Inf(1)
	}
	if !sn.BoltDBHealthy {
		return math.Inf(1)
	}

	capacity := 0.0
	if defaultMaxConcurrentTasks > 0 {
		capacity = float64(sn.RunningTasks) / float64(defaultMaxConcurrentTasks)
	}
	if capacity > 1 {
		capacity = 1
	}

	// Bandwidth: until the heartbeat carries a bandwidth limit, the
	// snapshot field BandwidthMBps reports the *last-1m usage* in MBps.
	// We can't form a ratio without a limit, so this term stays 0 until
	// the upstream proto adds BandwidthMBpsLimit. TODO(P1-4).
	bandwidth := 0.0

	cpu := sn.CPUPercent / 100.0
	if cpu < 0 {
		cpu = 0
	}
	if cpu > 1 {
		cpu = 1
	}

	// LastTaskFailureRate not yet plumbed — treat as 0. TODO(P1-4).
	failureRate := 0.0

	return 0.4*capacity + 0.3*bandwidth + 0.2*cpu + 0.1*failureRate
}

// LoadScore returns the load score for the syncnode at addr, or +Inf
// if the node is unknown / stale / unhealthy.
func (d *SyncDispatcher) LoadScore(addr string) float64 {
	var found *SyncNode
	d.source.rangeSyncNodes(func(a string, sn *SyncNode) bool {
		if a == addr {
			found = sn
			return false
		}
		return true
	})
	if found == nil {
		return math.Inf(1)
	}
	return computeLoadScore(found, d.now())
}

// Candidates returns the addrs of nodes eligible to receive a new task,
// sorted ascending by load score. Ties within ±0.05 are broken by the
// node's lastDispatchAt (older = better — produces round-robin behavior
// across equal-load nodes).
//
// staleThreshold is the heartbeat-age cutoff; pass dispatcherStaleness
// in production.
func (d *SyncDispatcher) Candidates(staleThreshold time.Duration) []string {
	now := d.now()
	type scored struct {
		addr         string
		score        float64
		lastDispatch time.Time
	}
	cands := make([]scored, 0)

	d.mu.RLock()
	lastMap := make(map[string]time.Time, len(d.lastDispatchAt))
	for k, v := range d.lastDispatchAt {
		lastMap[k] = v
	}
	d.mu.RUnlock()

	d.source.rangeSyncNodes(func(addr string, sn *SyncNode) bool {
		sn.RLock()
		active := sn.IsActive
		rt := sn.ReportTime
		bolt := sn.BoltDBHealthy
		running := sn.RunningTasks
		sn.RUnlock()

		if !active {
			return true
		}
		if now.Sub(rt) > staleThreshold {
			return true
		}
		if !bolt {
			return true
		}
		if defaultMaxConcurrentTasks > 0 && running >= int64(defaultMaxConcurrentTasks) {
			return true
		}
		cands = append(cands, scored{
			addr:         addr,
			score:        computeLoadScore(sn, now),
			lastDispatch: lastMap[addr],
		})
		return true
	})

	sort.SliceStable(cands, func(i, j int) bool {
		if math.Abs(cands[i].score-cands[j].score) < 0.05 {
			return cands[i].lastDispatch.Before(cands[j].lastDispatch)
		}
		return cands[i].score < cands[j].score
	})

	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.addr
	}
	return out
}

// Dispatch picks the best candidate per Candidates(), invokes sendFn to
// hand the task to that node, and records ownership on success. If
// sendFn fails it falls back to the next-best candidate; gives up after
// maxRetries+1 total attempts (so maxRetries=3 → up to 4 candidates
// tried).
//
// Returns the chosen addr on success, or an error if every candidate
// rejected the task. Returns ErrNoCandidates if the fleet is empty.
func (d *SyncDispatcher) Dispatch(taskID string, sendFn func(addr string) error, maxRetries int) (string, error) {
	if taskID == "" {
		return "", fmt.Errorf("syncdispatcher: empty taskID")
	}
	if sendFn == nil {
		return "", fmt.Errorf("syncdispatcher: nil sendFn")
	}
	cands := d.Candidates(dispatcherStaleness)
	if len(cands) == 0 {
		return "", ErrNoCandidates
	}
	limit := maxRetries + 1
	if limit < 1 {
		limit = 1
	}
	var lastErr error
	tried := 0
	for _, addr := range cands {
		if tried >= limit {
			break
		}
		tried++
		if err := sendFn(addr); err != nil {
			lastErr = err
			log.LogWarnf("syncdispatcher: send task %q to %s failed (attempt %d/%d): %v",
				taskID, addr, tried, limit, err)
			continue
		}
		d.recordOwnership(taskID, addr)
		log.LogInfof("syncdispatcher: dispatched task %q to %s (attempt %d)", taskID, addr, tried)
		return addr, nil
	}
	if lastErr != nil {
		return "", fmt.Errorf("syncdispatcher: all %d candidates rejected task %q: %w", tried, taskID, lastErr)
	}
	return "", fmt.Errorf("syncdispatcher: all %d candidates rejected task %q", tried, taskID)
}

// Release removes ownership for taskID. Idempotent — unknown taskID is
// a no-op. Called when a task finishes (success / failure / cancel) so
// the slot frees up.
func (d *SyncDispatcher) Release(taskID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	addr, ok := d.taskOwner[taskID]
	if !ok {
		return
	}
	delete(d.taskOwner, taskID)
	if owned, ok2 := d.ownedByAddr[addr]; ok2 {
		delete(owned, taskID)
		if len(owned) == 0 {
			delete(d.ownedByAddr, addr)
		}
	}
}

// OwnerOf returns the addr currently owning taskID, or "" if not
// dispatched.
func (d *SyncDispatcher) OwnerOf(taskID string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.taskOwner[taskID]
}

// TasksOwnedBy returns a snapshot slice of taskIDs the dispatcher has
// assigned to addr. Safe to iterate without holding the dispatcher
// lock. Used by P1-4 failover to know what to reassign when a node
// dies.
func (d *SyncDispatcher) TasksOwnedBy(addr string) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	owned, ok := d.ownedByAddr[addr]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(owned))
	for tid := range owned {
		out = append(out, tid)
	}
	return out
}

// handleNodeDeath releases every task previously assigned to addr.
//
// P1-1/P1-2 only requires that the ownership map drops the dead node
// so dispatch math stays consistent. P1-4 will replace this with an
// interrupt-and-redispatch flow; we keep the hook in place so the
// failover agent can swap the body without touching call sites.
//
// TODO(P1-4): instead of releasing, redispatch each released task to
// the next-best candidate (must coordinate with the syncnode-side TTL
// cleanup so the same logical task doesn't run on two nodes).
func (d *SyncDispatcher) handleNodeDeath(addr string) {
	d.mu.Lock()
	owned := d.ownedByAddr[addr]
	delete(d.ownedByAddr, addr)
	delete(d.lastDispatchAt, addr)
	released := make([]string, 0, len(owned))
	for tid := range owned {
		delete(d.taskOwner, tid)
		released = append(released, tid)
	}
	d.mu.Unlock()
	if len(released) > 0 {
		log.LogWarnf("syncdispatcher: handleNodeDeath(%s) released %d in-flight tasks: %v",
			addr, len(released), released)
	}
}

// recordOwnership stores the (taskID, addr) ownership tuple and
// refreshes the addr's last-dispatch timestamp for round-robin
// tiebreak.
func (d *SyncDispatcher) recordOwnership(taskID, addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.taskOwner[taskID] = addr
	owned, ok := d.ownedByAddr[addr]
	if !ok {
		owned = make(map[string]struct{})
		d.ownedByAddr[addr] = owned
	}
	owned[taskID] = struct{}{}
	d.lastDispatchAt[addr] = d.now()
}
