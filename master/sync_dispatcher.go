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
// snapshot doesn't carry an explicit MaxConcurrentTasks (legacy syncnode
// builds < this commit, or a syncnode whose config omits the field).
// design.md §4.1 defaults a syncnode to 8 parallel tasks.
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

	// failoverHook is invoked once per owned task when the owner dies
	// (see handleNodeDeath). Production injects a closure that
	// re-dispatches via Dispatch(); tests inject a counter / error-
	// injecting stub. nil → no-op (a released task stays released).
	// Guarded by mu.
	failoverHook func(taskID string) error
}

// WithFailoverHook installs the failover orchestrator. Safe to call at
// any time; invocation is serialized via the dispatcher's lock.
func (d *SyncDispatcher) WithFailoverHook(h func(taskID string) error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failoverHook = h
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
// All four inputs come from the heartbeat snapshot now (P1 alignment).
// Defensive zero-divisor handling: if MaxConcurrentTasks is missing the
// node is treated as having unlimited capacity → capacity term contributes
// 0; if BandwidthMBpsLimit is 0 the bandwidth term contributes 0.
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

	// Capacity term: prefer the node-reported MaxConcurrentTasks; if
	// missing (zero) treat as unlimited → 0 capacity utilization. The
	// const default is reserved for the saturation gate in Candidates().
	capacity := 0.0
	if sn.MaxConcurrentTasks > 0 {
		capacity = float64(sn.RunningTasks) / float64(sn.MaxConcurrentTasks)
	}
	if capacity > 1 {
		capacity = 1
	}

	// Bandwidth term: BandwidthMBps is last-1m egress, BandwidthMBpsLimit
	// is the configured ceiling. Zero limit ⇒ no local cap ⇒ contribute 0
	// (avoids divide-by-zero).
	bandwidth := 0.0
	if sn.BandwidthMBpsLimit > 0 {
		bandwidth = sn.BandwidthMBps / sn.BandwidthMBpsLimit
	}
	if bandwidth < 0 {
		bandwidth = 0
	}
	if bandwidth > 1 {
		bandwidth = 1
	}

	cpu := sn.CPUPercent / 100.0
	if cpu < 0 {
		cpu = 0
	}
	if cpu > 1 {
		cpu = 1
	}

	failureRate := sn.LastTaskFailureRate
	if failureRate < 0 {
		failureRate = 0
	}
	if failureRate > 1 {
		failureRate = 1
	}

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

// LoadScoreAll returns the current load score per registered syncnode in
// a single pass over c.syncNodes. Used by /syncNode/list to avoid the
// O(N²) trap of calling LoadScore once per node — at 1000 nodes that
// was ~1M map walks per request. The returned map is freshly allocated
// (one entry per registered node); callers may mutate it freely.
//
// Returned values include +Inf for stale / inactive / bolt-unhealthy
// nodes (same contract as single-node LoadScore). A single now()
// snapshot is taken at the top so every score uses one wallclock
// reference — small consistency win when the fleet straddles the
// staleness boundary.
func (d *SyncDispatcher) LoadScoreAll() map[string]float64 {
	now := d.now()
	out := make(map[string]float64)
	d.source.rangeSyncNodes(func(addr string, sn *SyncNode) bool {
		out[addr] = computeLoadScore(sn, now)
		return true
	})
	return out
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
		maxConc := sn.MaxConcurrentTasks
		state := sn.State
		sn.RUnlock()

		if !active {
			return true
		}
		// P2: operator-controlled drain. Draining nodes stop receiving
		// new task dispatch but keep finishing what they have.
		if state == SyncNodeStateDraining {
			return true
		}
		if now.Sub(rt) > staleThreshold {
			return true
		}
		if !bolt {
			return true
		}
		// Saturation gate: prefer the per-node ceiling when the heartbeat
		// snapshot carries one; otherwise fall back to the cluster-wide
		// default (legacy syncnode builds).
		cap := maxConc
		if cap <= 0 {
			cap = defaultMaxConcurrentTasks
		}
		if cap > 0 && running >= int64(cap) {
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

// AllOwnerships returns a snapshot of the entire ownership ledger
// (taskID → owner addr). Used by SyncFanout.Recover to rebuild
// in-memory parent state after a master leader transition (FIX #7).
// The returned map is a copy; callers may mutate it without affecting
// the dispatcher.
func (d *SyncDispatcher) AllOwnerships() map[string]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]string, len(d.taskOwner))
	for k, v := range d.taskOwner {
		out[k] = v
	}
	return out
}

// handleNodeDeath releases every task previously assigned to addr and,
// when a failover hook is installed, invokes it once per released task
// to trigger re-dispatch to a fresh candidate.
//
// The hook signature is intentionally narrow (just the taskID): the
// orchestrator (sync_failover.go) is responsible for remembering the
// original task payload and picking a new owner via Dispatch().
//
// Hook errors are logged but never abort the loop — a single failed
// re-dispatch must not block the rest of the dying node's tasks from
// being re-homed.
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
	hook := d.failoverHook
	d.mu.Unlock()

	if len(released) == 0 {
		return
	}
	log.LogWarnf("syncdispatcher: handleNodeDeath(%s) released %d in-flight tasks: %v",
		addr, len(released), released)

	if hook == nil {
		return
	}
	for _, taskID := range released {
		if err := hook(taskID); err != nil {
			log.LogWarnf("syncdispatcher: failover hook for task %q returned error: %v", taskID, err)
		}
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
