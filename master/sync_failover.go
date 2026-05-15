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
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// -----------------------------------------------------------------------
// SyncFailover (Phase P1-4)
//
// When checkSyncNodeHeartbeat declares a syncnode dead, the dispatcher
// calls handleNodeDeath(addr). The dispatcher itself only knows ownership
// (taskID → addr); it does NOT remember the original RunTask payload.
//
// SyncFailover is the orchestrator that fills that gap:
//
//   1. After every successful Dispatch the caller invokes Remember(taskID,
//      payload) to stash the original *proto.AdminTask carrying the
//      RunTaskRequest.
//   2. The dispatcher's failoverHook (wired by NewSyncFailover) calls
//      redispatch(taskID) once per orphaned task; redispatch re-Dispatches
//      the saved payload, choosing a fresh candidate.
//   3. Forget(taskID) drops the payload once the task reaches a terminal
//      state on its owning node.
//
// A bounded ring of FailoverRecord (last 100 events) backs Recent() so
// the ops dashboard / integration tests can observe failover activity
// without scraping logs.
//
// Tasks that exhaust their re-dispatch retries (no candidates, or every
// candidate nack'd) land in deadLetter for manual operator intervention.
// -----------------------------------------------------------------------

// failoverHistoryCap bounds how many FailoverRecord entries Recent()
// preserves. 100 is chosen to cover roughly one wave of node turnover in
// a 50-node fleet without unbounded growth.
const failoverHistoryCap = 100

// failoverDispatchRetries is how many alternate candidates Dispatch will
// try per orphaned task before giving up and dead-lettering.
const failoverDispatchRetries = 3

// FailoverRecord captures one redispatch event. The ring is bounded by
// failoverHistoryCap; older entries roll off.
type FailoverRecord struct {
	TaskID    string    `json:"taskId"`
	FromAddr  string    `json:"fromAddr"`         // empty when we don't track the dying addr
	ToAddr    string    `json:"toAddr,omitempty"` // empty when redispatch failed
	Err       string    `json:"err,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// syncFailoverCluster is the narrow subset of *Cluster SyncFailover
// touches. Defined locally so unit tests can inject a stub without
// dragging in real cluster scaffolding.
//
// sendRunTask delivers a freshly-built OpSyncNodeRunTask to the named
// syncnode. In production it routes through SyncNode.TaskManager.AddTask;
// tests pass a recording stub. Returns an error if the addr is unknown
// or the task manager rejects the task.
type syncFailoverCluster interface {
	sendRunTask(addr string, task *proto.AdminTask) error
}

// clusterFailoverSource adapts *Cluster to syncFailoverCluster.
type clusterFailoverSource struct {
	c *Cluster
}

func (s *clusterFailoverSource) sendRunTask(addr string, task *proto.AdminTask) error {
	v, ok := s.c.syncNodes.Load(addr)
	if !ok {
		return fmt.Errorf("syncnode %s not registered", addr)
	}
	sn, _ := v.(*SyncNode)
	if sn == nil || sn.TaskManager == nil {
		return fmt.Errorf("syncnode %s has no task manager", addr)
	}
	sn.TaskManager.AddTask(task)
	return nil
}

// SyncFailover orchestrates task redispatch when a syncnode dies. It
// owns:
//
//   - payloads: the saved RunTask AdminTask per active task, used to
//     rebuild the wire packet for the new owner.
//   - history:  bounded ring of recent failover events.
//   - deadLetter: tasks the orchestrator couldn't re-home after retries.
type SyncFailover struct {
	cluster syncFailoverCluster
	disp    *SyncDispatcher

	mu         sync.Mutex
	payloads   map[string]*proto.AdminTask
	history    []FailoverRecord
	deadLetter map[string]error

	// now is the wallclock function; tests override it.
	now func() time.Time
}

// NewSyncFailover wires the orchestrator into the supplied dispatcher.
// The dispatcher's failoverHook is replaced with f.redispatch.
func NewSyncFailover(c *Cluster, disp *SyncDispatcher) *SyncFailover {
	return newSyncFailoverFromSource(&clusterFailoverSource{c: c}, disp)
}

// newSyncFailoverFromSource is the test seam: callers pass a stub
// cluster source instead of a real *Cluster. The hook is installed on
// the dispatcher so behavior matches production.
func newSyncFailoverFromSource(src syncFailoverCluster, disp *SyncDispatcher) *SyncFailover {
	f := &SyncFailover{
		cluster:    src,
		disp:       disp,
		payloads:   make(map[string]*proto.AdminTask),
		deadLetter: make(map[string]error),
		now:        time.Now,
	}
	disp.WithFailoverHook(f.redispatch)
	return f
}

// Remember saves the original RunTask payload for a task. Production
// calls this immediately after a successful Dispatch from the admin
// API handler. Subsequent Remember calls for the same taskID overwrite
// the previous payload (intentional: callers may re-issue with refined
// arguments).
func (f *SyncFailover) Remember(taskID string, payload *proto.AdminTask) {
	if taskID == "" || payload == nil {
		return
	}
	f.mu.Lock()
	f.payloads[taskID] = payload
	// A retry of a previously-dead task should clear its dead-letter
	// flag so operators see the recovery in DeadLetter().
	delete(f.deadLetter, taskID)
	f.mu.Unlock()
}

// Forget releases the saved payload for a task and clears any dead-
// letter entry. Call on terminal (success / failure / cancel).
func (f *SyncFailover) Forget(taskID string) {
	f.mu.Lock()
	delete(f.payloads, taskID)
	delete(f.deadLetter, taskID)
	f.mu.Unlock()
}

// Recent returns the most recent n FailoverRecord entries in
// chronological order (oldest first). n<=0 returns all retained records.
func (f *SyncFailover) Recent(n int) []FailoverRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := len(f.history)
	if n <= 0 || n >= total {
		out := make([]FailoverRecord, total)
		copy(out, f.history)
		return out
	}
	out := make([]FailoverRecord, n)
	copy(out, f.history[total-n:])
	return out
}

// DeadLetter returns a snapshot of taskIDs that couldn't be redispatched
// (no candidates, or all retries exhausted). Operators handle these
// manually. The returned map is a copy — safe to iterate without holding
// the orchestrator lock.
func (f *SyncFailover) DeadLetter() map[string]error {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]error, len(f.deadLetter))
	for k, v := range f.deadLetter {
		out[k] = v
	}
	return out
}

// redispatch is the dispatcher's failoverHook. It runs once per orphaned
// task on owner death.
//
// Behavior:
//   - Unknown taskID (no saved payload) → record + return nil. Most
//     likely the task already finished and Forget cleared it; treat as
//     benign.
//   - Dispatch succeeds → record the (taskID → new addr) swap.
//   - Dispatch fails    → record the error and dead-letter the task.
//
// Returning an error is informational only; the dispatcher logs it and
// continues iterating the remaining orphans.
func (f *SyncFailover) redispatch(taskID string) error {
	f.mu.Lock()
	payload, ok := f.payloads[taskID]
	f.mu.Unlock()

	rec := FailoverRecord{
		TaskID:    taskID,
		Timestamp: f.now(),
	}

	if !ok {
		rec.Err = "no saved payload — task may have completed already"
		f.appendHistory(rec)
		log.LogInfof("failover: task %q has no saved payload, skipping redispatch", taskID)
		return nil
	}

	sendFn := func(addr string) error {
		// Rebuild the AdminTask so the new owner's TaskManager treats it
		// as fresh (CreateTime / SendCount reset, new RequestID). The
		// RunTaskRequest payload is reused verbatim — re-running the
		// rule from the top is exactly the failover contract.
		runTask := proto.NewAdminTaskEx(proto.OpSyncNodeRunTask, addr, payload.Request, taskID)
		return f.cluster.sendRunTask(addr, runTask)
	}

	addr, err := f.disp.Dispatch(taskID, sendFn, failoverDispatchRetries)
	if err != nil {
		rec.Err = err.Error()
		f.markDead(taskID, err)
		f.appendHistory(rec)
		log.LogWarnf("failover: task %q could not be redispatched: %v", taskID, err)
		return err
	}
	rec.ToAddr = addr
	f.appendHistory(rec)
	log.LogInfof("failover: task %q redispatched to %s", taskID, addr)
	return nil
}

// appendHistory keeps the ring bounded at failoverHistoryCap entries.
func (f *SyncFailover) appendHistory(rec FailoverRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.history = append(f.history, rec)
	if len(f.history) > failoverHistoryCap {
		f.history = f.history[len(f.history)-failoverHistoryCap:]
	}
}

// markDead flags a task as un-redispatchable; surfaced via DeadLetter().
func (f *SyncFailover) markDead(taskID string, err error) {
	f.mu.Lock()
	f.deadLetter[taskID] = err
	f.mu.Unlock()
}
