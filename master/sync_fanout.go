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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// -----------------------------------------------------------------------
// SyncFanout (Phase P1-7)
//
// Splits one parent sync task into N sub-tasks, each owned by a separate
// syncnode. Owns the cross-node coordination state; the actual node
// selection delegates to *SyncDispatcher so load-score + ownership ledger
// + failover hook keep working for every shard.
//
// Sub-task identity convention (mirrors syncnode/tasks.TriggerSubTask):
//   sub_task_id := "<parent_task_id>/<shard_index>"
//
// Progress aggregation is per-parent: each owner pushes its leaf
// progress via RecordProgress; the parent's view is the element-wise sum
// of the latest snapshot per shard. Failover (P1-4) doesn't reset shard
// progress — a re-dispatched leaf re-runs from scratch, and its new
// snapshot overwrites the old one in the parents map.
// -----------------------------------------------------------------------

// ErrInsufficientCandidates is returned by DispatchN when fewer live
// syncnodes are available than shardTotal. The error carries the partial
// result so the caller can decide whether to retry or surface it.
var ErrInsufficientCandidates = errors.New("syncfanout: insufficient candidates for shard count")

// SyncFanout coordinates N-way dispatch + progress aggregation. Owns
// nothing by itself — it operates on the existing dispatcher plus the
// cluster's syncNodes registry.
type SyncFanout struct {
	disp *SyncDispatcher

	mu      sync.Mutex
	parents map[string]*parentTask // parentTaskID → tracking state
}

// parentTask tracks the per-parent fan-out: which shards exist, which
// owner runs each, and the latest progress snapshot per shard.
type parentTask struct {
	parentTaskID string
	ruleID       string
	shardTotal   int
	subTasks     map[int]string       // shardIndex → owner addr
	progress     map[int]TaskProgress // shardIndex → latest snapshot
	terminal     map[int]bool         // shardIndex → reported terminal (Bug S2)
	startedAt    time.Time
	doneAt       time.Time
	status       string // "running" / "done" / "failed"
}

// TaskProgress is the compact wire shape of executor.Progress used by
// the fan-out aggregator. We only track totals; the throughput term is
// derived per parent at read time (BytesDone / elapsed).
type TaskProgress struct {
	FilesDone  int64
	FilesTotal int64
	BytesDone  int64
	BytesTotal int64
}

// SubTaskInfo is the wire shape mirrored from
// syncnode/task_handler.go's RunSubTaskInfo. Master injects this into
// each shard's RunTaskRequest payload via DispatchN's payloadTemplate.
//
// Defined here (not imported from syncnode) so master doesn't pull the
// syncnode binary into its own build graph. The two struct shapes must
// stay JSON-compatible.
//
// Prefixes (P2-5) carries the literal prefix list when the rule uses
// prefix-mode sharding (operator-declared or auto-probed). It is
// omitempty so hash-mode dispatches don't emit the field. Readers
// (executor.ShouldKeep) treat a non-empty Prefixes as a signal to skip
// hash math entirely.
type SubTaskInfo struct {
	ParentTaskID string   `json:"parentTaskId"`
	ShardIndex   int      `json:"shardIndex"`
	ShardTotal   int      `json:"shardTotal"`
	Prefixes     []string `json:"prefixes,omitempty"`
}

// PayloadCloner is the contract DispatchN uses to clone the parent
// payload and inject a SubTaskInfo per shard. Production callers (the
// HTTP handler) implement it inline with a JSON round-trip; tests can
// supply a stub that captures cloned payloads for assertion.
type PayloadCloner interface {
	CloneWithSubTask(parent interface{}, info SubTaskInfo) (interface{}, error)
}

// PayloadClonerFunc adapts a free function to PayloadCloner.
type PayloadClonerFunc func(parent interface{}, info SubTaskInfo) (interface{}, error)

// CloneWithSubTask satisfies PayloadCloner.
func (f PayloadClonerFunc) CloneWithSubTask(parent interface{}, info SubTaskInfo) (interface{}, error) {
	return f(parent, info)
}

// SendFunc is the per-shard send hook. Given a target addr and the
// cloned shard payload it must wrap the payload in an OpSyncNodeRunTask
// envelope and push it to the node. Returns nil on accept, non-nil to
// trigger dispatcher fallback to the next candidate for THIS shard
// only — other shards keep running independently.
type SendFunc func(addr string, shardIndex int, payload interface{}) error

// NewSyncFanout constructs a fanout coordinator bound to the supplied
// dispatcher. The dispatcher must outlive the fanout (it owns the
// ownership ledger that DispatchN updates).
func NewSyncFanout(disp *SyncDispatcher) *SyncFanout {
	return &SyncFanout{
		disp:    disp,
		parents: make(map[string]*parentTask),
	}
}

// DispatchN splits parentTaskID into shardTotal sub-tasks and dispatches
// each onto a separate syncnode. Each shard goes through the dispatcher
// (so load-score + ownership ledger + failover hook all work naturally)
// AND each shard gets its own retry budget via the dispatcher's
// maxRetries.
//
// owners maps shardIndex → the addr that accepted the shard. On any
// shard's complete failure (no candidate took it) DispatchN returns a
// partial owners map + a wrapped error. The caller can:
//   - retry the missing shards later (the parent state is still
//     persisted so progress aggregation keeps working for the shards
//     that did land), or
//   - propagate the failure to the operator.
//
// payloadTemplate is the parent's RunTaskRequest-shaped payload (or any
// JSON-marshalable struct). Cloner injects the per-shard SubTaskInfo
// into a fresh copy so the syncnode receives the right shard descriptor.
func (f *SyncFanout) DispatchN(parentTaskID, ruleID string, shardTotal int,
	payloadTemplate interface{}, cloner PayloadCloner, send SendFunc, maxRetries int,
) (map[int]string, error) {
	if parentTaskID == "" {
		return nil, fmt.Errorf("syncfanout: empty parentTaskID")
	}
	if shardTotal <= 0 {
		return nil, fmt.Errorf("syncfanout: shardTotal must be > 0, got %d", shardTotal)
	}
	if cloner == nil {
		return nil, fmt.Errorf("syncfanout: nil PayloadCloner")
	}
	if send == nil {
		return nil, fmt.Errorf("syncfanout: nil SendFunc")
	}
	if f.disp == nil {
		return nil, fmt.Errorf("syncfanout: nil dispatcher")
	}

	// Register the parent up front so progress aggregation works even
	// for partial dispatches (some shards may fail to land; the owners
	// that did accept can still push progress).
	f.registerParent(parentTaskID, ruleID, shardTotal)

	owners := make(map[int]string, shardTotal)
	dispatched := 0
	var firstErr error
	for shard := 0; shard < shardTotal; shard++ {
		shard := shard // closure capture
		subID := subTaskID(parentTaskID, shard)
		payload, cloneErr := cloner.CloneWithSubTask(payloadTemplate, SubTaskInfo{
			ParentTaskID: parentTaskID,
			ShardIndex:   shard,
			ShardTotal:   shardTotal,
		})
		if cloneErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("clone payload shard %d: %w", shard, cloneErr)
			}
			continue
		}
		sendForShard := func(addr string) error {
			return send(addr, shard, payload)
		}
		addr, err := f.disp.Dispatch(subID, sendForShard, maxRetries)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("shard %d: %w", shard, err)
			}
			continue
		}
		owners[shard] = addr
		f.recordOwner(parentTaskID, shard, addr)
		dispatched++
	}

	if dispatched < shardTotal {
		if firstErr == nil {
			firstErr = ErrInsufficientCandidates
		}
		return owners, fmt.Errorf("%w: dispatched %d/%d (first error: %v)",
			ErrInsufficientCandidates, dispatched, shardTotal, firstErr)
	}
	return owners, nil
}

// DispatchNWithPrefixBuckets is the prefix-mode counterpart to DispatchN.
// shardTotal == len(buckets); each shard gets the bucket's prefix list
// attached to its SubTaskInfo.Prefixes field via the cloner. The
// executor on the receiving syncnode will use the prefix list to filter
// the source listing instead of the hash math (see
// syncnode/executor/shard.go::ShouldKeep).
//
// buckets MUST be non-empty and each inner slice MUST also be non-empty
// (we don't want a shard with no work). Returns owners map +/- the same
// partial-success semantics as DispatchN.
//
// The implementation reuses the dispatcher + cloner + registerParent
// scaffolding so failover + progress aggregation work identically to
// hash-mode fan-out.
func (f *SyncFanout) DispatchNWithPrefixBuckets(parentTaskID, ruleID string,
	payloadTemplate interface{}, buckets [][]string, cloner PayloadCloner,
	send SendFunc, maxRetries int,
) (map[int]string, error) {
	if parentTaskID == "" {
		return nil, fmt.Errorf("syncfanout: empty parentTaskID")
	}
	if len(buckets) == 0 {
		return nil, fmt.Errorf("syncfanout: empty buckets")
	}
	for i, b := range buckets {
		if len(b) == 0 {
			return nil, fmt.Errorf("syncfanout: bucket %d is empty", i)
		}
	}
	if cloner == nil {
		return nil, fmt.Errorf("syncfanout: nil PayloadCloner")
	}
	if send == nil {
		return nil, fmt.Errorf("syncfanout: nil SendFunc")
	}
	if f.disp == nil {
		return nil, fmt.Errorf("syncfanout: nil dispatcher")
	}
	shardTotal := len(buckets)
	f.registerParent(parentTaskID, ruleID, shardTotal)

	owners := make(map[int]string, shardTotal)
	dispatched := 0
	var firstErr error
	for shard := 0; shard < shardTotal; shard++ {
		shard := shard // closure capture
		subID := subTaskID(parentTaskID, shard)
		payload, cloneErr := cloner.CloneWithSubTask(payloadTemplate, SubTaskInfo{
			ParentTaskID: parentTaskID,
			ShardIndex:   shard,
			ShardTotal:   shardTotal,
			Prefixes:     buckets[shard],
		})
		if cloneErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("clone payload shard %d: %w", shard, cloneErr)
			}
			continue
		}
		sendForShard := func(addr string) error {
			return send(addr, shard, payload)
		}
		addr, err := f.disp.Dispatch(subID, sendForShard, maxRetries)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("shard %d: %w", shard, err)
			}
			continue
		}
		owners[shard] = addr
		f.recordOwner(parentTaskID, shard, addr)
		dispatched++
	}
	if dispatched < shardTotal {
		if firstErr == nil {
			firstErr = ErrInsufficientCandidates
		}
		return owners, fmt.Errorf("%w: dispatched %d/%d (first error: %v)",
			ErrInsufficientCandidates, dispatched, shardTotal, firstErr)
	}
	return owners, nil
}

// RecordProgress merges a per-sub-task snapshot into the parent's
// aggregate. Idempotent — later snapshots from the same shard overwrite
// earlier ones, so a re-dispatched leaf (P1-4 failover path) doesn't
// double-count its bytes against the prior owner's contribution.
//
// No-op if parentTaskID is unknown (e.g. progress arrives after the
// parent has been cleared).
func (f *SyncFanout) RecordProgress(parentTaskID string, shardIndex int, p TaskProgress) {
	f.mu.Lock()
	defer f.mu.Unlock()
	parent, ok := f.parents[parentTaskID]
	if !ok {
		return
	}
	if shardIndex < 0 || shardIndex >= parent.shardTotal {
		return
	}
	if parent.progress == nil {
		parent.progress = make(map[int]TaskProgress, parent.shardTotal)
	}
	parent.progress[shardIndex] = p
}

// AggregateProgress returns the element-wise sum of every shard's
// latest snapshot for parentTaskID. ok is false when the parent is
// unknown (no DispatchN call has been made for this ID).
func (f *SyncFanout) AggregateProgress(parentTaskID string) (TaskProgress, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	parent, ok := f.parents[parentTaskID]
	if !ok {
		return TaskProgress{}, false
	}
	var sum TaskProgress
	for _, p := range parent.progress {
		sum.FilesDone += p.FilesDone
		sum.FilesTotal += p.FilesTotal
		sum.BytesDone += p.BytesDone
		sum.BytesTotal += p.BytesTotal
	}
	return sum, true
}

// IsParent reports whether taskID was registered via DispatchN. Used by
// /admin/sync/task/get handlers to decide whether to render the parent
// (aggregate) view or a single leaf record.
func (f *SyncFanout) IsParent(taskID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.parents[taskID]
	return ok
}

// Owners returns a snapshot of the shardIndex → owner addr map for
// parentTaskID. Returns nil if the parent is unknown. The map is safe
// to mutate without affecting fanout state.
func (f *SyncFanout) Owners(parentTaskID string) map[int]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	parent, ok := f.parents[parentTaskID]
	if !ok {
		return nil
	}
	out := make(map[int]string, len(parent.subTasks))
	for k, v := range parent.subTasks {
		out[k] = v
	}
	return out
}

// Clear removes the parent's tracking state. Called after every shard
// has reached a terminal status (success / failure / cancel) so the
// fanout map doesn't grow unbounded over the master's lifetime. Caller
// is responsible for invoking this; failure to do so leaks O(shardTotal)
// per orphan parent.
func (f *SyncFanout) Clear(parentTaskID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.parents, parentTaskID)
}

// registerParent inserts the parent record (or refreshes its
// shardTotal if it already exists — supports DispatchN being called
// again after a partial failure).
func (f *SyncFanout) registerParent(parentTaskID, ruleID string, shardTotal int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.parents[parentTaskID]; ok {
		// Refresh the shardTotal but preserve any progress already
		// recorded. Re-dispatch of missing shards is an explicit caller
		// action; we don't reset state here.
		existing.shardTotal = shardTotal
		existing.ruleID = ruleID
		return
	}
	f.parents[parentTaskID] = &parentTask{
		parentTaskID: parentTaskID,
		ruleID:       ruleID,
		shardTotal:   shardTotal,
		subTasks:     make(map[int]string, shardTotal),
		progress:     make(map[int]TaskProgress, shardTotal),
		startedAt:    time.Now(),
		status:       "running",
	}
}

// recordOwner stores the (parentTaskID, shardIndex, addr) triple after
// the dispatcher has accepted the sub-task.
func (f *SyncFanout) recordOwner(parentTaskID string, shardIndex int, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	parent, ok := f.parents[parentTaskID]
	if !ok {
		return
	}
	if parent.subTasks == nil {
		parent.subTasks = make(map[int]string, parent.shardTotal)
	}
	parent.subTasks[shardIndex] = addr
}

// subTaskID derives the canonical sub-task id. Must match the format
// used by syncnode/tasks.TriggerSubTask so failover lookup works.
func subTaskID(parentTaskID string, shardIndex int) string {
	return fmt.Sprintf("%s/%d", parentTaskID, shardIndex)
}

// splitSubTaskID is the inverse of subTaskID. Returns (parent, shard,
// true) when s has the "<parent>/<shard>" shape with a numeric shard
// suffix; (s, 0, false) otherwise (single-task path; not a fan-out
// sub-task).
//
// FIX Q4: splits on the LAST "/" so parent IDs may legitimately contain
// slashes (e.g. "job/2026-05-14"). Since shardIndex is always a
// non-negative integer (subTaskID compose uses %d), the suffix after the
// last "/" is the canonical shard portion. Splitting on the first "/"
// (the previous behaviour) silently mis-identified parents whenever
// operators / external callers used slash-bearing IDs — the parent
// lookup pointed at a wrong prefix and the dispatcher's MarkShardTerminal
// / Clear path didn't fire, leaving leaked parent entries.
func splitSubTaskID(s string) (parent string, shard int, ok bool) {
	slash := strings.LastIndex(s, "/")
	if slash < 0 {
		return s, 0, false
	}
	suffix := s[slash+1:]
	idx, err := strconv.Atoi(suffix)
	if err != nil || idx < 0 {
		return s, 0, false
	}
	return s[:slash], idx, true
}

// MarkShardTerminal records that shardIndex of parentTaskID has reached
// a terminal status (done / failed / cancelled). Returns
// (allTerminal, exists): allTerminal is true when every shard of the
// parent has been marked terminal; exists is false when no parent is
// registered with parentTaskID (e.g. the single-task path, or a
// late report after Clear). Idempotent: marking the same shard twice
// is harmless.
//
// Caller (the task-response handler) is responsible for invoking
// Clear(parentTaskID) when allTerminal && exists.
func (f *SyncFanout) MarkShardTerminal(parentTaskID string, shardIndex int) (allTerminal bool, exists bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.parents[parentTaskID]
	if !ok {
		return false, false
	}
	if shardIndex < 0 || shardIndex >= p.shardTotal {
		return false, true
	}
	if p.terminal == nil {
		p.terminal = make(map[int]bool, p.shardTotal)
	}
	p.terminal[shardIndex] = true
	return len(p.terminal) >= p.shardTotal, true
}

// IsTerminalForParent reports whether every shard of parentTaskID has
// reported terminal. ok is false when the parent is unknown. Intended
// for callers that want to inspect state without mutating it.
func (f *SyncFanout) IsTerminalForParent(parentTaskID string) (allTerminal bool, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, exists := f.parents[parentTaskID]
	if !exists {
		return false, false
	}
	return len(p.terminal) >= p.shardTotal, true
}

// AllParents returns a snapshot of the currently-tracked parent task
// IDs. Order is undefined. Cheap O(n) copy — used by ops dashboards
// and tests; not on the hot path.
func (f *SyncFanout) AllParents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.parents))
	for id := range f.parents {
		out = append(out, id)
	}
	return out
}

// jsonRoundTripFanoutCloner is the production PayloadCloner: JSON-
// encodes the parent payload as a map and overlays the SubTaskInfo on
// the "subTask" field. Mirrors the wire format the syncnode side
// expects (RunTaskRequest.SubTask). Exposed so api_service.go can wire
// it into dispatchSyncTask without duplicating the round-trip logic.
var jsonRoundTripFanoutCloner PayloadCloner = PayloadClonerFunc(func(parent interface{}, info SubTaskInfo) (interface{}, error) {
	raw, err := json.Marshal(parent)
	if err != nil {
		return nil, fmt.Errorf("marshal parent payload: %w", err)
	}
	out := map[string]interface{}{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("unmarshal parent payload into map: %w", err)
	}
	out["subTask"] = info
	// Rewrite taskId to the shard's own sub-task ID so the syncnode reports
	// back the correct ID in its terminal push. Without this the terminal
	// handler receives rep.TaskID = parentTaskID and marks the parent record
	// terminal instead of the shard record, leaving shards stuck "running".
	if info.ParentTaskID != "" {
		out["taskId"] = subTaskID(info.ParentTaskID, info.ShardIndex)
	}
	return out, nil
})

// Recover rebuilds the in-memory parents map from the dispatcher's
// ownership ledger after a master leader transition (FIX #7).
//
// The fan-out parents map lives in master process memory; a leader
// election or master restart wipes it. Sub-tasks on syncnodes survive
// because they're independent runs identified by `<parent>/<shard>` IDs
// in the dispatcher's ownership ledger. Recover scans the ledger,
// groups by parent ID, and reconstructs minimal parent state
// (shardTotal = max(shardIndex)+1, subTasks = the surviving owner map).
//
// Lost on recovery:
//   - ruleID (parent ID alone doesn't carry it) — set to "" and let
//     callers fill it in if they re-trigger;
//   - startedAt / progress history (no persistent store of these);
//   - sub-tasks whose dispatcher record has already been Released
//     (i.e. terminal sub-tasks pre-restart).
//
// Survives:
//   - sub-task ownership (master can still send Cancel / receive
//     terminal responses for them);
//   - parent identification (IsParent works correctly so
//     /admin/sync/task/get on the parent ID renders the parent shape);
//   - new RecordProgress / Owners / Clear calls.
//
// Idempotent: Recover-on-empty-ledger is a no-op; re-running over the
// same ledger doesn't duplicate state.
//
// Returns the number of parent records reconstructed.
func (f *SyncFanout) Recover() int {
	if f == nil || f.disp == nil {
		return 0
	}
	ownerships := f.disp.AllOwnerships()
	// Group sub-task IDs by parent prefix (split on the FIRST "/").
	type shardInfo struct {
		index int
		owner string
	}
	grouped := make(map[string][]shardInfo)
	for taskID, addr := range ownerships {
		slash := strings.Index(taskID, "/")
		if slash < 0 {
			continue // not a sub-task
		}
		parentID := taskID[:slash]
		idxStr := taskID[slash+1:]
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue // sub-task ID without numeric shard suffix; skip
		}
		grouped[parentID] = append(grouped[parentID], shardInfo{idx, addr})
	}
	if len(grouped) == 0 {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	recovered := 0
	for parentID, shards := range grouped {
		// Skip if we already have this parent (idempotency).
		if _, exists := f.parents[parentID]; exists {
			continue
		}
		maxIdx := -1
		subTasks := make(map[int]string, len(shards))
		for _, s := range shards {
			subTasks[s.index] = s.owner
			if s.index > maxIdx {
				maxIdx = s.index
			}
		}
		f.parents[parentID] = &parentTask{
			parentTaskID: parentID,
			ruleID:       "", // lost on recovery — caller can re-attach
			shardTotal:   maxIdx + 1,
			subTasks:     subTasks,
			progress:     make(map[int]TaskProgress),
			startedAt:    time.Now(), // approximate — we don't persist the true value
			status:       "running",
		}
		recovered++
	}
	return recovered
}
