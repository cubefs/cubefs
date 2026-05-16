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
	"container/list"
	"sync"
	"time"
)

// Phase 4 — master-side task ledger for /syncTask/* + /syncNode/tasks.
//
// The ledger answers two console-driven questions that the existing
// SyncDispatcher.taskOwner ledger does not:
//
//  1. "Which tasks are running on this node right now?"
//     → reverse index addr → set[taskID]
//
//  2. "What was the terminal status of this task and on which node?"
//     → forward index taskID → SyncTaskRecord with status + owner +
//       progress + error
//
// LRU bounded by SyncTaskLedgerCap (default 10k). On Add when the
// capacity is hit, the least-recently-used record is evicted (its addr
// reverse index entry is also cleared). For longer history operators
// rely on Prometheus + log scraping — master is not the persistent
// audit store.
//
// All methods are safe for concurrent use; reads and writes share a
// single RWMutex. The reverse index is a map[addr]map[taskID]struct{}
// for O(1) set membership; List operations copy out so callers can
// iterate without holding the lock.

// SyncTaskLedgerCap caps the active+terminal record count. Adjust at
// startup via NewSyncTaskLedger.
const SyncTaskLedgerCap = 10000

// SyncTaskStatus mirrors the syncnode-side executor.Status string. We
// keep a master-local type so the master doesn't import syncnode (cycle).
// Values are byte-identical to syncnode's so JSON travels unchanged.
type SyncTaskStatus string

const (
	SyncTaskStatusQueued    SyncTaskStatus = "queued"
	SyncTaskStatusRunning   SyncTaskStatus = "running"
	SyncTaskStatusSucceeded SyncTaskStatus = "succeeded"
	SyncTaskStatusFailed    SyncTaskStatus = "failed"
	SyncTaskStatusCancelled SyncTaskStatus = "cancelled"
)

// IsTerminal returns true when the status is one of the end-state
// values: succeeded, failed, or cancelled.
func (s SyncTaskStatus) IsTerminal() bool {
	switch s {
	case SyncTaskStatusSucceeded, SyncTaskStatusFailed, SyncTaskStatusCancelled:
		return true
	}
	return false
}

// SyncTaskProgress is a snapshot of one task's progress. Mirrors
// syncnode/executor.Progress so JSON travels through the heartbeat reply
// + /syncNode/response without re-marshal.
type SyncTaskProgress struct {
	FilesTotal     int64   `json:"filesTotal"`
	FilesDone      int64   `json:"filesDone"`
	FilesSkipped   int64   `json:"filesSkipped"`
	FilesFailed    int64   `json:"filesFailed"`
	BytesTotal     int64   `json:"bytesTotal"`
	BytesDone      int64   `json:"bytesDone"`
	ThroughputMBps float64 `json:"throughputMBps"`
}

// SyncTaskRecord is what /syncTask/get and /syncTask/list return. Carries
// enough context for an operator to triage a task without further calls
// (status, owner, rule id, progress snapshot, error if failed). Parent
// fan-out tasks set ShardTotal > 0 + leave Owner empty; their child
// records carry ShardIndex + an Owner addr.
type SyncTaskRecord struct {
	TaskID     string           `json:"taskID"`
	RuleID     string           `json:"ruleID"`
	Type       string           `json:"type"`
	Status     SyncTaskStatus   `json:"status"`
	Owner      string           `json:"owner,omitempty"` // empty for parent fan-out tasks
	ShardIdx   int              `json:"shardIdx,omitempty"`
	ShardTotal int              `json:"shardTotal,omitempty"`
	StartedAt  time.Time        `json:"startedAt"`
	DoneAt     time.Time        `json:"doneAt,omitempty"`
	Error      string           `json:"error,omitempty"`
	Progress   SyncTaskProgress `json:"progress"`
}

// SyncTaskLedger is the in-memory record store with LRU bounded
// capacity and an addr-keyed reverse index for /syncNode/tasks.
type SyncTaskLedger struct {
	mu sync.RWMutex

	cap int

	// records is the primary index: taskID → element pointer into lru.
	records map[string]*list.Element
	// lru is the LRU list ordered most-recent-first. Each element value
	// is *SyncTaskRecord.
	lru *list.List
	// byOwner is the reverse index: owner addr → set of taskIDs.
	byOwner map[string]map[string]struct{}
}

// NewSyncTaskLedger constructs a ledger with the given capacity. cap<=0
// falls back to SyncTaskLedgerCap. The instance is safe to construct at
// Cluster init time and shared across goroutines.
func NewSyncTaskLedger(cap int) *SyncTaskLedger {
	if cap <= 0 {
		cap = SyncTaskLedgerCap
	}
	return &SyncTaskLedger{
		cap:     cap,
		records: make(map[string]*list.Element, cap),
		lru:     list.New(),
		byOwner: make(map[string]map[string]struct{}),
	}
}

// Put inserts or updates the record for rec.TaskID. New entries push to
// the LRU front; updates move the existing element to front. When the
// capacity is exceeded the oldest record is evicted (its reverse-index
// entry is also cleaned).
func (l *SyncTaskLedger) Put(rec *SyncTaskRecord) {
	if l == nil || rec == nil || rec.TaskID == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if elem, ok := l.records[rec.TaskID]; ok {
		// Update path. Maintain reverse-index if owner changed (e.g. a
		// SyncFailover redispatch flipped ownership).
		old, _ := elem.Value.(*SyncTaskRecord)
		if old != nil && old.Owner != rec.Owner {
			l.unindexOwnerLocked(old.Owner, rec.TaskID)
			l.indexOwnerLocked(rec.Owner, rec.TaskID)
		}
		elem.Value = rec
		l.lru.MoveToFront(elem)
		return
	}
	// Insert path.
	elem := l.lru.PushFront(rec)
	l.records[rec.TaskID] = elem
	l.indexOwnerLocked(rec.Owner, rec.TaskID)
	if l.lru.Len() > l.cap {
		l.evictOldestLocked()
	}
}

// Get returns the record for taskID, or nil if absent. The returned
// pointer is the LIVE record — callers MUST treat it as read-only.
func (l *SyncTaskLedger) Get(taskID string) *SyncTaskRecord {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if elem, ok := l.records[taskID]; ok {
		return elem.Value.(*SyncTaskRecord)
	}
	return nil
}

// Remove drops the record for taskID. Idempotent. Used when failover
// redispatches a task and the old record should not pollute /syncTask
// listings.
func (l *SyncTaskLedger) Remove(taskID string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	elem, ok := l.records[taskID]
	if !ok {
		return
	}
	rec := elem.Value.(*SyncTaskRecord)
	l.lru.Remove(elem)
	delete(l.records, taskID)
	l.unindexOwnerLocked(rec.Owner, taskID)
}

// Move re-points a task's owner from oldAddr to newAddr (SyncFailover
// invokes this when a node dies and the task is redispatched). The
// record's fields are NOT mutated other than Owner; the caller is
// expected to fold in fresh progress via a follow-up Put when the new
// owner reports back.
func (l *SyncTaskLedger) Move(taskID, newAddr string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	elem, ok := l.records[taskID]
	if !ok {
		return
	}
	rec := elem.Value.(*SyncTaskRecord)
	if rec.Owner == newAddr {
		return
	}
	l.unindexOwnerLocked(rec.Owner, taskID)
	rec.Owner = newAddr
	l.indexOwnerLocked(newAddr, taskID)
	l.lru.MoveToFront(elem)
}

// List returns a slice of records matching the supplied filters. Empty
// status / ruleID / owner act as wildcards. Results are sorted by
// StartedAt descending (most-recent-first via LRU order). Caller may
// mutate the slice but MUST treat individual records as read-only.
func (l *SyncTaskLedger) List(status SyncTaskStatus, ruleID, owner string) []*SyncTaskRecord {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]*SyncTaskRecord, 0)
	for e := l.lru.Front(); e != nil; e = e.Next() {
		rec := e.Value.(*SyncTaskRecord)
		if status != "" && rec.Status != status {
			continue
		}
		if ruleID != "" && rec.RuleID != ruleID {
			continue
		}
		if owner != "" && rec.Owner != owner {
			continue
		}
		out = append(out, rec)
	}
	return out
}

// UpdateProgress updates the progress snapshot of a non-terminal record.
// This is called on every heartbeat so the task ledger shows in-flight
// progress. No-op when the record is absent or already terminal (we
// never overwrite a completed task's final progress with a stale
// in-flight reading that arrives after the terminal report).
func (l *SyncTaskLedger) UpdateProgress(taskID string, progress SyncTaskProgress) {
	if l == nil || taskID == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	elem, ok := l.records[taskID]
	if !ok {
		return
	}
	rec := elem.Value.(*SyncTaskRecord)
	if rec.Status.IsTerminal() {
		return
	}
	updated := *rec
	updated.Progress = progress
	elem.Value = &updated
}

// ListByOwner returns every record assigned to owner addr. Empty filter
// → all records on that node (regardless of terminal status). Use
// status="" for an unfiltered snapshot.
func (l *SyncTaskLedger) ListByOwner(addr string, status SyncTaskStatus) []*SyncTaskRecord {
	if l == nil || addr == "" {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	ids, ok := l.byOwner[addr]
	if !ok {
		return nil
	}
	out := make([]*SyncTaskRecord, 0, len(ids))
	for id := range ids {
		elem, ok := l.records[id]
		if !ok {
			continue
		}
		rec := elem.Value.(*SyncTaskRecord)
		if status != "" && rec.Status != status {
			continue
		}
		out = append(out, rec)
	}
	return out
}

// ActiveTaskIDsOnOwner returns the taskIDs on addr whose status is NOT
// terminal. Used by /syncNode/drain to know what to cancel.
func (l *SyncTaskLedger) ActiveTaskIDsOnOwner(addr string) []string {
	if l == nil || addr == "" {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	ids, ok := l.byOwner[addr]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		elem, ok := l.records[id]
		if !ok {
			continue
		}
		rec := elem.Value.(*SyncTaskRecord)
		if rec.Status.IsTerminal() {
			continue
		}
		out = append(out, id)
	}
	return out
}

// Len returns the total number of records currently cached.
func (l *SyncTaskLedger) Len() int {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lru.Len()
}

// indexOwnerLocked adds taskID to the byOwner reverse index.
// Caller must hold l.mu. addr=="" → no-op.
func (l *SyncTaskLedger) indexOwnerLocked(addr, taskID string) {
	if addr == "" {
		return
	}
	bucket, ok := l.byOwner[addr]
	if !ok {
		bucket = make(map[string]struct{}, 4)
		l.byOwner[addr] = bucket
	}
	bucket[taskID] = struct{}{}
}

// unindexOwnerLocked removes taskID from the byOwner reverse index.
// Caller must hold l.mu. Cleans up the bucket if empty.
func (l *SyncTaskLedger) unindexOwnerLocked(addr, taskID string) {
	if addr == "" {
		return
	}
	bucket, ok := l.byOwner[addr]
	if !ok {
		return
	}
	delete(bucket, taskID)
	if len(bucket) == 0 {
		delete(l.byOwner, addr)
	}
}

// evictOldestLocked removes the least-recently-used record. Caller must
// hold l.mu and have already verified that lru.Len() > cap.
func (l *SyncTaskLedger) evictOldestLocked() {
	elem := l.lru.Back()
	if elem == nil {
		return
	}
	rec := elem.Value.(*SyncTaskRecord)
	l.lru.Remove(elem)
	delete(l.records, rec.TaskID)
	l.unindexOwnerLocked(rec.Owner, rec.TaskID)
}
