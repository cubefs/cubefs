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
	"testing"
	"time"
)

func mkRec(id, owner string, st SyncTaskStatus) *SyncTaskRecord {
	return &SyncTaskRecord{
		TaskID:    id,
		Owner:     owner,
		Status:    st,
		StartedAt: time.Now(),
	}
}

func TestSyncTaskLedger_PutGet(t *testing.T) {
	l := NewSyncTaskLedger(100)
	l.Put(mkRec("t-1", "a:1", SyncTaskStatusRunning))
	got := l.Get("t-1")
	if got == nil {
		t.Fatal("Get returned nil for inserted record")
	}
	if got.Owner != "a:1" {
		t.Errorf("Owner = %q, want a:1", got.Owner)
	}
}

func TestSyncTaskLedger_PutUpdatesOwner(t *testing.T) {
	l := NewSyncTaskLedger(100)
	l.Put(mkRec("t-1", "a:1", SyncTaskStatusRunning))
	l.Put(mkRec("t-1", "b:2", SyncTaskStatusRunning))
	if got := l.ListByOwner("a:1", ""); len(got) != 0 {
		t.Errorf("old owner index still holds: %v", got)
	}
	if got := l.ListByOwner("b:2", ""); len(got) != 1 {
		t.Errorf("new owner index = %d, want 1", len(got))
	}
}

func TestSyncTaskLedger_Move(t *testing.T) {
	l := NewSyncTaskLedger(100)
	l.Put(mkRec("t-1", "a:1", SyncTaskStatusRunning))
	l.Move("t-1", "b:2")
	rec := l.Get("t-1")
	if rec.Owner != "b:2" {
		t.Errorf("Owner after Move = %q, want b:2", rec.Owner)
	}
	if len(l.ListByOwner("a:1", "")) != 0 {
		t.Error("old owner index still has t-1")
	}
	if len(l.ListByOwner("b:2", "")) != 1 {
		t.Error("new owner index missing t-1")
	}
}

func TestSyncTaskLedger_Remove(t *testing.T) {
	l := NewSyncTaskLedger(100)
	l.Put(mkRec("t-1", "a:1", SyncTaskStatusRunning))
	l.Remove("t-1")
	if l.Get("t-1") != nil {
		t.Error("Get returned non-nil after Remove")
	}
	if len(l.ListByOwner("a:1", "")) != 0 {
		t.Error("owner index still has removed task")
	}
}

func TestSyncTaskLedger_LRUEviction(t *testing.T) {
	l := NewSyncTaskLedger(3)
	l.Put(mkRec("t-1", "a:1", SyncTaskStatusRunning))
	l.Put(mkRec("t-2", "a:1", SyncTaskStatusRunning))
	l.Put(mkRec("t-3", "a:1", SyncTaskStatusRunning))
	l.Put(mkRec("t-4", "a:1", SyncTaskStatusRunning))
	if l.Get("t-1") != nil {
		t.Error("oldest record t-1 should have been evicted")
	}
	if l.Get("t-4") == nil {
		t.Error("newest record t-4 should be present")
	}
	if l.Len() != 3 {
		t.Errorf("Len = %d, want 3 (cap)", l.Len())
	}
	if got := l.ListByOwner("a:1", ""); len(got) != 3 {
		t.Errorf("owner index after eviction = %d, want 3", len(got))
	}
}

func TestSyncTaskLedger_ListFilters(t *testing.T) {
	l := NewSyncTaskLedger(100)
	l.Put(&SyncTaskRecord{TaskID: "t-1", RuleID: "r-a", Owner: "n1", Status: SyncTaskStatusRunning})
	l.Put(&SyncTaskRecord{TaskID: "t-2", RuleID: "r-a", Owner: "n2", Status: SyncTaskStatusSucceeded})
	l.Put(&SyncTaskRecord{TaskID: "t-3", RuleID: "r-b", Owner: "n1", Status: SyncTaskStatusFailed})

	tests := []struct {
		name  string
		st    SyncTaskStatus
		rule  string
		owner string
		wantN int
	}{
		{"all", "", "", "", 3},
		{"status running", SyncTaskStatusRunning, "", "", 1},
		{"rule r-a", "", "r-a", "", 2},
		{"owner n1", "", "", "n1", 2},
		{"status+rule", SyncTaskStatusSucceeded, "r-a", "", 1},
		{"empty result", SyncTaskStatusRunning, "r-b", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := l.List(tc.st, tc.rule, tc.owner)
			if len(got) != tc.wantN {
				t.Errorf("len = %d, want %d", len(got), tc.wantN)
			}
		})
	}
}

func TestSyncTaskLedger_ActiveTaskIDsOnOwner(t *testing.T) {
	l := NewSyncTaskLedger(100)
	l.Put(mkRec("t-run", "n1", SyncTaskStatusRunning))
	l.Put(mkRec("t-queued", "n1", SyncTaskStatusQueued))
	l.Put(mkRec("t-done", "n1", SyncTaskStatusSucceeded))
	l.Put(mkRec("t-failed", "n1", SyncTaskStatusFailed))
	l.Put(mkRec("t-other", "n2", SyncTaskStatusRunning))

	got := l.ActiveTaskIDsOnOwner("n1")
	if len(got) != 2 {
		t.Errorf("len = %d (got %v), want 2 (running + queued)", len(got), got)
	}
	for _, id := range got {
		if id == "t-done" || id == "t-failed" {
			t.Errorf("terminal task %q leaked into active set", id)
		}
	}
}

func TestSyncTaskLedger_NilSafe(t *testing.T) {
	var l *SyncTaskLedger
	l.Put(mkRec("t-1", "", SyncTaskStatusRunning))
	if l.Get("t-1") != nil {
		t.Error("nil ledger Get should return nil")
	}
	if l.Len() != 0 {
		t.Error("nil ledger Len should return 0")
	}
	if got := l.List("", "", ""); got != nil {
		t.Errorf("nil ledger List should return nil, got %v", got)
	}
}
