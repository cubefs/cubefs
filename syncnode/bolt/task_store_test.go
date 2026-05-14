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

package bolt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/tasks"
)

// Static interface assertion: taskStore must implement tasks.Store.
var _ tasks.Store = (*taskStore)(nil)

func newRunningRec(id, ruleID string, startedAt time.Time) *tasks.Record {
	return &tasks.Record{
		TaskID:    id,
		RuleID:    ruleID,
		Type:      executor.TaskTypeSync,
		Status:    executor.StatusRunning,
		StartedAt: startedAt,
	}
}

func newDoneRec(id, ruleID string, doneAt time.Time) *tasks.Record {
	return &tasks.Record{
		TaskID:    id,
		RuleID:    ruleID,
		Type:      executor.TaskTypeSync,
		Status:    executor.StatusDone,
		StartedAt: doneAt.Add(-time.Minute),
		DoneAt:    doneAt,
	}
}

func TestTaskStore_PutGet(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	rec := newRunningRec("t1", "r1", time.Now())
	if err := s.Put(ctx, rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.TaskID != "t1" || got.RuleID != "r1" || got.Status != executor.StatusRunning {
		t.Errorf("got = %+v", got)
	}
}

func TestTaskStore_PutNilOrEmpty(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()
	if err := s.Put(ctx, nil); !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Errorf("nil Put err = %v", err)
	}
	if err := s.Put(ctx, &tasks.Record{}); !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Errorf("empty-id Put err = %v", err)
	}
}

func TestTaskStore_GetUnknown(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	_, err := s.Get(context.Background(), "ghost")
	if !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Errorf("err = %v", err)
	}
}

func TestTaskStore_OverwriteOnPut(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	if err := s.Put(ctx, newRunningRec("t1", "r1", time.Now())); err != nil {
		t.Fatalf("Put: %v", err)
	}
	updated := newRunningRec("t1", "r1", time.Now())
	updated.Status = executor.StatusDone
	updated.DoneAt = time.Now()
	if err := s.Put(ctx, updated); err != nil {
		t.Fatalf("overwrite Put: %v", err)
	}
	got, _ := s.Get(ctx, "t1")
	if got.Status != executor.StatusDone {
		t.Errorf("status not overwritten: %v", got.Status)
	}
}

func TestTaskStore_ListSortedAndFiltered(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	now := time.Now()
	_ = s.Put(ctx, newRunningRec("oldest", "r1", now.Add(-2*time.Hour)))
	_ = s.Put(ctx, newRunningRec("middle", "r1", now.Add(-time.Hour)))
	doneRec := newDoneRec("newest_done", "r1", now)
	_ = s.Put(ctx, doneRec)

	all, err := s.List(ctx, "")
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("len = %d", len(all))
	}
	if all[0].TaskID != "newest_done" || all[2].TaskID != "oldest" {
		t.Errorf("List not sorted by StartedAt desc: %+v",
			[]string{all[0].TaskID, all[1].TaskID, all[2].TaskID})
	}

	running, err := s.List(ctx, executor.StatusRunning)
	if err != nil {
		t.Fatalf("filtered List: %v", err)
	}
	if len(running) != 2 {
		t.Errorf("running count = %d", len(running))
	}
	for _, r := range running {
		if r.Status != executor.StatusRunning {
			t.Errorf("filter leaked: %s status=%s", r.TaskID, r.Status)
		}
	}
}

func TestTaskStore_Delete(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	_ = s.Put(ctx, newRunningRec("t1", "r1", time.Now()))
	if err := s.Delete(ctx, "t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, "t1"); !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Errorf("second Delete err = %v", err)
	}
}

func TestTaskStore_MoveToHistory_NonTerminal(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	_ = s.Put(ctx, newRunningRec("t1", "r1", time.Now()))
	err := s.MoveToHistory(ctx, "t1")
	if !errors.Is(err, tasks.ErrTaskNotTerminal) {
		t.Errorf("err = %v, want ErrTaskNotTerminal", err)
	}
}

func TestTaskStore_MoveToHistory_Unknown(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	err := s.MoveToHistory(context.Background(), "ghost")
	if !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Errorf("err = %v, want ErrTaskNotFound", err)
	}
}

func TestTaskStore_MoveToHistory_HappyPath(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	rec := newDoneRec("t1", "r1", time.Now())
	if err := s.Put(ctx, rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.MoveToHistory(ctx, "t1"); err != nil {
		t.Fatalf("MoveToHistory: %v", err)
	}
	if _, err := s.Get(ctx, "t1"); !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Errorf("after move, active Get err = %v", err)
	}
	hist, err := s.ListHistory(ctx, time.Time{})
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	if len(hist) != 1 || hist[0].TaskID != "t1" {
		t.Errorf("ListHistory result = %+v", hist)
	}
}

func TestTaskStore_MoveToHistory_Idempotent(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	rec := newDoneRec("t1", "r1", time.Now())
	_ = s.Put(ctx, rec)
	if err := s.MoveToHistory(ctx, "t1"); err != nil {
		t.Fatalf("first MoveToHistory: %v", err)
	}
	// Second call is a no-op (already in history).
	if err := s.MoveToHistory(ctx, "t1"); err != nil {
		t.Fatalf("second MoveToHistory: %v", err)
	}
}

func TestTaskStore_MoveToHistory_ConvergesActiveStraggler(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	rec := newDoneRec("t1", "r1", time.Now())
	_ = s.Put(ctx, rec)
	if err := s.MoveToHistory(ctx, "t1"); err != nil {
		t.Fatalf("first MoveToHistory: %v", err)
	}
	// Pretend a stale active row reappears (e.g. a race resurrected it).
	_ = s.Put(ctx, rec)
	if err := s.MoveToHistory(ctx, "t1"); err != nil {
		t.Fatalf("convergence MoveToHistory: %v", err)
	}
	if _, err := s.Get(ctx, "t1"); !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Errorf("active row should be gone after convergence: %v", err)
	}
}

func TestTaskStore_ListHistory_SinceCutoff(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	old := newDoneRec("old", "r1", time.Now().Add(-48*time.Hour))
	mid := newDoneRec("mid", "r1", time.Now().Add(-24*time.Hour))
	fresh := newDoneRec("fresh", "r1", time.Now())
	for _, r := range []*tasks.Record{old, mid, fresh} {
		_ = s.Put(ctx, r)
		if err := s.MoveToHistory(ctx, r.TaskID); err != nil {
			t.Fatalf("MoveToHistory %s: %v", r.TaskID, err)
		}
	}
	got, err := s.ListHistory(ctx, time.Now().Add(-36*time.Hour))
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (mid + fresh)", len(got))
	}
	if got[0].TaskID != "fresh" || got[1].TaskID != "mid" {
		t.Errorf("not sorted by DoneAt desc: %+v",
			[]string{got[0].TaskID, got[1].TaskID})
	}
}

func TestTaskStore_PurgeHistoryBefore(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	old := newDoneRec("old", "r1", time.Now().Add(-48*time.Hour))
	fresh := newDoneRec("fresh", "r1", time.Now())
	for _, r := range []*tasks.Record{old, fresh} {
		_ = s.Put(ctx, r)
		if err := s.MoveToHistory(ctx, r.TaskID); err != nil {
			t.Fatalf("move %s: %v", r.TaskID, err)
		}
	}
	n, err := s.PurgeHistoryBefore(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 1 {
		t.Errorf("purged = %d, want 1", n)
	}
	left, _ := s.ListHistory(ctx, time.Time{})
	if len(left) != 1 || left[0].TaskID != "fresh" {
		t.Errorf("after purge, history = %+v", left)
	}
}

func TestTaskStore_PurgeHistoryBefore_NothingMatches(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	fresh := newDoneRec("fresh", "r1", time.Now())
	_ = s.Put(ctx, fresh)
	_ = s.MoveToHistory(ctx, "fresh")

	n, err := s.PurgeHistoryBefore(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 0 {
		t.Errorf("purged = %d, want 0", n)
	}
}

func TestTaskStore_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	db1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	now := time.Now()
	_ = db1.TaskStore().Put(context.Background(), newRunningRec("t1", "r1", now))
	rec := newDoneRec("t2", "r1", now)
	_ = db1.TaskStore().Put(context.Background(), rec)
	_ = db1.TaskStore().MoveToHistory(context.Background(), "t2")
	if err := db1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	got, err := db2.TaskStore().Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Get t1 after reopen: %v", err)
	}
	if got.TaskID != "t1" {
		t.Errorf("got = %+v", got)
	}
	hist, _ := db2.TaskStore().ListHistory(context.Background(), time.Time{})
	if len(hist) != 1 || hist[0].TaskID != "t2" {
		t.Errorf("history after reopen = %+v", hist)
	}
}

func TestTaskStore_CloseNoop(t *testing.T) {
	db := newTestDB(t)
	if err := db.TaskStore().Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestTaskStore_DeepCopyOnRead(t *testing.T) {
	db := newTestDB(t)
	s := db.TaskStore()
	ctx := context.Background()

	rec := newRunningRec("t1", "r1", time.Now())
	rec.Mismatches = []executor.Mismatch{{Key: "k", Reason: executor.MismatchETagDiffer}}
	_ = s.Put(ctx, rec)

	got, _ := s.Get(ctx, "t1")
	got.Mismatches[0].Key = "MUTATED"

	again, _ := s.Get(ctx, "t1")
	if again.Mismatches[0].Key != "k" {
		t.Errorf("mutation leaked through Get: %s", again.Mismatches[0].Key)
	}
}
