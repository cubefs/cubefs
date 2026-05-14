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
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/tasks"
)

func TestRecover_MarksRunningAndPendingAsFailed(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	ts := db.TaskStore()

	now := time.Now()
	// Seed: 1 done (should not be touched), 1 running, 1 pending.
	done := newDoneRec("done", "r1", now.Add(-time.Hour))
	running := newRunningRec("running", "r1", now.Add(-time.Minute))
	pending := &tasks.Record{
		TaskID:    "pending",
		RuleID:    "r1",
		Type:      executor.TaskTypeSync,
		Status:    executor.StatusPending,
		StartedAt: now,
	}
	for _, r := range []*tasks.Record{done, running, pending} {
		if err := ts.Put(ctx, r); err != nil {
			t.Fatalf("Put %s: %v", r.TaskID, err)
		}
	}

	adjusted, err := db.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if adjusted != 2 {
		t.Errorf("adjusted = %d, want 2", adjusted)
	}

	// Verify each record's post-state.
	doneAfter, _ := ts.Get(ctx, "done")
	if doneAfter.Status != executor.StatusDone {
		t.Errorf("done record was touched: %v", doneAfter.Status)
	}
	for _, id := range []string{"running", "pending"} {
		r, err := ts.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if r.Status != executor.StatusFailed {
			t.Errorf("%s: status = %v, want Failed", id, r.Status)
		}
		if r.Error != InterruptedErrorString {
			t.Errorf("%s: Error = %q, want %q", id, r.Error, InterruptedErrorString)
		}
		if r.DoneAt.IsZero() {
			t.Errorf("%s: DoneAt should be stamped", id)
		}
	}
}

func TestRecover_Idempotent(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	ts := db.TaskStore()

	now := time.Now()
	_ = ts.Put(ctx, newRunningRec("t1", "r1", now))

	first, err := db.Recover(ctx)
	if err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	if first != 1 {
		t.Errorf("first adjusted = %d", first)
	}
	second, err := db.Recover(ctx)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	if second != 0 {
		t.Errorf("second adjusted = %d, want 0 (idempotent)", second)
	}
}

func TestRecover_EmptyDB(t *testing.T) {
	db := newTestDB(t)
	n, err := db.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover empty: %v", err)
	}
	if n != 0 {
		t.Errorf("adjusted = %d on empty DB", n)
	}
}

func TestRecover_CtxCancelled(t *testing.T) {
	db := newTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.Recover(ctx); err == nil {
		t.Error("Recover with cancelled ctx should return error")
	}
}

func TestOrphanBreakpoints(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	// Seed: 1 active task with matching breakpoint, 1 lonely breakpoint.
	_ = db.TaskStore().Put(ctx, newRunningRec("t1", "r1", time.Now()))
	_ = db.InProgress().Put(ctx, &Breakpoint{TaskID: "t1", Key: "k1", BytesDone: 100})
	_ = db.InProgress().Put(ctx, &Breakpoint{TaskID: "ghost", Key: "k2", BytesDone: 200})

	orphans, err := db.OrphanBreakpoints(ctx)
	if err != nil {
		t.Fatalf("OrphanBreakpoints: %v", err)
	}
	if len(orphans) != 1 || orphans[0].TaskID != "ghost" {
		t.Errorf("orphans = %+v", orphans)
	}
}

func TestOrphanBreakpoints_CtxCancelled(t *testing.T) {
	db := newTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.OrphanBreakpoints(ctx); err == nil {
		t.Error("OrphanBreakpoints with cancelled ctx should return error")
	}
}

// TestRecover_AcceptanceCriterion exercises the F-2 AC narrative: a task
// that was running at kill -9 surfaces as a failed record with a clear
// "interrupted" cause after Recover (= the moral equivalent of the spec's
// "status=interrupted 任务可见").
func TestRecover_AcceptanceCriterion(t *testing.T) {
	dir := t.TempDir()
	db1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Simulate task halfway through (50% per the AC).
	rec := &tasks.Record{
		TaskID:    "halfway",
		RuleID:    "r1",
		Type:      executor.TaskTypeSync,
		Status:    executor.StatusRunning,
		StartedAt: time.Now().Add(-time.Minute),
		Progress: executor.Progress{
			BytesDone:  50,
			BytesTotal: 100,
		},
	}
	if err := db1.TaskStore().Put(context.Background(), rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// kill -9 surrogate: drop the handle without Close.
	if err := db1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Restart.
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	if err := db2.Health(); err != nil {
		t.Errorf("Health after restart: %v", err)
	}
	n, err := db2.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n != 1 {
		t.Errorf("adjusted = %d, want 1", n)
	}
	got, err := db2.TaskStore().Get(context.Background(), "halfway")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != executor.StatusFailed {
		t.Errorf("Status = %v, want Failed", got.Status)
	}
	if got.Error != InterruptedErrorString {
		t.Errorf("Error = %q, want %q", got.Error, InterruptedErrorString)
	}
	// 50% progress should be preserved so operators can see how far
	// the task got before the kill.
	if got.Progress.BytesDone != 50 || got.Progress.BytesTotal != 100 {
		t.Errorf("Progress mangled: %+v", got.Progress)
	}
}
