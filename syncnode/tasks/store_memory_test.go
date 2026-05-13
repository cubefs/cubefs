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

package tasks

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
)

func newRunningRecord(id, ruleID string, startedAt time.Time) *Record {
	return &Record{
		TaskID:    id,
		RuleID:    ruleID,
		Type:      executor.TaskTypeSync,
		Status:    executor.StatusRunning,
		StartedAt: startedAt,
	}
}

func TestMemoryStore_PutGetRoundtrip(t *testing.T) {
	s := NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	rec := newRunningRecord("t1", "r1", time.Now())
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

func TestMemoryStore_GetUnknown(t *testing.T) {
	s := NewMemoryStore()
	_, err := s.Get(context.Background(), "ghost")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("err = %v, want ErrTaskNotFound", err)
	}
}

func TestMemoryStore_PutNilOrEmptyID(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Put(context.Background(), nil); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("nil Put err = %v", err)
	}
	if err := s.Put(context.Background(), &Record{TaskID: ""}); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("empty-id Put err = %v", err)
	}
}

func TestMemoryStore_OverwriteOnPut(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	_ = s.Put(ctx, newRunningRecord("t", "r", time.Now()))
	rec, _ := s.Get(ctx, "t")
	rec.Status = executor.StatusDone
	_ = s.Put(ctx, rec)
	got, _ := s.Get(ctx, "t")
	if got.Status != executor.StatusDone {
		t.Errorf("Status = %q, want done", got.Status)
	}
}

func TestMemoryStore_ListSortedByStartedAtDesc(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	now := time.Now()
	_ = s.Put(ctx, newRunningRecord("t1", "r", now.Add(-2*time.Hour)))
	_ = s.Put(ctx, newRunningRecord("t2", "r", now))
	_ = s.Put(ctx, newRunningRecord("t3", "r", now.Add(-1*time.Hour)))

	got, err := s.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{"t2", "t3", "t1"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i, r := range got {
		if r.TaskID != want[i] {
			t.Errorf("[%d] = %q, want %q", i, r.TaskID, want[i])
		}
	}
}

func TestMemoryStore_ListEmpty(t *testing.T) {
	s := NewMemoryStore()
	got, err := s.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func TestMemoryStore_ListWithStatusFilter(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	r1 := newRunningRecord("t1", "r", time.Now())
	r1.Status = executor.StatusDone
	r2 := newRunningRecord("t2", "r", time.Now())
	r2.Status = executor.StatusFailed
	r3 := newRunningRecord("t3", "r", time.Now())
	// running

	_ = s.Put(ctx, r1)
	_ = s.Put(ctx, r2)
	_ = s.Put(ctx, r3)

	got, _ := s.List(ctx, executor.StatusDone)
	if len(got) != 1 || got[0].TaskID != "t1" {
		t.Errorf("done filter = %+v", got)
	}
	got, _ = s.List(ctx, executor.StatusFailed)
	if len(got) != 1 || got[0].TaskID != "t2" {
		t.Errorf("failed filter = %+v", got)
	}
	got, _ = s.List(ctx, "")
	if len(got) != 3 {
		t.Errorf("all len = %d, want 3", len(got))
	}
}

func TestMemoryStore_Delete(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	_ = s.Put(ctx, newRunningRecord("t", "r", time.Now()))
	if err := s.Delete(ctx, "t"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "t"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("post-delete Get err = %v", err)
	}
}

func TestMemoryStore_DeleteUnknown(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Delete(context.Background(), "ghost"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("err = %v, want ErrTaskNotFound", err)
	}
}

func TestMemoryStore_GetReturnsDeepCopy(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	rec := newRunningRecord("t", "r", time.Now())
	rec.Mismatches = []executor.Mismatch{{Key: "k", Reason: executor.MismatchSizeDiffer}}
	_ = s.Put(ctx, rec)

	got1, _ := s.Get(ctx, "t")
	got1.RuleID = "MUTATED"
	got1.Mismatches[0].Key = "MUTATED"

	got2, _ := s.Get(ctx, "t")
	if got2.RuleID != "r" {
		t.Errorf("RuleID mutated through Get: %q", got2.RuleID)
	}
	if got2.Mismatches[0].Key != "k" {
		t.Errorf("Mismatches mutated through Get: %q", got2.Mismatches[0].Key)
	}
}

func TestMemoryStore_ConcurrentPutsAreSafe(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "t" + itoa(i)
			_ = s.Put(ctx, newRunningRecord(id, "r", time.Now()))
		}(i)
	}
	wg.Wait()
	got, _ := s.List(ctx, "")
	if len(got) != 50 {
		t.Errorf("len = %d, want 50", len(got))
	}
}

func TestMemoryStore_CloseIsNoop(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	// double-close also safe
	if err := s.Close(); err != nil {
		t.Errorf("double Close = %v", err)
	}
}

// itoa is a tiny dep-free integer formatter for test ids.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 4)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
