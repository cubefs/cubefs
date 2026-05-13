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

package rules

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// newTestRule builds a Rule with the smallest viable RuleConfig for tests.
// Filter slices are populated to exercise the deep-copy path.
func newTestRule(id string) *Rule {
	return NewRule(spec.RuleConfig{
		ID:   id,
		Type: "sync",
		Filter: spec.FilterConfig{
			Include: []string{"*.log"},
			Exclude: []string{"*.tmp"},
		},
	})
}

func TestMemoryStore_Lifecycle(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })

	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			name: "list empty",
			fn: func(t *testing.T) {
				out, err := s.List(ctx)
				if err != nil {
					t.Fatalf("List err: %v", err)
				}
				if len(out) != 0 {
					t.Errorf("expected empty, got %d", len(out))
				}
			},
		},
		{
			name: "create then get",
			fn: func(t *testing.T) {
				r := newTestRule("rule-1")
				if err := s.Create(ctx, r); err != nil {
					t.Fatalf("Create: %v", err)
				}
				got, err := s.Get(ctx, "rule-1")
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if got.Config.ID != "rule-1" {
					t.Errorf("ID = %q, want rule-1", got.Config.ID)
				}
				if got.State != StateActive {
					t.Errorf("State = %q, want active", got.State)
				}
				if got.CreatedAt.IsZero() {
					t.Error("CreatedAt should be set")
				}
			},
		},
		{
			name: "duplicate create rejected",
			fn: func(t *testing.T) {
				err := s.Create(ctx, newTestRule("rule-1"))
				if !errors.Is(err, ErrRuleExists) {
					t.Errorf("expected ErrRuleExists, got %v", err)
				}
			},
		},
		{
			name: "list returns sorted",
			fn: func(t *testing.T) {
				if err := s.Create(ctx, newTestRule("rule-3")); err != nil {
					t.Fatalf("Create: %v", err)
				}
				if err := s.Create(ctx, newTestRule("rule-2")); err != nil {
					t.Fatalf("Create: %v", err)
				}
				out, err := s.List(ctx)
				if err != nil {
					t.Fatalf("List: %v", err)
				}
				if len(out) != 3 {
					t.Fatalf("len = %d, want 3", len(out))
				}
				want := []string{"rule-1", "rule-2", "rule-3"}
				for i, r := range out {
					if r.Config.ID != want[i] {
						t.Errorf("[%d] = %s, want %s", i, r.Config.ID, want[i])
					}
				}
			},
		},
		{
			name: "update preserves CreatedAt",
			fn: func(t *testing.T) {
				orig, _ := s.Get(ctx, "rule-1")
				time.Sleep(2 * time.Millisecond) // ensure UpdatedAt > CreatedAt
				updated := &Rule{Config: spec.RuleConfig{ID: "rule-1", Type: "load"}}
				if err := s.Update(ctx, updated); err != nil {
					t.Fatalf("Update: %v", err)
				}
				got, _ := s.Get(ctx, "rule-1")
				if !got.CreatedAt.Equal(orig.CreatedAt) {
					t.Errorf("CreatedAt changed: %v → %v", orig.CreatedAt, got.CreatedAt)
				}
				if got.Config.Type != "load" {
					t.Errorf("Type = %q, want load", got.Config.Type)
				}
				if !got.UpdatedAt.After(orig.UpdatedAt) {
					t.Errorf("UpdatedAt did not advance: orig=%v got=%v", orig.UpdatedAt, got.UpdatedAt)
				}
			},
		},
		{
			name: "update unknown id",
			fn: func(t *testing.T) {
				err := s.Update(ctx, newTestRule("nope"))
				if !errors.Is(err, ErrRuleNotFound) {
					t.Errorf("expected ErrRuleNotFound, got %v", err)
				}
			},
		},
		{
			name: "set state + update last run",
			fn: func(t *testing.T) {
				if err := s.SetState(ctx, "rule-1", StatePaused); err != nil {
					t.Fatalf("SetState: %v", err)
				}
				got, _ := s.Get(ctx, "rule-1")
				if got.State != StatePaused {
					t.Errorf("State = %q, want paused", got.State)
				}
				now := time.Now()
				if err := s.UpdateLastRun(ctx, "rule-1", LastRunSummary{
					At: now, Status: "done",
				}); err != nil {
					t.Fatalf("UpdateLastRun: %v", err)
				}
				got, _ = s.Get(ctx, "rule-1")
				if got.LastRunStatus != "done" {
					t.Errorf("LastRunStatus = %q, want done", got.LastRunStatus)
				}
				if !got.LastRunAt.Equal(now) {
					t.Errorf("LastRunAt = %v, want %v", got.LastRunAt, now)
				}
			},
		},
		{
			name: "delete + get",
			fn: func(t *testing.T) {
				if err := s.Delete(ctx, "rule-2"); err != nil {
					t.Fatalf("Delete: %v", err)
				}
				if _, err := s.Get(ctx, "rule-2"); !errors.Is(err, ErrRuleNotFound) {
					t.Errorf("expected ErrRuleNotFound, got %v", err)
				}
			},
		},
		{
			name: "delete unknown",
			fn: func(t *testing.T) {
				err := s.Delete(ctx, "missing")
				if !errors.Is(err, ErrRuleNotFound) {
					t.Errorf("expected ErrRuleNotFound, got %v", err)
				}
			},
		},
		{
			name: "set state unknown",
			fn: func(t *testing.T) {
				err := s.SetState(ctx, "missing", StatePaused)
				if !errors.Is(err, ErrRuleNotFound) {
					t.Errorf("expected ErrRuleNotFound, got %v", err)
				}
			},
		},
		{
			name: "update last run unknown",
			fn: func(t *testing.T) {
				err := s.UpdateLastRun(ctx, "missing", LastRunSummary{})
				if !errors.Is(err, ErrRuleNotFound) {
					t.Errorf("expected ErrRuleNotFound, got %v", err)
				}
			},
		},
		{
			name: "nil rule rejected",
			fn: func(t *testing.T) {
				if err := s.Create(ctx, nil); err == nil {
					t.Error("Create(nil) should error")
				}
				if err := s.Update(ctx, nil); err == nil {
					t.Error("Update(nil) should error")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.fn)
	}
}

func TestMemoryStore_DeepCopy(t *testing.T) {
	// Mutating a returned Rule must NOT corrupt the store.
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.Create(ctx, newTestRule("r")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, _ := s.Get(ctx, "r")
	got.Config.Type = "MUTATED"
	got.Config.Filter.Include[0] = "CORRUPTED"

	fresh, _ := s.Get(ctx, "r")
	if fresh.Config.Type != "sync" {
		t.Errorf("internal Type mutated: %q", fresh.Config.Type)
	}
	if fresh.Config.Filter.Include[0] != "*.log" {
		t.Errorf("internal Filter.Include mutated: %v", fresh.Config.Filter.Include)
	}
}

func TestMemoryStore_ConcurrentReadersWriters(t *testing.T) {
	// Exercise the mutex under -race. 50 goroutines hammer the store on
	// different keys for a short burst; we only check that nothing panics
	// and the final count is consistent.
	ctx := context.Background()
	s := NewMemoryStore()
	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := "rule-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
			_ = s.Create(ctx, newTestRule(id))
			_, _ = s.Get(ctx, id)
			_, _ = s.List(ctx)
			_ = s.SetState(ctx, id, StatePaused)
		}(i)
	}
	wg.Wait()
	out, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(out) == 0 {
		t.Error("expected non-empty store")
	}
}
