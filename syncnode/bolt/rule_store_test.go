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

	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/spec"
)

// Static interface assertion: ruleStore must implement rules.Store.
var _ rules.Store = (*ruleStore)(nil)

func newRule(id string) *rules.Rule {
	return rules.NewRule(spec.RuleConfig{
		ID:       id,
		Type:     "sync",
		Schedule: "@daily",
	})
}

func TestRuleStore_CreateGet(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	ctx := context.Background()

	r := newRule("r1")
	if err := s.Create(ctx, r); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Get(ctx, "r1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Config.ID != "r1" || got.State != rules.StateActive {
		t.Errorf("got = %+v", got)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Error("timestamps must be populated")
	}
}

func TestRuleStore_CreateDuplicate(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	ctx := context.Background()

	if err := s.Create(ctx, newRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	err := s.Create(ctx, newRule("r1"))
	if !errors.Is(err, rules.ErrRuleExists) {
		t.Errorf("duplicate Create err = %v, want ErrRuleExists", err)
	}
}

func TestRuleStore_CreateNilOrEmpty(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	ctx := context.Background()

	if err := s.Create(ctx, nil); !errors.Is(err, rules.ErrInvalidState) {
		t.Errorf("nil Create err = %v", err)
	}
	if err := s.Create(ctx, &rules.Rule{}); !errors.Is(err, rules.ErrInvalidState) {
		t.Errorf("empty-id Create err = %v", err)
	}
}

func TestRuleStore_UpdateUnknown(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	err := s.Update(context.Background(), newRule("ghost"))
	if !errors.Is(err, rules.ErrRuleNotFound) {
		t.Errorf("Update unknown err = %v, want ErrRuleNotFound", err)
	}
}

func TestRuleStore_UpdatePreservesCreatedAtAndRuntime(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	ctx := context.Background()

	r := newRule("r1")
	r.CreatedAt = time.Now().Add(-24 * time.Hour)
	if err := s.Create(ctx, r); err != nil {
		t.Fatalf("Create: %v", err)
	}
	orig, _ := s.Get(ctx, "r1")
	// Set state + last-run via dedicated methods so we have something
	// to preserve.
	if err := s.SetState(ctx, "r1", rules.StatePaused); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	if err := s.UpdateLastRun(ctx, "r1", rules.LastRunSummary{
		At:     time.Now(),
		Status: "done",
	}); err != nil {
		t.Fatalf("UpdateLastRun: %v", err)
	}

	upd := newRule("r1")
	// Intentionally leave State zero and LastRunAt zero so Update
	// must preserve them.
	upd.State = ""
	upd.Config.Schedule = "@hourly"
	if err := s.Update(ctx, upd); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ := s.Get(ctx, "r1")
	if !got.CreatedAt.Equal(orig.CreatedAt) {
		t.Errorf("CreatedAt drifted: %v -> %v", orig.CreatedAt, got.CreatedAt)
	}
	if got.State != rules.StatePaused {
		t.Errorf("State not preserved: %v", got.State)
	}
	if got.LastRunStatus != "done" {
		t.Errorf("LastRunStatus not preserved: %v", got.LastRunStatus)
	}
	if got.Config.Schedule != "@hourly" {
		t.Errorf("Config not updated: %s", got.Config.Schedule)
	}
}

func TestRuleStore_SetStateUnknown(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	err := s.SetState(context.Background(), "ghost", rules.StatePaused)
	if !errors.Is(err, rules.ErrRuleNotFound) {
		t.Errorf("err = %v, want ErrRuleNotFound", err)
	}
}

func TestRuleStore_UpdateLastRunUnknown(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	err := s.UpdateLastRun(context.Background(), "ghost", rules.LastRunSummary{})
	if !errors.Is(err, rules.ErrRuleNotFound) {
		t.Errorf("err = %v, want ErrRuleNotFound", err)
	}
}

func TestRuleStore_Delete(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	ctx := context.Background()

	if err := s.Create(ctx, newRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Delete(ctx, "r1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "r1"); !errors.Is(err, rules.ErrRuleNotFound) {
		t.Errorf("after Delete, Get err = %v", err)
	}
	// Idempotent? — spec says ErrRuleNotFound, matching memory store.
	if err := s.Delete(ctx, "r1"); !errors.Is(err, rules.ErrRuleNotFound) {
		t.Errorf("second Delete err = %v", err)
	}
}

func TestRuleStore_GetUnknown(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	_, err := s.Get(context.Background(), "ghost")
	if !errors.Is(err, rules.ErrRuleNotFound) {
		t.Errorf("err = %v, want ErrRuleNotFound", err)
	}
}

func TestRuleStore_ListSorted(t *testing.T) {
	db := newTestDB(t)
	s := db.RuleStore()
	ctx := context.Background()

	for _, id := range []string{"r3", "r1", "r2"} {
		if err := s.Create(ctx, newRule(id)); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}
	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d", len(got))
	}
	want := []string{"r1", "r2", "r3"}
	for i, r := range got {
		if r.Config.ID != want[i] {
			t.Errorf("[%d] id = %s, want %s", i, r.Config.ID, want[i])
		}
	}
}

func TestRuleStore_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	db1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db1.RuleStore().Create(context.Background(), newRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	got, err := db2.RuleStore().Get(context.Background(), "r1")
	if err != nil {
		t.Fatalf("post-reopen Get: %v", err)
	}
	if got.Config.ID != "r1" {
		t.Errorf("got = %+v", got)
	}
}

func TestRuleStore_CloseNoop(t *testing.T) {
	db := newTestDB(t)
	if err := db.RuleStore().Close(); err != nil {
		t.Errorf("RuleStore Close: %v", err)
	}
}
