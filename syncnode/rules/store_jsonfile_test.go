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
	"os"
	"path/filepath"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
)

func TestJSONFileStore_SurvivesRestart(t *testing.T) {
	// The whole point of the E-2 acceptance criterion: rules created via
	// the store remain after process restart. Simulate restart by closing
	// and re-opening the store in the same temp dir.
	dir := t.TempDir()
	ctx := context.Background()

	first, err := NewJSONFileStore(dir)
	if err != nil {
		t.Fatalf("open #1: %v", err)
	}
	if err := first.Create(ctx, newTestRule("r-alpha")); err != nil {
		t.Fatalf("Create r-alpha: %v", err)
	}
	if err := first.Create(ctx, newTestRule("r-beta")); err != nil {
		t.Fatalf("Create r-beta: %v", err)
	}
	if err := first.SetState(ctx, "r-beta", StatePaused); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify the on-disk file actually exists at the expected path.
	if _, err := os.Stat(filepath.Join(dir, rulesFileName)); err != nil {
		t.Fatalf("rules.json not present: %v", err)
	}

	second, err := NewJSONFileStore(dir)
	if err != nil {
		t.Fatalf("open #2: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	out, err := second.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2 (got %+v)", len(out), out)
	}

	got, err := second.Get(ctx, "r-beta")
	if err != nil {
		t.Fatalf("Get r-beta: %v", err)
	}
	if got.State != StatePaused {
		t.Errorf("State = %q, want paused (state did not survive restart)", got.State)
	}
}

func TestJSONFileStore_AtomicWriteVisible(t *testing.T) {
	// After every Create the file must be readable as a valid JSON object,
	// confirming we never leave the file in a half-written state.
	dir := t.TempDir()
	ctx := context.Background()
	s, err := NewJSONFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	for i, id := range []string{"a", "b", "c"} {
		if err := s.Create(ctx, newTestRule(id)); err != nil {
			t.Fatalf("Create #%d: %v", i, err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, rulesFileName))
		if err != nil {
			t.Fatalf("read after #%d: %v", i, err)
		}
		if len(raw) == 0 {
			t.Errorf("file empty after #%d create", i)
		}
		// .tmp file must NOT linger.
		if _, err := os.Stat(filepath.Join(dir, rulesFileName+".tmp")); !os.IsNotExist(err) {
			t.Errorf(".tmp file should be removed after rename, got err=%v", err)
		}
	}
}

func TestJSONFileStore_LoadCorruptFile(t *testing.T) {
	// Corrupt JSON should NOT silently start the process with an empty store.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, rulesFileName), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("seed corrupt file: %v", err)
	}
	if _, err := NewJSONFileStore(dir); err == nil {
		t.Fatal("expected open to fail on corrupt JSON, got nil")
	}
}

func TestJSONFileStore_EmptyDirRequired(t *testing.T) {
	if _, err := NewJSONFileStore(""); err == nil {
		t.Fatal("expected error for empty dir")
	}
}

func TestJSONFileStore_LoadEmptyFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, rulesFileName), []byte{}, 0o644); err != nil {
		t.Fatalf("seed empty file: %v", err)
	}
	s, err := NewJSONFileStore(dir)
	if err != nil {
		t.Fatalf("open with empty file: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	out, _ := s.List(context.Background())
	if len(out) != 0 {
		t.Errorf("expected empty store, got %d entries", len(out))
	}
}

func TestJSONFileStore_DeleteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := NewJSONFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if err := s.Create(ctx, newTestRule("r")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Delete(ctx, "r"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, "r"); !errors.Is(err, ErrRuleNotFound) {
		t.Errorf("second Delete err = %v, want ErrRuleNotFound", err)
	}

	// Re-open and confirm the delete persisted.
	_ = s.Close()
	s2, err := NewJSONFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if _, err := s2.Get(ctx, "r"); !errors.Is(err, ErrRuleNotFound) {
		t.Errorf("after reopen, Get err = %v, want ErrRuleNotFound", err)
	}
}

func TestJSONFileStore_UpdateAndLastRunPersist(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := NewJSONFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := s.Create(ctx, newTestRule("r")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Update via the file-backed wrapper.
	if err := s.Update(ctx, &Rule{Config: spec.RuleConfig{ID: "r", Type: "load"}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.UpdateLastRun(ctx, "r", LastRunSummary{Status: "done"}); err != nil {
		t.Fatalf("UpdateLastRun: %v", err)
	}
	_ = s.Close()

	s2, err := NewJSONFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	got, err := s2.Get(ctx, "r")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Config.Type != "load" {
		t.Errorf("Type after restart = %q, want load", got.Config.Type)
	}
	if got.LastRunStatus != "done" {
		t.Errorf("LastRunStatus after restart = %q, want done", got.LastRunStatus)
	}
}
