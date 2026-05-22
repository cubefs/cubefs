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

package executor

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"
)

// newTestSoakStore opens a fresh bbolt DB under t.TempDir() and returns a
// SoakStore plus a cleanup hook closing the DB. Centralised so every
// soak test gets identical setup semantics (one bucket, isolated file).
func newTestSoakStore(t *testing.T) (SoakStore, *bbolt.DB) {
	t.Helper()
	dir := t.TempDir()
	db, err := bbolt.Open(filepath.Join(dir, "soak.db"), 0o644, &bbolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("bbolt open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewBoltSoakStore(db)
	if err != nil {
		t.Fatalf("NewBoltSoakStore: %v", err)
	}
	return store, db
}

func TestSoakStore_SaveLoadRoundTrip(t *testing.T) {
	store, _ := newTestSoakStore(t)
	ctx := context.Background()

	want := SoakCheckpoint{
		TaskID:         "task-1",
		Stage:          "stage-a",
		ShardID:        2,
		ElapsedSec:     120,
		OpsCompleted:   5000,
		LastUpdateUnix: 1_700_000_000,
		RestartCount:   1,
		Snapshot:       []byte{0xde, 0xad, 0xbe, 0xef},
	}
	if err := store.Save(ctx, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := store.Load(ctx, "task-1", "stage-a", 2)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got == nil {
		t.Fatalf("Load returned nil for existing key")
	}
	if got.TaskID != want.TaskID || got.Stage != want.Stage || got.ShardID != want.ShardID {
		t.Errorf("identity mismatch: got %+v want %+v", got, want)
	}
	if got.ElapsedSec != want.ElapsedSec || got.OpsCompleted != want.OpsCompleted {
		t.Errorf("counters mismatch: got %+v want %+v", got, want)
	}
	if got.RestartCount != want.RestartCount {
		t.Errorf("RestartCount: got %d want %d", got.RestartCount, want.RestartCount)
	}
	if !bytes.Equal(got.Snapshot, want.Snapshot) {
		t.Errorf("Snapshot bytes mismatch: got %x want %x", got.Snapshot, want.Snapshot)
	}
}

func TestSoakStore_LoadMissingReturnsNilNil(t *testing.T) {
	store, _ := newTestSoakStore(t)
	got, err := store.Load(context.Background(), "no-such-task", "no-stage", 0)
	if err != nil {
		t.Fatalf("Load on missing key: unexpected err %v", err)
	}
	if got != nil {
		t.Fatalf("Load on missing key: expected nil, got %+v", got)
	}
}

func TestSoakStore_Delete(t *testing.T) {
	store, _ := newTestSoakStore(t)
	ctx := context.Background()
	cp := SoakCheckpoint{TaskID: "t", Stage: "s", ShardID: 0, ElapsedSec: 10}
	if err := store.Save(ctx, cp); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Delete(ctx, "t", "s", 0); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := store.Load(ctx, "t", "s", 0)
	if err != nil {
		t.Fatalf("post-Delete Load: %v", err)
	}
	if got != nil {
		t.Fatalf("post-Delete Load: expected nil, got %+v", got)
	}
	// Idempotency: deleting again is a no-op (bbolt Bucket.Delete is
	// already idempotent; just sanity-check we don't surface an error).
	if err := store.Delete(ctx, "t", "s", 0); err != nil {
		t.Fatalf("idempotent Delete: %v", err)
	}
}

func TestSoakStore_ListByTask(t *testing.T) {
	store, _ := newTestSoakStore(t)
	ctx := context.Background()

	// Two tasks, multiple stages × shards each.
	saves := []SoakCheckpoint{
		{TaskID: "alpha", Stage: "write", ShardID: 0, ElapsedSec: 5},
		{TaskID: "alpha", Stage: "write", ShardID: 1, ElapsedSec: 6},
		{TaskID: "alpha", Stage: "read", ShardID: 0, ElapsedSec: 7},
		{TaskID: "beta", Stage: "write", ShardID: 0, ElapsedSec: 99},
	}
	for _, cp := range saves {
		if err := store.Save(ctx, cp); err != nil {
			t.Fatalf("Save %v: %v", cp, err)
		}
	}

	got, err := store.ListByTask(ctx, "alpha")
	if err != nil {
		t.Fatalf("ListByTask alpha: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ListByTask alpha: got %d entries want 3 (%+v)", len(got), got)
	}
	// All returned entries belong to "alpha"
	for _, cp := range got {
		if cp.TaskID != "alpha" {
			t.Errorf("ListByTask alpha returned foreign task %q", cp.TaskID)
		}
	}

	// Beta has exactly one
	got, err = store.ListByTask(ctx, "beta")
	if err != nil {
		t.Fatalf("ListByTask beta: %v", err)
	}
	if len(got) != 1 || got[0].ElapsedSec != 99 {
		t.Fatalf("ListByTask beta unexpected: %+v", got)
	}

	// Unknown task → empty slice, not error.
	got, err = store.ListByTask(ctx, "ghost")
	if err != nil {
		t.Fatalf("ListByTask ghost: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ListByTask ghost: expected empty, got %+v", got)
	}
}

func TestSoakStore_SaveOverwrite(t *testing.T) {
	store, _ := newTestSoakStore(t)
	ctx := context.Background()
	if err := store.Save(ctx, SoakCheckpoint{TaskID: "t", Stage: "s", ShardID: 0, ElapsedSec: 10}); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if err := store.Save(ctx, SoakCheckpoint{TaskID: "t", Stage: "s", ShardID: 0, ElapsedSec: 99}); err != nil {
		t.Fatalf("overwrite Save: %v", err)
	}
	got, err := store.Load(ctx, "t", "s", 0)
	if err != nil || got == nil {
		t.Fatalf("Load post-overwrite: got=%+v err=%v", got, err)
	}
	if got.ElapsedSec != 99 {
		t.Errorf("overwrite did not persist: ElapsedSec=%d want 99", got.ElapsedSec)
	}
}

func TestSoakStore_RejectEmptyKey(t *testing.T) {
	store, _ := newTestSoakStore(t)
	if err := store.Save(context.Background(), SoakCheckpoint{Stage: "s"}); err == nil {
		t.Error("Save with empty TaskID: expected error, got nil")
	}
	if err := store.Save(context.Background(), SoakCheckpoint{TaskID: "t"}); err == nil {
		t.Error("Save with empty Stage: expected error, got nil")
	}
}

func TestSoakStore_NilDB(t *testing.T) {
	if _, err := NewBoltSoakStore(nil); err == nil {
		t.Error("NewBoltSoakStore(nil): expected error, got nil")
	}
}
