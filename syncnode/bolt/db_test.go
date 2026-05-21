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
	"os"
	"path/filepath"
	"testing"
	"time"
)

// newTestDB opens a fresh BoltDB inside t.TempDir() and registers a
// Close cleanup. Returns the *DB so each test can drive the API.
func newTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpen_HappyPath(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if db.Path() != filepath.Join(dir, dbFileName) {
		t.Errorf("Path() = %s, want suffix %s", db.Path(), dbFileName)
	}

	if _, err := os.Stat(db.Path()); err != nil {
		t.Errorf("DB file should exist on disk: %v", err)
	}
}

func TestOpen_EmptyPathFails(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("Open with empty path must fail")
	}
}

func TestOpen_CreatesParentDir(t *testing.T) {
	root := t.TempDir()
	// Use a sub-sub-dir that doesn't exist yet — Open should mkdir -p it.
	nested := filepath.Join(root, "data", "syncnode")
	db, err := Open(nested)
	if err != nil {
		t.Fatalf("Open with nested dir: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := os.Stat(nested); err != nil {
		t.Errorf("parent dir not created: %v", err)
	}
}

func TestHealth_FreshDB(t *testing.T) {
	db := newTestDB(t)
	if err := db.Health(); err != nil {
		t.Errorf("Health on fresh DB: %v", err)
	}
}

func TestHealth_AfterClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = db.Close()
	if err := db.Health(); err == nil {
		t.Error("Health on closed DB should fail")
	}
}

func TestClose_Idempotent(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("second Close should be a no-op, got: %v", err)
	}
}

func TestOpen_SecondOpenFlockTimesOut(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir, WithFlockTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := Open(dir, WithFlockTimeout(100*time.Millisecond))
	if err == nil {
		_ = second.Close()
		t.Fatal("second Open while first held the flock should fail")
	}
}

func TestDB_AllBucketsInitialised(t *testing.T) {
	db := newTestDB(t)
	// Verifying via Health is the contract; deepen the check by
	// touching each Store accessor — they panic-free indicates the
	// buckets are reachable.
	if rs := db.RuleStore(); rs == nil {
		t.Error("RuleStore() returned nil")
	}
	if ts := db.TaskStore(); ts == nil {
		t.Error("TaskStore() returned nil")
	}
	if ip := db.InProgress(); ip == nil {
		t.Error("InProgress() returned nil")
	}
}

func TestNilDB_Methods(t *testing.T) {
	var d *DB
	if err := d.Close(); err != nil {
		t.Errorf("Close on nil DB: %v", err)
	}
	if err := d.Health(); err == nil {
		t.Error("Health on nil DB should fail")
	}
	if _, err := d.Recover(context.Background()); err == nil {
		t.Error("Recover on nil DB should fail")
	}
	if _, err := d.OrphanBreakpoints(context.Background()); err == nil {
		t.Error("OrphanBreakpoints on nil DB should fail")
	}
}
