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
)

// Static interface assertion: inProgressStore must satisfy InProgressStore.
var _ InProgressStore = (*inProgressStore)(nil)

func TestInProgress_PutGet(t *testing.T) {
	db := newTestDB(t)
	s := db.InProgress()
	ctx := context.Background()

	bp := &Breakpoint{
		TaskID:    "t1",
		Key:       "obj/path",
		BytesDone: 1024,
		UploadID:  "u-1",
	}
	if err := s.Put(ctx, bp); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.TaskID != "t1" || got.BytesDone != 1024 || got.UploadID != "u-1" {
		t.Errorf("got = %+v", got)
	}
	if got.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should be stamped on Put when caller leaves it zero")
	}
}

func TestInProgress_PutPreservesProvidedUpdatedAt(t *testing.T) {
	db := newTestDB(t)
	s := db.InProgress()
	ctx := context.Background()

	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	bp := &Breakpoint{TaskID: "t1", Key: "k", UpdatedAt: when}
	if err := s.Put(ctx, bp); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, _ := s.Get(ctx, "t1")
	if !got.UpdatedAt.Equal(when) {
		t.Errorf("UpdatedAt clobbered: got %v want %v", got.UpdatedAt, when)
	}
}

func TestInProgress_PutNilOrEmpty(t *testing.T) {
	db := newTestDB(t)
	s := db.InProgress()
	ctx := context.Background()
	if err := s.Put(ctx, nil); !errors.Is(err, ErrBreakpointNotFound) {
		t.Errorf("nil Put err = %v", err)
	}
	if err := s.Put(ctx, &Breakpoint{}); !errors.Is(err, ErrBreakpointNotFound) {
		t.Errorf("empty Put err = %v", err)
	}
}

func TestInProgress_GetUnknown(t *testing.T) {
	db := newTestDB(t)
	s := db.InProgress()
	_, err := s.Get(context.Background(), "ghost")
	if !errors.Is(err, ErrBreakpointNotFound) {
		t.Errorf("err = %v", err)
	}
}

func TestInProgress_Delete(t *testing.T) {
	db := newTestDB(t)
	s := db.InProgress()
	ctx := context.Background()

	_ = s.Put(ctx, &Breakpoint{TaskID: "t1", Key: "k"})
	if err := s.Delete(ctx, "t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, "t1"); !errors.Is(err, ErrBreakpointNotFound) {
		t.Errorf("second Delete err = %v", err)
	}
}

func TestInProgress_ListSortedByUpdatedDesc(t *testing.T) {
	db := newTestDB(t)
	s := db.InProgress()
	ctx := context.Background()

	now := time.Now()
	_ = s.Put(ctx, &Breakpoint{TaskID: "old", Key: "k1", UpdatedAt: now.Add(-time.Hour)})
	_ = s.Put(ctx, &Breakpoint{TaskID: "fresh", Key: "k2", UpdatedAt: now})
	_ = s.Put(ctx, &Breakpoint{TaskID: "mid", Key: "k3", UpdatedAt: now.Add(-30 * time.Minute)})

	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d", len(got))
	}
	want := []string{"fresh", "mid", "old"}
	for i, b := range got {
		if b.TaskID != want[i] {
			t.Errorf("[%d] %s, want %s", i, b.TaskID, want[i])
		}
	}
}

func TestInProgress_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	db1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	bp := &Breakpoint{TaskID: "t1", Key: "k", BytesDone: 42, UpdatedAt: time.Now()}
	if err := db1.InProgress().Put(context.Background(), bp); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	got, err := db2.InProgress().Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("post-reopen Get: %v", err)
	}
	if got.BytesDone != 42 {
		t.Errorf("got = %+v", got)
	}
}

func TestInProgress_CloseNoop(t *testing.T) {
	db := newTestDB(t)
	if err := db.InProgress().Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
