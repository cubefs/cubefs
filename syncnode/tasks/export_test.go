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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
)

func TestWriteHistoryJSONL_Empty(t *testing.T) {
	s := NewMemoryStore()
	var buf bytes.Buffer
	if err := WriteHistoryJSONL(context.Background(), s, &buf, time.Time{}); err != nil {
		t.Fatalf("WriteHistoryJSONL: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("body = %q, want empty", buf.String())
	}
}

func TestWriteHistoryJSONL_ThreeRecords(t *testing.T) {
	s := NewMemoryStore()
	now := fixedNow()
	// Insert in mixed order; ListHistory sorts by DoneAt desc so the
	// stream order is deterministic.
	for i, age := range []time.Duration{2 * time.Hour, 1 * time.Hour, 3 * time.Hour} {
		id := []string{"a", "b", "c"}[i]
		seedHistoryRecord(t, s, id, now.Add(-age))
	}

	var buf bytes.Buffer
	if err := WriteHistoryJSONL(context.Background(), s, &buf, time.Time{}); err != nil {
		t.Fatalf("WriteHistoryJSONL: %v", err)
	}
	lines := splitLines(buf.String())
	if len(lines) != 3 {
		t.Fatalf("lines = %d, want 3 (body=%q)", len(lines), buf.String())
	}
	// Each line should decode to a Record cleanly.
	ids := make([]string, 0, 3)
	for _, line := range lines {
		var r Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("line decode: %v line=%q", err, line)
		}
		ids = append(ids, r.TaskID)
	}
	// Sorted desc by DoneAt: b (1h) > a (2h) > c (3h).
	want := []string{"b", "a", "c"}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("ids[%d] = %q, want %q (full=%v)", i, ids[i], want[i], ids)
		}
	}
}

func TestWriteHistoryJSONL_SinceFilter(t *testing.T) {
	s := NewMemoryStore()
	now := fixedNow()
	seedHistoryRecord(t, s, "old", now.Add(-10*time.Hour))
	seedHistoryRecord(t, s, "new", now.Add(-1*time.Hour))

	var buf bytes.Buffer
	since := now.Add(-5 * time.Hour)
	if err := WriteHistoryJSONL(context.Background(), s, &buf, since); err != nil {
		t.Fatalf("WriteHistoryJSONL: %v", err)
	}
	lines := splitLines(buf.String())
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	var r Record
	if err := json.Unmarshal([]byte(lines[0]), &r); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if r.TaskID != "new" {
		t.Errorf("TaskID = %q, want new", r.TaskID)
	}
}

func TestWriteHistoryJSONL_CtxCancelledMidStream(t *testing.T) {
	// Seed two records; cancel the context BEFORE writing so the very
	// first ctx.Err() check returns the cancellation error.
	s := NewMemoryStore()
	now := fixedNow()
	seedHistoryRecord(t, s, "a", now.Add(-1*time.Hour))
	seedHistoryRecord(t, s, "b", now.Add(-2*time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	err := WriteHistoryJSONL(ctx, s, &buf, time.Time{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestWriteHistoryJSONL_ListHistoryError(t *testing.T) {
	// A store that errors on ListHistory must surface that error directly.
	s := &errListHistoryStore{base: NewMemoryStore()}
	var buf bytes.Buffer
	err := WriteHistoryJSONL(context.Background(), s, &buf, time.Time{})
	if err == nil {
		t.Fatalf("expected error from failing ListHistory")
	}
}

func TestWriteHistoryJSONL_WriterError(t *testing.T) {
	// A writer that errors mid-stream surfaces the error.
	s := NewMemoryStore()
	now := fixedNow()
	seedHistoryRecord(t, s, "a", now.Add(-1*time.Hour))

	w := &alwaysErrWriter{}
	err := WriteHistoryJSONL(context.Background(), s, w, time.Time{})
	if err == nil {
		t.Fatalf("expected writer error to surface")
	}
}

// splitLines is a small helper that splits on "\n" and drops the trailing
// empty token produced by Encoder.Encode's trailing newline.
func splitLines(body string) []string {
	if body == "" {
		return nil
	}
	body = strings.TrimRight(body, "\n")
	if body == "" {
		return nil
	}
	return strings.Split(body, "\n")
}

// errListHistoryStore returns an error from ListHistory only.
type errListHistoryStore struct {
	base Store
}

func (e *errListHistoryStore) Put(ctx context.Context, r *Record) error { return e.base.Put(ctx, r) }
func (e *errListHistoryStore) Get(ctx context.Context, id string) (*Record, error) {
	return e.base.Get(ctx, id)
}
func (e *errListHistoryStore) List(ctx context.Context, s executor.Status) ([]*Record, error) {
	return e.base.List(ctx, s)
}
func (e *errListHistoryStore) Delete(ctx context.Context, id string) error {
	return e.base.Delete(ctx, id)
}
func (e *errListHistoryStore) MoveToHistory(ctx context.Context, id string) error {
	return e.base.MoveToHistory(ctx, id)
}
func (e *errListHistoryStore) ListHistory(ctx context.Context, since time.Time) ([]*Record, error) {
	return nil, errors.New("listhistory boom")
}
func (e *errListHistoryStore) PurgeHistoryBefore(ctx context.Context, c time.Time) (int, error) {
	return e.base.PurgeHistoryBefore(ctx, c)
}
func (e *errListHistoryStore) Close() error { return e.base.Close() }

// alwaysErrWriter implements io.Writer with a permanent failure mode so
// json.Encoder.Encode bubbles the error back to WriteHistoryJSONL.
type alwaysErrWriter struct{}

func (alwaysErrWriter) Write(p []byte) (int, error) {
	return 0, errors.New("writer broken")
}
