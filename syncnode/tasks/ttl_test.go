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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
)

// seedRecord inserts a record straight into the active compartment with
// the supplied status + DoneAt. Bypasses the Runner; the TTL tests don't
// care how the record got there, only about its TTL anchors.
func seedRecord(t *testing.T, s *memoryStore, id string, status executor.Status, doneAt time.Time) {
	t.Helper()
	rec := &Record{
		TaskID:    id,
		Status:    status,
		StartedAt: doneAt.Add(-1 * time.Hour),
		DoneAt:    doneAt,
	}
	if err := s.Put(context.Background(), rec); err != nil {
		t.Fatalf("Put(%s): %v", id, err)
	}
}

// seedHistoryRecord pushes a record straight into the history compartment.
// Used by purge tests that want a pre-aged history without going through
// the active → history transition.
func seedHistoryRecord(t *testing.T, s *memoryStore, id string, doneAt time.Time) {
	t.Helper()
	// Insert into active first then move — we go through the public API
	// to keep this test honest about the contract.
	seedRecord(t, s, id, executor.StatusDone, doneAt)
	if err := s.MoveToHistory(context.Background(), id); err != nil {
		t.Fatalf("MoveToHistory(%s): %v", id, err)
	}
}

// fixedNow returns a deterministic time so all the table tests are
// referenced from the same anchor.
func fixedNow() time.Time {
	return time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
}

func newTestTTL(s Store, cfg TTLConfig, now time.Time) *TTLRunner {
	clk := newFakeClock(now)
	return NewTTLRunner(s, WithTTLConfig(cfg), WithClock(clk.Now))
}

func TestTTL_SweepOnce_EmptyStore(t *testing.T) {
	s := NewMemoryStore()
	r := newTestTTL(s, DefaultTTLConfig(), fixedNow())
	moved, purged, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if moved != 0 || purged != 0 {
		t.Errorf("moved=%d purged=%d, want 0 0", moved, purged)
	}
}

func TestTTL_SweepOnce_MovesOldTerminal(t *testing.T) {
	s := NewMemoryStore()
	now := fixedNow()
	cfg := TTLConfig{ActiveAge: 1 * time.Hour, HistoryRetention: 24 * time.Hour, SweepInterval: time.Hour}
	r := newTestTTL(s, cfg, now)

	// DoneAt = now - 2h, beyond the 1h cutoff.
	seedRecord(t, s, "old", executor.StatusDone, now.Add(-2*time.Hour))

	moved, purged, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if moved != 1 {
		t.Errorf("moved = %d, want 1", moved)
	}
	if purged != 0 {
		t.Errorf("purged = %d, want 0", purged)
	}
	if _, err := s.Get(context.Background(), "old"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("active.Get after move: err=%v, want ErrTaskNotFound", err)
	}
	hist, err := s.ListHistory(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	if len(hist) != 1 || hist[0].TaskID != "old" {
		t.Errorf("history = %+v, want [old]", hist)
	}
}

func TestTTL_SweepOnce_KeepsYoungTerminal(t *testing.T) {
	s := NewMemoryStore()
	now := fixedNow()
	cfg := TTLConfig{ActiveAge: 1 * time.Hour, HistoryRetention: 24 * time.Hour, SweepInterval: time.Hour}
	r := newTestTTL(s, cfg, now)

	// DoneAt = now - 30m, under the 1h cutoff → should stay.
	seedRecord(t, s, "young", executor.StatusDone, now.Add(-30*time.Minute))

	moved, _, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if moved != 0 {
		t.Errorf("moved = %d, want 0", moved)
	}
	if _, err := s.Get(context.Background(), "young"); err != nil {
		t.Errorf("young record vanished: %v", err)
	}
}

func TestTTL_SweepOnce_SkipsRunning(t *testing.T) {
	s := NewMemoryStore()
	now := fixedNow()
	cfg := TTLConfig{ActiveAge: 1 * time.Hour, HistoryRetention: 24 * time.Hour, SweepInterval: time.Hour}
	r := newTestTTL(s, cfg, now)

	// Running task with no DoneAt — not terminal, must not move.
	rec := &Record{TaskID: "running", Status: executor.StatusRunning, StartedAt: now.Add(-3 * time.Hour)}
	if err := s.Put(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	moved, _, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if moved != 0 {
		t.Errorf("moved = %d, want 0 (running)", moved)
	}
	if _, err := s.Get(context.Background(), "running"); err != nil {
		t.Errorf("running record vanished: %v", err)
	}
}

func TestTTL_SweepOnce_SkipsTerminalWithZeroDoneAt(t *testing.T) {
	// Defensive: a record with terminal status but DoneAt == zero is
	// malformed; TTL must NOT move it (we'd lose the audit anchor).
	s := NewMemoryStore()
	now := fixedNow()
	cfg := TTLConfig{ActiveAge: 1 * time.Hour, HistoryRetention: 24 * time.Hour, SweepInterval: time.Hour}
	r := newTestTTL(s, cfg, now)

	rec := &Record{TaskID: "weird", Status: executor.StatusDone, StartedAt: now.Add(-3 * time.Hour)}
	if err := s.Put(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	moved, _, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if moved != 0 {
		t.Errorf("moved = %d, want 0 (zero DoneAt)", moved)
	}
}

func TestTTL_SweepOnce_PurgesOldHistory(t *testing.T) {
	s := NewMemoryStore()
	now := fixedNow()
	cfg := TTLConfig{ActiveAge: 1 * time.Hour, HistoryRetention: 24 * time.Hour, SweepInterval: time.Hour}
	r := newTestTTL(s, cfg, now)

	// Pre-aged record: DoneAt = now - 25h, beyond the 24h retention.
	seedHistoryRecord(t, s, "ancient", now.Add(-25*time.Hour))
	// Fresh history record: still within retention.
	seedHistoryRecord(t, s, "recent", now.Add(-1*time.Hour))

	moved, purged, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if moved != 0 {
		t.Errorf("moved = %d, want 0", moved)
	}
	if purged != 1 {
		t.Errorf("purged = %d, want 1", purged)
	}
	hist, _ := s.ListHistory(context.Background(), time.Time{})
	if len(hist) != 1 || hist[0].TaskID != "recent" {
		t.Errorf("history after purge = %+v, want [recent]", hist)
	}
}

func TestTTL_SweepOnce_Combined(t *testing.T) {
	s := NewMemoryStore()
	now := fixedNow()
	cfg := TTLConfig{ActiveAge: 1 * time.Hour, HistoryRetention: 24 * time.Hour, SweepInterval: time.Hour}
	r := newTestTTL(s, cfg, now)

	// Active terminal — eligible to move.
	seedRecord(t, s, "active-old-done", executor.StatusDone, now.Add(-2*time.Hour))
	seedRecord(t, s, "active-old-failed", executor.StatusFailed, now.Add(-3*time.Hour))
	// Active terminal but young — must NOT move.
	seedRecord(t, s, "active-young", executor.StatusDone, now.Add(-30*time.Minute))
	// Pre-aged history — must purge.
	seedHistoryRecord(t, s, "hist-old", now.Add(-100*time.Hour))
	// Fresh history — keep.
	seedHistoryRecord(t, s, "hist-fresh", now.Add(-2*time.Hour))

	moved, purged, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if moved != 2 {
		t.Errorf("moved = %d, want 2", moved)
	}
	if purged != 1 {
		t.Errorf("purged = %d, want 1", purged)
	}
	// Active compartment must still contain only the young record.
	list, _ := s.List(context.Background(), "")
	if len(list) != 1 || list[0].TaskID != "active-young" {
		t.Errorf("active list = %+v, want [active-young]", list)
	}
	// History should contain three: hist-fresh + 2 newly moved.
	hist, _ := s.ListHistory(context.Background(), time.Time{})
	if len(hist) != 3 {
		t.Errorf("history len = %d, want 3", len(hist))
	}
}

func TestTTL_StartStop(t *testing.T) {
	s := NewMemoryStore()
	now := fixedNow()
	clk := newFakeClock(now)
	cfg := TTLConfig{ActiveAge: 1 * time.Hour, HistoryRetention: 24 * time.Hour, SweepInterval: 5 * time.Millisecond}
	r := NewTTLRunner(s, WithTTLConfig(cfg), WithClock(clk.Now))

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Idempotent second Start.
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start (second): %v", err)
	}
	if err := r.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Idempotent second Stop.
	if err := r.Stop(); err != nil {
		t.Fatalf("Stop (second): %v", err)
	}
}

// observingStore counts SweepOnce-driven calls so the periodic-loop test
// can assert "the goroutine actually ticks".
type observingStore struct {
	*memoryStore
	listCalls   atomic.Int32
	purgeCalls  atomic.Int32
	purgeReturn func(time.Time) (int, error)
}

func (o *observingStore) List(ctx context.Context, status executor.Status) ([]*Record, error) {
	o.listCalls.Add(1)
	return o.memoryStore.List(ctx, status)
}

func (o *observingStore) PurgeHistoryBefore(ctx context.Context, cutoff time.Time) (int, error) {
	o.purgeCalls.Add(1)
	if o.purgeReturn != nil {
		return o.purgeReturn(cutoff)
	}
	return o.memoryStore.PurgeHistoryBefore(ctx, cutoff)
}

func TestTTL_LoopFiresOnInterval(t *testing.T) {
	o := &observingStore{memoryStore: NewMemoryStore()}
	cfg := TTLConfig{ActiveAge: time.Second, HistoryRetention: time.Second, SweepInterval: 10 * time.Millisecond}
	r := NewTTLRunner(o, WithTTLConfig(cfg))

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Sleep enough wall-clock for several ticks. The first sweep fires
	// immediately + N more on the ticker. Allow some slack — we only need
	// to see "more than 1" to confirm the goroutine isn't stuck.
	time.Sleep(80 * time.Millisecond)
	_ = r.Stop()

	if got := o.listCalls.Load(); got < 3 {
		t.Errorf("listCalls = %d, want >= 3 (loop did not tick)", got)
	}
	if got := o.purgeCalls.Load(); got < 3 {
		t.Errorf("purgeCalls = %d, want >= 3", got)
	}
}

func TestTTL_SweepOnce_ListError(t *testing.T) {
	// A failing List bubbles up through SweepOnce, halting the sweep
	// before the purge step.
	o := &failingTTLStore{base: NewMemoryStore(), listErr: errors.New("boom")}
	r := newTestTTL(o, DefaultTTLConfig(), fixedNow())
	_, _, err := r.SweepOnce(context.Background())
	if err == nil {
		t.Fatalf("SweepOnce should surface List error")
	}
}

func TestTTL_SweepOnce_PurgeError(t *testing.T) {
	o := &failingTTLStore{base: NewMemoryStore(), purgeErr: errors.New("boom")}
	r := newTestTTL(o, DefaultTTLConfig(), fixedNow())
	_, _, err := r.SweepOnce(context.Background())
	if err == nil {
		t.Fatalf("SweepOnce should surface Purge error")
	}
}

func TestTTL_SweepOnce_MoveErrorSkipped(t *testing.T) {
	// MoveToHistory failures for individual records must NOT abort the
	// sweep — the count for the successful move + purge still advances.
	s := NewMemoryStore()
	now := fixedNow()
	cfg := TTLConfig{ActiveAge: 1 * time.Hour, HistoryRetention: 24 * time.Hour, SweepInterval: time.Hour}
	// Wrap memoryStore so the first MoveToHistory call fails but the
	// second succeeds.
	moveStore := &moveErrStore{Store: s, failFirst: true}
	r := newTestTTL(moveStore, cfg, now)

	seedRecord(t, s, "a", executor.StatusDone, now.Add(-2*time.Hour))
	seedRecord(t, s, "b", executor.StatusFailed, now.Add(-3*time.Hour))

	moved, _, err := r.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	// We injected exactly one MoveToHistory failure; the other record
	// moved cleanly. The sweep must not bail out.
	if moved != 1 {
		t.Errorf("moved = %d, want 1 (one ok + one skipped)", moved)
	}
}

// failingTTLStore returns errors from List / PurgeHistoryBefore on demand.
type failingTTLStore struct {
	base     Store
	listErr  error
	purgeErr error
}

func (f *failingTTLStore) Put(ctx context.Context, r *Record) error { return f.base.Put(ctx, r) }
func (f *failingTTLStore) Get(ctx context.Context, id string) (*Record, error) {
	return f.base.Get(ctx, id)
}
func (f *failingTTLStore) List(ctx context.Context, s executor.Status) ([]*Record, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.base.List(ctx, s)
}
func (f *failingTTLStore) Delete(ctx context.Context, id string) error { return f.base.Delete(ctx, id) }
func (f *failingTTLStore) MoveToHistory(ctx context.Context, id string) error {
	return f.base.MoveToHistory(ctx, id)
}
func (f *failingTTLStore) ListHistory(ctx context.Context, since time.Time) ([]*Record, error) {
	return f.base.ListHistory(ctx, since)
}
func (f *failingTTLStore) PurgeHistoryBefore(ctx context.Context, c time.Time) (int, error) {
	if f.purgeErr != nil {
		return 0, f.purgeErr
	}
	return f.base.PurgeHistoryBefore(ctx, c)
}
func (f *failingTTLStore) Close() error { return f.base.Close() }

// moveErrStore wraps a Store and forces MoveToHistory to fail the first
// time it is called.
type moveErrStore struct {
	Store
	failFirst bool
	calls     int
}

func (m *moveErrStore) MoveToHistory(ctx context.Context, id string) error {
	m.calls++
	if m.failFirst && m.calls == 1 {
		return fmt.Errorf("simulated move failure")
	}
	return m.Store.MoveToHistory(ctx, id)
}
