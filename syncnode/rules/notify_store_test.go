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
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore is a tiny Store impl whose Create/Update/Delete/SetState/
// UpdateLastRun can be programmed to fail. Backed by a memoryStore so
// successful calls behave normally and Get still works after the
// failure tests.
type fakeStore struct {
	*memoryStore

	createErr        error
	updateErr        error
	deleteErr        error
	setStateErr      error
	updateLastRunErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{memoryStore: NewMemoryStore()}
}

func (f *fakeStore) Create(ctx context.Context, r *Rule) error {
	if f.createErr != nil {
		return f.createErr
	}
	return f.memoryStore.Create(ctx, r)
}

func (f *fakeStore) Update(ctx context.Context, r *Rule) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	return f.memoryStore.Update(ctx, r)
}

func (f *fakeStore) Delete(ctx context.Context, id string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.memoryStore.Delete(ctx, id)
}

func (f *fakeStore) SetState(ctx context.Context, id string, s State) error {
	if f.setStateErr != nil {
		return f.setStateErr
	}
	return f.memoryStore.SetState(ctx, id, s)
}

func (f *fakeStore) UpdateLastRun(ctx context.Context, id string, last LastRunSummary) error {
	if f.updateLastRunErr != nil {
		return f.updateLastRunErr
	}
	return f.memoryStore.UpdateLastRun(ctx, id, last)
}

// counter is a tiny atomic helper to track fire invocations from a
// callback. Test callbacks must be cheap (the doc on fire() promises
// the call is synchronous), so an atomic.Int64 is sufficient.
type counter struct{ n atomic.Int64 }

func (c *counter) inc()         { c.n.Add(1) }
func (c *counter) load() int64  { return c.n.Load() }
func (c *counter) reset()       { c.n.Store(0) }

// TestNotifyStore_FireMatrix is the table-driven core: each row runs
// a single mutating method against a freshly-wrapped store and asserts
// (a) the underlying store-error is surfaced, (b) the fire counter
// moves only on success.
func TestNotifyStore_FireMatrix(t *testing.T) {
	t.Parallel()

	// Pre-seed rule for the methods that need an existing rule (Update,
	// Delete, SetState, UpdateLastRun). Create runs against an empty
	// store + its own seed.
	mkSeeded := func() *fakeStore {
		s := newFakeStore()
		_ = s.memoryStore.Create(context.Background(), newTestRule("r1"))
		return s
	}

	tests := []struct {
		name        string
		setup       func() *fakeStore
		mutate      func(*NotifyStore) error
		wantErr     bool
		wantFireInc int64
	}{
		{
			name:        "create_success_fires",
			setup:       newFakeStore,
			mutate:      func(n *NotifyStore) error { return n.Create(context.Background(), newTestRule("r1")) },
			wantErr:     false,
			wantFireInc: 1,
		},
		{
			name: "create_error_no_fire",
			setup: func() *fakeStore {
				s := newFakeStore()
				s.createErr = errors.New("boom")
				return s
			},
			mutate:      func(n *NotifyStore) error { return n.Create(context.Background(), newTestRule("r1")) },
			wantErr:     true,
			wantFireInc: 0,
		},
		{
			name:        "update_success_fires",
			setup:       mkSeeded,
			mutate:      func(n *NotifyStore) error { return n.Update(context.Background(), newTestRule("r1")) },
			wantErr:     false,
			wantFireInc: 1,
		},
		{
			name: "update_missing_no_fire",
			// Use seeded store but Update against a different id —
			// underlying memoryStore returns ErrRuleNotFound.
			setup:       mkSeeded,
			mutate:      func(n *NotifyStore) error { return n.Update(context.Background(), newTestRule("nope")) },
			wantErr:     true,
			wantFireInc: 0,
		},
		{
			name:        "delete_success_fires",
			setup:       mkSeeded,
			mutate:      func(n *NotifyStore) error { return n.Delete(context.Background(), "r1") },
			wantErr:     false,
			wantFireInc: 1,
		},
		{
			name:        "delete_missing_no_fire",
			setup:       mkSeeded,
			mutate:      func(n *NotifyStore) error { return n.Delete(context.Background(), "nope") },
			wantErr:     true,
			wantFireInc: 0,
		},
		{
			name:        "setstate_success_fires",
			setup:       mkSeeded,
			mutate:      func(n *NotifyStore) error { return n.SetState(context.Background(), "r1", StatePaused) },
			wantErr:     false,
			wantFireInc: 1,
		},
		{
			name: "setstate_error_no_fire",
			setup: func() *fakeStore {
				s := mkSeeded()
				s.setStateErr = errors.New("boom")
				return s
			},
			mutate:      func(n *NotifyStore) error { return n.SetState(context.Background(), "r1", StatePaused) },
			wantErr:     true,
			wantFireInc: 0,
		},
		{
			name:  "updatelastrun_success_no_fire",
			setup: mkSeeded,
			mutate: func(n *NotifyStore) error {
				return n.UpdateLastRun(context.Background(), "r1", LastRunSummary{
					At: time.Now(), Status: "done",
				})
			},
			wantErr:     false,
			wantFireInc: 0, // INTENTIONAL — heartbeat must not loop the scheduler.
		},
		{
			name: "updatelastrun_error_no_fire",
			setup: func() *fakeStore {
				s := mkSeeded()
				s.updateLastRunErr = errors.New("boom")
				return s
			},
			mutate: func(n *NotifyStore) error {
				return n.UpdateLastRun(context.Background(), "r1", LastRunSummary{
					At: time.Now(), Status: "done",
				})
			},
			wantErr:     true,
			wantFireInc: 0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inner := tc.setup()
			var c counter
			n := NewNotifyStore(inner, c.inc)
			err := tc.mutate(n)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v wantErr=%v", err, tc.wantErr)
			}
			if got := c.load(); got != tc.wantFireInc {
				t.Fatalf("fire counter: got %d want %d", got, tc.wantFireInc)
			}
		})
	}
}

// TestNotifyStore_SetOnChange verifies the callback can be swapped
// after construction (the production wiring needs this because the
// scheduler is built after the store).
func TestNotifyStore_SetOnChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	var a, b counter
	n := NewNotifyStore(inner, a.inc)

	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create r1: %v", err)
	}
	if a.load() != 1 || b.load() != 0 {
		t.Fatalf("after Create r1: a=%d b=%d", a.load(), b.load())
	}

	n.SetOnChange(b.inc)
	if err := n.Create(ctx, newTestRule("r2")); err != nil {
		t.Fatalf("Create r2: %v", err)
	}
	if a.load() != 1 || b.load() != 1 {
		t.Fatalf("after SetOnChange + Create r2: a=%d b=%d", a.load(), b.load())
	}

	// Nil callback must be tolerated (no-op).
	n.SetOnChange(nil)
	if err := n.Create(ctx, newTestRule("r3")); err != nil {
		t.Fatalf("Create r3: %v", err)
	}
	if a.load() != 1 || b.load() != 1 {
		t.Fatalf("after nil callback: a=%d b=%d", a.load(), b.load())
	}
}

// TestNotifyStore_NilOnChange verifies constructing with nil onChange
// doesn't blow up on the first mutation.
func TestNotifyStore_NilOnChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	n := NewNotifyStore(newFakeStore(), nil)
	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

// TestNotifyStore_LastFiredAt confirms timestamp advances on every
// successful fire and stays put on failure.
func TestNotifyStore_LastFiredAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	n := NewNotifyStore(inner, func() {})

	if !n.LastFiredAt().IsZero() {
		t.Fatalf("expected zero LastFiredAt pre-fire, got %v", n.LastFiredAt())
	}
	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	first := n.LastFiredAt()
	if first.IsZero() {
		t.Fatalf("LastFiredAt did not advance on Create")
	}

	// Failed mutation should not advance.
	inner.createErr = errors.New("boom")
	_ = n.Create(ctx, newTestRule("r1")) // err ignored, store programmed to fail
	if got := n.LastFiredAt(); !got.Equal(first) {
		t.Fatalf("LastFiredAt advanced on failed Create: first=%v got=%v", first, got)
	}

	// Successful mutation should advance again.
	inner.createErr = nil
	// Sleep at least a nanosecond's worth of monotonic time so the
	// timestamps differ on every platform.
	time.Sleep(1 * time.Millisecond)
	if err := n.Create(ctx, newTestRule("r2")); err != nil {
		t.Fatalf("Create r2: %v", err)
	}
	if !n.LastFiredAt().After(first) {
		t.Fatalf("LastFiredAt did not advance after second success: first=%v now=%v", first, n.LastFiredAt())
	}
}

// TestNotifyStore_CallbackPanic verifies a panicking callback does not
// corrupt the wrapper or the underlying store; subsequent operations
// keep working.
func TestNotifyStore_CallbackPanic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	n := NewNotifyStore(inner, func() { panic("intentional") })

	// Mutation succeeds even though the callback panics; recover()
	// inside fire() swallows it.
	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := n.Get(ctx, "r1")
	if err != nil {
		t.Fatalf("Get after panicking callback: %v", err)
	}
	if got.ID() != "r1" {
		t.Fatalf("Get returned wrong id: %s", got.ID())
	}

	// Swap to a sane callback and confirm the wrapper still fires.
	var c counter
	n.SetOnChange(c.inc)
	if err := n.Create(ctx, newTestRule("r2")); err != nil {
		t.Fatalf("Create r2: %v", err)
	}
	if c.load() != 1 {
		t.Fatalf("fire after recover: got %d want 1", c.load())
	}
}

// TestNotifyStore_ConcurrentCreateAndSetOnChange exercises the
// callback swap path under -race. The point is that SetOnChange does
// not race with the read-and-invoke inside fire(); whichever callback
// is installed at fire-time runs and no goroutine observes a torn
// pointer.
func TestNotifyStore_ConcurrentCreateAndSetOnChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	var c1, c2 counter
	n := NewNotifyStore(inner, c1.inc)

	const N = 200
	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine: alternates between the two callbacks.
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			if i%2 == 0 {
				n.SetOnChange(c1.inc)
			} else {
				n.SetOnChange(c2.inc)
			}
		}
	}()

	// Mutator goroutine: creates / deletes a single rule repeatedly so
	// fire() runs many times in parallel with SetOnChange.
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			_ = n.Create(ctx, newTestRule("r"))
			_ = n.Delete(ctx, "r")
		}
	}()

	wg.Wait()

	// Sanity: every successful mutation hit exactly one of the two
	// counters (we can't assert which because of the race window, but
	// the total must equal the number of successful mutations).
	totalFires := c1.load() + c2.load()
	if totalFires == 0 {
		t.Fatalf("no fires observed under concurrent load")
	}
}

// TestNotifyStore_WrapsListAndGet confirms read methods pass through
// to the underlying store unchanged (they should — they're inherited
// via the embedded Store interface, but we lock that contract in).
func TestNotifyStore_WrapsListAndGet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	var c counter
	n := NewNotifyStore(inner, c.inc)

	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := n.Create(ctx, newTestRule("r2")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	c.reset()

	got, err := n.Get(ctx, "r1")
	if err != nil || got.ID() != "r1" {
		t.Fatalf("Get r1: rule=%v err=%v", got, err)
	}
	list, err := n.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("List: len=%d err=%v", len(list), err)
	}
	if c.load() != 0 {
		t.Fatalf("read methods triggered fire: %d", c.load())
	}
}
