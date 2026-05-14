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

func (c *counter) inc()        { c.n.Add(1) }
func (c *counter) load() int64 { return c.n.Load() }
func (c *counter) reset()      { c.n.Store(0) }

// waitForCounterAtLeast polls the counter and returns once it reaches
// `want` or the deadline expires. Used by the S6-async tests where
// the worker runs on its own goroutine — every assert on "fire
// happened" needs to allow the worker to drain. Returns the value
// actually observed.
func waitForCounterAtLeast(t *testing.T, c *counter, want int64, d time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if got := c.load(); got >= want {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	return c.load()
}

// quiesceWorker gives the async worker a chance to drain any pending
// fire before the test asserts. Used in failure-path tests that want
// to prove "no fire ever happens" — we can't prove a negative, but we
// can wait long enough that any in-flight fire would have completed.
func quiesceWorker(t *testing.T) {
	t.Helper()
	// 25ms is a generous lower bound: the worker invokeOnChange path
	// runs in microseconds on a healthy host, so anything that hasn't
	// fired by now would have starvation issues unrelated to the test.
	time.Sleep(25 * time.Millisecond)
}

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
			t.Cleanup(func() { _ = n.Close() })
			err := tc.mutate(n)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v wantErr=%v", err, tc.wantErr)
			}
			// S6: fire is async — wait briefly for the worker to drain
			// when we expect a fire; for "no fire" cases, quiesce so any
			// stray dispatch would have run by the assertion.
			var got int64
			if tc.wantFireInc > 0 {
				got = waitForCounterAtLeast(t, &c, tc.wantFireInc, time.Second)
			} else {
				quiesceWorker(t)
				got = c.load()
			}
			if got != tc.wantFireInc {
				t.Fatalf("fire counter: got %d want %d", got, tc.wantFireInc)
			}
		})
	}
}

// TestNotifyStore_SetOnChange verifies the callback can be swapped
// after construction (the production wiring needs this because the
// scheduler is built after the store). S6: each fire is async, so
// we wait for the counter to advance before swapping.
func TestNotifyStore_SetOnChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	var a, b counter
	n := NewNotifyStore(inner, a.inc)
	t.Cleanup(func() { _ = n.Close() })

	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create r1: %v", err)
	}
	if got := waitForCounterAtLeast(t, &a, 1, time.Second); got != 1 || b.load() != 0 {
		t.Fatalf("after Create r1: a=%d b=%d", got, b.load())
	}

	n.SetOnChange(b.inc)
	if err := n.Create(ctx, newTestRule("r2")); err != nil {
		t.Fatalf("Create r2: %v", err)
	}
	if got := waitForCounterAtLeast(t, &b, 1, time.Second); got != 1 || a.load() != 1 {
		t.Fatalf("after SetOnChange + Create r2: a=%d b=%d", a.load(), got)
	}

	// Nil callback must be tolerated (no-op).
	n.SetOnChange(nil)
	if err := n.Create(ctx, newTestRule("r3")); err != nil {
		t.Fatalf("Create r3: %v", err)
	}
	quiesceWorker(t)
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
	t.Cleanup(func() { _ = n.Close() })
	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Allow the worker to handle the fire (no-op) before returning.
	quiesceWorker(t)
}

// TestNotifyStore_LastFiredAt confirms timestamp advances on every
// successful fire and stays put on failure. S6: stamping happens
// inside the worker, so we wait via waitForCounter on a side counter.
func TestNotifyStore_LastFiredAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	var c counter
	n := NewNotifyStore(inner, c.inc)
	t.Cleanup(func() { _ = n.Close() })

	if !n.LastFiredAt().IsZero() {
		t.Fatalf("expected zero LastFiredAt pre-fire, got %v", n.LastFiredAt())
	}
	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitForCounterAtLeast(t, &c, 1, time.Second)
	first := n.LastFiredAt()
	if first.IsZero() {
		t.Fatalf("LastFiredAt did not advance on Create")
	}

	// Failed mutation should not advance.
	inner.createErr = errors.New("boom")
	_ = n.Create(ctx, newTestRule("r1")) // err ignored, store programmed to fail
	quiesceWorker(t)
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
	waitForCounterAtLeast(t, &c, 2, time.Second)
	if !n.LastFiredAt().After(first) {
		t.Fatalf("LastFiredAt did not advance after second success: first=%v now=%v", first, n.LastFiredAt())
	}
}

// TestNotifyStore_CallbackPanic verifies a panicking callback does not
// corrupt the wrapper or the underlying store; subsequent operations
// keep working AND the worker survives. S6: panic recovery happens
// inside the worker goroutine.
func TestNotifyStore_CallbackPanic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	n := NewNotifyStore(inner, func() { panic("intentional") })
	t.Cleanup(func() { _ = n.Close() })

	// Mutation succeeds even though the callback panics; recover()
	// inside the worker swallows it.
	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Give the worker time to invoke + recover from the panic.
	quiesceWorker(t)
	got, err := n.Get(ctx, "r1")
	if err != nil {
		t.Fatalf("Get after panicking callback: %v", err)
	}
	if got.ID() != "r1" {
		t.Fatalf("Get returned wrong id: %s", got.ID())
	}

	// Swap to a sane callback and confirm the wrapper still fires —
	// proves the worker survived the panic.
	var c counter
	n.SetOnChange(c.inc)
	if err := n.Create(ctx, newTestRule("r2")); err != nil {
		t.Fatalf("Create r2: %v", err)
	}
	if got := waitForCounterAtLeast(t, &c, 1, time.Second); got != 1 {
		t.Fatalf("fire after recover: got %d want 1", got)
	}
}

// TestNotifyStore_ConcurrentCreateAndSetOnChange exercises the
// callback swap path under -race. The point is that SetOnChange does
// not race with the read-and-invoke inside the worker. S6: with async
// dispatch + collapsing, totalFires may be less than the number of
// successful mutations — we just assert "at least one fire" landed.
func TestNotifyStore_ConcurrentCreateAndSetOnChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	var c1, c2 counter
	n := NewNotifyStore(inner, c1.inc)
	t.Cleanup(func() { _ = n.Close() })

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

	// Drain any final pending fire — S6 worker may have one fire still
	// queued after the writer/mutator goroutines exit.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if c1.load()+c2.load() > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

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
	t.Cleanup(func() { _ = n.Close() })

	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := n.Create(ctx, newTestRule("r2")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Wait for the worker to consume any pending fires before resetting,
	// so we know the post-reset count truly captures read-side fires.
	waitForCounterAtLeast(t, &c, 1, time.Second)
	quiesceWorker(t)
	c.reset()

	got, err := n.Get(ctx, "r1")
	if err != nil || got.ID() != "r1" {
		t.Fatalf("Get r1: rule=%v err=%v", got, err)
	}
	list, err := n.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("List: len=%d err=%v", len(list), err)
	}
	quiesceWorker(t)
	if c.load() != 0 {
		t.Fatalf("read methods triggered fire: %d", c.load())
	}
}

// TestNotifyStore_AsyncDispatch_CollapsesBursts (S6) verifies that a
// burst of mutations does NOT produce one onChange per mutation —
// the single-slot channel collapses bursts so the scheduler sees
// snapshots, not edit logs. We block the callback on a release
// channel so the first fire pins the worker while N more mutations
// queue; after release we expect the counter to be strictly less
// than the total mutation count.
func TestNotifyStore_AsyncDispatch_CollapsesBursts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()

	var fires counter
	release := make(chan struct{})
	cb := func() {
		// First invocation blocks until release closes; subsequent ones
		// run instantly. This guarantees a burst window during which
		// fire() calls collapse.
		<-release
		fires.inc()
	}
	n := NewNotifyStore(inner, cb)
	closed := false
	t.Cleanup(func() {
		// Unblock any worker still parked on release before Close so it
		// can drain. Idempotent against close() panics.
		if !closed {
			close(release)
			closed = true
		}
		_ = n.Close()
	})

	// First mutation parks the worker on <-release.
	if err := n.Create(ctx, newTestRule("r0")); err != nil {
		t.Fatalf("Create r0: %v", err)
	}
	// Give the worker time to pick up the first fire and block on
	// release. Without this, the burst might be fully consumed if the
	// scheduler is unusually fast.
	time.Sleep(10 * time.Millisecond)

	const burst = 50
	for i := 1; i <= burst; i++ {
		// Delete + recreate to make every fire path-success — the inner
		// memory store rejects Create of an existing id.
		_ = n.Delete(ctx, "r0")
		_ = n.Create(ctx, newTestRule("r0"))
	}

	// Release the worker; it will drain whatever's in the channel.
	close(release)
	closed = true

	// Wait for the worker to settle. We expect SIGNIFICANTLY fewer
	// fires than the total mutation count — the single-slot channel
	// collapses bursts down to at most "the one currently being run +
	// at most one queued".
	waitForCounterAtLeast(t, &fires, 1, time.Second)
	// Sleep a beat to make sure no late fires sneak in after we read.
	quiesceWorker(t)

	got := fires.load()
	totalMutations := int64(1 + 2*burst) // initial Create + (Delete+Create) per iter

	if got < 1 {
		t.Fatalf("expected at least one fire, got %d", got)
	}
	if got >= totalMutations {
		t.Fatalf("expected fires < mutations (collapse), got %d >= %d", got, totalMutations)
	}
}

// TestNotifyStore_Close_StopsWorker verifies Close() shuts the worker
// down and that subsequent fires are no-ops. Run with -race to catch
// any post-close worker activity.
func TestNotifyStore_Close_StopsWorker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	var c counter
	n := NewNotifyStore(inner, c.inc)

	if err := n.Create(ctx, newTestRule("r1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitForCounterAtLeast(t, &c, 1, time.Second)

	// Close the wrapper. This blocks until the worker has exited.
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second Close must be idempotent — exercising stopOnce.
	if err := n.Close(); err != nil {
		t.Fatalf("Close (idempotent): %v", err)
	}

	// Post-close fire() must be a no-op (the worker is gone). We can't
	// call any mutating method through the wrapper because Close
	// closed the inner Store, but a direct fire() should not panic.
	before := c.load()
	n.fire()
	quiesceWorker(t)
	if got := c.load(); got != before {
		t.Fatalf("fire after Close advanced counter: before=%d after=%d", before, got)
	}
}

// TestNotifyStore_RaceCreateClose stresses Close concurrent with
// in-flight Create calls. Run with -race; success is "no panic, no
// data race report, worker terminates within the deadline".
func TestNotifyStore_RaceCreateClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newFakeStore()
	var c counter
	n := NewNotifyStore(inner, c.inc)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = n.Create(ctx, newTestRule("r"))
			_ = n.Delete(ctx, "r")
		}
	}()

	// Give the writer a head start so Close races against in-flight
	// mutations rather than landing on a quiescent store.
	time.Sleep(2 * time.Millisecond)
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wg.Wait()
}
