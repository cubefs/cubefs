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
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/spec"
)

// emptyBackend lists nothing and returns ErrKeyNotFound for every fetch.
// Used so executor.Run(...) terminates quickly with Status=Done.
type emptyBackend struct {
	closed atomic.Bool
}

func (b *emptyBackend) Kind() string { return "test" }
func (b *emptyBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch := make(chan backend.Entry)
	close(ch)
	return ch, nil
}
func (b *emptyBackend) Get(ctx context.Context, k string, off, sz int64) (io.ReadCloser, error) {
	return nil, backend.ErrKeyNotFound
}
func (b *emptyBackend) Head(ctx context.Context, k string) (int64, string, time.Time, error) {
	return 0, "", time.Time{}, backend.ErrKeyNotFound
}
func (b *emptyBackend) Put(ctx context.Context, k string, body io.Reader, sz int64, opts backend.PutOptions) (backend.PutResult, error) {
	return backend.PutResult{}, nil
}
func (b *emptyBackend) GetChecksum(ctx context.Context, k string) (string, string, error) {
	return "", "", backend.ErrKeyNotFound
}
func (b *emptyBackend) Delete(ctx context.Context, k string) error     { return nil }
func (b *emptyBackend) Rename(ctx context.Context, o, nk string) error { return nil }
func (b *emptyBackend) Capabilities() backend.Caps                     { return backend.Caps{} }
func (b *emptyBackend) Close() error                                   { b.closed.Store(true); return nil }

// blockingBackend.List blocks until ctx is cancelled. Used to exercise
// Runner.Cancel: the task only terminates when executor cancels its
// internal ctx (triggered by exec.Cancel).
type blockingBackend struct {
	emptyBackend
}

func (b *blockingBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch := make(chan backend.Entry)
	go func() {
		<-ctx.Done()
		// Return one entry with Err so the runSync producer sees a fatal
		// listing error AND exits — runSync's producer loop only checks
		// ctx.Done() between reads from the channel.
		select {
		case ch <- backend.Entry{Err: ctx.Err()}:
		default:
		}
		close(ch)
	}()
	return ch, nil
}

// listErrBackend fails List with a fixed error so the task ends in Failed.
type listErrBackend struct {
	emptyBackend
	err error
}

func (b *listErrBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	return nil, b.err
}

// stubRuleLookup implements RuleLookup with a fixed map.
type stubRuleLookup struct {
	mu    sync.RWMutex
	rules map[string]*rules.Rule
}

func newStubRuleLookup() *stubRuleLookup {
	return &stubRuleLookup{rules: make(map[string]*rules.Rule)}
}

func (s *stubRuleLookup) Get(_ context.Context, id string) (*rules.Rule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rules[id]
	if !ok {
		return nil, rules.ErrRuleNotFound
	}
	cp := *r
	return &cp, nil
}

func (s *stubRuleLookup) put(r *rules.Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[r.Config.ID] = r
}

// stubBackendBuilder builds a backend by calling factory(ep). Tests inject
// whatever backend they need (empty, blocking, list-error).
type stubBackendBuilder struct {
	factory func(ep *spec.EndpointConfig) (backend.Backend, error)
}

func (s *stubBackendBuilder) Build(_ context.Context, ep *spec.EndpointConfig) (backend.Backend, error) {
	return s.factory(ep)
}

func newSyncRule(id string) *rules.Rule {
	return rules.NewRule(spec.RuleConfig{
		ID:   id,
		Type: "sync",
		Src:  spec.EndpointConfig{Kind: "cfs", Vol: "v", Path: "/p"},
		Dst:  spec.EndpointConfig{Kind: "s3", Bucket: "b", Prefix: "pfx"},
	})
}

func newLoadRule(id string) *rules.Rule {
	r := newSyncRule(id)
	r.Config.Type = "load"
	return r
}

func newRunnerHarness(t *testing.T, build func(ep *spec.EndpointConfig) (backend.Backend, error)) (*Runner, *stubRuleLookup, *executor.Executor, *memoryStore) {
	t.Helper()
	exec := executor.New(executor.WithProgressInterval(20 * time.Millisecond))
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	builder := &stubBackendBuilder{factory: build}
	runner := NewRunner(exec, store, lookup, builder)
	return runner, lookup, exec, store
}

func TestRunner_TriggerUnknownRule(t *testing.T) {
	r, _, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	_, err := r.Trigger(context.Background(), "ghost", false)
	if !errors.Is(err, rules.ErrRuleNotFound) {
		t.Errorf("err = %v, want ErrRuleNotFound", err)
	}
}

func TestRunner_TriggerEmptyRuleID(t *testing.T) {
	r, _, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	if _, err := r.Trigger(context.Background(), "", false); err == nil {
		t.Error("err = nil, want error")
	}
}

func TestRunner_TriggerNoWaitReturnsRunning(t *testing.T) {
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &blockingBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	rec, err := r.Trigger(context.Background(), "rule1", false)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rec.Status != executor.StatusRunning {
		t.Errorf("Status = %q, want running", rec.Status)
	}
	if rec.RuleID != "rule1" || rec.TaskID == "" {
		t.Errorf("Record = %+v", rec)
	}

	// Cleanup: cancel so the goroutine exits before t.Cleanup runs.
	if err := r.Cancel(context.Background(), rec.TaskID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	waitForStatus(t, store, rec.TaskID, executor.StatusCancelled, 3*time.Second)
}

func TestRunner_TriggerWaitBlocksUntilDone(t *testing.T) {
	r, lookup, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	rec, err := r.Trigger(context.Background(), "rule1", true)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rec.Status != executor.StatusDone {
		t.Errorf("Status = %q, want done", rec.Status)
	}
	if rec.DoneAt.IsZero() {
		t.Error("DoneAt is zero on terminal record")
	}
}

func TestRunner_TriggerWaitRespectsCtxCancel(t *testing.T) {
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &blockingBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	ctx, cancel := context.WithCancel(context.Background())
	type res struct {
		rec *Record
		err error
	}
	resCh := make(chan res, 1)
	// Start trigger with wait=true in a goroutine, then cancel the request ctx.
	go func() {
		rec, err := r.Trigger(ctx, "rule1", true)
		resCh <- res{rec, err}
	}()

	// Give the runner time to register the waiter.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case got := <-resCh:
		if !errors.Is(got.err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Trigger did not return after ctx cancel")
	}

	// Task itself is still running until we explicitly cancel the runner.
	// Look it up — the record must still exist and be Running.
	recs, _ := store.List(context.Background(), executor.StatusRunning)
	if len(recs) != 1 {
		t.Fatalf("running records = %d, want 1", len(recs))
	}
	taskID := recs[0].TaskID
	if err := r.Cancel(context.Background(), taskID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	waitForStatus(t, store, taskID, executor.StatusCancelled, 3*time.Second)
}

func TestRunner_CancelUnknown(t *testing.T) {
	r, _, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	if err := r.Cancel(context.Background(), "ghost"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("err = %v, want ErrTaskNotFound", err)
	}
}

func TestRunner_CancelFlipsStatus(t *testing.T) {
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &blockingBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	rec, err := r.Trigger(context.Background(), "rule1", false)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}

	if err := r.Cancel(context.Background(), rec.TaskID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	waitForStatus(t, store, rec.TaskID, executor.StatusCancelled, 2*time.Second)
}

func TestRunner_RetryFailedRecord(t *testing.T) {
	// First Trigger uses listErrBackend → task fails. Then flip the factory
	// to emptyBackend so the retry succeeds.
	var useFailing atomic.Bool
	useFailing.Store(true)
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		if useFailing.Load() {
			return &listErrBackend{err: errors.New("boom")}, nil
		}
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	rec, err := r.Trigger(context.Background(), "rule1", true)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rec.Status != executor.StatusFailed {
		t.Fatalf("first Status = %q, want failed; err=%q", rec.Status, rec.Error)
	}

	useFailing.Store(false)
	retry, err := r.Retry(context.Background(), rec.TaskID)
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if retry.TaskID == rec.TaskID {
		t.Errorf("retry TaskID = %q, want different from %q", retry.TaskID, rec.TaskID)
	}
	if retry.RuleID != "rule1" {
		t.Errorf("retry RuleID = %q", retry.RuleID)
	}

	// Wait for retry to finish.
	waitForStatus(t, store, retry.TaskID, executor.StatusDone, 2*time.Second)

	// Original record must still be Failed.
	orig, err := store.Get(context.Background(), rec.TaskID)
	if err != nil {
		t.Fatalf("Get original: %v", err)
	}
	if orig.Status != executor.StatusFailed {
		t.Errorf("original Status = %q, want failed", orig.Status)
	}
}

func TestRunner_RetryUnknown(t *testing.T) {
	r, _, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	if _, err := r.Retry(context.Background(), "ghost"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("err = %v, want ErrTaskNotFound", err)
	}
}

func TestRunner_TriggerAsTypeMismatch(t *testing.T) {
	r, lookup, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("syncrule"))

	if _, err := r.TriggerAs(context.Background(), "syncrule", executor.TaskTypeLoad, false); !errors.Is(err, ErrRuleTypeMismatch) {
		t.Errorf("err = %v, want ErrRuleTypeMismatch", err)
	}
}

func TestRunner_TriggerAsTypeMatches(t *testing.T) {
	r, lookup, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newLoadRule("loadrule"))

	rec, err := r.TriggerAs(context.Background(), "loadrule", executor.TaskTypeLoad, true)
	if err != nil {
		t.Fatalf("TriggerAs: %v", err)
	}
	if rec.Type != executor.TaskTypeLoad {
		t.Errorf("Type = %q, want load", rec.Type)
	}
}

func TestRunner_TriggerAsUnknownRule(t *testing.T) {
	r, _, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	if _, err := r.TriggerAs(context.Background(), "ghost", executor.TaskTypeSync, false); !errors.Is(err, rules.ErrRuleNotFound) {
		t.Errorf("err = %v, want ErrRuleNotFound", err)
	}
}

func TestRunner_BuilderErrorSurfacesFailure(t *testing.T) {
	r, lookup, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return nil, errors.New("build refused")
	})
	lookup.put(newSyncRule("rule1"))

	_, err := r.Trigger(context.Background(), "rule1", false)
	if err == nil {
		t.Fatal("err = nil, want builder failure")
	}
}

func TestRunner_BuilderDstErrorClosesSrc(t *testing.T) {
	var srcInstance *emptyBackend
	step := 0
	r, lookup, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		step++
		if step == 1 {
			srcInstance = &emptyBackend{}
			return srcInstance, nil
		}
		return nil, errors.New("dst boom")
	})
	lookup.put(newSyncRule("rule1"))

	if _, err := r.Trigger(context.Background(), "rule1", false); err == nil {
		t.Fatal("err = nil, want dst failure")
	}
	if srcInstance == nil || !srcInstance.closed.Load() {
		t.Error("src backend was not closed when dst build failed")
	}
}

func TestRunner_ConcurrentTriggersDoNotRace(t *testing.T) {
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.Trigger(context.Background(), "rule1", true)
			if err != nil {
				t.Errorf("Trigger: %v", err)
			}
		}()
	}
	wg.Wait()

	got, _ := store.List(context.Background(), "")
	if len(got) != 10 {
		t.Errorf("records = %d, want 10", len(got))
	}
}

func TestRunner_DefaultIDFactoryUnique(t *testing.T) {
	r, _, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := r.defaultIDFactory()
		if seen[id] {
			t.Fatalf("duplicate id %q at iter %d", id, i)
		}
		seen[id] = true
	}
}

func TestRunner_WithIDFactoryOverride(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	}}
	var seq int32
	r := NewRunner(exec, store, lookup, builder, WithIDFactory(func() string {
		return fmt.Sprintf("fixed-%d", atomic.AddInt32(&seq, 1))
	}))
	rec, err := r.Trigger(context.Background(), "rule1", true)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rec.TaskID != "fixed-1" {
		t.Errorf("TaskID = %q, want fixed-1", rec.TaskID)
	}
}

// TestRunner_TriggerWithIDHonoursSuppliedID confirms master can pin the
// taskID on a trigger so the syncnode's local Record key stays in sync
// with master's taskOwner ledger (Bug #1 fix).
func TestRunner_TriggerWithIDHonoursSuppliedID(t *testing.T) {
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	rec, err := r.TriggerWithID(context.Background(), "rule1", "master-t-42", true)
	if err != nil {
		t.Fatalf("TriggerWithID: %v", err)
	}
	if rec.TaskID != "master-t-42" {
		t.Fatalf("TaskID = %q, want %q", rec.TaskID, "master-t-42")
	}
	// The persisted Record must use the same key — otherwise master's
	// Cancel(master-t-42) lookup misses.
	persisted, err := store.Get(context.Background(), "master-t-42")
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if persisted.TaskID != "master-t-42" {
		t.Fatalf("persisted TaskID = %q, want %q", persisted.TaskID, "master-t-42")
	}
}

// TestRunner_TriggerWithIDEmptyFallsBack confirms an empty taskID falls
// back to idFactory — defensive path for older masters that don't carry
// the field yet.
func TestRunner_TriggerWithIDEmptyFallsBack(t *testing.T) {
	r, lookup, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	rec, err := r.TriggerWithID(context.Background(), "rule1", "", true)
	if err != nil {
		t.Fatalf("TriggerWithID: %v", err)
	}
	if rec.TaskID == "" {
		t.Fatal("TaskID is empty, want auto-generated id")
	}
}

// TestRunner_OnTerminalFiresOncePerTask is the wire test for Bug #3: master
// learns of terminal status by way of this callback (which the server-side
// pushes back via OpSyncNodeRunTask response). Must fire exactly once per
// task with the final-state record.
func TestRunner_OnTerminalFiresOncePerTask(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	}}

	var (
		mu       sync.Mutex
		seenRecs []*Record
	)
	cb := func(rec *Record) {
		mu.Lock()
		defer mu.Unlock()
		seenRecs = append(seenRecs, rec)
	}

	r := NewRunner(exec, store, lookup, builder, WithOnTerminal(cb))
	rec, err := r.Trigger(context.Background(), "rule1", true)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seenRecs) != 1 {
		t.Fatalf("onTerminal fired %d times, want 1", len(seenRecs))
	}
	got := seenRecs[0]
	if got.TaskID != rec.TaskID {
		t.Fatalf("callback TaskID = %q, want %q", got.TaskID, rec.TaskID)
	}
	if got.Status != executor.StatusDone {
		t.Fatalf("callback Status = %q, want done", got.Status)
	}
}

// TestRunner_OnTerminalPanicDoesNotKillRunGoroutine wires a buggy callback
// that panics; the run goroutine must survive (otherwise it would leak the
// done channel and any wait=true caller would hang forever).
func TestRunner_OnTerminalPanicDoesNotKillRunGoroutine(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	}}

	r := NewRunner(exec, store, lookup, builder, WithOnTerminal(func(*Record) {
		panic("intentional test panic")
	}))
	rec, err := r.Trigger(context.Background(), "rule1", true)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rec.Status != executor.StatusDone {
		t.Fatalf("Status = %q, want done — task should have completed despite panic", rec.Status)
	}
}

func TestNextRetryID(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"t-123", "t-123-r1"},
		{"t-123-r1", "t-123-r2"},
		{"t-123-r9", "t-123-r10"},
		{"t-123-rabc", "t-123-rabc-r1"}, // non-numeric suffix → treat as base
		{"plain", "plain-r1"},
	}
	for _, tc := range cases {
		if got := nextRetryID(tc.in); got != tc.want {
			t.Errorf("nextRetryID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestEndpointPath(t *testing.T) {
	cases := []struct {
		ep   spec.EndpointConfig
		want string
	}{
		{spec.EndpointConfig{Kind: "cfs", Path: "/v"}, "/v"},
		{spec.EndpointConfig{Kind: "local", Path: "/local"}, "/local"},
		{spec.EndpointConfig{Kind: "s3", Prefix: "p"}, "p"},
	}
	for i, tc := range cases {
		if got := endpointPath(&tc.ep); got != tc.want {
			t.Errorf("[%d] = %q, want %q", i, got, tc.want)
		}
	}
}

// waitForStatus polls store.Get until the record reaches wantStatus or
// timeout fires. Fails the test on timeout to keep flakes loud.
func waitForStatus(t *testing.T, store Store, taskID string, want executor.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rec, err := store.Get(context.Background(), taskID)
		if err == nil && rec.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec, _ := store.Get(context.Background(), taskID)
	t.Fatalf("task %s never reached %q within %s; current=%+v", taskID, want, timeout, rec)
}

// gatedBackend is a controllable test backend: List signals it has
// started by closing/sending on `started`, then blocks until `release`
// is closed (or ctx is cancelled). Used to pin tasks in the "running"
// state for concurrency-cap tests.
type gatedBackend struct {
	emptyBackend
	started chan struct{}
	release chan struct{}
	// inFlight counts how many Lists are currently blocked between
	// started and release. Atomic so concurrent test assertions are safe.
	inFlight *atomic.Int32
}

func (b *gatedBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch := make(chan backend.Entry)
	go func() {
		// Mark started so the test can observe slot occupancy.
		select {
		case b.started <- struct{}{}:
		default:
		}
		if b.inFlight != nil {
			b.inFlight.Add(1)
			defer b.inFlight.Add(-1)
		}
		// Block until release or ctx cancel. Either way the producer
		// loop in runSync exits without enumerating any entries, so
		// executor.Run returns StatusDone (empty listing) or
		// StatusCancelled (ctx cancel) — both terminal.
		select {
		case <-b.release:
		case <-ctx.Done():
		}
		close(ch)
	}()
	return ch, nil
}

// newGatedBackend constructs a gatedBackend with fresh channels. The
// returned `release` channel — closing it lets every still-blocked List
// finish, draining tasks held by this backend.
func newGatedBackend(inFlight *atomic.Int32) (*gatedBackend, chan struct{}) {
	rel := make(chan struct{})
	return &gatedBackend{
		started:  make(chan struct{}, 64),
		release:  rel,
		inFlight: inFlight,
	}, rel
}

// TestRunner_MaxConcurrent_AdmitsUpToCap verifies cap=2 admits exactly
// two tasks simultaneously while a third waits (or is rejected, depending
// on queue size — here queue=0 so the third must error).
func TestRunner_MaxConcurrent_AdmitsUpToCap(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))

	var inFlight atomic.Int32
	be, release := newGatedBackend(&inFlight)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return be, nil
	}}
	r := NewRunner(exec, store, lookup, builder, WithMaxConcurrent(2))

	// Fire 2 tasks — both should be admitted and reach the running gate.
	recs := make([]*Record, 0, 2)
	for i := 0; i < 2; i++ {
		rec, err := r.Trigger(context.Background(), "rule1", false)
		if err != nil {
			t.Fatalf("Trigger[%d]: %v", i, err)
		}
		recs = append(recs, rec)
	}
	// Wait until both Lists have entered the gate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if inFlight.Load() == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := inFlight.Load(); got != 2 {
		t.Fatalf("inFlight = %d, want 2", got)
	}
	if got := r.RunningCount(); got != 2 {
		t.Errorf("RunningCount = %d, want 2", got)
	}
	// Drain so the test exits cleanly (avoids racing executor.Close()
	// against still-running goroutines).
	close(release)
	for _, rec := range recs {
		waitForStatus(t, store, rec.TaskID, executor.StatusDone, 3*time.Second)
	}
}

// TestRunner_MaxConcurrent_FailFastWhenFull verifies that with cap=1 and
// queue=0 a second Trigger immediately returns ErrQueueFull AND the
// rejected record is persisted as failed so operators see it in the
// task list.
func TestRunner_MaxConcurrent_FailFastWhenFull(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))

	var inFlight atomic.Int32
	be, release := newGatedBackend(&inFlight)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return be, nil
	}}
	r := NewRunner(exec, store, lookup, builder, WithMaxConcurrent(1))

	first, err := r.Trigger(context.Background(), "rule1", false)
	if err != nil {
		t.Fatalf("first Trigger: %v", err)
	}
	// Wait until it occupies the slot.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.RunningCount() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Second trigger must fail-fast.
	rec, err := r.Trigger(context.Background(), "rule1", false)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	if rec == nil || rec.Status != executor.StatusFailed {
		t.Fatalf("rejected record Status = %q, want failed (rec=%+v)", rec.Status, rec)
	}
	// Persisted in the store so operators see it.
	persisted, err := store.Get(context.Background(), rec.TaskID)
	if err != nil {
		t.Fatalf("rejected record not persisted: %v", err)
	}
	if persisted.Status != executor.StatusFailed || persisted.Error != ErrQueueFull.Error() {
		t.Fatalf("persisted Record = %+v, want failed+queue-full error", persisted)
	}

	// Drain.
	close(release)
	waitForStatus(t, store, first.TaskID, executor.StatusDone, 2*time.Second)
}

// TestRunner_MaxConcurrent_QueueAdmitsThenRuns verifies the queueing path:
// cap=1, queue=2 → 3 Triggers all admitted, only 1 running at a time,
// rest drain in order.
func TestRunner_MaxConcurrent_QueueAdmitsThenRuns(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))

	var inFlight atomic.Int32
	// Each call to Build returns a fresh gatedBackend so tasks can be
	// released independently. We track them so the test can drain.
	var (
		buildMu  sync.Mutex
		gates    []chan struct{}
		gateUsed int
	)
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		buildMu.Lock()
		defer buildMu.Unlock()
		// Two Builds per Trigger (src + dst) — reuse the same gate per
		// pair so the per-task release is one signal.
		var be backend.Backend
		if gateUsed%2 == 0 {
			b, rel := newGatedBackend(&inFlight)
			gates = append(gates, rel)
			be = b
		} else {
			// Reuse the most-recent gate's backend so closing one
			// release frees the whole task (src AND dst). Listing
			// only runs on src in sync_task; dst's List would never
			// be called.
			b, _ := newGatedBackend(&inFlight)
			be = b
		}
		gateUsed++
		return be, nil
	}}
	r := NewRunner(exec, store, lookup, builder, WithMaxConcurrent(1), WithQueueSize(2))

	recs := make([]*Record, 0, 3)
	for i := 0; i < 3; i++ {
		rec, err := r.Trigger(context.Background(), "rule1", false)
		if err != nil {
			t.Fatalf("Trigger[%d]: %v", i, err)
		}
		recs = append(recs, rec)
	}
	// Cap=1 → exactly one running at a time; QueueLen should be 2.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.RunningCount() == 1 && r.QueueLen() == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got, want := r.RunningCount(), 1; got != want {
		t.Errorf("RunningCount = %d, want %d", got, want)
	}
	if got, want := r.QueueLen(), 2; got != want {
		t.Errorf("QueueLen = %d, want %d", got, want)
	}

	// Drain: release tasks one at a time, in any order. Each released
	// task lets the next queued one acquire the slot.
	buildMu.Lock()
	releases := append([]chan struct{}(nil), gates...)
	buildMu.Unlock()
	for _, rel := range releases {
		close(rel)
	}
	// All 3 must reach done eventually.
	for _, rec := range recs {
		waitForStatus(t, store, rec.TaskID, executor.StatusDone, 3*time.Second)
	}
	if got := r.QueueLen(); got != 0 {
		t.Errorf("final QueueLen = %d, want 0", got)
	}
}

// TestRunner_MaxConcurrent_QueueFullReturnsErr verifies the third
// trigger is rejected when cap=1 + queue=1 are both saturated.
func TestRunner_MaxConcurrent_QueueFullReturnsErr(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))

	var inFlight atomic.Int32
	be, release := newGatedBackend(&inFlight)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return be, nil
	}}
	r := NewRunner(exec, store, lookup, builder, WithMaxConcurrent(1), WithQueueSize(1))

	// First fills the slot.
	first, err := r.Trigger(context.Background(), "rule1", false)
	if err != nil {
		t.Fatalf("Trigger[0]: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && r.RunningCount() != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	// Second fills the queue.
	second, err := r.Trigger(context.Background(), "rule1", false)
	if err != nil {
		t.Fatalf("Trigger[1]: %v", err)
	}
	for time.Now().Before(deadline) && r.QueueLen() != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	// Third must be rejected.
	rec, err := r.Trigger(context.Background(), "rule1", false)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third err = %v, want ErrQueueFull", err)
	}
	if rec == nil || rec.Status != executor.StatusFailed {
		t.Fatalf("rejected Status = %q, want failed", rec.Status)
	}
	// Drain admitted tasks so t.Cleanup doesn't race against
	// still-running goroutines (executor.Close() nils the running map;
	// a late Run call would panic).
	close(release)
	waitForStatus(t, store, first.TaskID, executor.StatusDone, 3*time.Second)
	waitForStatus(t, store, second.TaskID, executor.StatusDone, 3*time.Second)
}

// TestRunner_MaxConcurrent_Unlimited_DefaultBehavior verifies the
// no-options Runner preserves pre-fix unlimited semantics: 100
// concurrent triggers all run (well, all complete) without any of them
// returning ErrQueueFull.
func TestRunner_MaxConcurrent_Unlimited_DefaultBehavior(t *testing.T) {
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	var wg sync.WaitGroup
	const N = 100
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.Trigger(context.Background(), "rule1", true); err != nil {
				t.Errorf("Trigger: %v", err)
			}
		}()
	}
	wg.Wait()

	got, _ := store.List(context.Background(), "")
	if len(got) != N {
		t.Errorf("records = %d, want %d", len(got), N)
	}
	// In unlimited mode RunningCount is intentionally 0 (slots == nil).
	if r.RunningCount() != 0 {
		t.Errorf("unlimited RunningCount = %d, want 0", r.RunningCount())
	}
}

// TestRunner_MaxConcurrent_PanicReleasesSlot verifies the deferred
// release in run() fires even when the executor panics, so the cap is
// reclaimed and the next trigger can proceed.
func TestRunner_MaxConcurrent_PanicReleasesSlot(t *testing.T) {
	r, lookup, _, _ := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	// Re-create runner with cap=1 — newRunnerHarness doesn't take options.
	exec := r.exec
	store2 := NewMemoryStore()
	t.Cleanup(func() { _ = store2.Close() })
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	}}
	r2 := NewRunner(exec, store2, lookup, builder, WithMaxConcurrent(1),
		WithOnTerminal(func(*Record) { panic("simulated terminal-hook panic") }))
	lookup.put(newSyncRule("rule1"))

	// First trigger: terminal hook panics; the recover in run() should
	// keep the goroutine alive AND the deferred release should reclaim
	// the slot. Wait for the task to terminate so the release has fired.
	rec, err := r2.Trigger(context.Background(), "rule1", true)
	if err != nil {
		t.Fatalf("first Trigger: %v", err)
	}
	if rec.Status != executor.StatusDone {
		t.Fatalf("first Status = %q, want done", rec.Status)
	}
	if r2.RunningCount() != 0 {
		t.Fatalf("RunningCount after terminate = %d, want 0 (slot leaked)", r2.RunningCount())
	}
	// Second trigger must succeed — proves the slot was released.
	rec2, err := r2.Trigger(context.Background(), "rule1", true)
	if err != nil {
		t.Fatalf("second Trigger: %v", err)
	}
	if rec2.Status != executor.StatusDone {
		t.Fatalf("second Status = %q, want done", rec2.Status)
	}

	// Sanity: store2 has 2 records.
	got, _ := store2.List(context.Background(), "")
	if len(got) != 2 {
		t.Errorf("records = %d, want 2", len(got))
	}
}

// TestRunner_QueueLength_ExposedViaCount verifies QueueLen reports the
// number of waiting tasks accurately, including under churn.
func TestRunner_QueueLength_ExposedViaCount(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))

	var inFlight atomic.Int32
	be, release := newGatedBackend(&inFlight)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return be, nil
	}}
	r := NewRunner(exec, store, lookup, builder, WithMaxConcurrent(1), WithQueueSize(3))

	if got := r.QueueLen(); got != 0 {
		t.Errorf("initial QueueLen = %d, want 0", got)
	}

	// Fill slot + 2 queued.
	recs := make([]*Record, 0, 3)
	for i := 0; i < 3; i++ {
		rec, err := r.Trigger(context.Background(), "rule1", false)
		if err != nil {
			t.Fatalf("Trigger[%d]: %v", i, err)
		}
		recs = append(recs, rec)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.QueueLen() == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := r.QueueLen(); got != 2 {
		t.Errorf("QueueLen with 2 waiters = %d, want 2", got)
	}
	close(release)
	// Wait for all 3 to finish so executor.Close() in t.Cleanup doesn't
	// race against still-running tasks.
	for _, rec := range recs {
		waitForStatus(t, store, rec.TaskID, executor.StatusDone, 3*time.Second)
	}
	if got := r.QueueLen(); got != 0 {
		t.Errorf("final QueueLen = %d, want 0", got)
	}
}

// TestRunner_Cancel_CancelsQueuedTask covers FIX Q1: a Cancel on a task
// that has been admitted to the queue but does not yet hold a slot must
// flip the record to Cancelled and prevent the task from ever running.
//
// Pre-fix behavior was a silent no-op — executor.Cancel only knows
// running tasks.
func TestRunner_Cancel_CancelsQueuedTask(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))

	var inFlight atomic.Int32
	be, release := newGatedBackend(&inFlight)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	// Count how many times the builder fires per task — proxy for
	// "did the queued task ever progress past triggerRule?". The
	// QUEUED task DOES still build (admission is post-build), but the
	// gated backend's List() is what would block the running task; we
	// assert the queued task never reaches gatedBackend.List by
	// watching the started channel.
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return be, nil
	}}
	r := NewRunner(exec, store, lookup, builder, WithMaxConcurrent(1), WithQueueSize(1))

	first, err := r.Trigger(context.Background(), "rule1", false)
	if err != nil {
		t.Fatalf("first Trigger: %v", err)
	}
	// Wait for first to occupy the slot (List started).
	select {
	case <-be.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first task did not start within 2s")
	}
	queued, err := r.Trigger(context.Background(), "rule1", false)
	if err != nil {
		t.Fatalf("queued Trigger: %v", err)
	}
	// Confirm the second task is genuinely queued, not running.
	if got := r.QueueLen(); got != 1 {
		t.Fatalf("QueueLen before cancel = %d, want 1", got)
	}

	// Snapshot the started-events seen so far so we can prove the queued
	// task never starts after cancel.
	startedBefore := len(be.started)

	// Q1: Cancel must work on a queued task.
	if err := r.Cancel(context.Background(), queued.TaskID); err != nil {
		t.Fatalf("Cancel queued: %v", err)
	}
	// The record must flip to Cancelled within a generous bound; in
	// practice the deregister + persist completes in microseconds.
	waitForStatus(t, store, queued.TaskID, executor.StatusCancelled, 2*time.Second)
	// Queue depth must drop to 0 (queued task aborted, never claimed a
	// slot) before we release first.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && r.QueueLen() != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := r.QueueLen(); got != 0 {
		t.Errorf("QueueLen after queued cancel = %d, want 0", got)
	}

	// Release the running task. The queued task must NOT have started
	// (gatedBackend.List should fire exactly once — for `first`).
	close(release)
	waitForStatus(t, store, first.TaskID, executor.StatusDone, 3*time.Second)

	if got := len(be.started); got != startedBefore {
		t.Errorf("queued task started after cancel: started events grew from %d to %d", startedBefore, got)
	}
}

// TestRunner_Close_DrainsQueueWithoutPanic covers FIX Q2: Close must
// cancel queued + running tasks and wait for every spawned goroutine
// to finish, BEFORE the caller proceeds to shut down the executor. A
// pre-fix shutdown sequence could race a queued goroutine into a nil
// executor.running map.
func TestRunner_Close_DrainsQueueWithoutPanic(t *testing.T) {
	exec := executor.New()
	t.Cleanup(func() { _ = exec.Close() })
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	lookup := newStubRuleLookup()
	lookup.put(newSyncRule("rule1"))

	var inFlight atomic.Int32
	be, release := newGatedBackend(&inFlight)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	builder := &stubBackendBuilder{factory: func(*spec.EndpointConfig) (backend.Backend, error) {
		return be, nil
	}}
	r := NewRunner(exec, store, lookup, builder, WithMaxConcurrent(1), WithQueueSize(3))

	recs := make([]*Record, 0, 3)
	for i := 0; i < 3; i++ {
		rec, err := r.Trigger(context.Background(), "rule1", false)
		if err != nil {
			t.Fatalf("Trigger[%d]: %v", i, err)
		}
		recs = append(recs, rec)
	}
	// One running, two queued. Wait for the steady state.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.RunningCount() == 1 && r.QueueLen() == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got, want := r.QueueLen(), 2; got != want {
		t.Fatalf("QueueLen before Close = %d, want %d", got, want)
	}

	// Close in a goroutine — running task is gated, so Close has to
	// cancel it via the per-task ctx to unblock the wg.Wait.
	closeDone := make(chan error, 1)
	go func() { closeDone <- r.Close() }()

	// Close must return within a reasonable timeout. The running
	// gated task only exits when its ctx fires — exec.Run propagates
	// taskCtx into the backend's List which respects ctx.Done.
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s — wg.Wait stuck")
	}

	// All three records must be terminal. Cancelled is the expected
	// status for both queued tasks and (because the running task's
	// ctx was cancelled mid-List) the in-flight task.
	for _, rec := range recs {
		got, err := store.Get(context.Background(), rec.TaskID)
		if err != nil {
			t.Fatalf("Get %s: %v", rec.TaskID, err)
		}
		if got.Status != executor.StatusCancelled && got.Status != executor.StatusDone {
			t.Errorf("task %s Status = %q, want cancelled or done", rec.TaskID, got.Status)
		}
		if got.DoneAt.IsZero() {
			t.Errorf("task %s DoneAt is zero after Close", rec.TaskID)
		}
	}

	// Second Close is idempotent.
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestRunner_Close_RefusesNewTriggers covers FIX Q2: once Close has
// fired, triggerRule must refuse new admissions to avoid spawning
// goroutines that race the shutdown.
func TestRunner_Close_RefusesNewTriggers(t *testing.T) {
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rec, err := r.Trigger(context.Background(), "rule1", false)
	if err == nil {
		t.Fatal("Trigger after Close = nil err, want error")
	}
	if rec != nil {
		t.Errorf("Trigger after Close returned record = %+v, want nil", rec)
	}
	// And no record should have been persisted.
	got, _ := store.List(context.Background(), "")
	if len(got) != 0 {
		t.Errorf("records after refused Trigger = %d, want 0", len(got))
	}
}

// TestRunner_Cancel_AfterRunStarts_StillWorks confirms the per-task
// canceller introduced by FIX Q1 still covers the running phase. With
// the new design the executor's per-task cancel is driven by the same
// ctx.
func TestRunner_Cancel_AfterRunStarts_StillWorks(t *testing.T) {
	r, lookup, _, store := newRunnerHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &blockingBackend{}, nil
	})
	lookup.put(newSyncRule("rule1"))

	rec, err := r.Trigger(context.Background(), "rule1", false)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	// Give the executor a moment to register the task with its internal
	// cancel map.
	time.Sleep(50 * time.Millisecond)
	if err := r.Cancel(context.Background(), rec.TaskID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	waitForStatus(t, store, rec.TaskID, executor.StatusCancelled, 3*time.Second)
}
