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

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
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
func (b *emptyBackend) Put(ctx context.Context, k string, body io.Reader, sz int64, opts backend.PutOptions) (string, error) {
	return "", nil
}
func (b *emptyBackend) Delete(ctx context.Context, k string) error    { return nil }
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
