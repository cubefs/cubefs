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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// fakeSoakStore is an in-memory SoakStore mock used by RunSoakLoop tests.
// Concurrent safe: the runner Save's from a goroutine.
type fakeSoakStore struct {
	mu      sync.Mutex
	saves   []SoakCheckpoint
	deletes int
	seed    map[string]SoakCheckpoint // returned by Load
}

func newFakeSoakStore() *fakeSoakStore {
	return &fakeSoakStore{seed: make(map[string]SoakCheckpoint)}
}

func fakeKey(taskID, stage string, shard int) string {
	return fmt.Sprintf("%s/%s/%d", taskID, stage, shard)
}

func (s *fakeSoakStore) Save(_ context.Context, cp SoakCheckpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves = append(s.saves, cp)
	return nil
}

func (s *fakeSoakStore) Load(_ context.Context, taskID, stage string, shard int) (*SoakCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.seed[fakeKey(taskID, stage, shard)]
	if !ok {
		return nil, nil
	}
	out := cp
	return &out, nil
}

func (s *fakeSoakStore) Delete(_ context.Context, _, _ string, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	return nil
}

func (s *fakeSoakStore) ListByTask(_ context.Context, _ string) ([]SoakCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SoakCheckpoint, 0, len(s.saves))
	out = append(out, s.saves...)
	return out, nil
}

func (s *fakeSoakStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.saves)
}

func (s *fakeSoakStore) lastSave() (SoakCheckpoint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.saves) == 0 {
		return SoakCheckpoint{}, false
	}
	return s.saves[len(s.saves)-1], true
}

// TestSoakRunner_CheckpointTickFires drives a 2s soak with a 1s checkpoint
// interval. Expect ≥1 Save call and the loop to exit cleanly within ~3s.
func TestSoakRunner_CheckpointTickFires(t *testing.T) {
	store := newFakeSoakStore()
	stageRan := make(chan struct{}, 1)
	stage := func(ctx context.Context, restartCount int) error {
		select {
		case stageRan <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	opts := SoakLoopOptions{
		Control: spec.SoakControl{DurationSec: 2, CheckpointInterval: 1},
		Store:   store,
		TaskID:  "t1",
		Stage:   "s",
		ShardID: 0,
	}
	start := time.Now()
	err := RunSoakLoop(context.Background(), opts, stage)
	elapsed := time.Since(start)
	if err != nil {
		// outer deadline returns nil; stage's ctx.Err() should be eaten.
		t.Fatalf("RunSoakLoop: %v", err)
	}
	if elapsed < 1500*time.Millisecond || elapsed > 4*time.Second {
		t.Errorf("elapsed = %v; want ~2s", elapsed)
	}
	if store.saveCount() < 1 {
		t.Errorf("expected >=1 Save calls, got %d", store.saveCount())
	}
	// Stage was invoked at least once.
	select {
	case <-stageRan:
	default:
		t.Error("stage callback was never invoked")
	}
}

// TestSoakRunner_RestartExhaustion: stage returns a non-context error every
// time. With MaxRestartCount=3 we expect 4 stage invocations (1 initial + 3
// restarts), then loop terminates with a wrapped error.
func TestSoakRunner_RestartExhaustion(t *testing.T) {
	// Shorten the back-off so the test finishes quickly.
	prevSleep := soakSleepBetweenRestarts
	soakSleepBetweenRestarts = 10 * time.Millisecond
	defer func() { soakSleepBetweenRestarts = prevSleep }()

	store := newFakeSoakStore()
	var invocations int32
	sentinel := errors.New("stage boom")
	stage := func(_ context.Context, restartCount int) error {
		atomic.AddInt32(&invocations, 1)
		// Stage returns immediately with the sentinel error.
		return sentinel
	}
	opts := SoakLoopOptions{
		// Give the loop a generous DurationSec so the restart cap is what
		// terminates the loop, not the deadline.
		Control: spec.SoakControl{DurationSec: 60, CheckpointInterval: 30, MaxRestartCount: 3},
		Store:   store,
		TaskID:  "t1",
		Stage:   "s",
		ShardID: 0,
	}
	err := RunSoakLoop(context.Background(), opts, stage)
	if err == nil {
		t.Fatalf("expected error after restart exhaustion, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel error, got %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 4 {
		t.Errorf("expected 4 invocations (1 initial + 3 restart), got %d", got)
	}
}

// TestSoakRunner_ResumeFromCheckpoint: pre-seed the store with a checkpoint;
// when ResumeFromCheckpoint=true the first stage attempt must observe the
// resume info via SoakResumeFromContext.
func TestSoakRunner_ResumeFromCheckpoint(t *testing.T) {
	store := newFakeSoakStore()
	store.seed[fakeKey("task-r", "warm", 7)] = SoakCheckpoint{
		TaskID:       "task-r",
		Stage:        "warm",
		ShardID:      7,
		ElapsedSec:   42,
		OpsCompleted: 1234,
		RestartCount: 2,
		Snapshot:     []byte("snap"),
	}

	var observed SoakResume
	var observedOK bool
	stage := func(ctx context.Context, _ int) error {
		v, ok := SoakResumeFromContext(ctx)
		observed = v
		observedOK = ok
		// Return nil — stage "finishes" immediately so the loop exits.
		return nil
	}

	opts := SoakLoopOptions{
		Control: spec.SoakControl{
			DurationSec:          120,
			CheckpointInterval:   60,
			ResumeFromCheckpoint: true,
		},
		Store:   store,
		TaskID:  "task-r",
		Stage:   "warm",
		ShardID: 7,
	}
	if err := RunSoakLoop(context.Background(), opts, stage); err != nil {
		t.Fatalf("RunSoakLoop: %v", err)
	}
	if !observedOK {
		t.Fatal("stage did not observe resume info on ctx")
	}
	if observed.ElapsedSec != 42 {
		t.Errorf("observed.ElapsedSec = %d, want 42", observed.ElapsedSec)
	}
	if observed.OpsCompleted != 1234 {
		t.Errorf("observed.OpsCompleted = %d, want 1234", observed.OpsCompleted)
	}
	if observed.RestartCount != 2 {
		t.Errorf("observed.RestartCount = %d, want 2", observed.RestartCount)
	}
	if string(observed.Snapshot) != "snap" {
		t.Errorf("observed.Snapshot = %q, want snap", observed.Snapshot)
	}
}

// TestSoakRunner_DisabledIsNoop: with Soak disabled (DurationSec=0), the
// wrapSoakIfEnabled helper returns the stage callback unchanged.
func TestSoakRunner_DisabledIsNoop(t *testing.T) {
	store := newFakeSoakStore()
	var called int
	stage := func(_ context.Context, _ int) error {
		called++
		return nil
	}
	opts := SoakLoopOptions{
		Control: spec.SoakControl{DurationSec: 0}, // disabled
		Store:   store,
		TaskID:  "t",
		Stage:   "s",
	}
	wrapped := wrapSoakIfEnabled(opts, stage)
	if err := wrapped(context.Background(), 0); err != nil {
		t.Fatalf("wrapped stage err: %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 stage invocation (no wrapping), got %d", called)
	}
	if store.saveCount() != 0 {
		t.Errorf("expected 0 Save (disabled), got %d", store.saveCount())
	}
}

// TestSoakRunner_StageReturnsNilExitsImmediately: when stage returns nil on
// the first attempt, the loop exits successfully without waiting for the
// outer deadline.
func TestSoakRunner_StageReturnsNilExitsImmediately(t *testing.T) {
	store := newFakeSoakStore()
	stage := func(_ context.Context, _ int) error { return nil }
	opts := SoakLoopOptions{
		Control: spec.SoakControl{DurationSec: 60, CheckpointInterval: 1},
		Store:   store,
		TaskID:  "t",
		Stage:   "s",
	}
	start := time.Now()
	if err := RunSoakLoop(context.Background(), opts, stage); err != nil {
		t.Fatalf("RunSoakLoop: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("loop took %v; should have exited promptly", elapsed)
	}
}

// TestSoakRunner_CtxCancelPropagates: caller cancels the parent ctx mid-soak;
// the loop returns ctx.Canceled.
func TestSoakRunner_CtxCancelPropagates(t *testing.T) {
	store := newFakeSoakStore()
	stage := func(ctx context.Context, _ int) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	opts := SoakLoopOptions{
		Control: spec.SoakControl{DurationSec: 60, CheckpointInterval: 30},
		Store:   store,
		TaskID:  "t",
		Stage:   "s",
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunSoakLoop(ctx, opts, stage)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected ctx.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunSoakLoop did not return after cancel")
	}
}

// TestSoakRunner_RejectsInvalidOpts ensures the runner refuses obviously-broken
// configurations rather than silently looping forever.
func TestSoakRunner_RejectsInvalidOpts(t *testing.T) {
	store := newFakeSoakStore()
	stage := func(_ context.Context, _ int) error { return nil }

	cases := []struct {
		name string
		opts SoakLoopOptions
	}{
		{"disabled control", SoakLoopOptions{Store: store, TaskID: "t", Stage: "s"}},
		{"nil store", SoakLoopOptions{Control: spec.SoakControl{DurationSec: 1}, TaskID: "t", Stage: "s"}},
		{"empty taskID", SoakLoopOptions{Control: spec.SoakControl{DurationSec: 1}, Store: store, Stage: "s"}},
		{"empty stage", SoakLoopOptions{Control: spec.SoakControl{DurationSec: 1}, Store: store, TaskID: "t"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := RunSoakLoop(context.Background(), tc.opts, stage); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}

	// Nil stage callback
	if err := RunSoakLoop(context.Background(),
		SoakLoopOptions{Control: spec.SoakControl{DurationSec: 1}, Store: store, TaskID: "t", Stage: "s"},
		nil); err == nil {
		t.Error("nil stage: expected error")
	}
}

// TestSoakRunner_DefaultCheckpointInterval: when CheckpointInterval is left
// 0, the helper falls back to 60s. Verified at the SoakControl level.
func TestSoakRunner_DefaultCheckpointInterval(t *testing.T) {
	c := spec.SoakControl{DurationSec: 10}
	if got := c.EffectiveCheckpointIntervalSec(); got != 60 {
		t.Errorf("default checkpoint interval = %d, want 60", got)
	}
	c.CheckpointInterval = 5
	if got := c.EffectiveCheckpointIntervalSec(); got != 5 {
		t.Errorf("explicit interval = %d, want 5", got)
	}
}

// TestSoakRunner_OpsCompletedPropagatesToCheckpoint exercises the
// OpsCompletedFn hook by feeding a counter through it and asserting the
// last saved checkpoint reflects the latest value.
func TestSoakRunner_OpsCompletedPropagatesToCheckpoint(t *testing.T) {
	store := newFakeSoakStore()
	var ops int64
	stage := func(ctx context.Context, _ int) error {
		// Bump the counter while the soak loop runs; the checkpoint ticker
		// will pick up the latest value.
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
				atomic.AddInt64(&ops, 100)
			}
		}
	}
	opts := SoakLoopOptions{
		Control:        spec.SoakControl{DurationSec: 2, CheckpointInterval: 1},
		Store:          store,
		TaskID:         "t",
		Stage:          "s",
		OpsCompletedFn: func() int64 { return atomic.LoadInt64(&ops) },
	}
	if err := RunSoakLoop(context.Background(), opts, stage); err != nil {
		t.Fatalf("RunSoakLoop: %v", err)
	}
	cp, ok := store.lastSave()
	if !ok {
		t.Fatal("no Save calls recorded")
	}
	if cp.OpsCompleted <= 0 {
		t.Errorf("OpsCompleted not propagated; got %d", cp.OpsCompleted)
	}
}
