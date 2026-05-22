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

package barrier

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestMemBarrier_AllShardsReady covers the happy path: three shards each
// announce readiness for the same (taskID, stage), and every Ready call
// returns nil promptly. We rely on the in-memory barrier exclusively here —
// a real Consul integration test requires an external agent and is left to
// staging.
func TestMemBarrier_AllShardsReady(t *testing.T) {
	mb := NewMemBarrier(3)

	const (
		taskID       = "task-1"
		stage        = "warmup"
		expectShards = 3
		timeout      = 2 * time.Second
	)

	var wg sync.WaitGroup
	errs := make([]error, 3)
	start := time.Now()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		shardID := []string{"a", "b", "c"}[i]
		idx := i
		go func() {
			defer wg.Done()
			errs[idx] = mb.Ready(context.Background(), taskID, stage, shardID, expectShards, timeout)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	for i, err := range errs {
		if err != nil {
			t.Fatalf("shard %d Ready returned %v, want nil", i, err)
		}
	}
	// Should resolve well under the 2s timeout.
	if elapsed > 500*time.Millisecond {
		t.Errorf("3-shard barrier took %v; expected near-instant resolution", elapsed)
	}
}

// TestMemBarrier_TimeoutLeavesPeersUnblocked covers the partial-cluster path:
// 2 of 3 shards arrive and 1 never does. The two arrivals must still block
// (they need 3) and ultimately return ErrBarrierTimeout, but they MUST NOT
// hang past the configured timeout. This is the property the executor relies
// on to keep partial clusters limping forward.
func TestMemBarrier_TimeoutLeavesPeersUnblocked(t *testing.T) {
	mb := NewMemBarrier(3)

	const (
		taskID       = "task-2"
		stage        = "stage-a"
		expectShards = 3
		// Short timeout so the test stays fast; long enough that
		// scheduling jitter cannot trip the elapsed-time assertion.
		timeout = 200 * time.Millisecond
	)

	var wg sync.WaitGroup
	errs := make([]error, 2)

	start := time.Now()
	for i := 0; i < 2; i++ {
		wg.Add(1)
		shardID := []string{"a", "b"}[i]
		idx := i
		go func() {
			defer wg.Done()
			errs[idx] = mb.Ready(context.Background(), taskID, stage, shardID, expectShards, timeout)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	for i, err := range errs {
		if !errors.Is(err, ErrBarrierTimeout) {
			t.Errorf("shard %d returned %v, want ErrBarrierTimeout", i, err)
		}
	}
	// Must not block significantly past the configured timeout.
	if elapsed > timeout*3 {
		t.Errorf("barrier blocked %v past timeout %v", elapsed, timeout)
	}
}

// TestMemBarrier_SoloShortcut covers expectShards<=1: Ready must return
// nil without waiting at all. Used by the executor when a bench task has
// only one shard.
func TestMemBarrier_SoloShortcut(t *testing.T) {
	mb := NewMemBarrier(1)
	start := time.Now()
	if err := mb.Ready(context.Background(), "task-3", "stage", "a", 1, time.Second); err != nil {
		t.Fatalf("solo Ready returned %v, want nil", err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Error("solo Ready blocked unexpectedly")
	}
}

// TestMemBarrier_FreshStateAcrossStages verifies that the same (taskID)
// across DIFFERENT stages doesn't leak readiness. After stage-1 fully
// resolves, stage-2 must require its own 3 calls before unblocking.
func TestMemBarrier_FreshStateAcrossStages(t *testing.T) {
	mb := NewMemBarrier(3)
	taskID := "task-4"
	timeout := time.Second

	// Stage 1: all three resolve.
	var wg sync.WaitGroup
	for _, s := range []string{"a", "b", "c"} {
		wg.Add(1)
		shard := s
		go func() {
			defer wg.Done()
			_ = mb.Ready(context.Background(), taskID, "s1", shard, 3, timeout)
		}()
	}
	wg.Wait()

	// Stage 2: only one shard registers, must time out — proves stage-1
	// state didn't leak into stage-2.
	err := mb.Ready(context.Background(), taskID, "s2", "a", 3, 150*time.Millisecond)
	if !errors.Is(err, ErrBarrierTimeout) {
		t.Errorf("stage-2 with 1 shard returned %v, want ErrBarrierTimeout", err)
	}
}
