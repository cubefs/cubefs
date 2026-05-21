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

package ratelimit

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// design.md §9 Phase G-2 acceptance criteria, scaled down so each scenario
// finishes in a few seconds:
//
//   (a) only node bucket configured           → throughput ≈ node rate
//   (b) only per-task bucket configured       → throughput ≈ task rate
//   (c) node + task + backend all configured  → throughput ≈ min(three)
//
// Each test is gated on -short.

// runScenario streams payload bytes through a LimitedReader built over
// Composite(layers...), returning the measured rate in MB/s.
func runScenario(t *testing.T, payload int, layers []Limiter) float64 {
	t.Helper()
	lr := NewLimitedReader(context.Background(), readerOf(payload), NewComposite(layers...))
	start := time.Now()
	n, err := io.Copy(io.Discard, lr)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	if int(n) != payload {
		t.Fatalf("copied %d, want %d", n, payload)
	}
	mb := float64(n) / 1024.0 / 1024.0
	return mb / elapsed.Seconds()
}

// expectedThroughput models the observed MB/s when streaming payload bytes
// through a token bucket of rate r and burst b. The first b bytes are
// "free" (paid from the prefilled bucket), the remaining (payload-b) flow
// at r; elapsed = (payload-b)/r, throughput = payload / elapsed.
func expectedThroughput(payloadBytes int, rateMBps int) float64 {
	rl := float64(rateMBps) * 1024 * 1024 // bytes / s
	burst := rl                            // 1 second of bandwidth
	if burst < minBurstBytes {
		burst = minBurstBytes
	}
	if float64(payloadBytes) <= burst {
		return float64(payloadBytes) // anything up to burst is "instant"
	}
	elapsedSec := (float64(payloadBytes) - burst) / rl
	return float64(payloadBytes) / 1024.0 / 1024.0 / elapsedSec
}

// withinPct verifies actual is within tolerance pct of expected. tolerance
// is generous (±25 %) because wall-clock CI environments are noisy.
func withinPct(t *testing.T, label string, actual, expected, pct float64) {
	t.Helper()
	lo := expected * (1 - pct)
	hi := expected * (1 + pct)
	if actual < lo || actual > hi {
		t.Errorf("%s: throughput %.2f MB/s, want %.2f±%.0f%% [%0.2f, %0.2f]",
			label, actual, expected, pct*100, lo, hi)
	}
}

// TestAcceptance_NodeOnly — AC (a). Configure only the node bucket; the
// observed rate must track it within tolerance after accounting for the
// initial burst.
func TestAcceptance_NodeOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance test is wall-clock; skipped in -short")
	}
	t.Parallel()

	const (
		nodeMBps = 16
		payload  = 80 * 1024 * 1024 // 80 MiB → ~4.5 s at 16 MB/s after burst
	)
	reg := NewRegistry(nodeMBps)
	rate := runScenario(t, payload, []Limiter{reg.NodeBucket()})
	withinPct(t, "node-only", rate, expectedThroughput(payload, nodeMBps), 0.20)
}

// TestAcceptance_TaskOnly — AC (b).
func TestAcceptance_TaskOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance test is wall-clock; skipped in -short")
	}
	t.Parallel()

	const (
		taskMBps = 8
		payload  = 40 * 1024 * 1024
	)
	taskBucket := NewBucket(taskMBps)
	rate := runScenario(t, payload, []Limiter{taskBucket})
	withinPct(t, "task-only", rate, expectedThroughput(payload, taskMBps), 0.20)
}

// TestAcceptance_AllThreeMinWins — AC (c). With node=32, task=8 and
// backend=16 the effective rate must hug the task bucket (the smallest).
// We compare to the task-rate's expected throughput because that bucket
// dominates and its burst sets the freebie.
func TestAcceptance_AllThreeMinWins(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance test is wall-clock; skipped in -short")
	}
	t.Parallel()

	const (
		nodeMBps    = 32
		backendMBps = 16
		taskMBps    = 8
		payload     = 40 * 1024 * 1024
	)
	reg := NewRegistry(nodeMBps)
	bk := BackendKey{Kind: "s3", Endpoint: "ep-c", Region: "r"}
	reg.SetBackendLimit(bk, backendMBps)

	layers := []Limiter{NewBucket(taskMBps), reg.NodeBucket(), reg.BackendBucket(bk)}
	rate := runScenario(t, payload, layers)
	withinPct(t, "all-three", rate, expectedThroughput(payload, taskMBps), 0.20)
}

// TestAcceptance_MultiTaskShareNode — sibling test exercising the
// "multiple tasks share one node bucket" scenario at the same scale: two
// parallel readers, each with a generous task cap, against a tight node
// cap. The aggregate throughput across the two must still hug the node
// rate, not 2× it.
func TestAcceptance_MultiTaskShareNode(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance test is wall-clock; skipped in -short")
	}
	t.Parallel()

	const (
		nodeMBps = 16
		taskMBps = 200 // effectively unlimited at this payload
		payload  = 40 * 1024 * 1024
	)
	reg := NewRegistry(nodeMBps)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		totalNS int64
		totalMB float64
	)
	start := time.Now()
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			layers := []Limiter{NewBucket(taskMBps), reg.NodeBucket()}
			lr := NewLimitedReader(context.Background(), readerOf(payload), NewComposite(layers...))
			n, err := io.Copy(io.Discard, lr)
			if err != nil {
				t.Errorf("copy: %v", err)
				return
			}
			mu.Lock()
			totalMB += float64(n) / 1024.0 / 1024.0
			mu.Unlock()
		}()
	}
	wg.Wait()
	totalNS = time.Since(start).Nanoseconds()

	aggregate := totalMB / (float64(totalNS) / 1e9)
	// Both tasks share one node bucket of 16 MB/s. Aggregate throughput
	// over total wall clock = (2*payload - burst) / time, but we measure
	// it as totalMB / total elapsed which is exactly the aggregate rate
	// out of the node bucket. The burst contributes 2*16 MiB = 32 MiB of
	// freebies (or so — close enough), so expected ≈ node-rate after the
	// initial sprint. We use the same model on 2x payload to capture that.
	withinPct(t, "multi-task-share-node", aggregate, expectedThroughput(2*payload, nodeMBps), 0.30)
}
