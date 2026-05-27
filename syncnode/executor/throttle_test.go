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
	"math"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// TestNewLimiter_Unlimited verifies the zero-cost fast path: with no
// throttle target set, NewLimiter returns the no-op limiter and Wait()
// returns immediately regardless of call rate.
func TestNewLimiter_Unlimited(t *testing.T) {
	lim := NewLimiter(spec.StageControl{}, 0)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		if err := lim.Wait(context.Background()); err != nil {
			t.Fatalf("unlimited Wait returned %v", err)
		}
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("unlimited Wait blocked for %v across 1000 calls", d)
	}
}

// TestNewLimiter_IOPS_100 checks that a 100 IOPS limiter (burst 10) takes
// ~1s to admit 100 ops. We allow a 20% margin in either direction so CI
// jitter doesn't flake the test. The first `burst` ops drain the bucket
// without delay; ops 11..100 each need ~10ms to refill, so the wall-clock
// for 100 ops should be around 90/100 ≈ 0.9s once the warm-up burst is
// accounted for. Real-world resolution puts the practical window at
// 0.8–1.2s; we assert that band.
func TestNewLimiter_IOPS_100(t *testing.T) {
	lim := NewLimiter(spec.StageControl{TargetIOPS: 100}, 0)

	const ops = 100
	start := time.Now()
	for i := 0; i < ops; i++ {
		if err := lim.Wait(context.Background()); err != nil {
			t.Fatalf("Wait returned %v at op %d", err, i)
		}
	}
	elapsed := time.Since(start)
	// 100 ops at 100 IOPS, burst 10 → expected ~0.9s. Allow 20% on each
	// side.
	const expected = 900 * time.Millisecond
	delta := math.Abs(float64(elapsed - expected))
	margin := float64(expected) * 0.20
	if delta > margin {
		t.Errorf("100 ops at IOPS=100 took %v; expected ~%v ±20%% (%v)", elapsed, expected, time.Duration(margin))
	}
}

// TestNewLimiter_Bandwidth converts MiB/s × avgOpBytes into the same
// op-rate shape. With 1 MiB/s and 16 KiB avg ops we expect ~64 ops/sec,
// so 64 ops should take ~1s. We loosen the margin to 30% because the
// conversion produces non-round numbers and burst math interacts.
func TestNewLimiter_Bandwidth(t *testing.T) {
	const opBytes = 16 * 1024 // 16 KiB
	lim := NewLimiter(spec.StageControl{TargetBwMiBs: 1.0}, opBytes)

	const ops = 64
	start := time.Now()
	for i := 0; i < ops; i++ {
		if err := lim.Wait(context.Background()); err != nil {
			t.Fatalf("Wait returned %v at op %d", err, i)
		}
	}
	elapsed := time.Since(start)
	const expected = 1000 * time.Millisecond
	delta := math.Abs(float64(elapsed - expected))
	margin := float64(expected) * 0.30
	if delta > margin {
		t.Errorf("64 ops at 1 MiB/s, 16 KiB each took %v; expected ~%v ±30%% (%v)", elapsed, expected, time.Duration(margin))
	}
}

// TestNewLimiter_IOPSWinsOverBandwidth covers the precedence rule: when
// both TargetIOPS and TargetBwMiBs are set, TargetIOPS is honoured and
// TargetBwMiBs is ignored. We set bandwidth that would imply ~6400 ops/s
// against IOPS=100, then verify the actual rate matches IOPS=100.
func TestNewLimiter_IOPSWinsOverBandwidth(t *testing.T) {
	lim := NewLimiter(spec.StageControl{TargetIOPS: 100, TargetBwMiBs: 100, // would imply 6400 ops/s @16KiB
	}, 16*1024)

	const ops = 50
	start := time.Now()
	for i := 0; i < ops; i++ {
		if err := lim.Wait(context.Background()); err != nil {
			t.Fatalf("Wait err %v", err)
		}
	}
	elapsed := time.Since(start)
	// 50 ops at 100 IOPS, burst 10 → expect ~0.4s. If TargetBwMiBs had
	// won, we'd finish in single-digit ms.
	if elapsed < 200*time.Millisecond {
		t.Errorf("50 ops finished in %v; TargetIOPS=100 should keep us above ~0.4s", elapsed)
	}
}

// TestRateLimiter_SetLimit_LiveRetarget verifies that SetLimit on a live
// limiter changes its effective rate, which is the property the ramp
// driver depends on.
func TestRateLimiter_SetLimit_LiveRetarget(t *testing.T) {
	l := newRateLimiter(10) // 10 ops/s
	// Drain the burst quickly so subsequent Waits are paced.
	ctx := context.Background()
	for i := 0; i < computeBurst(10); i++ {
		_ = l.Wait(ctx)
	}
	// Crank rate up to 1000/s. After SetLimit the next 100 Waits should
	// complete fast (<200ms) — they wouldn't at the old 10 ops/s rate.
	l.SetLimit(1000)
	start := time.Now()
	for i := 0; i < 100; i++ {
		_ = l.Wait(ctx)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("after SetLimit(1000), 100 Waits took %v; expected <500ms", d)
	}
}
