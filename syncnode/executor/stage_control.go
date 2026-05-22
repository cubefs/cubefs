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
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/barrier"
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// Package-level barrier used by all bench paths (s3 / posix / mdtest).
// Set once at boot via SetBarrier; getBarrier always returns a usable
// implementation (falls back to a process-local MemBarrier) so callers
// never need to nil-check.
var (
	pkgBarrier atomic.Value // holds barrier.Barrier
)

// SetBarrier installs the cross-shard barrier used by every bench stage
// that opts in via StageControl.WaitForPeers. Pass nil to revert to the
// in-memory fallback (mostly useful from tests).
func SetBarrier(b barrier.Barrier) {
	if b == nil {
		pkgBarrier.Store(barrierBox{b: barrier.NewMemBarrier(1)})
		return
	}
	pkgBarrier.Store(barrierBox{b: b})
}

// barrierBox lets us atomically swap a typed interface value. Storing
// interface values directly into atomic.Value panics on type mismatch
// across swaps, so we wrap the interface in a concrete struct.
type barrierBox struct{ b barrier.Barrier }

// getBarrier returns the configured Barrier (or the MemBarrier fallback
// if no operator wired Consul). Never returns nil.
func getBarrier() barrier.Barrier {
	v := pkgBarrier.Load()
	if v == nil {
		// Lazy default: tests that never call SetBarrier still get a
		// working in-process barrier.
		mb := barrier.NewMemBarrier(1)
		pkgBarrier.Store(barrierBox{b: mb})
		return mb
	}
	return v.(barrierBox).b
}

// waitForPeers calls Barrier.Ready when stage.WaitForPeers is set,
// otherwise returns immediately. On timeout the executor logs and
// continues — the barrier is a coordination hint, not a hard gate. On
// context cancel the error propagates so the stage aborts cleanly.
func waitForPeers(ctx context.Context, taskID, stage, shardID string, shardTotal int, ctrl spec.StageControl) error {
	if !ctrl.WaitForPeers {
		return nil
	}
	if shardTotal <= 1 {
		// Solo task — no peers to wait for; skip the round-trip.
		return nil
	}
	timeout := time.Duration(ctrl.BarrierTimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	err := getBarrier().Ready(ctx, taskID, stage, shardID, shardTotal, timeout)
	if err != nil {
		// Don't promote barrier errors to stage failure: a degraded
		// cluster should still produce best-effort measurements. Caller
		// only sees an error when the surrounding context is cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.LogWarnf("bench barrier [task=%s stage=%s shard=%s shards=%d]: %v — continuing without sync",
			taskID, stage, shardID, shardTotal, err)
		return nil
	}
	return nil
}

// rampSchedule is the precomputed timeline of a single bench stage.
// Total = RampUp + Steady + RampDown. The driver goroutine walks this
// timeline retargeting the Limiter as it goes.
type rampSchedule struct {
	rampUp   time.Duration
	steady   time.Duration
	rampDown time.Duration
	target   float64 // ops/sec target during the steady window
}

// computeRampSchedule turns StageControl + the resolved target rate into
// a concrete schedule. If the stage didn't request a ramp the returned
// schedule has zero RampUp/RampDown and uses Steady for the entire run
// (caller computes Steady from stage.Runtime in that case).
func computeRampSchedule(ctrl spec.StageControl, targetPerSec float64, fallbackSteady time.Duration) rampSchedule {
	if !ctrl.HasRampSchedule() {
		return rampSchedule{steady: fallbackSteady, target: targetPerSec}
	}
	return rampSchedule{
		rampUp:   time.Duration(ctrl.RampUpSec) * time.Second,
		steady:   time.Duration(ctrl.SteadySec) * time.Second,
		rampDown: time.Duration(ctrl.RampDownSec) * time.Second,
		target:   targetPerSec,
	}
}

// totalDuration is the wall-clock budget for the entire schedule.
func (s rampSchedule) totalDuration() time.Duration {
	return s.rampUp + s.steady + s.rampDown
}

// runRampDriver walks the schedule and retargets lim accordingly. Returns
// when (a) the schedule completes, (b) ctx is cancelled, or (c) done is
// closed (caller decided to stop early — e.g. NumObjects cap reached).
//
// During RampUp the limit linearly rises 0 → target.
// During Steady the limit is held at target.
// During RampDown the limit linearly falls target → 0.
//
// We tick every 100ms; finer granularity buys no smoothing because the
// underlying rate.Limiter already smooths within its burst window.
func runRampDriver(ctx context.Context, lim Limiter, sched rampSchedule, done <-chan struct{}) {
	if sched.target <= 0 {
		// Nothing to ramp — degenerate case. The Wait calls will all
		// pass through unlimitedLimiter, but the caller still relies on
		// our blocking until done to honour the steady window.
		select {
		case <-ctx.Done():
		case <-done:
		case <-time.After(sched.totalDuration()):
		}
		return
	}

	tick := 100 * time.Millisecond
	t0 := time.Now()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			lim.SetLimit(0)
			return
		case <-done:
			lim.SetLimit(0)
			return
		case now := <-ticker.C:
			elapsed := now.Sub(t0)
			if elapsed >= sched.totalDuration() {
				lim.SetLimit(0)
				return
			}
			lim.SetLimit(currentRate(sched, elapsed))
		}
	}
}

// currentRate returns the instantaneous target rate at elapsed time t
// within the schedule.
func currentRate(s rampSchedule, t time.Duration) float64 {
	if t < s.rampUp {
		if s.rampUp == 0 {
			return s.target
		}
		return s.target * float64(t) / float64(s.rampUp)
	}
	t -= s.rampUp
	if t < s.steady {
		return s.target
	}
	t -= s.steady
	if t < s.rampDown {
		if s.rampDown == 0 {
			return 0
		}
		return s.target * (1.0 - float64(t)/float64(s.rampDown))
	}
	return 0
}
