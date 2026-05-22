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
	"sync"

	"github.com/cubefs/cubefs/syncnode/spec"
	"golang.org/x/time/rate"
)

// Limiter is the bench-stage rate shaper. One Wait() call corresponds to
// one bench op (PUT/GET/HEAD/DELETE/...). The implementation MUST be safe
// for concurrent use — every worker goroutine in an obj stage shares one
// Limiter.
//
// Two production implementations:
//
//   - unlimitedLimiter: zero-allocation no-op. Returned when neither
//     TargetIOPS nor TargetBwMiBs is configured. Keeps the unthrottled
//     fast path branch-free.
//   - rateLimiter: thin wrapper around golang.org/x/time/rate that lets
//     the stage-control driver (stage_control.go) call SetLimit() to
//     ramp the target rate over time.
type Limiter interface {
	Wait(ctx context.Context) error
	// SetLimit dynamically retargets the per-second rate. The
	// unlimited implementation ignores this. Used by the ramp driver
	// to walk the rate from 0 → target → 0.
	SetLimit(perSec float64)
}

// unlimitedLimiter is the no-op fast path. Returned by NewLimiter when the
// stage requested no rate shaping. Wait() never allocates and never blocks.
type unlimitedLimiter struct{}

func (unlimitedLimiter) Wait(_ context.Context) error { return nil }
func (unlimitedLimiter) SetLimit(_ float64)           {}

// rateLimiter wraps a golang.org/x/time/rate.Limiter so the stage controller
// can SetLimit() without callers ever holding the rate package type
// directly.
//
// The embedded sync.Mutex guards SetLimit's read of the current state when
// computing the new burst. rate.Limiter is itself goroutine-safe for
// Wait()/Allow()/SetLimit(), so the mutex is only here to keep "compute
// new burst then apply" atomic.
type rateLimiter struct {
	mu  sync.Mutex
	rl  *rate.Limiter
	cur float64
}

func newRateLimiter(perSec float64) *rateLimiter {
	if perSec < 0 {
		perSec = 0
	}
	burst := computeBurst(perSec)
	return &rateLimiter{
		rl:  rate.NewLimiter(rate.Limit(perSec), burst),
		cur: perSec,
	}
}

func (l *rateLimiter) Wait(ctx context.Context) error {
	return l.rl.Wait(ctx)
}

func (l *rateLimiter) SetLimit(perSec float64) {
	if perSec < 0 {
		perSec = 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if perSec == l.cur {
		return
	}
	l.cur = perSec
	l.rl.SetLimit(rate.Limit(perSec))
	l.rl.SetBurst(computeBurst(perSec))
}

// computeBurst sizes the token bucket. Rule of thumb: 1/10s of capacity,
// minimum 1, capped at 1000 — large bursts let a slow consumer drain a
// big backlog at once, defeating the smoothing the ramp is trying to do.
func computeBurst(perSec float64) int {
	if perSec <= 0 {
		return 1
	}
	b := int(perSec / 10)
	if b < 1 {
		b = 1
	}
	if b > 1000 {
		b = 1000
	}
	return b
}

// NewLimiter constructs the right Limiter for a StageControl spec.
//
// Selection logic (mirrors StageControl doc):
//   - Neither target set → unlimitedLimiter (no-op).
//   - TargetIOPS > 0 → token bucket sized at IOPS ops/sec. TargetBwMiBs
//     is ignored when both are non-zero (TargetIOPS wins).
//   - TargetIOPS == 0 && TargetBwMiBs > 0 → convert to ops/sec via
//     avgOpBytes: rate = (TargetBwMiBs * 1MiB) / avgOpBytes. avgOpBytes
//     must be > 0 for this branch to engage; the executor passes a sane
//     fallback (e.g. ObjSize.Fixed or 4 KiB) when the stage size is
//     dynamic.
//
// Returns an unlimitedLimiter on any pathological input (zero rate,
// invalid avgOpBytes) so the caller is guaranteed a usable handle.
func NewLimiter(c spec.StageControl, avgOpBytes int) Limiter {
	if !c.HasThrottle() {
		return unlimitedLimiter{}
	}
	if c.TargetIOPS > 0 {
		return newRateLimiter(float64(c.TargetIOPS))
	}
	if c.TargetBwMiBs > 0 && avgOpBytes > 0 {
		opsPerSec := (c.TargetBwMiBs * 1024 * 1024) / float64(avgOpBytes)
		if opsPerSec > 0 {
			return newRateLimiter(opsPerSec)
		}
	}
	return unlimitedLimiter{}
}
