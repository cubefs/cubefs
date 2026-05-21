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
	"sync"
	"time"
)

const (
	// bwWindow is the rolling window over which current bandwidth is computed.
	bwWindow = 30 * time.Second
	// bwCap is the ring buffer capacity. At the default 2s progress interval
	// this covers 64s of history — more than twice the window to ensure the
	// oldest sample within the window is always present.
	bwCap = 64
)

type bwSample struct {
	t     time.Time
	bytes int64 // cumulative bytes transferred at this instant
}

// bandwidthTracker computes a rolling-window current-bandwidth estimate.
// It maintains a fixed-capacity ring buffer of periodic cumulative-byte
// samples. Safe for concurrent use.
type bandwidthTracker struct {
	mu  sync.Mutex
	buf [bwCap]bwSample
	pos int // next write position (ring)
	n   int // number of valid samples (≤ bwCap)
}

// record adds the current cumulative byte count to the ring buffer.
// Called by the progress ticker goroutine each interval.
func (t *bandwidthTracker) record(bytes int64) {
	now := time.Now()
	t.mu.Lock()
	t.buf[t.pos] = bwSample{t: now, bytes: bytes}
	t.pos = (t.pos + 1) % bwCap
	if t.n < bwCap {
		t.n++
	}
	t.mu.Unlock()
}

// currentMBps returns the average transfer rate (MB/s) over samples
// within the last bwWindow. Returns 0 when there are fewer than 2 samples
// or the elapsed time between the oldest and newest window sample is under
// 500 ms (avoids division-by-near-zero noise at task start).
func (t *bandwidthTracker) currentMBps() float64 {
	t.mu.Lock()
	n := t.n
	pos := t.pos
	buf := t.buf // value copy while holding lock
	t.mu.Unlock()

	if n < 2 {
		return 0
	}

	// Reconstruct oldest-first ordering from the ring buffer.
	// When n < bwCap: samples are in buf[0..n-1], buf[0] is oldest.
	// When n == bwCap: oldest is at buf[pos], newest at buf[(pos-1+bwCap)%bwCap].
	ordered := make([]bwSample, n)
	if n < bwCap {
		for i := 0; i < n; i++ {
			ordered[i] = buf[i]
		}
	} else {
		for i := 0; i < n; i++ {
			ordered[i] = buf[(pos+i)%bwCap]
		}
	}

	newest := ordered[n-1]
	cutoff := newest.t.Add(-bwWindow)

	// Advance past samples that fall outside the window.
	oldestIdx := 0
	for oldestIdx < n-1 && ordered[oldestIdx].t.Before(cutoff) {
		oldestIdx++
	}
	oldest := ordered[oldestIdx]

	dt := newest.t.Sub(oldest.t).Seconds()
	if dt < 0.5 {
		return 0
	}
	delta := newest.bytes - oldest.bytes
	if delta <= 0 {
		return 0
	}
	return float64(delta) / dt / (1024 * 1024)
}
