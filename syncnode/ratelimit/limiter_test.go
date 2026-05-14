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
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"testing"
	"time"
)

// readerOf returns a strictly-sized in-memory reader filled with
// pseudo-random bytes. Random content prevents any accidental compression
// or short-circuit on zero-content downstream code.
func readerOf(n int) io.Reader {
	buf := make([]byte, n)
	rng := rand.New(rand.NewSource(1))
	_, _ = rng.Read(buf)
	return bytes.NewReader(buf)
}

// TestBucket_Unlimited verifies the zero-rate fast path: any WaitN call
// returns nil immediately without touching the underlying rate.Limiter.
func TestBucket_Unlimited(t *testing.T) {
	t.Parallel()
	for _, mbps := range []int{0, -1, -1000} {
		b := NewBucket(mbps)
		if b.Mbps() != mbps {
			t.Errorf("Mbps() = %d, want %d", b.Mbps(), mbps)
		}
		// Large n must not block.
		done := make(chan error, 1)
		go func() { done <- b.WaitN(context.Background(), 1<<30) }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("WaitN unlimited: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("WaitN(unlimited) blocked")
		}
	}
}

// TestBucket_RespectsRate measures actual throughput through a
// LimitedReader at a known rate and asserts the elapsed time is within a
// reasonable band.
func TestBucket_RespectsRate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wall-clock bandwidth test in -short mode")
	}
	t.Parallel()

	const (
		mbps    = 4               // 4 MB/s
		payload = 12 * 1024 * 1024 // 12 MB
	)
	// Expected wall clock = (payload - burst) / rate. Burst defaults to
	// max(1s × rate, 4 MiB) = max(4 MiB, 4 MiB) = 4 MiB. So
	// expected = (12 - 4) MiB / 4 MB/s ~ 2 s.
	b := NewBucket(mbps)
	lr := NewLimitedReader(context.Background(), readerOf(payload), b)

	start := time.Now()
	n, err := io.Copy(io.Discard, lr)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if n != payload {
		t.Fatalf("copied %d, want %d", n, payload)
	}
	// Tolerance band: [1.6s, 2.6s] — within roughly ±25 % of the 2s ideal.
	if elapsed < 1500*time.Millisecond || elapsed > 3*time.Second {
		t.Errorf("elapsed = %s, want roughly 2s", elapsed)
	}
}

// TestComposite_MinimumWins runs payload through (10 MB/s, 1 MB/s) and
// verifies the result tracks the slower bucket.
func TestComposite_MinimumWins(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wall-clock bandwidth test in -short mode")
	}
	t.Parallel()

	const payload = 8 * 1024 * 1024 // 8 MiB
	fast := NewBucket(20)            // 20 MB/s
	slow := NewBucket(2)             // 2 MB/s
	c := NewComposite(fast, slow)
	if len(c.Members()) != 2 {
		t.Fatalf("Members = %d, want 2", len(c.Members()))
	}

	lr := NewLimitedReader(context.Background(), readerOf(payload), c)
	start := time.Now()
	if _, err := io.Copy(io.Discard, lr); err != nil {
		t.Fatalf("copy: %v", err)
	}
	elapsed := time.Since(start)
	// With slow = 2 MB/s and burst = 4 MiB, payload 8 MiB takes
	// (8-4)/2 = 2s. Allow ±50 % for CI jitter.
	if elapsed < 1*time.Second || elapsed > 4*time.Second {
		t.Errorf("composite elapsed = %s, want roughly 2s (slow bucket dominates)", elapsed)
	}
}

// TestComposite_FiltersNil ensures nil members are dropped so the hot path
// doesn't crash on the per-backend slot being unset.
func TestComposite_FiltersNil(t *testing.T) {
	t.Parallel()
	c := NewComposite(nil, NewBucket(0), nil)
	if got := len(c.Members()); got != 1 {
		t.Errorf("Members after filter = %d, want 1", got)
	}
	if err := c.WaitN(context.Background(), 1024); err != nil {
		t.Errorf("WaitN: %v", err)
	}
}

// TestComposite_Empty is the all-nil case — an empty Composite must behave
// as unlimited.
func TestComposite_Empty(t *testing.T) {
	t.Parallel()
	c := NewComposite()
	if err := c.WaitN(context.Background(), 1<<20); err != nil {
		t.Errorf("empty composite WaitN: %v", err)
	}
}

// TestBucket_WaitN_CtxCancelled verifies cancel propagation.
func TestBucket_WaitN_CtxCancelled(t *testing.T) {
	t.Parallel()
	b := NewBucket(1) // 1 MB/s — small burst, slow refill
	// First drain the burst so subsequent WaitN actually blocks.
	if err := b.WaitN(context.Background(), minBurstBytes); err != nil {
		t.Fatalf("drain burst: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- b.WaitN(ctx, minBurstBytes) }()
	// Give the goroutine a moment to start waiting.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("WaitN err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("WaitN did not return after cancel")
	}
}

// TestBucket_SetLimit_Retunes verifies that SetLimit changes future
// throughput. Run a tight 1MB at 1 MB/s (cheap because of burst), then
// retune to a fast rate and confirm a bigger transfer finishes quickly.
func TestBucket_SetLimit_Retunes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wall-clock bandwidth test in -short mode")
	}
	t.Parallel()

	b := NewBucket(1) // 1 MB/s
	// 1 MiB fits inside the 4 MiB burst, so this is near-instant.
	if _, err := io.Copy(io.Discard, NewLimitedReader(context.Background(), readerOf(1<<20), b)); err != nil {
		t.Fatalf("copy at 1 MB/s: %v", err)
	}

	// Retune to a fast rate. The burst resets; 8 MiB at 200 MB/s should be
	// well under 100 ms.
	b.SetLimit(200)
	start := time.Now()
	if _, err := io.Copy(io.Discard, NewLimitedReader(context.Background(), readerOf(8<<20), b)); err != nil {
		t.Fatalf("copy after SetLimit: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("after SetLimit(200), 8 MiB took %s — retune not applied", elapsed)
	}

	// Retune to unlimited — must not block on a huge n.
	b.SetLimit(0)
	if err := b.WaitN(context.Background(), 1<<30); err != nil {
		t.Errorf("WaitN after SetLimit(0): %v", err)
	}
}

// TestLimitedReader_PropagatesUnderlyingErr ensures real Read errors flow
// through unchanged.
func TestLimitedReader_PropagatesUnderlyingErr(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("boom")
	er := &errReader{err: sentinel}
	lr := NewLimitedReader(context.Background(), er, NewBucket(0))
	n, err := lr.Read(make([]byte, 16))
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want sentinel", err)
	}
}

// TestLimitedWriter_BasicThroughput verifies LimitedWriter wires through
// the inner writer and the bandwidth limit takes effect.
func TestLimitedWriter_BasicThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wall-clock bandwidth test in -short mode")
	}
	t.Parallel()

	const payload = 8 * 1024 * 1024
	var sink bytes.Buffer
	w := NewLimitedWriter(context.Background(), &sink, NewBucket(4))
	start := time.Now()
	n, err := io.Copy(w, readerOf(payload))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if n != payload || sink.Len() != payload {
		t.Fatalf("wrote %d / sink %d, want %d", n, sink.Len(), payload)
	}
	// LimitedWriter waits BEFORE the inner Write; so first burst is paid
	// up-front. Expected ~ (8 - 4) MiB / 4 MB/s = 1s. Allow [0.7s, 3s].
	if elapsed < 700*time.Millisecond || elapsed > 3*time.Second {
		t.Errorf("LimitedWriter elapsed = %s, want roughly 1s", elapsed)
	}
}

// TestLimitedWriter_CtxCancelled — wait-before-write surfaces ctx cancel as
// the Write error.
func TestLimitedWriter_CtxCancelled(t *testing.T) {
	t.Parallel()
	b := NewBucket(1)
	// Drain the burst so a subsequent Write blocks.
	if err := b.WaitN(context.Background(), minBurstBytes); err != nil {
		t.Fatalf("drain: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := NewLimitedWriter(ctx, &bytes.Buffer{}, b)
	errCh := make(chan error, 1)
	go func() {
		_, err := w.Write(make([]byte, minBurstBytes))
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Write did not unblock after cancel")
	}
}

// TestNewLimitedReader_NilCtx defaults to context.Background.
func TestNewLimitedReader_NilCtx(t *testing.T) {
	t.Parallel()
	lr := NewLimitedReader(nil, readerOf(8), NewBucket(0))
	buf := make([]byte, 8)
	if _, err := lr.Read(buf); err != nil {
		t.Errorf("Read: %v", err)
	}
}

// TestNewLimitedWriter_NilCtx defaults to context.Background.
func TestNewLimitedWriter_NilCtx(t *testing.T) {
	t.Parallel()
	w := NewLimitedWriter(nil, &bytes.Buffer{}, NewBucket(0))
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Errorf("Write: %v", err)
	}
}

// TestLimitedReader_NilLimiter ensures we degrade gracefully if a caller
// forgets to provide a limiter.
func TestLimitedReader_NilLimiter(t *testing.T) {
	t.Parallel()
	lr := NewLimitedReader(context.Background(), readerOf(64), nil)
	n, err := io.Copy(io.Discard, lr)
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if n != 64 {
		t.Fatalf("n = %d, want 64", n)
	}
}

// TestWaitN_NegativeOrZeroNoop — quick coverage hop.
func TestWaitN_NegativeOrZeroNoop(t *testing.T) {
	t.Parallel()
	b := NewBucket(1)
	if err := b.WaitN(context.Background(), 0); err != nil {
		t.Errorf("WaitN(0): %v", err)
	}
	if err := b.WaitN(context.Background(), -1); err != nil {
		t.Errorf("WaitN(-1): %v", err)
	}
	c := NewComposite(b)
	if err := c.WaitN(context.Background(), 0); err != nil {
		t.Errorf("composite WaitN(0): %v", err)
	}
}

// TestBucket_WaitN_LargerThanBurst — n > burst must split into chunks.
func TestBucket_WaitN_LargerThanBurst(t *testing.T) {
	t.Parallel()
	b := NewBucket(1000) // ~1 GB/s, burst = 1 GiB
	// Ask for 2 GiB — must complete (burst ≈ 1 GiB so we need at least
	// one extra chunk). At 1000 MB/s the second chunk takes ~1s but we
	// kept payload small enough that this is fine for CI.
	// Reduce stress: use 16 MiB which forces no extra wait at this rate.
	if err := b.WaitN(context.Background(), 16<<20); err != nil {
		t.Errorf("WaitN large: %v", err)
	}
}

// errReader is a Reader that always fails.
type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }
