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

// Package ratelimit implements the layered bandwidth control described in
// design.md §12.4. Four layers compose: per-task, per-rule (P1+), per-node
// and per-backend. Each layer is a token bucket backed by
// golang.org/x/time/rate.Limiter; multiple layers are combined with a
// Composite that waits on every member, so the effective throughput
// converges to min(layers).
//
// Phase G-2 wires layers 1 (per-task), 3 (per-node) and 4 (per-backend,
// node-local). Layer 2 (per-rule, cross-node) is P1 and intentionally
// absent here.
package ratelimit

import (
	"context"
	"io"
	"sync"

	"golang.org/x/time/rate"
)

// minBurstBytes is the floor for the token-bucket burst capacity. A burst
// smaller than the typical Read chunk would force the very first Read of a
// stream to block, which is both surprising and slow on cold starts. 4 MiB
// matches the io.Copy default buffer × a few rounds.
const minBurstBytes = 4 * 1024 * 1024

// Limiter is the single behaviour every bandwidth layer satisfies. Bucket
// implements it directly; Composite chains multiple Limiters.
type Limiter interface {
	// WaitN blocks until n bytes of budget are available across this layer.
	// It returns ctx.Err() if the context is cancelled before the budget
	// becomes available. n <= 0 is a no-op (returns nil immediately).
	WaitN(ctx context.Context, n int) error
}

// Bucket is one token-bucket layer. A zero / negative configured rate means
// "unlimited" — WaitN returns nil immediately without consulting the
// underlying rate.Limiter.
//
// The configured rate is stored as float64 because cluster-wide caps
// distributed across N nodes (sync_quota.Compute) produce fractional MB/s
// (e.g. 400 / 7 ≈ 57.14). Truncating to int under-enforced the total cap
// by up to N-1 MB/s (SEC5). NewBucket accepts int for the static
// constructor convenience used by server.go / executor.go; SetLimit takes
// float64 since the dynamic-update path receives fractional values from
// master.
//
// Bucket is safe for concurrent use.
type Bucket struct {
	mu   sync.Mutex
	rl   *rate.Limiter // nil when unlimited
	mbps float64       // configured Mbps, kept for SetLimit / diagnostics
}

// NewBucket constructs a token bucket capped at mbps megabytes per second.
// mbps <= 0 is treated as unlimited; the returned Bucket is still valid and
// its WaitN is a no-op. Burst defaults to max(1 second of bandwidth,
// 4 MiB) so freshly minted buckets don't stall the first Read of a stream.
//
// Accepts int for backward compatibility with the static node-level
// constructor at server boot. Dynamic / fractional retuning goes through
// SetLimit which takes float64.
func NewBucket(mbps int) *Bucket {
	b := &Bucket{}
	b.setLimitLocked(float64(mbps))
	return b
}

// SetLimit retunes the bucket to mbps megabytes per second. Passing 0 or a
// negative value disables limiting (subsequent WaitN calls return nil
// immediately). Safe for concurrent use; callers in flight on the old
// limiter complete their wait against the old rate, future callers see the
// new one.
//
// Takes float64 because cross-node quotas (per-rule / per-backend) come
// from master's equal-division of cluster caps and are commonly
// fractional. See SEC5 in the design notes.
func (b *Bucket) SetLimit(mbps float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.setLimitLocked(mbps)
}

func (b *Bucket) setLimitLocked(mbps float64) {
	b.mbps = mbps
	if mbps <= 0 {
		b.rl = nil
		return
	}
	bytesPerSec := mbps * 1024 * 1024
	burst := int(bytesPerSec)
	if burst < minBurstBytes {
		burst = minBurstBytes
	}
	b.rl = rate.NewLimiter(rate.Limit(bytesPerSec), burst)
}

// Mbps returns the configured rate (0 means unlimited). Exposed for
// diagnostics / metrics; not used in the hot path.
func (b *Bucket) Mbps() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mbps
}

// WaitN implements Limiter. Returns immediately when the bucket is
// unlimited or n <= 0. Otherwise blocks until n bytes of budget are
// available or ctx is cancelled.
//
// Note: rate.Limiter.WaitN requires n <= burst; when a caller asks for more
// bytes than the burst we split the request into burst-sized chunks. This
// keeps the wrapper agnostic of the underlying Read buffer size.
func (b *Bucket) WaitN(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	b.mu.Lock()
	rl := b.rl
	b.mu.Unlock()
	if rl == nil {
		return nil
	}
	burst := rl.Burst()
	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}
		if err := rl.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

// Composite chains multiple Limiters into a single Limiter. WaitN waits on
// each member in order; the effective rate is min(member rates).
//
// Nil / unlimited members are filtered out at construction so the hot path
// stays branch-free. Empty Composites are valid (their WaitN is a no-op).
type Composite struct {
	members []Limiter
}

// NewComposite returns a Composite over the supplied members. Nil members
// are dropped. An empty result (all members nil) is allowed and behaves as
// "unlimited".
func NewComposite(members ...Limiter) *Composite {
	out := make([]Limiter, 0, len(members))
	for _, m := range members {
		if m == nil {
			continue
		}
		out = append(out, m)
	}
	return &Composite{members: out}
}

// Members returns the wrapped Limiters for tests / diagnostics. The slice
// is shared — do not mutate.
func (c *Composite) Members() []Limiter {
	return c.members
}

// WaitN implements Limiter.
func (c *Composite) WaitN(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	for _, m := range c.members {
		if err := m.WaitN(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

// LimitedReader wraps an io.Reader and enforces bandwidth limits. After
// each Read returns n bytes, LimitedReader blocks on the underlying
// Limiter for n bytes of budget before handing the bytes back to the
// caller. Waiting after the Read (rather than before) means a single
// chunky Read on a fresh bucket doesn't stall — the first burst is free,
// subsequent reads converge to the configured rate. This matches the
// pseudo-code in design.md §12.4.2.
//
// LimitedReader is single-goroutine, mirroring io.Reader contract.
type LimitedReader struct {
	inner io.Reader
	lim   Limiter
	ctx   context.Context
}

// NewLimitedReader wraps r with the supplied Limiter. ctx is consulted via
// WaitN: a cancelled ctx surfaces as the Read error.
func NewLimitedReader(ctx context.Context, r io.Reader, lim Limiter) *LimitedReader {
	if ctx == nil {
		ctx = context.Background()
	}
	return &LimitedReader{inner: r, lim: lim, ctx: ctx}
}

// Read implements io.Reader. It first reads from the inner reader, then
// blocks on the limiter for the number of bytes returned.
func (lr *LimitedReader) Read(p []byte) (int, error) {
	n, err := lr.inner.Read(p)
	if n > 0 && lr.lim != nil {
		if werr := lr.lim.WaitN(lr.ctx, n); werr != nil {
			// Propagate cancellation as the read error so the surrounding
			// io.Copy stops cleanly. The bytes are still returned — the
			// caller MAY consume them before observing the error.
			return n, werr
		}
	}
	return n, err
}

// LimitedWriter wraps an io.Writer and enforces bandwidth limits. Unlike
// the reader, the writer waits BEFORE handing bytes to the inner Writer:
// if the underlying writer buffers internally, waiting after the Write
// would let the bytes through unthrottled. The semantics align with
// "writes happen at most at rate X" rather than "bytes leave at most at
// rate X".
type LimitedWriter struct {
	inner io.Writer
	lim   Limiter
	ctx   context.Context
}

// NewLimitedWriter wraps w with the supplied Limiter. ctx cancellation
// surfaces as the Write error.
func NewLimitedWriter(ctx context.Context, w io.Writer, lim Limiter) *LimitedWriter {
	if ctx == nil {
		ctx = context.Background()
	}
	return &LimitedWriter{inner: w, lim: lim, ctx: ctx}
}

// Write implements io.Writer.
func (lw *LimitedWriter) Write(p []byte) (int, error) {
	if lw.lim != nil && len(p) > 0 {
		if werr := lw.lim.WaitN(lw.ctx, len(p)); werr != nil {
			return 0, werr
		}
	}
	return lw.inner.Write(p)
}
