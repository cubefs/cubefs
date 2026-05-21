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
	"log"
	"sync"
	"time"
)

// TTLConfig governs the timing windows for the TTL Runner.
//
// Two windows are separately tunable because they answer different
// questions: ActiveAge is "how long should operators see a finished task in
// /list?" and HistoryRetention is "how long do we keep the audit trail
// before reclaiming disk?". Defaults match design.md §7.2 — "任务历史 TTL
// 7d".
type TTLConfig struct {
	// ActiveAge: how long a terminal record stays in the ACTIVE compartment
	// before being moved to history. Default 24h — keeps recent runs handy
	// for operators inspecting /admin/sync/task/list without dragging in
	// every old record.
	ActiveAge time.Duration

	// HistoryRetention: how long records stay in history after their DoneAt
	// before being permanently purged. Default 7 days — matches design.md
	// §7.2.
	HistoryRetention time.Duration

	// SweepInterval: how often the TTL goroutine wakes up to do both
	// sweeps. Default 1 hour. Tests override to ~10ms via WithTTLConfig to
	// drive the periodic loop without sleeping.
	SweepInterval time.Duration
}

// DefaultTTLConfig returns the production-default config.
func DefaultTTLConfig() TTLConfig {
	return TTLConfig{
		ActiveAge:        24 * time.Hour,
		HistoryRetention: 7 * 24 * time.Hour,
		SweepInterval:    1 * time.Hour,
	}
}

// TTLRunner runs the periodic move-to-history + purge-old-history sweeps.
// One TTLRunner per process; the caller is responsible for Start / Stop
// during server lifecycle.
type TTLRunner struct {
	store Store
	cfg   TTLConfig
	now   nowFunc // injectable clock; defaults to time.Now

	mu     sync.Mutex
	cancel context.CancelFunc // nil when not running
	done   chan struct{}      // closed when goroutine exits
}

// TTLOption configures a TTLRunner.
type TTLOption func(*TTLRunner)

// WithTTLConfig overrides the default TTL windows.
func WithTTLConfig(cfg TTLConfig) TTLOption {
	return func(r *TTLRunner) {
		r.cfg = cfg
	}
}

// WithClock injects a custom clock. Tests use this with a fakeClock so
// SweepOnce can be exercised at arbitrary virtual times without waiting.
func WithClock(now func() time.Time) TTLOption {
	return func(r *TTLRunner) {
		if now != nil {
			r.now = now
		}
	}
}

// NewTTLRunner constructs a stopped TTL runner. Caller must call Start to
// kick off the background sweep goroutine.
func NewTTLRunner(store Store, opts ...TTLOption) *TTLRunner {
	r := &TTLRunner{
		store: store,
		cfg:   DefaultTTLConfig(),
		now:   time.Now,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Start spins up the sweep goroutine. Idempotent — second Start while
// already running is a no-op. Returns immediately; the goroutine runs in
// the background until Stop is called or ctx is cancelled.
func (r *TTLRunner) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return nil
	}
	loopCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	doneCh := make(chan struct{})
	r.done = doneCh
	r.mu.Unlock()

	// Pass the done channel by value so Stop() racing with the goroutine
	// (nil-ing r.done) cannot make us close(nil) inside loop().
	go r.loop(loopCtx, doneCh)
	return nil
}

// Stop signals the goroutine to exit and blocks until it does. Idempotent
// — second Stop is a no-op. Safe to call from any goroutine.
func (r *TTLRunner) Stop() error {
	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	r.cancel = nil
	r.done = nil
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	if done != nil {
		<-done
	}
	return nil
}

// loop is the goroutine body. It calls SweepOnce on each tick and exits
// when ctx is cancelled. The done channel is passed by value so a racing
// Stop()/Start() pair cannot make us close a nil channel.
func (r *TTLRunner) loop(ctx context.Context, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(r.cfg.SweepInterval)
	defer t.Stop()
	// Fire one sweep immediately so a freshly-started runner converges
	// without waiting a full SweepInterval (useful when intervals are
	// hour-scale in production).
	if _, _, err := r.SweepOnce(ctx); err != nil && ctx.Err() == nil {
		log.Printf("tasks: TTL initial sweep: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, _, err := r.SweepOnce(ctx); err != nil && ctx.Err() == nil {
				log.Printf("tasks: TTL sweep: %v", err)
			}
		}
	}
}

// SweepOnce runs ONE pass of move-to-history + purge-history against the
// store, anchored at r.now(). Returns (movedCount, purgedCount, error).
//
// Move-to-history: every active record with terminal status whose DoneAt
// is older than ActiveAge is moved. Individual MoveToHistory errors are
// logged and skipped — one bad record must not stall the whole sweep.
//
// Purge: records in history with DoneAt strictly before
// (now - HistoryRetention) are deleted in one call.
func (r *TTLRunner) SweepOnce(ctx context.Context) (int, int, error) {
	now := r.now()
	moved, err := r.moveTerminalToHistory(ctx, now)
	if err != nil {
		return moved, 0, err
	}
	purged, err := r.purgeOldHistory(ctx, now)
	if err != nil {
		return moved, purged, err
	}
	return moved, purged, nil
}

// moveTerminalToHistory iterates the active compartment and migrates any
// terminal record whose DoneAt is older than ActiveAge. List failures
// surface; per-record MoveToHistory failures are logged and skipped so a
// single corrupt record cannot stall the loop.
func (r *TTLRunner) moveTerminalToHistory(ctx context.Context, now time.Time) (int, error) {
	recs, err := r.store.List(ctx, "")
	if err != nil {
		return 0, err
	}
	cutoff := now.Add(-r.cfg.ActiveAge)
	moved := 0
	for _, rec := range recs {
		if !isTerminal(rec.Status) {
			continue
		}
		// DoneAt may be zero on legacy records; treat zero as ineligible
		// (we never want to age a record without a known finish time).
		if rec.DoneAt.IsZero() {
			continue
		}
		// "Older than ActiveAge" means DoneAt <= cutoff. We accept the
		// inclusive boundary so a record that completes exactly at the
		// boundary doesn't linger an extra cycle.
		if rec.DoneAt.After(cutoff) {
			continue
		}
		if err := r.store.MoveToHistory(ctx, rec.TaskID); err != nil {
			log.Printf("tasks: TTL MoveToHistory(%s): %v", rec.TaskID, err)
			continue
		}
		moved++
	}
	return moved, nil
}

// purgeOldHistory delegates to Store.PurgeHistoryBefore with the cutoff
// derived from HistoryRetention.
func (r *TTLRunner) purgeOldHistory(ctx context.Context, now time.Time) (int, error) {
	cutoff := now.Add(-r.cfg.HistoryRetention)
	return r.store.PurgeHistoryBefore(ctx, cutoff)
}
