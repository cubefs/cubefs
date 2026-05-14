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

package rules

import (
	"context"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

// NotifyStore wraps a Store and fires OnChange after every successful
// mutating operation. Wire this in front of the persistent Store at
// process start so the live scheduler / tasks subsystem sees rule
// changes immediately (HTTP CRUD, G-3 auto-degrade, SIGHUP reload all
// pass through here).
//
// S6: OnChange is invoked ASYNCHRONOUSLY on a dedicated worker
// goroutine — every successful mutation does a non-blocking send onto
// a single-slot channel, and the worker drains the channel and
// invokes onChange. Multiple sends that arrive before the worker has
// drained collapse into one onChange call, which is exactly what the
// scheduler wants (the relevant input is "the current rule set", not
// "the sequence of edits"). This decouples HTTP handler goroutines
// from the multi-millisecond cost of scheduler.ApplyRules and
// contains callback panics to the worker.
//
// Read operations (List, Get) pass through unchanged. UpdateLastRun is
// intentionally NOT a fire trigger — it's the executor's heartbeat
// writing back terminal status, and re-applying the rule list on every
// heartbeat would create a feedback loop with the scheduler.
type NotifyStore struct {
	Store

	mu        sync.Mutex
	onChange  func()
	lastFired time.Time

	// fireCh is the wakeup channel for the worker. Single-slot — a
	// non-blocking send collapses bursts of mutations into one
	// onChange invocation. stopCh ends the worker on Close.
	fireCh   chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
	workerWg sync.WaitGroup
}

// NewNotifyStore wraps inner. onChange may be nil (no-op wrapper);
// SetOnChange installs the callback once the scheduler exists. The
// wrapper is transparent to handlers and Degrade because it embeds
// Store and only overrides the mutating methods.
//
// Spawns a single worker goroutine that drains fireCh. Call Close
// when the wrapper is no longer needed; without Close, the worker
// will run until the process exits (cheap — it blocks on a channel).
func NewNotifyStore(inner Store, onChange func()) *NotifyStore {
	n := &NotifyStore{
		Store:    inner,
		onChange: onChange,
		fireCh:   make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}
	n.workerWg.Add(1)
	go n.worker()
	return n
}

// SetOnChange swaps the callback after construction. Useful when the
// scheduler is built AFTER the rule store (chicken-and-egg in
// server.go.doStart). Safe to call concurrently with mutating methods.
func (n *NotifyStore) SetOnChange(fn func()) {
	n.mu.Lock()
	n.onChange = fn
	n.mu.Unlock()
}

// LastFiredAt returns when the worker most recently invoked the
// callback. Zero value until the first fire. For diagnostics.
func (n *NotifyStore) LastFiredAt() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastFired
}

// worker drains fireCh and invokes the registered callback. Exits on
// stopCh close. A panic inside the callback is recovered so the
// worker survives bad callbacks; the next fire still runs.
func (n *NotifyStore) worker() {
	defer n.workerWg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.fireCh:
			n.invokeOnChange()
		}
	}
}

// invokeOnChange reads the live callback under the mutex, updates
// lastFired, then calls the callback with a recover in place so a
// buggy callback can't kill the worker.
func (n *NotifyStore) invokeOnChange() {
	n.mu.Lock()
	cb := n.onChange
	n.lastFired = time.Now()
	n.mu.Unlock()
	if cb == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("rules.NotifyStore: onChange panic: %v", r)
		}
	}()
	cb()
}

// fire wakes the worker. Non-blocking: if a fire is already pending
// the duplicate is dropped — the worker will collapse the burst into
// one onChange invocation. This is the intended scheduler input
// (current rule snapshot, not edit log).
func (n *NotifyStore) fire() {
	select {
	case n.fireCh <- struct{}{}:
	default:
		// already pending — collapse.
	}
}

// Close stops the worker, waits for it to exit, then closes the
// underlying Store. Safe to call multiple times. The order matters:
// the worker MUST exit before we close the inner Store so the
// callback can't fire on a teardown scheduler.
func (n *NotifyStore) Close() error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.workerWg.Wait()
	return n.Store.Close()
}

// Create inserts the rule and fires OnChange on success.
func (n *NotifyStore) Create(ctx context.Context, r *Rule) error {
	if err := n.Store.Create(ctx, r); err != nil {
		return err
	}
	n.fire()
	return nil
}

// Update replaces the rule and fires OnChange on success.
func (n *NotifyStore) Update(ctx context.Context, r *Rule) error {
	if err := n.Store.Update(ctx, r); err != nil {
		return err
	}
	n.fire()
	return nil
}

// Delete removes the rule and fires OnChange on success.
func (n *NotifyStore) Delete(ctx context.Context, id string) error {
	if err := n.Store.Delete(ctx, id); err != nil {
		return err
	}
	n.fire()
	return nil
}

// SetState transitions the rule lifecycle (active/paused/degraded) and
// fires OnChange on success. Paused/degraded rules must reach the
// scheduler immediately so it stops firing them.
func (n *NotifyStore) SetState(ctx context.Context, id string, s State) error {
	if err := n.Store.SetState(ctx, id, s); err != nil {
		return err
	}
	n.fire()
	return nil
}

// UpdateLastRun does NOT fire OnChange — it's the executor's heartbeat
// write of terminal status. Firing here would feedback-loop with the
// scheduler (every completed run -> ApplyRules -> reschedule).
func (n *NotifyStore) UpdateLastRun(ctx context.Context, id string, last LastRunSummary) error {
	return n.Store.UpdateLastRun(ctx, id, last)
}
