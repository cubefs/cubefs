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
// Concurrency: OnChange is invoked SYNCHRONOUSLY on the goroutine that
// called the mutating method. The callback MUST be cheap (atomic
// counter bump, a non-blocking send onto a channel) — the production
// callback in server.go takes a snapshot + calls scheduler.ApplyRules
// (which is itself non-blocking; the scheduler diffs in its own
// goroutine).
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
}

// NewNotifyStore wraps inner. onChange may be nil (no-op wrapper);
// SetOnChange installs the callback once the scheduler exists. The
// wrapper is transparent to handlers and Degrade because it embeds
// Store and only overrides the mutating methods.
func NewNotifyStore(inner Store, onChange func()) *NotifyStore {
	return &NotifyStore{Store: inner, onChange: onChange}
}

// SetOnChange swaps the callback after construction. Useful when the
// scheduler is built AFTER the rule store (chicken-and-egg in
// server.go.doStart). Safe to call concurrently with mutating methods.
func (n *NotifyStore) SetOnChange(fn func()) {
	n.mu.Lock()
	n.onChange = fn
	n.mu.Unlock()
}

// LastFiredAt returns when the most recent successful mutation fired
// the callback. Zero value until the first fire. For diagnostics.
func (n *NotifyStore) LastFiredAt() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastFired
}

// fire runs the registered callback. Recovers from panics so a buggy
// callback can't corrupt the store's state — the mutation already
// succeeded by the time we get here, and we don't want the caller of
// Create/Update/etc. to see a panic that wasn't theirs.
func (n *NotifyStore) fire() {
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
