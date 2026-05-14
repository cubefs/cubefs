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

// Package scheduler is the cron-driven dispatcher that turns persisted Rules
// into Task triggers. One Scheduler per syncnode process; it owns a
// robfig/cron engine and reconciles its entries to match the active rule
// set every time ApplyRules is called (typically once at boot + after each
// admin-API mutation that lands in F-3).
//
// Design constraints (see docs/plan/syncnode/design.md §9 F-1):
//
//   - Active rules with a non-empty Schedule fire on every tick.
//   - Paused / degraded rules and rules with empty Schedule never fire.
//   - One bad schedule in a batch must not poison the others; ApplyRules
//     reports a non-fatal error and keeps going.
//   - Per-fire timeout (default 30s) bounds how long the cron goroutine
//     waits on Trigger; the actual task keeps running because the runner
//     uses a fresh background context internally.
//   - Trigger errors are logged but never crash the scheduler.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/tasks"

	"github.com/robfig/cron/v3"
)

// Trigger is the narrow surface the scheduler needs from the task layer.
// Defined here so tests can stub it without spinning up a real Runner.
type Trigger interface {
	Trigger(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error)
}

// Logger is the minimal logging surface the scheduler uses. The stdlib
// *log.Logger satisfies it; tests can pass a noop or a buffer-backed one.
type Logger interface {
	Printf(format string, args ...interface{})
}

const (
	// defaultJobTimeout caps how long the cron goroutine waits on a single
	// Trigger call. Triggers themselves return quickly with wait=false; this
	// is belt-and-suspenders for slow store writes.
	defaultJobTimeout = 30 * time.Second
)

// options holds the optional knobs configured via With* functional options.
type options struct {
	logger     Logger
	jobTimeout time.Duration
}

// Option configures a Scheduler.
type Option func(*options)

// WithLogger replaces the default stdlib logger. Pass nil to silence
// scheduler logs entirely (a noop logger is installed instead of nil so
// the scheduler never has to nil-check).
func WithLogger(l Logger) Option {
	return func(o *options) {
		if l == nil {
			o.logger = noopLogger{}
			return
		}
		o.logger = l
	}
}

// WithJobTimeout overrides the default 30s per-fire timeout. Values <=0
// are ignored.
func WithJobTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.jobTimeout = d
		}
	}
}

// entry tracks one armed cron entry. Stored under its ruleID so we can
// diff against the next ApplyRules batch.
type entry struct {
	id       cron.EntryID
	schedule string
}

// Scheduler dispatches Trigger calls on each rule's cron schedule. Construct
// with New; start with Start; reconcile the rule set with ApplyRules. Stop
// is idempotent and safe to call multiple times.
type Scheduler struct {
	store   rules.Store
	trigger Trigger
	opts    options

	mu      sync.Mutex
	cron    *cron.Cron
	entries map[string]entry // ruleID -> entry
	started bool
	stopped bool

	// parentCtx is captured at Start and passed (with timeout) to each
	// Trigger fire. Cancelled by Stop.
	parentCtx    context.Context
	cancelParent context.CancelFunc
}

// New constructs a Scheduler. store is read (via List) when Apply* helpers
// want the current rule set; the scheduler itself never auto-discovers — the
// admin layer is responsible for calling ApplyRules. trigger is the task
// dispatcher (tasks.Runner satisfies it).
func New(store rules.Store, trigger Trigger, opts ...Option) *Scheduler {
	o := options{
		logger:     log.Default(),
		jobTimeout: defaultJobTimeout,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &Scheduler{
		store:   store,
		trigger: trigger,
		opts:    o,
		entries: make(map[string]entry),
	}
}

// Start spins up the cron loop. Idempotent. Callers must invoke ApplyRules
// at least once (typically right after Start) to load the initial rule set.
// ctx is the parent context for every triggered task; cancelling it stops
// all in-flight cron-driven Trigger calls (the executor keeps running its
// own task because tasks.Runner detaches the context internally).
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return errors.New("scheduler: stopped, construct a new one")
	}
	if s.started {
		return nil
	}

	// cron.WithSeconds() makes the parser accept both 5-field standard cron
	// and 6-field cron-with-seconds (and the @descriptor shorthands). This
	// matches what validateCronExpr in syncnode/config.go accepts.
	parser := cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour |
			cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
	s.cron = cron.New(cron.WithParser(parser))

	s.parentCtx, s.cancelParent = context.WithCancel(ctx)
	s.cron.Start()
	s.started = true
	return nil
}

// Stop tears down the cron loop. Idempotent. Returns once all in-flight
// cron callbacks have returned (cron.Stop returns a context whose Done
// channel closes when the last running job exits).
func (s *Scheduler) Stop() error {
	s.mu.Lock()
	if !s.started || s.stopped {
		s.stopped = true
		s.mu.Unlock()
		return nil
	}
	c := s.cron
	cancel := s.cancelParent
	s.stopped = true
	// Clear entries so RegisteredCount reflects the post-Stop state.
	s.entries = make(map[string]entry)
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if c != nil {
		<-c.Stop().Done()
	}
	return nil
}

// ApplyRules diffs the supplied rules against currently-registered cron
// entries and adds/replaces/removes entries so the engine matches the
// input set. A rule fires only if all of the following hold:
//
//   - State == StateActive
//   - Schedule is non-empty
//   - Schedule parses cleanly under cron.WithSeconds()
//
// Parse failures for individual rules are logged and accumulated into the
// returned error; other rules in the same batch are still (re)registered.
// Safe to call concurrently with itself, Start, and Stop.
func (s *Scheduler) ApplyRules(rs []*rules.Rule) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return errors.New("scheduler: stopped")
	}
	if !s.started || s.cron == nil {
		return errors.New("scheduler: not started")
	}

	// Build the incoming set of (ruleID -> desired schedule). Inactive /
	// empty-schedule rules are intentionally absent so the diff below
	// removes them.
	want := make(map[string]string, len(rs))
	var parseErrs []string
	for _, r := range rs {
		if r == nil {
			continue
		}
		id := r.Config.ID
		if id == "" {
			continue
		}
		if r.State != rules.StateActive {
			continue
		}
		sched := strings.TrimSpace(r.Config.Schedule)
		if sched == "" {
			// manual-trigger only; never armed.
			continue
		}
		want[id] = sched
	}

	// Remove entries that are gone or whose schedule changed.
	for id, e := range s.entries {
		newSched, present := want[id]
		if !present || newSched != e.schedule {
			s.cron.Remove(e.id)
			delete(s.entries, id)
		}
	}

	// Add (or re-add) entries for the desired set.
	for id, sched := range want {
		if _, ok := s.entries[id]; ok {
			// Already armed at the desired schedule.
			continue
		}
		ruleID := id // capture by value
		entryID, err := s.cron.AddFunc(sched, func() {
			s.fire(ruleID)
		})
		if err != nil {
			parseErrs = append(parseErrs, fmt.Sprintf("rule %q: %v", id, err))
			s.opts.logger.Printf("scheduler: invalid schedule for rule %q (%q): %v", id, sched, err)
			continue
		}
		s.entries[id] = entry{id: entryID, schedule: sched}
	}

	if len(parseErrs) > 0 {
		return fmt.Errorf("apply rules: %d invalid: %s",
			len(parseErrs), strings.Join(parseErrs, "; "))
	}
	return nil
}

// RegisteredCount returns the number of cron entries currently armed.
// Used by tests and by the /admin/syncnode/stat handler (F-3).
func (s *Scheduler) RegisteredCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// fire is the cron callback invoked for one rule. It bounds the Trigger
// call with the configured per-fire timeout and logs (but never propagates)
// errors.
func (s *Scheduler) fire(ruleID string) {
	s.mu.Lock()
	parent := s.parentCtx
	timeout := s.opts.jobTimeout
	stopped := s.stopped
	logger := s.opts.logger
	s.mu.Unlock()

	if stopped || parent == nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	if _, err := s.trigger.Trigger(ctx, ruleID, false); err != nil {
		logger.Printf("scheduler: trigger for rule %q failed: %v", ruleID, err)
	}
}

// noopLogger drops everything. Used when WithLogger(nil) is passed.
type noopLogger struct{}

func (noopLogger) Printf(string, ...interface{}) {}
