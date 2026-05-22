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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// RuleLookup is the narrow read-only view of the rules.Store the Runner
// depends on. Keeping it tiny lets tests stub it without dragging the rest
// of the rules package in.
type RuleLookup interface {
	Get(ctx context.Context, ruleID string) (*rules.Rule, error)
}

// BackendBuilder constructs a Backend from a single EndpointConfig. The
// Runner builds one src + one dst per task. Production wires this to the
// backend Pool; tests pass a stub that returns in-memory backends.
type BackendBuilder interface {
	Build(ctx context.Context, ep *spec.EndpointConfig) (backend.Backend, error)
}

// IDFactory makes task IDs. Callers may inject a deterministic factory in
// tests; the default uses time-based monotonic IDs (no external uuid dep
// here — the package import list is tightly constrained).
type IDFactory func() string

// OnTerminalFunc is called once per task after it reaches a terminal status
// and the record has been persisted to the Store. Errors from the callback
// are logged but do NOT affect the task result.
//
// Production wires this to syncnode.SyncNode.onTaskTerminal so master gets
// pushed a TaskTerminalReport via OpSyncNodeRunTask response. The Runner
// holds at most one callback; passing nil resets to "no-op".
type OnTerminalFunc func(rec *Record)

// ErrRuleTypeMismatch is returned by Save / Load aliases when the rule's
// declared type doesn't match the requested alias.
var ErrRuleTypeMismatch = errors.New("rule type does not match endpoint")

// ErrQueueFull is returned by Trigger / Retry when the concurrency cap is
// reached AND the queue is full (or queueing is disabled). Operators
// observing this in /admin/sync/task/list see a Record with Status=failed
// and Error="runner: task queue full" — that is the contract.
var ErrQueueFull = errors.New("runner: task queue full")

// Concurrency-gate defaults (FIX C). Zero means "unlimited", which
// preserves pre-fix behavior for callers (and tests) that do not opt in
// via WithMaxConcurrent / WithQueueSize.
const (
	defaultMaxConcurrent = 0
	defaultQueueSize     = 0
)

// Runner owns the executor and dispatches Runs in response to HTTP triggers.
// One Runner per process. Safe for concurrent use.
type Runner struct {
	exec    *executor.Executor
	store   Store
	rules   RuleLookup
	builder BackendBuilder

	// idFactory generates fresh task IDs. Pluggable for tests.
	idFactory IDFactory

	// reporterFactory produces an executor.Reporter for each run. Defaults
	// to a recording reporter that snapshots Progress into the Record.
	reporterFactory func(taskID string) executor.Reporter

	// onTerminal is the optional lifecycle hook fired exactly once per
	// task after a terminal record has been persisted. nil → skip. Set
	// via WithOnTerminal.
	onTerminal OnTerminalFunc

	// waiters tracks per-task done channels for wait=true callers. The
	// goroutine running the task closes its channel when the record reaches
	// a terminal status.
	mu      sync.Mutex
	waiters map[string]chan struct{}

	// idSeq feeds the default ID factory (monotonic per Runner).
	idSeq uint64

	// Concurrency gate (FIX C). Both default to 0 → unlimited (preserves
	// pre-fix behavior for tests that don't opt in). When maxConcurrent > 0
	// the Runner enforces an in-flight cap; when maxQueue > 0 over-cap
	// triggers are admitted to a bounded waiting queue instead of being
	// rejected outright.
	maxConcurrent int
	maxQueue      int

	// slots is a buffered chan acting as a counting semaphore. len(slots)
	// == currently-running tasks; cap == maxConcurrent. nil when
	// maxConcurrent == 0 (unlimited).
	slots chan struct{}

	// queueLen counts admitted-but-not-yet-running tasks. Updated when a
	// trigger lands on the bounded queue path; decremented when the waiter
	// goroutine successfully claims a slot. Exposed via QueueLen for
	// diagnostics (heartbeat snapshot).
	queueLen atomic.Int64

	// FIX Q1 + Q2 — per-task cancellation surface that covers BOTH
	// queued (awaiting slot) AND running (in executor) phases.
	// cancellers[taskID] is invoked by Cancel() AND by Close() to abort
	// the per-task ctx that runAfterWait + run honour. Cleared in
	// run()'s defer after the executor returns.
	cancellersMu sync.Mutex
	cancellers   map[string]context.CancelFunc

	// closed is set by Close(); triggerRule refuses to spawn when set.
	// closedCh is used by runAfterWait's slot-wait select so queued tasks
	// can abort during shutdown without waiting for their semaphore turn.
	closed   atomic.Bool
	closedCh chan struct{}

	// wg tracks every spawned run/runAfterWait goroutine so Close can
	// block until all in-flight work has finished. Add fires in
	// triggerRule before the goroutine launches; Done fires in each
	// goroutine's defer.
	wg sync.WaitGroup
}

// RunnerOption configures a Runner.
type RunnerOption func(*Runner)

// WithIDFactory overrides the default task-ID factory.
func WithIDFactory(f IDFactory) RunnerOption {
	return func(r *Runner) {
		if f != nil {
			r.idFactory = f
		}
	}
}

// WithOnTerminal registers a callback fired once per task after it hits a
// terminal status (done / failed / cancelled) and the resulting Record has
// been persisted via Store.Put. The callback runs on the same goroutine as
// the task's run loop, AFTER all bookkeeping is complete, so it should not
// do unbounded work. Production wires this to the master push-back path;
// tests use it as an observer.
func WithOnTerminal(fn OnTerminalFunc) RunnerOption {
	return func(r *Runner) { r.onTerminal = fn }
}

// WithMaxConcurrent caps the number of tasks the Runner will run
// simultaneously. n <= 0 disables the cap (unlimited, pre-fix behavior).
// Wired from cfg.Concurrency.MaxConcurrentTasks in initExecutorAndRunner.
func WithMaxConcurrent(n int) RunnerOption {
	return func(r *Runner) {
		if n > 0 {
			r.maxConcurrent = n
		}
	}
}

// WithQueueSize caps the number of tasks that may wait for a slot once
// the concurrency cap is reached. n <= 0 (the default) means fail-fast:
// over-cap triggers immediately return ErrQueueFull instead of blocking.
// Wired from cfg.Concurrency.MaxQueueSize in initExecutorAndRunner.
func WithQueueSize(n int) RunnerOption {
	return func(r *Runner) {
		if n > 0 {
			r.maxQueue = n
		}
	}
}

// NewRunner returns a Runner. exec / store / rules / builder must be non-nil.
// Callers own exec and store lifecycles; the Runner does not Close them.
func NewRunner(exec *executor.Executor, store Store, ruleLookup RuleLookup, b BackendBuilder, opts ...RunnerOption) *Runner {
	r := &Runner{
		exec:          exec,
		store:         store,
		rules:         ruleLookup,
		builder:       b,
		waiters:       make(map[string]chan struct{}),
		cancellers:    make(map[string]context.CancelFunc),
		closedCh:      make(chan struct{}),
		maxConcurrent: defaultMaxConcurrent,
		maxQueue:      defaultQueueSize,
	}
	r.idFactory = r.defaultIDFactory
	r.reporterFactory = func(string) executor.Reporter { return executor.NoopReporter{} }
	for _, o := range opts {
		o(r)
	}
	// Build the counting-semaphore AFTER options have been applied so the
	// cap reflects WithMaxConcurrent. nil channel = unlimited.
	if r.maxConcurrent > 0 {
		r.slots = make(chan struct{}, r.maxConcurrent)
	}
	return r
}

// Trigger looks up the rule, builds backends, constructs an executor.Task,
// persists a running Record, and runs it asynchronously. When wait=true the
// call blocks until the task reaches a terminal status (or ctx is done).
//
// Returned *Record is a fresh copy; mutating it does not affect store state.
func (r *Runner) Trigger(ctx context.Context, ruleID string, wait bool) (*Record, error) {
	if ruleID == "" {
		return nil, fmt.Errorf("ruleID required")
	}
	rule, err := r.rules.Get(ctx, ruleID)
	if err != nil {
		return nil, err
	}
	return r.triggerRule(ctx, rule, "", nil, wait)
}

// TriggerWithID is the master-driven entry point: like Trigger, but uses
// the supplied taskID verbatim instead of generating one via idFactory.
// This is what makes master's taskOwner ledger stay in sync with the
// syncnode's local Record store — a Cancel(t-1) from master can land
// because syncnode's local Record key is exactly "t-1".
//
// Empty taskID falls back to the regular Trigger behaviour (a fresh id
// is allocated). Defensive: master is the source of truth for the ID;
// the empty-fallback exists so older masters that don't carry the field
// still get a working trigger.
func (r *Runner) TriggerWithID(ctx context.Context, ruleID, taskID string, wait bool) (*Record, error) {
	if ruleID == "" {
		return nil, fmt.Errorf("ruleID required")
	}
	rule, err := r.rules.Get(ctx, ruleID)
	if err != nil {
		return nil, err
	}
	return r.triggerRule(ctx, rule, taskID, nil, wait)
}

// TriggerSubTask is the P1-7 fan-out entry point. It runs the same rule
// the regular Trigger would, but pins the executor.Task to a single
// shard of an N-way split: only entries whose key hashes to shardIndex
// are processed; the other N-1 are silently skipped by the producer
// loop in sync_task.go / load_task.go.
//
// Identity wiring:
//   - task.ID becomes "<parentTaskID>/<shardIndex>" so master can pair
//     responses back to the parent and a redispatched sub-task on a new
//     owner keeps the same id.
//   - rec.RuleID points at the rule (same as Trigger) so retries /
//     auto-fix /degrade pathways keep working unchanged.
//
// Returns a fresh Record like Trigger. shardTotal <= 1 is treated as
// "no sharding" — the run is identical to a regular Trigger, but the
// caller still gets the parent-prefixed task id back so accounting is
// consistent.
func (r *Runner) TriggerSubTask(ctx context.Context, ruleID, parentTaskID string,
	shardIndex, shardTotal int, wait bool,
) (*Record, error) {
	if ruleID == "" {
		return nil, fmt.Errorf("ruleID required")
	}
	if parentTaskID == "" {
		return nil, fmt.Errorf("parentTaskID required")
	}
	if shardTotal < 0 {
		return nil, fmt.Errorf("invalid shardTotal: %d", shardTotal)
	}
	if shardTotal > 0 && (shardIndex < 0 || shardIndex >= shardTotal) {
		return nil, fmt.Errorf("shardIndex %d out of range [0,%d)", shardIndex, shardTotal)
	}
	rule, err := r.rules.Get(ctx, ruleID)
	if err != nil {
		return nil, err
	}
	subID := fmt.Sprintf("%s/%d", parentTaskID, shardIndex)
	shard := &shardOverride{index: shardIndex, total: shardTotal}
	return r.triggerRule(ctx, rule, subID, shard, wait)
}

// TriggerAs is a typed alias for Trigger: it asserts that the rule's
// declared type matches wantType before triggering. Used by /save (sync)
// and /load (load) endpoints.
func (r *Runner) TriggerAs(ctx context.Context, ruleID string, wantType executor.TaskType, wait bool) (*Record, error) {
	rule, err := r.rules.Get(ctx, ruleID)
	if err != nil {
		return nil, err
	}
	if executor.TaskType(rule.Config.Type) != wantType {
		return nil, fmt.Errorf("%w: want %q, got %q", ErrRuleTypeMismatch, wantType, rule.Config.Type)
	}
	return r.triggerRule(ctx, rule, "", nil, wait)
}

// TriggerWithRule is the P2-6 master-driven entry point. The master
// ships the full SyncRule snapshot in the OpSyncNodeRunTask payload,
// so the syncnode does NOT need a local rule lookup — this path
// bypasses r.rules entirely and consumes the supplied rule directly.
//
// taskID is honoured verbatim when non-empty (master's task ledger
// keys by exactly this id). subtask carries the optional fan-out
// descriptor: shardIndex / shardTotal / prefixes — all passed through
// to executor.Task. nil sub = single-task run.
//
// Wait=true blocks until the task reaches a terminal status (or ctx is
// done). The syncnode master loop typically uses wait=false so the TCP
// reply is fast; the executor reports the terminal state separately
// via the master responder hook.
func (r *Runner) TriggerWithRule(ctx context.Context, rule *rules.Rule, taskID string,
	shardIndex, shardTotal int, prefixes []string, wait bool,
) (*Record, error) {
	if rule == nil {
		return nil, errors.New("nil rule")
	}
	if rule.Config.ID == "" {
		return nil, errors.New("rule has empty ID")
	}
	var sub *shardOverride
	if shardTotal > 0 || len(prefixes) > 0 {
		if shardTotal > 0 && (shardIndex < 0 || shardIndex >= shardTotal) {
			return nil, fmt.Errorf("shardIndex %d out of range [0,%d)", shardIndex, shardTotal)
		}
		sub = &shardOverride{index: shardIndex, total: shardTotal, prefixes: prefixes}
	}
	return r.triggerRule(ctx, rule, taskID, sub, wait)
}

// Cancel signals the named task to stop. Covers tasks in any phase:
//
//   - QUEUED   : context cancel makes runAfterWait abort before
//     acquiring the slot (was a silent no-op before Q1).
//   - RUNNING  : context cancel propagates into executor.Run; executor
//     also gets a direct Cancel call as belt-and-braces.
//   - UNKNOWN  : returns ErrTaskNotFound (matches pre-fix behavior).
//
// FIX Q1 — pre-fix code only called executor.Cancel, which is a silent
// no-op for tasks still waiting for a concurrency slot. The per-task
// canceller registered in triggerRule covers both phases.
func (r *Runner) Cancel(ctx context.Context, taskID string) error {
	if _, err := r.store.Get(ctx, taskID); err != nil {
		return err
	}
	r.cancellersMu.Lock()
	cancel := r.cancellers[taskID]
	r.cancellersMu.Unlock()
	if cancel != nil {
		cancel() // aborts taskCtx; runAfterWait + executor exit
	}
	// Belt-and-braces for the running phase: even if the canceller fired
	// before the executor registered the task, hitting executor.Cancel
	// after the registration closes the race.
	r.exec.Cancel(taskID)
	return nil
}

// Retry re-runs a prior failed / cancelled task. The original record is
// preserved; a fresh record with a new taskID ("<old>-r<n>") is created.
// Returns the new Record.
func (r *Runner) Retry(ctx context.Context, taskID string) (*Record, error) {
	old, err := r.store.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if old.RuleID == "" {
		return nil, fmt.Errorf("cannot retry ad-hoc task without ruleID: %s", taskID)
	}
	rule, err := r.rules.Get(ctx, old.RuleID)
	if err != nil {
		return nil, err
	}
	newID := nextRetryID(old.TaskID)
	return r.triggerRule(ctx, rule, newID, nil, false)
}

// shardOverride carries the (index, total) shard descriptor through
// triggerRule so the same constructor handles both regular Trigger and
// the P1-7 fan-out path. nil means "no override". P2-5 added Prefixes
// for prefix-mode sharding — non-empty Prefixes flips executor.ShouldKeep
// to literal-prefix match and bypasses hash math.
type shardOverride struct {
	index    int
	total    int
	prefixes []string
}

// triggerRule is the shared core for Trigger / TriggerAs / Retry /
// TriggerSubTask. If newID is empty, a fresh id is allocated by
// r.idFactory. If shard != nil and shard.total > 0, the constructed
// executor.Task is pinned to that shard (see executor.ShouldKeep).
func (r *Runner) triggerRule(ctx context.Context, rule *rules.Rule, newID string,
	shard *shardOverride, wait bool,
) (*Record, error) {
	// FIX Q2 — refuse new triggers after Close. Avoids spawning a
	// goroutine whose runAfterWait + run would race against a torn-down
	// executor.
	if r.closed.Load() {
		return nil, errors.New("runner: closed")
	}

	// Idempotency guard: when a specific ID is requested and that task is
	// already non-terminal, return the existing record instead of spawning
	// a second goroutine. This handles master re-dispatch (e.g. after a
	// brief heartbeat gap that caused master to believe the node was dead
	// and re-send OpSyncNodeRunTask for the same ID). Running two goroutines
	// for the same task resets progress to zero on each restart.
	if newID != "" {
		if existing, getErr := r.store.Get(ctx, newID); getErr == nil &&
			existing.Status == executor.StatusRunning {
			log.LogInfof("tasks: task=%q already running, ignoring duplicate trigger", newID)
			return cloneRecord(existing), nil
		}
	}

	// Mirror rule-level OnSymlink down into both endpoint configs so the
	// BackendBuilder (and the local backend it constructs) can read the
	// policy without changing the Build(ep) interface. Rule-level validation
	// remains authoritative; endpoint-level field is purely a transport.
	// Only the local backend consumes it; s3/cfs builders ignore (with a
	// warn log).
	if rule.Config.OnSymlink != "" {
		rule.Config.Src.OnSymlink = rule.Config.OnSymlink
		rule.Config.Dst.OnSymlink = rule.Config.OnSymlink
	}

	src, err := r.builder.Build(ctx, &rule.Config.Src)
	if err != nil {
		log.LogWarnf("tasks: rule=%q task=%q build src backend: %v", rule.Config.ID, newID, err)
		return nil, fmt.Errorf("build src backend: %w", err)
	}
	dst, err := r.builder.Build(ctx, &rule.Config.Dst)
	if err != nil {
		_ = src.Close()
		log.LogWarnf("tasks: rule=%q task=%q build dst backend: %v", rule.Config.ID, newID, err)
		return nil, fmt.Errorf("build dst backend: %w", err)
	}

	if newID == "" {
		newID = r.idFactory()
	}
	task := buildTask(rule, src, dst, newID)
	if shard != nil && shard.total > 0 {
		task.ShardIndex = shard.index
		task.ShardTotal = shard.total
	}
	if shard != nil && len(shard.prefixes) > 0 {
		task.ShardPrefixes = shard.prefixes
	}

	// Concurrency gate (FIX C). Decide on admission BEFORE persisting a
	// running Record so we don't claim "running" for a task we end up
	// rejecting with ErrQueueFull.
	admitted, queued := r.tryAdmit()
	if !admitted && !queued {
		// Cap reached + queue full (or queueing disabled). Persist a
		// failed Record so operators see the rejection in
		// /admin/sync/task/list, then close backends + return.
		rec := &Record{
			TaskID:    task.ID,
			RuleID:    rule.Config.ID,
			Type:      task.Type,
			Status:    executor.StatusFailed,
			StartedAt: time.Now(),
			DoneAt:    time.Now(),
			Error:     ErrQueueFull.Error(),
		}
		_ = r.store.Put(ctx, rec)
		_ = src.Close()
		_ = dst.Close()
		return cloneRecord(rec), ErrQueueFull
	}

	// Admitted (either directly or via the queue). Persist the initial
	// Record. Queued tasks still report Status=running because they will
	// transition to running as soon as a slot frees; operators care about
	// "is this work going to happen" — we expose the queue depth
	// separately via QueueLen for the heartbeat snapshot.
	rec := &Record{
		TaskID:    task.ID,
		RuleID:    rule.Config.ID,
		Type:      task.Type,
		Status:    executor.StatusRunning,
		StartedAt: time.Now(),
	}
	if err := r.store.Put(ctx, rec); err != nil {
		// Persist failed — release the slot we just acquired (or
		// decrement queueLen if queued) before unwinding.
		if admitted {
			r.release()
		} else if queued {
			r.queueLen.Add(-1)
		}
		_ = src.Close()
		_ = dst.Close()
		return nil, fmt.Errorf("persist record: %w", err)
	}

	done := make(chan struct{})
	r.mu.Lock()
	r.waiters[task.ID] = done
	r.mu.Unlock()

	// FIX Q1 + Q2 — per-task cancellable context. Background root
	// (not the inbound HTTP ctx, which represents "stop reading more
	// requests"). Cancel() and Close() both invoke taskCancel to drive
	// queued + running phases off the same surface. taskCancel is
	// deregistered in run()'s defer (or in runAfterWait's cancel path).
	taskCtx, taskCancel := context.WithCancel(context.Background())
	r.cancellersMu.Lock()
	r.cancellers[task.ID] = taskCancel
	r.cancellersMu.Unlock()

	r.wg.Add(1)
	if admitted {
		go r.run(taskCtx, task, src, dst, done)
	} else {
		// Queued: spawn a waiter that blocks until a slot frees, then
		// runs. Decrement queueLen as soon as we own the slot.
		go r.runAfterWait(taskCtx, task, src, dst, done)
	}

	if !wait {
		return cloneRecord(rec), nil
	}
	return r.waitForRecord(ctx, task.ID, done)
}

// run executes one task, updates the record on completion, and signals the
// done channel for any waiters.
func (r *Runner) run(taskCtx context.Context, task *executor.Task, src, dst backend.Backend, done chan struct{}) {
	defer func() {
		// Release the concurrency slot FIRST so a queued task can claim
		// it before we close the done channel (avoids a brief gap where
		// the cap is under-utilised while the next waiter still blocks
		// in slots <-).
		r.release()
		if src != nil {
			_ = src.Close()
		}
		if dst != nil {
			_ = dst.Close()
		}
		r.mu.Lock()
		delete(r.waiters, task.ID)
		r.mu.Unlock()
		r.deregisterCanceller(task.ID)
		close(done)
		r.wg.Done()
	}()

	reporter := r.reporterFactory(task.ID)
	log.LogInfof("tasks: start task=%q rule=%q type=%q", task.ID, task.RuleID, task.Type)
	result := r.exec.Run(taskCtx, task, reporter)

	// Reload to avoid losing concurrent updates (none today, but cheap and
	// future-proof against e.g. a metadata patcher).
	cur, err := r.store.Get(context.Background(), task.ID)
	if err != nil {
		// Record vanished — nothing to update. This only happens if an
		// operator deleted the record mid-run, which is a no-op for the
		// data path. Still log via record_lost path: drop silently.
		return
	}
	cur.Status = result.Status
	cur.DoneAt = result.DoneAt
	cur.Progress = result.Progress
	cur.Error = result.Error
	cur.Mismatches = result.Mismatches
	cur.BenchResult = result.BenchResult
	_ = r.store.Put(context.Background(), cur)
	log.LogInfof("tasks: done task=%q rule=%q status=%q error=%q", cur.TaskID, cur.RuleID, cur.Status, cur.Error)

	// Phase G-3 hook: if the run terminated in failure with a class of
	// error that warrants degradation (vol_not_found / path_not_allowed /
	// auth_failure), flip the rule into StateDegraded so the scheduler
	// stops re-firing it until an operator resumes. Best-effort — failure
	// here is logged via the task record's Error string already.
	//
	// The Runner only holds a narrow RuleLookup; production wires this to
	// the full rules.Store so the type assertion succeeds. Tests pass a
	// stub that is NOT a Store; the assertion fails and we silently skip
	// — which is exactly what the existing tests need.
	if cur.RuleID != "" && result.Status == executor.StatusFailed {
		if class := rules.ClassifyError(result.Error); class.IsDegrading() {
			if store, ok := r.rules.(rules.Store); ok {
				_ = rules.Degrade(context.Background(), store, cur.RuleID, result.Error)
			}
		}
	}

	// Terminal lifecycle hook: fire AFTER the record is persisted and the
	// degrade path has run, so observers see the final-state Record. Guard
	// against panics with a recover — a buggy hook must NOT take the run
	// goroutine down (which would leak the done channel + waiters entry).
	if r.onTerminal != nil {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					// Best-effort log; rules pkg log is in scope already
					// via the imports above. We avoid pulling in
					// util/log here to keep the package import surface
					// stable.
					_ = rec
				}
			}()
			r.onTerminal(cloneRecord(cur))
		}()
	}
}

// runAfterWait is the queued-task variant of run. It blocks until the
// counting-semaphore yields a slot, decrements queueLen, then delegates
// to run for the actual work. Spawned by triggerRule when the cap is
// reached but the queue has room.
//
// FIX Q1 + Q2 — the slot wait honours taskCtx (per-task Cancel) and
// closedCh (Runner Close). Aborts before run() ever calls into the
// executor, so a queued cancel + an in-flight shutdown both terminate
// cleanly instead of either silently running anyway (Q1) or panicking
// on a torn-down executor (Q2).
//
// SAFETY: r.slots is guaranteed non-nil here because runAfterWait is only
// ever spawned when tryAdmit returned (false, true), which itself
// requires r.maxConcurrent > 0 (i.e. r.slots != nil).
func (r *Runner) runAfterWait(taskCtx context.Context, task *executor.Task, src, dst backend.Backend, done chan struct{}) {
	select {
	case r.slots <- struct{}{}:
		// Both this arm and taskCtx.Done() can be ready simultaneously when
		// a cancel arrives at the same instant the slot becomes free. Go's
		// select picks randomly, so we may land here even though the task
		// was already cancelled. Check Err() after acquiring the slot and
		// treat a non-nil error as a cancel: release the slot and abort
		// without ever starting the executor.
		if err := taskCtx.Err(); err != nil {
			r.release()
			r.abortQueued(task, src, dst, done, err.Error())
			return
		}
		// Slot acquired and context is live — proceed to run().
		// run() owns wg.Done + all cleanup; queueLen is decremented here
		// (not inside abortQueued) because the slot was legitimately taken.
		r.queueLen.Add(-1)
		r.run(taskCtx, task, src, dst, done)
		return
	case <-taskCtx.Done():
		r.abortQueued(task, src, dst, done, taskCtx.Err().Error())
		return
	case <-r.closedCh:
		r.abortQueued(task, src, dst, done, "runner closed")
		return
	}
}

// abortQueued tears down a task that was cancelled BEFORE acquiring a
// concurrency slot. Persists a cancelled Record so operators see the
// outcome, closes backends, deregisters waiters/cancellers and decrements
// the queue depth.
func (r *Runner) abortQueued(task *executor.Task, src, dst backend.Backend, done chan struct{}, reason string) {
	defer func() {
		r.queueLen.Add(-1)
		if src != nil {
			_ = src.Close()
		}
		if dst != nil {
			_ = dst.Close()
		}
		r.mu.Lock()
		delete(r.waiters, task.ID)
		r.mu.Unlock()
		r.deregisterCanceller(task.ID)
		close(done)
		r.wg.Done()
	}()
	// Best-effort update. If the record was deleted between persist and
	// abort, swallow the error: the cancel intent is already on the wire.
	now := time.Now()
	cur, err := r.store.Get(context.Background(), task.ID)
	if err != nil {
		return
	}
	cur.Status = executor.StatusCancelled
	cur.DoneAt = now
	cur.Error = reason
	_ = r.store.Put(context.Background(), cur)
	if r.onTerminal != nil {
		func() {
			defer func() { _ = recover() }()
			r.onTerminal(cloneRecord(cur))
		}()
	}
}

// deregisterCanceller removes the per-task cancel func from the registry.
// Idempotent — safe to call from both the normal exit path and the
// queued-abort path.
func (r *Runner) deregisterCanceller(taskID string) {
	r.cancellersMu.Lock()
	delete(r.cancellers, taskID)
	r.cancellersMu.Unlock()
}

// tryAdmit attempts to admit a task to either the running set or the
// bounded queue. Returns (admitted, queued):
//
//   - (true, false):  slot acquired; caller spawns run() immediately.
//   - (false, true):  queued; caller spawns runAfterWait().
//   - (false, false): cap + queue full; caller returns ErrQueueFull.
//
// When r.slots is nil (unlimited mode, the default) this always returns
// (true, false) — pre-fix behavior is preserved verbatim.
func (r *Runner) tryAdmit() (admitted, queued bool) {
	if r.slots == nil {
		return true, false
	}
	select {
	case r.slots <- struct{}{}:
		return true, false
	default:
	}
	if r.maxQueue <= 0 {
		return false, false
	}
	// Atomically bump queueLen iff it is below cap. We use a CAS-style
	// loop on the atomic to avoid the read-then-add race where two
	// concurrent rejectees could both observe queueLen=maxQueue-1 and
	// both succeed.
	for {
		cur := r.queueLen.Load()
		if int(cur) >= r.maxQueue {
			return false, false
		}
		if r.queueLen.CompareAndSwap(cur, cur+1) {
			return false, true
		}
	}
}

// release frees one concurrency slot. Safe to call when r.slots is nil
// (unlimited mode) — becomes a no-op.
func (r *Runner) release() {
	if r.slots == nil {
		return
	}
	select {
	case <-r.slots:
	default:
		// Slot already drained — should not happen if tryAdmit/release
		// are balanced, but defensive against a future caller that
		// double-releases.
	}
}

// RunningCount returns the number of tasks currently holding a
// concurrency slot. Returns 0 when the cap is disabled (slots == nil) —
// callers that want the true in-flight count in unlimited mode should
// use executor.RunningCount() instead, since the Runner has no
// independent view of how many goroutines it has spawned.
func (r *Runner) RunningCount() int {
	if r.slots == nil {
		return 0
	}
	return len(r.slots)
}

// QueueLen returns the number of tasks currently waiting for a slot.
// Exposed for the heartbeat snapshot — master uses it as one input to
// the load-score so an overloaded node sheds new triggers earlier.
func (r *Runner) QueueLen() int {
	return int(r.queueLen.Load())
}

// Close stops accepting new triggers, cancels every queued task ctx so
// runAfterWait goroutines exit without ever calling into the executor,
// signals every running task to terminate, and blocks until all in-flight
// goroutines have finished. Idempotent — second Close returns nil.
//
// FIX Q2 — must be called BEFORE Executor.Close in server.doShutdown.
// Otherwise queued goroutines blocked on the semaphore would, on slot
// release, race against the executor's nil running map and panic.
func (r *Runner) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(r.closedCh)
	// Cancel every registered task (queued + running). Either phase
	// honours the per-task ctx; ordering doesn't matter.
	r.cancellersMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(r.cancellers))
	for _, c := range r.cancellers {
		cancels = append(cancels, c)
	}
	r.cancellersMu.Unlock()
	for _, c := range cancels {
		c()
	}
	r.wg.Wait()
	return nil
}

// TriggerBench queues a bench task for execution. For S3/SDK storage types,
// rule.BackendEndpoint must be non-nil (pre-resolved by the dispatch caller).
// taskID is the master-assigned identifier; shardIdx is the shard index for
// multi-node fan-out (0 for single-node runs).
func (r *Runner) TriggerBench(ctx context.Context, rule *spec.BenchRule, taskID string, shardIdx int, wait bool) (*Record, error) {
	if rule == nil {
		return nil, errors.New("TriggerBench: nil rule")
	}
	if r.closed.Load() {
		return nil, errors.New("runner: closed")
	}
	if taskID == "" {
		taskID = r.idFactory()
	}

	// Idempotency guard mirroring triggerRule: master may re-send
	// OpSyncNodeRunTask for the same taskID after a brief heartbeat gap.
	// Without this, every retry spawns a new goroutine racing the original
	// over the shared workDir + /tmp/fio-<taskID>-<stage>.json output file —
	// the loser's parseFIOResult returns "no such file or directory" which
	// bench_posix.go silently flattens to a zero-stat record that then
	// overwrites the original's real numbers.
	if existing, getErr := r.store.Get(ctx, taskID); getErr == nil &&
		existing.Status == executor.StatusRunning {
		log.LogInfof("tasks: bench task=%q already running, ignoring duplicate trigger", taskID)
		return cloneRecord(existing), nil
	}

	task := &executor.Task{
		ID:         taskID,
		Type:       executor.TaskTypeBench,
		BenchRule:  rule,
		ShardIndex: shardIdx,
	}

	// For S3/SDK bench, build a backend from the pre-resolved endpoint config.
	if rule.StorageType == spec.BenchStorageS3 || rule.StorageType == spec.BenchStorageSDK {
		if rule.BackendEndpoint == nil {
			return nil, fmt.Errorf("TriggerBench: rule %q requires BackendEndpoint (BackendID=%q) but it is nil", rule.ID, rule.BackendID)
		}
		b, err := r.builder.Build(ctx, rule.BackendEndpoint)
		if err != nil {
			return nil, fmt.Errorf("TriggerBench: build backend for rule %q: %w", rule.ID, err)
		}
		task.SetBenchBackend(b)
	}

	// Persist an initial running record.
	rec := &Record{
		TaskID:    taskID,
		Type:      executor.TaskTypeBench,
		Status:    executor.StatusRunning,
		StartedAt: time.Now(),
	}
	if err := r.store.Put(ctx, rec); err != nil {
		return nil, fmt.Errorf("persist bench record: %w", err)
	}

	done := make(chan struct{})
	r.mu.Lock()
	r.waiters[taskID] = done
	r.mu.Unlock()

	taskCtx, taskCancel := context.WithCancel(context.Background())
	r.cancellersMu.Lock()
	r.cancellers[taskID] = taskCancel
	r.cancellersMu.Unlock()

	admitted, queued := r.tryAdmit()
	if !admitted && !queued {
		// Cap + queue full: persist failure and return.
		r.mu.Lock()
		delete(r.waiters, taskID)
		r.mu.Unlock()
		r.deregisterCanceller(taskID)
		taskCancel()
		close(done)
		rec.Status = executor.StatusFailed
		rec.DoneAt = time.Now()
		rec.Error = ErrQueueFull.Error()
		_ = r.store.Put(context.Background(), rec)
		return cloneRecord(rec), ErrQueueFull
	}

	r.wg.Add(1)
	if admitted {
		go r.run(taskCtx, task, nil, nil, done)
	} else {
		go r.runAfterWait(taskCtx, task, nil, nil, done)
	}

	if !wait {
		return cloneRecord(rec), nil
	}
	return r.waitForRecord(ctx, taskID, done)
}

// On ctx cancellation the task keeps running; only explicit Runner.Cancel
// stops it.
func (r *Runner) waitForRecord(ctx context.Context, taskID string, done <-chan struct{}) (*Record, error) {
	select {
	case <-done:
		return r.store.Get(ctx, taskID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// defaultIDFactory produces monotonically increasing per-Runner IDs. Format
// "t-<unixnano>-<seq>" — collision-free within one Runner without external
// deps.
func (r *Runner) defaultIDFactory() string {
	r.mu.Lock()
	r.idSeq++
	seq := r.idSeq
	r.mu.Unlock()
	return fmt.Sprintf("t-%d-%d", time.Now().UnixNano(), seq)
}

// buildTask converts a persisted Rule + already-built backends into an
// executor.Task. The mapping is deliberately mechanical — semantic
// validation lives in syncnode/config.go and rules/handlers.go.
func buildTask(rule *rules.Rule, src, dst backend.Backend, taskID string) *executor.Task {
	cfg := rule.Config
	return &executor.Task{
		ID:                 taskID,
		RuleID:             cfg.ID,
		Type:               executor.TaskType(cfg.Type),
		Src:                src,
		Dst:                dst,
		SrcPath:            endpointPath(&cfg.Src),
		DstPath:            endpointPath(&cfg.Dst),
		AfterCopy:          executor.AfterCopy(cfg.AfterCopy),
		DownloadStrategy:   executor.DownloadStrategy(cfg.DownloadStrategy),
		OnMismatch:         executor.OnMismatch(cfg.OnMismatch),
		SampleStrategy:     cfg.SampleStrategy,
		SampleRate:         cfg.SampleRate,
		BandwidthLimitMBps: cfg.BandwidthLimitMBps,
		Parallelism:        cfg.Parallelism,
		// Data-integrity P0/P1/P2 knobs. All four are opt-in; default zero
		// values preserve legacy behaviour. validateTask in the executor
		// enforces the AfterCopy=verify_then_delete_src ⇒ ChecksumMode=strong
		// invariant before Run() proceeds.
		ChecksumMode:    cfg.ChecksumMode,
		OnSourceMutated: cfg.OnSourceMutated,
		MaxRetries:      cfg.MaxRetries,
		ResumeEnabled:   cfg.ResumeEnabled,
		// OnExisting (rclone overwrite-policy parity, 子项 3): forwarded
		// untouched; validateTask in the executor normalises the empty
		// string + rejects unknown values + enforces type=move互斥.
		OnExisting: cfg.OnExisting,
		// OnSymlink (rclone local-symlink parity, 子项 1): forwarded so
		// validateTask can apply the whitelist as a second defence; the
		// actual file-walk behaviour lives in syncnode/backend/local and
		// reads the value off the EndpointConfig (mirrored above).
		OnSymlink: cfg.OnSymlink,
		// DryRun / Confirm (rclone-gap 子项 2): forwarded so the executor
		// sees them on validateTask + syncOneFile. Confirm only matters
		// for destructive tasks (type=move OR AfterCopy=verify_then_delete_src)
		// and validateTask rejects Confirm=true + DryRun=false on those.
		DryRun:  cfg.DryRun,
		Confirm: cfg.Confirm,
	}
}

// endpointPath picks the right path field for an endpoint: cfs/local use
// Path, s3 uses Prefix. Both are pre-validated by config parsing so the
// runner doesn't need to fail loudly on unknown kinds — empty string is a
// safe default ("root of the backend").
func endpointPath(ep *spec.EndpointConfig) string {
	switch ep.Kind {
	case "s3", "tos", "bos":
		return ep.Prefix
	default:
		return ep.Path
	}
}

// nextRetryID derives a fresh task id from an existing one. The convention
// is "<base>-r<n>", incrementing n on each retry so the chain is human-
// readable.
//
//	"t-123"        → "t-123-r1"
//	"t-123-r1"     → "t-123-r2"
//	"t-123-r9"     → "t-123-r10"
func nextRetryID(prev string) string {
	idx := strings.LastIndex(prev, "-r")
	if idx < 0 {
		return prev + "-r1"
	}
	suffix := prev[idx+2:]
	n := 0
	for _, c := range suffix {
		if c < '0' || c > '9' {
			// not a clean -r<num> suffix; treat the whole thing as base
			return prev + "-r1"
		}
		n = n*10 + int(c-'0')
	}
	return fmt.Sprintf("%s-r%d", prev[:idx], n+1)
}
