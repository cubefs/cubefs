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
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
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

// ErrRuleTypeMismatch is returned by Save / Load aliases when the rule's
// declared type doesn't match the requested alias.
var ErrRuleTypeMismatch = errors.New("rule type does not match endpoint")

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

	// waiters tracks per-task done channels for wait=true callers. The
	// goroutine running the task closes its channel when the record reaches
	// a terminal status.
	mu      sync.Mutex
	waiters map[string]chan struct{}

	// idSeq feeds the default ID factory (monotonic per Runner).
	idSeq uint64
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

// NewRunner returns a Runner. exec / store / rules / builder must be non-nil.
// Callers own exec and store lifecycles; the Runner does not Close them.
func NewRunner(exec *executor.Executor, store Store, ruleLookup RuleLookup, b BackendBuilder, opts ...RunnerOption) *Runner {
	r := &Runner{
		exec:    exec,
		store:   store,
		rules:   ruleLookup,
		builder: b,
		waiters: make(map[string]chan struct{}),
	}
	r.idFactory = r.defaultIDFactory
	r.reporterFactory = func(string) executor.Reporter { return executor.NoopReporter{} }
	for _, o := range opts {
		o(r)
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
	return r.triggerRule(ctx, rule, "", wait)
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
	return r.triggerRule(ctx, rule, "", wait)
}

// Cancel signals the executor to stop the named task. Returns
// ErrTaskNotFound if no record exists for that taskID.
//
// Note: there is an unavoidable tiny window between Trigger spawning the
// run goroutine and the executor inserting the task into its cancel map.
// We close the per-task done channel only when the task TERMINATES, so
// observing the running record + a stable cancel call is the contract. To
// make Cancel resilient against the race, we re-fire executor.Cancel a
// few times until either the task is observably cancelled (done channel
// fires + status flips) or the budget expires.
func (r *Runner) Cancel(ctx context.Context, taskID string) error {
	if _, err := r.store.Get(ctx, taskID); err != nil {
		return err
	}
	r.mu.Lock()
	done := r.waiters[taskID]
	r.mu.Unlock()

	r.exec.Cancel(taskID)

	if done == nil {
		// Already terminal; nothing to cancel.
		return nil
	}
	// If the task hasn't registered with the executor yet, retry briefly so
	// the cancel actually lands. The executor registers the task at the top
	// of Run; once the run goroutine has handed off control this loop sees
	// at most ~one extra iteration.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			return nil
		case <-time.After(5 * time.Millisecond):
			r.exec.Cancel(taskID)
		}
	}
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
	return r.triggerRule(ctx, rule, newID, false)
}

// triggerRule is the shared core for Trigger / TriggerAs / Retry. If newID
// is empty, a fresh id is allocated by r.idFactory.
func (r *Runner) triggerRule(ctx context.Context, rule *rules.Rule, newID string, wait bool) (*Record, error) {
	src, err := r.builder.Build(ctx, &rule.Config.Src)
	if err != nil {
		return nil, fmt.Errorf("build src backend: %w", err)
	}
	dst, err := r.builder.Build(ctx, &rule.Config.Dst)
	if err != nil {
		_ = src.Close()
		return nil, fmt.Errorf("build dst backend: %w", err)
	}

	if newID == "" {
		newID = r.idFactory()
	}
	task := buildTask(rule, src, dst, newID)

	rec := &Record{
		TaskID:    task.ID,
		RuleID:    rule.Config.ID,
		Type:      task.Type,
		Status:    executor.StatusRunning,
		StartedAt: time.Now(),
	}
	if err := r.store.Put(ctx, rec); err != nil {
		_ = src.Close()
		_ = dst.Close()
		return nil, fmt.Errorf("persist record: %w", err)
	}

	done := make(chan struct{})
	r.mu.Lock()
	r.waiters[task.ID] = done
	r.mu.Unlock()

	// Run in a fresh background context so an HTTP-client disconnect (which
	// cancels ctx) does NOT cancel an in-flight task. Cancellation is
	// explicit via Runner.Cancel.
	go r.run(task, src, dst, done)

	if !wait {
		return cloneRecord(rec), nil
	}
	return r.waitForRecord(ctx, task.ID, done)
}

// run executes one task, updates the record on completion, and signals the
// done channel for any waiters.
func (r *Runner) run(task *executor.Task, src, dst backend.Backend, done chan struct{}) {
	defer func() {
		_ = src.Close()
		_ = dst.Close()
		r.mu.Lock()
		delete(r.waiters, task.ID)
		r.mu.Unlock()
		close(done)
	}()

	reporter := r.reporterFactory(task.ID)
	result := r.exec.Run(context.Background(), task, reporter)

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
	_ = r.store.Put(context.Background(), cur)

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
}

// waitForRecord blocks until the task's done channel fires or ctx is done.
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
	}
}

// endpointPath picks the right path field for an endpoint: cfs/local use
// Path, s3 uses Prefix. Both are pre-validated by config parsing so the
// runner doesn't need to fail loudly on unknown kinds — empty string is a
// safe default ("root of the backend").
func endpointPath(ep *spec.EndpointConfig) string {
	switch ep.Kind {
	case "s3":
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
