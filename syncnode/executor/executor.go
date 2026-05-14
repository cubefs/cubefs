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

// Package executor runs sync / load / check Tasks against pairs of Backend
// instances. Each Task is independent — the executor doesn't own any global
// state; scheduling, queueing, persistence and master coordination live in
// higher-level packages (Phase E + F).
//
// See design.md §3.3 (task model) + §8 (data flows) + §9 Phase D.
package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/ratelimit"
)

// TaskType identifies which data-path the executor runs for a Task.
type TaskType string

const (
	TaskTypeSync  TaskType = "sync"  // src → dst, retention on dst
	TaskTypeLoad  TaskType = "load"  // src → dst with temp_rename, strict verify
	TaskTypeCheck TaskType = "check" // both-sided integrity check, no data move
)

// Status is the terminal state of a Task. Pending / Running are tracked by
// the scheduler (Phase F), not in this struct.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusDone      Status = "done"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// AfterCopy controls source disposition after a successful sync entry.
type AfterCopy string

const (
	AfterCopyKeep                AfterCopy = "keep"
	AfterCopyVerifyThenDeleteSrc AfterCopy = "verify_then_delete_src"
)

// DownloadStrategy controls how Load lands data at the destination.
type DownloadStrategy string

const (
	DownloadStrategyTempRename DownloadStrategy = "temp_rename" // default
	DownloadStrategyDirect     DownloadStrategy = "direct"
)

// OnMismatch controls Check task behaviour when an entry pair disagrees.
type OnMismatch string

const (
	OnMismatchAlert   OnMismatch = "alert"    // default
	OnMismatchAutoFix OnMismatch = "auto_fix" // schedule sync sub-task
	OnMismatchIgnore  OnMismatch = "ignore"
)

// Task is a single unit of work the executor runs end-to-end.
//
// Fields are intentionally flat (no nested config blobs) so a Task can be
// constructed by either the scheduler (from a Rule) or an API trigger
// (from explicit override params) without going through a YAML round-trip.
type Task struct {
	// Identity
	ID     string
	RuleID string
	Type   TaskType

	// Endpoints (the source-of-truth Backend instances, already constructed
	// from the backend.Pool).
	Src backend.Backend
	Dst backend.Backend

	// Subpath under each endpoint (e.g. rule.src.path or rule.src.prefix).
	// All listing / object-key operations are scoped under these.
	SrcPath string
	DstPath string

	// Behaviour controls.
	Filter           Filter
	Retention        Retention
	AfterCopy        AfterCopy
	DownloadStrategy DownloadStrategy
	OnMismatch       OnMismatch

	// Sampling for Check task. SampleRate ∈ [0,1]. SampleStrategy ∈
	// {"", "random", "oldest", "largest", "full"}; empty = "full".
	SampleStrategy string
	SampleRate     float64

	// Rate limits (per-task, layer 1 of §12.4). Per-rule / per-node /
	// per-backend layers live in the executor's parent (the SyncNode).
	BandwidthLimitMBps int

	// Parallelism: # of concurrent transfer workers WITHIN this task.
	// (Multi-node fan-out is at a higher layer; this is single-node
	// parallelism only.) 0 → use Executor.opts.transfersPerTask default.
	Parallelism int
}

// Result reports the terminal outcome of a Task run.
type Result struct {
	TaskID    string
	Status    Status
	StartedAt time.Time
	DoneAt    time.Time
	Progress  Progress
	Error     string // populated when Status == Failed

	// For Check tasks: the mismatches found. Empty on success / for
	// non-check task types.
	Mismatches []Mismatch
}

// Mismatch is one entry returned by a Check task.
type Mismatch struct {
	Key    string
	Reason MismatchReason
	// Optional context fields
	SrcSize int64
	DstSize int64
	SrcETag string
	DstETag string
}

// MismatchReason is the explanation for one Mismatch.
type MismatchReason string

const (
	MismatchMissingDst   MismatchReason = "missing_dst"
	MismatchMissingSrc   MismatchReason = "missing_src"
	MismatchSizeDiffer   MismatchReason = "size_mismatch"
	MismatchETagDiffer   MismatchReason = "etag_mismatch"
	MismatchMtimeNewer   MismatchReason = "src_newer"
)

// Progress holds an in-flight snapshot. All fields are written via atomic
// helpers so Reporter implementations can read without locking.
type Progress struct {
	FilesTotal     int64
	FilesDone      int64
	FilesSkipped   int64
	FilesFailed    int64
	BytesTotal     int64
	BytesDone      int64
	ThroughputMBps float64
}

// Reporter receives progress callbacks during task execution. Implementers
// must NOT block significantly — these are called on the executor's hot
// path. The metrics-recording Reporter is the canonical production impl;
// tests provide a noop reporter or a recording one.
type Reporter interface {
	OnFileStart(key string, size int64)
	OnFileDone(key string, bytes int64, err error)
	OnProgress(snapshot Progress)
}

// NoopReporter discards all callbacks. Useful for tests / one-off runs.
type NoopReporter struct{}

func (NoopReporter) OnFileStart(string, int64)      {}
func (NoopReporter) OnFileDone(string, int64, error) {}
func (NoopReporter) OnProgress(Progress)             {}

// Executor is the long-lived runner. One Executor handles many Tasks
// concurrently, bounded by maxConcurrentTasks. Each Task gets its own
// goroutine pool sized by Task.Parallelism (or the executor default).
type Executor struct {
	opts options

	mu       sync.Mutex
	running  map[string]context.CancelFunc // task_id → cancel
	resultCh chan Result
}

type options struct {
	transfersPerTask   int
	bandwidthLimitMBps int // node-level (layer 3); honoured via rateLimits
	progressInterval   time.Duration

	// rateLimits is the layered bandwidth registry described in §12.4.
	// When non-nil, syncOneFile / loadOne wrap source readers with a
	// LimitedReader composing layer 1 (per-task) + layer 3 (node) + layer
	// 4 (per-backend). When nil, transfers run unthrottled — preserving
	// the historical executor behaviour for tests that don't care.
	rateLimits *ratelimit.Registry
}

// Option configures a new Executor.
type Option func(*options)

// WithTransfersPerTask sets the default number of in-task transfer workers.
func WithTransfersPerTask(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.transfersPerTask = n
		}
	}
}

// WithBandwidthLimit sets the node-level (layer 3) bandwidth cap.
//
// When used together with WithRateLimitRegistry, this option is recorded
// for diagnostics but the registry's node bucket is the authority — the
// caller is expected to construct the registry with the same value.
func WithBandwidthLimit(mbps int) Option {
	return func(o *options) {
		if mbps > 0 {
			o.bandwidthLimitMBps = mbps
		}
	}
}

// WithRateLimitRegistry injects the node-level + per-backend rate-limit
// registry. The executor wraps every transfer's source reader with a
// LimitedReader composing per-task (Task.BandwidthLimitMBps), node and
// per-backend buckets. Pass nil (or omit) to run unthrottled.
func WithRateLimitRegistry(reg *ratelimit.Registry) Option {
	return func(o *options) {
		o.rateLimits = reg
	}
}

// WithProgressInterval sets how often OnProgress callbacks fire.
func WithProgressInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.progressInterval = d
		}
	}
}

// New returns a fresh Executor. Caller must Close() when done.
func New(opts ...Option) *Executor {
	o := options{
		transfersPerTask:   4,
		bandwidthLimitMBps: 0, // 0 = unlimited
		progressInterval:   2 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &Executor{
		opts:     o,
		running:  make(map[string]context.CancelFunc),
		resultCh: make(chan Result, 256),
	}
}

// Run executes one Task to completion (or cancellation). Blocks the caller
// goroutine. Reporter is invoked from worker goroutines during the run.
//
// Implementation note: the actual sync/load/check entry points live in
// sync_task.go / load_task.go / check_task.go. This method just dispatches.
func (e *Executor) Run(ctx context.Context, t *Task, r Reporter) Result {
	if r == nil {
		r = NoopReporter{}
	}
	if err := validateTask(t); err != nil {
		return Result{TaskID: t.ID, Status: StatusFailed,
			Error: err.Error(), StartedAt: time.Now(), DoneAt: time.Now()}
	}

	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.mu.Lock()
	e.running[t.ID] = cancel
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.running, t.ID)
		e.mu.Unlock()
	}()

	startedAt := time.Now()
	var (
		progress Progress
		mismatches []Mismatch
		runErr error
	)
	progressTicker := time.NewTicker(e.opts.progressInterval)
	defer progressTicker.Stop()
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		for {
			select {
			case <-taskCtx.Done():
				return
			case <-progressTicker.C:
				snap := snapshotProgress(&progress, startedAt)
				r.OnProgress(snap)
			}
		}
	}()

	switch t.Type {
	case TaskTypeSync:
		runErr = e.runSync(taskCtx, t, r, &progress)
	case TaskTypeLoad:
		runErr = e.runLoad(taskCtx, t, r, &progress)
	case TaskTypeCheck:
		mismatches, runErr = e.runCheck(taskCtx, t, r, &progress)
		// D-5: if the check completed successfully AND the policy is
		// auto_fix, repair every fixable mismatch in-process. A repair
		// failure is reported as the task error so Status flips to Failed;
		// the original Mismatches list is still returned so operators see
		// what was found (fixed or otherwise).
		if runErr == nil && t.OnMismatch == OnMismatchAutoFix && len(mismatches) > 0 {
			runErr = e.runAutoFix(taskCtx, t, mismatches, r, &progress)
		}
	default:
		runErr = fmt.Errorf("unknown task type: %q", t.Type)
	}

	cancel()
	<-progressDone

	doneAt := time.Now()
	status := StatusDone
	errMsg := ""
	if runErr != nil {
		if errors.Is(runErr, context.Canceled) {
			status = StatusCancelled
		} else {
			status = StatusFailed
			errMsg = runErr.Error()
		}
	}
	finalProg := snapshotProgress(&progress, startedAt)
	r.OnProgress(finalProg)
	return Result{
		TaskID:     t.ID,
		Status:     status,
		StartedAt:  startedAt,
		DoneAt:     doneAt,
		Progress:   finalProg,
		Error:      errMsg,
		Mismatches: mismatches,
	}
}

// Cancel asks the running Task with task_id to stop. No-op if not running.
func (e *Executor) Cancel(taskID string) {
	e.mu.Lock()
	cancel, ok := e.running[taskID]
	e.mu.Unlock()
	if ok {
		cancel()
	}
}

// RunningCount returns the current number of in-flight tasks.
func (e *Executor) RunningCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.running)
}

// Close cancels all in-flight tasks and releases internal resources.
func (e *Executor) Close() error {
	e.mu.Lock()
	for _, cancel := range e.running {
		cancel()
	}
	e.running = nil
	e.mu.Unlock()
	return nil
}

// transfersPerTask resolves Task.Parallelism or falls back to default.
func (e *Executor) transfersPerTask(t *Task) int {
	if t.Parallelism > 0 {
		return t.Parallelism
	}
	return e.opts.transfersPerTask
}

// validateTask catches obvious misconfigurations before doing real work.
func validateTask(t *Task) error {
	if t == nil {
		return errors.New("nil task")
	}
	if t.ID == "" {
		return errors.New("task.ID required")
	}
	if t.Src == nil || t.Dst == nil {
		return errors.New("task.Src and task.Dst must be non-nil")
	}
	switch t.Type {
	case TaskTypeSync, TaskTypeLoad, TaskTypeCheck:
	default:
		return fmt.Errorf("invalid task.Type: %q", t.Type)
	}
	return nil
}

// snapshotProgress reads the atomic progress fields and computes throughput.
func snapshotProgress(p *Progress, startedAt time.Time) Progress {
	elapsed := time.Since(startedAt).Seconds()
	out := Progress{
		FilesTotal:   atomic.LoadInt64(&p.FilesTotal),
		FilesDone:    atomic.LoadInt64(&p.FilesDone),
		FilesSkipped: atomic.LoadInt64(&p.FilesSkipped),
		FilesFailed:  atomic.LoadInt64(&p.FilesFailed),
		BytesTotal:   atomic.LoadInt64(&p.BytesTotal),
		BytesDone:    atomic.LoadInt64(&p.BytesDone),
	}
	if elapsed > 0 {
		out.ThroughputMBps = float64(out.BytesDone) / 1024.0 / 1024.0 / elapsed
	}
	return out
}

// buildTransferLimiter composes the per-transfer bandwidth limiter from
// the task (layer 1), node (layer 3) and per-backend (layer 4) buckets.
// Returns nil if no layer is configured — callers should treat nil as
// "skip wrapping" so the unthrottled fast path stays branch-free.
//
// Both src and dst contribute layer-4 buckets; in the common case where
// neither has a configured backend cap, this still folds the task + node
// layers into the same Composite.
func (e *Executor) buildTransferLimiter(t *Task) ratelimit.Limiter {
	if t == nil {
		return nil
	}
	var layers []ratelimit.Limiter
	if t.BandwidthLimitMBps > 0 {
		layers = append(layers, ratelimit.NewBucket(t.BandwidthLimitMBps))
	}
	if reg := e.opts.rateLimits; reg != nil {
		if nb := reg.NodeBucket(); nb != nil {
			layers = append(layers, nb)
		}
		if t.Src != nil {
			if b := reg.BackendBucket(ratelimit.BackendKey{Kind: t.Src.Kind()}); b != nil {
				layers = append(layers, b)
			}
		}
		if t.Dst != nil {
			if b := reg.BackendBucket(ratelimit.BackendKey{Kind: t.Dst.Kind()}); b != nil {
				layers = append(layers, b)
			}
		}
	}
	if len(layers) == 0 {
		return nil
	}
	return ratelimit.NewComposite(layers...)
}
