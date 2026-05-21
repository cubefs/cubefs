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
	"github.com/cubefs/cubefs/syncnode/spec"
)

// TaskType identifies which data-path the executor runs for a Task.
type TaskType string

const (
	TaskTypeSync  TaskType = "sync"  // src → dst, retention on dst
	TaskTypeLoad  TaskType = "load"  // src → dst with temp_rename, strict verify
	TaskTypeCheck TaskType = "check" // both-sided integrity check, no data move
	TaskTypeBench TaskType = "bench" // distributed benchmark (S3 or POSIX/fio)
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

	// ShardIndex / ShardTotal: cross-node fan-out shard descriptor (§9 P1-7).
	// When ShardTotal > 1 the producer loops in sync_task.go / load_task.go
	// skip every entry whose hashed key does not map to ShardIndex. Both
	// fields default to 0 / 0 which disables sharding and preserves the
	// pre-P1-7 single-node behaviour (every entry kept).
	//
	// ShardIndex must satisfy 0 <= ShardIndex < ShardTotal. The math is
	// implemented in shard.go (ShouldKeep / shardKey). The split is
	// deterministic so a re-dispatched sub-task on a new owner reproduces
	// exactly the same subset.
	ShardIndex int
	ShardTotal int

	// ShardPrefixes (P2-5) opts the producer loop into prefix-mode
	// sharding: when non-empty, the loop keeps only entries whose key
	// has one of the listed strings as a prefix, ignoring the hash
	// math. Master populates this from SyncRule.Config.ShardPrefixes
	// (explicit mode) or from an OpSyncNodeListPrefixes probe (auto
	// mode). Empty (nil or len==0) preserves hash-mode behaviour.
	ShardPrefixes []string

	// BenchRule carries the benchmark configuration for TaskTypeBench tasks.
	// Nil for all other task types.
	BenchRule *spec.BenchRule
	// benchBackend is the pre-built backend for S3/SDK bench tasks.
	// Built by the Runner before the Task is submitted to the executor.
	benchBackend backend.Backend

	// ChecksumMode controls post-copy verification strictness (P0).
	//   "" / "size_etag" → legacy size + (etag when both have one)
	//   "strong"         → compute sha256 on src during transfer; verify
	//                      against dst checksum (native or metadata sha256).
	//                      REQUIRED for AfterCopy=verify_then_delete_src.
	ChecksumMode string

	// OnSourceMutated controls behaviour when src key changes (size/mtime/
	// etag) between the pre-transfer Head and post-transfer Head (P1).
	//   ""       → disabled (no Pre/Post-Head, legacy behaviour)
	//   "fail"   → error the file; FilesFailed++; src is never deleted
	//   "skip"   → log + FilesSkipped++; dst is rolled back; src is never
	//              deleted
	//   "retry"  → re-fetch and re-upload up to MaxRetries; failed after
	//              exhaustion
	OnSourceMutated string

	// MaxRetries is the per-file retry cap (P2). 0 means 1 attempt total
	// (legacy behaviour). Backoff is exponential with a 30s cap.
	MaxRetries int

	// ResumeEnabled toggles the breakpoint-resume code path (P2). Default
	// off for safety; operators opt in. When true, executor consults
	// bolt.InProgressStore at file start (resume from BytesDone /
	// UploadID) and clears the breakpoint on successful Put.
	ResumeEnabled bool
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

	// For Bench tasks: the shard result returned by the benchmark runner.
	// Nil for non-bench task types.
	BenchResult *spec.BenchShardResult
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
	MismatchMissingDst MismatchReason = "missing_dst"
	MismatchMissingSrc MismatchReason = "missing_src"
	MismatchSizeDiffer MismatchReason = "size_mismatch"
	MismatchETagDiffer MismatchReason = "etag_mismatch"
	MismatchMtimeNewer MismatchReason = "src_newer"
)

// maxSkipSamples is the per-task cap on collected skipped-file keys sent
// to the master. Enough to show a representative sample without inflating
// heartbeat payloads.
const maxSkipSamples = 200

// skipSampler collects up to maxSkipSamples skipped-file keys under a
// mutex. Only the live Progress holds a Sampler; snapshot copies carry
// SkippedSamples []string instead.
type skipSampler struct {
	mu      sync.Mutex
	samples []string
}

func (s *skipSampler) add(key string) {
	s.mu.Lock()
	if len(s.samples) < maxSkipSamples {
		s.samples = append(s.samples, key)
	}
	s.mu.Unlock()
}

func (s *skipSampler) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return nil
	}
	out := make([]string, len(s.samples))
	copy(out, s.samples)
	return out
}

// Progress holds an in-flight snapshot. All numeric fields are written via
// atomic helpers so Reporter implementations can read without locking.
// Sampler is a pointer so Progress can be copied as a value type (used by
// snapshotProgress); snapshot copies populate SkippedSamples instead.
type Progress struct {
	FilesTotal           int64   `json:"filesTotal"`
	FilesDone            int64   `json:"filesDone"`
	FilesSkipped         int64   `json:"filesSkipped"`
	FilesFailed          int64   `json:"filesFailed"`
	BytesTotal           int64   `json:"bytesTotal"`
	BytesDone            int64   `json:"bytesDone"`
	BytesSkipped         int64   `json:"bytesSkipped"`
	ThroughputMBps       float64 `json:"throughputMBps"`
	CurrentBandwidthMBps float64 `json:"currentBandwidthMBps,omitempty"`

	// SkippedSamples is populated only in snapshot copies (not in the live
	// Progress held by Run). It carries up to maxSkipSamples skipped keys.
	SkippedSamples []string `json:"skippedSamples,omitempty"`

	// Sampler is not serialised; only the live Progress initialises it.
	Sampler *skipSampler `json:"-"`
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

func (NoopReporter) OnFileStart(string, int64)       {}
func (NoopReporter) OnFileDone(string, int64, error) {}
func (NoopReporter) OnProgress(Progress)             {}

// Executor is the long-lived runner. One Executor handles many Tasks
// concurrently, bounded by maxConcurrentTasks. Each Task gets its own
// goroutine pool sized by Task.Parallelism (or the executor default).
type Executor struct {
	opts options

	mu       sync.Mutex
	running  map[string]context.CancelFunc // task_id → cancel
	progMap  map[string]*Progress          // task_id → in-flight progress pointer
	startMap map[string]time.Time          // task_id → task startedAt (for correct ThroughputMBps in snapshots)
	trackers map[string]*bandwidthTracker  // task_id → rolling-window bandwidth tracker
	resultCh chan Result

	// inprogress is the optional breakpoint store consulted by syncOneFile
	// when Task.ResumeEnabled is true. nil disables the resume path
	// entirely (preserves legacy behaviour and keeps unit tests honest
	// about what's wired).
	inprogress InProgressStore

	// closed is set by Close(); Run() refuses to start once true. Combined
	// with the nil-map guard below this closes the race where a queued
	// Runner goroutine could call Run() against a torn-down executor and
	// nil-deref into running map.
	closed atomic.Bool
}

// ErrExecutorClosed is returned (via Result.Error) by Run when the
// executor has been Close()'d. Callers — primarily Runner.runAfterWait —
// treat this as "task terminated cancelled".
var ErrExecutorClosed = errors.New("executor: closed")

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

	// inprogress is the optional breakpoint store. Plumbed through here so
	// the construction site can use the existing Option pattern; copied
	// onto the Executor in New().
	inprogress InProgressStore
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

// WithInProgressStore wires the breakpoint store consumed by syncOneFile
// when Task.ResumeEnabled is true. Pass nil (or omit) to leave resume
// disabled; legacy callers and tests that don't need P2 should not set
// this. The store is shared by reference — the caller owns its lifetime.
func WithInProgressStore(s InProgressStore) Option {
	return func(o *options) {
		o.inprogress = s
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
		opts:       o,
		running:    make(map[string]context.CancelFunc),
		progMap:    make(map[string]*Progress),
		startMap:   make(map[string]time.Time),
		trackers:   make(map[string]*bandwidthTracker),
		resultCh:   make(chan Result, 256),
		inprogress: o.inprogress,
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
	// FIX Q2 — refuse to start when the executor has been Close()'d.
	// The nil-map guard below catches the same race even if a caller
	// races past this check, but checking the flag first lets the fast
	// path return without taking the mu.
	if e.closed.Load() {
		now := time.Now()
		return Result{
			TaskID:    t.ID,
			Status:    StatusCancelled,
			Error:     ErrExecutorClosed.Error(),
			StartedAt: now,
			DoneAt:    now,
		}
	}

	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.mu.Lock()
	if e.running == nil {
		// Closed between the atomic check and acquiring mu — bail without
		// touching the nil map. Returning a cancelled Result keeps
		// Runner's bookkeeping consistent (it persists Cancelled and
		// fires the terminal hook).
		e.mu.Unlock()
		now := time.Now()
		return Result{
			TaskID:    t.ID,
			Status:    StatusCancelled,
			Error:     ErrExecutorClosed.Error(),
			StartedAt: now,
			DoneAt:    now,
		}
	}
	e.running[t.ID] = cancel
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		if e.running != nil {
			delete(e.running, t.ID)
		}
		e.mu.Unlock()
	}()

	startedAt := time.Now()
	var (
		progress   Progress
		mismatches []Mismatch
		runErr     error
	)
	progress.Sampler = &skipSampler{}

	// Register in-flight progress pointer, startedAt, and bandwidth tracker
	// so RunningSnapshots can produce accurate per-task snapshots.
	tracker := &bandwidthTracker{}
	e.mu.Lock()
	if e.progMap != nil {
		e.progMap[t.ID] = &progress
		e.startMap[t.ID] = startedAt
		e.trackers[t.ID] = tracker
	}
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		if e.progMap != nil {
			delete(e.progMap, t.ID)
			delete(e.startMap, t.ID)
			delete(e.trackers, t.ID)
		}
		e.mu.Unlock()
	}()
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
				tracker.record(atomic.LoadInt64(&progress.BytesDone))
				snap := snapshotProgress(&progress, startedAt, tracker)
				r.OnProgress(snap)
			}
		}
	}()

	var benchResult *spec.BenchShardResult
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
	case TaskTypeBench:
		benchResult, runErr = e.runBench(taskCtx, t)
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
	finalProg := snapshotProgress(&progress, startedAt, tracker)
	r.OnProgress(finalProg)
	return Result{
		TaskID:      t.ID,
		Status:      status,
		StartedAt:   startedAt,
		DoneAt:      doneAt,
		Progress:    finalProg,
		Error:       errMsg,
		Mismatches:  mismatches,
		BenchResult: benchResult,
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

// RunningSnapshots returns a map of taskID → current progress snapshot for
// every in-flight task. Reads progress atomically from each task's live
// Progress pointer. Callers receive value copies — safe to use after the
// lock is released. Returns nil when the executor has been closed.
func (e *Executor) RunningSnapshots() map[string]Progress {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.progMap == nil || len(e.progMap) == 0 {
		return nil
	}
	out := make(map[string]Progress, len(e.progMap))
	for id, p := range e.progMap {
		start := e.startMap[id]
		out[id] = snapshotProgress(p, start, e.trackers[id])
	}
	return out
}

// Close cancels all in-flight tasks and releases internal resources.
//
// FIX Q2 — sets the closed flag atomically BEFORE clearing the running
// map. Combined with the flag + nil-map guards in Run(), a queued
// Runner goroutine that calls Run() after Close() now gets a cancelled
// Result instead of panicking on a nil-map write.
func (e *Executor) Close() error {
	e.closed.Store(true)
	e.mu.Lock()
	for _, cancel := range e.running {
		cancel()
	}
	e.running = nil
	e.progMap = nil
	e.startMap = nil
	e.trackers = nil
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
	// Bench tasks do not use Src/Dst backends; all other types require them.
	if t.Type != TaskTypeBench && (t.Src == nil || t.Dst == nil) {
		return errors.New("task.Src and task.Dst must be non-nil")
	}
	switch t.Type {
	case TaskTypeSync, TaskTypeLoad, TaskTypeCheck, TaskTypeBench:
	default:
		return fmt.Errorf("invalid task.Type: %q", t.Type)
	}
	// P0 防呆: 把"老用户没读 release note 就升级"导致的静默回退堵住——必须显式选
	// strong 才能开 verify_then_delete_src，否则核心搬运语义会退化到只 size 比对。
	if t.AfterCopy == AfterCopyVerifyThenDeleteSrc && t.ChecksumMode != "strong" {
		return fmt.Errorf("verify_then_delete_src requires checksumMode=strong, got %q", t.ChecksumMode)
	}
	return nil
}

// snapshotProgress reads the atomic progress fields and computes throughput.
// tracker may be nil (tests or tasks that have no bandwidth tracker).
func snapshotProgress(p *Progress, startedAt time.Time, tracker *bandwidthTracker) Progress {
	elapsed := time.Since(startedAt).Seconds()
	out := Progress{
		FilesTotal:   atomic.LoadInt64(&p.FilesTotal),
		FilesDone:    atomic.LoadInt64(&p.FilesDone),
		FilesSkipped: atomic.LoadInt64(&p.FilesSkipped),
		FilesFailed:  atomic.LoadInt64(&p.FilesFailed),
		BytesTotal:   atomic.LoadInt64(&p.BytesTotal),
		BytesDone:    atomic.LoadInt64(&p.BytesDone),
		BytesSkipped: atomic.LoadInt64(&p.BytesSkipped),
	}
	if elapsed > 0 {
		out.ThroughputMBps = float64(out.BytesDone) / 1024.0 / 1024.0 / elapsed
	}
	if tracker != nil {
		out.CurrentBandwidthMBps = tracker.currentMBps()
	}
	if p.Sampler != nil {
		out.SkippedSamples = p.Sampler.snapshot()
	}
	return out
}

// SetBenchBackend sets the pre-built backend for S3/SDK bench tasks.
func (t *Task) SetBenchBackend(b backend.Backend) { t.benchBackend = b }

// runBench is the dispatch point for bench tasks. The actual benchmark logic
// lives in bench_posix.go and bench_s3.go. Bench tasks do not use the
// executor's Src/Dst backend fields — they carry their own configuration
// via BenchRule, and S3/SDK tasks receive a pre-built backend via benchBackend.
func (e *Executor) runBench(ctx context.Context, t *Task) (*spec.BenchShardResult, error) {
	if t.BenchRule == nil {
		return nil, fmt.Errorf("bench task has nil BenchRule")
	}
	rule := t.BenchRule
	pushIntervalSec := 5 // default; could be wired from config in future

	switch rule.StorageType {
	case spec.BenchStoragePosix:
		return runBenchPosix(ctx, rule, t.ID, t.ShardIndex, pushIntervalSec)
	case spec.BenchStorageS3, spec.BenchStorageSDK:
		if t.benchBackend == nil {
			return nil, fmt.Errorf("bench task for storage type %q has nil backend", rule.StorageType)
		}
		return runBenchS3(ctx, rule, t.ID, t.ShardIndex, t.benchBackend, pushIntervalSec)
	case spec.BenchStorageMdtest:
		return runBenchMdtest(ctx, rule, t.ID, t.ShardIndex, pushIntervalSec)
	default:
		return nil, fmt.Errorf("unknown bench storage type: %q", rule.StorageType)
	}
}

// buildTransferLimiter composes the per-transfer bandwidth limiter from
// the task (layer 1), rule (layer 2), node (layer 3) and per-backend
// (layer 4) buckets. Returns nil if no layer is configured — callers
// should treat nil as "skip wrapping" so the unthrottled fast path stays
// branch-free.
//
// Both src and dst contribute layer-4 buckets; in the common case where
// neither has a configured backend cap, this still folds the task / rule
// / node layers into the same Composite. Layer 2 (per-rule) is only added
// when t.RuleID is non-empty AND master has installed a quota for it
// (heartbeat-reply driven; see master_client.go's sendHeartbeat).
func (e *Executor) buildTransferLimiter(t *Task) ratelimit.Limiter {
	if t == nil {
		return nil
	}
	var layers []ratelimit.Limiter
	if t.BandwidthLimitMBps > 0 {
		layers = append(layers, ratelimit.NewBucket(t.BandwidthLimitMBps))
	}
	if reg := e.opts.rateLimits; reg != nil {
		// Layer 2 (per-rule). t.RuleID may be empty for ad-hoc tasks; the
		// bucket is absent until master ships a quota for this rule.
		if t.RuleID != "" {
			if rb := reg.RuleBucket(t.RuleID); rb != nil {
				layers = append(layers, rb)
			}
		}
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
