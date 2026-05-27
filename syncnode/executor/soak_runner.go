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

// S3.2 — Soak 模式 runner。
//
// 设计要点：
//   - RunSoakLoop 是 stage 回调的"装饰器"：把单次 stage 执行包成长跑循环，
//     周期写 checkpoint、超过 MaxRestartCount 则上抛错误终止。
//   - 不引入 goroutine 调度抽象——stage 回调自己跑在调用方 goroutine 上；
//     RunSoakLoop 在另一个 goroutine 里跑 checkpoint ticker。
//   - 当 ResumeFromCheckpoint=true 时，恢复后通过 ctx value 把 ElapsedSec /
//     RestartCount 传给 stage 回调（key 为 soakCtxKey），stage 内部可以读取
//     用于跳过已 ramp 完的段。
//   - 调用方负责保证 spec.SoakControl.DurationSec > 0（Soak 开启）；否则
//     wrapSoakIfEnabled 会直接返回原 stage callback。
package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// soakCtxKey is the unexported context-value key Soak runner uses to forward
// resume hints (ElapsedSec, RestartCount) to the stage callback. Stage code
// that needs the values pulls them via SoakResumeFromContext(ctx). Default
// (no key set) keeps stage code branchless when Soak is disabled.
type soakCtxKeyT struct{}

var soakCtxKey = soakCtxKeyT{}

// SoakResume carries the resume hints embedded in ctx by RunSoakLoop. Stage
// callbacks read it via SoakResumeFromContext to align with already-elapsed
// progress (skip already-finished ramp window, shorten remaining runtime,
// rebuild HDR histogram, …).
//
// Zero value means "no resume, fresh start".
type SoakResume struct {
	ElapsedSec   int
	OpsCompleted int64
	RestartCount int
	Snapshot     []byte // HDR snapshot (gzip+base64-encoded) — stage decodes if needed
}

// SoakResumeFromContext returns the SoakResume previously installed by
// RunSoakLoop. When the ctx carries no Soak resume info the second return
// is false; callers should treat that as the fresh-run path.
func SoakResumeFromContext(ctx context.Context) (SoakResume, bool) {
	if ctx == nil {
		return SoakResume{}, false
	}
	v, ok := ctx.Value(soakCtxKey).(SoakResume)
	return v, ok
}

// withSoakResume installs SoakResume on ctx. Internal helper.
func withSoakResume(ctx context.Context, r SoakResume) context.Context {
	return context.WithValue(ctx, soakCtxKey, r)
}

// SoakStageFunc is the stage callback shape RunSoakLoop drives. Each invocation
// represents one "attempt" — either the initial run, or a restart after a
// previous attempt returned an error. restartCount is 0 on the initial run and
// monotonically increments on each restart.
//
// The callback should honour ctx.Done() (used by RunSoakLoop to stop the loop
// when DurationSec elapses or the parent cancels). Returning context.Canceled
// or context.DeadlineExceeded ends the loop without consuming a restart slot.
type SoakStageFunc func(ctx context.Context, restartCount int) error

// SoakLoopOptions wires RunSoakLoop to its environment. Store / Identifiers
// are required when DurationSec > 0; the runner returns an error otherwise.
//
// Now is injected so tests can drive wall-clock-dependent code deterministically.
// nil means time.Now.
type SoakLoopOptions struct {
	Control spec.SoakControl
	Store   SoakStore
	TaskID  string
	Stage   string
	ShardID int

	// Now lets tests override the wall clock. Production code passes nil
	// and runtime uses time.Now.
	Now func() time.Time

	// OnCheckpoint, when non-nil, is invoked after each successful Save with
	// the just-persisted checkpoint. Used by tests; production code leaves
	// it nil (metrics are updated inside RunSoakLoop directly).
	OnCheckpoint func(cp SoakCheckpoint)

	// SnapshotFn, when non-nil, is called immediately before each Save to
	// produce the HDR histogram snapshot bytes. Stage code may inject a
	// real HDR exporter here; the default nil yields no Snapshot (Save
	// stores cp.Snapshot=nil which is harmless).
	SnapshotFn func() []byte

	// OpsCompletedFn, when non-nil, is called before each Save to read the
	// cumulative ops count. Default nil → OpsCompleted carries the last
	// value the runner saw (zero on first save).
	OpsCompletedFn func() int64
}

// soakSleepBetweenRestarts is exposed as a package var (not const) so tests
// can shorten the inter-restart back-off without waiting 5 real seconds.
// Production default is 5s, matching the design doc.
var soakSleepBetweenRestarts = 5 * time.Second

// RunSoakLoop drives the Soak long-running loop. Behaviour:
//
//  1. If opts.Control.ResumeFromCheckpoint && a checkpoint exists, load it
//     and seed the loop with (ElapsedSec, RestartCount). The stage callback
//     reads the resume info via SoakResumeFromContext.
//  2. Run stage. While it runs, a ticker fires every CheckpointInterval and
//     calls opts.Store.Save with the latest progress.
//  3. When stage returns:
//     - context.Canceled / DeadlineExceeded → loop exits with that error
//       (no restart consumed).
//     - any other non-nil error → if RestartCount < MaxRestartCount, sleep
//       soakSleepBetweenRestarts and restart stage; otherwise return that
//       error wrapped with restart-exhausted context.
//     - nil → loop exits successfully (stage finished its own ramp, etc.).
//  4. When wall-clock elapsed >= DurationSec, the loop exits successfully
//     even if stage is mid-attempt (the runner cancels stage via the inner
//     ctx).
//
// Return values: the terminal error of the loop. nil = soak ran to completion
// (DurationSec reached or stage finished cleanly). context.* errors pass
// through unchanged. A wrapped non-nil otherwise indicates restart-exhausted.
func RunSoakLoop(ctx context.Context, opts SoakLoopOptions, stage SoakStageFunc) error {
	if !opts.Control.Enabled() {
		// Defensive: caller should have checked. Without DurationSec > 0
		// we have no exit criterion and could loop forever; reject loudly.
		return errors.New("soak: RunSoakLoop requires Control.DurationSec > 0")
	}
	if opts.Store == nil {
		return errors.New("soak: RunSoakLoop requires non-nil Store")
	}
	if stage == nil {
		return errors.New("soak: RunSoakLoop requires non-nil stage callback")
	}
	if opts.TaskID == "" || opts.Stage == "" {
		return errors.New("soak: RunSoakLoop requires TaskID and Stage")
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	// Resume hint: load checkpoint if requested. Missing checkpoint is a
	// soft no-op (fresh start). Hard errors abort RunSoakLoop loudly so
	// operators don't silently lose 12h of state to a corrupt db file.
	resume := SoakResume{}
	if opts.Control.ResumeFromCheckpoint {
		cp, err := opts.Store.Load(ctx, opts.TaskID, opts.Stage, opts.ShardID)
		if err != nil {
			return fmt.Errorf("soak: load checkpoint: %w", err)
		}
		if cp != nil {
			resume = SoakResume{
				ElapsedSec:   cp.ElapsedSec,
				OpsCompleted: cp.OpsCompleted,
				RestartCount: cp.RestartCount,
				Snapshot:     cp.Snapshot,
			}
		}
	}

	// loopStart is the wall-clock at which we _started_ this RunSoakLoop
	// invocation. We add resume.ElapsedSec to it so the "elapsed since
	// virtual start of soak" math survives restarts.
	loopStart := now()
	totalDeadline := loopStart.Add(time.Duration(opts.Control.DurationSec-resume.ElapsedSec) * time.Second)
	if !totalDeadline.After(loopStart) {
		// Resumed past the deadline — nothing left to do. Clean up and
		// exit successfully.
		_ = opts.Store.Delete(ctx, opts.TaskID, opts.Stage, opts.ShardID)
		return nil
	}

	// Outer ctx with a hard deadline so stage cancels cleanly when soak ends.
	outerCtx, outerCancel := context.WithDeadline(ctx, totalDeadline)
	defer outerCancel()

	intervalSec := opts.Control.EffectiveCheckpointIntervalSec()
	maxRestart := opts.Control.MaxRestartCount
	if maxRestart < 0 {
		maxRestart = 0
	}

	// progress carries the values the checkpoint ticker uses. The stage
	// itself updates OpsCompleted via opts.OpsCompletedFn (callback at
	// checkpoint time); we don't need a shared mutable struct.
	var (
		restartCount = int64(resume.RestartCount)
		opsSeen      int64 = resume.OpsCompleted
		opsSeenMu    sync.Mutex
	)

	// Checkpoint writer goroutine. Runs until outerCtx is done.
	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		t := time.NewTicker(time.Duration(intervalSec) * time.Second)
		defer t.Stop()
		for {
			select {
			case <-outerCtx.Done():
				return
			case <-t.C:
				elapsed := int(now().Sub(loopStart).Seconds()) + resume.ElapsedSec
				if elapsed < 0 {
					elapsed = 0
				}
				if opts.OpsCompletedFn != nil {
					v := opts.OpsCompletedFn()
					opsSeenMu.Lock()
					opsSeen = v
					opsSeenMu.Unlock()
				}
				var snap []byte
				if opts.SnapshotFn != nil {
					snap = opts.SnapshotFn()
				}
				opsSeenMu.Lock()
				ops := opsSeen
				opsSeenMu.Unlock()
				cp := SoakCheckpoint{
					TaskID:         opts.TaskID,
					Stage:          opts.Stage,
					ShardID:        opts.ShardID,
					ElapsedSec:     elapsed,
					OpsCompleted:   ops,
					LastUpdateUnix: now().Unix(),
					RestartCount:   int(atomic.LoadInt64(&restartCount)),
					Snapshot:       snap,
				}
				if err := opts.Store.Save(outerCtx, cp); err == nil {
					// Successful save: update metrics + caller callback.
					soakObserveCheckpoint(cp)
					if opts.OnCheckpoint != nil {
						opts.OnCheckpoint(cp)
					}
				}
				// Save errors are swallowed: a transient bolt hiccup
				// shouldn't kill an 8h soak. The next tick will retry.
			}
		}
	}()

	// Attempt loop. Each iteration runs the stage callback once.
	var loopErr error
	for {
		// Build the per-attempt ctx and embed resume info so stage code
		// can read it via SoakResumeFromContext.
		attemptElapsed := int(now().Sub(loopStart).Seconds()) + resume.ElapsedSec
		if attemptElapsed < 0 {
			attemptElapsed = 0
		}
		opsSeenMu.Lock()
		ops := opsSeen
		opsSeenMu.Unlock()
		attemptCtx := withSoakResume(outerCtx, SoakResume{
			ElapsedSec:   attemptElapsed,
			OpsCompleted: ops,
			RestartCount: int(atomic.LoadInt64(&restartCount)),
			Snapshot:     resume.Snapshot, // only first attempt sees the prior snapshot
		})

		err := stage(attemptCtx, int(atomic.LoadInt64(&restartCount)))

		// Soak's natural exit: outer deadline tripped. Stage may return
		// ctx.Canceled/DeadlineExceeded or nil — both are success.
		if outerCtx.Err() != nil {
			// Parent ctx cancelled (not deadline) → propagate.
			if ctx.Err() != nil {
				loopErr = ctx.Err()
			}
			break
		}

		if err == nil {
			// Stage finished its own ramp window before DurationSec. Soak
			// treats this as success (the stage knew when to stop).
			break
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Stage observed cancellation; outerCtx wasn't done yet
			// (rare race). Treat as caller cancellation.
			loopErr = err
			break
		}

		// Real error → restart if we still have budget.
		if int(atomic.LoadInt64(&restartCount)) >= maxRestart {
			loopErr = fmt.Errorf("soak: stage %q restart budget exhausted (max=%d): %w",
				opts.Stage, maxRestart, err)
			break
		}
		atomic.AddInt64(&restartCount, 1)
		soakObserveRestart(opts.TaskID, opts.Stage, opts.ShardID, int(atomic.LoadInt64(&restartCount)))

		// Back-off then loop. Sleep is interruptible via outerCtx.
		select {
		case <-outerCtx.Done():
			// Deadline tripped during sleep → exit gracefully.
			if ctx.Err() != nil {
				loopErr = ctx.Err()
			}
			break
		case <-time.After(soakSleepBetweenRestarts):
		}
		if outerCtx.Err() != nil {
			if ctx.Err() != nil {
				loopErr = ctx.Err()
			}
			break
		}
	}

	// Drain the checkpoint goroutine cleanly before returning, otherwise
	// late saves may race with caller-owned store teardown.
	outerCancel()
	<-tickerDone

	// Successful exit (loopErr == nil) → delete the checkpoint so the next
	// fresh run doesn't accidentally resume to a finished soak. On error
	// we keep the checkpoint around for operator inspection / next-pid
	// resume.
	if loopErr == nil {
		_ = opts.Store.Delete(context.Background(), opts.TaskID, opts.Stage, opts.ShardID)
	}
	return loopErr
}

// wrapSoakIfEnabled returns stage as-is when Soak is disabled (DurationSec=0),
// or a wrapped callback that drives stage through RunSoakLoop when enabled.
//
// The wrapped form requires opts.Store / opts.TaskID / opts.Stage to be set
// by the caller. Calling this with Soak enabled but Store == nil returns a
// callback that immediately errors so the misconfiguration surfaces loudly
// rather than silently falling back to one-shot mode.
func wrapSoakIfEnabled(opts SoakLoopOptions, stage SoakStageFunc) SoakStageFunc {
	if !opts.Control.Enabled() {
		return stage
	}
	return func(ctx context.Context, restartCount int) error {
		// restartCount from the outer caller is irrelevant; RunSoakLoop
		// manages its own counter. We ignore it.
		_ = restartCount
		return RunSoakLoop(ctx, opts, stage)
	}
}
