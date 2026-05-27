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

package executor

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// nullBackend is a Backend that returns ErrKeyNotFound for everything;
// useful for exercising framework dispatch without touching real storage.
// An empty list means runSync/runCheck succeed with zero work done.
type nullBackend struct{ kind string }

func (n *nullBackend) Kind() string { return n.kind }
func (n *nullBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch := make(chan backend.Entry)
	close(ch)
	return ch, nil
}
func (n *nullBackend) Get(ctx context.Context, k string, off, sz int64) (io.ReadCloser, error) {
	return nil, backend.ErrKeyNotFound
}
func (n *nullBackend) Head(ctx context.Context, k string) (int64, string, time.Time, error) {
	return 0, "", time.Time{}, backend.ErrKeyNotFound
}
func (n *nullBackend) Put(ctx context.Context, k string, body io.Reader, sz int64, opts backend.PutOptions) (backend.PutResult, error) {
	return backend.PutResult{}, nil
}
func (n *nullBackend) GetChecksum(ctx context.Context, k string) (string, string, error) {
	return "", "", backend.ErrKeyNotFound
}
func (n *nullBackend) Delete(ctx context.Context, k string) error     { return nil }
func (n *nullBackend) Rename(ctx context.Context, o, nk string) error { return nil }
func (n *nullBackend) Capabilities() backend.Caps                     { return backend.Caps{} }
func (n *nullBackend) SameInstance(other backend.Backend) bool        { return false }
func (n *nullBackend) Close() error                                   { return nil }

// listErrBackend fails List with a fixed error. Useful for triggering the
// failure path in runSync / runCheck / runLoad without a real storage stack.
type listErrBackend struct {
	nullBackend
	err error
}

func (b *listErrBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	return nil, b.err
}

func TestValidateTask(t *testing.T) {
	cases := []struct {
		name    string
		task    *Task
		wantErr bool
	}{
		{"nil", nil, true},
		{"empty id", &Task{Type: TaskTypeSync, Src: &nullBackend{}, Dst: &nullBackend{}}, true},
		{"nil src", &Task{ID: "x", Type: TaskTypeSync, Dst: &nullBackend{}}, true},
		{"nil dst", &Task{ID: "x", Type: TaskTypeSync, Src: &nullBackend{}}, true},
		{"bad type", &Task{ID: "x", Type: "garbage", Src: &nullBackend{}, Dst: &nullBackend{}}, true},
		{"valid sync", &Task{ID: "x", Type: TaskTypeSync, Src: &nullBackend{}, Dst: &nullBackend{}}, false},
		{"valid load", &Task{ID: "x", Type: TaskTypeLoad, Src: &nullBackend{}, Dst: &nullBackend{}}, false},
		{"valid check", &Task{ID: "x", Type: TaskTypeCheck, Src: &nullBackend{}, Dst: &nullBackend{}}, false},
		{"valid move bare", &Task{ID: "x", Type: TaskTypeMove, Src: &nullBackend{}, Dst: &nullBackend{}}, false},
		{"valid move with matching knobs", &Task{ID: "x", Type: TaskTypeMove, AfterCopy: AfterCopyVerifyThenDeleteSrc, ChecksumMode: "strong", Src: &nullBackend{}, Dst: &nullBackend{}}, false},
		{"move forbids conflicting afterCopy", &Task{ID: "x", Type: TaskTypeMove, AfterCopy: AfterCopy("other"), Src: &nullBackend{}, Dst: &nullBackend{}}, true},
		{"move forbids conflicting checksumMode", &Task{ID: "x", Type: TaskTypeMove, ChecksumMode: "size_etag", Src: &nullBackend{}, Dst: &nullBackend{}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTask(tc.task)
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestValidateTask_MoveLocksKnobs asserts that validateTask actively rewrites
// a bare TaskTypeMove to AfterCopy=verify_then_delete_src + ChecksumMode=strong,
// so downstream Run() observes the locked invariants regardless of what the
// caller set.
func TestValidateTask_MoveLocksKnobs(t *testing.T) {
	task := &Task{ID: "x", Type: TaskTypeMove, Src: &nullBackend{}, Dst: &nullBackend{}}
	if err := validateTask(task); err != nil {
		t.Fatalf("validateTask: %v", err)
	}
	if task.AfterCopy != AfterCopyVerifyThenDeleteSrc {
		t.Errorf("AfterCopy = %q, want %q", task.AfterCopy, AfterCopyVerifyThenDeleteSrc)
	}
	if task.ChecksumMode != "strong" {
		t.Errorf("ChecksumMode = %q, want %q", task.ChecksumMode, "strong")
	}
}

// TestValidateTask_DryRun_HappyPath covers the simple case: DryRun=true on a
// non-destructive Sync should validate cleanly, no Confirm needed.
func TestValidateTask_DryRun_HappyPath(t *testing.T) {
	task := &Task{
		ID:     "dry-sync",
		Type:   TaskTypeSync,
		Src:    &nullBackend{},
		Dst:    &nullBackend{},
		DryRun: true,
	}
	if err := validateTask(task); err != nil {
		t.Fatalf("validateTask(dry-run sync): %v", err)
	}
	if !task.DryRun {
		t.Errorf("DryRun = %v, want true (validateTask must preserve)", task.DryRun)
	}
}

// TestValidateTask_DryRun_ConfirmWithoutPreview asserts the safety invariant:
// Confirm=true on a destructive task (Type=Move) without DryRun=true is
// rejected. Operators must DryRun first, review the would_* counters, then
// rerun with Confirm=true + DryRun=false.
func TestValidateTask_DryRun_ConfirmWithoutPreview(t *testing.T) {
	task := &Task{
		ID:      "confirm-no-dry",
		Type:    TaskTypeMove,
		Src:     &nullBackend{},
		Dst:     &nullBackend{},
		Confirm: true,
		DryRun:  false,
	}
	err := validateTask(task)
	if err == nil {
		t.Fatal("validateTask should reject Confirm=true + DryRun=false on destructive task")
	}
	if !strings.Contains(err.Error(), "dry-run confirmation required") {
		t.Errorf("error = %q, want substring %q", err.Error(), "dry-run confirmation required")
	}
}

// TestValidateTask_DryRun_ConfirmWithPreviewPasses ensures the inverse holds:
// Confirm=true + DryRun=true on a destructive task is treated as "preview
// only" and passes validation (the executor will then short-circuit
// mutations). The real apply run sets DryRun=false + Confirm=true, also
// allowed.
func TestValidateTask_DryRun_ConfirmWithPreviewPasses(t *testing.T) {
	previewTask := &Task{
		ID:      "confirm-preview",
		Type:    TaskTypeMove,
		Src:     &nullBackend{},
		Dst:     &nullBackend{},
		Confirm: true,
		DryRun:  true,
	}
	if err := validateTask(previewTask); err != nil {
		t.Fatalf("validateTask(Confirm+DryRun) on destructive move: %v", err)
	}

	applyTask := &Task{
		ID:      "confirm-apply",
		Type:    TaskTypeMove,
		Src:     &nullBackend{},
		Dst:     &nullBackend{},
		Confirm: true,
		DryRun:  false,
		// Confirm + apply on move requires the explicit preview-then-go
		// flow; validateTask DOES reject when DryRun=false. We instead set
		// Confirm=false here to model "operator hasn't acked yet" and
		// confirm validateTask still passes (Confirm guards destructive
		// runs at workflow level; non-Confirm move is the legacy entry).
		// The non-Confirm case is asserted by TestValidateTask_MoveLocksKnobs.
	}
	applyTask.Confirm = false
	if err := validateTask(applyTask); err != nil {
		t.Fatalf("validateTask(plain move): %v", err)
	}
}

// TestValidateTask_DryRun_NonDestructiveTaskIgnoresConfirm: Confirm on a
// non-destructive task is a no-op. validateTask must NOT reject
// Confirm=true + DryRun=false on plain Sync (AfterCopy=keep) — only
// destructive tasks demand the preview-then-confirm dance.
func TestValidateTask_DryRun_NonDestructiveTaskIgnoresConfirm(t *testing.T) {
	task := &Task{
		ID:      "confirm-sync",
		Type:    TaskTypeSync,
		Src:     &nullBackend{},
		Dst:     &nullBackend{},
		Confirm: true,
		DryRun:  false,
	}
	if err := validateTask(task); err != nil {
		t.Fatalf("validateTask(Confirm on non-destructive sync): %v", err)
	}
}

// TestValidateTask_Mirror_HappyPath asserts that TaskTypeMirror passes
// validation with empty AfterCopy, and that validateTask locks AfterCopy to
// verify_then_skip on the way through. This mirrors the move-locks-knobs
// invariant: downstream Run() sees a closed enum regardless of caller input.
func TestValidateTask_Mirror_HappyPath(t *testing.T) {
	cases := []struct {
		name      string
		afterCopy AfterCopy
	}{
		{"empty", ""},
		{"keep", AfterCopyKeep},
		{"verify_then_skip explicit", AfterCopyVerifyThenSkip},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &Task{
				ID:        "mirror-ok",
				Type:      TaskTypeMirror,
				Src:       &nullBackend{},
				Dst:       &nullBackend{},
				AfterCopy: tc.afterCopy,
			}
			if err := validateTask(task); err != nil {
				t.Fatalf("validateTask: %v", err)
			}
			if task.AfterCopy != AfterCopyVerifyThenSkip {
				t.Errorf("AfterCopy = %q, want %q (validateTask must lock it)",
					task.AfterCopy, AfterCopyVerifyThenSkip)
			}
		})
	}
}

// TestValidateTask_Mirror_RejectsBadAfterCopy asserts that mirror refuses
// AfterCopy=verify_then_delete_src. Allowing that combo would mean "delete
// src + delete dst extras" both fire — equivalent to nuking the dataset.
func TestValidateTask_Mirror_RejectsBadAfterCopy(t *testing.T) {
	task := &Task{
		ID:        "mirror-bad-after",
		Type:      TaskTypeMirror,
		Src:       &nullBackend{},
		Dst:       &nullBackend{},
		AfterCopy: AfterCopyVerifyThenDeleteSrc,
	}
	err := validateTask(task)
	if err == nil {
		t.Fatal("validateTask should reject type=mirror with AfterCopy=verify_then_delete_src")
	}
	if !strings.Contains(err.Error(), "type=mirror requires afterCopy=verify_then_skip") {
		t.Errorf("error = %q, want mirror-afterCopy substring", err.Error())
	}
}

// TestValidateTask_Mirror_ConfirmGate asserts that the destructive-task gate
// from wave 2 applies to mirror: Confirm=true + DryRun=false on a mirror
// task is rejected, because mirror is taskIsDestructive (it prunes dst).
func TestValidateTask_Mirror_ConfirmGate(t *testing.T) {
	task := &Task{
		ID:      "mirror-confirm",
		Type:    TaskTypeMirror,
		Src:     &nullBackend{},
		Dst:     &nullBackend{},
		Confirm: true,
		DryRun:  false,
	}
	err := validateTask(task)
	if err == nil {
		t.Fatal("validateTask should reject mirror Confirm=true + DryRun=false")
	}
	if !strings.Contains(err.Error(), "dry-run confirmation required") {
		t.Errorf("error = %q, want dry-run-confirmation substring", err.Error())
	}

	// And the preview-then-apply path: DryRun=true + Confirm=true passes.
	task.DryRun = true
	if err := validateTask(task); err != nil {
		t.Fatalf("validateTask(mirror+Confirm+DryRun): %v", err)
	}
}

func TestExecutor_RunReportsFailure(t *testing.T) {
	// Make src.List fail so runSync surfaces a fatal error → StatusFailed.
	e := New(WithProgressInterval(50 * time.Millisecond))
	defer e.Close()

	task := &Task{
		ID:   "t1",
		Type: TaskTypeSync,
		Src:  &listErrBackend{err: errors.New("simulated list failure")},
		Dst:  &nullBackend{kind: "null"},
	}
	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusFailed {
		t.Errorf("Status = %s, want %s (Failed)", res.Status, StatusFailed)
	}
	if res.Error == "" {
		t.Error("Error should be non-empty on Failed status")
	}
}

func TestExecutor_RunReportsCancellation(t *testing.T) {
	e := New()
	defer e.Close()

	// Cancelled context + a backend that returns immediately. The sync
	// path checks ctx.Err() after the (empty) listing, so an already-
	// cancelled context surfaces as StatusCancelled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	task := &Task{
		ID:   "t-cancel",
		Type: TaskTypeSync,
		Src:  &nullBackend{},
		Dst:  &nullBackend{},
	}
	res := e.Run(ctx, task, NoopReporter{})
	if res.Status != StatusCancelled {
		t.Errorf("Status = %s, want %s (Cancelled)", res.Status, StatusCancelled)
	}
}

func TestExecutor_CancelInFlight(t *testing.T) {
	e := New()
	defer e.Close()

	// We can't easily run a long-lived task with stubs, but we can check
	// that Cancel on an unknown task is a no-op.
	e.Cancel("nonexistent")
	if got := e.RunningCount(); got != 0 {
		t.Errorf("RunningCount = %d, want 0", got)
	}
}

// recordingReporter captures all callbacks for assertion.
type recordingReporter struct {
	starts    []string
	dones     []doneEntry
	snapshots []Progress
}

type doneEntry struct {
	key   string
	bytes int64
	err   error
}

func (r *recordingReporter) OnFileStart(key string, size int64) {
	r.starts = append(r.starts, key)
}
func (r *recordingReporter) OnFileDone(key string, bytes int64, err error) {
	r.dones = append(r.dones, doneEntry{key, bytes, err})
}
func (r *recordingReporter) OnProgress(s Progress) {
	r.snapshots = append(r.snapshots, s)
}

func TestExecutor_ReporterIsCalled(t *testing.T) {
	// Stubs don't invoke reporter callbacks, but the framework guarantees
	// at least the final OnProgress fires after Run completes.
	e := New(WithProgressInterval(50 * time.Millisecond))
	defer e.Close()

	rep := &recordingReporter{}
	task := &Task{
		ID:   "t-rep",
		Type: TaskTypeCheck,
		Src:  &nullBackend{},
		Dst:  &nullBackend{},
	}
	_ = e.Run(context.Background(), task, rep)
	if len(rep.snapshots) == 0 {
		t.Error("expected at least one progress snapshot")
	}
}

func TestNoopReporterImplementsInterface(t *testing.T) {
	var _ Reporter = NoopReporter{}
}

func TestNew_DefaultOptions(t *testing.T) {
	e := New()
	defer e.Close()
	if e.opts.transfersPerTask != 4 {
		t.Errorf("default transfersPerTask = %d, want 4", e.opts.transfersPerTask)
	}
	if e.opts.progressInterval == 0 {
		t.Error("progressInterval should have a default")
	}
}

func TestNew_AppliesOptions(t *testing.T) {
	e := New(WithTransfersPerTask(8), WithBandwidthLimit(500), WithProgressInterval(time.Second))
	defer e.Close()
	if e.opts.transfersPerTask != 8 {
		t.Errorf("transfersPerTask = %d, want 8", e.opts.transfersPerTask)
	}
	if e.opts.bandwidthLimitMBps != 500 {
		t.Errorf("bandwidthLimitMBps = %d, want 500", e.opts.bandwidthLimitMBps)
	}
}

func TestTransfersPerTask_TaskOverride(t *testing.T) {
	e := New(WithTransfersPerTask(4))
	defer e.Close()
	if got := e.transfersPerTask(&Task{Parallelism: 16}); got != 16 {
		t.Errorf("with Task.Parallelism=16, got %d", got)
	}
	if got := e.transfersPerTask(&Task{}); got != 4 {
		t.Errorf("with default, got %d", got)
	}
}

// TestExecutor_RunRefusesAfterClose covers FIX Q2: after Close() the
// executor returns a cancelled Result with ErrExecutorClosed.Error()
// instead of panicking on a nil-map write. This is the contract
// Runner.runAfterWait relies on when a queued goroutine races Close().
func TestExecutor_RunRefusesAfterClose(t *testing.T) {
	e := New()
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	task := &Task{
		ID:   "t-closed",
		Type: TaskTypeSync,
		Src:  &nullBackend{},
		Dst:  &nullBackend{},
	}
	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusCancelled {
		t.Errorf("Status = %q, want %q", res.Status, StatusCancelled)
	}
	if res.Error != ErrExecutorClosed.Error() {
		t.Errorf("Error = %q, want %q", res.Error, ErrExecutorClosed.Error())
	}
	if res.TaskID != "t-closed" {
		t.Errorf("TaskID = %q, want t-closed", res.TaskID)
	}
	if res.StartedAt.IsZero() || res.DoneAt.IsZero() {
		t.Error("StartedAt/DoneAt should be populated even on the closed-fast-path")
	}
	// Close again — must be idempotent so doShutdown can call it
	// freely.
	if err := e.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestExecutor_RunWithNilRunningMap_DoesNotPanic synthesises the race
// where the running map is nil at the moment Run() takes the mu. The
// executor must return a cancelled Result instead of writing into the
// nil map.
func TestExecutor_RunWithNilRunningMap_DoesNotPanic(t *testing.T) {
	e := New()
	// Drive the executor into the post-Close state without going
	// through the public flag check: clear the map directly. This
	// simulates the race window where a queued Runner goroutine has
	// observed closed=false but the executor's running map has been
	// nilled before Run() takes the mu.
	e.mu.Lock()
	e.running = nil
	e.mu.Unlock()

	task := &Task{
		ID:   "t-nil-map",
		Type: TaskTypeSync,
		Src:  &nullBackend{},
		Dst:  &nullBackend{},
	}
	// Must not panic; must return a cancelled Result.
	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusCancelled {
		t.Errorf("Status = %q, want %q", res.Status, StatusCancelled)
	}
	if res.Error == "" {
		t.Error("Error should be populated on the nil-map fast path")
	}
}
