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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/local"
)

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

// newLocalBackend constructs a local backend rooted at a temp dir and
// returns the backend along with the resolved absolute root path (with
// symlinks expanded, since macOS /tmp is a symlink to /private/tmp).
func newLocalBackend(t *testing.T) (backend.Backend, string) {
	t.Helper()
	root := t.TempDir()
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", root, err)
	}
	b, err := local.New(&local.Config{
		AllowedRoots:         []string{resolved},
		DefaultBufferSizeKiB: 64,
	})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, resolved
}

// writeFile writes `size` bytes of pattern data at root/relPath and sets
// its mtime if non-zero. Creates parent dirs.
func writeFile(t *testing.T, root, relPath string, size int64, mtime time.Time) {
	t.Helper()
	abs := filepath.Join(root, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("mkdir parent of %q: %v", abs, err)
	}
	// Cheap deterministic content: repeating 'a' bytes is enough for the
	// size-mismatch tests; content equality isn't asserted.
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = 'a'
	}
	if err := os.WriteFile(abs, buf, 0o644); err != nil {
		t.Fatalf("write %q: %v", abs, err)
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(abs, mtime, mtime); err != nil {
			t.Fatalf("chtimes %q: %v", abs, err)
		}
	}
}

// runCheckTask builds and runs a check Task against the supplied roots and
// returns the executor result.
func runCheckTask(t *testing.T, src, dst backend.Backend, srcPath, dstPath string, opts ...func(*Task)) Result {
	t.Helper()
	task := &Task{
		ID:      "check-test",
		Type:    TaskTypeCheck,
		Src:     src,
		Dst:     dst,
		SrcPath: srcPath,
		DstPath: dstPath,
	}
	for _, opt := range opts {
		opt(task)
	}
	e := New(WithProgressInterval(50 * time.Millisecond))
	t.Cleanup(func() { _ = e.Close() })
	return e.Run(context.Background(), task, NoopReporter{})
}

// reasonsOf returns the mismatch reasons in deterministic order.
func reasonsOf(res Result) []MismatchReason {
	out := make([]MismatchReason, 0, len(res.Mismatches))
	for _, m := range res.Mismatches {
		out = append(out, m.Reason)
	}
	return out
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

func TestRunCheck_IdenticalSides(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	for _, name := range []string{"a.pt", "b.pt", "sub/c.pt"} {
		writeFile(t, srcRoot, name, 1024, time.Time{})
		writeFile(t, dstRoot, name, 1024, time.Time{})
	}

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot)
	if res.Status != StatusDone {
		t.Fatalf("Status = %s want Done; err=%s", res.Status, res.Error)
	}
	if got := len(res.Mismatches); got != 0 {
		t.Fatalf("Mismatches = %d want 0: %+v", got, res.Mismatches)
	}
}

func TestRunCheck_MissingDst(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	writeFile(t, srcRoot, "a.pt", 100, time.Time{})
	writeFile(t, srcRoot, "b.pt", 100, time.Time{})
	writeFile(t, srcRoot, "c.pt", 100, time.Time{})
	writeFile(t, dstRoot, "a.pt", 100, time.Time{})
	writeFile(t, dstRoot, "b.pt", 100, time.Time{})

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot)
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	if len(res.Mismatches) != 1 {
		t.Fatalf("Mismatches = %d want 1: %+v", len(res.Mismatches), res.Mismatches)
	}
	if res.Mismatches[0].Reason != MismatchMissingDst {
		t.Fatalf("Reason = %s want %s", res.Mismatches[0].Reason, MismatchMissingDst)
	}
	if res.Mismatches[0].Key != "c.pt" {
		t.Fatalf("Key = %q want c.pt", res.Mismatches[0].Key)
	}
}

func TestRunCheck_MissingSrc(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	writeFile(t, srcRoot, "a.pt", 100, time.Time{})
	writeFile(t, srcRoot, "b.pt", 100, time.Time{})
	writeFile(t, dstRoot, "a.pt", 100, time.Time{})
	writeFile(t, dstRoot, "b.pt", 100, time.Time{})
	writeFile(t, dstRoot, "stale.pt", 100, time.Time{})

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot)
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	if len(res.Mismatches) != 1 {
		t.Fatalf("Mismatches = %d want 1: %+v", len(res.Mismatches), res.Mismatches)
	}
	if res.Mismatches[0].Reason != MismatchMissingSrc {
		t.Fatalf("Reason = %s want %s", res.Mismatches[0].Reason, MismatchMissingSrc)
	}
	if res.Mismatches[0].Key != "stale.pt" {
		t.Fatalf("Key = %q want stale.pt", res.Mismatches[0].Key)
	}
}

func TestRunCheck_SizeMismatch(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	writeFile(t, srcRoot, "a.pt", 100, time.Time{})
	writeFile(t, srcRoot, "b.pt", 200, time.Time{})
	writeFile(t, dstRoot, "a.pt", 100, time.Time{})
	writeFile(t, dstRoot, "b.pt", 999, time.Time{}) // wrong size

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot)
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	if len(res.Mismatches) != 1 {
		t.Fatalf("Mismatches = %d want 1: %+v", len(res.Mismatches), res.Mismatches)
	}
	m := res.Mismatches[0]
	if m.Reason != MismatchSizeDiffer {
		t.Fatalf("Reason = %s want %s", m.Reason, MismatchSizeDiffer)
	}
	if m.Key != "b.pt" {
		t.Fatalf("Key = %q want b.pt", m.Key)
	}
	if m.SrcSize != 200 || m.DstSize != 999 {
		t.Fatalf("Sizes src=%d dst=%d want 200/999", m.SrcSize, m.DstSize)
	}
}

func TestRunCheck_ManyMismatchesWithSampleRate(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	// 100 files on both sides, all with size mismatch.
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("f%03d.bin", i)
		writeFile(t, srcRoot, name, 100, time.Time{})
		writeFile(t, dstRoot, name, 200, time.Time{}) // different
	}

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot, func(task *Task) {
		task.SampleStrategy = "random"
		task.SampleRate = 0.1
	})
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	// floor(100 * 0.1) = 10, allow ±1 for rounding tolerance.
	n := len(res.Mismatches)
	if n < 9 || n > 11 {
		t.Fatalf("Mismatches = %d, want 10±1: got %+v", n, res.Mismatches)
	}
	for _, m := range res.Mismatches {
		if m.Reason != MismatchSizeDiffer {
			t.Errorf("unexpected reason %s for %s", m.Reason, m.Key)
		}
	}
}

func TestRunCheck_SampleStrategyLargest(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	// 5 files with sizes 1, 10, 100, 1000, 10000 — all differ from dst.
	sizes := []int64{1, 10, 100, 1000, 10000}
	for i, s := range sizes {
		name := fmt.Sprintf("f%d.bin", i)
		writeFile(t, srcRoot, name, s, time.Time{})
		writeFile(t, dstRoot, name, s+1, time.Time{})
	}

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot, func(task *Task) {
		task.SampleStrategy = "largest"
		task.SampleRate = 0.4
	})
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	// floor(5 * 0.4) = 2 → expect 2 largest: f4 (10000) and f3 (1000).
	if len(res.Mismatches) != 2 {
		t.Fatalf("Mismatches = %d want 2: %+v", len(res.Mismatches), res.Mismatches)
	}
	gotSizes := []int64{res.Mismatches[0].SrcSize, res.Mismatches[1].SrcSize}
	sort.Slice(gotSizes, func(i, j int) bool { return gotSizes[i] > gotSizes[j] })
	wantSizes := []int64{10000, 1000}
	if gotSizes[0] != wantSizes[0] || gotSizes[1] != wantSizes[1] {
		t.Fatalf("got sizes %v want %v (mismatches=%+v)", gotSizes, wantSizes, res.Mismatches)
	}
}

func TestRunCheck_SampleStrategyOldest(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	// 5 differing files with mtimes spread across days. Indices 0..4 → 5d..1d ago.
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	mtimes := []time.Time{
		base.AddDate(0, 0, -5),
		base.AddDate(0, 0, -4),
		base.AddDate(0, 0, -3),
		base.AddDate(0, 0, -2),
		base.AddDate(0, 0, -1),
	}
	// Both sides need the same mtime so the *dst* side determines the
	// ordering used by "oldest" (the strategy reads dst mtimes from the
	// already-collected map, which is the cheapest lookup).
	for i, mt := range mtimes {
		name := fmt.Sprintf("f%d.bin", i)
		writeFile(t, srcRoot, name, 100, mt)
		writeFile(t, dstRoot, name, 200, mt) // size differ
	}

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot, func(task *Task) {
		task.SampleStrategy = "oldest"
		task.SampleRate = 0.4
	})
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	if len(res.Mismatches) != 2 {
		t.Fatalf("Mismatches = %d want 2: %+v", len(res.Mismatches), res.Mismatches)
	}
	// Expect f0 (5d ago) + f1 (4d ago).
	gotKeys := []string{res.Mismatches[0].Key, res.Mismatches[1].Key}
	sort.Strings(gotKeys)
	if gotKeys[0] != "f0.bin" || gotKeys[1] != "f1.bin" {
		t.Fatalf("got keys %v want [f0.bin f1.bin] (mismatches=%+v)", gotKeys, res.Mismatches)
	}
}

func TestRunCheck_FilterApplied(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	// Src has both filtered and unfiltered files. Dst is empty.
	writeFile(t, srcRoot, "keep.pt", 100, time.Time{})
	writeFile(t, srcRoot, "ignore.tmp", 100, time.Time{})
	writeFile(t, srcRoot, "ignore2.log", 100, time.Time{})

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot, func(task *Task) {
		task.Filter = Filter{
			Include: []string{"*.pt"},
		}
	})
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	// Only keep.pt should be checked; it's missing on dst.
	// ignore.* files are filtered out so they're NOT flagged.
	// And because a filter is set, dst-side extras would also be ignored
	// (here there are none anyway).
	if len(res.Mismatches) != 1 {
		t.Fatalf("Mismatches = %d want 1: %+v", len(res.Mismatches), res.Mismatches)
	}
	m := res.Mismatches[0]
	if m.Reason != MismatchMissingDst || m.Key != "keep.pt" {
		t.Fatalf("got %s/%s, want missing_dst/keep.pt", m.Reason, m.Key)
	}
}

func TestRunCheck_OnMismatchIgnore(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	// Same setup as MissingDst: 3 src, 2 dst → 1 mismatch on c.pt.
	writeFile(t, srcRoot, "a.pt", 100, time.Time{})
	writeFile(t, srcRoot, "b.pt", 100, time.Time{})
	writeFile(t, srcRoot, "c.pt", 100, time.Time{})
	writeFile(t, dstRoot, "a.pt", 100, time.Time{})
	writeFile(t, dstRoot, "b.pt", 100, time.Time{})

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot, func(task *Task) {
		task.OnMismatch = OnMismatchIgnore
	})
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	if len(res.Mismatches) != 0 {
		t.Fatalf("Mismatches = %d want 0 (ignored): %+v", len(res.Mismatches), res.Mismatches)
	}
	// But progress should still register the count.
	if res.Progress.FilesFailed != 1 {
		t.Errorf("Progress.FilesFailed = %d want 1", res.Progress.FilesFailed)
	}
}

func TestRunCheck_EmptyBothSides(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	res := runCheckTask(t, srcB, dstB, srcRoot, dstRoot)
	if res.Status != StatusDone {
		t.Fatalf("Status=%s err=%s", res.Status, res.Error)
	}
	if len(res.Mismatches) != 0 {
		t.Fatalf("Mismatches = %d want 0: %+v", len(res.Mismatches), res.Mismatches)
	}
}

// sanity: reasonsOf is referenced by potential future tests; this keeps
// the helper from being dead while we wait for D-5.
var _ = reasonsOf
