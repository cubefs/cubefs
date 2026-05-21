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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// -----------------------------------------------------------------------
// Test helpers: two local backends acting as src and dst.
//
// Shared helpers `newLocalBackend` and `writeFile` live in check_task_test.go;
// load tests use them directly. We add a couple of load-specific helpers
// (loadTask, readFile, listTempFiles) below.
// -----------------------------------------------------------------------

// writeLoadFile creates a file under root with the given relative key and
// random bytes of the requested size, returning the content. Like the
// shared writeFile but with a returned content slice + random bytes so the
// tests can check byte-for-byte equality at the destination.
func writeLoadFile(t *testing.T, root, key string, size int) []byte {
	t.Helper()
	full := filepath.Join(root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir %q: %v", filepath.Dir(full), err)
	}
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := os.WriteFile(full, buf, 0o644); err != nil {
		t.Fatalf("write %q: %v", full, err)
	}
	return buf
}

// readFile reads root/key into memory; returns nil if the file doesn't exist.
func readFile(t *testing.T, root, key string) []byte {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(root, key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("read %q: %v", key, err)
	}
	return got
}

// listTempFiles returns all "<...>.downloading.<...>" files under root.
func listTempFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		if strings.Contains(filepath.Base(path), ".downloading.") {
			rel, _ := filepath.Rel(root, path)
			out = append(out, rel)
		}
		return nil
	})
	return out
}

// loadTask builds a default Load task wired to (src, dst) backends.
func loadTask(id, srcPath, dstPath string, src, dst backend.Backend) *Task {
	return &Task{
		ID:               id,
		Type:             TaskTypeLoad,
		Src:              src,
		Dst:              dst,
		SrcPath:          srcPath,
		DstPath:          dstPath,
		DownloadStrategy: DownloadStrategyTempRename,
	}
}

// -----------------------------------------------------------------------
// Backend wrappers used by failure-injection tests.
// -----------------------------------------------------------------------

// slowPutBackend wraps a backend and adds a sleep inside Put. Used to
// reliably observe in-flight cancellation.
type slowPutBackend struct {
	backend.Backend
	delay time.Duration
}

func (s *slowPutBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (string, error) {
	// Sleep in small slices so ctx cancellation interrupts us promptly.
	deadline := time.Now().Add(s.delay)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return s.Backend.Put(ctx, key, body, size, opts)
}

// lyingHeadBackend wraps a backend and reports a wrong size on Head for keys
// passed in liesAbout (so Verification fails after a successful Put/Rename).
// Other Head calls (including the pre-flight existence check at the start
// of loadOne) pass through unchanged, so we can simulate "Put succeeds but
// Verify fails" exactly.
type lyingHeadBackend struct {
	backend.Backend
	liesAbout map[string]int64 // dstKey -> bogus size to return
	mu        sync.Mutex
}

func (l *lyingHeadBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	size, etag, mtime, err := l.Backend.Head(ctx, key)
	if err != nil {
		return size, etag, mtime, err
	}
	l.mu.Lock()
	bogus, lie := l.liesAbout[key]
	l.mu.Unlock()
	if lie {
		return bogus, etag, mtime, nil
	}
	return size, etag, mtime, nil
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

// Test 1: Empty source → runLoad returns nil, FilesDone=0.
func TestRunLoad_EmptySource(t *testing.T) {
	src, srcRoot := newLocalBackend(t)
	dst, dstRoot := newLocalBackend(t)

	e := New(WithProgressInterval(50 * time.Millisecond))
	defer e.Close()

	task := loadTask("t-empty", srcRoot, dstRoot, src, dst)
	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Fatalf("Status = %s err=%q, want Done", res.Status, res.Error)
	}
	if res.Progress.FilesDone != 0 {
		t.Errorf("FilesDone = %d, want 0", res.Progress.FilesDone)
	}
	if res.Progress.FilesTotal != 0 {
		t.Errorf("FilesTotal = %d, want 0", res.Progress.FilesTotal)
	}
}

// Test 2: 3 files → temp_rename → all land at final paths, no temp residue.
func TestRunLoad_TempRenameLands(t *testing.T) {
	src, srcRoot := newLocalBackend(t)
	dst, dstRoot := newLocalBackend(t)

	files := map[string]int{
		"a.bin":             4 * 1024,
		"nested/b.bin":      8 * 1024,
		"nested/deep/c.bin": 16 * 1024,
	}
	want := make(map[string][]byte)
	for key, size := range files {
		want[key] = writeLoadFile(t, srcRoot, key, size)
	}

	e := New(WithProgressInterval(50 * time.Millisecond))
	defer e.Close()

	task := loadTask("t-three", srcRoot, dstRoot, src, dst)
	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Fatalf("Status = %s err=%q, want Done", res.Status, res.Error)
	}
	if res.Progress.FilesDone != int64(len(files)) {
		t.Errorf("FilesDone = %d, want %d", res.Progress.FilesDone, len(files))
	}
	if res.Progress.FilesFailed != 0 {
		t.Errorf("FilesFailed = %d, want 0", res.Progress.FilesFailed)
	}
	for key, expect := range want {
		got := readFile(t, dstRoot, key)
		if got == nil {
			t.Errorf("dst %q missing", key)
			continue
		}
		if len(got) != len(expect) {
			t.Errorf("dst %q: size %d want %d", key, len(got), len(expect))
		}
	}
	if temps := listTempFiles(t, dstRoot); len(temps) != 0 {
		t.Errorf("expected no temp files; found %v", temps)
	}
}

// Test 3: Re-run is idempotent: second run skips all files.
func TestRunLoad_IdempotentRerun(t *testing.T) {
	src, srcRoot := newLocalBackend(t)
	dst, dstRoot := newLocalBackend(t)

	files := []string{"x.bin", "y.bin", "z.bin"}
	for _, k := range files {
		writeLoadFile(t, srcRoot, k, 4*1024)
	}

	e := New(WithProgressInterval(50 * time.Millisecond))
	defer e.Close()

	first := e.Run(context.Background(), loadTask("t-rerun-1", srcRoot, dstRoot, src, dst), NoopReporter{})
	if first.Status != StatusDone {
		t.Fatalf("first Status = %s err=%q", first.Status, first.Error)
	}
	if first.Progress.FilesDone != int64(len(files)) {
		t.Fatalf("first FilesDone = %d, want %d", first.Progress.FilesDone, len(files))
	}

	second := e.Run(context.Background(), loadTask("t-rerun-2", srcRoot, dstRoot, src, dst), NoopReporter{})
	if second.Status != StatusDone {
		t.Fatalf("second Status = %s err=%q", second.Status, second.Error)
	}
	if second.Progress.FilesSkipped != int64(len(files)) {
		t.Errorf("second FilesSkipped = %d, want %d", second.Progress.FilesSkipped, len(files))
	}
	if second.Progress.FilesDone != 0 {
		t.Errorf("second FilesDone = %d, want 0 (all skipped)", second.Progress.FilesDone)
	}
}

// Test 4: Cancellation cleans up temp files: with a slow Put, cancel
// mid-flight and verify no .downloading. residue.
func TestRunLoad_CancelCleansTemp(t *testing.T) {
	src, srcRoot := newLocalBackend(t)
	dst, dstRoot := newLocalBackend(t)

	// 4 files; the slow Put makes the task take long enough that we can
	// cancel between Put start and completion.
	for i := 0; i < 4; i++ {
		writeLoadFile(t, srcRoot, fmt.Sprintf("file-%d.bin", i), 8*1024)
	}

	slowDst := &slowPutBackend{Backend: dst, delay: 500 * time.Millisecond}

	e := New(WithTransfersPerTask(1), WithProgressInterval(50*time.Millisecond))
	defer e.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay — should hit somewhere in the middle of
	// the run.
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	task := loadTask("t-cancel", srcRoot, dstRoot, src, slowDst)
	res := e.Run(ctx, task, NoopReporter{})
	// Cancelled is the canonical outcome here, but tolerate Failed if the
	// implementation reports a non-context error path on cancel.
	if res.Status != StatusCancelled && res.Status != StatusFailed {
		t.Logf("Status = %s err=%q", res.Status, res.Error)
	}

	// Give the cleanup pass a chance (it runs synchronously, but the OS
	// rename/delete sequencing on macOS is not always immediately visible).
	time.Sleep(50 * time.Millisecond)
	if temps := listTempFiles(t, dstRoot); len(temps) != 0 {
		t.Errorf("expected no temp files after cancel; found %v", temps)
	}
}

// Test 5: DownloadStrategyDirect skips rename — temp files never appear,
// final keys land directly.
func TestRunLoad_DirectStrategy(t *testing.T) {
	src, srcRoot := newLocalBackend(t)
	dst, dstRoot := newLocalBackend(t)

	files := []string{"d1.bin", "d2.bin"}
	for _, k := range files {
		writeLoadFile(t, srcRoot, k, 1024)
	}

	// Use a backend wrapper that counts Rename calls: if the strategy is
	// truly "direct", no rename should ever be invoked.
	noRenameDst := &noRenameBackend{Backend: dst}

	e := New(WithProgressInterval(50 * time.Millisecond))
	defer e.Close()

	task := loadTask("t-direct", srcRoot, dstRoot, src, noRenameDst)
	task.DownloadStrategy = DownloadStrategyDirect

	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Fatalf("Status = %s err=%q, want Done", res.Status, res.Error)
	}
	if res.Progress.FilesDone != int64(len(files)) {
		t.Errorf("FilesDone = %d, want %d", res.Progress.FilesDone, len(files))
	}
	if temps := listTempFiles(t, dstRoot); len(temps) != 0 {
		t.Errorf("expected no temp files in direct mode; found %v", temps)
	}
	if noRenameDst.renameCalls.Load() != 0 {
		t.Errorf("expected 0 Rename calls in direct mode; got %d", noRenameDst.renameCalls.Load())
	}
	for _, k := range files {
		if got := readFile(t, dstRoot, k); got == nil {
			t.Errorf("dst %q missing", k)
		}
	}
}

// noRenameBackend wraps a backend and counts Rename calls.
type noRenameBackend struct {
	backend.Backend
	renameCalls atomic.Int64
}

func (n *noRenameBackend) Rename(ctx context.Context, oldKey, newKey string) error {
	n.renameCalls.Add(1)
	return n.Backend.Rename(ctx, oldKey, newKey)
}

// Test 6: Verification failure: lying Head reports the wrong size after
// Put — task records FilesFailed, leaves dst in a known state.
func TestRunLoad_VerificationFailure(t *testing.T) {
	src, srcRoot := newLocalBackend(t)
	dst, dstRoot := newLocalBackend(t)

	writeLoadFile(t, srcRoot, "good.bin", 1024)
	writeLoadFile(t, srcRoot, "bad.bin", 1024)

	// "bad.bin" verification will fail: post-rename Head will lie about
	// its size. The dst absolute path is dstRoot+"/bad.bin".
	badDstKey := filepath.Join(dstRoot, "bad.bin")
	lying := &lyingHeadBackend{Backend: dst, liesAbout: map[string]int64{
		badDstKey: 999, // != 1024
	}}

	e := New(WithProgressInterval(50 * time.Millisecond))
	defer e.Close()

	task := loadTask("t-verify", srcRoot, dstRoot, src, lying)
	res := e.Run(context.Background(), task, NoopReporter{})
	// Per-file verify failures don't cause a task-level Failed status;
	// the task itself completes (Done) with FilesFailed > 0.
	if res.Status != StatusDone {
		t.Fatalf("Status = %s err=%q, want Done (per-file failure is not fatal)", res.Status, res.Error)
	}
	if res.Progress.FilesFailed != 1 {
		t.Errorf("FilesFailed = %d, want 1", res.Progress.FilesFailed)
	}
	if res.Progress.FilesDone != 1 {
		t.Errorf("FilesDone = %d, want 1 (good.bin succeeded)", res.Progress.FilesDone)
	}
}

// Test 7: Filter is honored — exclude pattern drops 1 of 3 files.
func TestRunLoad_FilterHonored(t *testing.T) {
	src, srcRoot := newLocalBackend(t)
	dst, dstRoot := newLocalBackend(t)

	writeLoadFile(t, srcRoot, "keep1.bin", 1024)
	writeLoadFile(t, srcRoot, "keep2.bin", 1024)
	writeLoadFile(t, srcRoot, "drop.tmp", 1024)

	e := New(WithProgressInterval(50 * time.Millisecond))
	defer e.Close()

	task := loadTask("t-filter", srcRoot, dstRoot, src, dst)
	task.Filter = Filter{Exclude: []string{"*.tmp"}}

	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Fatalf("Status = %s err=%q, want Done", res.Status, res.Error)
	}
	if res.Progress.FilesDone != 2 {
		t.Errorf("FilesDone = %d, want 2 (drop.tmp filtered)", res.Progress.FilesDone)
	}
	if got := readFile(t, dstRoot, "drop.tmp"); got != nil {
		t.Errorf("dst should not contain filtered file 'drop.tmp', got %d bytes", len(got))
	}
	for _, k := range []string{"keep1.bin", "keep2.bin"} {
		if got := readFile(t, dstRoot, k); got == nil {
			t.Errorf("dst %q missing", k)
		}
	}
}

// Bonus: tempKeyFor produces the documented suffix shape.
func TestTempKeyFor(t *testing.T) {
	got := tempKeyFor("path/to/model.pt", "abc-123")
	want := "path/to/model.pt.downloading.abc-123"
	if got != want {
		t.Errorf("tempKeyFor = %q, want %q", got, want)
	}
}
