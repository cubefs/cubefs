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
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Tests cover the D-5 wiring: a Check task with OnMismatchAutoFix should
// repair every fixable mismatch in-process and leave dst consistent with src.
// Helpers `newLocalBackend` / `writeFile` live in check_task_test.go.

func TestAutoFix_RepairsMissingDst(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	// Two files only exist on src; dst is empty.
	writeFile(t, srcRoot, "a.bin", 100, time.Time{})
	writeFile(t, srcRoot, "sub/b.bin", 200, time.Time{})

	task := &Task{
		ID:         "fix-missing-dst",
		Type:       TaskTypeCheck,
		Src:        srcB,
		Dst:        dstB,
		SrcPath:    srcRoot,
		DstPath:    dstRoot,
		OnMismatch: OnMismatchAutoFix,
	}
	e := New(WithProgressInterval(50 * time.Millisecond))
	t.Cleanup(func() { _ = e.Close() })

	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want %s; err=%s", res.Status, StatusDone, res.Error)
	}
	if len(res.Mismatches) != 2 {
		t.Fatalf("found %d mismatches, want 2", len(res.Mismatches))
	}
	for _, rel := range []string{"a.bin", "sub/b.bin"} {
		info, err := os.Stat(filepath.Join(dstRoot, rel))
		if err != nil {
			t.Fatalf("dst missing %q after auto_fix: %v", rel, err)
		}
		if info.Size() == 0 {
			t.Errorf("dst %q is empty after auto_fix", rel)
		}
	}
}

func TestAutoFix_RepairsSizeMismatch(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	writeFile(t, srcRoot, "x.bin", 500, time.Time{})
	writeFile(t, dstRoot, "x.bin", 200, time.Time{}) // wrong size

	task := &Task{
		ID:         "fix-size",
		Type:       TaskTypeCheck,
		Src:        srcB,
		Dst:        dstB,
		SrcPath:    srcRoot,
		DstPath:    dstRoot,
		OnMismatch: OnMismatchAutoFix,
	}
	e := New(WithProgressInterval(50 * time.Millisecond))
	t.Cleanup(func() { _ = e.Close() })

	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want %s; err=%s", res.Status, StatusDone, res.Error)
	}
	info, err := os.Stat(filepath.Join(dstRoot, "x.bin"))
	if err != nil {
		t.Fatalf("stat dst x.bin: %v", err)
	}
	if info.Size() != 500 {
		t.Errorf("dst size = %d, want 500 (auto_fix should have rewritten)", info.Size())
	}
}

func TestAutoFix_DoesNotDeleteMissingSrc(t *testing.T) {
	// dst has an extra file with no src counterpart. Auto-fix must NOT delete
	// it — that's destructive and outside the auto_fix contract.
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	writeFile(t, dstRoot, "extra.bin", 50, time.Time{})

	task := &Task{
		ID:         "no-delete",
		Type:       TaskTypeCheck,
		Src:        srcB,
		Dst:        dstB,
		SrcPath:    srcRoot,
		DstPath:    dstRoot,
		OnMismatch: OnMismatchAutoFix,
	}
	e := New(WithProgressInterval(50 * time.Millisecond))
	t.Cleanup(func() { _ = e.Close() })

	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want %s; err=%s", res.Status, StatusDone, res.Error)
	}
	// The mismatch is reported …
	if len(res.Mismatches) != 1 {
		t.Fatalf("got %d mismatches, want 1", len(res.Mismatches))
	}
	if res.Mismatches[0].Reason != MismatchMissingSrc {
		t.Errorf("reason = %s, want %s", res.Mismatches[0].Reason, MismatchMissingSrc)
	}
	// … and dst's extra file MUST still exist.
	if _, err := os.Stat(filepath.Join(dstRoot, "extra.bin")); err != nil {
		t.Errorf("auto_fix improperly removed dst extra.bin: %v", err)
	}
}

func TestAutoFix_NotTriggeredByAlertPolicy(t *testing.T) {
	// Without OnMismatchAutoFix, check should report the mismatch but NOT
	// touch dst.
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	writeFile(t, srcRoot, "only-src.bin", 10, time.Time{})

	task := &Task{
		ID:         "alert-only",
		Type:       TaskTypeCheck,
		Src:        srcB,
		Dst:        dstB,
		SrcPath:    srcRoot,
		DstPath:    dstRoot,
		OnMismatch: OnMismatchAlert,
	}
	e := New(WithProgressInterval(50 * time.Millisecond))
	t.Cleanup(func() { _ = e.Close() })

	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want %s; err=%s", res.Status, StatusDone, res.Error)
	}
	if len(res.Mismatches) != 1 {
		t.Fatalf("got %d mismatches, want 1", len(res.Mismatches))
	}
	if _, err := os.Stat(filepath.Join(dstRoot, "only-src.bin")); !os.IsNotExist(err) {
		t.Errorf("alert policy should leave dst untouched; stat err=%v", err)
	}
}

func TestAutoFix_NoMismatchesIsNoop(t *testing.T) {
	srcB, srcRoot := newLocalBackend(t)
	dstB, dstRoot := newLocalBackend(t)

	writeFile(t, srcRoot, "same.bin", 100, time.Time{})
	writeFile(t, dstRoot, "same.bin", 100, time.Time{})

	task := &Task{
		ID:         "no-op",
		Type:       TaskTypeCheck,
		Src:        srcB,
		Dst:        dstB,
		SrcPath:    srcRoot,
		DstPath:    dstRoot,
		OnMismatch: OnMismatchAutoFix,
	}
	e := New(WithProgressInterval(50 * time.Millisecond))
	t.Cleanup(func() { _ = e.Close() })

	res := e.Run(context.Background(), task, NoopReporter{})
	if res.Status != StatusDone {
		t.Errorf("Status = %s, want %s", res.Status, StatusDone)
	}
	if len(res.Mismatches) != 0 {
		t.Errorf("got %d mismatches, want 0", len(res.Mismatches))
	}
}

func TestIsAutoFixable(t *testing.T) {
	cases := []struct {
		reason MismatchReason
		want   bool
	}{
		{MismatchMissingDst, true},
		{MismatchSizeDiffer, true},
		{MismatchETagDiffer, true},
		{MismatchMtimeNewer, true},
		{MismatchMissingSrc, false},
		{MismatchReason("unknown"), false},
	}
	for _, c := range cases {
		if got := isAutoFixable(c.reason); got != c.want {
			t.Errorf("isAutoFixable(%s) = %v, want %v", c.reason, got, c.want)
		}
	}
}

func TestJoinKey(t *testing.T) {
	cases := []struct {
		prefix string
		rel    string
		want   string
	}{
		{"runs", "a/b.pt", "runs/a/b.pt"},
		{"runs/", "a/b.pt", "runs/a/b.pt"},
		{"runs/", "/a/b.pt", "runs/a/b.pt"},
		{"", "a/b.pt", "a/b.pt"},
		{"runs", "", "runs"},
		{"", "", ""},
		{"/abs/path", "rel.bin", "/abs/path/rel.bin"},
	}
	for _, c := range cases {
		if got := joinKey(c.prefix, c.rel); got != c.want {
			t.Errorf("joinKey(%q, %q) = %q, want %q", c.prefix, c.rel, got, c.want)
		}
	}
}
