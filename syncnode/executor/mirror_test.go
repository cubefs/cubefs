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
	"reflect"
	"testing"
)

// resetMirrorStats zeros the package-level mirror counters so each test
// starts from a clean slate. Symmetric with resetDryRunStats.
func resetMirrorStats(t *testing.T) {
	t.Helper()
	mirrorDeleted.Store(0)
	mirrorWouldDelete.Store(0)
	mirrorDeleteErr.Store(0)
	t.Cleanup(func() {
		mirrorDeleted.Store(0)
		mirrorWouldDelete.Store(0)
		mirrorDeleteErr.Store(0)
	})
}

// newMirrorTask reuses the sync test env / baseline task and flips Type to
// TaskTypeMirror. AfterCopy is left empty so validateTask locks it to
// verify_then_skip — exercising the lock as part of every mirror test.
func newMirrorTask(env *syncTestEnv, id string) *Task {
	t := newSyncTask(env, id)
	t.Type = TaskTypeMirror
	return t
}

// TestRunMirror_DeletesDstExtras: src has {a,b,c}; dst pre-seeded with
// {a,b,c,d,e}. After mirror, dst must be {a,b,c} and src must be
// unchanged. MirrorStats().Deleted == 2 (d,e).
func TestRunMirror_DeletesDstExtras(t *testing.T) {
	resetMirrorStats(t)
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "a.bin", []byte("alpha"))
	env.writeSrcFile(t, "b.bin", []byte("bravo"))
	env.writeSrcFile(t, "c.bin", []byte("charlie"))
	env.writeDstFile(t, "a.bin", []byte("alpha"))
	env.writeDstFile(t, "b.bin", []byte("bravo"))
	env.writeDstFile(t, "c.bin", []byte("charlie"))
	env.writeDstFile(t, "d.bin", []byte("dst-only-1"))
	env.writeDstFile(t, "e.bin", []byte("dst-only-2"))

	task := newMirrorTask(env, "t-mirror-extras")
	res := runSyncTask(context.Background(), t, task)
	if res.Status != StatusDone {
		t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
	}

	got := env.listDstKeys(t)
	want := []string{"a.bin", "b.bin", "c.bin"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dst keys = %v, want %v", got, want)
	}

	// src untouched — mirror is non-destructive on src.
	for _, k := range []string{"a.bin", "b.bin", "c.bin"} {
		if !env.srcExists(t, k) {
			t.Errorf("src %q missing after mirror — must not delete src", k)
		}
	}

	snap := MirrorStats()
	if snap.Deleted != 2 {
		t.Errorf("MirrorStats.Deleted = %d, want 2", snap.Deleted)
	}
	if snap.WouldDelete != 0 || snap.DeleteErr != 0 {
		t.Errorf("unexpected counters: %+v", snap)
	}
}

// TestRunMirror_DryRun_DoesNotDelete: same setup as DeletesDstExtras but
// DryRun=true. Dst content must be unchanged, WouldDelete=2, Deleted=0.
func TestRunMirror_DryRun_DoesNotDelete(t *testing.T) {
	resetMirrorStats(t)
	resetDryRunStats(t)
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "a.bin", []byte("alpha"))
	env.writeSrcFile(t, "b.bin", []byte("bravo"))
	env.writeDstFile(t, "a.bin", []byte("alpha"))
	env.writeDstFile(t, "b.bin", []byte("bravo"))
	env.writeDstFile(t, "ghost.bin", []byte("would-go-away"))

	task := newMirrorTask(env, "t-mirror-dry")
	task.DryRun = true

	res := runSyncTask(context.Background(), t, task)
	if res.Status != StatusDone {
		t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
	}

	// Nothing was actually deleted; dst still has the ghost.
	got := env.listDstKeys(t)
	want := []string{"a.bin", "b.bin", "ghost.bin"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dst keys = %v, want %v (dry-run must not mutate dst)", got, want)
	}

	snap := MirrorStats()
	if snap.WouldDelete != 1 {
		t.Errorf("MirrorStats.WouldDelete = %d, want 1", snap.WouldDelete)
	}
	if snap.Deleted != 0 {
		t.Errorf("MirrorStats.Deleted = %d, want 0 (dry-run must not delete)", snap.Deleted)
	}
	if snap.DeleteErr != 0 {
		t.Errorf("MirrorStats.DeleteErr = %d, want 0", snap.DeleteErr)
	}
}

// TestRunMirror_EmptySrc_ClearsDst: empty src + dst={x,y,z} → dst empty.
// This is the "rclone --delete with empty source" footgun rclone warns
// about; mirror replicates the behaviour but only after validateTask's
// Confirm/DryRun gate has armed it.
func TestRunMirror_EmptySrc_ClearsDst(t *testing.T) {
	resetMirrorStats(t)
	env := newSyncTestEnv(t)
	env.writeDstFile(t, "x.bin", []byte("xxx"))
	env.writeDstFile(t, "y.bin", []byte("yyy"))
	env.writeDstFile(t, "z.bin", []byte("zzz"))

	task := newMirrorTask(env, "t-mirror-empty-src")
	res := runSyncTask(context.Background(), t, task)
	if res.Status != StatusDone {
		t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
	}

	got := env.listDstKeys(t)
	if len(got) != 0 {
		t.Errorf("dst keys = %v, want empty (empty src must clear dst)", got)
	}

	snap := MirrorStats()
	if snap.Deleted != 3 {
		t.Errorf("MirrorStats.Deleted = %d, want 3", snap.Deleted)
	}
}

// TestRunMirror_NoExtras_NoOp: src and dst are identical, no extras to
// prune. Deleted/WouldDelete/DeleteErr all zero.
func TestRunMirror_NoExtras_NoOp(t *testing.T) {
	resetMirrorStats(t)
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "m.bin", []byte("match"))
	env.writeSrcFile(t, "n.bin", []byte("nominal"))
	env.writeDstFile(t, "m.bin", []byte("match"))
	env.writeDstFile(t, "n.bin", []byte("nominal"))

	task := newMirrorTask(env, "t-mirror-noop")
	res := runSyncTask(context.Background(), t, task)
	if res.Status != StatusDone {
		t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
	}

	got := env.listDstKeys(t)
	want := []string{"m.bin", "n.bin"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dst keys = %v, want %v", got, want)
	}

	snap := MirrorStats()
	if snap.Deleted != 0 || snap.WouldDelete != 0 || snap.DeleteErr != 0 {
		t.Errorf("MirrorStats = %+v, want all zero", snap)
	}
}

// TestUnrebaseKey covers the dst→src key translation matrix that
// deleteDstExtras relies on. Mirror correctness collapses entirely if
// unrebaseKey misclassifies an entry as "not under dst" and thus skips a
// real extra, or vice versa.
func TestUnrebaseKey(t *testing.T) {
	cases := []struct {
		name    string
		dstKey  string
		src     string
		dst     string
		want    string
		wantErr bool
	}{
		{"trailing slash both", "warm/a/b.pt", "runs/", "warm/", "runs/a/b.pt", false},
		{"no trailing slash", "warm/a/b.pt", "runs", "warm", "runs/a/b.pt", false},
		{"single-file dst equals dstPath", "warm", "runs/", "warm", "runs", false},
		{"key not under dst", "other/x", "runs/", "warm", "", true},
		{"empty src non-empty dst", "warm/x", "", "warm", "x", false},
		{"empty dst means identity", "x/y", "runs", "", "runs/x/y", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := unrebaseKey(tc.dstKey, tc.src, tc.dst)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
