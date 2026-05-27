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
	"testing"
	"time"
)

// TestRunSync_DryRun_StatsAccurate exercises all three would_* branches of
// syncOneFile in a single scenario, asserting that DryRunStats reports the
// exact per-action counter we expect when the executor takes:
//   - the transfer path (would_copy)
//   - the skip path under OnExisting=always_skip with a pre-seeded dst
//     (would_skip_existing)
//   - the server-side copy path against a backend that advertises
//     ServerSideCopy + SameInstance (would_server_side_copy)
//
// Each branch is run as its own subtest under a t.Run scope so the package-
// level counters can be reset between them via resetDryRunStats. Without
// the reset the second subtest would see a non-zero baseline and the
// assertions become positional ("WouldCopy = previous + 1") instead of
// declarative — harder to read and easy to drift.
func TestRunSync_DryRun_StatsAccurate(t *testing.T) {
	t.Run("would_copy", func(t *testing.T) {
		resetDryRunStats(t)
		env := newSyncTestEnv(t)
		env.writeSrcFile(t, "alpha.bin", []byte("alpha-payload"))
		env.writeSrcFile(t, "beta.bin", []byte("beta-payload"))

		task := newSyncTask(env, "t-dry-copy")
		task.DryRun = true

		res := runSyncTask(context.Background(), t, task)
		if res.Status != StatusDone {
			t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
		}
		snap := DryRunStats()
		if snap.WouldCopy != 2 {
			t.Errorf("WouldCopy = %d, want 2", snap.WouldCopy)
		}
		if snap.WouldSkip != 0 || snap.WouldServerSideCopy != 0 || snap.WouldDeleteSrc != 0 {
			t.Errorf("unexpected counters: %+v", snap)
		}
		// Per-task Progress mirrors the package-level counters: files
		// were counted as "done" because dry-run uses FilesDone to keep
		// the progress bar realistic.
		if res.Progress.FilesDone != 2 {
			t.Errorf("FilesDone = %d, want 2", res.Progress.FilesDone)
		}
	})

	t.Run("would_skip_existing", func(t *testing.T) {
		resetDryRunStats(t)
		env := newSyncTestEnv(t)
		// Seed both sides: dst already has the same key, OnExisting=always_skip
		// means we'd skip it on a real run; in dry-run we just account it.
		env.writeSrcFile(t, "gamma.bin", []byte("gamma-payload"))
		env.writeDstFile(t, "gamma.bin", []byte("existing-content"))

		task := newSyncTask(env, "t-dry-skip")
		task.DryRun = true
		task.OnExisting = OnExistingAlwaysSkip

		res := runSyncTask(context.Background(), t, task)
		if res.Status != StatusDone {
			t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
		}
		snap := DryRunStats()
		if snap.WouldSkip != 1 {
			t.Errorf("WouldSkip = %d, want 1", snap.WouldSkip)
		}
		if snap.WouldCopy != 0 || snap.WouldServerSideCopy != 0 || snap.WouldDeleteSrc != 0 {
			t.Errorf("unexpected counters: %+v", snap)
		}
		if res.Progress.FilesSkipped != 1 {
			t.Errorf("FilesSkipped = %d, want 1", res.Progress.FilesSkipped)
		}
		// Dst content untouched (sanity — DryRun must not write over it).
		got := env.readDstFile(t, "gamma.bin")
		if string(got) != "existing-content" {
			t.Errorf("dst content mutated in dry-run: got %q, want %q", got, "existing-content")
		}
	})

	t.Run("would_server_side_copy", func(t *testing.T) {
		resetDryRunStats(t)
		// Reuse the fake from server_side_copy_test.go — it advertises
		// SameInstance + Caps.ServerSideCopy and records ServerSideCopy /
		// Get / Put calls. In dry-run, NONE of those should fire.
		src := newServerSideCopyBackend(map[string][]byte{
			"/data/ssc.bin": []byte("server-side-payload"),
		})
		dst := newServerSideCopyBackend(nil)
		src.peer = dst

		task := &Task{
			ID:          "t-dry-ssc",
			Type:        TaskTypeSync,
			Src:         src,
			Dst:         dst,
			SrcPath:     "/data",
			DstPath:     "/dest",
			Parallelism: 1,
			DryRun:      true,
		}

		e := New(WithProgressInterval(20 * time.Millisecond))
		defer e.Close()
		res := e.Run(context.Background(), task, NoopReporter{})
		if res.Status != StatusDone {
			t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
		}
		snap := DryRunStats()
		if snap.WouldServerSideCopy != 1 {
			t.Errorf("WouldServerSideCopy = %d, want 1", snap.WouldServerSideCopy)
		}
		if snap.WouldCopy != 0 || snap.WouldSkip != 0 || snap.WouldDeleteSrc != 0 {
			t.Errorf("unexpected counters: %+v", snap)
		}
		// Critical invariant: the SSC fake must NOT have been called.
		if src.sscCalls.Load() != 0 {
			t.Errorf("ServerSideCopy calls = %d, want 0 (dry-run must not invoke backend)",
				src.sscCalls.Load())
		}
		if src.getCalls.Load() != 0 || dst.putCalls.Load() != 0 {
			t.Errorf("Get=%d Put=%d, want both 0 in dry-run",
				src.getCalls.Load(), dst.putCalls.Load())
		}
	})
}
