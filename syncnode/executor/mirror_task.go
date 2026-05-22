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

// runMirror / deleteDstExtras implement the type=mirror task path
// (rclone-gap-roadmap.md §6). Kept in a separate file from sync_task.go to
// hold sync_task.go under the 800-line guideline — sync_task.go already
// owns the runSync + syncOneFile complexity and the prompt explicitly asks
// runSync to remain untouched by mirror semantics. The two functions here
// live in the same package and use the same Reporter / Progress / counters
// patterns as runSync, so the split is purely organisational.
package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cubefs/cubefs/util/log"
)

// Mirror counters. Mirror is "sync copy phase + dst-only delete phase";
// these counters tally only the delete phase (the copy phase already
// feeds Progress via syncOneFile). Pattern matches DryRunStats /
// ServerSideCopyStats: package-level atomics + a struct-shaped snapshot
// accessor so dashboards / tests / metrics exporters can read by name.
//
// MirrorDeleted accumulates real Dst.Delete successes; MirrorWouldDelete
// accumulates the dry-run "would_delete_dst" branch; MirrorDeleteErr
// counts Dst.Delete failures that escaped to the task error.
var (
	mirrorDeleted     atomic.Int64
	mirrorWouldDelete atomic.Int64
	mirrorDeleteErr   atomic.Int64
)

// MirrorStatsSnapshot is the structured form returned by MirrorStats.
// Mirrors DryRunStatsSnapshot's shape so dashboards rendering both can
// share a render path.
type MirrorStatsSnapshot struct {
	Deleted     int64 `json:"deleted"`
	WouldDelete int64 `json:"wouldDelete"`
	DeleteErr   int64 `json:"deleteErr"`
}

// MirrorStats returns the current cumulative mirror counters. Each field
// is read with atomic.LoadInt64; concurrent updates may produce a
// slightly stale view across fields but each field is individually
// consistent. Tests reset via resetMirrorStats(t) (mirror_test.go).
func MirrorStats() MirrorStatsSnapshot {
	return MirrorStatsSnapshot{
		Deleted:     mirrorDeleted.Load(),
		WouldDelete: mirrorWouldDelete.Load(),
		DeleteErr:   mirrorDeleteErr.Load(),
	}
}

// runMirror is a thin wrapper that chains the sync copy phase
// (runSync — unchanged, AfterCopy locked to verify_then_skip by
// validateTask) with the dst-only prune phase (deleteDstExtras). Keeping
// runSync untouched means the legacy sync code path is bit-for-bit
// preserved; the only mirror-specific code lives here and in
// deleteDstExtras.
//
// On failure of the copy phase we DO NOT delete dst extras — a partial
// copy + a destructive prune would amplify the damage. Operators can
// retry; once the copy succeeds the prune runs.
func (e *Executor) runMirror(ctx context.Context, t *Task, r Reporter, p *Progress) error {
	if err := e.runSync(ctx, t, r, p); err != nil {
		return fmt.Errorf("mirror copy phase: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.deleteDstExtras(ctx, t); err != nil {
		return fmt.Errorf("mirror prune phase: %w", err)
	}
	return nil
}

// deleteDstExtras lists dst, lists src, and deletes every dst entry whose
// rebased key is not present in the src index.
//
// Memory trade-off: we re-list src once instead of piggybacking on the
// copy phase. Re-listing keeps the two phases decoupled (runSync stays
// untouched, no cross-phase shared state to thread through worker pools)
// at the cost of one extra src List + an in-memory map of src keys.
// Mirror is documented as a "reasonably-sized volume" tool; very large
// trees that exceed available RAM should use LCNode lifecycle for cold-
// tier pruning instead.
//
// DryRun short-circuits to counter increments + structured logs; no
// Dst.Delete is invoked. Confirm + DryRun gating happens in validateTask
// so by the time we get here a non-DryRun mirror run is operator-armed.
func (e *Executor) deleteDstExtras(ctx context.Context, t *Task) error {
	srcSet, err := listSrcKeySet(ctx, t)
	if err != nil {
		return err
	}

	dstCh, err := t.Dst.List(ctx, t.DstPath, true)
	if err != nil {
		return fmt.Errorf("list dst: %w", err)
	}
	for entry := range dstCh {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Err != nil {
			return fmt.Errorf("list dst: %w", entry.Err)
		}
		if entry.IsDir {
			continue
		}
		// Reverse-rebase the dst key into the src namespace so we can
		// diff against the src index built above. unrebaseKey mirrors
		// rebaseKey's semantics (strip dstPath, optionally re-prefix with
		// srcPath). On a rebase failure we skip the entry rather than
		// fail the whole prune — a malformed key is operator-visible via
		// log but not worth aborting a partial cleanup.
		correspondingSrcKey, rerr := unrebaseKey(entry.Key, t.SrcPath, t.DstPath)
		if rerr != nil {
			log.LogWarnf("syncnode: mirror skip dst key %q: %v", entry.Key, rerr)
			continue
		}
		if _, present := srcSet[correspondingSrcKey]; present {
			continue
		}
		// dst-only key — prune.
		if t.DryRun {
			mirrorWouldDelete.Add(1)
			log.LogDebugf("syncnode: task=%s dryrun=true action=would_delete_dst key=%q size=%d",
				t.ID, entry.Key, entry.Size)
			continue
		}
		if derr := t.Dst.Delete(ctx, entry.Key); derr != nil {
			// Soft-fail: continue iterating so a single transient error
			// doesn't leave the dst half-pruned. The aggregate error is
			// reported via the counter, and the first error is returned
			// at the end so the task surfaces Failed when delete failed.
			mirrorDeleteErr.Add(1)
			log.LogWarnf("syncnode: task=%s mirror delete dst key=%q failed: %v",
				t.ID, entry.Key, derr)
			// First-error-wins via errors.Join would over-complicate the
			// return; instead remember the first delete failure and
			// surface it.
			if err == nil {
				err = fmt.Errorf("delete dst %q: %w", entry.Key, derr)
			}
			continue
		}
		mirrorDeleted.Add(1)
		log.LogDebugf("syncnode: task=%s action=deleted_dst_extra key=%q size=%d",
			t.ID, entry.Key, entry.Size)
	}
	return err
}

// listSrcKeySet lists src under t.SrcPath and returns the set of src
// keys present (recursive). Used by deleteDstExtras to compute the
// dst-only diff. Errors are propagated verbatim so the caller can wrap
// them with mirror-phase context.
func listSrcKeySet(ctx context.Context, t *Task) (map[string]struct{}, error) {
	srcCh, lerr := t.Src.List(ctx, t.SrcPath, true)
	if lerr != nil {
		return nil, fmt.Errorf("list src: %w", lerr)
	}
	out := make(map[string]struct{}, 64)
	for entry := range srcCh {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.Err != nil {
			return nil, fmt.Errorf("list src: %w", entry.Err)
		}
		if entry.IsDir {
			continue
		}
		out[entry.Key] = struct{}{}
	}
	return out, nil
}

// unrebaseKey is the inverse of rebaseKey from sync_task.go: given a key
// that already lives under dstPath, returns what it would have been
// under srcPath. Used by deleteDstExtras to look up the src-side key for
// a dst entry.
//
//	unrebaseKey("warm/a/b.pt", "runs/", "warm/") → ("runs/a/b.pt", nil)
//	unrebaseKey("warm/a/b.pt", "runs",  "warm")  → ("runs/a/b.pt", nil)
//	unrebaseKey("warm",        "runs/", "warm")  → ("runs", nil)   (single-file)
//	unrebaseKey("other/x",     "runs/", "warm")  → (_,    error)  (not under dst)
func unrebaseKey(dstKey, srcPath, dstPath string) (string, error) {
	src := strings.TrimRight(srcPath, "/")
	dst := strings.TrimRight(dstPath, "/")
	var rel string
	switch {
	case dst == "":
		rel = dstKey
	case dstKey == dst:
		if src == "" {
			return "", errors.New("unrebase: single-file dst with empty src")
		}
		return src, nil
	case strings.HasPrefix(dstKey, dst+"/"):
		rel = strings.TrimPrefix(dstKey, dst+"/")
	default:
		return "", fmt.Errorf("unrebase: key %q is not under dstPath %q", dstKey, dstPath)
	}
	rel = strings.TrimLeft(rel, "/")
	if src == "" {
		return rel, nil
	}
	if rel == "" {
		return src, nil
	}
	return src + "/" + rel, nil
}

