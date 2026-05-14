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
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/ratelimit"
)

// runSync is the entry point for TaskTypeSync. Called from Executor.Run.
//
// Flow (per design.md §8.1):
//  1. List src under t.SrcPath (recursive=true).
//  2. For each Entry:
//     a. Skip if Filter.Match == false (FilesSkipped++).
//     b. Rebase entry key onto t.DstPath.
//     c. Head dst: if size matches (and etag matches when both have one),
//     treat as already-synced and skip.
//     d. Get src → Put dst with a worker pool of t.Parallelism for fan-out
//     across files (each file streams sequentially via io.Pipe).
//     e. AfterCopy == verify_then_delete_src: re-Head dst to confirm size
//     matches, then Delete src.
//     f. Update Progress (atomic adds) and call Reporter callbacks.
//  3. After ALL transfers succeed (no errors), if t.Retention is configured:
//     list dst under t.DstPath, run Retention.SelectToDelete, Delete each.
//     §G-1 invariant: retention MUST NOT run if any sync entry failed.
//  4. Return nil on success, or the first fatal error otherwise.
func (e *Executor) runSync(ctx context.Context, t *Task, r Reporter, p *Progress) error {
	now := time.Now()

	// ------------------------------------------------------------------
	// Phase 1: list source.
	// ------------------------------------------------------------------
	listCh, err := t.Src.List(ctx, t.SrcPath, true)
	if err != nil {
		return fmt.Errorf("sync: list src %q: %w", t.SrcPath, err)
	}

	// ------------------------------------------------------------------
	// Phase 2: worker pool.
	//
	// jobs is a buffered channel feeding N transfer workers. The producer
	// goroutine reads from listCh, applies Filter, and emits a job per
	// admitted entry. Skipped entries update Progress directly.
	// ------------------------------------------------------------------
	workers := e.transfersPerTask(t)
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan backend.Entry, workers*2)

	// firstErr records the first fatal error from any worker or the
	// producer. Wrapped in sync.Once so concurrent errors don't clobber.
	var (
		firstErr  error
		firstOnce sync.Once
	)
	recordErr := func(e error) {
		if e == nil {
			return
		}
		firstOnce.Do(func() { firstErr = e })
	}

	// Worker goroutines.
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case entry, ok := <-jobs:
					if !ok {
						return
					}
					if werr := e.syncOneFile(ctx, t, entry, r, p); werr != nil {
						atomic.AddInt64(&p.FilesFailed, 1)
						r.OnFileDone(entry.Key, 0, werr)
						recordErr(werr)
						// Do not return — drain remaining jobs so the
						// producer doesn't block on a full channel. The
						// recorded error fails the whole task at the end.
					}
				}
			}
		}()
	}

	// Producer: read listCh → filter → enqueue. Runs inline (this
	// goroutine) so workers can drain jobs as we produce them.
	produceErr := func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case entry, ok := <-listCh:
				if !ok {
					return nil
				}
				if entry.Err != nil {
					return fmt.Errorf("sync: list src: %w", entry.Err)
				}
				if entry.IsDir {
					continue
				}
				// P1-7 sharding: drop entries that don't map to our
				// shard BEFORE counting them. The other N-1 sub-tasks
				// will count their share, so the parent's aggregate
				// FilesTotal across all shards equals the un-sharded
				// total. Default ShardTotal=0 keeps every entry.
				if t.ShardTotal > 0 && !ShouldKeep(entry.Key, t.ShardIndex, t.ShardTotal) {
					continue
				}
				atomic.AddInt64(&p.FilesTotal, 1)
				atomic.AddInt64(&p.BytesTotal, entry.Size)
				if !t.Filter.Match(entry, now) {
					atomic.AddInt64(&p.FilesSkipped, 1)
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case jobs <- entry:
				}
			}
		}
	}()
	close(jobs)
	wg.Wait()

	if produceErr != nil {
		recordErr(produceErr)
	}

	// Honour cancellation explicitly so the caller sees context.Canceled
	// (which Run maps to StatusCancelled).
	if err := ctx.Err(); err != nil {
		recordErr(err)
	}

	if firstErr != nil {
		return firstErr
	}

	// ------------------------------------------------------------------
	// Phase 3: retention (only if every transfer succeeded).
	// §G-1 invariant — see design.md §9 Phase G-1.
	// ------------------------------------------------------------------
	if t.Retention.Pattern != "" {
		if rerr := e.runRetention(ctx, t, now); rerr != nil {
			return fmt.Errorf("sync: retention on dst %q: %w", t.DstPath, rerr)
		}
	}
	return nil
}

// syncOneFile transfers a single entry from src to dst. Heads dst first to
// support idempotent re-runs; on size (and optional etag) match the file is
// counted as Skipped and no bytes are moved.
func (e *Executor) syncOneFile(
	ctx context.Context,
	t *Task,
	entry backend.Entry,
	r Reporter,
	p *Progress,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dstKey, err := rebaseKey(entry.Key, t.SrcPath, t.DstPath)
	if err != nil {
		return fmt.Errorf("rebase key %q: %w", entry.Key, err)
	}

	// Idempotency check: if dst already has matching size (and matching
	// etag when both sides report one), skip.
	if dstSize, dstETag, _, herr := t.Dst.Head(ctx, dstKey); herr == nil {
		if dstSize == entry.Size {
			if entry.ETag == "" || dstETag == "" || entry.ETag == dstETag {
				atomic.AddInt64(&p.FilesSkipped, 1)
				r.OnFileDone(entry.Key, 0, nil)
				return nil
			}
		}
	} else if !errors.Is(herr, backend.ErrKeyNotFound) {
		// Surface unexpected Head errors. ErrKeyNotFound is the happy
		// path — dst doesn't have the file yet, we'll write it below.
		return fmt.Errorf("head dst %q: %w", dstKey, herr)
	}

	r.OnFileStart(entry.Key, entry.Size)

	// Stream src → dst through an io.Pipe. The reader side runs Put in a
	// goroutine; the writer side runs Get-then-Copy on this goroutine.
	pr, pw := io.Pipe()

	putErrCh := make(chan error, 1)
	go func() {
		// Note: StorageClass intentionally not threaded through here.
		// PutOptions.StorageClass would be sourced from the rule config
		// (dst.storageClass) — that's a future wiring step (see
		// design.md §4.2). Empty is the safe default; every Backend
		// honours its own.
		_, perr := t.Dst.Put(ctx, dstKey, pr, entry.Size, backend.PutOptions{})
		// Drain any remaining bytes / unblock the writer on Put failure.
		_ = pr.CloseWithError(perr)
		putErrCh <- perr
	}()

	rc, gerr := t.Src.Get(ctx, entry.Key, 0, 0)
	if gerr != nil {
		_ = pw.CloseWithError(gerr)
		<-putErrCh // drain the writer goroutine
		return fmt.Errorf("get src %q: %w", entry.Key, gerr)
	}
	// Wrap the source reader with the layered bandwidth limiter (§12.4).
	// Wrapping the READER side is the canonical placement — the Put side
	// consumes from the pipe, so throttling the producer naturally back-
	// pressures the consumer through the io.Pipe.
	var src io.Reader = rc
	if lim := e.buildTransferLimiter(t); lim != nil {
		src = ratelimit.NewLimitedReader(ctx, rc, lim)
	}
	n, copyErr := io.Copy(pw, src)
	_ = rc.Close()
	_ = pw.CloseWithError(copyErr)
	putErr := <-putErrCh

	if copyErr != nil {
		return fmt.Errorf("copy %q -> %q: %w", entry.Key, dstKey, copyErr)
	}
	if putErr != nil {
		return fmt.Errorf("put dst %q: %w", dstKey, putErr)
	}

	atomic.AddInt64(&p.FilesDone, 1)
	atomic.AddInt64(&p.BytesDone, n)

	// AfterCopy: verify_then_delete_src re-heads dst and only deletes src
	// if the bytes landed at the expected size.
	if t.AfterCopy == AfterCopyVerifyThenDeleteSrc {
		dstSize, _, _, verr := t.Dst.Head(ctx, dstKey)
		if verr != nil {
			return fmt.Errorf("verify dst %q: %w", dstKey, verr)
		}
		if dstSize != entry.Size {
			return fmt.Errorf("verify dst %q: size %d != src size %d", dstKey, dstSize, entry.Size)
		}
		if derr := t.Src.Delete(ctx, entry.Key); derr != nil {
			return fmt.Errorf("delete src %q after verify: %w", entry.Key, derr)
		}
	}

	r.OnFileDone(entry.Key, n, nil)
	return nil
}

// runRetention lists dst under t.DstPath, asks Retention.SelectToDelete
// which entries violate the policy, and deletes them one by one. Called
// only after a fully successful sync pass.
func (e *Executor) runRetention(ctx context.Context, t *Task, now time.Time) error {
	dstCh, err := t.Dst.List(ctx, t.DstPath, true)
	if err != nil {
		return fmt.Errorf("list dst: %w", err)
	}
	entries := make([]backend.Entry, 0, 64)
	for entry := range dstCh {
		if entry.Err != nil {
			return fmt.Errorf("list dst: %w", entry.Err)
		}
		if entry.IsDir {
			continue
		}
		entries = append(entries, entry)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	toDelete := t.Retention.SelectToDelete(entries, now)
	for _, victim := range toDelete {
		if err := ctx.Err(); err != nil {
			return err
		}
		if derr := t.Dst.Delete(ctx, victim.Key); derr != nil {
			return fmt.Errorf("delete %q: %w", victim.Key, derr)
		}
	}
	return nil
}

// rebaseKey rewrites a source key under srcPath to live under dstPath. Both
// paths are treated as object-store style slash keys.
//
//	rebaseKey("runs/a/b.pt", "runs/", "warm/") → ("warm/a/b.pt", nil)
//	rebaseKey("runs/a/b.pt", "runs",  "warm")  → ("warm/a/b.pt", nil)
//	rebaseKey("runs/a/b.pt", "",      "warm/") → ("warm/runs/a/b.pt", nil)
//
// If srcKey does not have srcPath as a prefix (a lister bug), returns an
// error so the per-file failure is visible. Shared with load_task.go.
func rebaseKey(srcKey, srcPath, dstPath string) (string, error) {
	src := strings.TrimRight(srcPath, "/")
	dst := strings.TrimRight(dstPath, "/")
	var rel string
	switch {
	case src == "":
		rel = srcKey
	case srcKey == src:
		if dst == "" {
			return "", fmt.Errorf("rebase: single-file src %q with empty dst", srcKey)
		}
		return dst, nil
	case strings.HasPrefix(srcKey, src+"/"):
		rel = strings.TrimPrefix(srcKey, src+"/")
	default:
		return "", fmt.Errorf("rebase: key %q is not under srcPath %q", srcKey, srcPath)
	}
	rel = strings.TrimLeft(rel, "/")
	if dst == "" {
		return rel, nil
	}
	if rel == "" {
		return dst, nil
	}
	return dst + "/" + rel, nil
}
