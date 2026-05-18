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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/ratelimit"
)

// runLoad is the entry point for TaskTypeLoad. Called from Executor.Run.
//
// See design.md §8.2 (load data flow) and §9 Phase D-3 (this task).
//
// Flow:
//  1. List src under t.SrcPath (recursive=true).
//  2. For each Entry that passes t.Filter.Match, schedule a transfer worker
//     to:
//     a. Compute dst key by rebasing entry.Key from t.SrcPath onto t.DstPath.
//     b. Head dst → if size matches src size, skip (FilesSkipped++).
//     c. According to DownloadStrategy:
//     - temp_rename (default): Put body to "<dst>.downloading.<task_id>",
//     then Rename(temp, dst). Falls back to a non-atomic Delete + log
//     when the backend reports ErrBackendUnsupported (§10.6 Caps).
//     - direct: Put straight to dst key (no temp suffix).
//     d. Verify: Head dst, confirm size matches src size; on mismatch
//     FilesFailed++ and best-effort cleanup of the temp.
//  3. On cancellation / fatal error: best-effort delete any temp keys we
//     created. The orphan-temp cleanup is sequential and ignores errors —
//     we just want the next run to see a clean destination.
func (e *Executor) runLoad(ctx context.Context, t *Task, r Reporter, p *Progress) error {
	workers := e.transfersPerTask(t)
	if workers < 1 {
		workers = 1
	}

	// tempKeys tracks the in-flight / created temp destination keys so we
	// can clean them up if the task is cancelled before they're renamed
	// into place. We add the temp key to the map BEFORE the Put call and
	// remove it AFTER a successful Rename. On cancel/error we iterate the
	// map and best-effort Delete each remaining entry.
	var tempKeys sync.Map // map[string]struct{}

	now := time.Now()
	listCh, err := t.Src.List(ctx, t.SrcPath, true)
	if err != nil {
		return fmt.Errorf("list src %q: %w", t.SrcPath, err)
	}

	// entryCh fans entries out to the worker pool. We size the buffer
	// conservatively — listing is generally faster than transferring,
	// and an unbounded queue would hide backpressure problems.
	entryCh := make(chan backend.Entry, workers*2)

	// errCh captures the first fatal error from workers / lister. Workers
	// don't abort on per-file failures (those bump FilesFailed); a fatal
	// error here is something that should stop the entire task (e.g. the
	// listing channel itself failed, or ctx cancelled).
	var (
		fatalOnce sync.Once
		fatalErr  error
	)
	setFatal := func(err error) {
		if err == nil {
			return
		}
		fatalOnce.Do(func() { fatalErr = err })
	}

	// Worker pool. Each worker pulls entries off entryCh and processes them.
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for entry := range entryCh {
				// Cheap, frequent cancellation check: if ctx is cancelled,
				// drain the channel without doing further work so the
				// lister isn't blocked sending into it.
				if ctx.Err() != nil {
					continue
				}
				e.loadOne(ctx, t, entry, p, r, &tempKeys)
			}
		}()
	}

	// Lister goroutine: pipes filtered entries onto entryCh until the
	// source's List channel closes or ctx is cancelled. On ctx cancel we
	// close entryCh so workers exit cleanly; on a list-channel error we
	// record it as fatal.
	var listWg sync.WaitGroup
	listWg.Add(1)
	go func() {
		defer listWg.Done()
		defer close(entryCh)
		for {
			select {
			case <-ctx.Done():
				setFatal(ctx.Err())
				return
			case entry, ok := <-listCh:
				if !ok {
					return
				}
				if entry.Err != nil {
					setFatal(fmt.Errorf("list src: %w", entry.Err))
					return
				}
				if entry.IsDir {
					continue
				}
				// P1-7 + P2-5 sharding: hash- or prefix-mode filter
				// before counting / dispatching to workers. Default
				// ShardTotal=0 disables sharding (every entry kept).
				if t.ShardTotal > 0 && !ShouldKeep(entry.Key, t.ShardIndex, t.ShardTotal, t.ShardPrefixes) {
					continue
				}
				if !t.Filter.Match(entry, now) {
					continue
				}
				atomic.AddInt64(&p.FilesTotal, 1)
				atomic.AddInt64(&p.BytesTotal, entry.Size)
				select {
				case entryCh <- entry:
				case <-ctx.Done():
					setFatal(ctx.Err())
					return
				}
			}
		}
	}()

	listWg.Wait()
	wg.Wait()

	// Cleanup any temp keys still in flight (either because we were
	// cancelled mid-Put, or because Verification failed and we didn't get
	// to clean them inline). Errors here are intentionally ignored — the
	// goal is "leave the dst clean for next run", not "fail loudly".
	tempKeys.Range(func(k, _ interface{}) bool {
		key, _ := k.(string)
		if key == "" {
			return true
		}
		// Use a fresh context (parent may already be cancelled). Bound the
		// cleanup so a wedged Delete doesn't block forever.
		cleanCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = t.Dst.Delete(cleanCtx, key)
		cancel()
		return true
	})

	if fatalErr != nil {
		return fatalErr
	}
	return nil
}

// loadOne handles a single source Entry → dst transfer. Per-file errors
// (Head failures, Put failures, Verification mismatches) increment
// FilesFailed and are reported via Reporter.OnFileDone; they do NOT cause
// the surrounding task to fail. Only ctx cancellation / list-channel errors
// (handled in runLoad) are fatal.
func (e *Executor) loadOne(ctx context.Context, t *Task, entry backend.Entry, p *Progress, r Reporter, tempKeys *sync.Map) {
	if ctx.Err() != nil {
		return
	}

	dstKey, err := rebaseKey(entry.Key, t.SrcPath, t.DstPath)
	if err != nil {
		atomic.AddInt64(&p.FilesFailed, 1)
		r.OnFileDone(entry.Key, 0, err)
		return
	}

	// Skip if dst already exists at the right size. We deliberately don't
	// compare etags — cross-backend etag semantics aren't portable (S3
	// multipart vs. POSIX has-no-etag), so size-only is the safe choice
	// here per the D-3 spec.
	if dstSize, _, _, headErr := t.Dst.Head(ctx, dstKey); headErr == nil {
		if dstSize == entry.Size {
			atomic.AddInt64(&p.FilesSkipped, 1)
			atomic.AddInt64(&p.BytesSkipped, entry.Size)
			r.OnFileDone(entry.Key, 0, nil)
			return
		}
	} else if !errors.Is(headErr, backend.ErrKeyNotFound) {
		// Treat unexpected Head errors as per-file failures rather than
		// fatal. The next run will retry.
		atomic.AddInt64(&p.FilesFailed, 1)
		r.OnFileDone(entry.Key, 0, fmt.Errorf("head dst %q: %w", dstKey, headErr))
		return
	}

	useTemp := t.DownloadStrategy != DownloadStrategyDirect
	var landingKey string
	if useTemp {
		landingKey = tempKeyFor(dstKey, t.ID)
	} else {
		landingKey = dstKey
	}

	r.OnFileStart(entry.Key, entry.Size)

	// Track the temp key BEFORE opening the source — if ctx cancels while
	// we're mid-transfer the cleanup pass in runLoad will Delete it.
	if useTemp {
		tempKeys.Store(landingKey, struct{}{})
	}

	bytesWritten, err := e.transferOne(ctx, t, entry, landingKey)
	if err != nil {
		// Best-effort cleanup of the partial temp; ignore errors.
		if useTemp {
			_ = t.Dst.Delete(context.Background(), landingKey)
			tempKeys.Delete(landingKey)
		}
		atomic.AddInt64(&p.FilesFailed, 1)
		r.OnFileDone(entry.Key, bytesWritten, err)
		return
	}

	if useTemp {
		// Atomic rename into place. Backends that lack atomic rename
		// (object stores) typically still implement Rename via copy+delete
		// — we accept that. If the backend explicitly reports
		// ErrBackendUnsupported, fall back to Delete(dst) + we accept the
		// non-atomic outcome (the temp landed, we just can't promote it
		// atomically). This is documented in §10.6 Caps.
		renameErr := t.Dst.Rename(ctx, landingKey, dstKey)
		if renameErr != nil {
			if errors.Is(renameErr, backend.ErrBackendUnsupported) {
				// Non-atomic fallback: delete any existing dst, leave the
				// temp in place. This is the best we can do on a backend
				// without rename support.
				_ = t.Dst.Delete(ctx, dstKey)
				atomic.AddInt64(&p.FilesFailed, 1)
				r.OnFileDone(entry.Key, bytesWritten, fmt.Errorf("rename unsupported on backend %q; temp left at %q", t.Dst.Kind(), landingKey))
				// Don't remove from tempKeys — let the cleanup pass clear
				// it so we don't pollute the destination.
				return
			}
			_ = t.Dst.Delete(context.Background(), landingKey)
			tempKeys.Delete(landingKey)
			atomic.AddInt64(&p.FilesFailed, 1)
			r.OnFileDone(entry.Key, bytesWritten, fmt.Errorf("rename %q -> %q: %w", landingKey, dstKey, renameErr))
			return
		}
		tempKeys.Delete(landingKey)
	}

	// Verify: re-Head dst, confirm size matches.
	if dstSize, _, _, headErr := t.Dst.Head(ctx, dstKey); headErr != nil || dstSize != entry.Size {
		// Verification failed. Per spec, best-effort delete and bump
		// FilesFailed. We don't delete dst (the user may prefer to keep
		// the half-good file for inspection) — only the temp if we still
		// have one.
		atomic.AddInt64(&p.FilesFailed, 1)
		if headErr != nil {
			r.OnFileDone(entry.Key, bytesWritten, fmt.Errorf("verify head %q: %w", dstKey, headErr))
		} else {
			r.OnFileDone(entry.Key, bytesWritten, fmt.Errorf("verify %q: size mismatch (have=%d want=%d)", dstKey, dstSize, entry.Size))
		}
		return
	}

	atomic.AddInt64(&p.FilesDone, 1)
	atomic.AddInt64(&p.BytesDone, bytesWritten)
	if e.opts.rateLimits != nil {
		e.opts.rateLimits.ObserveBytes(bytesWritten)
	}
	r.OnFileDone(entry.Key, bytesWritten, nil)
}

// transferOne streams entry.Size bytes from src[entry.Key] → dst[landingKey].
// Returns the number of bytes successfully written to the destination (which
// for a successful run equals entry.Size). When the executor has a rate-
// limit registry configured (or the task has a per-task cap) the body is
// wrapped in a LimitedReader so the byte stream conforms to the layered
// bandwidth budget (§12.4).
func (e *Executor) transferOne(ctx context.Context, t *Task, entry backend.Entry, landingKey string) (int64, error) {
	body, err := t.Src.Get(ctx, entry.Key, 0, 0)
	if err != nil {
		return 0, fmt.Errorf("get src %q: %w", entry.Key, err)
	}
	defer body.Close()

	// Apply the layered rate limit on the source side; back-pressure
	// propagates into Put via the wrapped reader. If no layer is
	// configured buildTransferLimiter returns nil and we use the raw body
	// (preserving the historical fast path).
	src := io.Reader(body)
	if lim := e.buildTransferLimiter(t); lim != nil {
		src = ratelimit.NewLimitedReader(ctx, body, lim)
	}

	// We can't observe the byte count cheaply through Put — backends own
	// the read loop. The Verify Head call upstream confirms the final size,
	// so we report entry.Size as the byte count on success and 0 on failure.
	if _, err := t.Dst.Put(ctx, landingKey, src, entry.Size, backend.PutOptions{}); err != nil {
		return 0, fmt.Errorf("put dst %q: %w", landingKey, err)
	}
	return entry.Size, nil
}

// tempKeyFor returns the in-flight landing key for dstKey under taskID. The
// suffix is `.downloading.<task_id>` per design §4.3.
func tempKeyFor(dstKey, taskID string) string {
	return dstKey + ".downloading." + taskID
}

// rebaseKey is defined in sync_task.go and shared between both tasks.
