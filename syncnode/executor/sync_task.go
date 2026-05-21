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
				// P1-7 + P2-5 sharding: drop entries that don't map to our
				// shard BEFORE counting them. The other N-1 sub-tasks
				// will count their share, so the parent's aggregate
				// FilesTotal across all shards equals the un-sharded
				// total. ShouldKeep handles hash-mode (default) AND
				// prefix-mode (Task.ShardPrefixes != nil) — see
				// shard.go for the dispatch rule.
				if t.ShardTotal > 0 && !ShouldKeep(entry.Key, t.ShardIndex, t.ShardTotal, t.ShardPrefixes) {
					continue
				}
				atomic.AddInt64(&p.FilesTotal, 1)
				atomic.AddInt64(&p.BytesTotal, entry.Size)
				if !t.Filter.Match(entry, now) {
					atomic.AddInt64(&p.FilesSkipped, 1)
					atomic.AddInt64(&p.BytesSkipped, entry.Size)
					if p.Sampler != nil {
						p.Sampler.add(entry.Key)
					}
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

// errSourceMutated is the per-file sentinel for the P1 「source mutated mid-
// transfer」 path. Local to executor — callers above the worker fold it into
// FilesFailed / FilesSkipped via OnSourceMutated routing.
var errSourceMutated = errors.New("sync: source mutated mid-transfer")

// headSnapshot captures the size + mtime + etag triple used to detect
// concurrent mutation of the source key. Zero value means "not snapshotted"
// (mutated() returns false in that case to keep the legacy code path).
type headSnapshot struct {
	taken bool
	size  int64
	mtime time.Time
	etag  string
}

// headSnapshotOf takes a fresh Head of key on b. headErr is surfaced to the
// caller; on error we still return a non-taken snapshot so mutated() short-
// circuits to "no change observable".
func headSnapshotOf(ctx context.Context, b backend.Backend, key string) (headSnapshot, error) {
	sz, etag, mt, err := b.Head(ctx, key)
	if err != nil {
		return headSnapshot{}, err
	}
	return headSnapshot{taken: true, size: sz, mtime: mt, etag: etag}, nil
}

// mutated reports whether two snapshots disagree on size / etag / mtime.
// When either snapshot is not taken, returns false (cannot prove change).
// ETag is only compared when both sides have one — locally / on POSIX it's
// usually empty and we lean on size + mtime instead.
func mutated(pre, post headSnapshot) bool {
	if !pre.taken || !post.taken {
		return false
	}
	if pre.size != post.size {
		return true
	}
	if pre.etag != "" && post.etag != "" && pre.etag != post.etag {
		return true
	}
	if !pre.mtime.IsZero() && !post.mtime.IsZero() && !pre.mtime.Equal(post.mtime) {
		return true
	}
	return false
}

// checksumEqual reports whether two backend-reported checksums are
// comparable AND equal. Differing algorithms (sha256 vs md5) are treated as
// "not equal" — strong mode refuses to bridge across hash families.
func checksumEqual(sumA, algoA, sumB, algoB string) bool {
	if sumA == "" || sumB == "" {
		return false
	}
	if algoA == "" || algoB == "" {
		return false
	}
	if algoA != algoB {
		return false
	}
	return sumA == sumB
}

// backoffSleep waits 1s, 2s, 4s, ..., 32s (capped at 30s) before the next
// retry attempt. attempt is 1-based for the FIRST retry (attempt=0 means no
// sleep — the first try has not happened yet). Honours ctx cancellation so
// shutdown isn't blocked by a long backoff.
func backoffSleep(ctx context.Context, attempt int) {
	if attempt < 1 {
		return
	}
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	d := time.Duration(1<<shift) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// transferResult carries everything the per-file retry loop needs to
// progress: bytes acked, optional resume token (for s3 multipart), and the
// PutResult itself (so strong-mode can compare the upload-side checksum).
type transferResult struct {
	put          backend.PutResult
	bytesAcked   int64
	uploadID     string
	partialBytes int64 // bytes the dst is known to have on disk if Put failed
}

// transferOnce performs a single Get→io.Pipe→Put round. resumeOffset and
// resumeUploadID are advisory: backends that don't support resume ignore
// them. computeChecksum threads the strong-mode flag through PutOptions.
//
// Returns the transferResult on success; on failure the result still carries
// any partial-bytes count we observed via io.Copy (so the caller can persist
// a breakpoint before retrying).
func (e *Executor) transferOnce(
	ctx context.Context,
	t *Task,
	entry backend.Entry,
	dstKey string,
	resumeOffset int64,
	resumeUploadID string,
	computeChecksum bool,
) (transferResult, error) {
	pr, pw := io.Pipe()

	putOpts := backend.PutOptions{
		ComputeChecksum: computeChecksum,
	}
	// Note: StorageClass intentionally not threaded through here.
	// PutOptions.StorageClass would be sourced from the rule config
	// (dst.storageClass) — see design.md §4.2.

	type putOutcome struct {
		res backend.PutResult
		err error
	}
	putCh := make(chan putOutcome, 1)
	go func() {
		res, perr := t.Dst.Put(ctx, dstKey, pr, entry.Size, putOpts)
		_ = pr.CloseWithError(perr)
		putCh <- putOutcome{res: res, err: perr}
	}()

	rc, gerr := t.Src.Get(ctx, entry.Key, resumeOffset, 0)
	if gerr != nil {
		_ = pw.CloseWithError(gerr)
		<-putCh // drain the writer goroutine
		return transferResult{uploadID: resumeUploadID}, fmt.Errorf("get src %q: %w", entry.Key, gerr)
	}
	// Wrap source reader with the layered bandwidth limiter (§12.4).
	var src io.Reader = rc
	if lim := e.buildTransferLimiter(t); lim != nil {
		src = ratelimit.NewLimitedReader(ctx, rc, lim)
	}
	n, copyErr := io.Copy(pw, src)
	_ = rc.Close()
	_ = pw.CloseWithError(copyErr)
	out := <-putCh

	result := transferResult{
		put:        out.res,
		bytesAcked: out.res.BytesPut,
		uploadID:   resumeUploadID,
	}
	if copyErr != nil {
		// Copy aborted mid-stream; preserve what we observed for resume.
		result.partialBytes = resumeOffset + n
		return result, fmt.Errorf("copy %q -> %q: %w", entry.Key, dstKey, copyErr)
	}
	if out.err != nil {
		// Put failed; the bytes we shoved through io.Copy may or may not
		// have made it to dst — backend can't tell us without a Head, so
		// we surface what we know on the src side.
		result.partialBytes = resumeOffset + n
		return result, fmt.Errorf("put dst %q: %w", dstKey, out.err)
	}
	// Successful upload: bytes acked is the full source size.
	if result.bytesAcked == 0 {
		result.bytesAcked = n
	}
	return result, nil
}

// syncOneFile transfers a single entry from src to dst. The flow honours the
// data-integrity P0/P1/P2 design (docs/plan/syncnode/data-integrity-p0-p2.md):
//   - P0: ChecksumMode == "strong" computes sha256 on the src side during
//     transfer, verifies against dst checksum (native or metadata) before
//     allowing AfterCopy=verify_then_delete_src to delete src.
//   - P1: OnSourceMutated != "" enables Pre/Post-Head of src; on size /
//     mtime / etag drift dst is rolled back and the file is failed / skipped /
//     retried.
//   - P2: MaxRetries / ResumeEnabled give per-file exponential-backoff retry
//     and breakpoint resume from bolt.InProgressStore.
//
// All three layers are opt-in. The legacy code path (no ChecksumMode, no
// OnSourceMutated, MaxRetries=0, ResumeEnabled=false) is preserved bit-for-
// bit by short-circuiting through the same transferOnce helper.
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

	// 1) Pre-Head src (P1) — only when OnSourceMutated is enabled.
	var srcPre headSnapshot
	if t.OnSourceMutated != "" {
		snap, herr := headSnapshotOf(ctx, t.Src, entry.Key)
		if herr != nil {
			return fmt.Errorf("pre-head src %q: %w", entry.Key, herr)
		}
		srcPre = snap
	}

	// 2) Idempotent skip: if dst already matches what we'd write, return
	// without transferring. The decision tree:
	//   - size + ETag agree → skip (legacy behaviour)
	//   - dst size matches and dst has no ETag → skip (POSIX/CFS lack ETag)
	//   - ChecksumMode=="strong" → upgrade the skip decision: when both
	//     backends report comparable checksums for the same key AND the
	//     checksums agree, skip; otherwise fall through to a full transfer.
	if dstSize, dstETag, _, herr := t.Dst.Head(ctx, dstKey); herr == nil {
		if shouldSkipExistingDst(ctx, t, entry, dstKey, dstSize, dstETag) {
			atomic.AddInt64(&p.FilesSkipped, 1)
			atomic.AddInt64(&p.BytesSkipped, entry.Size)
			if p.Sampler != nil {
				p.Sampler.add(entry.Key)
			}
			r.OnFileDone(entry.Key, 0, nil)
			return nil
		}
	} else if !errors.Is(herr, backend.ErrKeyNotFound) {
		return fmt.Errorf("head dst %q: %w", dstKey, herr)
	}

	r.OnFileStart(entry.Key, entry.Size)

	// 3) Resume? (P2) — pull breakpoint when caller wired the store and the
	// task opts in. Failures are non-fatal: a missing breakpoint means we
	// start from offset 0.
	var resumeOffset int64
	var resumeUploadID string
	if t.ResumeEnabled && e.inprogress != nil {
		bp, gerr := e.inprogress.Get(ctx, breakpointKey(t.ID, entry.Key))
		if gerr == nil && bp != nil {
			resumeOffset = bp.BytesDone
			resumeUploadID = bp.UploadID
		}
	}

	// 4) Per-file retry loop (P2). attempt=0 is the first try; subsequent
	// rounds backoff before transferOnce. lastErr is the most recent
	// failure we've observed; on exhaustion it's what the caller sees.
	var (
		lastErr  error
		bytesAck int64
	)
	strongMode := t.ChecksumMode == "strong"
	for attempt := 0; attempt <= t.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			backoffSleep(ctx, attempt)
		}

		result, terr := e.transferOnce(ctx, t, entry, dstKey, resumeOffset, resumeUploadID, strongMode)
		if terr != nil {
			lastErr = terr
			// Persist breakpoint when we have something to resume from.
			if t.ResumeEnabled && e.inprogress != nil && result.partialBytes > 0 {
				_ = e.inprogress.Put(ctx, &Breakpoint{
					TaskID:    t.ID,
					Key:       breakpointKey(t.ID, entry.Key),
					BytesDone: result.partialBytes,
					UploadID:  result.uploadID,
				})
				resumeOffset = result.partialBytes
				resumeUploadID = result.uploadID
			}
			continue
		}
		bytesAck = result.bytesAcked
		if bytesAck == 0 {
			bytesAck = entry.Size
		}

		// 5) Post-Head src (P1) — detect mid-transfer mutation. We rolled
		// dst forward unconditionally, so a mutated src means the dst we
		// just wrote is stale; nuke it before deciding fail/skip/retry.
		if t.OnSourceMutated != "" {
			srcPost, herr := headSnapshotOf(ctx, t.Src, entry.Key)
			if herr != nil {
				lastErr = fmt.Errorf("post-head src %q: %w", entry.Key, herr)
				continue
			}
			if mutated(srcPre, srcPost) {
				_ = t.Dst.Delete(ctx, dstKey)
				lastErr = errSourceMutated
				switch t.OnSourceMutated {
				case "skip":
					atomic.AddInt64(&p.FilesSkipped, 1)
					atomic.AddInt64(&p.BytesSkipped, entry.Size)
					if p.Sampler != nil {
						p.Sampler.add(entry.Key)
					}
					r.OnFileDone(entry.Key, 0, nil)
					if t.ResumeEnabled && e.inprogress != nil {
						_ = e.inprogress.Delete(ctx, breakpointKey(t.ID, entry.Key))
					}
					return nil
				case "fail":
					return lastErr
				case "retry":
					// snapshot moves forward so the next attempt's diff is
					// versus the latest observed state.
					srcPre = srcPost
					continue
				default:
					// Unknown value → fail closed.
					return fmt.Errorf("invalid OnSourceMutated %q: %w", t.OnSourceMutated, lastErr)
				}
			}
		}

		// 6) Strong checksum verify (P0). Only the strong mode path runs
		// GetChecksum on dst; legacy modes already passed the size-based
		// idempotency check earlier.
		if strongMode {
			dstSum, dstAlgo, gerr := t.Dst.GetChecksum(ctx, dstKey)
			if gerr != nil {
				_ = t.Dst.Delete(ctx, dstKey)
				lastErr = fmt.Errorf("get dst checksum %q: %w", dstKey, gerr)
				continue
			}
			if !checksumEqual(result.put.Checksum, result.put.Algorithm, dstSum, dstAlgo) {
				_ = t.Dst.Delete(ctx, dstKey)
				lastErr = backend.ErrChecksumMismatch
				continue
			}
		}

		// 7) AfterCopy=verify_then_delete_src (P0 升级). validateTask
		// already guarantees ChecksumMode=="strong" at task start; the
		// strong verify above passed, so deletion is safe.
		if t.AfterCopy == AfterCopyVerifyThenDeleteSrc {
			if derr := t.Src.Delete(ctx, entry.Key); derr != nil {
				return fmt.Errorf("delete src %q after verify: %w", entry.Key, derr)
			}
		}

		// 8) Clear breakpoint and tally bytes done.
		if t.ResumeEnabled && e.inprogress != nil {
			_ = e.inprogress.Delete(ctx, breakpointKey(t.ID, entry.Key))
		}
		atomic.AddInt64(&p.FilesDone, 1)
		atomic.AddInt64(&p.BytesDone, bytesAck)
		r.OnFileDone(entry.Key, bytesAck, nil)
		return nil
	}
	return lastErr
}

// shouldSkipExistingDst implements the «is dst already a verified match?»
// decision used by the idempotency check at the top of syncOneFile. The
// data-integrity SDD says size-only skip is unsafe: same-size mutations
// silently slip through. So unless we have at least one comparable
// signature (matching ETag or strong-mode checksum), we MUST re-upload.
//
//   - ChecksumMode=="strong": skip iff both sides return comparable
//     checksums and they agree.
//   - Otherwise: skip iff src ETag and dst ETag are both non-empty and
//     equal. POSIX-style backends (no ETag) always re-upload; this is
//     the safe default.
func shouldSkipExistingDst(
	ctx context.Context,
	t *Task,
	entry backend.Entry,
	dstKey string,
	dstSize int64,
	dstETag string,
) bool {
	if dstSize != entry.Size {
		return false
	}
	if t.ChecksumMode == "strong" {
		srcSum, srcAlgo, serr := t.Src.GetChecksum(ctx, entry.Key)
		if serr != nil {
			return false
		}
		dstSum, dstAlgo, derr := t.Dst.GetChecksum(ctx, dstKey)
		if derr != nil {
			return false
		}
		return checksumEqual(srcSum, srcAlgo, dstSum, dstAlgo)
	}
	return entry.ETag != "" && dstETag != "" && entry.ETag == dstETag
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
