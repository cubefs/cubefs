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
	"github.com/cubefs/cubefs/util/log"
)

// Server-side copy counters. Mirrors a Prometheus
// `syncnode_server_side_copy_total{result="ok|fallback|error"}` triplet
// without pulling the prometheus dependency into the executor; the metrics
// scraper (or tests) can read these via the exported accessors.
var (
	serverSideCopyOK       atomic.Int64
	serverSideCopyFallback atomic.Int64
	serverSideCopyErr      atomic.Int64
)

// ServerSideCopyStats returns the current cumulative counts. Exposed
// primarily for tests; production code can call this from a metrics
// exporter without taking any lock.
func ServerSideCopyStats() (ok, fallback, errs int64) {
	return serverSideCopyOK.Load(), serverSideCopyFallback.Load(), serverSideCopyErr.Load()
}

// Metadata-unsupported counters (P2 POSIX metadata). Mirror the
// server-side-copy stats pattern: one atomic counter per OnMetadataUnsupported
// branch syncOneFile takes when the dst backend can't honour a Preserve*
// request. Metrics scraper / tests read via MetadataUnsupportedStats().
//
// `warn` counts files where we logged + proceeded without the offending
// metadata (the data still made it).
// `skip` counts files where the file itself was skipped (FilesSkipped++).
// `error` counts files where the failure was propagated up (FilesFailed++).
var (
	metadataUnsupportedWarn  atomic.Int64
	metadataUnsupportedSkip  atomic.Int64
	metadataUnsupportedError atomic.Int64
)

// MetadataUnsupportedStatsSnapshot is the structured form returned by
// MetadataUnsupportedStats(). Same pattern as DryRunStatsSnapshot — caller
// downstream wants to log named counters, not positional ints.
type MetadataUnsupportedStatsSnapshot struct {
	Warn  int64 `json:"warn"`
	Skip  int64 `json:"skip"`
	Error int64 `json:"error"`
}

// MetadataUnsupportedStats returns the cumulative metadata-unsupported
// counters. Exposed primarily for tests; production scrapers can read this
// from a metrics exporter without locking.
func MetadataUnsupportedStats() MetadataUnsupportedStatsSnapshot {
	return MetadataUnsupportedStatsSnapshot{
		Warn:  metadataUnsupportedWarn.Load(),
		Skip:  metadataUnsupportedSkip.Load(),
		Error: metadataUnsupportedError.Load(),
	}
}

// Dry-run counters (子项 2). Mirror the server-side-copy stats pattern: one
// atomic counter per "would_*" branch syncOneFile can take when DryRun=true.
// A metrics exporter can scrape these as
// `syncnode_dry_run_total{action="copy|skip|server_side_copy|delete_src"}`
// without forcing this package to depend on Prometheus.
//
// Counters are package-level and cumulative across all tasks. Tests reset
// them via resetDryRunStats(t) (see dry_run_test.go) so each scenario starts
// from zero. Production code never resets — the rate of change is what
// matters, not the absolute baseline.
var (
	dryRunWouldCopy           atomic.Int64
	dryRunWouldSkip           atomic.Int64
	dryRunWouldServerSideCopy atomic.Int64
	dryRunWouldDeleteSrc      atomic.Int64
)

// DryRunStatsSnapshot is the structured form returned by DryRunStats. We
// expose a struct (rather than a 4-tuple of int64s like ServerSideCopyStats)
// because callers downstream often want to log or render counters by name —
// e.g. a dashboard tooltip "[dry-run] would_copy=12 would_skip=3" — and
// positional returns get confusing once we add the type=mirror would_delete_dst
// counter in wave 3.
type DryRunStatsSnapshot struct {
	WouldCopy           int64 `json:"wouldCopy"`
	WouldSkip           int64 `json:"wouldSkip"`
	WouldServerSideCopy int64 `json:"wouldServerSideCopy"`
	WouldDeleteSrc      int64 `json:"wouldDeleteSrc"`
}

// DryRunStats returns the current cumulative dry-run counters as a struct.
// Exposed primarily for tests and metrics exporters; the values are wide
// snapshots (read with atomic.LoadInt64) so concurrent updates may produce
// a slightly stale view across fields, but each field individually is
// consistent.
func DryRunStats() DryRunStatsSnapshot {
	return DryRunStatsSnapshot{
		WouldCopy:           dryRunWouldCopy.Load(),
		WouldSkip:           dryRunWouldSkip.Load(),
		WouldServerSideCopy: dryRunWouldServerSideCopy.Load(),
		WouldDeleteSrc:      dryRunWouldDeleteSrc.Load(),
	}
}

// dryRunAction names the "would_*" outcome a dry-run pass would log for a
// single entry. Used purely as a typed key for accountDryRun so callers can
// stay declarative ("this branch would_copy") instead of remembering which
// atomic.Add to invoke.
type dryRunAction int

const (
	dryRunActionCopy dryRunAction = iota + 1
	dryRunActionSkip
	dryRunActionServerSideCopy
)

// accountDryRun records a single would_* outcome: the action-specific
// counter, the per-task Progress tally (so the task summary still reports
// realistic numbers in dry-run mode), the move-mode would_delete_src side
// effect when AfterCopy=verify_then_delete_src, and a structured log line
// dashboards can filter by `dryrun=true`.
//
// Returns nil so callers can `return accountDryRun(...)` cleanly. Reporter
// observers (OnFileDone) are still notified — dashboards expecting a Done
// event per listed entry continue to work in dry-run mode.
func accountDryRun(t *Task, entry backend.Entry, action dryRunAction, r Reporter, p *Progress) error {
	var actionName string
	switch action {
	case dryRunActionCopy:
		dryRunWouldCopy.Add(1)
		atomic.AddInt64(&p.FilesDone, 1)
		atomic.AddInt64(&p.BytesDone, entry.Size)
		actionName = "would_copy"
	case dryRunActionSkip:
		dryRunWouldSkip.Add(1)
		atomic.AddInt64(&p.FilesSkipped, 1)
		atomic.AddInt64(&p.BytesSkipped, entry.Size)
		actionName = "would_skip_existing"
	case dryRunActionServerSideCopy:
		dryRunWouldServerSideCopy.Add(1)
		atomic.AddInt64(&p.FilesDone, 1)
		atomic.AddInt64(&p.BytesDone, entry.Size)
		actionName = "would_server_side_copy"
	default:
		actionName = "would_unknown"
	}
	if t.AfterCopy == AfterCopyVerifyThenDeleteSrc {
		dryRunWouldDeleteSrc.Add(1)
	}
	if p.Sampler != nil {
		p.Sampler.add(entry.Key)
	}
	log.LogDebugf("syncnode: task=%s dryrun=true action=%s key=%s size=%d after_copy=%s",
		t.ID, actionName, entry.Key, entry.Size, t.AfterCopy)
	r.OnFileDone(entry.Key, 0, nil)
	return nil
}

// nonZeroMtimePtr returns &t when t is non-zero, else nil. Encodes the
// "preserve src mtime only if we know it" rule cleanly for callers that
// build PutOptions.
func nonZeroMtimePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// filterXattrs keeps only POSIX user.* xattrs and the system.posix_acl_*
// pair, dropping security.* / trusted.* / other system.* names. The
// rationale:
//   - user.*           — application metadata, always portable.
//   - system.posix_acl_access / system.posix_acl_default — POSIX ACLs.
//   - security.*       — SELinux/SMACK labels; rarely portable across
//                        hosts and a copy can break security policy.
//   - trusted.*        — root-only; copying these silently changes
//                        privileged metadata.
//   - other system.*   — kernel-managed, not user-settable.
//
// Returns a fresh map; nil/empty input returns nil so PutOptions.Xattrs
// stays at its zero value when nothing was kept.
func filterXattrs(in map[string][]byte) map[string][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for name, val := range in {
		if strings.HasPrefix(name, "user.") {
			out[name] = val
			continue
		}
		if name == "system.posix_acl_access" || name == "system.posix_acl_default" {
			out[name] = val
			continue
		}
		// security.*, trusted.*, other system.* — drop silently.
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// metadataExtras carries the POSIX metadata pulled from src.Stat() that the
// per-attempt transferOnce should thread into PutOptions. Separated out so
// the retry loop can mutate it (the warn policy on ErrMetadataTooLarge
// strips fields and retries).
type metadataExtras struct {
	Mode   *uint32
	UID    *uint32
	GID    *uint32
	Xattrs map[string][]byte
}

// hasAny reports whether any preserve* field is set. Used to short-circuit
// the metadataExtras merge into PutOptions when nothing was requested.
func (m metadataExtras) hasAny() bool {
	return m.Mode != nil || m.UID != nil || m.GID != nil || len(m.Xattrs) > 0
}

// buildMetadataExtras resolves the source's POSIX metadata (one src.Stat
// call) and returns the subset the dst backend can honour. Capability
// mismatches (e.g. dst.Caps.NativeXattrWrite=false but PreserveXattr=true)
// trigger the OnMetadataUnsupported policy via applyMetadataPolicy.
//
// Returns:
//   - extras: the PutOptions metadata to thread through transferOnce / SSC.
//   - skip:   true when policy=skip fired during capability check; caller
//             should account FilesSkipped and return nil.
//   - err:    non-nil when policy=error fired or src.Stat returned a real
//             error (other than missing-Stater, which is benign).
//
// When the source backend does not implement Stater, returns a zero
// extras + nil error — this is "src can't supply the metadata", a property
// of the source, not a policy violation.
func buildMetadataExtras(ctx context.Context, t *Task, entry backend.Entry) (metadataExtras, bool, error) {
	if !t.PreserveMode && !t.PreserveOwner && !t.PreserveXattr {
		return metadataExtras{}, false, nil
	}
	stater, ok := t.Src.(backend.Stater)
	if !ok {
		// Source can't supply POSIX bits — silently proceed without.
		// Not a policy violation; the source is the limitation.
		log.LogDebugf("syncnode: task=%s src does not implement Stater for %q; metadata preservation skipped",
			t.ID, entry.Key)
		return metadataExtras{}, false, nil
	}
	st, serr := stater.Stat(ctx, entry.Key)
	if serr != nil {
		// Couldn't read src metadata. Treat as transient — log warn and
		// proceed without (don't propagate; the file copy itself should
		// still succeed).
		log.LogWarnf("syncnode: task=%s stat src %q for metadata: %v", t.ID, entry.Key, serr)
		return metadataExtras{}, false, nil
	}

	caps := t.Dst.Capabilities()
	var extras metadataExtras
	// mode
	if t.PreserveMode && st.Mode != nil {
		if caps.NativeModeWrite {
			extras.Mode = st.Mode
		} else {
			if skip, err := applyMetadataPolicy(t, entry, "mode"); err != nil {
				return metadataExtras{}, false, err
			} else if skip {
				return metadataExtras{}, true, nil
			}
		}
	}
	// owner (uid+gid travel together; PreserveOwner is single switch but
	// the backends accept them on separate fields)
	if t.PreserveOwner && (st.UID != nil || st.GID != nil) {
		if caps.NativeOwnerWrite {
			extras.UID = st.UID
			extras.GID = st.GID
		} else {
			if skip, err := applyMetadataPolicy(t, entry, "owner"); err != nil {
				return metadataExtras{}, false, err
			} else if skip {
				return metadataExtras{}, true, nil
			}
		}
	}
	// xattrs (namespace-filtered before deciding capability)
	if t.PreserveXattr {
		kept := filterXattrs(st.Xattrs)
		if len(kept) > 0 {
			if caps.NativeXattrWrite {
				extras.Xattrs = kept
			} else {
				if skip, err := applyMetadataPolicy(t, entry, "xattrs"); err != nil {
					return metadataExtras{}, false, err
				} else if skip {
					return metadataExtras{}, true, nil
				}
			}
		}
	}
	return extras, false, nil
}

// applyMetadataPolicy dispatches on t.OnMetadataUnsupported when the dst
// cannot honour the requested metadata field (or returned
// ErrMetadataTooLarge / ErrBackendUnsupported at Put time).
//
// Returns:
//   - skip=true when the policy is "skip" — caller treats the file as skipped.
//   - err  non-nil when the policy is "error" — caller propagates upward.
//   - skip=false, err=nil when the policy is "warn" — caller logs + proceeds
//     without that metadata field.
//
// validateTask normalises empty → "warn" so dispatch is closed-set; default
// branch defends against future drift and treats unknown values as "warn".
func applyMetadataPolicy(t *Task, entry backend.Entry, field string) (bool, error) {
	switch t.OnMetadataUnsupported {
	case OnMetadataUnsupportedSkip:
		metadataUnsupportedSkip.Add(1)
		log.LogWarnf("syncnode: task=%s key=%s metadata field %q unsupported by dst (policy=skip)",
			t.ID, entry.Key, field)
		return true, nil
	case OnMetadataUnsupportedError:
		metadataUnsupportedError.Add(1)
		return false, fmt.Errorf("metadata %q unsupported by dst for %q: policy=error", field, entry.Key)
	default:
		// warn (default) and any unknown future value
		metadataUnsupportedWarn.Add(1)
		log.LogWarnf("syncnode: task=%s key=%s metadata field %q unsupported by dst (policy=warn): proceeding without",
			t.ID, entry.Key, field)
		return false, nil
	}
}

// applyPutOptionsExtras merges the metadata bits from extras into base.
// Caller-supplied base fields (Mtime / ComputeChecksum / StorageClass / …)
// are preserved untouched.
func applyPutOptionsExtras(base backend.PutOptions, extras metadataExtras) backend.PutOptions {
	if extras.Mode != nil {
		base.Mode = extras.Mode
	}
	if extras.UID != nil {
		base.UID = extras.UID
	}
	if extras.GID != nil {
		base.GID = extras.GID
	}
	if len(extras.Xattrs) > 0 {
		base.Xattrs = extras.Xattrs
	}
	return base
}

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
	extras metadataExtras,
) (transferResult, error) {
	pr, pw := io.Pipe()

	putOpts := backend.PutOptions{
		ComputeChecksum: computeChecksum,
	}
	// Preserve source mtime on the destination. Only set the pointer when
	// the listing actually produced an mtime — backends that don't expose
	// mtime via List would otherwise have us write a zero timestamp.
	if !entry.Mtime.IsZero() {
		mt := entry.Mtime
		putOpts.Mtime = &mt
	}
	// Merge POSIX metadata (mode/uid/gid/xattrs) when the rule requested
	// preservation AND the dst backend advertises native support. extras
	// is empty when neither side is in play; the merge is a no-op then.
	if extras.hasAny() {
		putOpts = applyPutOptionsExtras(putOpts, extras)
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

	// 1.5) Resolve POSIX metadata extras (mode/uid/gid/xattrs) once, before
	// we enter the SSC fast path / retry loop. extras is a per-file constant
	// (the src metadata doesn't change between transferOnce attempts) so the
	// Stat call is amortised across retries.
	//
	// buildMetadataExtras may decide the file is skipped by policy (capability
	// mismatch + OnMetadataUnsupported=skip) or fail it outright (policy=error)
	// — handle both before doing any other work.
	metaExtras, metaSkip, metaErr := buildMetadataExtras(ctx, t, entry)
	if metaErr != nil {
		return metaErr
	}
	if metaSkip {
		atomic.AddInt64(&p.FilesSkipped, 1)
		atomic.AddInt64(&p.BytesSkipped, entry.Size)
		if p.Sampler != nil {
			p.Sampler.add(entry.Key)
		}
		r.OnFileDone(entry.Key, 0, nil)
		return nil
	}

	// 2) Idempotent skip: dispatch on Task.OnExisting (rclone-style overwrite
	// strategy). validateTask normalises empty → verify_then_skip and rejects
	// unknown values, so dispatch is closed-set:
	//   - verify_then_skip  → legacy size + checksum/ETag comparison
	//                         (rclone default)
	//   - always_skip       → rclone --ignore-existing (any dst wins)
	//   - newer_only        → rclone --update (dst.Mtime ≥ src.Mtime − 1s)
	//   - overwrite         → rclone --ignore-times (never skip on this branch)
	//
	// Mtime is plumbed alongside size/etag because newer_only needs it; the
	// other strategies ignore it. dst.Head failure that isn't ErrKeyNotFound
	// is still propagated so callers see real backend trouble (unchanged
	// from the legacy path).
	if dstSize, dstETag, dstMtime, herr := t.Dst.Head(ctx, dstKey); herr == nil {
		if shouldSkipExistingDstByStrategy(ctx, t, entry, dstKey, dstSize, dstETag, dstMtime) {
			// Dry-run short-circuit: account the would_skip_existing outcome
			// (and would_delete_src when move-mode) without mutating Src.
			if t.DryRun {
				return accountDryRun(t, entry, dryRunActionSkip, r, p)
			}
			// rclone-move 语义：dst 已与 src 对齐时仍须删除 src。验证强度由
			// shouldSkipExistingDst 保证——validateTask 已锁定
			// verify_then_delete_src→strong，因此本分支命中时 src/dst
			// strong checksum 已比对一致，删除安全。
			if t.AfterCopy == AfterCopyVerifyThenDeleteSrc {
				if derr := t.Src.Delete(ctx, entry.Key); derr != nil {
					return fmt.Errorf("delete src %q after skip-verify: %w", entry.Key, derr)
				}
			}
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

	// 2.5) Server-side copy fast path (rclone-gap-roadmap §4). When src and
	// dst point at the same storage realm AND the backend advertises
	// ServerSideCopy, skip the Get→Put round trip by asking the backend to
	// move bytes internally. This avoids egress and is dramatically faster
	// on multi-GiB objects.
	//
	// Strong-mode (P0) and OnSourceMutated (P1) are NOT compatible with the
	// fast path: both require either a streaming sha256 we cannot compute
	// server-side, or a Pre/Post-Head snapshot pair that races the copy.
	// Fall through to the legacy Get/Put pipeline in those cases.
	if t.ChecksumMode != "strong" && t.OnSourceMutated == "" &&
		t.Src.SameInstance(t.Dst) && t.Src.Capabilities().ServerSideCopy {
		if _, ok := t.Src.(backend.ServerSideCopier); ok && t.DryRun {
			// Dry-run short-circuit: we'd take the SSC fast path, but
			// never call into the backend. Account would_server_side_copy
			// (and would_delete_src when move-mode) and return.
			return accountDryRun(t, entry, dryRunActionServerSideCopy, r, p)
		}
		if copier, ok := t.Src.(backend.ServerSideCopier); ok {
			sscOpts := backend.PutOptions{
				Mtime: nonZeroMtimePtr(entry.Mtime),
			}
			if metaExtras.hasAny() {
				sscOpts = applyPutOptionsExtras(sscOpts, metaExtras)
			}
			pr, cerr := copier.ServerSideCopy(ctx, entry.Key, dstKey, sscOpts)
			if cerr == nil {
				serverSideCopyOK.Add(1)
				if t.AfterCopy == AfterCopyVerifyThenDeleteSrc {
					if derr := t.Src.Delete(ctx, entry.Key); derr != nil {
						return fmt.Errorf("delete src %q after server-side copy: %w", entry.Key, derr)
					}
				}
				bytesAck := pr.BytesPut
				if bytesAck == 0 {
					bytesAck = entry.Size
				}
				atomic.AddInt64(&p.FilesDone, 1)
				atomic.AddInt64(&p.BytesDone, bytesAck)
				if p.Sampler != nil {
					p.Sampler.add(entry.Key)
				}
				r.OnFileDone(entry.Key, bytesAck, nil)
				return nil
			}
			if errors.Is(cerr, backend.ErrBackendUnsupported) {
				serverSideCopyFallback.Add(1)
				log.LogDebugf("syncnode: server-side copy unsupported for %s, falling back to Get/Put", entry.Key)
				// Fall through to legacy pipeline.
			} else {
				serverSideCopyErr.Add(1)
				log.LogWarnf("syncnode: server-side copy failed for %s: %v, falling back to Get/Put", entry.Key, cerr)
				// Don't return the error — fall back to the proven streaming
				// path. Returning here would surface transient backend issues
				// (e.g. bucket policy denying CopyObject) as hard task failures
				// when the slow path would have worked.
			}
		}
	}

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

	// Dry-run short-circuit: we'd transfer bytes here, but DryRun forbids
	// any mutation. Account would_copy (and would_delete_src for move
	// semantics) and return before the retry loop touches transferOnce.
	if t.DryRun {
		return accountDryRun(t, entry, dryRunActionCopy, r, p)
	}

	// 4) Per-file retry loop (P2). attempt=0 is the first try; subsequent
	// rounds backoff before transferOnce. lastErr is the most recent
	// failure we've observed; on exhaustion it's what the caller sees.
	var (
		lastErr           error
		bytesAck          int64
		metadataStripped  bool
	)
	strongMode := t.ChecksumMode == "strong"
	for attempt := 0; attempt <= t.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			backoffSleep(ctx, attempt)
		}

		result, terr := e.transferOnce(ctx, t, entry, dstKey, resumeOffset, resumeUploadID, strongMode, metaExtras)
		if terr != nil {
			// ErrMetadataTooLarge: dst's user-metadata budget is exhausted
			// (s3: 2 KiB cap). Dispatch on OnMetadataUnsupported:
			//   warn   → strip extras and retry without consuming an attempt
			//   skip   → FilesSkipped++ and return nil
			//   error  → propagate the failure
			//
			// metadataStripped guards against re-entering the warn branch
			// after we've already cleared extras — defends against backends
			// that return ErrMetadataTooLarge for non-metadata reasons.
			if errors.Is(terr, backend.ErrMetadataTooLarge) && metaExtras.hasAny() && !metadataStripped {
				switch t.OnMetadataUnsupported {
				case OnMetadataUnsupportedSkip:
					metadataUnsupportedSkip.Add(1)
					log.LogWarnf("syncnode: task=%s key=%s ErrMetadataTooLarge (policy=skip)", t.ID, entry.Key)
					atomic.AddInt64(&p.FilesSkipped, 1)
					atomic.AddInt64(&p.BytesSkipped, entry.Size)
					if p.Sampler != nil {
						p.Sampler.add(entry.Key)
					}
					r.OnFileDone(entry.Key, 0, nil)
					return nil
				case OnMetadataUnsupportedError:
					metadataUnsupportedError.Add(1)
					return fmt.Errorf("metadata too large for %q (policy=error): %w", entry.Key, terr)
				default:
					// warn (default): strip and retry without burning an attempt.
					metadataUnsupportedWarn.Add(1)
					log.LogWarnf("syncnode: task=%s key=%s ErrMetadataTooLarge (policy=warn): retrying without POSIX metadata", t.ID, entry.Key)
					metaExtras = metadataExtras{}
					metadataStripped = true
					attempt--
					continue
				}
			}
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

// newerOnlyMtimeTolerance is the cross-backend clock-skew tolerance used by
// the OnExisting=newer_only strategy: we treat dst as "newer or equal" when
// dst.Mtime + 1s ≥ src.Mtime. S3 reports mtime at second precision while
// local/CFS report at nanosecond precision; without slack the rule would
// always re-upload after a S3 → local round-trip even when nothing changed.
const newerOnlyMtimeTolerance = time.Second

// shouldSkipExistingDstByStrategy dispatches on Task.OnExisting (set by
// validateTask) and falls through to shouldSkipExistingDst for the legacy
// verify_then_skip path. Splitting the dispatch from the verify logic keeps
// the size+checksum/ETag code path (the most heavily tested branch)
// bit-for-bit identical to the pre-OnExisting behaviour.
//
// dstMtime is only consumed by newer_only; the other branches ignore it but
// the signature is uniform so callers don't have to thread an Option-style
// type.
func shouldSkipExistingDstByStrategy(
	ctx context.Context,
	t *Task,
	entry backend.Entry,
	dstKey string,
	dstSize int64,
	dstETag string,
	dstMtime time.Time,
) bool {
	switch t.OnExisting {
	case OnExistingAlwaysSkip:
		// rclone --ignore-existing: any dst at this key wins. We already
		// proved dst exists (caller only entered this branch on a
		// successful Head), so unconditional skip is correct.
		return true
	case OnExistingOverwrite:
		// rclone --ignore-times: never skip. The full transfer path runs
		// and replaces whatever dst held.
		return false
	case OnExistingNewerOnly:
		// rclone --update: skip when dst is at least as recent as src.
		// Missing mtimes (either side reports zero) force a re-upload —
		// fail-safe: without timestamp evidence we cannot prove dst is
		// fresher.
		if entry.Mtime.IsZero() || dstMtime.IsZero() {
			return false
		}
		return !entry.Mtime.After(dstMtime.Add(newerOnlyMtimeTolerance))
	case OnExistingVerifyThenSkip, "":
		// Legacy default (back-compat alias on empty). validateTask
		// normalises the empty string up front but we tolerate it here
		// defensively in case a caller bypasses validateTask.
		return shouldSkipExistingDst(ctx, t, entry, dstKey, dstSize, dstETag)
	default:
		// Unknown strategy reaching the dispatcher is a programmer bug —
		// validateTask should have rejected it. Fail-closed: never skip,
		// so the worst case is an unnecessary re-upload, never silent
		// data loss.
		return false
	}
}

// shouldSkipExistingDst implements the «is dst already a verified match?»
// decision used by the verify_then_skip path of shouldSkipExistingDstByStrategy.
// The data-integrity SDD says size-only skip is unsafe: same-size mutations
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
