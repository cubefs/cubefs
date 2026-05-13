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
	"hash/fnv"
	"math/rand"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// mtimeTolerance is the slack we allow before flagging a src file as newer
// than its dst counterpart. POSIX mtime resolution + clock skew across
// backends easily eats the first few seconds; anything below this is noise.
const mtimeTolerance = 2 * time.Second

// runCheck implements the Check task — see design.md §8.3.
//
// Flow:
//  1. List src + dst recursively under their respective subpaths.
//  2. Walk the src listing; for each src entry that passes Filter.Match:
//     - compute matching dst key via rebase
//     - record MismatchMissingDst / MismatchSizeDiffer / MismatchETagDiffer
//     / MismatchMtimeNewer as appropriate, with size/etag dominating over
//     mtime.
//  3. Walk leftover dst entries (those not paired with any src) and emit
//     MismatchMissingSrc — but only when no filter is configured (with a
//     filter, "extras" on dst are expected).
//  4. Apply SampleStrategy to the collected mismatches:
//     "full"/"" → all, "random" → fraction (deterministic by TaskID),
//     "oldest" → smallest-mtime first, "largest" → biggest-size first.
//  5. Apply OnMismatch policy:
//     "alert"/"" → return as-is, "auto_fix" → return as-is (executor
//     wrapper schedules the sub-task in D-5), "ignore" → empty slice but
//     FilesFailed still records the count.
func (e *Executor) runCheck(ctx context.Context, t *Task, r Reporter, p *Progress) ([]Mismatch, error) {
	if r == nil {
		r = NoopReporter{}
	}
	now := time.Now()

	// --- Step 1: list both sides ---
	srcEntries, err := listAll(ctx, t.Src, t.SrcPath)
	if err != nil {
		return nil, fmt.Errorf("list src %q: %w", t.SrcPath, err)
	}
	dstEntries, err := listAll(ctx, t.Dst, t.DstPath)
	if err != nil {
		return nil, fmt.Errorf("list dst %q: %w", t.DstPath, err)
	}

	// Index dst by relative path so lookups are O(1) and we can mark
	// what's been seen.
	dstByRel := make(map[string]backend.Entry, len(dstEntries))
	for _, d := range dstEntries {
		if d.IsDir {
			continue
		}
		rel := relativeKey(d.Key, t.DstPath)
		dstByRel[rel] = d
	}

	// --- Step 2: walk src, pair and compare ---
	hasFilter := filterIsSet(&t.Filter)
	sameKind := t.Src.Kind() == t.Dst.Kind()
	checkETags := t.SampleStrategy == "full" && sameKind

	mismatches := make([]Mismatch, 0)
	seenRels := make(map[string]struct{}, len(srcEntries))

	for _, s := range srcEntries {
		// Honour ctx between entries — long lists shouldn't trap.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.IsDir {
			continue
		}
		atomic.AddInt64(&p.FilesTotal, 1)

		// Filter applies to src entries only.
		if !t.Filter.Match(s, now) {
			atomic.AddInt64(&p.FilesSkipped, 1)
			continue
		}

		rel := relativeKey(s.Key, t.SrcPath)
		seenRels[rel] = struct{}{}

		d, ok := dstByRel[rel]
		if !ok {
			mismatches = append(mismatches, Mismatch{
				Key:     rel,
				Reason:  MismatchMissingDst,
				SrcSize: s.Size,
				SrcETag: s.ETag,
			})
			atomic.AddInt64(&p.FilesDone, 1)
			continue
		}

		// We have both sides. Determine reason in priority order:
		// size > etag > mtime. Only emit the highest-priority diff.
		switch {
		case s.Size != d.Size:
			mismatches = append(mismatches, Mismatch{
				Key:     rel,
				Reason:  MismatchSizeDiffer,
				SrcSize: s.Size,
				DstSize: d.Size,
				SrcETag: s.ETag,
				DstETag: d.ETag,
			})
		case checkETags && s.ETag != "" && d.ETag != "" && s.ETag != d.ETag:
			mismatches = append(mismatches, Mismatch{
				Key:     rel,
				Reason:  MismatchETagDiffer,
				SrcSize: s.Size,
				DstSize: d.Size,
				SrcETag: s.ETag,
				DstETag: d.ETag,
			})
		case !s.Mtime.IsZero() && !d.Mtime.IsZero() &&
			s.Mtime.After(d.Mtime.Add(mtimeTolerance)):
			// Only emit MismatchMtimeNewer when nothing else is wrong.
			mismatches = append(mismatches, Mismatch{
				Key:     rel,
				Reason:  MismatchMtimeNewer,
				SrcSize: s.Size,
				DstSize: d.Size,
				SrcETag: s.ETag,
				DstETag: d.ETag,
			})
		}
		atomic.AddInt64(&p.FilesDone, 1)
	}

	// --- Step 3: leftover dst entries ---
	// "Extras on dst" are only flagged when there's no filter; with a
	// filter, the user has already opted into ignoring non-matching files.
	if !hasFilter {
		// Deterministic ordering for stable test output.
		leftoverRels := make([]string, 0)
		for rel := range dstByRel {
			if _, seen := seenRels[rel]; seen {
				continue
			}
			leftoverRels = append(leftoverRels, rel)
		}
		sort.Strings(leftoverRels)
		for _, rel := range leftoverRels {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			d := dstByRel[rel]
			mismatches = append(mismatches, Mismatch{
				Key:     rel,
				Reason:  MismatchMissingSrc,
				DstSize: d.Size,
				DstETag: d.ETag,
			})
		}
	}

	// --- Step 4: sample strategy ---
	mismatches = applySampleStrategy(mismatches, dstByRel, t)

	// Record found count BEFORE the ignore filter so operators still see
	// it in metrics.
	atomic.AddInt64(&p.FilesFailed, int64(len(mismatches)))

	// --- Step 5: onMismatch policy ---
	switch t.OnMismatch {
	case OnMismatchIgnore:
		return []Mismatch{}, nil
	case OnMismatchAlert, OnMismatchAutoFix, "":
		// "alert" / "" → just return; "auto_fix" → executor wrapper
		// schedules the sub-task in D-5.
		return mismatches, nil
	default:
		// Unknown policies behave like alert.
		return mismatches, nil
	}
}

// listAll drains a backend.List channel and returns the collected entries.
// Surfaces the first listing-side error (Entry.Err) it sees.
func listAll(ctx context.Context, b backend.Backend, prefix string) ([]backend.Entry, error) {
	ch, err := b.List(ctx, prefix, true)
	if err != nil {
		return nil, err
	}
	out := make([]backend.Entry, 0, 64)
	for e := range ch {
		if e.Err != nil {
			// Drain the channel to let the producer goroutine exit.
			go func() {
				for range ch {
				}
			}()
			return nil, e.Err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// relativeKey strips a leading "prefix" segment (with optional trailing
// slash) from key, returning the relative path used for src/dst pairing.
// If key doesn't start with prefix the key is returned unchanged — that
// can happen for object stores where List may return keys without the
// prefix attached, depending on the implementation.
func relativeKey(key, prefix string) string {
	p := strings.TrimRight(prefix, "/")
	if p == "" {
		return strings.TrimLeft(key, "/")
	}
	if strings.HasPrefix(key, p+"/") {
		return key[len(p)+1:]
	}
	if key == p {
		return ""
	}
	return key
}

// filterIsSet reports whether the task carries any non-default filter
// constraint. An entirely zero Filter means "match everything".
func filterIsSet(f *Filter) bool {
	if f == nil {
		return false
	}
	return len(f.Include) > 0 || len(f.Exclude) > 0 ||
		f.MinSize > 0 || f.MaxSize > 0 || f.MinAge > 0 || f.MaxAge > 0
}

// applySampleStrategy reduces a full mismatch list according to
// t.SampleStrategy + t.SampleRate. The sample size is floor(N * rate).
// Sampling happens AFTER mismatch detection so the same scan yields the
// same set; the strategy only changes which entries are returned.
func applySampleStrategy(all []Mismatch, dstByRel map[string]backend.Entry, t *Task) []Mismatch {
	n := len(all)
	if n == 0 {
		return all
	}
	strategy := t.SampleStrategy
	if strategy == "" || strategy == "full" {
		return all
	}
	rate := t.SampleRate
	if rate <= 0 {
		return []Mismatch{}
	}
	if rate >= 1.0 {
		return all
	}
	target := int(float64(n) * rate)
	if target <= 0 {
		return []Mismatch{}
	}
	if target >= n {
		return all
	}

	switch strategy {
	case "random":
		// Deterministic seed so identical inputs produce identical
		// samples — required for tests, and harmless in production
		// (operators get reproducible reports across re-runs).
		seed := taskSeed(t.ID)
		rng := rand.New(rand.NewSource(seed))
		idx := rng.Perm(n)[:target]
		out := make([]Mismatch, target)
		for i, j := range idx {
			out[i] = all[j]
		}
		return out
	case "oldest":
		// Sort by src mtime ascending. mtime lookup uses the seen dst
		// map first (cheaper) then falls back to zero time, which
		// sorts to the front and is harmless.
		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		mtimeOf := func(m Mismatch) time.Time {
			if d, ok := dstByRel[m.Key]; ok {
				return d.Mtime
			}
			return time.Time{}
		}
		sort.SliceStable(idx, func(i, j int) bool {
			return mtimeOf(all[idx[i]]).Before(mtimeOf(all[idx[j]]))
		})
		out := make([]Mismatch, target)
		for i := 0; i < target; i++ {
			out[i] = all[idx[i]]
		}
		return out
	case "largest":
		// Sort by src size descending; fall back to dst size when src
		// is missing (MismatchMissingSrc only carries DstSize).
		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		sizeOf := func(m Mismatch) int64 {
			if m.SrcSize > 0 {
				return m.SrcSize
			}
			return m.DstSize
		}
		sort.SliceStable(idx, func(i, j int) bool {
			return sizeOf(all[idx[i]]) > sizeOf(all[idx[j]])
		})
		out := make([]Mismatch, target)
		for i := 0; i < target; i++ {
			out[i] = all[idx[i]]
		}
		return out
	default:
		// Unknown strategy → behave like "full".
		return all
	}
}

// taskSeed derives a deterministic int64 seed from the Task.ID so random
// sampling reproduces across runs of the same task.
func taskSeed(id string) int64 {
	if id == "" {
		return 1
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	// fnv64a is uint64; mask high bit for a positive seed.
	return int64(h.Sum64() & 0x7fffffffffffffff)
}
