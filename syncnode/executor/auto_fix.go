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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// runAutoFix repairs the fixable subset of a Check task's mismatches by
// re-syncing each affected key from t.Src to t.Dst. Called from Executor.Run
// when t.Type == TaskTypeCheck and t.OnMismatch == OnMismatchAutoFix.
//
// "Fixable" means the source is authoritative for the discrepancy — i.e. the
// reason names a divergence on the dst side that a src→dst sync would correct
// (see isAutoFixable). MismatchMissingSrc is NOT fixable: deleting dst data
// is destructive and outside auto_fix's contract.
//
// Returns the first error encountered, or nil if every fixable mismatch
// repaired cleanly. Non-fixable mismatches are silently passed through (they
// remain in the returned Result.Mismatches so operators see them).
//
// The repair loop is sequential by design: mismatch lists are typically small
// (post-sampling) and serializing repairs keeps progress accounting simple.
// If a future workload needs parallel repairs we can wrap this in a worker
// pool the same way runSync does.
func (e *Executor) runAutoFix(
	ctx context.Context,
	t *Task,
	mismatches []Mismatch,
	r Reporter,
	p *Progress,
) error {
	if r == nil {
		r = NoopReporter{}
	}
	var (
		firstErr  error
		firstOnce sync.Once
	)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		firstOnce.Do(func() { firstErr = err })
	}

	for _, m := range mismatches {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !isAutoFixable(m.Reason) {
			continue
		}
		srcKey := joinKey(t.SrcPath, m.Key)
		entry := backend.Entry{
			Key:  srcKey,
			Size: m.SrcSize,
			ETag: m.SrcETag,
		}
		if err := e.syncOneFile(ctx, t, entry, r, p); err != nil {
			atomic.AddInt64(&p.FilesFailed, 1)
			recordErr(fmt.Errorf("auto_fix %q: %w", m.Key, err))
			// Keep going — operators want every fixable mismatch attempted,
			// not just up to the first failure. The first error is what's
			// reported on the Result.
		}
	}
	return firstErr
}

// isAutoFixable reports whether a Mismatch reason can be repaired by re-running
// the src→dst transfer. Auto-fix never deletes dst-side data (MissingSrc),
// since that's destructive and warrants explicit operator action.
func isAutoFixable(reason MismatchReason) bool {
	switch reason {
	case MismatchMissingDst, MismatchSizeDiffer, MismatchETagDiffer, MismatchMtimeNewer:
		return true
	}
	return false
}

// joinKey concatenates a prefix path and a relative key with a single "/"
// between them, trimming redundant slashes on either side. Empty prefix
// returns the relative key as-is; empty rel returns the prefix as-is.
//
// This is the inverse of relativeKey() in check_task.go — relativeKey strips
// the prefix to produce the rel key during the Check scan; joinKey
// reattaches it so we can hand a fully-qualified Backend key to Get/Head.
func joinKey(prefix, rel string) string {
	p := strings.TrimRight(prefix, "/")
	r := strings.TrimLeft(rel, "/")
	switch {
	case p == "":
		return r
	case r == "":
		return p
	default:
		return p + "/" + r
	}
}
