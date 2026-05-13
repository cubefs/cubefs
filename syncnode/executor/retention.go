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
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// Retention selects which entries should remain on the destination after
// a successful sync. Two policies are supported (combined with AND):
//
//   - keepLast: keep at most N entries matching pattern (sorted by the
//     version number captured by {N})
//   - keepWithin: keep entries whose mtime is within `KeepWithin` of now
//
// SelectToDelete returns the entries that violate the policy; the executor
// is responsible for actually calling backend.Delete on each.
//
// Pattern grammar: literal text with a single `{N}` placeholder that
// captures a non-negative integer version number. Example:
//   "model-step-{N}.pt" matches "model-step-1.pt", "model-step-12000.pt"
type Retention struct {
	Pattern    string // empty = retention disabled
	KeepLast   int    // 0 = no count-based pruning
	KeepWithin time.Duration // 0 = no age-based pruning
}

// SelectToDelete returns the entries that should be deleted to satisfy the
// retention policy. Input entries may be in any order; the function sorts
// internally by version number (highest first = most recent) for keepLast,
// and by Mtime for keepWithin.
//
// Entries whose Key doesn't match Pattern are returned unchanged (not in
// the delete list) — retention only applies to entries matching the
// versioned pattern.
//
// Returns an empty slice (not nil) when nothing should be deleted.
func (r *Retention) SelectToDelete(entries []backend.Entry, now time.Time) []backend.Entry {
	if r.Pattern == "" {
		return []backend.Entry{}
	}
	re, err := compileRetentionPattern(r.Pattern)
	if err != nil {
		return []backend.Entry{}
	}

	type matched struct {
		entry   backend.Entry
		version int64
		matched bool
	}
	all := make([]matched, 0, len(entries))
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		base := lastSegment(e.Key)
		m := re.FindStringSubmatch(base)
		if m == nil {
			continue
		}
		v, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			continue
		}
		all = append(all, matched{entry: e, version: v, matched: true})
	}
	if len(all) == 0 {
		return []backend.Entry{}
	}

	// Sort descending by version: highest version is "newest".
	sort.Slice(all, func(i, j int) bool {
		return all[i].version > all[j].version
	})

	keepLastActive := r.KeepLast > 0
	keepWithinActive := r.KeepWithin > 0
	if !keepLastActive && !keepWithinActive {
		// Neither policy active → don't delete anything.
		return []backend.Entry{}
	}

	keep := make(map[string]bool, len(all))

	// keepLast: keep the top N by version.
	if keepLastActive {
		limit := r.KeepLast
		if limit > len(all) {
			limit = len(all)
		}
		for i := 0; i < limit; i++ {
			keep[all[i].entry.Key] = true
		}
	}

	// keepWithin: keep entries newer than (now - KeepWithin).
	if keepWithinActive {
		cutoff := now.Add(-r.KeepWithin)
		for _, m := range all {
			if !m.entry.Mtime.IsZero() && m.entry.Mtime.After(cutoff) {
				keep[m.entry.Key] = true
			}
		}
	}

	out := make([]backend.Entry, 0, len(all))
	for _, m := range all {
		if !keep[m.entry.Key] {
			out = append(out, m.entry)
		}
	}
	return out
}

// compileRetentionPattern turns "model-step-{N}.pt" into a regexp that
// captures the integer version. Pattern characters other than {N} are
// escaped, so things like "." don't accidentally match anything.
func compileRetentionPattern(p string) (*regexp.Regexp, error) {
	idx := strings.Index(p, "{N}")
	if idx < 0 {
		return nil, fmt.Errorf("retention pattern %q missing {N} placeholder", p)
	}
	prefix := regexp.QuoteMeta(p[:idx])
	suffix := regexp.QuoteMeta(p[idx+len("{N}"):])
	return regexp.Compile("^" + prefix + `(\d+)` + suffix + "$")
}

// lastSegment returns the basename of a slash-separated key. Object keys
// commonly look like "runs/exp-42/model-step-1000.pt" and we want to apply
// the pattern to "model-step-1000.pt".
func lastSegment(k string) string {
	if i := strings.LastIndex(k, "/"); i >= 0 {
		return k[i+1:]
	}
	return k
}
