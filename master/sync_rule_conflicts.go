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

package master

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cubefs/cubefs/proto"
)

// Phase 3 — sync rule conflict validator.
//
// Ported from syncnode/rules/conflicts.go (which becomes redundant once
// Phase 6 deletes the syncnode-local rule store). Pure function over a
// rule slice; admin handlers (create / update) call this against the
// in-memory cache contents augmented with the candidate rule.
//
// Codes intentionally match the syncnode-side numbering so error
// envelopes stay stable across the cutover.

// SyncRule conflict error codes — extend the existing syncnode/errors.go
// namespace (1001-1013) without modifying that file. Stable across
// releases; callers may assert against the numeric value.
const (
	// SyncRuleErrDuplicate — two rules have identical (src, dst)
	// endpoint+path tuples. Indicates copy-paste duplication.
	SyncRuleErrDuplicate = 1014

	// SyncRuleErrPrefixOverlap — two rules share the same backend pair AND
	// one rule's src.path is a path-prefix of the other's (likewise for
	// dst). Causes overlapping data-movement domains, retention races,
	// and double-writes.
	SyncRuleErrPrefixOverlap = 1015

	// SyncRuleErrCycle — A: X → Y and B: Y → X with identical endpoints.
	// Data ping-pongs between the two backends indefinitely.
	SyncRuleErrCycle = 1016
)

// SyncRuleConflictError is the typed error returned by ValidateSyncRules.
// Handlers translate this into HTTP 409 + the wire envelope. Tests assert
// via errors.As.
type SyncRuleConflictError struct {
	Code    int
	Msg     string
	RuleIDs []string // exactly 2 ids — the pair that conflicts
}

// Error satisfies the error interface.
func (e *SyncRuleConflictError) Error() string {
	return fmt.Sprintf("sync rule conflict (code=%d rules=%s): %s",
		e.Code, strings.Join(e.RuleIDs, ","), e.Msg)
}

// ValidateSyncRules runs all conflict checks against the supplied rule
// list. Returns the first conflict found, or nil. Check order:
//
//  1. duplicate (same src+dst quad-tuple)
//  2. prefix overlap (same backend pair, src or dst path prefix relation)
//  3. cycle sync (A.src == B.dst AND A.dst == B.src)
//
// Pure function: does not mutate the input slice nor the rules.
func ValidateSyncRules(rules []*proto.SyncRule) error {
	if len(rules) < 2 {
		return nil
	}
	if err := checkSyncDuplicate(rules); err != nil {
		return err
	}
	if err := checkSyncPrefixOverlap(rules); err != nil {
		return err
	}
	if err := checkSyncCycle(rules); err != nil {
		return err
	}
	return nil
}

// syncEndpointKey returns a canonical string identifying the storage
// "anchor" (kind + storage identity), independent of per-rule path/prefix.
// Two endpoints with the same key live on the same backend instance and
// therefore can collide on path overlap.
//
//   - cfs   → "cfs:<vol>"
//   - s3    → "s3:<endpoint>:<bucket>"
//   - local → "local:" (all local rules share one filesystem; path is
//     the per-rule differentiator returned by syncPathOf)
func syncEndpointKey(ep *proto.SyncEndpointConfig) string {
	if ep == nil {
		return ""
	}
	switch ep.Kind {
	case "cfs":
		return "cfs:" + ep.Vol
	case "s3":
		return "s3:" + ep.Endpoint + ":" + ep.Bucket
	case "local":
		return "local:"
	default:
		return ep.Kind + ":"
	}
}

// syncPathOf returns the per-endpoint path/prefix — the portion of the
// endpoint that differs between rules sharing a backend anchor.
//
//   - cfs   → ep.Path
//   - s3    → ep.Prefix
//   - local → filepath.Clean(ep.Path)
func syncPathOf(ep *proto.SyncEndpointConfig) string {
	if ep == nil {
		return ""
	}
	switch ep.Kind {
	case "cfs":
		return ep.Path
	case "s3":
		return ep.Prefix
	case "local":
		return filepath.Clean(ep.Path)
	default:
		return ""
	}
}

// checkSyncDuplicate reports the first pair of rules whose
// (srcKey, srcPath, dstKey, dstPath) 4-tuples are byte-equal.
// Rules of type "check" are exempt: a check rule shares src/dst with a
// sync rule by design (it reads the same endpoints to audit state) and
// causes no data-movement conflict.
func checkSyncDuplicate(rules []*proto.SyncRule) error {
	type quad struct{ sk, sp, dk, dp string }
	seen := make(map[quad]string, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		if r.Config.Type == "check" {
			continue
		}
		key := quad{
			sk: syncEndpointKey(&r.Config.Src),
			sp: syncPathOf(&r.Config.Src),
			dk: syncEndpointKey(&r.Config.Dst),
			dp: syncPathOf(&r.Config.Dst),
		}
		if prev, ok := seen[key]; ok {
			return &SyncRuleConflictError{
				Code: SyncRuleErrDuplicate,
				Msg: fmt.Sprintf("rules %q and %q have identical src/dst endpoints (src=%s%s dst=%s%s)",
					prev, r.ID(), key.sk, key.sp, key.dk, key.dp),
				RuleIDs: []string{prev, r.ID()},
			}
		}
		seen[key] = r.ID()
	}
	return nil
}

// checkSyncPrefixOverlap reports the first pair of rules sharing the
// same (srcKey, dstKey) backend pair where either src paths or dst paths
// sit in a non-equal prefix relation. Equal paths are handled by
// checkSyncDuplicate; this check only fires when at least one side is a
// strict prefix of the other.
func checkSyncPrefixOverlap(rules []*proto.SyncRule) error {
	for i := 0; i < len(rules); i++ {
		a := rules[i]
		if a == nil {
			continue
		}
		aSrcK := syncEndpointKey(&a.Config.Src)
		aDstK := syncEndpointKey(&a.Config.Dst)
		aSrcP := syncPathOf(&a.Config.Src)
		aDstP := syncPathOf(&a.Config.Dst)
		for j := i + 1; j < len(rules); j++ {
			b := rules[j]
			if b == nil {
				continue
			}
			if syncEndpointKey(&b.Config.Src) != aSrcK || syncEndpointKey(&b.Config.Dst) != aDstK {
				continue
			}
			bSrcP := syncPathOf(&b.Config.Src)
			bDstP := syncPathOf(&b.Config.Dst)
			if strictSyncPrefix(aSrcP, bSrcP) || strictSyncPrefix(bSrcP, aSrcP) {
				return &SyncRuleConflictError{
					Code: SyncRuleErrPrefixOverlap,
					Msg: fmt.Sprintf("rules %q and %q have overlapping src paths on backend %s (%q vs %q)",
						a.ID(), b.ID(), aSrcK, aSrcP, bSrcP),
					RuleIDs: []string{a.ID(), b.ID()},
				}
			}
			if strictSyncPrefix(aDstP, bDstP) || strictSyncPrefix(bDstP, aDstP) {
				return &SyncRuleConflictError{
					Code: SyncRuleErrPrefixOverlap,
					Msg: fmt.Sprintf("rules %q and %q have overlapping dst paths on backend %s (%q vs %q)",
						a.ID(), b.ID(), aDstK, aDstP, bDstP),
					RuleIDs: []string{a.ID(), b.ID()},
				}
			}
		}
	}
	return nil
}

// checkSyncCycle reports the first pair (A, B) where A's src endpoint
// equals B's dst endpoint AND A's dst endpoint equals B's src endpoint —
// data would ping-pong between two backends.
//
// This is the literal "inverted endpoints" case (design.md §9 E-4).
// Future improvement: promote to overlap-cycle detection (prefix-only
// inversion).
func checkSyncCycle(rules []*proto.SyncRule) error {
	type tuple struct{ k, p string }
	type pair struct{ src, dst tuple }
	idx := make(map[pair]string, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		p := pair{
			src: tuple{syncEndpointKey(&r.Config.Src), syncPathOf(&r.Config.Src)},
			dst: tuple{syncEndpointKey(&r.Config.Dst), syncPathOf(&r.Config.Dst)},
		}
		inv := pair{src: p.dst, dst: p.src}
		if prev, ok := idx[inv]; ok && prev != r.ID() {
			return &SyncRuleConflictError{
				Code: SyncRuleErrCycle,
				Msg: fmt.Sprintf("rules %q and %q form a sync cycle (%s%s ↔ %s%s)",
					prev, r.ID(), p.src.k, p.src.p, p.dst.k, p.dst.p),
				RuleIDs: []string{prev, r.ID()},
			}
		}
		if _, ok := idx[p]; !ok {
			idx[p] = r.ID()
		}
	}
	return nil
}

// strictSyncPrefix reports whether p1 is a strict (non-equal) path-prefix
// of p2. Matches filesystem path containment semantics: "/a" is a strict
// prefix of "/a/b" but NOT of "/ab". Empty strings are not strict prefixes
// of anything.
func strictSyncPrefix(p1, p2 string) bool {
	if p1 == "" || p2 == "" {
		return false
	}
	if p1 == p2 {
		return false
	}
	p1 = strings.TrimRight(p1, "/")
	p2 = strings.TrimRight(p2, "/")
	if p1 == p2 {
		return false
	}
	return strings.HasPrefix(p2, p1+"/")
}
