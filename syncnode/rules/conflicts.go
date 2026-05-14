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

package rules

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// Conflict error codes. These extend the existing syncnode/errors.go
// namespace (1001-1013) without modifying that file. They are stable and
// callers may assert against the numeric value.
const (
	// ErrCodeDuplicateRulePair — two rules have identical (src, dst)
	// endpoint+path tuples. Indicates copy-paste duplication.
	ErrCodeDuplicateRulePair = 1014

	// ErrCodePrefixOverlap — two rules share the same backend pair AND
	// one rule's src.path is a path-prefix of the other's (likewise for
	// dst). Causes overlapping data-movement domains, retention races,
	// and double-writes.
	ErrCodePrefixOverlap = 1015

	// ErrCodeCycleSync — A: X → Y and B: Y → X with identical endpoints.
	// Data ping-pongs between the two backends indefinitely.
	ErrCodeCycleSync = 1016
)

// ConflictError is the typed error returned by Validate. Carries the
// offending rule IDs so operators can locate them in their config /
// admin-API response. The handler layer (E-2) translates this into the
// HTTP response envelope.
type ConflictError struct {
	Code    int
	Msg     string
	RuleIDs []string // exactly 2 ids — the pair that conflicts
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("syncnode rule conflict (code=%d rules=%s): %s",
		e.Code, strings.Join(e.RuleIDs, ","), e.Msg)
}

// Validate runs all conflict checks against the supplied list of rules.
// Returns the FIRST conflict found, or nil. Check order:
//
//  1. duplicate (same src+dst quad-tuple)
//  2. prefix overlap (same backend pair, src or dst path prefix relation)
//  3. cycle sync (A.src == B.dst AND A.dst == B.src)
//
// Validate is a pure function: it does not mutate the input slice nor any
// of the *Rule values it points at.
func Validate(rules []*Rule) error {
	if len(rules) < 2 {
		return nil
	}
	if err := checkDuplicate(rules); err != nil {
		return err
	}
	if err := checkPrefixOverlap(rules); err != nil {
		return err
	}
	if err := checkCycleSync(rules); err != nil {
		return err
	}
	return nil
}

// endpointKey returns a canonical string identifying the storage "anchor"
// (the kind + storage identity), independent of per-rule path/prefix.
// Two endpoints with the same endpointKey live on the same backend
// instance — they can therefore collide on path overlap.
//
//   - cfs   → "cfs:<vol>"              (Path is per-rule, see pathOf)
//   - s3    → "s3:<endpoint>:<bucket>" (Prefix is per-rule, see pathOf)
//   - local → "local:"                 (all local rules share one filesystem;
//     Path is the per-rule field returned by pathOf)
func endpointKey(ep *spec.EndpointConfig) string {
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

// pathOf returns the rule's per-endpoint path/prefix — the portion of the
// endpoint that differs between rules on the same backend anchor.
//
//   - cfs   → ep.Path
//   - s3    → ep.Prefix
//   - local → filepath.Clean(ep.Path) (path IS the per-rule differentiator)
func pathOf(ep *spec.EndpointConfig) string {
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

// checkDuplicate reports the first pair of rules whose
// (srcKey, srcPath, dstKey, dstPath) 4-tuples are byte-equal.
func checkDuplicate(rules []*Rule) error {
	type quad struct{ sk, sp, dk, dp string }
	seen := make(map[quad]string, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		key := quad{
			sk: endpointKey(&r.Config.Src),
			sp: pathOf(&r.Config.Src),
			dk: endpointKey(&r.Config.Dst),
			dp: pathOf(&r.Config.Dst),
		}
		if prev, ok := seen[key]; ok {
			return &ConflictError{
				Code: ErrCodeDuplicateRulePair,
				Msg: fmt.Sprintf("rules %q and %q have identical src/dst endpoints (src=%s%s dst=%s%s)",
					prev, r.ID(), key.sk, key.sp, key.dk, key.dp),
				RuleIDs: []string{prev, r.ID()},
			}
		}
		seen[key] = r.ID()
	}
	return nil
}

// checkPrefixOverlap reports the first pair of rules sharing the same
// (srcKey, dstKey) backend pair where either src paths or dst paths sit
// in a non-equal prefix relation. Equal paths are handled by
// checkDuplicate; this check only fires when at least one side is a
// strict prefix of the other.
func checkPrefixOverlap(rules []*Rule) error {
	for i := 0; i < len(rules); i++ {
		a := rules[i]
		if a == nil {
			continue
		}
		aSrcK := endpointKey(&a.Config.Src)
		aDstK := endpointKey(&a.Config.Dst)
		aSrcP := pathOf(&a.Config.Src)
		aDstP := pathOf(&a.Config.Dst)
		for j := i + 1; j < len(rules); j++ {
			b := rules[j]
			if b == nil {
				continue
			}
			if endpointKey(&b.Config.Src) != aSrcK || endpointKey(&b.Config.Dst) != aDstK {
				continue
			}
			bSrcP := pathOf(&b.Config.Src)
			bDstP := pathOf(&b.Config.Dst)
			if strictPrefix(aSrcP, bSrcP) || strictPrefix(bSrcP, aSrcP) {
				return &ConflictError{
					Code: ErrCodePrefixOverlap,
					Msg: fmt.Sprintf("rules %q and %q have overlapping src paths on backend %s (%q vs %q)",
						a.ID(), b.ID(), aSrcK, aSrcP, bSrcP),
					RuleIDs: []string{a.ID(), b.ID()},
				}
			}
			if strictPrefix(aDstP, bDstP) || strictPrefix(bDstP, aDstP) {
				return &ConflictError{
					Code: ErrCodePrefixOverlap,
					Msg: fmt.Sprintf("rules %q and %q have overlapping dst paths on backend %s (%q vs %q)",
						a.ID(), b.ID(), aDstK, aDstP, bDstP),
					RuleIDs: []string{a.ID(), b.ID()},
				}
			}
		}
	}
	return nil
}

// checkCycleSync reports the first pair (A, B) where A's src endpoint
// equals B's dst endpoint AND A's dst endpoint equals B's src endpoint —
// i.e. data would ping-pong between the two backends.
//
// This is the literal "inverted endpoints" case from design.md §9 E-4
// ("循环 sync (A: cfs→s3 + B: s3→cfs 相同 path)"). A future improvement
// could promote this to overlap-cycle detection (where the endpoints are
// only prefix-overlapping, not equal); document the limitation here so a
// later reader knows the boundary.
func checkCycleSync(rules []*Rule) error {
	type tuple struct{ k, p string }
	type pair struct{ src, dst tuple }
	idx := make(map[pair]string, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		p := pair{
			src: tuple{endpointKey(&r.Config.Src), pathOf(&r.Config.Src)},
			dst: tuple{endpointKey(&r.Config.Dst), pathOf(&r.Config.Dst)},
		}
		// Look for an existing rule whose direction is inverted.
		inv := pair{src: p.dst, dst: p.src}
		if prev, ok := idx[inv]; ok && prev != r.ID() {
			return &ConflictError{
				Code: ErrCodeCycleSync,
				Msg: fmt.Sprintf("rules %q and %q form a sync cycle (%s%s ↔ %s%s)",
					prev, r.ID(), p.src.k, p.src.p, p.dst.k, p.dst.p),
				RuleIDs: []string{prev, r.ID()},
			}
		}
		// First-write wins for the forward direction so the first rule's
		// id is reported as the "earlier" partner in the pair.
		if _, ok := idx[p]; !ok {
			idx[p] = r.ID()
		}
	}
	return nil
}

// strictPrefix reports whether p1 is a strict (non-equal) path-prefix of
// p2. The boundary semantics match filesystem path containment: "/a" is
// a strict prefix of "/a/b" but NOT of "/ab".
//
// Empty strings are not strict prefixes of anything (they're equal to
// each other, which the caller handles via the duplicate check).
func strictPrefix(p1, p2 string) bool {
	if p1 == "" || p2 == "" {
		return false
	}
	if p1 == p2 {
		return false
	}
	// Normalise trailing slash so "/a/" and "/a" behave equivalently.
	p1 = strings.TrimRight(p1, "/")
	p2 = strings.TrimRight(p2, "/")
	if p1 == p2 {
		return false
	}
	return strings.HasPrefix(p2, p1+"/")
}
