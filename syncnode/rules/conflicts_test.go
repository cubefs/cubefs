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
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// --- builders -------------------------------------------------------------

// cfsEP builds a cfs EndpointConfig with vol + path.
func cfsEP(vol, path string) spec.EndpointConfig {
	return spec.EndpointConfig{Kind: "cfs", Vol: vol, Path: path}
}

// s3EP builds an s3 EndpointConfig with endpoint + bucket + prefix.
func s3EP(endpoint, bucket, prefix string) spec.EndpointConfig {
	return spec.EndpointConfig{Kind: "s3", Endpoint: endpoint, Bucket: bucket, Prefix: prefix}
}

// localEP builds a local EndpointConfig at the given path.
func localEP(path string) spec.EndpointConfig {
	return spec.EndpointConfig{Kind: "local", Path: path}
}

// mkRule builds a *Rule via the public factory so the input matches what
// the store actually holds at runtime.
func mkRule(id string, src, dst spec.EndpointConfig) *Rule {
	return NewRule(spec.RuleConfig{
		ID:   id,
		Type: "sync",
		Src:  src,
		Dst:  dst,
	})
}

// asConflict casts err to *ConflictError or fails the test.
func asConflict(t *testing.T, err error) *ConflictError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected ConflictError, got nil")
	}
	var ce *ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ConflictError, got %T: %v", err, err)
	}
	return ce
}

// sortedIDs returns a sorted copy so order-independent assertions are
// stable across map iteration.
func sortedIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

// --- Validate -------------------------------------------------------------

func TestValidate_HappyPaths(t *testing.T) {
	tests := []struct {
		name  string
		rules []*Rule
	}{
		{"empty", nil},
		{"single", []*Rule{mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p1/"))}},
		{
			name: "two unrelated rules",
			rules: []*Rule{
				mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p1/")),
				mkRule("r2", cfsEP("v2", "/b"), s3EP("https://s3", "b2", "p2/")),
			},
		},
		{
			// /a vs /ab — not a strict path prefix, so no overlap.
			name: "lookalike paths that are not prefixes",
			rules: []*Rule{
				mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p/")),
				mkRule("r2", cfsEP("v1", "/ab"), s3EP("https://s3", "b1", "q/")),
			},
		},
		{
			// Different backend pair → prefix overlap doesn't apply.
			name: "same path different backends",
			rules: []*Rule{
				mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p/")),
				mkRule("r2", cfsEP("v2", "/a"), s3EP("https://s3", "b1", "p/")),
			},
		},
		{
			// Inverted direction but the partner rule has a non-matching
			// path on one side → not a cycle.
			name: "near-inverted but path differs",
			rules: []*Rule{
				mkRule("r1", cfsEP("v1", "/x"), s3EP("https://s3", "b1", "q/")),
				mkRule("r2", s3EP("https://s3", "b1", "q/"), cfsEP("v1", "/y")),
			},
		},
		{
			// nil-rule entries must be tolerated (defensive).
			name: "nil entry skipped",
			rules: []*Rule{
				nil,
				mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p/")),
				nil,
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := Validate(tc.rules); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidate_Duplicate(t *testing.T) {
	rules := []*Rule{
		mkRule("r1", cfsEP("v1", "/data"), s3EP("https://s3", "b1", "p/")),
		mkRule("r2", cfsEP("v1", "/data"), s3EP("https://s3", "b1", "p/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodeDuplicateRulePair {
		t.Fatalf("Code = %d, want %d", ce.Code, ErrCodeDuplicateRulePair)
	}
	want := []string{"r1", "r2"}
	if got := sortedIDs(ce.RuleIDs); !reflect.DeepEqual(got, want) {
		t.Errorf("RuleIDs = %v, want %v", got, want)
	}
	if ce.Error() == "" {
		t.Error("expected non-empty Error()")
	}
}

func TestValidate_Duplicate_LocalEndpoint(t *testing.T) {
	// Two rules with identical local src path AND identical dst must be detected as duplicates.
	rules := []*Rule{
		mkRule("r1", localEP("/srv/data"), s3EP("https://s3", "b1", "p/")),
		mkRule("r2", localEP("/srv/data"), s3EP("https://s3", "b1", "p/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodeDuplicateRulePair {
		t.Fatalf("Code = %d, want %d", ce.Code, ErrCodeDuplicateRulePair)
	}
}

func TestValidate_PrefixOverlap_Src(t *testing.T) {
	rules := []*Rule{
		mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p/")),
		mkRule("r2", cfsEP("v1", "/a/sub"), s3EP("https://s3", "b1", "q/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodePrefixOverlap {
		t.Fatalf("Code = %d, want %d", ce.Code, ErrCodePrefixOverlap)
	}
	if got := sortedIDs(ce.RuleIDs); !reflect.DeepEqual(got, []string{"r1", "r2"}) {
		t.Errorf("RuleIDs = %v, want [r1 r2]", got)
	}
}

func TestValidate_PrefixOverlap_Dst(t *testing.T) {
	// srcs differ (no overlap there) but dsts share a strict prefix.
	rules := []*Rule{
		mkRule("r1", cfsEP("v1", "/srcA"), s3EP("https://s3", "b1", "out/")),
		mkRule("r2", cfsEP("v1", "/srcB"), s3EP("https://s3", "b1", "out/sub/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodePrefixOverlap {
		t.Fatalf("Code = %d, want %d", ce.Code, ErrCodePrefixOverlap)
	}
}

func TestValidate_PrefixOverlap_TrailingSlashNormalised(t *testing.T) {
	// "/a/" and "/a/sub" should still trigger prefix overlap even with
	// the trailing slash on the shorter side.
	rules := []*Rule{
		mkRule("r1", cfsEP("v1", "/a/"), s3EP("https://s3", "b1", "p/")),
		mkRule("r2", cfsEP("v1", "/a/sub"), s3EP("https://s3", "b1", "q/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodePrefixOverlap {
		t.Fatalf("Code = %d, want %d", ce.Code, ErrCodePrefixOverlap)
	}
}

func TestValidate_PrefixOverlap_LocalSrc(t *testing.T) {
	// local-to-s3: /tmp/dir/ and /tmp/dir/sub/ share the same filesystem.
	// The sub-path rule should be rejected as a prefix overlap (1015).
	rules := []*Rule{
		mkRule("r1", localEP("/tmp/dir/"), s3EP("https://s3", "b1", "out/")),
		mkRule("r2", localEP("/tmp/dir/sub/"), s3EP("https://s3", "b1", "out/inner/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodePrefixOverlap {
		t.Fatalf("Code = %d, want %d (local src prefix overlap)", ce.Code, ErrCodePrefixOverlap)
	}
}



func TestValidate_CycleSync(t *testing.T) {
	// Classic ping-pong: cfs:v1:/x <-> s3:b/q
	rules := []*Rule{
		mkRule("r1", cfsEP("v1", "/x"), s3EP("https://s3", "b1", "q/")),
		mkRule("r2", s3EP("https://s3", "b1", "q/"), cfsEP("v1", "/x")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodeCycleSync {
		t.Fatalf("Code = %d, want %d", ce.Code, ErrCodeCycleSync)
	}
	if got := sortedIDs(ce.RuleIDs); !reflect.DeepEqual(got, []string{"r1", "r2"}) {
		t.Errorf("RuleIDs = %v, want [r1 r2]", got)
	}
}

func TestValidate_CheckOrder_DuplicateBeatsOverlap(t *testing.T) {
	// A duplicate and a prefix-overlap both exist; Validate must report
	// the duplicate first.
	rules := []*Rule{
		mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p/")),
		mkRule("r2", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p/")), // dup of r1
		mkRule("r3", cfsEP("v1", "/a/sub"), s3EP("https://s3", "b1", "z/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodeDuplicateRulePair {
		t.Fatalf("Code = %d, want %d (dup wins over overlap)", ce.Code, ErrCodeDuplicateRulePair)
	}
}

func TestValidate_CheckOrder_OverlapBeatsCycle(t *testing.T) {
	// A prefix overlap and a cycle both exist; Validate must report the
	// overlap first.
	rules := []*Rule{
		mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p/")),
		mkRule("r2", cfsEP("v1", "/a/sub"), s3EP("https://s3", "b1", "q/")),
		mkRule("r3", s3EP("https://s3", "b2", "k/"), cfsEP("v9", "/z")),
		mkRule("r4", cfsEP("v9", "/z"), s3EP("https://s3", "b2", "k/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodePrefixOverlap {
		t.Fatalf("Code = %d, want %d (overlap wins over cycle)", ce.Code, ErrCodePrefixOverlap)
	}
}

func TestValidate_MixedManyValidPlusOneDup(t *testing.T) {
	rules := []*Rule{
		mkRule("r1", cfsEP("vA", "/x"), s3EP("https://s3", "bA", "px/")),
		mkRule("r2", cfsEP("vB", "/x"), s3EP("https://s3", "bB", "py/")),
		mkRule("r3", cfsEP("vC", "/y"), s3EP("https://s3", "bC", "pz/")),
		mkRule("r4", cfsEP("vD", "/q"), localEP("/srv/d1")),
		mkRule("r5", localEP("/srv/d2"), s3EP("https://s3", "bE", "px/")),
		// r6 duplicates r3 exactly.
		mkRule("r6", cfsEP("vC", "/y"), s3EP("https://s3", "bC", "pz/")),
	}
	ce := asConflict(t, Validate(rules))
	if ce.Code != ErrCodeDuplicateRulePair {
		t.Fatalf("Code = %d, want %d", ce.Code, ErrCodeDuplicateRulePair)
	}
	if got := sortedIDs(ce.RuleIDs); !reflect.DeepEqual(got, []string{"r3", "r6"}) {
		t.Errorf("RuleIDs = %v, want [r3 r6]", got)
	}
}

func TestValidate_DoesNotMutateInput(t *testing.T) {
	rules := []*Rule{
		mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p/")),
		mkRule("r2", cfsEP("v1", "/b"), s3EP("https://s3", "b1", "q/")),
	}
	// Snapshot order before. Run Validate twice — if it sorted or
	// otherwise mutated the slice, the second call would observe
	// different state.
	wantOrder := []string{"r1", "r2"}
	if err := Validate(rules); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if err := Validate(rules); err != nil {
		t.Fatalf("unexpected (2nd call): %v", err)
	}
	got := make([]string, len(rules))
	for i, r := range rules {
		got[i] = r.ID()
	}
	if !reflect.DeepEqual(got, wantOrder) {
		t.Errorf("input was mutated: got %v, want %v", got, wantOrder)
	}
}

// --- endpointKey + pathOf -------------------------------------------------

func TestEndpointKey(t *testing.T) {
	tests := []struct {
		name string
		ep   spec.EndpointConfig
		want string
	}{
		{"cfs uses vol only", cfsEP("v1", "/a"), "cfs:v1"},
		{"cfs equal vol same key regardless of path", cfsEP("v1", "/b"), "cfs:v1"},
		{"s3 uses endpoint+bucket", s3EP("https://s3", "bA", "px/"), "s3:https://s3:bA"},
		{"s3 different bucket → different key", s3EP("https://s3", "bB", "px/"), "s3:https://s3:bB"},
		{"s3 different endpoint → different key", s3EP("https://other", "bA", "px/"), "s3:https://other:bA"},
		{"local uses kind only (path is per-rule, see pathOf)", localEP("/srv/data/"), "local:"},
		{"unknown kind falls back to kind:", spec.EndpointConfig{Kind: "weird"}, "weird:"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := endpointKey(&tc.ep); got != tc.want {
				t.Errorf("endpointKey(%+v) = %q, want %q", tc.ep, got, tc.want)
			}
		})
	}

	// nil receiver guards against accidental misuse.
	if got := endpointKey(nil); got != "" {
		t.Errorf("endpointKey(nil) = %q, want empty", got)
	}
}

func TestEndpointKey_S3EndpointAndBucketBothContribute(t *testing.T) {
	a := endpointKey(&spec.EndpointConfig{Kind: "s3", Endpoint: "https://x", Bucket: "y"})
	b := endpointKey(&spec.EndpointConfig{Kind: "s3", Endpoint: "https://y", Bucket: "x"})
	if a == b {
		t.Errorf("s3 endpoint+bucket must distinguish keys: a=%q b=%q", a, b)
	}
}

func TestPathOf(t *testing.T) {
	tests := []struct {
		name string
		ep   spec.EndpointConfig
		want string
	}{
		{"cfs returns Path", cfsEP("v1", "/a"), "/a"},
		{"s3 returns Prefix", s3EP("https://s3", "b", "p/"), "p/"},
		{"local returns cleaned path (path is the per-rule differentiator)", localEP("/srv/data"), "/srv/data"},
		{"unknown returns empty", spec.EndpointConfig{Kind: "weird", Path: "/x"}, ""},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := pathOf(&tc.ep); got != tc.want {
				t.Errorf("pathOf(%+v) = %q, want %q", tc.ep, got, tc.want)
			}
		})
	}
	if got := pathOf(nil); got != "" {
		t.Errorf("pathOf(nil) = %q, want empty", got)
	}
}

// --- strictPrefix ---------------------------------------------------------

func TestStrictPrefix(t *testing.T) {
	tests := []struct {
		name   string
		p1, p2 string
		want   bool
	}{
		{"equal is not strict prefix", "/a", "/a", false},
		{"equal after trim", "/a/", "/a", false},
		{"sibling /a vs /ab", "/a", "/ab", false},
		{"reverse sibling /ab vs /a", "/ab", "/a", false},
		{"strict /a is prefix of /a/b", "/a", "/a/b", true},
		{"strict with trailing slash", "/a/", "/a/b", true},
		{"deep prefix", "/a/b", "/a/b/c/d", true},
		{"unrelated", "/x", "/y", false},
		{"empty never prefixes", "", "/a", false},
		{"never prefixed by anything if other empty", "/a", "", false},
		{"both empty", "", "", false},
		{"s3-style prefix p/ vs p/sub/", "p/", "p/sub/", true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := strictPrefix(tc.p1, tc.p2); got != tc.want {
				t.Errorf("strictPrefix(%q,%q) = %v, want %v", tc.p1, tc.p2, got, tc.want)
			}
		})
	}
}

// --- ConflictError.Error() ------------------------------------------------

func TestConflictError_Error(t *testing.T) {
	e := &ConflictError{
		Code:    ErrCodeDuplicateRulePair,
		Msg:     "boom",
		RuleIDs: []string{"r1", "r2"},
	}
	got := e.Error()
	for _, want := range []string{"1014", "r1", "r2", "boom"} {
		if !strings.Contains(got, want) {
			t.Errorf("Error() = %q, must contain %q", got, want)
		}
	}
}
