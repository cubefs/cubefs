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
	"errors"
	"testing"

	"github.com/cubefs/cubefs/proto"
)

// mkRule returns a SyncRule with the supplied id + src/dst endpoints.
// Other fields default to zero so the conflict checks see a stable
// minimal shape.
func mkRule(id string, src, dst proto.SyncEndpointConfig) *proto.SyncRule {
	return proto.NewSyncRule(proto.SyncRuleConfig{
		ID:  id,
		Src: src,
		Dst: dst,
	})
}

func cfsEP(vol, path string) proto.SyncEndpointConfig {
	return proto.SyncEndpointConfig{Kind: "cfs", Vol: vol, Path: path}
}
func s3EP(endpoint, bucket, prefix string) proto.SyncEndpointConfig {
	return proto.SyncEndpointConfig{Kind: "s3", Endpoint: endpoint, Bucket: bucket, Prefix: prefix}
}
func localEP(path string) proto.SyncEndpointConfig {
	return proto.SyncEndpointConfig{Kind: "local", Path: path}
}

func asConflict(t *testing.T, err error) *SyncRuleConflictError {
	t.Helper()
	var ce *SyncRuleConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *SyncRuleConflictError, got %T: %v", err, err)
	}
	return ce
}

func TestValidateSyncRules_NoConflict(t *testing.T) {
	rules := []*proto.SyncRule{
		mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p1/")),
		mkRule("r2", cfsEP("v2", "/a"), s3EP("https://s3", "b2", "p2/")),
	}
	if err := ValidateSyncRules(rules); err != nil {
		t.Errorf("expected no conflict, got %v", err)
	}
}

func TestValidateSyncRules_Duplicate(t *testing.T) {
	rules := []*proto.SyncRule{
		mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p1/")),
		mkRule("r2", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p1/")),
	}
	ce := asConflict(t, ValidateSyncRules(rules))
	if ce.Code != SyncRuleErrDuplicate {
		t.Errorf("Code = %d, want %d", ce.Code, SyncRuleErrDuplicate)
	}
}

func TestValidateSyncRules_PrefixOverlap_Src(t *testing.T) {
	rules := []*proto.SyncRule{
		mkRule("r1", cfsEP("v1", "/a"), s3EP("https://s3", "b1", "p1/")),
		mkRule("r2", cfsEP("v1", "/a/sub"), s3EP("https://s3", "b1", "p2/")),
	}
	ce := asConflict(t, ValidateSyncRules(rules))
	if ce.Code != SyncRuleErrPrefixOverlap {
		t.Errorf("Code = %d, want %d", ce.Code, SyncRuleErrPrefixOverlap)
	}
}

func TestValidateSyncRules_PrefixOverlap_LocalSrc(t *testing.T) {
	rules := []*proto.SyncRule{
		mkRule("r1", localEP("/tmp/dir/"), s3EP("https://s3", "b1", "out/")),
		mkRule("r2", localEP("/tmp/dir/sub/"), s3EP("https://s3", "b1", "out2/")),
	}
	ce := asConflict(t, ValidateSyncRules(rules))
	if ce.Code != SyncRuleErrPrefixOverlap {
		t.Errorf("Code = %d, want %d", ce.Code, SyncRuleErrPrefixOverlap)
	}
}

func TestValidateSyncRules_Cycle(t *testing.T) {
	rules := []*proto.SyncRule{
		mkRule("r1", cfsEP("v1", "/x"), s3EP("https://s3", "b1", "p/")),
		mkRule("r2", s3EP("https://s3", "b1", "p/"), cfsEP("v1", "/x")),
	}
	ce := asConflict(t, ValidateSyncRules(rules))
	if ce.Code != SyncRuleErrCycle {
		t.Errorf("Code = %d, want %d", ce.Code, SyncRuleErrCycle)
	}
}

func TestSyncEndpointKey(t *testing.T) {
	tests := []struct {
		name string
		ep   proto.SyncEndpointConfig
		want string
	}{
		{"cfs", cfsEP("v1", "/p"), "cfs:v1"},
		{"s3", s3EP("https://s3", "bA", "px/"), "s3:https://s3:bA"},
		{"s3 different bucket", s3EP("https://s3", "bB", "px/"), "s3:https://s3:bB"},
		{"local uses kind only", localEP("/srv/data/"), "local:"},
		{"unknown falls back to kind:", proto.SyncEndpointConfig{Kind: "weird"}, "weird:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := syncEndpointKey(&tc.ep); got != tc.want {
				t.Errorf("syncEndpointKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSyncPathOf(t *testing.T) {
	tests := []struct {
		name string
		ep   proto.SyncEndpointConfig
		want string
	}{
		{"cfs returns Path", cfsEP("v1", "/a"), "/a"},
		{"s3 returns Prefix", s3EP("https://s3", "b", "p/"), "p/"},
		{"local returns cleaned path", localEP("/srv/data/"), "/srv/data"},
		{"unknown returns empty", proto.SyncEndpointConfig{Kind: "weird", Path: "/x"}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := syncPathOf(&tc.ep); got != tc.want {
				t.Errorf("syncPathOf() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStrictSyncPrefix(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"/a", "/a/b", true},
		{"/a", "/ab", false},
		{"/a/", "/a/b", true},
		{"/a", "/a", false},
		{"", "/a", false},
		{"/a", "", false},
		{"/a/b", "/a", false},
	}
	for _, tc := range cases {
		if got := strictSyncPrefix(tc.a, tc.b); got != tc.want {
			t.Errorf("strictSyncPrefix(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
