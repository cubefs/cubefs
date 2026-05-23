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
	"strings"
	"testing"

	"github.com/cubefs/cubefs/proto"
)

// TestEffectiveShardCount pins the resolution priority that decouples
// the new ShardCount field from legacy Parallelism. ShardCount wins
// when > 0; Parallelism is the backward-compat fallback for rules
// persisted before ShardCount existed; both unset = single dispatch.
func TestEffectiveShardCount(t *testing.T) {
	cases := []struct {
		name        string
		shardCount  int
		parallelism int
		want        int
	}{
		{"both unset → 1", 0, 0, 1},
		{"shardCount only", 3, 0, 3},
		{"parallelism only (legacy)", 0, 4, 4},
		{"shardCount overrides parallelism", 2, 8, 2},
		{"shardCount=1 means no fan-out", 1, 8, 1},
		{"shardCount wins when both set", 5, 5, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &proto.SyncRuleConfig{
				ShardCount:  tc.shardCount,
				Parallelism: tc.parallelism,
			}
			if got := effectiveShardCount(cfg); got != tc.want {
				t.Errorf("effectiveShardCount(ShardCount=%d, Parallelism=%d) = %d, want %d",
					tc.shardCount, tc.parallelism, got, tc.want)
			}
		})
	}
}

// TestValidateSyncRuleShape_ShardCountNegative ensures the handler
// rejects ShardCount<0 at the boundary before the rule reaches raft.
// Zero is a valid sentinel ("legacy fallback / no fan-out") and must
// NOT be rejected.
func TestValidateSyncRuleShape_ShardCountNegative(t *testing.T) {
	mk := func(shardCount int) *proto.SyncRule {
		return &proto.SyncRule{Config: proto.SyncRuleConfig{
			ID:         "r-1",
			Type:       "sync",
			ShardCount: shardCount,
		}}
	}
	if err := validateSyncRuleShape(mk(-1)); err == nil {
		t.Error("validateSyncRuleShape(ShardCount=-1) should reject, got nil")
	} else if !strings.Contains(err.Error(), "shardCount") {
		t.Errorf("error should mention shardCount: %v", err)
	}
	if err := validateSyncRuleShape(mk(0)); err != nil {
		t.Errorf("validateSyncRuleShape(ShardCount=0) should pass (legacy fallback), got %v", err)
	}
	if err := validateSyncRuleShape(mk(3)); err != nil {
		t.Errorf("validateSyncRuleShape(ShardCount=3) should pass, got %v", err)
	}
}

// TestValidateSyncRuleShape_ParallelismNegative pins the same boundary
// guard for Parallelism — zero is a valid sentinel ("syncnode default"),
// negative is a malformed input.
func TestValidateSyncRuleShape_ParallelismNegative(t *testing.T) {
	mk := func(parallelism int) *proto.SyncRule {
		return &proto.SyncRule{Config: proto.SyncRuleConfig{
			ID:          "r-1",
			Type:        "sync",
			Parallelism: parallelism,
		}}
	}
	if err := validateSyncRuleShape(mk(-1)); err == nil {
		t.Error("validateSyncRuleShape(Parallelism=-1) should reject, got nil")
	} else if !strings.Contains(err.Error(), "parallelism") {
		t.Errorf("error should mention parallelism: %v", err)
	}
	if err := validateSyncRuleShape(mk(0)); err != nil {
		t.Errorf("validateSyncRuleShape(Parallelism=0) should pass (default), got %v", err)
	}
}

// TestBucketsForPrefix_ShardCountCap reproduces the dispatchPrefix
// behaviour where the bucket count is capped by min(len(prefixes),
// shardCount). This mirrors the new dispatch path that passes
// effectiveShardCount as the cap.
func TestBucketsForPrefix_ShardCountCap(t *testing.T) {
	prefixes := []string{"a/", "b/", "c/", "d/", "e/"}
	// shardCount=2 → 2 buckets even though we have 5 prefixes.
	got := bucketsForPrefix(prefixes, 2)
	if len(got) != 2 {
		t.Errorf("bucketsForPrefix(5 prefixes, cap=2) → %d buckets, want 2", len(got))
	}
	// shardCount=10 → capped to len(prefixes)=5.
	got = bucketsForPrefix(prefixes, 10)
	if len(got) != 5 {
		t.Errorf("bucketsForPrefix(5 prefixes, cap=10) → %d buckets, want 5 (len cap)", len(got))
	}
}
