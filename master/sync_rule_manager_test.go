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
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/proto"
)

func TestBucketsForPrefix_Empty(t *testing.T) {
	if got := bucketsForPrefix(nil, 4); got != nil {
		t.Errorf("nil input → got %v, want nil", got)
	}
	if got := bucketsForPrefix([]string{}, 4); got != nil {
		t.Errorf("empty input → got %v, want nil", got)
	}
}

func TestBucketsForPrefix_ShardLimitZeroOrNegative(t *testing.T) {
	// shardLimit <= 0 means "one bucket per prefix".
	prefixes := []string{"a/", "b/", "c/"}
	got := bucketsForPrefix(prefixes, 0)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for i, b := range got {
		if len(b) != 1 {
			t.Errorf("bucket[%d] = %v, want single-prefix", i, b)
		}
	}
}

func TestBucketsForPrefix_ShardLimitExceeds(t *testing.T) {
	// shardLimit > len(prefixes) caps to len(prefixes).
	prefixes := []string{"a/", "b/"}
	got := bucketsForPrefix(prefixes, 10)
	if len(got) != 2 {
		t.Errorf("len = %d, want 2 (capped)", len(got))
	}
}

func TestBucketsForPrefix_BalancedRoundRobin(t *testing.T) {
	// 6 prefixes / 3 shards → exactly 2 prefixes per shard, round-robin.
	prefixes := []string{"d/", "a/", "f/", "b/", "e/", "c/"}
	got := bucketsForPrefix(prefixes, 3)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	// Sort within each bucket and across all input for stable comparison.
	flat := []string{}
	for _, b := range got {
		if len(b) != 2 {
			t.Errorf("bucket %v should hold 2 prefixes, got %d", b, len(b))
		}
		flat = append(flat, b...)
	}
	sort.Strings(flat)
	if !reflect.DeepEqual(flat, []string{"a/", "b/", "c/", "d/", "e/", "f/"}) {
		t.Errorf("union mismatch: %v", flat)
	}
}

func TestBucketsForPrefix_Deterministic(t *testing.T) {
	// Same input → same output across multiple calls.
	in := []string{"x/", "z/", "a/", "m/"}
	a := bucketsForPrefix(in, 3)
	b := bucketsForPrefix(in, 3)
	if !reflect.DeepEqual(a, b) {
		t.Errorf("non-deterministic: a=%v b=%v", a, b)
	}
}

func TestBuildRunTaskRequest_FullSnapshot(t *testing.T) {
	rule := proto.NewSyncRule(proto.SyncRuleConfig{
		ID:                          "r-1",
		Type:                        "sync",
		Schedule:                    "*/30 * * * * *",
		Parallelism:                 3,
		ShardingStrategy:            "hash",
		BandwidthLimitMBps:          50,
		AggregateBandwidthLimitMBps: 200,
		Src:                         proto.SyncEndpointConfig{Kind: "local", Path: "/srv/data"},
		Dst:                         proto.SyncEndpointConfig{Kind: "s3", Endpoint: "https://s3", Bucket: "b"},
	})
	got := buildRunTaskRequest("task-1", rule, nil)
	if got.TaskID != "task-1" {
		t.Errorf("TaskID = %q, want task-1", got.TaskID)
	}
	if got.RuleID != "r-1" {
		t.Errorf("RuleID = %q, want r-1", got.RuleID)
	}
	if got.Rule == nil || got.Rule.ID() != "r-1" {
		t.Errorf("Rule snapshot not embedded; got %+v", got.Rule)
	}
	if got.SubTask != nil {
		t.Errorf("SubTask should be nil for single-task path, got %+v", got.SubTask)
	}
}

func TestSyncRuleManager_RegisterIgnoredWhenNotStarted(t *testing.T) {
	// A manager that was never Start()ed should accept Register without
	// crashing — admin handlers call Register unconditionally and the
	// not-leader path takes this branch via the `!m.started || m.cron ==
	// nil` short-circuit.
	mgr := NewSyncRuleManager(nil) // cluster ref unused on this path
	r := proto.NewSyncRule(proto.SyncRuleConfig{ID: "r-1", Schedule: "*/5 * * * * *"})
	if err := mgr.Register(r); err != nil {
		t.Errorf("Register on non-started manager: got err=%v, want nil", err)
	}
	mgr.Unregister("r-1") // should also be a no-op
	if got := mgr.RegisteredCount(); got != 0 {
		t.Errorf("RegisteredCount = %d, want 0", got)
	}
}

func TestSyncRuleManager_RegisterNil(t *testing.T) {
	mgr := NewSyncRuleManager(nil)
	if err := mgr.Register(nil); err == nil {
		t.Error("Register(nil) should return an error")
	}
}

func TestSyncRuleConflictError_Implements(t *testing.T) {
	err := &SyncRuleConflictError{Code: SyncRuleErrDuplicate, Msg: "dup", RuleIDs: []string{"a", "b"}}
	if !strings.Contains(err.Error(), "code=1014") {
		t.Errorf("Error() should embed code: %s", err.Error())
	}
	if !strings.Contains(err.Error(), "a,b") {
		t.Errorf("Error() should embed rule IDs: %s", err.Error())
	}
}
