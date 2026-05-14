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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fanoutHarness builds a dispatcher + fanout against an in-memory
// stubSource (defined in sync_dispatcher_test.go) for N nodes named
// "node-0", "node-1", ..., "node-{N-1}" with identical health so the
// dispatcher will round-robin across them.
func fanoutHarness(t *testing.T, nodeCount int) (*SyncFanout, *SyncDispatcher, *stubSource) {
	t.Helper()
	now := time.Now()
	src := newStubSource()
	for i := 0; i < nodeCount; i++ {
		addr := fmt.Sprintf("node-%d", i)
		src.set(addr, makeNode(addr, func(n *SyncNode) {
			n.ReportTime = now
			n.RunningTasks = 0
		}))
	}
	d := newSyncDispatcherFromSource(src)
	d.now = fixedNow(now)
	return NewSyncFanout(d), d, src
}

// Tests use the package-level jsonRoundTripFanoutCloner declared in
// sync_fanout.go so the test path exercises the same cloner the
// dispatchSyncTask handler ships with.

// -----------------------------------------------------------------------
// DispatchN: shape + happy path
// -----------------------------------------------------------------------

func TestSyncFanout_DispatchN_SpreadsAcrossNodes(t *testing.T) {
	fo, disp, _ := fanoutHarness(t, 4)

	type sent struct {
		addr  string
		shard int
	}
	var (
		mu   sync.Mutex
		seen []sent
	)
	send := func(addr string, shard int, payload interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, sent{addr: addr, shard: shard})
		return nil
	}

	parent := map[string]interface{}{"taskId": "parent-1", "ruleId": "rule-1"}
	owners, err := fo.DispatchN("parent-1", "rule-1", 4, parent,
		jsonRoundTripFanoutCloner, send, 3)
	if err != nil {
		t.Fatalf("DispatchN err = %v", err)
	}
	if len(owners) != 4 {
		t.Fatalf("owners = %v, want 4 entries", owners)
	}

	// Every shard got a distinct owner.
	addrs := map[string]bool{}
	for shard := 0; shard < 4; shard++ {
		a, ok := owners[shard]
		if !ok {
			t.Fatalf("shard %d missing from owners", shard)
		}
		if addrs[a] {
			t.Errorf("shard %d reused addr %q", shard, a)
		}
		addrs[a] = true
	}

	// Dispatcher records each sub-task id with its owner.
	for shard := 0; shard < 4; shard++ {
		subID := fmt.Sprintf("parent-1/%d", shard)
		got := disp.OwnerOf(subID)
		if got != owners[shard] {
			t.Errorf("OwnerOf(%q) = %q, want %q", subID, got, owners[shard])
		}
	}

	if len(seen) != 4 {
		t.Errorf("send invoked %d times, want 4", len(seen))
	}
}

func TestSyncFanout_DispatchN_CloneInjectsSubTaskInfo(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 2)

	var payloads []map[string]interface{}
	var mu sync.Mutex
	send := func(_ string, _ int, payload interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		m, _ := payload.(map[string]interface{})
		payloads = append(payloads, m)
		return nil
	}

	parent := map[string]interface{}{"ruleId": "r1"}
	if _, err := fo.DispatchN("p1", "r1", 2, parent, jsonRoundTripFanoutCloner, send, 3); err != nil {
		t.Fatalf("DispatchN err = %v", err)
	}
	if len(payloads) != 2 {
		t.Fatalf("payloads = %d, want 2", len(payloads))
	}
	seen := map[int]bool{}
	for _, p := range payloads {
		sub, ok := p["subTask"]
		if !ok {
			t.Fatalf("payload missing subTask: %v", p)
		}
		raw, _ := json.Marshal(sub)
		var info SubTaskInfo
		_ = json.Unmarshal(raw, &info)
		if info.ParentTaskID != "p1" || info.ShardTotal != 2 {
			t.Errorf("info = %+v, want parent=p1 total=2", info)
		}
		if info.ShardIndex < 0 || info.ShardIndex >= 2 {
			t.Errorf("shardIndex %d out of range", info.ShardIndex)
		}
		seen[info.ShardIndex] = true
	}
	if !seen[0] || !seen[1] {
		t.Errorf("expected shards {0,1} cloned, got %v", seen)
	}
}

// -----------------------------------------------------------------------
// DispatchN: partial failure
// -----------------------------------------------------------------------

func TestSyncFanout_DispatchN_FewerNodesThanShardsReturnsPartial(t *testing.T) {
	// 2 live nodes. The send hook rejects half of the shard attempts so
	// the dispatcher exhausts both candidates and DispatchN reports a
	// partial result.
	fo, _, _ := fanoutHarness(t, 2)
	send := func(addr string, shard int, payload interface{}) error {
		// Reject shards 2 and 3 from every node (simulates "all
		// candidates returned an error" for those shards).
		if shard >= 2 {
			return errors.New("synthetic nack")
		}
		return nil
	}

	owners, err := fo.DispatchN("p1", "r1", 4,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 1)
	if err == nil {
		t.Fatalf("DispatchN should error when shards can't all land")
	}
	if !errors.Is(err, ErrInsufficientCandidates) {
		t.Errorf("err = %v, want ErrInsufficientCandidates wrap", err)
	}
	// Shards 0 and 1 should appear; 2 and 3 must not.
	if _, ok := owners[0]; !ok {
		t.Error("shard 0 missing from owners")
	}
	if _, ok := owners[1]; !ok {
		t.Error("shard 1 missing from owners")
	}
	if _, ok := owners[2]; ok {
		t.Error("shard 2 should not appear in partial owners")
	}
	if _, ok := owners[3]; ok {
		t.Error("shard 3 should not appear in partial owners")
	}
}

func TestSyncFanout_DispatchN_EmptyFleetReturnsError(t *testing.T) {
	// Zero live syncnodes — dispatcher returns ErrNoCandidates per
	// shard, and DispatchN surfaces it as ErrInsufficientCandidates.
	fo, _, _ := fanoutHarness(t, 0)
	send := func(string, int, interface{}) error { return nil }

	owners, err := fo.DispatchN("p1", "r1", 3,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 0)
	if err == nil {
		t.Fatal("DispatchN should error with empty fleet")
	}
	if !errors.Is(err, ErrInsufficientCandidates) {
		t.Errorf("err = %v, want ErrInsufficientCandidates", err)
	}
	if len(owners) != 0 {
		t.Errorf("owners = %v, want empty", owners)
	}
}

func TestSyncFanout_DispatchN_CloneFailureSkipsShard(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 4)

	failCloner := PayloadClonerFunc(func(_ interface{}, info SubTaskInfo) (interface{}, error) {
		if info.ShardIndex == 1 {
			return nil, errors.New("synthetic clone failure")
		}
		return map[string]interface{}{"shard": info.ShardIndex}, nil
	})

	var sentShards []int
	var mu sync.Mutex
	send := func(_ string, shard int, _ interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		sentShards = append(sentShards, shard)
		return nil
	}

	owners, err := fo.DispatchN("p1", "r1", 4,
		map[string]interface{}{}, failCloner, send, 0)
	if err == nil {
		t.Fatalf("DispatchN should fail when a shard clone errors")
	}
	if _, ok := owners[1]; ok {
		t.Errorf("shard 1 should not appear in owners after clone failure")
	}
	if len(owners) != 3 {
		t.Errorf("owners = %v, want 3 entries", owners)
	}
	mu.Lock()
	got := append([]int(nil), sentShards...)
	mu.Unlock()
	for _, s := range got {
		if s == 1 {
			t.Errorf("shard 1 was sent despite clone failure")
		}
	}
}

func TestSyncFanout_DispatchN_RejectsBadInputs(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 2)
	send := func(string, int, interface{}) error { return nil }

	cases := []struct {
		name    string
		parent  string
		rule    string
		total   int
		cloner  PayloadCloner
		send    SendFunc
		wantErr bool
	}{
		{"empty parent", "", "r", 2, jsonRoundTripFanoutCloner, send, true},
		{"zero shardTotal", "p", "r", 0, jsonRoundTripFanoutCloner, send, true},
		{"negative shardTotal", "p", "r", -1, jsonRoundTripFanoutCloner, send, true},
		{"nil cloner", "p", "r", 2, nil, send, true},
		{"nil send", "p", "r", 2, jsonRoundTripFanoutCloner, nil, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := fo.DispatchN(tc.parent, tc.rule, tc.total,
				map[string]interface{}{}, tc.cloner, tc.send, 0)
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------
// RecordProgress + AggregateProgress
// -----------------------------------------------------------------------

func TestSyncFanout_AggregateProgress_SumsAcrossShards(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 4)
	send := func(string, int, interface{}) error { return nil }
	if _, err := fo.DispatchN("p1", "r1", 4,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 0); err != nil {
		t.Fatalf("DispatchN: %v", err)
	}

	fo.RecordProgress("p1", 0, TaskProgress{FilesDone: 25, FilesTotal: 25, BytesDone: 1000, BytesTotal: 1000})
	fo.RecordProgress("p1", 1, TaskProgress{FilesDone: 20, FilesTotal: 25, BytesDone: 800, BytesTotal: 1000})
	fo.RecordProgress("p1", 2, TaskProgress{FilesDone: 25, FilesTotal: 25, BytesDone: 1000, BytesTotal: 1000})
	fo.RecordProgress("p1", 3, TaskProgress{FilesDone: 10, FilesTotal: 25, BytesDone: 400, BytesTotal: 1000})

	agg, ok := fo.AggregateProgress("p1")
	if !ok {
		t.Fatal("AggregateProgress missing parent")
	}
	want := TaskProgress{FilesDone: 80, FilesTotal: 100, BytesDone: 3200, BytesTotal: 4000}
	if agg != want {
		t.Errorf("aggregate = %+v, want %+v", agg, want)
	}
}

func TestSyncFanout_RecordProgress_LaterOverwritesEarlier(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 2)
	send := func(string, int, interface{}) error { return nil }
	if _, err := fo.DispatchN("p1", "r1", 2,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 0); err != nil {
		t.Fatalf("DispatchN: %v", err)
	}
	fo.RecordProgress("p1", 0, TaskProgress{BytesDone: 100})
	fo.RecordProgress("p1", 0, TaskProgress{BytesDone: 200}) // overwrites
	agg, _ := fo.AggregateProgress("p1")
	if agg.BytesDone != 200 {
		t.Errorf("BytesDone = %d, want 200", agg.BytesDone)
	}
}

func TestSyncFanout_RecordProgress_UnknownParentIsNoop(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 1)
	fo.RecordProgress("ghost", 0, TaskProgress{BytesDone: 999})
	_, ok := fo.AggregateProgress("ghost")
	if ok {
		t.Error("AggregateProgress(ghost) returned ok, want false")
	}
}

func TestSyncFanout_RecordProgress_RejectsOutOfRangeShard(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 2)
	send := func(string, int, interface{}) error { return nil }
	if _, err := fo.DispatchN("p1", "r1", 2,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 0); err != nil {
		t.Fatalf("DispatchN: %v", err)
	}
	fo.RecordProgress("p1", -1, TaskProgress{BytesDone: 50})
	fo.RecordProgress("p1", 99, TaskProgress{BytesDone: 50})
	agg, _ := fo.AggregateProgress("p1")
	if agg.BytesDone != 0 {
		t.Errorf("BytesDone = %d, want 0 (out-of-range progress should be dropped)", agg.BytesDone)
	}
}

// -----------------------------------------------------------------------
// IsParent, Owners, Clear
// -----------------------------------------------------------------------

func TestSyncFanout_IsParent_ReportsParentVsLeaf(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 2)
	send := func(string, int, interface{}) error { return nil }
	if _, err := fo.DispatchN("parent-A", "r1", 2,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 0); err != nil {
		t.Fatalf("DispatchN: %v", err)
	}
	if !fo.IsParent("parent-A") {
		t.Error("IsParent(parent-A) = false, want true")
	}
	if fo.IsParent("parent-A/0") {
		t.Error("IsParent(parent-A/0) = true, want false (leaf)")
	}
	if fo.IsParent("ghost") {
		t.Error("IsParent(ghost) = true, want false")
	}
}

func TestSyncFanout_Owners_ReturnsSnapshot(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 3)
	send := func(string, int, interface{}) error { return nil }
	if _, err := fo.DispatchN("p1", "r1", 3,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 0); err != nil {
		t.Fatalf("DispatchN: %v", err)
	}
	owners := fo.Owners("p1")
	if len(owners) != 3 {
		t.Fatalf("Owners = %v, want 3 entries", owners)
	}
	// Mutating the returned map must not affect internal state.
	owners[99] = "tampered"
	again := fo.Owners("p1")
	if _, leaked := again[99]; leaked {
		t.Error("returned map is the same instance — must be a snapshot")
	}
	if fo.Owners("ghost") != nil {
		t.Error("Owners(ghost) should be nil")
	}
}

func TestSyncFanout_Clear_RemovesParent(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 2)
	send := func(string, int, interface{}) error { return nil }
	if _, err := fo.DispatchN("p1", "r1", 2,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 0); err != nil {
		t.Fatalf("DispatchN: %v", err)
	}
	fo.Clear("p1")
	if fo.IsParent("p1") {
		t.Error("IsParent(p1) = true after Clear")
	}
}

// -----------------------------------------------------------------------
// Concurrency
// -----------------------------------------------------------------------

func TestSyncFanout_ConcurrentRecordProgress(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 4)
	send := func(string, int, interface{}) error { return nil }
	if _, err := fo.DispatchN("p1", "r1", 4,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 0); err != nil {
		t.Fatalf("DispatchN: %v", err)
	}

	const writersPerShard = 50
	var wg sync.WaitGroup
	var maxRound int64 = 100
	for shard := 0; shard < 4; shard++ {
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int64(1); i <= maxRound; i++ {
				fo.RecordProgress("p1", shard, TaskProgress{
					FilesDone: i,
					BytesDone: i * 10,
				})
			}
		}()
	}
	// Reader goroutines: just exercise the read path concurrently to
	// surface race issues under -race.
	for i := 0; i < writersPerShard; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = fo.AggregateProgress("p1")
			_ = fo.IsParent("p1")
			_ = fo.Owners("p1")
		}()
	}
	wg.Wait()

	// Final state must reflect the last write per shard (BytesDone =
	// 4 * 10 * maxRound when every writer reaches its last iteration).
	agg, ok := fo.AggregateProgress("p1")
	if !ok {
		t.Fatal("aggregate missing")
	}
	atomic.LoadInt64(&maxRound) // keep linter happy
	wantBytes := int64(4) * 10 * maxRound
	if agg.BytesDone != wantBytes {
		t.Errorf("final BytesDone = %d, want %d", agg.BytesDone, wantBytes)
	}
}

// -----------------------------------------------------------------------
// SubTaskID format
// -----------------------------------------------------------------------

func TestSyncFanout_SubTaskID_FormatMatchesRunnerConvention(t *testing.T) {
	// Format must mirror syncnode/tasks.TriggerSubTask: "<parent>/<idx>"
	cases := []struct {
		parent string
		idx    int
		want   string
	}{
		{"t-1", 0, "t-1/0"},
		{"abc", 3, "abc/3"},
		{"complex-id-42", 10, "complex-id-42/10"},
	}
	for _, tc := range cases {
		if got := subTaskID(tc.parent, tc.idx); got != tc.want {
			t.Errorf("subTaskID(%q, %d) = %q, want %q", tc.parent, tc.idx, got, tc.want)
		}
	}
}

// TestSyncFanout_DispatchN_TwiceForSameParentRefreshes verifies that
// re-dispatching the same parent (e.g. after a partial failure on a
// previous call) preserves progress already recorded but refreshes the
// shardTotal / ruleID. Exercises the existing-parent branch of
// registerParent.
func TestSyncFanout_DispatchN_TwiceForSameParentRefreshes(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 4)
	send := func(string, int, interface{}) error { return nil }

	if _, err := fo.DispatchN("p1", "r1", 2,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 3); err != nil {
		t.Fatalf("DispatchN 1: %v", err)
	}
	// Record progress on the first dispatch.
	fo.RecordProgress("p1", 0, TaskProgress{BytesDone: 500})
	fo.RecordProgress("p1", 1, TaskProgress{BytesDone: 500})

	// Re-dispatch the SAME parent at a different shardTotal — progress
	// must be preserved. (In production this models the
	// "retry-missing-shards" recovery path.)
	if _, err := fo.DispatchN("p1", "r1-updated", 4,
		map[string]interface{}{}, jsonRoundTripFanoutCloner, send, 3); err != nil {
		t.Fatalf("DispatchN 2: %v", err)
	}
	agg, ok := fo.AggregateProgress("p1")
	if !ok {
		t.Fatal("aggregate missing after re-dispatch")
	}
	if agg.BytesDone != 1000 {
		t.Errorf("BytesDone = %d, want 1000 (progress preserved across re-dispatch)", agg.BytesDone)
	}
}

// -----------------------------------------------------------------------
// FIX #7: Recover tests — fan-out parents rebuild from dispatcher ledger
// -----------------------------------------------------------------------

func TestSyncFanout_Recover_Empty(t *testing.T) {
	fo, _, _ := fanoutHarness(t, 2)
	if n := fo.Recover(); n != 0 {
		t.Errorf("Recover on empty ledger returned %d, want 0", n)
	}
}

func TestSyncFanout_Recover_RebuildsParents(t *testing.T) {
	fo, disp, _ := fanoutHarness(t, 4)

	// Simulate that a previous master leader dispatched two parent tasks
	// (each split into 3 shards) and the dispatcher's ownership ledger
	// survives. The fanout's parents map is empty (fresh master).
	parents := map[string][]int{
		"job-1": {0, 1, 2},
		"job-2": {0, 1, 2},
	}
	for parentID, shards := range parents {
		for _, idx := range shards {
			taskID := fmt.Sprintf("%s/%d", parentID, idx)
			_, err := disp.Dispatch(taskID, func(addr string) error { return nil }, 0)
			if err != nil {
				t.Fatalf("seed dispatch %s: %v", taskID, err)
			}
		}
	}

	// Drop the fanout's parents map (simulate fresh in-memory state on
	// leader transition).
	fo.mu.Lock()
	fo.parents = map[string]*parentTask{}
	fo.mu.Unlock()

	recovered := fo.Recover()
	if recovered != 2 {
		t.Errorf("Recover returned %d, want 2", recovered)
	}
	for parentID := range parents {
		if !fo.IsParent(parentID) {
			t.Errorf("IsParent(%q) = false after Recover", parentID)
		}
		owners := fo.Owners(parentID)
		if len(owners) != 3 {
			t.Errorf("parent %q: %d owners, want 3", parentID, len(owners))
		}
	}
}

func TestSyncFanout_Recover_Idempotent(t *testing.T) {
	fo, disp, _ := fanoutHarness(t, 2)
	for _, idx := range []int{0, 1} {
		_, _ = disp.Dispatch(fmt.Sprintf("p/%d", idx), func(addr string) error { return nil }, 0)
	}
	first := fo.Recover()
	second := fo.Recover()
	if first != 1 {
		t.Errorf("first Recover = %d, want 1", first)
	}
	if second != 0 {
		t.Errorf("second Recover = %d, want 0 (idempotent)", second)
	}
}

func TestSyncFanout_Recover_SkipsNonShardKeys(t *testing.T) {
	fo, disp, _ := fanoutHarness(t, 2)
	// Single-shard tasks (no "/" in ID) and tasks with non-numeric
	// shard suffix MUST be skipped by Recover.
	for _, tid := range []string{"single-task", "weird/notanumber", "p2/1"} {
		_, _ = disp.Dispatch(tid, func(addr string) error { return nil }, 0)
	}
	recovered := fo.Recover()
	if recovered != 1 {
		t.Errorf("Recover = %d, want 1 (only p2)", recovered)
	}
	if !fo.IsParent("p2") {
		t.Errorf("p2 should be a parent")
	}
	if fo.IsParent("single-task") || fo.IsParent("weird") {
		t.Errorf("non-shard tasks should not become parents")
	}
}

// -----------------------------------------------------------------------
// Dispatcher: AllOwnerships snapshot
// -----------------------------------------------------------------------

func TestDispatcher_AllOwnerships_Snapshot(t *testing.T) {
	_, disp, _ := fanoutHarness(t, 3)
	for i := 0; i < 4; i++ {
		_, err := disp.Dispatch(fmt.Sprintf("t-%d", i), func(addr string) error { return nil }, 0)
		if err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}
	snap := disp.AllOwnerships()
	if len(snap) != 4 {
		t.Errorf("AllOwnerships = %d entries, want 4", len(snap))
	}
	// Mutating the snapshot must NOT affect the dispatcher.
	snap["bogus"] = "node-0"
	if disp.OwnerOf("bogus") != "" {
		t.Errorf("AllOwnerships should return a copy; dispatcher mutated")
	}
}
