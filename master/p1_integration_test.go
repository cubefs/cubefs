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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

// Phase P1-6 — multi-instance integration tests.
//
// These tests wire the four P1 components together (dispatcher, failover,
// fan-out, quota) against the existing in-memory test stubs and prove
// end-to-end behaviour:
//
//   - load distribution across N nodes
//   - failover redispatch when a node dies
//   - fan-out splits + per-shard owner assignment
//   - quota math holds the cluster total under the configured cap
//
// They do NOT spin up real syncnode processes — that's an operator-run
// integration test against a live cluster, not a unit test. The point
// here is the COMPONENTS COMPOSE: each piece's contract is satisfied by
// the next when they share the same SyncDispatcher.

// -----------------------------------------------------------------------
// P1-6 scaffolding — a "fleet" wrapping the test stubs.
// -----------------------------------------------------------------------

type p1Fleet struct {
	src      *stubSource
	cluster  *stubFailoverCluster
	disp     *SyncDispatcher
	failover *SyncFailover
	fanout   *SyncFanout
	quota    *SyncQuotaCalculator

	// taskLog records every Dispatch result so tests can assert
	// distribution / ordering without poking at internal state.
	mu      sync.Mutex
	taskLog []string // addrs, chronological
}

// newP1Fleet builds the dispatcher / failover / fan-out / quota stack
// around `addrs` worker nodes — all healthy at t=now with identical
// runtime fields. Tests override per-node fields after construction via
// the returned src.
func newP1Fleet(t *testing.T, addrs []string) *p1Fleet {
	t.Helper()
	src := newStubSource()
	cluster := newStubFailoverCluster(addrs...)
	for _, addr := range addrs {
		src.set(addr, makeNode(addr, func(sn *SyncNode) {
			sn.RunningTasks = 0
			sn.BandwidthMBps = 0
			sn.CPUPercent = 0
		}))
	}
	disp := newSyncDispatcherFromSource(src)
	failover := newSyncFailoverFromSource(cluster, disp)
	fanout := NewSyncFanout(disp)
	quota := newSyncQuotaCalculatorFromSource(src)
	return &p1Fleet{
		src:      src,
		cluster:  cluster,
		disp:     disp,
		failover: failover,
		fanout:   fanout,
		quota:    quota,
	}
}

// dispatch picks a node for taskID, records the owner, and remembers the
// payload with the failover orchestrator so a later node-death triggers
// a re-dispatch.
func (f *p1Fleet) dispatch(taskID string) (string, error) {
	payload := &proto.AdminTask{
		ID:      taskID,
		OpCode:  proto.OpSyncNodeRunTask,
		Request: map[string]any{"taskId": taskID},
	}
	sendFn := func(addr string) error {
		return f.cluster.sendRunTask(addr, payload)
	}
	addr, err := f.disp.Dispatch(taskID, sendFn, 3)
	if err != nil {
		return "", err
	}
	f.failover.Remember(taskID, payload)
	f.mu.Lock()
	f.taskLog = append(f.taskLog, addr)
	f.mu.Unlock()
	// Simulate the chosen node "accepting" the task — bump its
	// RunningTasks so subsequent load-score reads see it.
	f.bumpRunning(addr, +1)
	return addr, nil
}

// bumpRunning adjusts the addr's RunningTasks. Tests use this to model
// "task arrived" / "task done".
func (f *p1Fleet) bumpRunning(addr string, delta int64) {
	f.src.mu.Lock()
	sn, ok := f.src.nodes[addr]
	f.src.mu.Unlock()
	if !ok {
		return
	}
	atomic.AddInt64(&sn.RunningTasks, delta)
}

// killNode simulates a syncnode going away: the stub fleet drops it,
// the dispatcher's source no longer returns it, and we trigger
// handleNodeDeath to fire the failover hook.
func (f *p1Fleet) killNode(addr string) {
	f.cluster.mu.Lock()
	delete(f.cluster.live, addr)
	f.cluster.mu.Unlock()

	f.src.mu.Lock()
	delete(f.src.nodes, addr)
	f.src.mu.Unlock()

	f.disp.handleNodeDeath(addr)
}

// histogram returns map[addr]count of how many tasks each addr received
// across the entire taskLog (counts re-dispatches that landed elsewhere).
func (f *p1Fleet) histogram() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for _, a := range f.taskLog {
		out[a]++
	}
	// Also include failover re-dispatches recorded by the orchestrator.
	for _, rec := range f.failover.Recent(200) {
		if rec.ToAddr != "" {
			out[rec.ToAddr]++
		}
	}
	return out
}

// -----------------------------------------------------------------------
// AC (a) load distribution — 10 tasks across 3 nodes, std-dev ≤ 30% of mean
// -----------------------------------------------------------------------

func TestP1_LoadDistribution_ThreeNodesTenTasks(t *testing.T) {
	fleet := newP1Fleet(t, []string{"n1:17710", "n2:17710", "n3:17710"})
	const N = 10

	for i := 0; i < N; i++ {
		addr, err := fleet.dispatch(fmt.Sprintf("t-%d", i))
		if err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
		if addr == "" {
			t.Fatalf("dispatch %d: empty addr", i)
		}
	}

	hist := fleet.histogram()
	if len(hist) != 3 {
		t.Fatalf("expected 3 owners, got %d: %+v", len(hist), hist)
	}
	mean := float64(N) / 3.0
	var variance float64
	for _, c := range hist {
		variance += math.Pow(float64(c)-mean, 2)
	}
	std := math.Sqrt(variance / 3.0)
	tolerance := 0.30 * mean
	if std > tolerance {
		t.Errorf("std-dev %.2f > 30%% of mean %.2f: %+v", std, mean, hist)
	}
}

// -----------------------------------------------------------------------
// AC (b) failover — kill the owner, task lands on another node within 60s
// -----------------------------------------------------------------------

func TestP1_Failover_NodeDeathRedispatches(t *testing.T) {
	fleet := newP1Fleet(t, []string{"n1:17710", "n2:17710", "n3:17710"})

	owner, err := fleet.dispatch("t-die")
	if err != nil {
		t.Fatalf("initial dispatch: %v", err)
	}
	if fleet.disp.OwnerOf("t-die") != owner {
		t.Fatalf("OwnerOf mismatch")
	}

	fleet.killNode(owner)

	// After handleNodeDeath, the failover hook re-dispatched. The new
	// owner is recorded in Recent.
	recent := fleet.failover.Recent(10)
	var found *FailoverRecord
	for i := range recent {
		if recent[i].TaskID == "t-die" {
			found = &recent[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("failover record missing; recent=%+v", recent)
	}
	if found.ToAddr == "" {
		t.Fatalf("redispatch should have landed somewhere; err=%s", found.Err)
	}
	if found.ToAddr == owner {
		t.Fatalf("redispatched to the dead node %s", owner)
	}
	// The new owner is recorded; the dispatcher's ownership map agrees.
	if fleet.disp.OwnerOf("t-die") != found.ToAddr {
		t.Errorf("dispatcher OwnerOf=%q, want %q", fleet.disp.OwnerOf("t-die"), found.ToAddr)
	}
}

// TestP1_Failover_DeadLetter_NoSurvivors covers the edge case where every
// remaining node refuses the redispatch — the task lands in the
// dead-letter set.
func TestP1_Failover_DeadLetter_NoSurvivors(t *testing.T) {
	fleet := newP1Fleet(t, []string{"only:17710"})

	if _, err := fleet.dispatch("t-orphan"); err != nil {
		t.Fatalf("initial dispatch: %v", err)
	}
	fleet.killNode("only:17710")

	dl := fleet.failover.DeadLetter()
	if _, ok := dl["t-orphan"]; !ok {
		t.Errorf("expected dead-letter entry; got %+v", dl)
	}
}

// -----------------------------------------------------------------------
// AC (c) fan-out — splitting works + each shard gets a unique node
// -----------------------------------------------------------------------

func TestP1_FanOut_OneSubTaskPerNode(t *testing.T) {
	fleet := newP1Fleet(t, []string{"n1:17710", "n2:17710", "n3:17710", "n4:17710"})

	// Use the production cloner so the test path mirrors api_service.go's
	// dispatchSyncTask routing.
	parent := map[string]any{
		"taskId": "parent-1",
		"ruleId": "r-fan",
	}
	send := func(addr string, shardIndex int, payload interface{}) error {
		task := &proto.AdminTask{ID: fmt.Sprintf("parent-1/%d", shardIndex), OpCode: proto.OpSyncNodeRunTask, Request: payload}
		return fleet.cluster.sendRunTask(addr, task)
	}
	owners, err := fleet.fanout.DispatchN(
		"parent-1", "r-fan", 4, parent,
		jsonRoundTripFanoutCloner, send, 3)
	if err != nil {
		t.Fatalf("DispatchN: %v", err)
	}
	if len(owners) != 4 {
		t.Fatalf("expected 4 sub-task owners, got %d: %+v", len(owners), owners)
	}
	// Every shard mapped to a DIFFERENT node — the dispatcher's
	// tie-break + ownership accounting guarantees this when fleet size
	// >= shardCount.
	seen := map[string]int{}
	for shard, addr := range owners {
		seen[addr]++
		if seen[addr] > 1 {
			t.Errorf("shard %d collided with another on %s", shard, addr)
		}
	}
}

// -----------------------------------------------------------------------
// AC (d) per-rule quota math — cluster cap holds with equal division
// -----------------------------------------------------------------------

func TestP1_Quota_PerRule_EqualDivisionAcrossNodes(t *testing.T) {
	fleet := newP1Fleet(t, []string{"n1:17710", "n2:17710", "n3:17710"})
	fleet.quota.SetRuleLimit("r-aggregate", 400.0)
	fleet.quota.Compute()

	addrs := []string{"n1:17710", "n2:17710", "n3:17710"}
	var total float64
	for _, addr := range addrs {
		q := fleet.quota.QuotasFor(addr)
		got, ok := q.Rules["r-aggregate"]
		if !ok {
			t.Errorf("%s missing per-rule quota", addr)
			continue
		}
		total += got
		// Equal division across 3 active nodes → ~133 each.
		want := 400.0 / 3.0
		if math.Abs(got-want) > 1 {
			t.Errorf("%s quota=%.2f, want ~%.2f", addr, got, want)
		}
	}
	if math.Abs(total-400.0) > 1 {
		t.Errorf("sum of per-node quotas = %.2f, want 400", total)
	}
}

// TestP1_Quota_PerBackend_RespectsClusterCap mirrors the per-rule test
// for the per-backend code path so P1-9 has its own AC.
func TestP1_Quota_PerBackend_RespectsClusterCap(t *testing.T) {
	fleet := newP1Fleet(t, []string{"n1:17710", "n2:17710", "n3:17710"})
	const key = "s3|https://s3.example.com|us-east-1"
	fleet.quota.SetBackendLimit(key, 600.0)
	fleet.quota.Compute()

	var total float64
	for _, addr := range []string{"n1:17710", "n2:17710", "n3:17710"} {
		q := fleet.quota.QuotasFor(addr)
		got, ok := q.Backends[key]
		if !ok {
			t.Errorf("%s missing per-backend quota", addr)
			continue
		}
		total += got
	}
	if math.Abs(total-600.0) > 1 {
		t.Errorf("sum of per-node backend quotas = %.2f, want 600", total)
	}
}

// -----------------------------------------------------------------------
// AC (e) Master leader transition recovery — heartbeats rebuild the node
// table within 30s. P1-5's runtime path runs through loadSyncNodes from
// raft on master startup (B-2 wired this). After a leader switch the
// passive-observer dispatcher sees the new SyncNodes as soon as their
// heartbeats land — there's no separate "load score table" to rebuild
// because the dispatcher reads runtime fields fresh from each lookup.
// This test exercises that property: rebuilding the stubSource (the
// "leader transition" surrogate) immediately makes the dispatcher see
// the new fleet without explicit re-initialisation.
// -----------------------------------------------------------------------

func TestP1_LeaderTransition_DispatcherReadsRebuiltFleet(t *testing.T) {
	fleet := newP1Fleet(t, []string{"old-leader:17710"})

	// "Old leader" goes away — flush the source + cluster maps to model
	// a master process restart with empty in-memory state.
	fleet.src.mu.Lock()
	for k := range fleet.src.nodes {
		delete(fleet.src.nodes, k)
	}
	fleet.src.mu.Unlock()

	// Pre-transition: dispatcher has zero candidates.
	if cands := fleet.disp.Candidates(30 * time.Second); len(cands) != 0 {
		t.Errorf("expected empty candidate list after node teardown, got %v", cands)
	}

	// "New leader" comes up and the same syncnodes re-register via
	// heartbeats. Re-populate the source — same nodes, fresh state.
	for _, addr := range []string{"old-leader:17710", "n2:17710", "n3:17710"} {
		fleet.src.set(addr, makeNode(addr, func(sn *SyncNode) {
			sn.BoltDBHealthy = true
		}))
		fleet.cluster.mu.Lock()
		fleet.cluster.live[addr] = struct{}{}
		fleet.cluster.mu.Unlock()
	}

	// Post-transition: dispatcher sees them immediately. No re-init.
	cands := fleet.disp.Candidates(30 * time.Second)
	if len(cands) != 3 {
		t.Errorf("expected 3 candidates after recovery, got %d: %v", len(cands), cands)
	}
	// And a fresh dispatch picks one of the rebuilt nodes.
	addr, err := fleet.dispatch("t-post-transition")
	if err != nil {
		t.Fatalf("post-transition dispatch: %v", err)
	}
	if addr == "" {
		t.Fatalf("post-transition dispatch returned empty addr")
	}
}
