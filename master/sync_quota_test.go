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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// -----------------------------------------------------------------------
// Test scaffolding
// -----------------------------------------------------------------------
// Re-uses *stubSource from sync_dispatcher_test.go — it implements the
// same rangeSyncNodes signature so it satisfies syncQuotaSource. Helper
// newActiveSyncNode constructs a SyncNode in the active+fresh state.

func newActiveSyncNode(addr string) *SyncNode {
	sn := newSyncNode(addr, "test-cluster")
	sn.IsActive = true
	sn.ReportTime = time.Now()
	return sn
}

func newQuotaCalc(src syncQuotaSource) *SyncQuotaCalculator {
	q := newSyncQuotaCalculatorFromSource(src)
	q.staleness = 0 // disable staleness checks in tests for determinism
	q.now = func() time.Time { return time.Unix(0, 0).UTC() }
	return q
}

// approxEqual is a small float comparator — quotas are floats so we
// compare to ±1e-6 tolerance.
func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-6
}

// -----------------------------------------------------------------------
// SetRuleLimit / SetBackendLimit
// -----------------------------------------------------------------------

// TestSyncQuota_SetRuleLimit_AddRemove — Set then remove via mbps<=0.
func TestSyncQuota_SetRuleLimit_AddRemove(t *testing.T) {
	t.Parallel()
	q := newQuotaCalc(newStubSource())
	q.SetRuleLimit("r1", 300)
	rules, _ := q.Snapshot()
	if got := rules["r1"]; got != 300 {
		t.Errorf("rules[r1] = %v, want 300", got)
	}
	q.SetRuleLimit("r1", 0)
	rules, _ = q.Snapshot()
	if _, ok := rules["r1"]; ok {
		t.Errorf("rules[r1] still present after remove")
	}
	// Empty rule ID is a no-op (master must not push blank IDs).
	q.SetRuleLimit("", 100)
	rules, _ = q.Snapshot()
	if _, ok := rules[""]; ok {
		t.Errorf("blank rule ID was accepted")
	}
}

// TestSyncQuota_SetBackendLimit_AddRemove — symmetric for backends.
func TestSyncQuota_SetBackendLimit_AddRemove(t *testing.T) {
	t.Parallel()
	q := newQuotaCalc(newStubSource())
	const key = "s3|https://s3|us-east-1"
	q.SetBackendLimit(key, 600)
	_, backs := q.Snapshot()
	if got := backs[key]; got != 600 {
		t.Errorf("backends[%s] = %v, want 600", key, got)
	}
	q.SetBackendLimit(key, 0)
	_, backs = q.Snapshot()
	if _, ok := backs[key]; ok {
		t.Errorf("backend cap not removed")
	}
	q.SetBackendLimit("", 100)
	_, backs = q.Snapshot()
	if _, ok := backs[""]; ok {
		t.Errorf("blank backend key was accepted")
	}
}

// -----------------------------------------------------------------------
// Compute() — equal-division math
// -----------------------------------------------------------------------

// TestSyncQuota_Compute_EqualDivision — 3 nodes + 1 rule with cap=300 →
// each node gets 100 (P1-8 AC: cluster total in [360, 440] for cap=400).
func TestSyncQuota_Compute_EqualDivision(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	for _, addr := range []string{"n1", "n2", "n3"} {
		src.set(addr, newActiveSyncNode(addr))
	}
	q := newQuotaCalc(src)
	q.SetRuleLimit("ruleA", 300)
	q.SetBackendLimit("s3|ep|r1", 600)
	q.Compute()

	for _, addr := range []string{"n1", "n2", "n3"} {
		nq := q.QuotasFor(addr)
		if !approxEqual(nq.Rules["ruleA"], 100) {
			t.Errorf("%s: Rules[ruleA] = %v, want 100", addr, nq.Rules["ruleA"])
		}
		if !approxEqual(nq.Backends["s3|ep|r1"], 200) {
			t.Errorf("%s: Backends[s3|...] = %v, want 200", addr, nq.Backends["s3|ep|r1"])
		}
	}

	// Cluster total === C: 3 nodes * (C/3) = C. This proves the cluster
	// ceiling holds (P1-8 / P1-9 ACs allow ±10%; equal division gives 0%).
	total := 0.0
	for _, addr := range []string{"n1", "n2", "n3"} {
		total += q.QuotasFor(addr).Rules["ruleA"]
	}
	if !approxEqual(total, 300) {
		t.Errorf("cluster total = %v, want 300", total)
	}
}

// TestSyncQuota_Compute_CapRemoval — removing a cap drops the entry from
// subsequent Compute() output.
func TestSyncQuota_Compute_CapRemoval(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.set("n1", newActiveSyncNode("n1"))
	src.set("n2", newActiveSyncNode("n2"))
	q := newQuotaCalc(src)
	q.SetRuleLimit("r1", 200)
	q.Compute()
	if got := q.QuotasFor("n1").Rules["r1"]; !approxEqual(got, 100) {
		t.Fatalf("pre-removal: Rules[r1] = %v, want 100", got)
	}
	q.SetRuleLimit("r1", 0)
	q.Compute()
	nq := q.QuotasFor("n1")
	if _, ok := nq.Rules["r1"]; ok {
		t.Errorf("after removal Rules[r1] still present")
	}
}

// TestSyncQuota_Compute_NoActiveNodes — when nobody is active perNode is
// cleared so QuotasFor returns IsEmpty.
func TestSyncQuota_Compute_NoActiveNodes(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	// Inactive node
	sn := newActiveSyncNode("dead")
	sn.IsActive = false
	src.set("dead", sn)
	q := newQuotaCalc(src)
	q.SetRuleLimit("r1", 500)
	q.Compute()
	if !q.QuotasFor("dead").IsEmpty() {
		t.Errorf("inactive node got quotas: %#v", q.QuotasFor("dead"))
	}
}

// TestSyncQuota_Compute_Staleness — when staleness > 0, nodes whose
// ReportTime is too old are dropped from active.
func TestSyncQuota_Compute_Staleness(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	fresh := newActiveSyncNode("fresh")
	stale := newActiveSyncNode("stale")
	stale.ReportTime = time.Unix(0, 0).Add(-1 * time.Hour)
	src.set("fresh", fresh)
	src.set("stale", stale)
	q := newQuotaCalc(src)
	q.staleness = 30 * time.Second
	q.now = func() time.Time { return time.Unix(0, 0).UTC() }
	// fresh node has ReportTime = wallclock now (real now), but our test
	// clock returns epoch — so fresh is "stale" by our test clock. Set
	// fresh.ReportTime to the test now.
	fresh.ReportTime = time.Unix(0, 0).UTC()
	q.SetRuleLimit("r1", 200)
	q.Compute()
	if q.QuotasFor("fresh").IsEmpty() {
		t.Errorf("fresh node was filtered out")
	}
	if !q.QuotasFor("stale").IsEmpty() {
		t.Errorf("stale node was admitted: %#v", q.QuotasFor("stale"))
	}
}

// TestSyncQuota_Compute_ScalesWithNodeCount — verify the division
// changes as the fleet grows (3 nodes → C/3; 4 nodes → C/4).
func TestSyncQuota_Compute_ScalesWithNodeCount(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	for _, addr := range []string{"n1", "n2", "n3"} {
		src.set(addr, newActiveSyncNode(addr))
	}
	q := newQuotaCalc(src)
	q.SetRuleLimit("rA", 400)
	q.Compute()
	if got := q.QuotasFor("n1").Rules["rA"]; !approxEqual(got, 400.0/3.0) {
		t.Errorf("3-node share = %v, want 400/3", got)
	}
	src.set("n4", newActiveSyncNode("n4"))
	q.Compute()
	if got := q.QuotasFor("n1").Rules["rA"]; !approxEqual(got, 100) {
		t.Errorf("4-node share = %v, want 100", got)
	}
}

// TestSyncQuota_Compute_MultipleRulesAndBackends — keys round-trip
// independently.
func TestSyncQuota_Compute_MultipleRulesAndBackends(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.set("n1", newActiveSyncNode("n1"))
	src.set("n2", newActiveSyncNode("n2"))
	q := newQuotaCalc(src)
	q.SetRuleLimit("r1", 200)
	q.SetRuleLimit("r2", 800)
	q.SetBackendLimit("s3|a|r", 600)
	q.SetBackendLimit("cfs||", 400)
	q.Compute()
	nq := q.QuotasFor("n1")
	if !approxEqual(nq.Rules["r1"], 100) || !approxEqual(nq.Rules["r2"], 400) {
		t.Errorf("rules = %#v", nq.Rules)
	}
	if !approxEqual(nq.Backends["s3|a|r"], 300) || !approxEqual(nq.Backends["cfs||"], 200) {
		t.Errorf("backends = %#v", nq.Backends)
	}
}

// TestSyncQuota_QuotasFor_UnknownAddr — unknown addr returns zero value.
func TestSyncQuota_QuotasFor_UnknownAddr(t *testing.T) {
	t.Parallel()
	q := newQuotaCalc(newStubSource())
	if !q.QuotasFor("nobody").IsEmpty() {
		t.Errorf("unknown addr returned non-empty quotas")
	}
}

// TestSyncQuota_Snapshot_Defensive — Snapshot returns a copy.
func TestSyncQuota_Snapshot_Defensive(t *testing.T) {
	t.Parallel()
	q := newQuotaCalc(newStubSource())
	q.SetRuleLimit("r1", 100)
	rules, _ := q.Snapshot()
	rules["r1"] = 999
	rules2, _ := q.Snapshot()
	if rules2["r1"] != 100 {
		t.Errorf("Snapshot mutable: %v", rules2["r1"])
	}
}

// -----------------------------------------------------------------------
// Concurrency
// -----------------------------------------------------------------------

// TestSyncQuota_ConcurrentSetAndCompute — race-detector safety. Run with
// -race.
func TestSyncQuota_ConcurrentSetAndCompute(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	for i := 0; i < 4; i++ {
		addr := fmt.Sprintf("n%d", i)
		src.set(addr, newActiveSyncNode(addr))
	}
	q := newQuotaCalc(src)

	const iters = 200
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		i := i
		wg.Add(3)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				q.SetRuleLimit(fmt.Sprintf("r%d", i), float64(100+j%50))
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				q.SetBackendLimit(fmt.Sprintf("s3|ep%d|r", i), float64(200+j%50))
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_ = q.QuotasFor(fmt.Sprintf("n%d", i))
			}
		}()
	}
	wg.Add(1)
	var computes atomic.Int64
	go func() {
		defer wg.Done()
		for j := 0; j < iters; j++ {
			q.Compute()
			computes.Add(1)
		}
	}()
	wg.Wait()
	if computes.Load() == 0 {
		t.Errorf("Compute never ran")
	}
}

// TestSyncQuota_IsEmpty — sanity check the zero value.
func TestSyncQuota_IsEmpty(t *testing.T) {
	t.Parallel()
	var nq NodeQuotas
	if !nq.IsEmpty() {
		t.Errorf("zero NodeQuotas not empty")
	}
	nq.Rules = map[string]float64{"x": 1}
	if nq.IsEmpty() {
		t.Errorf("non-empty Rules said IsEmpty")
	}
}

// -----------------------------------------------------------------------
// P2 — perNode map reuse via generation counter
// -----------------------------------------------------------------------

// TestSyncQuotaCalculator_Compute_ReusesPerNodeMap is the P2 regression
// guard: the inner Rules / Backends map instances must survive across
// Compute() invocations when the active set is stable. Allocating fresh
// maps every 10s tick for 100 nodes × 50 rules is 5000+ entries per tick
// of avoidable GC churn.
//
// We compare reflect.ValueOf(map).Pointer() identities — equal pointers
// prove "same backing map".
func TestSyncQuotaCalculator_Compute_ReusesPerNodeMap(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	for _, addr := range []string{"n1", "n2", "n3"} {
		src.set(addr, newActiveSyncNode(addr))
	}
	q := newQuotaCalc(src)
	q.SetRuleLimit("ruleA", 300)
	q.SetBackendLimit("s3|ep|r", 600)

	// First compute populates the maps.
	q.Compute()
	first := make(map[string][2]uintptr, 3)
	q.mu.RLock()
	for _, addr := range []string{"n1", "n2", "n3"} {
		nq := q.perNode[addr]
		first[addr] = [2]uintptr{
			reflect.ValueOf(nq.Rules).Pointer(),
			reflect.ValueOf(nq.Backends).Pointer(),
		}
	}
	q.mu.RUnlock()

	// Second compute with the same active set must reuse the inner maps.
	q.Compute()
	q.mu.RLock()
	for _, addr := range []string{"n1", "n2", "n3"} {
		nq := q.perNode[addr]
		got := [2]uintptr{
			reflect.ValueOf(nq.Rules).Pointer(),
			reflect.ValueOf(nq.Backends).Pointer(),
		}
		if got != first[addr] {
			t.Errorf("%s: inner maps re-allocated across Compute() — "+
				"want pointer reuse (P2). first=%v second=%v",
				addr, first[addr], got)
		}
		// And the contents must still be correct.
		if !approxEqual(nq.Rules["ruleA"], 100) {
			t.Errorf("%s: Rules[ruleA] = %v, want 100", addr, nq.Rules["ruleA"])
		}
		if !approxEqual(nq.Backends["s3|ep|r"], 200) {
			t.Errorf("%s: Backends[s3|ep|r] = %v, want 200", addr, nq.Backends["s3|ep|r"])
		}
	}
	q.mu.RUnlock()
}

// TestSyncQuotaCalculator_Compute_DropsStaleNodes verifies the generation
// sweep removes entries for nodes that left the active set between
// compute invocations. We mark B / C inactive (rather than physically
// removing them from the source) — collectActive treats both as "not
// participating" so the sweep should drop their perNode entries either
// way.
func TestSyncQuotaCalculator_Compute_DropsStaleNodes(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.set("A", newActiveSyncNode("A"))
	snB := newActiveSyncNode("B")
	snC := newActiveSyncNode("C")
	src.set("B", snB)
	src.set("C", snC)
	q := newQuotaCalc(src)
	q.SetRuleLimit("r1", 300)
	q.Compute()

	// All three should be present.
	for _, addr := range []string{"A", "B", "C"} {
		if q.QuotasFor(addr).IsEmpty() {
			t.Fatalf("pre-shrink: %s missing", addr)
		}
	}

	// B + C leave the active set.
	snB.IsActive = false
	snC.IsActive = false
	q.Compute()

	if q.QuotasFor("A").IsEmpty() {
		t.Errorf("A should still have quotas")
	}
	if !q.QuotasFor("B").IsEmpty() {
		t.Errorf("B not swept: %#v", q.QuotasFor("B"))
	}
	if !q.QuotasFor("C").IsEmpty() {
		t.Errorf("C not swept: %#v", q.QuotasFor("C"))
	}

	// Internal-state check: perNode should only carry A; perNodeGen
	// should match.
	q.mu.RLock()
	if _, ok := q.perNode["B"]; ok {
		t.Errorf("perNode still holds B")
	}
	if _, ok := q.perNodeGen["C"]; ok {
		t.Errorf("perNodeGen still holds C")
	}
	q.mu.RUnlock()
}

// TestSyncQuotaCalculator_Compute_EmptyFleetSweepsAll verifies that when
// every node leaves the active set, perNode is fully cleared by the
// generation sweep (the pre-P2 implementation reassigned perNode to a
// fresh map).
func TestSyncQuotaCalculator_Compute_EmptyFleetSweepsAll(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	snA := newActiveSyncNode("A")
	src.set("A", snA)
	q := newQuotaCalc(src)
	q.SetRuleLimit("r1", 100)
	q.Compute()
	if q.QuotasFor("A").IsEmpty() {
		t.Fatalf("A missing pre-clear")
	}
	snA.IsActive = false
	q.Compute()
	if !q.QuotasFor("A").IsEmpty() {
		t.Errorf("A not cleared after fleet emptied: %#v", q.QuotasFor("A"))
	}
	q.mu.RLock()
	if len(q.perNode) != 0 {
		t.Errorf("perNode len = %d, want 0", len(q.perNode))
	}
	if len(q.perNodeGen) != 0 {
		t.Errorf("perNodeGen len = %d, want 0", len(q.perNodeGen))
	}
	q.mu.RUnlock()
}
