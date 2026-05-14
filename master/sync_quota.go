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
	"sync"
	"time"
)

// -----------------------------------------------------------------------
// SyncQuotaCalculator (Phase P1-8 + P1-9)
//
// Computes per-rule (layer 2) and per-backend (layer 4 cluster-wide)
// bandwidth quotas given:
//   - cluster-wide caps configured by the operator (set via SetRuleLimit
//     / SetBackendLimit) and
//   - the live syncnodes the dispatcher knows about.
//
// Compute() is called from the heartbeat-tick loop in cluster.go before
// the outgoing heartbeat tasks are built. Each tick it walks the active
// nodes, divides each cap by the count of active nodes, and publishes
// per-node maps. QuotasFor(addr) is then used by the heartbeat-reply
// path to attach the right map to each outgoing task.
//
// Algorithm — equal division across active syncnodes (§12.4.2):
//
//   For each rule with cluster cap C and N active syncnodes:
//     per_node_share = C / N
//
//   Same for each backend cap.
//
// Why equal division (instead of EWMA-weighted shares):
//
//   1. The wall-clock acceptance for P1-8 / P1-9 only requires ≤ cap +
//      10%. Equal division keeps the cluster total at exactly C across
//      the fleet (if all nodes run the rule), or strictly below C if
//      some nodes are idle, since each node's local layer-2 / layer-4
//      bucket enforces its share as a hard ceiling.
//   2. The syncnode-side heartbeat does NOT yet ship per-rule bandwidth
//      readings, so master can't weight shares by past usage. Treating
//      every active node as a potential participant in every capped rule
//      is the safe over-approximation: even if only one node actually
//      runs the rule its share (C / N) is strictly less than C, so the
//      cluster total can never exceed C.
//   3. Weighting / EWMA / borrowing-from-idle-nodes is a P2-M task.
//
// SyncQuotaCalculator owns its own mutex; safe for concurrent
// SetRuleLimit / SetBackendLimit / Compute / QuotasFor.
// -----------------------------------------------------------------------

// syncQuotaSource is the minimum interface SyncQuotaCalculator needs to
// read the syncnode fleet. Mirrors syncDispatcherSource so unit tests
// can inject a stub without standing up a full *Cluster.
type syncQuotaSource interface {
	rangeSyncNodes(func(addr string, sn *SyncNode) bool)
}

// quotaActiveStaleness mirrors dispatcherStaleness — a node whose last
// heartbeat is older than this is excluded from the active set. Kept
// separate so the two thresholds can evolve independently.
const quotaActiveStaleness = 30 * time.Second

// NodeQuotas is the per-node quota assignment computed by Compute().
// Rules and Backends are MB/s ceilings the syncnode applies via
// ratelimit.Registry.SetRuleLimit / SetBackendLimit. A zero / missing
// entry means "no cluster quota for this rule/backend" — the syncnode
// removes any existing bucket on a zero value.
type NodeQuotas struct {
	Rules    map[string]float64 // ruleID → MBps for THIS node
	Backends map[string]float64 // backendKey ("kind|endpoint|region") → MBps for THIS node
	Updated  time.Time
}

// IsEmpty reports whether the quotas carry no entries — a convenience for
// the cluster-side heartbeat builder to skip the proto fields entirely
// when nothing is configured.
func (q NodeQuotas) IsEmpty() bool {
	return len(q.Rules) == 0 && len(q.Backends) == 0
}

// SyncQuotaCalculator computes per-rule and per-backend cluster-wide
// bandwidth quotas and exposes per-node slices. One instance per Cluster;
// Compute is called from the heartbeat-tick loop in cluster.go.
type SyncQuotaCalculator struct {
	source syncQuotaSource

	// now is the wallclock function; tests override it.
	now func() time.Time

	// staleness threshold for "active" — defaults to quotaActiveStaleness;
	// tests override to 0 to ignore staleness entirely.
	staleness time.Duration

	mu sync.RWMutex
	// ruleLimits is the configured cluster-wide cap per rule, in MB/s.
	// Populated by the rule-management surface (operator sets
	// rule.aggregateBandwidthLimitMBps). For P1 the rule store lives on
	// the syncnode side; master receives a per-rule cap via a future
	// admin endpoint. Until that endpoint lands, callers (cluster.go +
	// tests) push caps in via SetRuleLimit.
	ruleLimits map[string]float64

	// backendLimits is the configured cluster-wide cap per backend, in
	// MB/s. Keyed by the same "kind|endpoint|region" string the syncnode
	// uses on the wire.
	backendLimits map[string]float64

	// perNode is the most-recent Compute() output, keyed by node addr.
	perNode map[string]NodeQuotas
}

// NewSyncQuotaCalculator constructs a calculator bound to the supplied
// Cluster. Compute() must be called by the heartbeat-tick goroutine to
// refresh the per-node maps.
func NewSyncQuotaCalculator(c *Cluster) *SyncQuotaCalculator {
	return newSyncQuotaCalculatorFromSource(&clusterSyncNodeSource{c: c})
}

// newSyncQuotaCalculatorFromSource is the testable constructor — accepts
// any source so unit tests can drive node lists deterministically.
func newSyncQuotaCalculatorFromSource(src syncQuotaSource) *SyncQuotaCalculator {
	return &SyncQuotaCalculator{
		source:        src,
		now:           time.Now,
		staleness:     quotaActiveStaleness,
		ruleLimits:    make(map[string]float64),
		backendLimits: make(map[string]float64),
		perNode:       make(map[string]NodeQuotas),
	}
}

// SetRuleLimit configures the cluster-wide cap for ruleID. mbps <= 0
// removes the cap entirely (subsequent Compute() emits zero shares so
// every node frees its rule bucket). Safe for concurrent calls; the
// effect is visible to the next Compute().
func (q *SyncQuotaCalculator) SetRuleLimit(ruleID string, mbps float64) {
	if ruleID == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if mbps <= 0 {
		delete(q.ruleLimits, ruleID)
		return
	}
	q.ruleLimits[ruleID] = mbps
}

// SetBackendLimit configures the cluster-wide cap for backendKey (which
// must be in "kind|endpoint|region" format — see
// ratelimit.BackendKey.String). mbps <= 0 removes the cap.
func (q *SyncQuotaCalculator) SetBackendLimit(backendKey string, mbps float64) {
	if backendKey == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if mbps <= 0 {
		delete(q.backendLimits, backendKey)
		return
	}
	q.backendLimits[backendKey] = mbps
}

// Compute walks the current syncNodes map, identifies the active set, and
// divides each rule / backend cap evenly across active nodes. Idempotent;
// safe to call from the heartbeat tick on every round (~10s).
//
// If there are no active nodes, perNode is cleared and the next
// QuotasFor() call returns a zero-value snapshot.
//
// Algorithm (equal division — see file header):
//
//   active = { sn ∈ syncNodes | sn.IsActive && now-sn.ReportTime <= staleness }
//   N = |active|
//   for each rule r with cap C_r:
//     for each n ∈ active:
//       perNode[n].Rules[r] = C_r / N
//   (same for backends)
func (q *SyncQuotaCalculator) Compute() {
	// Collect active node addrs first (so we hold q.mu only briefly).
	active := q.collectActive()
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	if len(active) == 0 {
		q.perNode = make(map[string]NodeQuotas)
		return
	}
	n := float64(len(active))
	out := make(map[string]NodeQuotas, len(active))
	for _, addr := range active {
		nq := NodeQuotas{
			Rules:    make(map[string]float64, len(q.ruleLimits)),
			Backends: make(map[string]float64, len(q.backendLimits)),
			Updated:  now,
		}
		for ruleID, limit := range q.ruleLimits {
			nq.Rules[ruleID] = limit / n
		}
		for key, limit := range q.backendLimits {
			nq.Backends[key] = limit / n
		}
		out[addr] = nq
	}
	q.perNode = out
}

// QuotasFor returns the most-recent NodeQuotas snapshot for addr. Returns
// the zero NodeQuotas (IsEmpty == true) when addr has no current quotas
// — either Compute() hasn't run, the node is inactive, or there are no
// caps configured. Cheap; intended to be called from the heartbeat-task
// builder on every tick.
func (q *SyncQuotaCalculator) QuotasFor(addr string) NodeQuotas {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.perNode[addr]
}

// Snapshot returns a defensive copy of the current cluster-wide caps for
// diagnostics / tests. Useful when an operator wants to confirm the
// configured cap matches what they set.
func (q *SyncQuotaCalculator) Snapshot() (rules, backends map[string]float64) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	rules = make(map[string]float64, len(q.ruleLimits))
	for k, v := range q.ruleLimits {
		rules[k] = v
	}
	backends = make(map[string]float64, len(q.backendLimits))
	for k, v := range q.backendLimits {
		backends[k] = v
	}
	return rules, backends
}

// collectActive walks the syncnode source under no lock of its own (the
// SyncNode has its own RLock for runtime fields) and returns the addrs
// of nodes that are IsActive AND not stale. Bolt-healthy is intentionally
// ignored: a syncnode that's degraded but still reachable still applies
// rate-limits locally, so excluding it would let the remaining nodes
// over-consume the cap.
func (q *SyncQuotaCalculator) collectActive() []string {
	q.mu.RLock()
	staleness := q.staleness
	now := q.now()
	q.mu.RUnlock()
	out := make([]string, 0, 8)
	q.source.rangeSyncNodes(func(addr string, sn *SyncNode) bool {
		sn.RLock()
		active := sn.IsActive
		rt := sn.ReportTime
		sn.RUnlock()
		if !active {
			return true
		}
		if staleness > 0 && now.Sub(rt) > staleness {
			return true
		}
		out = append(out, addr)
		return true
	})
	return out
}
