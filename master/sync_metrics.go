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

import "github.com/cubefs/cubefs/util/exporter"

// Phase 7 — Prometheus metric series for the P2 sync subsystem.
//
// Naming follows the existing master conventions (cfs_master_*). All
// series are constructed once via initSyncMetrics() invoked from
// Cluster construction so the metrics handle is non-nil from the
// first leader gain onwards.
//
// The exporter package exposes Gauge/GaugeVec (labeled gauge) and
// Counter (singleton counter; per-label increments use AddWithLabels).
// We model "counter with labels" via a fresh Counter per metric name
// plus the per-call labels map.

const (
	MetricSyncRuleTotal         = "cfs_master_syncrule_total"           // GaugeVec  labels: state
	MetricSyncRuleDispatchTotal = "cfs_master_syncrule_dispatch_total"  // Counter   labels: rule,strategy
	MetricSyncRuleDispatchFail  = "cfs_master_syncrule_dispatch_fail"   // Counter   labels: rule,reason
	MetricSyncRuleShardDispatch = "cfs_master_syncrule_shard_dispatch"  // Counter   labels: rule
	MetricSyncRuleAutoProbeFail = "cfs_master_syncrule_auto_probe_fail" // Counter   labels: rule,reason
	MetricSyncNodeState         = "cfs_master_syncnode_state"           // GaugeVec  labels: addr,state
	MetricSyncNodeDrainTotal    = "cfs_master_syncnode_drain_total"     // Counter   labels: addr,result
)

// syncMetricBundle holds the handles created at init time. Nil-safe —
// every record* helper checks before touching the handle so tests / early
// init paths don't panic.
type syncMetricBundle struct {
	ruleTotal     *exporter.GaugeVec
	dispatchTotal *exporter.Counter
	dispatchFail  *exporter.Counter
	shardDispatch *exporter.Counter
	autoProbeFail *exporter.Counter
	nodeState     *exporter.GaugeVec
	drainTotal    *exporter.Counter
}

// syncMetrics is the package-level bundle. Initialised exactly once via
// initSyncMetrics (idempotent).
var syncMetrics *syncMetricBundle

// initSyncMetrics constructs the metric bundle. Idempotent — re-calling
// returns the existing bundle.
func initSyncMetrics() *syncMetricBundle {
	if syncMetrics != nil {
		return syncMetrics
	}
	syncMetrics = &syncMetricBundle{
		ruleTotal:     exporter.NewGaugeVec(MetricSyncRuleTotal, "", []string{"state"}),
		dispatchTotal: exporter.NewCounter(MetricSyncRuleDispatchTotal),
		dispatchFail:  exporter.NewCounter(MetricSyncRuleDispatchFail),
		shardDispatch: exporter.NewCounter(MetricSyncRuleShardDispatch),
		autoProbeFail: exporter.NewCounter(MetricSyncRuleAutoProbeFail),
		nodeState:     exporter.NewGaugeVec(MetricSyncNodeState, "", []string{"addr", "state"}),
		drainTotal:    exporter.NewCounter(MetricSyncNodeDrainTotal),
	}
	return syncMetrics
}

// recordSyncDispatchSuccess increments the per-rule dispatch counter.
// Nil-safe.
func recordSyncDispatchSuccess(ruleID, strategy string) {
	if syncMetrics == nil || syncMetrics.dispatchTotal == nil {
		return
	}
	if strategy == "" {
		strategy = "hash"
	}
	syncMetrics.dispatchTotal.AddWithLabels(1, map[string]string{
		"rule":     ruleID,
		"strategy": strategy,
	})
}

// recordSyncDispatchFail bumps the failure counter with a human-readable
// reason (no_candidates, dispatch_n_err, etc.).
func recordSyncDispatchFail(ruleID, reason string) {
	if syncMetrics == nil || syncMetrics.dispatchFail == nil {
		return
	}
	syncMetrics.dispatchFail.AddWithLabels(1, map[string]string{
		"rule":   ruleID,
		"reason": reason,
	})
}

// recordSyncShardDispatch increments per shard dispatched in a fan-out.
// Multi-counter — caller calls once per shard.
func recordSyncShardDispatch(ruleID string) {
	if syncMetrics == nil || syncMetrics.shardDispatch == nil {
		return
	}
	syncMetrics.shardDispatch.AddWithLabels(1, map[string]string{"rule": ruleID})
}

// recordSyncAutoProbeFail increments the auto-prefix probe failure
// counter. reason ∈ {no_candidates, probe_err, decode_err, empty_reply}.
func recordSyncAutoProbeFail(ruleID, reason string) {
	if syncMetrics == nil || syncMetrics.autoProbeFail == nil {
		return
	}
	syncMetrics.autoProbeFail.AddWithLabels(1, map[string]string{
		"rule":   ruleID,
		"reason": reason,
	})
}

// setSyncNodeStateGauge sets the per-node state gauge: 1 for the
// reported state, 0 for the other(s). Operators alert on draining for
// too long via `cfs_master_syncnode_state{state="draining"} > 0 [10m]`.
func setSyncNodeStateGauge(addr string, state SyncNodeState) {
	if syncMetrics == nil || syncMetrics.nodeState == nil {
		return
	}
	for _, candidate := range []SyncNodeState{SyncNodeStateActive, SyncNodeStateDraining} {
		v := 0.0
		if candidate == state {
			v = 1.0
		}
		syncMetrics.nodeState.SetWithLabelValues(v, addr, string(candidate))
	}
}

// recordSyncDrainResult bumps the drain counter with a result label:
// success / partial / failed.
func recordSyncDrainResult(addr, result string) {
	if syncMetrics == nil || syncMetrics.drainTotal == nil {
		return
	}
	syncMetrics.drainTotal.AddWithLabels(1, map[string]string{
		"addr":   addr,
		"result": result,
	})
}
