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
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// evaluateBenchSLAForSingle scores a single-shard bench task against its
// rule's SLA list and stores the outcome on the ledger record. No-op
// when the rule cannot be located, has no SLA configured, or the task
// record has been evicted.
//
// Failure modes that DO emit a result:
//   - rule had SLA but task produced no stages (executor crash, cancel)
//     → fail every SLA with reason "no stages produced" so the dashboard
//     surfaces the gap instead of silently passing.
//
// Failure modes that do NOT emit a result (intentional silence):
//   - rule had no SLA configured (nil/empty SLA) — record stays nil so
//     the dashboard renders a "no SLA" badge instead of forcing pass.
//   - rule lookup failed (rule deleted between trigger and terminal) —
//     warn-log and leave SLAResult nil; we can't score what we can't
//     read.
func (c *Cluster) evaluateBenchSLAForSingle(taskID string) {
	if c.benchTaskLedger == nil || c.benchRuleStore == nil {
		return
	}
	rec := c.benchTaskLedger.Get(taskID)
	if rec == nil {
		return
	}
	rule, err := c.benchRuleStore.Get(rec.RuleID)
	if err != nil {
		log.LogWarnf("sn bench SLA: rule %s not found for task %s: %v", rec.RuleID, taskID, err)
		return
	}
	if len(rule.SLA) == 0 {
		return
	}
	var stages []spec.BenchStageResult
	if rec.BenchResult != nil {
		stages = rec.BenchResult.Stages
	}
	result := spec.EvaluateSLA(rule, stages)
	c.benchTaskLedger.SetSLAResult(taskID, result)
	log.LogInfof("sn bench SLA task=%s pass=%v items=%d", taskID, result.Pass, len(result.Items))
}

// evaluateBenchSLAForParent scores a fan-out bench task's parent record
// after all shards have reported terminal. It collapses each shard's
// per-stage results into one worst-case representative per stage name
// before scoring, so a single failing shard fails the whole task.
func (c *Cluster) evaluateBenchSLAForParent(parentID string) {
	if c.benchTaskLedger == nil || c.benchRuleStore == nil {
		return
	}
	rec := c.benchTaskLedger.Get(parentID)
	if rec == nil {
		return
	}
	rule, err := c.benchRuleStore.Get(rec.RuleID)
	if err != nil {
		log.LogWarnf("sn bench SLA: rule %s not found for parent task %s: %v", rec.RuleID, parentID, err)
		return
	}
	if len(rule.SLA) == 0 {
		return
	}
	perShard := make([][]spec.BenchStageResult, 0, len(rec.Shards))
	for _, sh := range rec.Shards {
		if len(sh.Stages) == 0 {
			continue
		}
		perShard = append(perShard, sh.Stages)
	}
	aggregated := spec.AggregateStagesWorstCase(perShard)
	result := spec.EvaluateSLA(rule, aggregated)
	c.benchTaskLedger.SetSLAResult(parentID, result)
	log.LogInfof("sn bench SLA parent=%s pass=%v items=%d shards=%d",
		parentID, result.Pass, len(result.Items), len(perShard))
}
