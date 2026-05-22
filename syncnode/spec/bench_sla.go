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

package spec

import (
	"fmt"
	"path"
)

// BenchLatencyResult percentiles are reported in microseconds; BenchSLA
// thresholds are in milliseconds for human ergonomics. Convert the SLA
// threshold once when comparing, never the measurements.
const microsPerMilli = 1000.0

// BenchStageResult.ThroughputMBs is decimal MB/s (1e6 B/s) for backward
// compatibility with the existing dashboard column; BenchSLA.BwMiBsMin is
// MiB/s (2^20 B/s) because storage SLAs are conventionally written in
// powers of two. This factor converts a MiB/s threshold into the MB/s
// units carried in BenchStageResult.
const mibToMb = 1.048576 // 1 MiB/s = 1.048576 MB/s

// EvaluateSLA checks every BenchSLA in rule against the supplied stages
// and returns the consolidated outcome. The result is never nil: when
// rule is nil, rule.SLA is empty, or stages is empty, the returned
// *BenchSLAResult has Pass=true and no Items — callers can rely on the
// non-nil contract and skip a separate length check.
//
// Caller contract:
//   - stages should already be aggregated to one entry per stage name.
//     Fan-out callers must collapse per-shard stages into a worst-case
//     summary before invoking EvaluateSLA, so each AppliesTo glob hits
//     each stage at most once and the result table stays one row per
//     (sla, stage) pair.
//   - Stage matching uses Go path.Match glob syntax ("*", "?", "[abc]").
//     AppliesTo == "" matches every stage in stages.
//   - A BenchSLA whose AppliesTo glob matches nothing emits exactly one
//     failing Item with Stage="" and Reasons carrying the missing-stage
//     message. Silently passing in that case would let typos hide real
//     misses.
//
// Pass semantics: every Item must pass for the overall result to pass;
// any single failing Item fails the whole BenchSLAResult.
func EvaluateSLA(rule *BenchRule, stages []BenchStageResult) *BenchSLAResult {
	res := &BenchSLAResult{Pass: true}
	if rule == nil || len(rule.SLA) == 0 {
		return res
	}
	for idx, sla := range rule.SLA {
		matched := false
		for _, stage := range stages {
			ok, _ := matchesAppliesTo(sla.AppliesTo, stage.Name)
			if !ok {
				continue
			}
			matched = true
			item := evaluateOneStage(idx, sla, stage)
			if !item.Pass {
				res.Pass = false
			}
			res.Items = append(res.Items, item)
		}
		if !matched {
			res.Pass = false
			res.Items = append(res.Items, BenchSLAItem{
				Index:     idx,
				AppliesTo: sla.AppliesTo,
				Pass:      false,
				Reasons:   []string{fmt.Sprintf("no stage matched appliesTo=%q", sla.AppliesTo)},
			})
		}
	}
	return res
}

// matchesAppliesTo applies the AppliesTo glob to a stage name. An empty
// pattern matches everything. Returns (match, err); err is non-nil only
// when the pattern itself is malformed. A malformed pattern is treated
// as "no match" so a typo in AppliesTo cannot silently pass every stage.
func matchesAppliesTo(pattern, stageName string) (bool, error) {
	if pattern == "" {
		return true, nil
	}
	ok, err := path.Match(pattern, stageName)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// evaluateOneStage checks a single BenchSLA × stage pair. Every non-zero
// constraint is evaluated; reasons accumulate so the dashboard can
// surface all failed dimensions at once rather than just the first.
func evaluateOneStage(idx int, sla BenchSLA, stage BenchStageResult) BenchSLAItem {
	item := BenchSLAItem{
		Index:     idx,
		AppliesTo: sla.AppliesTo,
		Stage:     stage.Name,
		Pass:      true,
	}
	if sla.P99MsMax > 0 {
		ms := stage.Latency.P99 / microsPerMilli
		if ms > sla.P99MsMax {
			item.Pass = false
			item.Reasons = append(item.Reasons,
				fmt.Sprintf("p99 %.3fms exceeds max %.3fms", ms, sla.P99MsMax))
		}
	}
	if sla.P999MsMax > 0 {
		ms := stage.Latency.P999 / microsPerMilli
		if ms > sla.P999MsMax {
			item.Pass = false
			item.Reasons = append(item.Reasons,
				fmt.Sprintf("p99.9 %.3fms exceeds max %.3fms", ms, sla.P999MsMax))
		}
	}
	if sla.BwMiBsMin > 0 {
		// stage.ThroughputMBs is in decimal MB/s; convert the MiB/s
		// floor into the same unit before comparing.
		thresholdMB := sla.BwMiBsMin * mibToMb
		if stage.ThroughputMBs < thresholdMB {
			item.Pass = false
			item.Reasons = append(item.Reasons,
				fmt.Sprintf("bandwidth %.3fMB/s below min %.3fMiB/s (%.3fMB/s)",
					stage.ThroughputMBs, sla.BwMiBsMin, thresholdMB))
		}
	}
	if sla.IopsMin > 0 {
		if stage.OpsPerSec < float64(sla.IopsMin) {
			item.Pass = false
			item.Reasons = append(item.Reasons,
				fmt.Sprintf("iops %.1f below min %d", stage.OpsPerSec, sla.IopsMin))
		}
	}
	if sla.ErrorRateMax > 0 {
		// errorRate = errors / (errors + totalOps). totalOps already
		// counts successful ops only (executor convention); add errors
		// back in for the denominator to get the true attempt count.
		attempts := stage.TotalOps + stage.Errors
		if attempts > 0 {
			rate := float64(stage.Errors) / float64(attempts)
			if rate > sla.ErrorRateMax {
				item.Pass = false
				item.Reasons = append(item.Reasons,
					fmt.Sprintf("errorRate %.4f exceeds max %.4f", rate, sla.ErrorRateMax))
			}
		}
	}
	return item
}

// AggregateStagesWorstCase collapses per-shard BenchStageResult lists into
// one entry per stage name suitable for EvaluateSLA. Within a stage name,
// each metric is taken as its worst observed value across shards:
//   - latency percentiles (p50/p95/p99/p99.9): max across shards
//   - throughput, opsPerSec: min across shards
//   - totalOps, totalBytes, errors: sum across shards
//   - durationSec: max across shards
//
// Rationale: SLA is "every shard must satisfy", so worst-case aggregation
// produces a single representative stage whose failure implies at least
// one shard failed. Order of returned stages preserves first-seen-name
// ordering across the input.
func AggregateStagesWorstCase(perShardStages [][]BenchStageResult) []BenchStageResult {
	type acc struct {
		idx   int
		stage BenchStageResult
	}
	byName := make(map[string]*acc)
	order := make([]string, 0)
	for _, shard := range perShardStages {
		for _, s := range shard {
			a, ok := byName[s.Name]
			if !ok {
				cp := s
				byName[s.Name] = &acc{idx: len(order), stage: cp}
				order = append(order, s.Name)
				continue
			}
			// latency: worst = max
			if s.Latency.P50 > a.stage.Latency.P50 {
				a.stage.Latency.P50 = s.Latency.P50
			}
			if s.Latency.P95 > a.stage.Latency.P95 {
				a.stage.Latency.P95 = s.Latency.P95
			}
			if s.Latency.P99 > a.stage.Latency.P99 {
				a.stage.Latency.P99 = s.Latency.P99
			}
			if s.Latency.P999 > a.stage.Latency.P999 {
				a.stage.Latency.P999 = s.Latency.P999
			}
			// mean: not strictly worst-case; keep the larger of the two
			// as a defensible representative.
			if s.Latency.Mean > a.stage.Latency.Mean {
				a.stage.Latency.Mean = s.Latency.Mean
			}
			// throughput / iops: worst = min
			if s.ThroughputMBs < a.stage.ThroughputMBs {
				a.stage.ThroughputMBs = s.ThroughputMBs
			}
			if s.OpsPerSec < a.stage.OpsPerSec {
				a.stage.OpsPerSec = s.OpsPerSec
			}
			// counts: sum (gives the true error rate denominator)
			a.stage.TotalOps += s.TotalOps
			a.stage.TotalBytes += s.TotalBytes
			a.stage.Errors += s.Errors
			// duration: worst = max
			if s.DurationSec > a.stage.DurationSec {
				a.stage.DurationSec = s.DurationSec
			}
		}
	}
	out := make([]BenchStageResult, 0, len(order))
	for _, name := range order {
		out = append(out, byName[name].stage)
	}
	return out
}
