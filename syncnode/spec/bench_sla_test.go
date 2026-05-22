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
	"strings"
	"testing"
)

// helper: build a stage with the fields SLA actually inspects. Other
// fields stay zero; tests should not depend on them.
func mkStage(name string, p99us, p999us, mbps, ips float64, totalOps, errors int64) BenchStageResult {
	return BenchStageResult{
		Name:          name,
		ThroughputMBs: mbps,
		OpsPerSec:     ips,
		TotalOps:      totalOps,
		Errors:        errors,
		Latency: BenchLatencyResult{
			P99:  p99us,
			P999: p999us,
		},
	}
}

func TestEvaluateSLA_NilAndEmpty(t *testing.T) {
	cases := []struct {
		name   string
		rule   *BenchRule
		stages []BenchStageResult
	}{
		{"nil rule", nil, []BenchStageResult{mkStage("write", 0, 0, 0, 0, 0, 0)}},
		{"empty SLA", &BenchRule{}, []BenchStageResult{mkStage("write", 0, 0, 0, 0, 0, 0)}},
		{"empty SLA empty stages", &BenchRule{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := EvaluateSLA(tc.rule, tc.stages)
			if r == nil {
				t.Fatal("EvaluateSLA must never return nil")
			}
			if !r.Pass {
				t.Errorf("expected Pass=true, got false")
			}
			if len(r.Items) != 0 {
				t.Errorf("expected zero items, got %d", len(r.Items))
			}
		})
	}
}

func TestEvaluateSLA_SingleStageSinglePass(t *testing.T) {
	rule := &BenchRule{SLA: []BenchSLA{{
		P99MsMax:     10,
		P999MsMax:    50,
		BwMiBsMin:    100,
		IopsMin:      1000,
		ErrorRateMax: 0.01,
		AppliesTo:    "write",
	}}}
	// p99=5ms, p999=20ms, 200 MiB/s → 209.7 MB/s, 5000 iops, 0/5000 err.
	stages := []BenchStageResult{mkStage("write", 5000, 20000, 200*mibToMb, 5000, 5000, 0)}
	r := EvaluateSLA(rule, stages)
	if !r.Pass {
		t.Fatalf("expected pass, got %+v", r)
	}
	if len(r.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(r.Items))
	}
	if !r.Items[0].Pass || r.Items[0].Stage != "write" {
		t.Errorf("unexpected item: %+v", r.Items[0])
	}
}

func TestEvaluateSLA_FailEachReason(t *testing.T) {
	cases := []struct {
		name       string
		sla        BenchSLA
		stage      BenchStageResult
		needSubstr string
	}{
		{
			name:       "p99 exceeded",
			sla:        BenchSLA{P99MsMax: 5, AppliesTo: "write"},
			stage:      mkStage("write", 10000, 0, 0, 0, 0, 0),
			needSubstr: "p99 ",
		},
		{
			name:       "p999 exceeded",
			sla:        BenchSLA{P999MsMax: 5, AppliesTo: "write"},
			stage:      mkStage("write", 0, 9000, 0, 0, 0, 0),
			needSubstr: "p99.9 ",
		},
		{
			name:       "bw below",
			sla:        BenchSLA{BwMiBsMin: 100, AppliesTo: "write"},
			stage:      mkStage("write", 0, 0, 50*mibToMb, 0, 0, 0),
			needSubstr: "bandwidth ",
		},
		{
			name:       "iops below",
			sla:        BenchSLA{IopsMin: 1000, AppliesTo: "write"},
			stage:      mkStage("write", 0, 0, 0, 500, 0, 0),
			needSubstr: "iops ",
		},
		{
			name:       "errorRate exceeded",
			sla:        BenchSLA{ErrorRateMax: 0.01, AppliesTo: "write"},
			stage:      mkStage("write", 0, 0, 0, 0, 900, 100),
			needSubstr: "errorRate ",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rule := &BenchRule{SLA: []BenchSLA{tc.sla}}
			r := EvaluateSLA(rule, []BenchStageResult{tc.stage})
			if r.Pass {
				t.Fatalf("expected fail, got pass: %+v", r)
			}
			if len(r.Items) != 1 || r.Items[0].Pass {
				t.Fatalf("expected single failing item, got %+v", r.Items)
			}
			found := false
			for _, reason := range r.Items[0].Reasons {
				if strings.Contains(reason, tc.needSubstr) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected reason containing %q, got %v", tc.needSubstr, r.Items[0].Reasons)
			}
		})
	}
}

func TestEvaluateSLA_AppliesToWildcard(t *testing.T) {
	rule := &BenchRule{SLA: []BenchSLA{{
		P99MsMax:  10,
		AppliesTo: "write-*",
	}}}
	stages := []BenchStageResult{
		mkStage("write-1m", 5000, 0, 0, 0, 0, 0),
		mkStage("write-100m", 6000, 0, 0, 0, 0, 0),
		mkStage("read-1m", 999_999_999, 0, 0, 0, 0, 0), // should be excluded
	}
	r := EvaluateSLA(rule, stages)
	if !r.Pass {
		t.Fatalf("expected pass (only write-* checked), got %+v", r)
	}
	if len(r.Items) != 2 {
		t.Fatalf("expected 2 items (write-1m + write-100m), got %d: %+v", len(r.Items), r.Items)
	}
	for _, it := range r.Items {
		if it.Stage != "write-1m" && it.Stage != "write-100m" {
			t.Errorf("unexpected matched stage: %q", it.Stage)
		}
	}
}

func TestEvaluateSLA_AppliesToEmptyMatchesAll(t *testing.T) {
	rule := &BenchRule{SLA: []BenchSLA{{P99MsMax: 10}}} // AppliesTo empty
	stages := []BenchStageResult{
		mkStage("write", 5000, 0, 0, 0, 0, 0),
		mkStage("read", 6000, 0, 0, 0, 0, 0),
	}
	r := EvaluateSLA(rule, stages)
	if !r.Pass {
		t.Fatalf("expected pass, got %+v", r)
	}
	if len(r.Items) != 2 {
		t.Fatalf("expected 2 items (one per stage), got %d", len(r.Items))
	}
}

func TestEvaluateSLA_MultipleSLAsAnyFailFails(t *testing.T) {
	rule := &BenchRule{SLA: []BenchSLA{
		{P99MsMax: 10, AppliesTo: "write"},  // passes
		{IopsMin: 10000, AppliesTo: "read"}, // fails
	}}
	stages := []BenchStageResult{
		mkStage("write", 5000, 0, 0, 0, 0, 0),
		mkStage("read", 0, 0, 0, 100, 0, 0),
	}
	r := EvaluateSLA(rule, stages)
	if r.Pass {
		t.Fatalf("expected fail (read iops below), got pass: %+v", r)
	}
	// One item per SLA × matched stage = 1 + 1 = 2.
	if len(r.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(r.Items))
	}
	var failed int
	for _, it := range r.Items {
		if !it.Pass {
			failed++
		}
	}
	if failed != 1 {
		t.Errorf("expected exactly 1 failing item, got %d", failed)
	}
}

func TestEvaluateSLA_AppliesToNoMatch(t *testing.T) {
	rule := &BenchRule{SLA: []BenchSLA{{
		P99MsMax:  10,
		AppliesTo: "nonexistent",
	}}}
	stages := []BenchStageResult{mkStage("write", 5000, 0, 0, 0, 0, 0)}
	r := EvaluateSLA(rule, stages)
	if r.Pass {
		t.Fatalf("expected fail (no stage matched), got pass: %+v", r)
	}
	if len(r.Items) != 1 || r.Items[0].Pass || r.Items[0].Stage != "" {
		t.Fatalf("expected single failing item with empty Stage, got %+v", r.Items)
	}
	if len(r.Items[0].Reasons) == 0 ||
		!strings.Contains(r.Items[0].Reasons[0], "no stage matched") {
		t.Errorf("expected 'no stage matched' reason, got %v", r.Items[0].Reasons)
	}
}

func TestEvaluateSLA_NoStagesWithSLA(t *testing.T) {
	// Rule has SLA but task produced no stages (executor crash). Should
	// fail loudly via the stage-missing path so the dashboard surfaces
	// the gap.
	rule := &BenchRule{SLA: []BenchSLA{{P99MsMax: 10, AppliesTo: "write"}}}
	r := EvaluateSLA(rule, nil)
	if r.Pass {
		t.Fatalf("expected fail, got pass: %+v", r)
	}
	if len(r.Items) != 1 || r.Items[0].Pass {
		t.Fatalf("expected single failing item, got %+v", r.Items)
	}
}

func TestEvaluateSLA_BadGlobPatternFails(t *testing.T) {
	// Malformed glob ("[") returns no match — surface as failure rather
	// than silently passing all stages.
	rule := &BenchRule{SLA: []BenchSLA{{P99MsMax: 10, AppliesTo: "[invalid"}}}
	stages := []BenchStageResult{mkStage("write", 5000, 0, 0, 0, 0, 0)}
	r := EvaluateSLA(rule, stages)
	if r.Pass {
		t.Fatalf("expected fail on malformed glob, got pass: %+v", r)
	}
}

func TestAggregateStagesWorstCase(t *testing.T) {
	// Shard A: write p99=5ms, bw=300MB/s, 5000 ops 0 err.
	// Shard B: write p99=8ms, bw=100MB/s, 5000 ops 100 err.
	// Expected aggregate: write p99=8ms (max), bw=100MB/s (min),
	// totalOps=10000, errors=100.
	per := [][]BenchStageResult{
		{mkStage("write", 5000, 0, 300, 0, 5000, 0)},
		{mkStage("write", 8000, 0, 100, 0, 5000, 100)},
	}
	agg := AggregateStagesWorstCase(per)
	if len(agg) != 1 || agg[0].Name != "write" {
		t.Fatalf("expected 1 'write' stage, got %+v", agg)
	}
	w := agg[0]
	if w.Latency.P99 != 8000 {
		t.Errorf("p99: want 8000, got %v", w.Latency.P99)
	}
	if w.ThroughputMBs != 100 {
		t.Errorf("bw: want 100, got %v", w.ThroughputMBs)
	}
	if w.TotalOps != 10000 || w.Errors != 100 {
		t.Errorf("counts: want totalOps=10000 errors=100, got totalOps=%d errors=%d", w.TotalOps, w.Errors)
	}
}

func TestAggregateStagesWorstCase_PreservesOrder(t *testing.T) {
	per := [][]BenchStageResult{
		{mkStage("write", 0, 0, 100, 0, 0, 0), mkStage("read", 0, 0, 100, 0, 0, 0)},
		{mkStage("read", 0, 0, 200, 0, 0, 0), mkStage("write", 0, 0, 50, 0, 0, 0)},
	}
	agg := AggregateStagesWorstCase(per)
	if len(agg) != 2 {
		t.Fatalf("want 2 stages, got %d", len(agg))
	}
	if agg[0].Name != "write" || agg[1].Name != "read" {
		t.Errorf("order not preserved: %s,%s", agg[0].Name, agg[1].Name)
	}
}

func TestEvaluateSLA_FanoutWorstCaseFails(t *testing.T) {
	// End-to-end: rule expects p99 < 10ms; shard A passes (5ms), shard B
	// busts at 12ms. After worst-case aggregate, SLA should fail.
	rule := &BenchRule{SLA: []BenchSLA{{P99MsMax: 10, AppliesTo: "write"}}}
	per := [][]BenchStageResult{
		{mkStage("write", 5000, 0, 0, 0, 0, 0)},
		{mkStage("write", 12000, 0, 0, 0, 0, 0)},
	}
	agg := AggregateStagesWorstCase(per)
	r := EvaluateSLA(rule, agg)
	if r.Pass {
		t.Fatalf("expected fail (shard B busts p99), got %+v", r)
	}
}
