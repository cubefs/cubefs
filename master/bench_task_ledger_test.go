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
	"strings"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// TestMergeShardStages_MixedComponents_AcrossShards 验证 #121 在 master 端的
// 透传：两个 shard 各自上报同名 stage 的 MixedComponents（small/large），
// mergeShardStages 必须按 component name 跨 shard 汇总（计数累加 / latency 取
// shard 平均 / duration 取最大），并保留 stage.Mixed 配置顺序。
func TestMergeShardStages_MixedComponents_AcrossShards(t *testing.T) {
	shards := []BenchShardResult{
		{
			ShardIdx: 0,
			Stages: []spec.BenchStageResult{
				{
					Name:          "mix",
					DurationSec:   10,
					ThroughputMBs: 200,
					OpsPerSec:     100,
					TotalOps:      1000,
					TotalBytes:    1 << 20,
					Errors:        2,
					Latency:       spec.BenchLatencyResult{Mean: 1500, P50: 1000, P95: 2000, P99: 2500},
					MixedComponents: []spec.BenchComponentResult{
						{
							Name: "small", SizeClass: "small", Weight: 3,
							DurationSec: 7, ThroughputMBs: 50, OpsPerSec: 40,
							TotalOps: 400, TotalBytes: 1 << 18, Errors: 1,
							Latency: spec.BenchLatencyResult{Mean: 800, P50: 600, P95: 1200, P99: 1500},
						},
						{
							Name: "large", SizeClass: "large", Weight: 1,
							DurationSec: 3, ThroughputMBs: 150, OpsPerSec: 60,
							TotalOps: 600, TotalBytes: 3 << 18, Errors: 1,
							Latency: spec.BenchLatencyResult{Mean: 2000, P50: 1800, P95: 2400, P99: 2800},
						},
					},
				},
			},
		},
		{
			ShardIdx: 1,
			Stages: []spec.BenchStageResult{
				{
					Name:          "mix",
					DurationSec:   12, // 跨 shard duration 取 max
					ThroughputMBs: 220,
					OpsPerSec:     110,
					TotalOps:      1100,
					TotalBytes:    1 << 20,
					Errors:        3,
					Latency:       spec.BenchLatencyResult{Mean: 1600, P50: 1100, P95: 2100, P99: 2600},
					MixedComponents: []spec.BenchComponentResult{
						{
							Name: "small", SizeClass: "small", Weight: 3,
							DurationSec: 9, ThroughputMBs: 60, OpsPerSec: 45,
							TotalOps: 450, TotalBytes: 1 << 18, Errors: 2,
							Latency: spec.BenchLatencyResult{Mean: 1000, P50: 700, P95: 1300, P99: 1700},
						},
						{
							Name: "large", SizeClass: "large", Weight: 1,
							DurationSec: 3, ThroughputMBs: 160, OpsPerSec: 65,
							TotalOps: 650, TotalBytes: 3 << 18, Errors: 1,
							Latency: spec.BenchLatencyResult{Mean: 2200, P50: 1900, P95: 2500, P99: 2900},
						},
					},
				},
			},
		},
	}

	out := mergeShardStages(shards)
	if len(out) != 1 {
		t.Fatalf("merge produced %d stages, want 1", len(out))
	}
	stg := out[0]
	if stg.Name != "mix" {
		t.Errorf("stage name = %q, want mix", stg.Name)
	}
	if stg.DurationSec != 12 {
		t.Errorf("stage duration = %f, want 12 (max across shards)", stg.DurationSec)
	}
	if len(stg.MixedComponents) != 2 {
		t.Fatalf("MixedComponents len = %d, want 2", len(stg.MixedComponents))
	}
	// 顺序：第一个 shard 贡献的顺序（small, large）。
	small := stg.MixedComponents[0]
	if small.Name != "small" || small.SizeClass != "small" || small.Weight != 3 {
		t.Errorf("MixedComponents[0] identity: name=%q class=%q weight=%d, want small/small/3",
			small.Name, small.SizeClass, small.Weight)
	}
	// 计数：跨 shard 累加。
	if small.TotalOps != 850 { // 400 + 450
		t.Errorf("small TotalOps = %d, want 850", small.TotalOps)
	}
	if small.Errors != 3 { // 1 + 2
		t.Errorf("small Errors = %d, want 3", small.Errors)
	}
	if small.ThroughputMBs != 110 { // 50 + 60
		t.Errorf("small Throughput = %f, want 110", small.ThroughputMBs)
	}
	if small.OpsPerSec != 85 { // 40 + 45
		t.Errorf("small OpsPerSec = %f, want 85", small.OpsPerSec)
	}
	// Duration: max(7, 9) = 9
	if small.DurationSec != 9 {
		t.Errorf("small DurationSec = %f, want 9 (max)", small.DurationSec)
	}
	// Latency: shard-average → (800+1000)/2 = 900
	if small.Latency.Mean != 900 {
		t.Errorf("small Latency.Mean = %f, want 900 (avg)", small.Latency.Mean)
	}
	if small.Latency.P50 != 650 { // (600+700)/2
		t.Errorf("small Latency.P50 = %f, want 650", small.Latency.P50)
	}

	large := stg.MixedComponents[1]
	if large.Name != "large" || large.SizeClass != "large" || large.Weight != 1 {
		t.Errorf("MixedComponents[1] identity: name=%q class=%q weight=%d, want large/large/1",
			large.Name, large.SizeClass, large.Weight)
	}
	if large.TotalOps != 1250 { // 600 + 650
		t.Errorf("large TotalOps = %d, want 1250", large.TotalOps)
	}
	// large duration: max(3, 3) = 3
	if large.DurationSec != 3 {
		t.Errorf("large DurationSec = %f, want 3", large.DurationSec)
	}
	// Latency.Mean: (2000+2200)/2 = 2100
	if large.Latency.Mean != 2100 {
		t.Errorf("large Latency.Mean = %f, want 2100", large.Latency.Mean)
	}
}

// TestMergeShardStages_NoMixedComponents_OmitsField 验证 #121 在非 mixed stage
// 的非干扰性：上报 MixedComponents=nil 的 shard merge 完后，stg.MixedComponents
// 也必须保持 nil，dashboard 通过字段存在性判断是否渲染分项面板的逻辑才不会
// 被空切片误导。
func TestMergeShardStages_NoMixedComponents_OmitsField(t *testing.T) {
	shards := []BenchShardResult{
		{
			ShardIdx: 0,
			Stages: []spec.BenchStageResult{
				{Name: "plain", DurationSec: 5, TotalOps: 100},
			},
		},
	}
	out := mergeShardStages(shards)
	if len(out) != 1 {
		t.Fatalf("merge produced %d stages, want 1", len(out))
	}
	if out[0].MixedComponents != nil {
		t.Fatalf("plain stage should have nil MixedComponents, got %v", out[0].MixedComponents)
	}
}

// TestBenchTaskRecord_MixedComponents_JSONRoundTrip 验证 ledger 透传：把含
// MixedComponents 的 BenchShardResult 写入 BenchTaskRecord，json marshal/
// unmarshal 后字段必须无丢失。这条覆盖 API GET /benchTask/get 的 wire format
// 实质上是 BenchTaskRecord 整体 JSON encode，不存在白名单字段拷贝。
func TestBenchTaskRecord_MixedComponents_JSONRoundTrip(t *testing.T) {
	original := BenchTaskRecord{
		TaskID: "t-mixed-1",
		RuleID: "r-mixed",
		Status: BenchTaskStatusSucceeded,
		BenchResult: &spec.BenchShardResult{
			ShardIdx: 0,
			Status:   "done",
			Stages: []spec.BenchStageResult{
				{
					Name:        "mix",
					DurationSec: 10,
					TotalOps:    1000,
					MixedComponents: []spec.BenchComponentResult{
						{
							Name: "small", SizeClass: "small", Weight: 3,
							DurationSec: 7, TotalOps: 400, Errors: 1,
							Latency: spec.BenchLatencyResult{Mean: 800, P50: 600},
						},
						{
							Name: "large", SizeClass: "large", Weight: 1,
							DurationSec: 3, TotalOps: 600, Errors: 0,
							Latency: spec.BenchLatencyResult{Mean: 2000, P50: 1800},
						},
					},
				},
			},
		},
	}

	blob, err := json.Marshal(&original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded BenchTaskRecord
	if err := json.Unmarshal(blob, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.BenchResult == nil {
		t.Fatalf("BenchResult missing after roundtrip")
	}
	if len(decoded.BenchResult.Stages) != 1 {
		t.Fatalf("stages len = %d, want 1", len(decoded.BenchResult.Stages))
	}
	mc := decoded.BenchResult.Stages[0].MixedComponents
	if len(mc) != 2 {
		t.Fatalf("MixedComponents len = %d, want 2", len(mc))
	}
	if mc[0].Name != "small" || mc[0].SizeClass != "small" || mc[0].Weight != 3 ||
		mc[0].TotalOps != 400 || mc[0].Latency.Mean != 800 {
		t.Errorf("small component roundtrip mismatch: %+v", mc[0])
	}
	if mc[1].Name != "large" || mc[1].SizeClass != "large" || mc[1].Weight != 1 ||
		mc[1].TotalOps != 600 || mc[1].Latency.Mean != 2000 {
		t.Errorf("large component roundtrip mismatch: %+v", mc[1])
	}
}

// TestBenchTaskRecord_PlainStage_JSONOmitsMixedComponents 验证非 mixed stage
// 的 JSON payload 不出现 mixedComponents 字段——dashboard 通过字段存在性来
// 决定是否渲染分项面板，omitempty 在普通 stage 上必须生效。
func TestBenchTaskRecord_PlainStage_JSONOmitsMixedComponents(t *testing.T) {
	stg := spec.BenchStageResult{Name: "plain", TotalOps: 1}
	blob, err := json.Marshal(stg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(blob); strings.Contains(got, "mixedComponents") {
		t.Errorf("plain stage JSON must omit mixedComponents, got %s", got)
	}
}
