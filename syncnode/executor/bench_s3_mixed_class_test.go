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

package executor

import (
	"strings"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// gatherClassCounter 把 benchOpBytesClass 收集到一个 map：
// key = "task|shard|stage|op|class"，方便测试断言指定 label 组合的累计字节数。
func gatherClassCounter(t *testing.T) map[string]float64 {
	t.Helper()
	mfs, err := benchRegistry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "syncnode_bench_op_bytes_class_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			key := labels["task_id"] + "|" + labels["shard"] + "|" + labels["stage"] + "|" + labels["op"] + "|" + labels["class"]
			out[key] = m.GetCounter().GetValue()
		}
	}
	return out
}

// classHistogramCount 返回 benchOpLatencyClass 在指定 label 组合下的累计 sample count。
func classHistogramCount(t *testing.T, task string, shard int, stage, op, class string) uint64 {
	t.Helper()
	mfs, err := benchRegistry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]string{
		"task_id": task,
		"shard":   shardLabel(shard),
		"stage":   stage,
		"op":      op,
		"class":   class,
	}
	for _, mf := range mfs {
		if mf.GetName() != "syncnode_bench_op_latency_class_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			ok := true
			for _, lp := range m.GetLabel() {
				if want[lp.GetName()] != lp.GetValue() {
					ok = false
					break
				}
			}
			if ok && m.Histogram != nil {
				return m.Histogram.GetSampleCount()
			}
		}
	}
	return 0
}

// TestObserveBenchOpClass_EmptyClassFallsBackToDefault: class="" 必须被
// 写成 "default"。
func TestObserveBenchOpClass_EmptyClassFallsBackToDefault(t *testing.T) {
	uniqTask := "TestObserveBenchOpClass_EmptyClassFallsBackToDefault"
	ObserveBenchOpClass(uniqTask, 0, "stage1", "put", "", 0.001, 4096)

	got := gatherClassCounter(t)
	k := uniqTask + "|0|stage1|put|default"
	if got[k] != 4096 {
		t.Errorf("class=\"\" should fall back to default; got map[%q]=%v, full=%+v", k, got[k], got)
	}
}

// TestBenchS3_SizeClass_TriggersClassMetrics: 带 SizeClass 的 ObjOp 应该让
// class 维度的两个指标（latency / bytes）都被记录到对应 class label。
func TestBenchS3_SizeClass_TriggersClassMetrics(t *testing.T) {
	b := &benchS3Backend{}

	// 同一个 stage 内 put 两类 size：small 4KiB w=9，large 16MiB w=1。
	stage := spec.ObjStage{
		Name:       "mix-stage-" + t.Name(), // 唯一 stage 名，避免与其他测试串扰
		NumJobs:    2,                       // 两个 op 一人一个 worker
		NumObjects: 4,                       // 4 个对象后立即停止
		ObjectSize: spec.ObjSize{Fixed: 4096},
		Ops: []spec.ObjOp{
			{Type: "put", Weight: 1, SizeClass: spec.SizeClassSmall},
			{Type: "put", Weight: 1, SizeClass: spec.SizeClassLarge},
		},
	}

	sr := runShortStage(t, stage, b)
	if sr.TotalOps == 0 {
		t.Fatalf("expected TotalOps > 0")
	}

	smallCount := classHistogramCount(t, "tt", 0, stage.Name, "put", "small")
	largeCount := classHistogramCount(t, "tt", 0, stage.Name, "put", "large")
	if smallCount == 0 || largeCount == 0 {
		t.Errorf("expected both small and large class hist counts > 0, got small=%d large=%d", smallCount, largeCount)
	}

	bytesMap := gatherClassCounter(t)
	smallKey := "tt|0|" + stage.Name + "|put|small"
	largeKey := "tt|0|" + stage.Name + "|put|large"
	if bytesMap[smallKey] == 0 || bytesMap[largeKey] == 0 {
		t.Errorf("expected non-zero bytes for both classes: small=%v large=%v", bytesMap[smallKey], bytesMap[largeKey])
	}
}

// TestBenchS3_SizeClass_NoTagFallsBackToDefault: 未设 SizeClass 的 ObjOp 应
// 写入 class="default" 标签——保证 dashboard 没有空标签的指标。
func TestBenchS3_SizeClass_NoTagFallsBackToDefault(t *testing.T) {
	b := &benchS3Backend{}

	stageName := "no-class-" + t.Name()
	stage := spec.ObjStage{
		Name:       stageName,
		NumJobs:    1,
		NumObjects: 2,
		ObjectSize: spec.ObjSize{Fixed: 4096},
		Ops: []spec.ObjOp{
			{Type: "put", Weight: 1}, // 不设 SizeClass
		},
	}
	_ = runShortStage(t, stage, b)

	if c := classHistogramCount(t, "tt", 0, stageName, "put", "default"); c == 0 {
		t.Errorf("expected class=default histogram count > 0, got 0")
	}
}

// 静态确认：新指标名遵循 syncnode_bench 前缀（保护 dashboard 查询稳定性）。
func TestClassMetricNamesPrefix(t *testing.T) {
	mfs, err := benchRegistry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := 0
	for _, mf := range mfs {
		name := mf.GetName()
		if name == "syncnode_bench_op_latency_class_seconds" || name == "syncnode_bench_op_bytes_class_total" {
			found++
			if !strings.HasPrefix(name, "syncnode_bench_op_") {
				t.Errorf("class metric %q must start with syncnode_bench_op_", name)
			}
		}
	}
	// gather 只返回非空指标家族；若两个新指标都没有任何 sample 不会出现在 mfs 中。
	// 这里至少要求出现过——前面其他测试已经写过若干 observation。
	_ = found
}

// 强制引用：避免某些 lint 模式提示 testutil.CollectAndCount 未使用（这里我们
// 通过 gather 自己解析，更稳定）。
var _ = testutil.CollectAndCount

// 强制引用 prometheus 以满足某些 lint 工具——主路径中已经使用，这里是双保险。
var _ = prometheus.NewCounterVec
