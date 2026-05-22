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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// fakeFioRunner 记录每个 component 的 run 调用：实际不执行 fio，只把
// component / runtime 入参攒到切片中，返回一个虚拟 BenchStageResult 让聚合
// 流程正常往下走。
type fakeFioRunner struct {
	mu    sync.Mutex
	calls []fakeFioCall
}

type fakeFioCall struct {
	component spec.FIOMixedComponent
	runtime   int
}

func (f *fakeFioRunner) run(_ context.Context, _ spec.FIOConfig, stage spec.FIOStage, component spec.FIOMixedComponent, runtime int, _ , _ string, _ int) (*spec.BenchStageResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeFioCall{component: component, runtime: runtime})
	// 不同 component 给不同 mean 让 runFIOStageMixed 能选出最慢组件做断言。
	return &spec.BenchStageResult{
		Name:       stage.Name + "/" + component.Name,
		TotalOps:   int64(component.Weight * 100),
		TotalBytes: int64(component.Weight * 1024),
		Latency:    spec.BenchLatencyResult{Mean: float64(component.Weight) * 1000.0},
	}, nil
}

func (f *fakeFioRunner) snapshot() []fakeFioCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeFioCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// TestRunFIOStageMixed_TimeSlicing_3to1: Mixed=[A(w=3), B(w=1)] + Runtime=40
// 必须把 A 分到 30s、B 分到 10s（误差 ±1s 由整除截断造成）。
func TestRunFIOStageMixed_TimeSlicing_3to1(t *testing.T) {
	fake := &fakeFioRunner{}
	restore := setFioRunner(fake)
	defer restore()

	stage := spec.FIOStage{
		Name:    "mix",
		Runtime: 40,
		Mixed: []spec.FIOMixedComponent{
			{Name: "small", SizeClass: spec.SizeClassSmall, Weight: 3, BlockSize: "4k", RW: "randread"},
			{Name: "large", SizeClass: spec.SizeClassLarge, Weight: 1, BlockSize: "16m", RW: "read"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sr, err := runFIOStageMixed(ctx, spec.FIOConfig{}, stage, t.TempDir(), "t1", 0, 0)
	if err != nil {
		t.Fatalf("runFIOStageMixed: %v", err)
	}
	calls := fake.snapshot()
	if len(calls) != 2 {
		t.Fatalf("want 2 fio runs, got %d", len(calls))
	}
	if calls[0].component.Name != "small" || calls[0].runtime != 30 {
		t.Errorf("small slice: got name=%q runtime=%d, want small/30", calls[0].component.Name, calls[0].runtime)
	}
	if calls[1].component.Name != "large" || calls[1].runtime != 10 {
		t.Errorf("large slice: got name=%q runtime=%d, want large/10", calls[1].component.Name, calls[1].runtime)
	}
	// 聚合：TotalOps = 3*100 + 1*100 = 400；TotalBytes = 3*1024 + 1*1024 = 4096
	if sr.TotalOps != 400 {
		t.Errorf("aggregated TotalOps = %d, want 400", sr.TotalOps)
	}
	if sr.TotalBytes != 4096 {
		t.Errorf("aggregated TotalBytes = %d, want 4096", sr.TotalBytes)
	}
	// Latency.Mean 取最慢组件：small w=3 -> mean=3000，large w=1 -> mean=1000，应取 3000。
	if sr.Latency.Mean != 3000.0 {
		t.Errorf("aggregated Latency.Mean = %f, want 3000 (slowest)", sr.Latency.Mean)
	}
}

// TestRunFIOStageMixed_EqualWeights: Weight=[1,1] + Runtime=20 → 每段 10s。
// 顺便覆盖：runtime 整除后即 totalRuntime / 2，无截断误差。
func TestRunFIOStageMixed_EqualWeights(t *testing.T) {
	fake := &fakeFioRunner{}
	defer setFioRunner(fake)()

	stage := spec.FIOStage{
		Name:    "even",
		Runtime: 20,
		Mixed: []spec.FIOMixedComponent{
			{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"},
			{Name: "b", Weight: 1, BlockSize: "1m", RW: "read"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := runFIOStageMixed(ctx, spec.FIOConfig{}, stage, t.TempDir(), "t2", 0, 0); err != nil {
		t.Fatalf("err: %v", err)
	}
	calls := fake.snapshot()
	if calls[0].runtime != 10 || calls[1].runtime != 10 {
		t.Errorf("equal weights should split 10/10, got %d/%d", calls[0].runtime, calls[1].runtime)
	}
}

// TestRunFIOStageMixed_RejectZeroWeight: 全 0 weight 必须报错而不是 NaN。
func TestRunFIOStageMixed_RejectZeroWeight(t *testing.T) {
	fake := &fakeFioRunner{}
	defer setFioRunner(fake)()

	stage := spec.FIOStage{
		Name:    "bad",
		Runtime: 10,
		Mixed: []spec.FIOMixedComponent{
			{Name: "a", Weight: 0, BlockSize: "4k", RW: "randread"},
		},
	}
	_, err := runFIOStageMixed(context.Background(), spec.FIOConfig{}, stage, t.TempDir(), "tx", 0, 0)
	if err == nil {
		t.Fatalf("zero total weight must return error")
	}
}

// TestRunFIOStageMixed_RuntimeFallbackToDefault: Runtime=0 时回落到默认 60s
// （orDefaultInt 的 fallback），单组件应拿到 60s 时间片，不报错。
func TestRunFIOStageMixed_RuntimeFallbackToDefault(t *testing.T) {
	fake := &fakeFioRunner{}
	defer setFioRunner(fake)()

	stage := spec.FIOStage{
		Name:    "rt-default",
		Runtime: 0, // 触发 fallback
		Mixed: []spec.FIOMixedComponent{
			{Name: "a", Weight: 1, BlockSize: "4k", RW: "randread"},
		},
	}
	if _, err := runFIOStageMixed(context.Background(), spec.FIOConfig{}, stage, t.TempDir(), "tx", 0, 0); err != nil {
		t.Fatalf("runtime=0 should fall back to default, got err: %v", err)
	}
	calls := fake.snapshot()
	if len(calls) != 1 || calls[0].runtime != 60 {
		t.Fatalf("expected single 60s slice from default, got %+v", calls)
	}
}
