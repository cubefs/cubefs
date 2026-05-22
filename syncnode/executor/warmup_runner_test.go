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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

func TestRunWarmup_NilSpecNoOp(t *testing.T) {
	var calls int32
	loop := func(_ context.Context) error { atomic.AddInt32(&calls, 1); return nil }
	RunWarmup(context.Background(), "t", "0", "s", nil, loop)
	if atomic.LoadInt32(&calls) != 0 {
		t.Errorf("nil spec should not invoke loopFn; got %d", calls)
	}
}

func TestRunWarmup_ZeroDurationNoOp(t *testing.T) {
	var calls int32
	loop := func(_ context.Context) error { atomic.AddInt32(&calls, 1); return nil }
	sp := &spec.WarmupSpec{DurationSeconds: 0, TargetQPS: 10}
	RunWarmup(context.Background(), "t", "0", "s", sp, loop)
	if atomic.LoadInt32(&calls) != 0 {
		t.Errorf("DurationSeconds=0 should not invoke loopFn; got %d", calls)
	}
}

func TestRunWarmup_NilLoopFnNoOp(t *testing.T) {
	sp := &spec.WarmupSpec{DurationSeconds: 1}
	// 不应 panic
	RunWarmup(context.Background(), "t", "0", "s", sp, nil)
}

func TestRunWarmup_QPSLimited(t *testing.T) {
	var calls int32
	loop := func(_ context.Context) error { atomic.AddInt32(&calls, 1); return nil }

	sp := &spec.WarmupSpec{DurationSeconds: 1, TargetQPS: 10}
	start := time.Now()
	RunWarmup(context.Background(), "t", "0", "warm", sp, loop)
	elapsed := time.Since(start)

	n := atomic.LoadInt32(&calls)
	// 10 QPS × 1s = 10 ops。bucket 初始 1 token + 1s 内补充 10 token，所以理论 11。
	// 留充裕 [8, 14] 区间避免 CI 抖动。
	if n < 8 || n > 14 {
		t.Errorf("expected ~10 ops at 10 QPS × 1s, got %d (elapsed=%v)", n, elapsed)
	}
	if elapsed < 500*time.Millisecond {
		t.Errorf("expected RunWarmup to honor duration ~1s, returned in %v", elapsed)
	}
}

func TestRunWarmup_CtxCancelStops(t *testing.T) {
	var calls int32
	loop := func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	sp := &spec.WarmupSpec{DurationSeconds: 60, TargetQPS: 100}

	// 启动一个 goroutine 跑 RunWarmup，cancel 后应快速返回。
	done := make(chan struct{})
	go func() {
		RunWarmup(ctx, "t", "0", "warm", sp, loop)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatalf("RunWarmup did not exit promptly after ctx cancel")
	}
}

func TestRunWarmup_LoopErrCountsAsFail(t *testing.T) {
	// 没有直接读取 prometheus 计数的便利 API；这里只验证 loopFn 在出错时
	// 被持续调用（不会因 loopFn 返回 err 而提前退出）。
	var calls int32
	loop := func(_ context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("boom")
	}
	sp := &spec.WarmupSpec{DurationSeconds: 1, TargetQPS: 5}
	RunWarmup(context.Background(), "t", "0", "warm", sp, loop)

	n := atomic.LoadInt32(&calls)
	if n < 3 {
		t.Errorf("loopFn errors should not abort warmup; expected >=3 calls, got %d", n)
	}
}

func TestRunWarmup_UnlimitedRuns(t *testing.T) {
	var calls int32
	loop := func(_ context.Context) error {
		atomic.AddInt32(&calls, 1)
		// 慢一点避免热循环烫 CPU
		time.Sleep(2 * time.Millisecond)
		return nil
	}
	sp := &spec.WarmupSpec{DurationSeconds: 1, TargetQPS: 0} // 不限速
	RunWarmup(context.Background(), "t", "0", "warm", sp, loop)
	n := atomic.LoadInt32(&calls)
	if n < 50 {
		t.Errorf("unlimited warmup should call loopFn many times; got %d", n)
	}
}
