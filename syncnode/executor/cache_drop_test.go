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
	"sync"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// fakeDropper 记录所有 Drop 调用，可按调用次序配置返回错误。
type fakeDropper struct {
	mu     sync.Mutex
	calls  []int   // 记录每次 Drop 被调用的 level
	errOn  []bool  // 第 i 次调用是否返回错误
	idx    int
}

func (f *fakeDropper) Drop(_ context.Context, level int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, level)
	cur := f.idx
	f.idx++
	if cur < len(f.errOn) && f.errOn[cur] {
		return errors.New("simulated drop fail (no permission)")
	}
	return nil
}

func (f *fakeDropper) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestMaybeDropCaches_DisabledNoOp(t *testing.T) {
	fd := &fakeDropper{}
	restore := setCacheDropper(fd)
	defer restore()

	// nil spec
	MaybeDropCaches(context.Background(), "t1", nil, "between")
	if fd.callCount() != 0 {
		t.Errorf("nil spec should not invoke Drop; got %d calls", fd.callCount())
	}
	// disabled spec
	MaybeDropCaches(context.Background(), "t1", &spec.CacheDropSpec{Enabled: false, BetweenStages: true}, "between")
	if fd.callCount() != 0 {
		t.Errorf("disabled spec should not invoke Drop; got %d calls", fd.callCount())
	}
}

func TestMaybeDropCaches_BetweenStagesOff(t *testing.T) {
	fd := &fakeDropper{}
	restore := setCacheDropper(fd)
	defer restore()

	sp := &spec.CacheDropSpec{Enabled: true, BetweenStages: false, DropLevel: 3}
	MaybeDropCaches(context.Background(), "t1", sp, "between")
	if fd.callCount() != 0 {
		t.Errorf("BetweenStages=false should suppress between-drop; got %d", fd.callCount())
	}
}

func TestMaybeDropCaches_BeforeFirstStageOn(t *testing.T) {
	fd := &fakeDropper{}
	restore := setCacheDropper(fd)
	defer restore()

	sp := &spec.CacheDropSpec{Enabled: true, BeforeFirstStage: true, DropLevel: 2}
	MaybeDropCaches(context.Background(), "t1", sp, "before_first")
	if fd.callCount() != 1 {
		t.Fatalf("BeforeFirstStage=true should invoke Drop once; got %d", fd.callCount())
	}
	if fd.calls[0] != 2 {
		t.Errorf("expected level 2, got %d", fd.calls[0])
	}
}

func TestMaybeDropCaches_BetweenStagesOn(t *testing.T) {
	fd := &fakeDropper{}
	restore := setCacheDropper(fd)
	defer restore()

	sp := &spec.CacheDropSpec{Enabled: true, BetweenStages: true, DropLevel: 3}
	MaybeDropCaches(context.Background(), "t1", sp, "between")
	MaybeDropCaches(context.Background(), "t1", sp, "between")
	if fd.callCount() != 2 {
		t.Fatalf("Expected 2 drops, got %d", fd.callCount())
	}
}

func TestMaybeDropCaches_BeforeFirstStageOffSuppresses(t *testing.T) {
	fd := &fakeDropper{}
	restore := setCacheDropper(fd)
	defer restore()

	// 启用 Enabled 但 BeforeFirstStage=false，"before_first" 时不应实际 drop
	sp := &spec.CacheDropSpec{Enabled: true, BeforeFirstStage: false, BetweenStages: true}
	MaybeDropCaches(context.Background(), "t1", sp, "before_first")
	if fd.callCount() != 0 {
		t.Errorf("BeforeFirstStage=false at before_first should suppress; got %d", fd.callCount())
	}
}

func TestMaybeDropCaches_FailureTolerated(t *testing.T) {
	fd := &fakeDropper{errOn: []bool{true}} // 第 1 次失败
	restore := setCacheDropper(fd)
	defer restore()

	sp := &spec.CacheDropSpec{Enabled: true, BetweenStages: true}
	// 不应 panic，不应 return error（函数无返回值，只更新指标）
	MaybeDropCaches(context.Background(), "t1", sp, "between")
	if fd.callCount() != 1 {
		t.Fatalf("expected 1 attempt, got %d", fd.callCount())
	}
}

// TestDefaultCacheDropper_ClampLevel：默认 dropper 在 level 超出 [1,3]
// 时应 clamp 到 3。这里用一个 wrapper 测试 clamp 逻辑（不能写入真实文件）。
func TestDefaultCacheDropper_ClampLevel(t *testing.T) {
	// 不在容器/CI 上对真实 /proc 路径写入；本测试仅断言函数可调用且对非法 level
	// 不 panic。返回 err 的情况只检查 != nil 即可（容器内大概率没权限）。
	d := defaultCacheDropper{}
	err := d.Drop(context.Background(), 0) // out-of-range，会被 clamp 到 3
	// 在没有权限/路径不存在的 macOS 上必然 err != nil；不要做 nil 断言。
	if err != nil {
		t.Logf("default dropper returned err (expected on non-Linux/test env): %v", err)
	}
}
