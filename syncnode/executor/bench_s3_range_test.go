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

// runRangeStage 用预填的 keyRing 直接跑一个 get_range stage。
// 与默认 helper 不同点：keyRing 由调用方提供（必须非空，否则 worker 会
// 因为 ring 为空一直跳过、stage 无法形成有效操作计数）。
func runRangeStage(t *testing.T, stage spec.ObjStage, b *benchS3Backend, seedKeys []string) *spec.BenchStageResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keyRing := append([]string(nil), seedKeys...)
	var mu sync.Mutex
	sr, err := runObjStage(ctx, stage, b, "test-prefix/", &keyRing, &mu, "tt-range", 0, 0)
	if err != nil {
		t.Fatalf("runObjStage: %v", err)
	}
	return sr
}

// TestBenchS3_GetRange_TransparentOffsetSize: get_range 必须把 ObjOp.RangeOffset
// / RangeSize 透传给 backend.Get(ctx, key, off, size)。
func TestBenchS3_GetRange_TransparentOffsetSize(t *testing.T) {
	b := &benchS3Backend{}

	stage := spec.ObjStage{
		Name:       "wave-range",
		NumJobs:    1,
		NumObjects: 4, // stage 跑满 4 次 op 就停
		ObjectSize: spec.ObjSize{Fixed: 1024},
		Ops: []spec.ObjOp{
			{
				Type:        "get_range",
				Weight:      1,
				RangeOffset: 1 << 20, // 1 MiB
				RangeSize:   256 << 10, // 256 KiB
			},
		},
	}

	sr := runRangeStage(t, stage, b, []string{"k1", "k2", "k3", "k4"})

	gets := b.snapshotGets()
	if len(gets) == 0 {
		t.Fatalf("expected at least one Get, got 0")
	}
	for i, g := range gets {
		if g.off != 1<<20 {
			t.Errorf("get #%d: off want 1MiB(%d), got %d", i, 1<<20, g.off)
		}
		if g.size != 256<<10 {
			t.Errorf("get #%d: size want 256KiB(%d), got %d", i, 256<<10, g.size)
		}
	}
	if sr.TotalOps == 0 {
		t.Errorf("stage TotalOps should be > 0")
	}
}

// TestBenchS3_GetRange_FallbackToFullRead: 当 RangeOffset==0 且 RangeSize==0
// 时，行为应回落到全量读 —— backend.Get 收到的 off/size 也是 (0,0)。
// 这保证旧 rule 升级到 get_range 但忘填 range 字段时不会无声变成
// "读 0 字节"。
func TestBenchS3_GetRange_FallbackToFullRead(t *testing.T) {
	b := &benchS3Backend{}

	stage := spec.ObjStage{
		Name:       "wave-range-fallback",
		NumJobs:    1,
		NumObjects: 2,
		ObjectSize: spec.ObjSize{Fixed: 1024},
		Ops: []spec.ObjOp{
			{Type: "get_range", Weight: 1}, // 不设 RangeOffset/RangeSize
		},
	}

	_ = runRangeStage(t, stage, b, []string{"k1", "k2"})

	gets := b.snapshotGets()
	if len(gets) == 0 {
		t.Fatalf("expected at least one Get")
	}
	for i, g := range gets {
		if g.off != 0 || g.size != 0 {
			t.Errorf("get #%d: fallback want off=0/size=0, got off=%d/size=%d", i, g.off, g.size)
		}
	}
}
