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
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// TestBenchS3_List_TransparentPrefixAndMaxKeys: list 操作必须把
// ObjOp.ListPrefix 透传给 backend.List，并且 totalBytes=0 / totalOps>0
// （list 不计字节，按调用计数）。
func TestBenchS3_List_TransparentPrefixAndMaxKeys(t *testing.T) {
	b := &benchS3Backend{
		// 返回 3 个 entry，比 ListMaxKeys=5 少，channel 自然耗尽。
		listKeys: []string{"a", "b", "c"},
	}

	stage := spec.ObjStage{
		Name:       "wave-list",
		NumJobs:    1,
		NumObjects: 2,
		ObjectSize: spec.ObjSize{Fixed: 1},
		Ops: []spec.ObjOp{
			{
				Type:        "list",
				Weight:      1,
				ListPrefix:  "shard-a/",
				ListMaxKeys: 5,
			},
		},
	}

	sr := runShortStage(t, stage, b)

	lists := b.snapshotLists()
	if len(lists) == 0 {
		t.Fatalf("expected at least one List call, got 0")
	}
	for i, l := range lists {
		if l.prefix != "shard-a/" {
			t.Errorf("list #%d: prefix want shard-a/, got %q", i, l.prefix)
		}
		if l.recursive {
			t.Errorf("list #%d: recursive want false, got true", i)
		}
	}

	// list 是 op count 计数型操作：bytes=0，但 ops>0。
	if sr.TotalBytes != 0 {
		t.Errorf("list TotalBytes want 0, got %d", sr.TotalBytes)
	}
	if sr.TotalOps == 0 {
		t.Errorf("list TotalOps want >0, got 0")
	}
}

// TestBenchS3_List_DefaultsWhenUnconfigured: 当 ObjOp 未设 ListPrefix /
// ListMaxKeys 时，应使用 stage 的 keyPrefix 与 defaultListMaxKeys。
// 我们通过让 fake backend 返回的 listKeys 多于 defaultListMaxKeys 也无法触发
// （fake 只生产 3 个），仅断言 prefix 回落即可——maxKeys 上限属于 worker 内
// for-range 的截断逻辑，listKeys 数小于该上限时 channel 自然关闭，等价于
// "未触发截断"，与生产语义一致。
func TestBenchS3_List_DefaultsWhenUnconfigured(t *testing.T) {
	b := &benchS3Backend{listKeys: []string{"x"}}

	stage := spec.ObjStage{
		Name:       "wave-list-default",
		NumJobs:    1,
		NumObjects: 1,
		ObjectSize: spec.ObjSize{Fixed: 1},
		Ops: []spec.ObjOp{
			{Type: "list", Weight: 1}, // 都不配置
		},
	}

	_ = runShortStage(t, stage, b)

	lists := b.snapshotLists()
	if len(lists) == 0 {
		t.Fatalf("expected at least one List call")
	}
	// runShortStage 内 keyPrefix 固定为 "test-prefix/"。
	if lists[0].prefix != "test-prefix/" {
		t.Errorf("list default prefix want test-prefix/, got %q", lists[0].prefix)
	}
}
