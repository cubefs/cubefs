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
	"fmt"
	"os"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// CacheDropper 抽象写 /proc/sys/vm/drop_caches。测试通过 setCacheDropper 注入。
type CacheDropper interface {
	Drop(ctx context.Context, level int) error
}

type defaultCacheDropper struct{}

func (defaultCacheDropper) Drop(_ context.Context, level int) error {
	if level < 1 || level > 3 {
		level = 3
	}
	return os.WriteFile("/proc/sys/vm/drop_caches", []byte(fmt.Sprintf("%d\n", level)), 0o644)
}

var cacheDropper CacheDropper = defaultCacheDropper{}

// setCacheDropper 在测试中替换默认 dropper。返回的 cleanup 闭包用于恢复。
func setCacheDropper(c CacheDropper) func() {
	prev := cacheDropper
	cacheDropper = c
	return func() { cacheDropper = prev }
}

// MaybeDropCaches 若 spec 启用了 cache drop 则执行；否则空跑。
// 失败仅记录指标，不中断 stage（容器没权限是常态）。
//
// where 取值约定：
//   - "between"      stage 之间触发；仅当 BetweenStages 为 true 时实际 drop
//   - "before_first" 首个 stage 之前触发；仅当 BeforeFirstStage 为 true 时实际 drop
func MaybeDropCaches(ctx context.Context, taskID string, sp *spec.CacheDropSpec, where string) {
	if sp == nil || !sp.Enabled {
		return
	}
	if where == "between" && !sp.BetweenStages {
		return
	}
	if where == "before_first" && !sp.BeforeFirstStage {
		return
	}
	err := cacheDropper.Drop(ctx, sp.DropLevel)
	observeCacheDrop(taskID, where, err)
}
