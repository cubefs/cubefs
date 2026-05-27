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
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// RunWarmup 在 stage 主测量前空跑 spec 描述的预热 workload。
// loopFn 每被调用一次就执行一次"代表性操作"（一次 PUT / 一次 randread / 一次 mdtest create）。
// loopFn 内部已带 errorAttr 上报；warmup 不调用 ObserveBenchOp，只调 observeWarmupOp。
//
// 行为约定：
//   - sp == nil 或 DurationSeconds <= 0 立即返回（no-op）
//   - loopFn == nil 立即返回（防御性，避免空 nil 解引用）
//   - TargetQPS > 0 时使用 rate.Limiter 限速；为 0 时尽力跑
//   - ctx Done 时立即停止；不返回 error（warmup 失败不应阻塞 stage）
func RunWarmup(
	ctx context.Context,
	taskID, shardID, stage string,
	sp *spec.WarmupSpec,
	loopFn func(ctx context.Context) error,
) {
	if sp == nil || sp.DurationSeconds <= 0 || loopFn == nil {
		return
	}
	deadline := time.Now().Add(time.Duration(sp.DurationSeconds) * time.Second)
	var limiter *rate.Limiter
	if sp.TargetQPS > 0 {
		limiter = rate.NewLimiter(rate.Limit(sp.TargetQPS), 1)
	}
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return
			}
		}
		if err := loopFn(ctx); err != nil {
			observeWarmupOp(taskID, shardID, stage, "fail")
		} else {
			observeWarmupOp(taskID, shardID, stage, "ok")
		}
	}
}
