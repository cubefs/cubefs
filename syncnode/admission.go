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

package syncnode

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/loadutil"
	"github.com/cubefs/cubefs/util/log"
)

// 缺陷3: syncnode 接活前的资源准入控制
//
// 目标：在 syncnode 真正把 BenchRule 交给 executor 之前，估算这条 rule 在
// 本节点的内存峰值，与当前 cgroup limit 对照；若估算 + 当前 RSS 会越过
// admissionMemBudget × limit，则直接拒绝接活，避免 OOMKilled 把整个
// syncnode pod 拖死、连带把其他正在跑的任务也带走。
//
// 设计取舍：
//
//   - 估算只是 best-effort 上界，宁可偏保守也不要让任务跑起来再 OOM。
//     POSIX/FIO 用 sum(BS × IODepth × NumJobs) × 1.5 + 256 MiB；
//     S3/SDK 用 NumJobs × maxObjectSize × 2；mdtest / ior 给保守常数。
//
//   - admissionMemBudget = 0.7：留 30% 给 syncnode 自身、bench-tools
//     sidecar、page cache 等无法精确估算的部分；test-hb 12 GiB 节点上
//     允许大约 8.4 GiB 给单个 bench 任务，仍能装下当前 24-iodepth × 4-job
//     × 1 MiB BS = ~140 MiB 的常规 workload。
//
//   - 读 cgroup 限额失败时 fail-open：直接放行，不阻塞已经在跑的环境。
//     这是 admission 的最终防线，不是首要安全闸门——master 选 owner 时
//     已经看过节点心跳里的内存使用率。
//
//   - 当前 RSS 用 cgroup current 而不是 /proc/self/status，确保 sidecar
//     (cubefs-bench-tools) 等同 pod 内进程的占用也算进来。
const admissionMemBudget = 0.70

// estimateBenchMemoryBytes 返回这条 rule 在本节点（一个 shard）预计的内存峰值。
// 上界估算，保守优先；只用于 admission 决策，不参与计费/可观测。
func estimateBenchMemoryBytes(rule *spec.BenchRule) uint64 {
	if rule == nil {
		return 0
	}
	const baseline = uint64(256) << 20 // 256 MiB，bench-tools + go runtime + 容差

	switch rule.StorageType {
	case spec.BenchStoragePosix:
		return estimateFIOMemoryBytes(rule) + baseline
	case spec.BenchStorageS3, spec.BenchStorageSDK:
		return estimateObjMemoryBytes(rule) + baseline
	case spec.BenchStorageMdtest:
		// mdtest 单进程内存固定 ~32 MiB，主要开销是 mpirun NumTasks 个
		// 子进程。conservative: 64 MiB × NumTasks，无 NumTasks 时取 8。
		n := 8
		if rule.MdtestDefaults != nil && rule.MdtestDefaults.NumTasks > 0 {
			n = rule.MdtestDefaults.NumTasks
		}
		for _, st := range rule.MdtestStages {
			if st.NumTasks > n {
				n = st.NumTasks
			}
		}
		return uint64(n)*(uint64(64)<<20) + baseline
	case spec.BenchStorageIOR:
		// IOR/mdtest 经由 sidecar；常见 ior 默认 NumTasks 较小，给 1 GiB 保底。
		return (uint64(1) << 30) + baseline
	default:
		// 未知类型保守按 1 GiB 估算，避免漏挡。
		return (uint64(1) << 30) + baseline
	}
}

// estimateFIOMemoryBytes 仅估算 fio 自身的 IO buffer 占用——
// 每个 fio job 在 libaio 引擎下需要 IODepth × BS 的预分配缓冲，
// NumJobs 把这个量再放大一倍；多 stage 串行执行，取 stage 间峰值即可。
func estimateFIOMemoryBytes(rule *spec.BenchRule) uint64 {
	def := rule.FIODefaults
	var peak uint64
	for _, st := range rule.FIOStages {
		if st.Skip {
			continue
		}
		bsStr := st.BS
		if bsStr == "" {
			bsStr = def.BS
		}
		bs := parseSizeBytes(bsStr)
		if bs == 0 {
			bs = 4 << 10 // 4 KiB fio 默认
		}
		iodepth := st.IODepth
		if iodepth <= 0 {
			iodepth = def.IODepth
		}
		if iodepth <= 0 {
			iodepth = 1
		}
		numjobs := st.NumJobs
		if numjobs <= 0 {
			numjobs = def.NumJobs
		}
		if numjobs <= 0 {
			numjobs = 1
		}
		stageBytes := uint64(bs) * uint64(iodepth) * uint64(numjobs)
		// libaio 双缓冲 + 内核 io_uring/aio 上下文开销，放大 1.5x
		stageBytes = stageBytes + stageBytes/2
		if stageBytes > peak {
			peak = stageBytes
		}
	}
	return peak
}

// estimateObjMemoryBytes 估算 S3 / SDK 任务的 buffer 占用。
// 每个 worker 同时持有一个 max-object 缓冲；NumJobs 是单 stage 的 worker 数。
func estimateObjMemoryBytes(rule *spec.BenchRule) uint64 {
	var peak uint64
	for _, st := range rule.Stages {
		jobs := st.NumJobs
		if jobs <= 0 {
			jobs = 1
		}
		maxObj := st.ObjectSize.Fixed
		if st.ObjectSize.Max > maxObj {
			maxObj = st.ObjectSize.Max
		}
		if maxObj <= 0 {
			maxObj = 4 << 20 // 4 MiB 兜底
		}
		// 双缓冲：一份在进 IO，一份在收 next。
		stageBytes := uint64(jobs) * uint64(maxObj) * 2
		if stageBytes > peak {
			peak = stageBytes
		}
	}
	return peak
}

// parseSizeBytes 解析 fio 风格的尺寸字符串："4k" / "1M" / "2g" / "1024"。
// 返回 0 表示解析失败，调用方走兜底逻辑。
func parseSizeBytes(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	mult := int64(1)
	last := s[len(s)-1]
	switch last {
	case 'k', 'K':
		mult = 1 << 10
		s = s[:len(s)-1]
	case 'm', 'M':
		mult = 1 << 20
		s = s[:len(s)-1]
	case 'g', 'G':
		mult = 1 << 30
		s = s[:len(s)-1]
	case 't', 'T':
		mult = 1 << 40
		s = s[:len(s)-1]
	}
	// fio 容忍 "4k" / "4K" / "4kb"，吃掉末尾的 b/B
	if len(s) > 0 && (s[len(s)-1] == 'b' || s[len(s)-1] == 'B') {
		s = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n * mult
}

// currentMemUsageBytes 返回本 pod 当前的内存使用（cgroup current）。
// 失败时返回 (0, err)；调用方可以选择 fail-open。
func currentMemUsageBytes() (uint64, error) {
	if usage, err := loadutil.GetContainerMemoryUsageBytes(); err == nil {
		return usage, nil
	} else {
		return 0, err
	}
}

// admitBenchTask 判定能否接活；ok=false 时 reason 给出拒绝理由（供 master /
// 操作员看）。fail-open 策略：cgroup 限额未知（=0 即未限）时直接放行。
//
// admissionDisabled 测试用钩子：true 时直接放行，绕过所有估算。
func admitBenchTask(rule *spec.BenchRule) (ok bool, reason string) {
	if admissionDisabledFromEnv() {
		return true, ""
	}
	if rule == nil {
		return true, ""
	}
	limit, err := loadutil.GetContainerMemoryLimitBytes()
	if err != nil {
		log.LogWarnf("syncnode admission: read cgroup mem limit: %v (fail-open)", err)
		return true, ""
	}
	if limit == 0 {
		// 未设上限（裸跑 / 物理机），不阻断。
		return true, ""
	}
	estimate := estimateBenchMemoryBytes(rule)
	current, err := currentMemUsageBytes()
	if err != nil {
		log.LogWarnf("syncnode admission: read cgroup mem usage: %v (fail-open)", err)
		return true, ""
	}
	budget := uint64(float64(limit) * admissionMemBudget)
	projected := current + estimate
	log.LogInfof("syncnode admission: rule=%q storage=%q current=%d estimate=%d projected=%d budget=%d limit=%d",
		rule.ID, rule.StorageType, current, estimate, projected, budget, limit)
	if projected > budget {
		reason = fmt.Sprintf(
			"projected mem %dMiB (current=%dMiB + estimate=%dMiB) exceeds budget %dMiB (=%.0f%% of cgroup limit %dMiB)",
			projected>>20, current>>20, estimate>>20, budget>>20, admissionMemBudget*100, limit>>20)
		return false, reason
	}
	return true, ""
}

// admissionDisabledFromEnv 让运维/测试可以临时关掉准入门。
// 设置 CUBEFS_SYNCNODE_ADMISSION_DISABLED=1 即生效。
func admissionDisabledFromEnv() bool {
	v := os.Getenv("CUBEFS_SYNCNODE_ADMISSION_DISABLED")
	return v == "1" || strings.EqualFold(v, "true")
}
