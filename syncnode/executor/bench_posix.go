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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// runBenchPosix runs a POSIX fio benchmark for a single shard.
// workDir is created under rule.MountPath and optionally removed on completion
// when rule.FIODefaults.CleanupAfterDone is true. shardTotal carries the
// cluster-wide shard count for S1.6 cross-shard barriers.
func runBenchPosix(ctx context.Context, rule *spec.BenchRule, taskID string, shardIdx, shardTotal int, pushIntervalSec int) (*spec.BenchShardResult, error) {
	result := &spec.BenchShardResult{
		ShardIdx:  shardIdx,
		Status:    "running",
		StartedAt: time.Now().UnixMilli(),
	}

	// Fan-out shard task IDs are "<parent>/<N>" — strip the slash so the
	// workdir / fio result file paths don't introduce a phantom subdir.
	safeID := strings.ReplaceAll(taskID, "/", "_")

	workDir := filepath.Join(rule.MountPath, fmt.Sprintf("bench-%s-shard-%d", safeID, shardIdx))
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("mkdir workDir: %v", err)
		result.DoneAt = time.Now().UnixMilli()
		return result, err
	}

	for stageIdx, stage := range rule.FIOStages {
		if ctx.Err() != nil {
			result.Status = "failed"
			result.Error = "context cancelled"
			break
		}
		if stage.Skip {
			result.Stages = append(result.Stages, spec.BenchStageResult{Name: stage.Name})
			continue
		}
		// rc8 #120: rule 级 CacheDrop 在每个 stage 进入前触发，按位置区分
		// "before_first"（首个 stage）/ "between"（后续 stage）。MaybeDropCaches
		// 内部根据 spec.BeforeFirstStage/BetweenStages 决定是否实际 drop；
		// CacheDrop=nil 或 Enabled=false 时整链路 no-op。失败仅写指标不阻断 stage。
		dropWhere := "between"
		if stageIdx == 0 {
			dropWhere = "before_first"
		}
		MaybeDropCaches(ctx, taskID, rule.CacheDrop, dropWhere)
		// S1.6: cross-shard barrier before each fio stage when requested.
		// Errors from waitForPeers are nil for timeouts (logged and
		// continues), only propagating ctx-cancel as a hard stop.
		shardID := strconv.Itoa(shardIdx)
		if err := waitForPeers(ctx, taskID, stage.Name, shardID, shardTotal, stage.Control); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("stage %q barrier: %v", stage.Name, err)
			result.DoneAt = time.Now().UnixMilli()
			return result, err
		}
		// S3.3: 当 stage.Mixed 非空时走混合负载路径，按 weight 把 Runtime
		// 分片成多次串行 fio 运行；否则保持单 BS/RW 路径完全不变。
		var (
			sr  *spec.BenchStageResult
			err error
		)
		if len(stage.Mixed) > 0 {
			sr, err = runFIOStageMixed(ctx, rule.FIODefaults, stage, workDir, taskID, shardIdx, pushIntervalSec)
		} else {
			sr, err = runFIOStage(ctx, rule.FIODefaults, stage, workDir, taskID, shardIdx, pushIntervalSec)
		}
		if err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("stage %q: %v", stage.Name, err)
			result.DoneAt = time.Now().UnixMilli()
			return result, err
		}
		result.Stages = append(result.Stages, *sr)
	}

	if result.Status == "running" {
		result.Status = "done"
	}
	result.DoneAt = time.Now().UnixMilli()

	if rule.FIODefaults.CleanupAfterDone {
		if err := os.RemoveAll(workDir); err != nil {
			log.LogWarnf("bench posix: cleanup workDir %q: %v", workDir, err)
		}
	}
	return result, nil
}

// runFIOStage invokes fio for a single stage, parses the JSON+ output, and
// returns a BenchStageResult. The result file is written to os.TempDir() and
// removed after parsing.
func runFIOStage(ctx context.Context, defaults spec.FIOConfig, stage spec.FIOStage, workDir, taskID string, shardIdx, pushIntervalSec int) (*spec.BenchStageResult, error) {
	safeID := strings.ReplaceAll(taskID, "/", "_")
	resultFile := filepath.Join(os.TempDir(), fmt.Sprintf("fio-%s-%d-%s.json", safeID, shardIdx, stage.Name))
	defer os.Remove(resultFile)

	// fio doesn't expose a per-op hook, so we only carry stage-level state
	// + (below) increment the error counter when fio itself fails. The
	// /metrics/bench histogram stays empty for fio paths — dashboards use
	// fio's own JSON percentiles via BenchLatencyResult.
	SetStageState(taskID, shardIdx, stage.Name, StageStateRunning)
	defer func() {
		// Final state is patched below on the failure paths; this is the
		// "we returned without error" fallback.
		SetStageState(taskID, shardIdx, stage.Name, StageStateDone)
	}()

	// rc8 #120: stage 主测量前的预热窗口。spec.Warmup 为 nil 或
	// DurationSeconds<=0 时 RunWarmup 直接返回。RunWarmup 无 error 返回，
	// 失败仅记 metrics 不阻断 stage —— FIO 子进程模型不支持"loopFn 每秒多次
	// 调用"的细粒度预热，loopFn 仅响应 ctx，让 RunWarmup 的 DurationSeconds +
	// TargetQPS 节拍生效（observeWarmupOp 打点）。真正 I/O 预热由 stage.Control
	// 的 ramp 字段承担。
	runStageWarmup(ctx, stage.Warmup, taskID, shardIdx, stage.Name)

	// 预热耗时不计入 stage DurationSec：t0 在 warmup 完成后起点。
	t0 := time.Now()

	si := pushIntervalSec
	if si <= 0 {
		si = 5
	}

	args := buildFIOArgs(defaults, stage, workDir)
	args = append(args,
		"--output-format=json+",
		"--output="+resultFile,
		"--status-interval="+strconv.Itoa(si),
	)

	cmd := exec.CommandContext(ctx, "fio", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
		IncErr(taskID, shardIdx, stage.Name, "fio", "other")
		// S3.4: 错误归因 metric。
		observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, "fio", ClassifyErr(err))
		return nil, fmt.Errorf("stdoutpipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
		IncErr(taskID, shardIdx, stage.Name, "fio", "other")
		// S3.4: 错误归因 metric。
		observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, "fio", ClassifyErr(err))
		return nil, fmt.Errorf("fio start: %w", err)
	}
	go drainFIOStdout(stdout, taskID, shardIdx, stage.Name)
	if err := cmd.Wait(); err != nil {
		SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
		IncErr(taskID, shardIdx, stage.Name, "fio", ClassifyError(err))
		// S3.4: 错误归因 metric。
		observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, "fio", ClassifyErr(err))
		return nil, fmt.Errorf("fio wait: %w", err)
	}

	sr, err := parseFIOResult(resultFile, stage.Name)
	if err != nil {
		// Propagate: fio finished with exit 0 but produced no JSON output
		// (typically because something else wrote/removed it, or the path
		// is unwritable). Returning a zero-stat success here used to mask
		// real failures — most notably racing goroutines from duplicate
		// master dispatches overwriting each other's results.
		SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
		IncErr(taskID, shardIdx, stage.Name, "fio", "other")
		// S3.4: 错误归因 metric。
		observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, "fio", ClassifyErr(err))
		log.LogWarnf("bench posix: parse fio result %q: %v", resultFile, err)
		return nil, fmt.Errorf("parse fio result %q: %w", resultFile, err)
	}
	sr.DurationSec = time.Since(t0).Seconds()
	return sr, nil
}

// buildFIOArgs constructs the fio argument list from defaults + stage overrides.
// IOEngine is sourced from defaults only — it is not overridable per stage.
func buildFIOArgs(defaults spec.FIOConfig, stage spec.FIOStage, workDir string) []string {
	ioengine := defaults.IOEngine
	if ioengine == "" {
		ioengine = "libaio"
	}
	iodepth := orDefaultInt(stage.IODepth, defaults.IODepth, 32)
	numjobs := orDefaultInt(stage.NumJobs, defaults.NumJobs, 1)
	size := orDefaultStr(stage.Size, defaults.Size, "1G")
	runtime := orDefaultInt(stage.Runtime, defaults.Runtime, 60)
	direct := orDefaultInt(stage.Direct, defaults.Direct, 1)
	bs := orDefaultStr(stage.BS, defaults.BS, "4k")

	// reuseFiles + sourceStage tell fio "this stage reads back files
	// written by an earlier stage". A common UX trap is configuring
	// sourceStage without flipping reuseFiles — fio then creates empty
	// files named after THIS stage and "reads" 54M empty ops in 30s with
	// zero bytes / zero latency (the symptom that prompted this fix).
	//
	// So if sourceStage is set we coerce reuse semantics on, regardless
	// of what the rule's reuseFiles bool says. Logging the auto-coercion
	// keeps the behaviour observable; the rule itself is not rewritten.
	reuseFiles := stage.ReuseFiles
	if stage.SourceStage != "" && !reuseFiles {
		log.LogWarnf("bench posix: stage %q has sourceStage=%q but reuseFiles=false — forcing reuse semantics so the read actually hits the previous stage's files", stage.Name, stage.SourceStage)
		reuseFiles = true
	}

	// When reuseFiles is true, fio reuses the files written by SourceStage by
	// using the same job name (which determines the filename pattern).
	jobName := stage.Name
	if reuseFiles && stage.SourceStage != "" {
		jobName = stage.SourceStage
	}

	args := []string{
		"--name=" + jobName,
		"--ioengine=" + ioengine,
		"--iodepth=" + strconv.Itoa(iodepth),
		"--numjobs=" + strconv.Itoa(numjobs),
		"--size=" + size,
		"--rw=" + stage.RW,
		"--bs=" + bs,
		"--direct=" + strconv.Itoa(direct),
		"--directory=" + workDir,
		"--group_reporting",
	}
	if runtime > 0 {
		args = append(args, "--runtime="+strconv.Itoa(runtime), "--time_based")
	}
	if !reuseFiles {
		args = append(args, "--create_on_open=1", "--fallocate=none")
	}
	if stage.RW == "randrw" && stage.RWMixRead > 0 {
		args = append(args, "--rwmixread="+strconv.Itoa(stage.RWMixRead))
	}
	if defaults.ExtraArgs != "" {
		args = append(args, strings.Fields(defaults.ExtraArgs)...)
	}
	return args
}

// orDefaultInt returns stageVal if positive, defaultVal if positive, else fallback.
func orDefaultInt(stageVal, defaultVal, fallback int) int {
	if stageVal > 0 {
		return stageVal
	}
	if defaultVal > 0 {
		return defaultVal
	}
	return fallback
}

// orDefaultStr returns stageVal if non-empty, defaultVal if non-empty, else fallback.
func orDefaultStr(stageVal, defaultVal, fallback string) string {
	if stageVal != "" {
		return stageVal
	}
	if defaultVal != "" {
		return defaultVal
	}
	return fallback
}

// drainFIOStdout reads fio's stdout (status-interval lines) and emits debug
// log lines. It must run in its own goroutine to prevent the pipe from
// blocking the fio process.
func drainFIOStdout(r io.Reader, taskID string, shardIdx int, stageName string) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			log.LogDebugf("fio[%s shard=%d stage=%s]: %s", taskID, shardIdx, stageName, string(buf[:n]))
		}
		if err != nil {
			return
		}
	}
}

// fioJSONResult is a minimal subset of the fio JSON+ output needed for
// computing throughput, IOPS and latency.
type fioJSONResult struct {
	Jobs []struct {
		JobName string      `json:"jobname"`
		Read    fioJobStats `json:"read"`
		Write   fioJobStats `json:"write"`
	} `json:"jobs"`
}

type fioJobStats struct {
	IOPS    float64 `json:"iops"`
	BWBytes int64   `json:"bw_bytes"`
	// lat_ns aggregates submission + completion latency, percentile usually
	// absent unless --lat_percentiles=1 is set. clat_ns is the completion
	// latency only and its percentile map is populated by fio by default —
	// we read percentiles from clat_ns and fall back to lat_ns if missing.
	LatNs struct {
		Mean       float64            `json:"mean"`
		Percentile map[string]float64 `json:"percentile"`
	} `json:"lat_ns"`
	ClatNs struct {
		Mean       float64            `json:"mean"`
		Percentile map[string]float64 `json:"percentile"`
	} `json:"clat_ns"`
	TotalIOs int64 `json:"total_ios"`
}

// parseFIOResult reads the fio JSON+ result file, extracts the last JSON
// object (fio may prepend status-interval snapshots), and builds a
// BenchStageResult.
func parseFIOResult(resultFile, stageName string) (*spec.BenchStageResult, error) {
	data, err := os.ReadFile(resultFile)
	if err != nil {
		return nil, fmt.Errorf("read result: %w", err)
	}
	// fio json+ may have multiple JSON objects concatenated; take the last one.
	last := lastJSONObject(data)
	var r fioJSONResult
	if err := json.Unmarshal(last, &r); err != nil {
		return nil, fmt.Errorf("unmarshal fio json: %w", err)
	}

	sr := &spec.BenchStageResult{Name: stageName}
	for _, j := range r.Jobs {
		sr.OpsPerSec += j.Read.IOPS + j.Write.IOPS
		sr.ThroughputMBs += float64(j.Read.BWBytes+j.Write.BWBytes) / (1024 * 1024)
		sr.TotalOps += j.Read.TotalIOs + j.Write.TotalIOs
		sr.TotalBytes += j.Read.BWBytes + j.Write.BWBytes

		// Use whichever side (read/write) has higher mean latency.
		latSrc := j.Read.LatNs
		clatSrc := j.Read.ClatNs
		if j.Write.LatNs.Mean > j.Read.LatNs.Mean {
			latSrc = j.Write.LatNs
			clatSrc = j.Write.ClatNs
		}
		sr.Latency.Mean = latSrc.Mean / 1000 // ns → µs
		// Prefer clat_ns.percentile (populated by fio by default) and fall
		// back to lat_ns.percentile if available (--lat_percentiles=1).
		pct := clatSrc.Percentile
		if len(pct) == 0 {
			pct = latSrc.Percentile
		}
		if v, ok := pct["50.000000"]; ok {
			sr.Latency.P50 = v / 1000
		}
		if v, ok := pct["95.000000"]; ok {
			sr.Latency.P95 = v / 1000
		}
		if v, ok := pct["99.000000"]; ok {
			sr.Latency.P99 = v / 1000
		}
		if v, ok := pct["99.900000"]; ok {
			sr.Latency.P999 = v / 1000
		}
	}
	return sr, nil
}

// lastJSONObject extracts the last complete top-level JSON object from data.
// fio json+ output may contain multiple JSON blobs (one per status-interval
// snapshot plus the final summary); the final one is the complete result.
func lastJSONObject(data []byte) []byte {
	depth := 0
	end := -1
	for i := len(data) - 1; i >= 0; i-- {
		switch data[i] {
		case '}':
			depth++
			if end == -1 {
				end = i
			}
		case '{':
			depth--
			if depth == 0 {
				return data[i : end+1]
			}
		}
	}
	return data
}

// ---------------------------------------------------------------------------
// S3.3 Mixed workload — append-only block. 混合 FIO 负载（单 stage 内多组件
// 按 weight 时间分片串行执行）。Mixed 为空时所有原路径不受影响。
// S3.4 应在本块末尾继续追加 `// S3.4 ... append-only block` 锚点。
// ---------------------------------------------------------------------------

// fioRunner 把 fio 子进程执行抽象成可替换接口，单元测试可注入 fake runner
// 避免真正 exec fio。生产路径默认走 execFioRunner（exec.CommandContext）。
//
// 入参：
//   - runtime: 本次 fio 运行的目标时长（秒），<=0 时不带 --runtime 限制。
//   - component: 当前混合负载组件，提供 Name/RW/BS/IODepth/NumJobs/Size。
//
// 返回：fio JSON 解析后的 BenchStageResult（Name=component.Name）+ error。
type fioRunner interface {
	run(ctx context.Context, defaults spec.FIOConfig, stage spec.FIOStage, component spec.FIOMixedComponent, runtime int, workDir, taskID string, shardIdx int) (*spec.BenchStageResult, error)
}

// fioRunnerImpl 走真实 exec.CommandContext("fio", ...)，由生产路径使用。
type fioRunnerImpl struct{}

func (fioRunnerImpl) run(ctx context.Context, defaults spec.FIOConfig, stage spec.FIOStage, component spec.FIOMixedComponent, runtime int, workDir, taskID string, shardIdx int) (*spec.BenchStageResult, error) {
	// 用 component 字段覆盖 stage 上的 RW/BS/IODepth/NumJobs/Size，然后复用
	// 既有 buildFIOArgs + parseFIOResult 路径，避免重复实现 fio 调用逻辑。
	mergedStage := stage
	if component.RW != "" {
		mergedStage.RW = component.RW
	}
	if component.BlockSize != "" {
		mergedStage.BS = component.BlockSize
	}
	if component.IODepth > 0 {
		mergedStage.IODepth = component.IODepth
	}
	if component.NumJobs > 0 {
		mergedStage.NumJobs = component.NumJobs
	}
	if component.Size != "" {
		mergedStage.Size = component.Size
	}
	mergedStage.Runtime = runtime
	// component.Name 作为 fio job name + 结果文件后缀，便于日志/调试区分组件。
	mergedStage.Name = stage.Name + "/" + component.Name

	safeID := strings.ReplaceAll(taskID, "/", "_")
	resultFile := filepath.Join(os.TempDir(), fmt.Sprintf("fio-%s-%d-%s-%s.json", safeID, shardIdx, stage.Name, component.Name))
	defer os.Remove(resultFile)

	args := buildFIOArgs(defaults, mergedStage, workDir)
	args = append(args,
		"--output-format=json+",
		"--output="+resultFile,
	)
	cmd := exec.CommandContext(ctx, "fio", args...)
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("fio component %q: %w", component.Name, err)
	}
	return parseFIOResult(resultFile, mergedStage.Name)
}

// defaultFioRunner 是生产路径的实际 runner；测试中通过 setFioRunner 替换。
var defaultFioRunner fioRunner = fioRunnerImpl{}

// setFioRunner 用于测试注入 fake runner；返回 restore 闭包供 t.Cleanup。
func setFioRunner(r fioRunner) func() {
	prev := defaultFioRunner
	defaultFioRunner = r
	return func() { defaultFioRunner = prev }
}

// runFIOStageMixed 实现 stage.Mixed 非空时的混合负载执行：按 weight 把
// stage.Runtime 拆给每个组件，串行运行；每段独立写入 class 维度 metrics。
// 聚合返回的 BenchStageResult 把各组件的 TotalOps / TotalBytes 累加，
// Latency 取最慢组件（保守 SLA 检查）。
func runFIOStageMixed(ctx context.Context, defaults spec.FIOConfig, stage spec.FIOStage, workDir, taskID string, shardIdx, _ int) (*spec.BenchStageResult, error) {
	SetStageState(taskID, shardIdx, stage.Name, StageStateRunning)
	// rc8 #120: 同 runFIOStage，先跑 warmup（spec.Warmup 为 nil 时立即返回）；
	// 预热耗时不计入 stage DurationSec。
	runStageWarmup(ctx, stage.Warmup, taskID, shardIdx, stage.Name)
	t0 := time.Now()

	totalWeight := 0
	for _, c := range stage.Mixed {
		if c.Weight > 0 {
			totalWeight += c.Weight
		}
	}
	if totalWeight <= 0 {
		SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
		IncErr(taskID, shardIdx, stage.Name, "fio_mixed", "other")
		// S3.4: 错误归因 metric（配置错误归到 other）。
		observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, "fio_mixed", "other")
		return nil, fmt.Errorf("stage %q: mixed total weight must be > 0", stage.Name)
	}

	// orDefaultInt 的 fallback 是 60，所以 totalRuntime 永远 > 0，无需再校验。
	totalRuntime := orDefaultInt(stage.Runtime, defaults.Runtime, 60)

	agg := &spec.BenchStageResult{Name: stage.Name}
	var maxMean float64

	for _, comp := range stage.Mixed {
		if ctx.Err() != nil {
			SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
			return nil, ctx.Err()
		}
		if comp.Weight <= 0 {
			continue
		}
		slice := totalRuntime * comp.Weight / totalWeight
		if slice <= 0 {
			slice = 1
		}

		sliceStart := time.Now()
		sr, err := defaultFioRunner.run(ctx, defaults, stage, comp, slice, workDir, taskID, shardIdx)
		if err != nil {
			SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
			IncErr(taskID, shardIdx, stage.Name, "fio_mixed/"+comp.Name, ClassifyError(err))
			// S3.4: 错误归因 metric。
			observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, "fio_mixed/"+comp.Name, ClassifyErr(err))
			log.LogWarnf("bench posix mixed: stage %q component %q: %v", stage.Name, comp.Name, err)
			return nil, fmt.Errorf("stage %q component %q: %w", stage.Name, comp.Name, err)
		}
		sliceDur := time.Since(sliceStart)

		// 累加到 stage 聚合结果。
		agg.TotalOps += sr.TotalOps
		agg.TotalBytes += sr.TotalBytes
		agg.OpsPerSec += sr.OpsPerSec
		agg.ThroughputMBs += sr.ThroughputMBs
		agg.Errors += sr.Errors
		if sr.Latency.Mean > maxMean {
			maxMean = sr.Latency.Mean
			agg.Latency = sr.Latency
		}

		// #121: 保留每个 component 的独立结果到 MixedComponents，dashboard
		// 终态结果页直接读这里展示 small / large 分项数据；stage 聚合的
		// Total* / Latency 仍由上面的累加得出，用于 SLA 与 headline tile。
		agg.MixedComponents = append(agg.MixedComponents, spec.BenchComponentResult{
			Name:          comp.Name,
			SizeClass:     comp.SizeClass.ClassLabel(),
			Weight:        comp.Weight,
			DurationSec:   sr.DurationSec,
			ThroughputMBs: sr.ThroughputMBs,
			OpsPerSec:     sr.OpsPerSec,
			TotalOps:      sr.TotalOps,
			TotalBytes:    sr.TotalBytes,
			Errors:        sr.Errors,
			Latency:       sr.Latency,
		})

		// 注：fio 子进程内部完成 I/O，syncnode 不感知每个 op。
		// per-component 数据通过 agg.MixedComponents（#121）走结构化 API，
		// 不再向 Prometheus emit 误导性的 op-level 样本（#122）。
		_ = sliceDur
	}

	agg.DurationSec = time.Since(t0).Seconds()
	SetStageState(taskID, shardIdx, stage.Name, StageStateDone)
	return agg, nil
}

// ---------------------------------------------------------------------------
// S3.4 error attribution — append-only hooks. observeErrorAttr 调用点已在
// fio 启动/wait/parse 失败分支以及 fio_mixed 配置/执行失败分支内联，向新的
// cubefs_bench_error_attr_total 指标写入错误归因。旧的 IncErr 调用保持不变。
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// rc8 #120 Warmup wiring — 共享 helper。Sprint 3.4 的 warmup_runner.go /
// cache_drop.go 之前没有任何 stage runner 引用，被 Go linker 死代码消除从二
// 进制中删除。本 helper 把 RunWarmup 接入到 FIO / S3 stage 入口，让符号被引
// 用从而留在二进制里；IOR / mdtest 路径 spec 上没有 stage.Warmup 字段，仅
// 接入 MaybeDropCaches。warmupRunner 提供测试注入点：测试可替换为 fake，
// 断言 stage 入口确实调用了预热（生产路径走默认 productionWarmupRunner，
// 直连 RunWarmup）。
// ---------------------------------------------------------------------------

// warmupRunner 抽象 stage 入口处的预热调用，便于测试断言 "warmup 被触发"。
// 生产路径走 productionWarmupRunner.run，内部转调 RunWarmup（包内函数，
// 引用本身就阻止 linker 死代码消除）。
type warmupRunner interface {
	run(ctx context.Context, taskID, shardID, stage string, sp *spec.WarmupSpec, loopFn func(ctx context.Context) error)
}

type productionWarmupRunner struct{}

func (productionWarmupRunner) run(ctx context.Context, taskID, shardID, stage string, sp *spec.WarmupSpec, loopFn func(ctx context.Context) error) {
	RunWarmup(ctx, taskID, shardID, stage, sp, loopFn)
}

// defaultWarmupRunner 是生产路径使用的 runner；测试通过 setWarmupRunner 替换。
var defaultWarmupRunner warmupRunner = productionWarmupRunner{}

// setWarmupRunner 在测试中替换默认 runner；返回的 cleanup 闭包供 t.Cleanup 使用。
func setWarmupRunner(r warmupRunner) func() {
	prev := defaultWarmupRunner
	defaultWarmupRunner = r
	return func() { defaultWarmupRunner = prev }
}

// runStageWarmup 在 stage 主测量前触发一次 RunWarmup。spec 为 nil 或
// DurationSeconds<=0 时立即返回（observeWarmupOp 不打点），与
// RunWarmup 自身的 no-op 语义一致。loopFn 设计为不做真实 I/O —— FIO/IOR/
// mdtest 子进程模型不适合"每秒多次 loop"的细粒度预热，让 RunWarmup 在
// DurationSeconds 窗口内按 TargetQPS 节拍打 observeWarmupOp 即可；真正
// 的预热 I/O 由 stage.Control 的 ramp 字段承担。S3 路径在 runObjStage 内
// 复用本 helper 但提供真实 backend op 的 loopFn（见 bench_s3.go）。
func runStageWarmup(ctx context.Context, sp *spec.WarmupSpec, taskID string, shardIdx int, stageName string) {
	if sp == nil || sp.DurationSeconds <= 0 {
		return
	}
	loop := func(ctx context.Context) error { return ctx.Err() }
	defaultWarmupRunner.run(ctx, taskID, strconv.Itoa(shardIdx), stageName, sp, loop)
}
