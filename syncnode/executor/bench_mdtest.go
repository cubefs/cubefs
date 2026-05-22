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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// runBenchMdtest runs an mdtest metadata benchmark for a single shard via
// mpirun. rule.MountPath is the working directory root; each shard gets a
// dedicated subdirectory `bench-<taskID>-shard-<idx>` so concurrent shards
// don't trample each other's namespaces.
//
// mdtest emits per-operation rate lines (Directory creation / stat / removal,
// File creation / stat / read / removal, Tree creation / removal) that we
// parse and project into BenchStageResult{OpsPerSec, TotalOps, DurationSec}.
func runBenchMdtest(ctx context.Context, rule *spec.BenchRule, taskID string, shardIdx, shardTotal int, pushIntervalSec int) (*spec.BenchShardResult, error) {
	result := &spec.BenchShardResult{
		ShardIdx:  shardIdx,
		Status:    "running",
		StartedAt: time.Now().UnixMilli(),
	}

	if rule.MountPath == "" {
		result.Status = "failed"
		result.Error = "mdtest: rule.mountPath is empty"
		result.DoneAt = time.Now().UnixMilli()
		return result, errors.New(result.Error)
	}

	workDir := filepath.Join(rule.MountPath, fmt.Sprintf("bench-%s-shard-%d", strings.ReplaceAll(taskID, "/", "_"), shardIdx))
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("mkdir workDir: %v", err)
		result.DoneAt = time.Now().UnixMilli()
		return result, err
	}

	defaults := spec.MdtestConfig{}
	if rule.MdtestDefaults != nil {
		defaults = *rule.MdtestDefaults
	}

	for stageIdx, stage := range rule.MdtestStages {
		if ctx.Err() != nil {
			result.Status = "failed"
			result.Error = "context cancelled"
			break
		}
		if stage.Skip {
			result.Stages = append(result.Stages, spec.BenchStageResult{Name: stage.Name})
			continue
		}
		// rc8 #120: rule 级 CacheDrop 在每个 stage 进入前触发。MdtestStage 上没有
		// Warmup 字段（mdtest 自身的 iterations 已经覆盖了 warmup 语义），所以
		// mdtest 路径只接 cache_drop。MaybeDropCaches 按 spec 决定是否实际 drop。
		dropWhere := "between"
		if stageIdx == 0 {
			dropWhere = "before_first"
		}
		MaybeDropCaches(ctx, taskID, rule.CacheDrop, dropWhere)
		// S1.6: cross-shard barrier before mpirun starts; logged + skipped
		// on timeout, hard-failed only when ctx is cancelled.
		shardID := strconv.Itoa(shardIdx)
		if err := waitForPeers(ctx, taskID, stage.Name, shardID, shardTotal, stage.Control); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("stage %q barrier: %v", stage.Name, err)
			result.DoneAt = time.Now().UnixMilli()
			return result, err
		}
		SetStageState(taskID, shardIdx, stage.Name, StageStateRunning)
		stageResults, err := runMdtestStage(ctx, defaults, stage, workDir, taskID, shardIdx)
		if err != nil {
			SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
			IncErr(taskID, shardIdx, stage.Name, "mdtest", ClassifyError(err))
			// S3.4: 错误归因 metric。
			observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, "mdtest", ClassifyErr(err))
			result.Status = "failed"
			result.Error = fmt.Sprintf("stage %q: %v", stage.Name, err)
			result.DoneAt = time.Now().UnixMilli()
			return result, err
		}
		SetStageState(taskID, shardIdx, stage.Name, StageStateDone)
		result.Stages = append(result.Stages, stageResults...)
	}

	if result.Status == "running" {
		result.Status = "done"
	}
	result.DoneAt = time.Now().UnixMilli()

	_ = pushIntervalSec // reserved for future progress push parity with FIO path
	return result, nil
}

// runMdtestStage invokes `mpirun -n N mdtest -d <workDir> ...` for a single
// stage and returns one BenchStageResult per mdtest operation type that was
// reported (creation/stat/removal/read for files and directories, plus
// tree creation/removal). Stage name is prefixed onto each op so multiple
// stages don't collide in the result list.
func runMdtestStage(ctx context.Context, defaults spec.MdtestConfig, stage spec.MdtestStage, workDir, taskID string, shardIdx int) ([]spec.BenchStageResult, error) {
	t0 := time.Now()

	mpiBin := orStr(defaults.MpiBin, "mpirun")
	mdtestBin := orStr(defaults.MdtestBin, "mdtest")
	numTasks := orInt(stage.NumTasks, defaults.NumTasks, 1)

	args := []string{
		"-n", strconv.Itoa(numTasks),
		"--allow-run-as-root", // common in container deployments
		mdtestBin,
		"-d", workDir,
	}
	if stage.Iterations > 0 {
		args = append(args, "-i", strconv.Itoa(stage.Iterations))
	}
	if stage.NumItems > 0 {
		args = append(args, "-n", strconv.Itoa(stage.NumItems))
	}
	if stage.ItemsPerDir > 0 {
		args = append(args, "-I", strconv.Itoa(stage.ItemsPerDir))
	}
	if stage.Depth > 0 {
		args = append(args, "-z", strconv.Itoa(stage.Depth))
	}
	if stage.Branching > 0 {
		args = append(args, "-b", strconv.Itoa(stage.Branching))
	}
	if stage.WriteBytes > 0 {
		args = append(args, "-w", strconv.FormatInt(stage.WriteBytes, 10))
	}
	if stage.ReadBytes > 0 {
		args = append(args, "-e", strconv.FormatInt(stage.ReadBytes, 10))
	}
	if stage.OnlyFiles {
		args = append(args, "-F")
	}
	if stage.OnlyDirs {
		args = append(args, "-d") // mdtest -d is dir-only mode
	}
	if stage.UniqueDir {
		args = append(args, "-u")
	}
	if defaults.ExtraArgs != "" {
		args = append(args, strings.Fields(defaults.ExtraArgs)...)
	}
	if stage.ExtraArgs != "" {
		args = append(args, strings.Fields(stage.ExtraArgs)...)
	}

	cmd := exec.CommandContext(ctx, mpiBin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdoutpipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderrpipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("mpirun start: %w", err)
	}

	// Drain stdout, capture every line for parsing.
	var buf strings.Builder
	doneStdout := make(chan struct{})
	go func() {
		defer close(doneStdout)
		s := bufio.NewScanner(stdout)
		s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for s.Scan() {
			line := s.Text()
			buf.WriteString(line)
			buf.WriteByte('\n')
			log.LogDebugf("bench mdtest [task=%s shard=%d stage=%s] %s", taskID, shardIdx, stage.Name, line)
		}
	}()
	// Drain stderr separately; failures land here.
	go func() {
		_, _ = io.Copy(io.Discard, stderr)
	}()

	err = cmd.Wait()
	<-doneStdout
	dur := time.Since(t0).Seconds()
	if err != nil {
		return nil, fmt.Errorf("mpirun wait: %w (output tail: %s)", err, tail(buf.String(), 800))
	}

	ops := parseMdtestOutput(buf.String())
	if len(ops) == 0 {
		// At least produce a synthetic stage entry so the caller can see
		// the stage ran, even if the parser found no rate lines.
		return []spec.BenchStageResult{{Name: stage.Name + "/raw", DurationSec: dur}}, nil
	}
	out := make([]spec.BenchStageResult, 0, len(ops))
	for _, op := range ops {
		out = append(out, spec.BenchStageResult{
			Name:        stage.Name + "/" + op.name,
			DurationSec: dur,
			OpsPerSec:   op.mean,
		})
	}
	return out, nil
}

// mdtestOp is one parsed rate line (creation/stat/removal/read for File/Dir/Tree).
type mdtestOp struct {
	name           string
	max, min, mean float64
	stddev         float64
}

// parseMdtestOutput scans the mdtest summary block for "<Op>: max min mean stddev"
// lines. Format excerpt:
//
//	   Operation                      Max            Min           Mean        Std Dev
//	   ---------                      ---            ---           ----        -------
//	   Directory creation     :     2415.213       2415.213       2415.213          0.000
//	   Directory stat         :     7032.119       7032.119       7032.119          0.000
//	   ...
//
// Names are normalized to lower-kebab (e.g. "dir-create", "file-stat", "tree-removal").
func parseMdtestOutput(output string) []mdtestOp {
	// Group 1: human label until the colon, group 2-5: max/min/mean/stddev.
	re := regexp.MustCompile(`^\s*([A-Za-z][A-Za-z ]+?)\s*:\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s*$`)
	var out []mdtestOp
	for _, line := range strings.Split(output, "\n") {
		m := re.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name := normalizeMdtestOpName(m[1])
		if name == "" {
			continue
		}
		op := mdtestOp{name: name}
		op.max, _ = strconv.ParseFloat(m[2], 64)
		op.min, _ = strconv.ParseFloat(m[3], 64)
		op.mean, _ = strconv.ParseFloat(m[4], 64)
		op.stddev, _ = strconv.ParseFloat(m[5], 64)
		out = append(out, op)
	}
	return out
}

func normalizeMdtestOpName(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	s = strings.ReplaceAll(s, "directory", "dir")
	s = strings.ReplaceAll(s, "  ", " ")
	known := map[string]string{
		"dir creation":  "dir-create",
		"dir stat":      "dir-stat",
		"dir removal":   "dir-removal",
		"dir rename":    "dir-rename",
		"file creation": "file-create",
		"file stat":     "file-stat",
		"file read":     "file-read",
		"file removal":  "file-removal",
		"tree creation": "tree-create",
		"tree removal":  "tree-removal",
	}
	if v, ok := known[s]; ok {
		return v
	}
	return ""
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func orStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func orInt(over, def, fallback int) int {
	if over > 0 {
		return over
	}
	if def > 0 {
		return def
	}
	return fallback
}

// ---------------------------------------------------------------------------
// S3.4 error attribution — append-only hooks. observeErrorAttr 调用点已在
// mdtest stage 执行失败分支内联，向新的 cubefs_bench_error_attr_total 指标
// 写入错误归因。旧的 IncErr 调用保持不变。
// ---------------------------------------------------------------------------

