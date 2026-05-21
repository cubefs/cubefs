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
// when rule.FIODefaults.CleanupAfterDone is true.
func runBenchPosix(ctx context.Context, rule *spec.BenchRule, taskID string, shardIdx int, pushIntervalSec int) (*spec.BenchShardResult, error) {
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

	for _, stage := range rule.FIOStages {
		if ctx.Err() != nil {
			result.Status = "failed"
			result.Error = "context cancelled"
			break
		}
		if stage.Skip {
			result.Stages = append(result.Stages, spec.BenchStageResult{Name: stage.Name})
			continue
		}
		sr, err := runFIOStage(ctx, rule.FIODefaults, stage, workDir, taskID, shardIdx, pushIntervalSec)
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
	t0 := time.Now()
	safeID := strings.ReplaceAll(taskID, "/", "_")
	resultFile := filepath.Join(os.TempDir(), fmt.Sprintf("fio-%s-%d-%s.json", safeID, shardIdx, stage.Name))
	defer os.Remove(resultFile)

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
		return nil, fmt.Errorf("stdoutpipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("fio start: %w", err)
	}
	go drainFIOStdout(stdout, taskID, shardIdx, stage.Name)
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("fio wait: %w", err)
	}

	sr, err := parseFIOResult(resultFile, stage.Name)
	if err != nil {
		// Propagate: fio finished with exit 0 but produced no JSON output
		// (typically because something else wrote/removed it, or the path
		// is unwritable). Returning a zero-stat success here used to mask
		// real failures — most notably racing goroutines from duplicate
		// master dispatches overwriting each other's results.
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
