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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// defaultSidecarEndpoint is where the cubefs-bench-tools sidecar listens
// inside the syncnode Pod. Pod-local loopback only.
const defaultSidecarEndpoint = "http://127.0.0.1:18000"

// iorRunRequest mirrors cmd/bench-tools-runner.runRequest. Duplicated here
// (vs imported) to keep the runner image free of cubefs dependencies.
type iorRunRequest struct {
	Tool       string   `json:"tool"`
	Args       []string `json:"args"`
	WorkDir    string   `json:"workdir"`
	UseMpi     bool     `json:"useMpi"`
	NumTasks   int      `json:"numTasks"`
	MpiBin     string   `json:"mpiBin"`
	TimeoutSec int      `json:"timeoutSec"`
}

// iorRunResponse mirrors cmd/bench-tools-runner.runResponse.
type iorRunResponse struct {
	ExitCode    int     `json:"exitCode"`
	Stdout      string  `json:"stdout"`
	Stderr      string  `json:"stderr"`
	DurationSec float64 `json:"durationSec"`
}

// runBenchIOR runs an IOR / mdtest benchmark for a single shard via the
// cubefs-bench-tools sidecar. Each stage POSTs to the sidecar's /run
// endpoint and parses the resulting summary JSON.
//
// rule.MountPath is the working directory root (shared with the sidecar
// via the bench-mount PVC); each shard gets a dedicated subdirectory
// `bench-<taskID>-shard-<idx>`.
func runBenchIOR(ctx context.Context, rule *spec.BenchRule, taskID string, shardIdx, shardTotal int, pushIntervalSec int) (*spec.BenchShardResult, error) {
	return runBenchIORWithClient(ctx, rule, taskID, shardIdx, shardTotal, pushIntervalSec, http.DefaultClient)
}

// runBenchIORWithClient is the testable form: callers (tests) inject an
// HTTP client so they can point at a httptest.Server.
func runBenchIORWithClient(ctx context.Context, rule *spec.BenchRule, taskID string, shardIdx, shardTotal int, pushIntervalSec int, client *http.Client) (*spec.BenchShardResult, error) {
	_ = pushIntervalSec // reserved for future progress streaming

	result := &spec.BenchShardResult{
		ShardIdx:  shardIdx,
		Status:    "running",
		StartedAt: time.Now().UnixMilli(),
	}

	if rule.MountPath == "" {
		result.Status = "failed"
		result.Error = "ior: rule.mountPath is empty"
		result.DoneAt = time.Now().UnixMilli()
		return result, errors.New(result.Error)
	}

	safeID := strings.ReplaceAll(taskID, "/", "_")
	workDir := filepath.Join(rule.MountPath, fmt.Sprintf("bench-%s-shard-%d", safeID, shardIdx))
	// mkdir locally: syncnode container and the sidecar share the same
	// hostPath / PVC, so the directory is visible to both.
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("mkdir workDir: %v", err)
		result.DoneAt = time.Now().UnixMilli()
		return result, err
	}

	defaults := spec.IORConfig{}
	if rule.IORDefaults != nil {
		defaults = *rule.IORDefaults
	}
	endpoint := rule.SidecarEndpoint
	if endpoint == "" {
		endpoint = defaultSidecarEndpoint
	}

	for _, stage := range rule.IORStages {
		if ctx.Err() != nil {
			result.Status = "failed"
			result.Error = "context cancelled"
			break
		}
		if stage.Skip {
			result.Stages = append(result.Stages, spec.BenchStageResult{Name: stage.Name})
			continue
		}
		shardID := strconv.Itoa(shardIdx)
		if err := waitForPeers(ctx, taskID, stage.Name, shardID, shardTotal, stage.Control); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("stage %q barrier: %v", stage.Name, err)
			result.DoneAt = time.Now().UnixMilli()
			return result, err
		}

		SetStageState(taskID, shardIdx, stage.Name, StageStateRunning)
		sr, err := runIORStage(ctx, client, endpoint, defaults, stage, workDir, taskID, shardIdx)
		if err != nil {
			SetStageState(taskID, shardIdx, stage.Name, StageStateFailed)
			IncErr(taskID, shardIdx, stage.Name, opLabelForIOR(stage.Tool), ClassifyError(err))
			// S3.4: 错误归因 metric。
			observeErrorAttr(taskID, shardLabel(shardIdx), stage.Name, opLabelForIOR(stage.Tool), ClassifyErr(err))
			result.Status = "failed"
			result.Error = fmt.Sprintf("stage %q: %v", stage.Name, err)
			result.DoneAt = time.Now().UnixMilli()
			return result, err
		}
		SetStageState(taskID, shardIdx, stage.Name, StageStateDone)
		result.Stages = append(result.Stages, *sr)
	}

	if result.Status == "running" {
		result.Status = "done"
	}
	result.DoneAt = time.Now().UnixMilli()
	return result, nil
}

// runIORStage drives a single IOR / mdtest stage through the sidecar HTTP
// API and translates the response into a BenchStageResult.
func runIORStage(ctx context.Context, client *http.Client, endpoint string, defaults spec.IORConfig, stage spec.IORStage, workDir, taskID string, shardIdx int) (*spec.BenchStageResult, error) {
	tool := strings.ToLower(strings.TrimSpace(stage.Tool))
	if tool == "" {
		tool = "ior"
	}
	if tool != "ior" && tool != "mdtest" {
		return nil, fmt.Errorf("unknown tool %q (want ior|mdtest)", stage.Tool)
	}

	args := buildIORArgs(defaults, stage, workDir, tool)

	useMpi := stage.UseMpi || defaults.UseMpi
	numTasks := stage.NumTasks
	if numTasks <= 0 {
		numTasks = defaults.NumTasks
	}
	mpiBin := defaults.MpiBin
	timeoutSec := stage.TimeoutSec
	if timeoutSec <= 0 {
		timeoutSec = defaults.TimeoutSec
	}

	req := iorRunRequest{
		Tool:       tool,
		Args:       args,
		WorkDir:    workDir,
		UseMpi:     useMpi,
		NumTasks:   numTasks,
		MpiBin:     mpiBin,
		TimeoutSec: timeoutSec,
	}

	t0 := time.Now()
	resp, err := postRun(ctx, client, endpoint, req)
	dur := time.Since(t0).Seconds()
	if err != nil {
		return nil, fmt.Errorf("sidecar /run: %w", err)
	}
	if resp.ExitCode != 0 {
		// Surface stderr tail for diagnostics; classify as "other" upstream.
		return nil, fmt.Errorf("%s exit=%d stderr=%s", tool, resp.ExitCode, tailString(resp.Stderr, 600))
	}

	sr := parseIORResult(resp.Stdout, stage.Name)
	// Prefer sidecar-measured duration (includes mpirun spawn) when
	// available; fall back to client-side wall clock.
	if resp.DurationSec > 0 {
		sr.DurationSec = resp.DurationSec
	} else {
		sr.DurationSec = dur
	}

	// Drop a debug line; mdtest / IOR summaries can be large.
	log.LogDebugf("bench ior [task=%s shard=%d stage=%s] exit=%d dur=%.2fs",
		taskID, shardIdx, stage.Name, resp.ExitCode, sr.DurationSec)
	return sr, nil
}

// buildIORArgs constructs the IOR / mdtest argument list from defaults +
// stage overrides. The executor always forces `-O summaryFormat=JSON` so
// the response can be parsed structurally; callers can pass `-o` / `-d` /
// runtime args via Args + Targets + Runtime.
func buildIORArgs(defaults spec.IORConfig, stage spec.IORStage, workDir, tool string) []string {
	args := make([]string, 0, len(stage.Args)+8)
	args = append(args, stage.Args...)

	// Targets — for ior `-o <path>`; for mdtest `-d <path>`. When multiple
	// targets are provided, ior repeats `-o`; mdtest takes a comma-joined list.
	switch tool {
	case "ior":
		for _, t := range stage.Targets {
			args = append(args, "-o", t)
		}
	case "mdtest":
		if len(stage.Targets) > 0 {
			args = append(args, "-d", strings.Join(stage.Targets, ","))
		} else if !argsContain(stage.Args, "-d") {
			args = append(args, "-d", workDir)
		}
	}

	if stage.Runtime > 0 && !argsContain(stage.Args, "-D") {
		// IOR `-D` is deadlineForStonewalling (seconds); mdtest uses `-i`
		// iterations rather than seconds — for parity we only apply
		// Runtime to ior.
		if tool == "ior" {
			args = append(args, "-D", strconv.Itoa(stage.Runtime))
		}
	}

	if defaults.ExtraArgs != "" {
		args = append(args, strings.Fields(defaults.ExtraArgs)...)
	}
	if stage.ExtraArgs != "" {
		args = append(args, strings.Fields(stage.ExtraArgs)...)
	}

	// Force JSON summary unless the caller already requested a format.
	if !argsContain(args, "summaryFormat=") {
		args = append(args, "-O", "summaryFormat=JSON")
	}
	return args
}

// argsContain returns true if any arg contains `needle` (substring match).
// Used to avoid double-appending `-O summaryFormat=...` / `-d` when the
// caller already supplied one.
func argsContain(args []string, needle string) bool {
	for _, a := range args {
		if strings.Contains(a, needle) {
			return true
		}
	}
	return false
}

// postRun marshals the request, POSTs to <endpoint>/run, decodes the JSON
// response and returns it. The HTTP-level errors are wrapped so the caller
// can classify them via ClassifyError.
func postRun(ctx context.Context, client *http.Client, endpoint string, req iorRunRequest) (*iorRunResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/run", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, tailString(string(raw), 400))
	}
	var out iorRunResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode: %w (body=%s)", err, tailString(string(raw), 400))
	}
	return &out, nil
}

// iorJSON is a minimal subset of IOR's JSON summary needed to compute
// throughput / IOPS / latency. Field names and units follow IOR 3.3.0.
//
//	{
//	  "summary": [
//	    {
//	      "operation": "write",
//	      "API": "POSIX",
//	      "bwMaxMIB": 312.45,
//	      "bwMeanMIB": 280.10,
//	      "OPsMean": 71705.6,
//	      "latencyMin": 0.000020,
//	      "latencyMean": 0.000048,
//	      "latencyMax": 0.000310,
//	      ...
//	    },
//	    { "operation": "read", ... }
//	  ],
//	  "tests": [ ... ]
//	}
//
// mdtest's JSON output uses the same outer "summary" array with one entry
// per operation (e.g. "File creation", "File stat", ...). For mdtest the
// rate is reported in "OPsMean" while bw fields are absent.
type iorJSON struct {
	Summary []iorSummary `json:"summary"`
}

type iorSummary struct {
	Operation   string  `json:"operation"`
	API         string  `json:"API"`
	BWMaxMIB    float64 `json:"bwMaxMIB"`
	BWMeanMIB   float64 `json:"bwMeanMIB"`
	BWMinMIB    float64 `json:"bwMinMIB"`
	OPsMean     float64 `json:"OPsMean"`
	OPsMax      float64 `json:"OPsMax"`
	OPsMin      float64 `json:"OPsMin"`
	LatencyMin  float64 `json:"latencyMin"`  // seconds
	LatencyMean float64 `json:"latencyMean"` // seconds
	LatencyMax  float64 `json:"latencyMax"`  // seconds
	// mdtest variants — some IOR releases name fields differently.
	Rate float64 `json:"rate,omitempty"` // mdtest fallback for OPsMean
}

// parseIORResult tries to extract the JSON summary block from IOR's stdout
// and project the dominant operation onto a single BenchStageResult.
//
// IOR / mdtest prepend a banner (version, command line, etc.) before the
// JSON document; we locate the first '{' and decode from there. When
// multiple operations are present we pick the one with the highest
// throughput (or highest ops/sec when no throughput is reported).
//
// On parse failure the stage result is returned with the stage name but
// zero metrics, plus the original stdout truncated to 400 bytes in a
// reserved future field. Callers should treat this as "stage ran but
// metrics unavailable" rather than failing the whole task; IOR may emit
// human-readable error tails even on exit=0.
func parseIORResult(stdout, stageName string) *spec.BenchStageResult {
	sr := &spec.BenchStageResult{Name: stageName}

	start := strings.Index(stdout, "{")
	if start < 0 {
		return sr
	}
	// Find the matching final '}' — IOR may emit trailing chatter after
	// the JSON, e.g. cleanup messages.
	end := strings.LastIndex(stdout, "}")
	if end <= start {
		return sr
	}
	var doc iorJSON
	if err := json.Unmarshal([]byte(stdout[start:end+1]), &doc); err != nil {
		log.LogDebugf("bench ior: parse json: %v", err)
		return sr
	}
	if len(doc.Summary) == 0 {
		return sr
	}

	// Pick the summary entry with the highest bw (ior) or highest ops/sec
	// (mdtest, which doesn't report bw). Aggregate read+write throughput
	// when both are present so dashboards see "total" rather than
	// "whichever IOR happened to list first".
	var totalBW, totalOps float64
	var dominantLat iorSummary
	dominantBW := -1.0
	for _, s := range doc.Summary {
		bw := s.BWMeanMIB
		ops := s.OPsMean
		if ops == 0 {
			ops = s.Rate
		}
		totalBW += bw
		totalOps += ops
		score := bw
		if score == 0 {
			score = ops
		}
		if score > dominantBW {
			dominantBW = score
			dominantLat = s
		}
	}

	// IOR reports throughput in MiB/s; BenchStageResult.ThroughputMBs is
	// labelled MB/s but used as MiB/s throughout the codebase (see
	// fio path), so passthrough is intentional and consistent.
	sr.ThroughputMBs = totalBW
	sr.OpsPerSec = totalOps
	// latency: IOR reports seconds, BenchLatencyResult is microseconds.
	sr.Latency.Mean = dominantLat.LatencyMean * 1e6
	// IOR's summary lacks percentiles; approximate P50≈mean, P99≈max.
	// This keeps the SLA evaluator working when an IOR rule sets P99MsMax
	// — the worst observed completion latency is a sound upper bound.
	sr.Latency.P50 = dominantLat.LatencyMean * 1e6
	sr.Latency.P95 = dominantLat.LatencyMax * 1e6
	sr.Latency.P99 = dominantLat.LatencyMax * 1e6
	sr.Latency.Max = dominantLat.LatencyMax * 1e6
	return sr
}

// opLabelForIOR maps the stage's Tool into a stable op label used by the
// /metrics/bench errors counter. We can't pick a per-op label like "write"
// without parsing IOR's output, so we use the tool name itself.
func opLabelForIOR(tool string) string {
	switch strings.ToLower(strings.TrimSpace(tool)) {
	case "mdtest":
		return "mdtest"
	default:
		return "ior"
	}
}

// tailString returns the last n bytes of s, used for error message
// truncation. Keeps the response small when IOR dumps a long trace.
func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// ---------------------------------------------------------------------------
// S3.4 error attribution — append-only hooks. observeErrorAttr 调用点已在
// stage 执行失败分支内联，向新的 cubefs_bench_error_attr_total 指标写入
// 错误归因。旧的 IncErr 调用保持不变。
// ---------------------------------------------------------------------------

