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
	"math"
	"net"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Bench-specific Prometheus metrics on a SEPARATE Registry so they do not
// pollute the default exporter registry (which carries node-level / syncer
// gauges). Exposed via /metrics/bench on the syncnode's exporter port.
//
// Metric naming follows the bench-platform plan:
//   - syncnode_bench_op_latency_seconds (histogram)
//   - syncnode_bench_op_bytes_total     (counter)
//   - syncnode_bench_op_errors_total    (counter)
//   - syncnode_bench_stage_state        (gauge)
//
// Labels: task_id, shard, stage, op (+ kind for errors). High-cardinality
// risk is bounded by N_shards × N_stages × N_ops per task; bench tasks have
// short lifetimes and the metrics live in an isolated registry that scrapers
// pick up only via /metrics/bench. Stale series age out when the syncnode
// drops them at TTL (today: process restart; future: explicit cleanup).
var (
	benchRegistry = prometheus.NewRegistry()

	// stageStateBuckets: 30 buckets covering 100µs ~ 10s geometric.
	// Lower bound 100µs catches sub-ms ops (memory caches, NVMe direct I/O);
	// upper bound 10s catches first-byte stalls on cold S3 paths without
	// blowing up bucket count.
	benchOpLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "syncnode_bench_op_latency_seconds",
		Help:    "Bench per-op latency histogram (seconds).",
		Buckets: geometricBuckets(100e-6, 10.0, 30),
	}, []string{"task_id", "shard", "stage", "op"})

	benchOpBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "syncnode_bench_op_bytes_total",
		Help: "Bench per-op bytes transferred (cumulative).",
	}, []string{"task_id", "shard", "stage", "op"})

	benchOpErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "syncnode_bench_op_errors_total",
		Help: "Bench per-op errors by kind (cumulative).",
	}, []string{"task_id", "shard", "stage", "op", "kind"})

	benchStageState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncnode_bench_stage_state",
		Help: "Stage state: 0=pending, 1=running, 2=done, 3=failed.",
	}, []string{"task_id", "shard", "stage"})
)

// Stage state values exposed via benchStageState gauge.
const (
	StageStatePending = 0.0
	StageStateRunning = 1.0
	StageStateDone    = 2.0
	StageStateFailed  = 3.0
)

func init() {
	benchRegistry.MustRegister(benchOpLatency, benchOpBytes, benchOpErrors, benchStageState)
}

// BenchRegistry returns the isolated Prometheus registry holding all bench
// metrics. The syncnode server mounts it under /metrics/bench during startup.
func BenchRegistry() *prometheus.Registry { return benchRegistry }

// geometricBuckets returns n geometrically spaced upper bounds from min to max
// (inclusive). Used so the histogram covers many decades with bounded bucket
// count.
func geometricBuckets(min, max float64, n int) []float64 {
	if n < 2 || min <= 0 || max <= min {
		return prometheus.DefBuckets
	}
	out := make([]float64, n)
	ratio := math.Pow(max/min, 1.0/float64(n-1))
	v := min
	for i := 0; i < n; i++ {
		out[i] = v
		v *= ratio
	}
	return out
}

// shardLabel renders an integer shard index as a label value. Centralised
// so all callers use the same format.
func shardLabel(shard int) string {
	// strconv would work; tiny helper keeps the import surface small.
	if shard < 0 {
		return "-"
	}
	return itoa(shard)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	buf := [20]byte{}
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// ObserveBenchOp records one successful op: latency histogram + bytes counter.
// Pass bytes==0 when the op carries no payload (e.g. head, delete).
func ObserveBenchOp(taskID string, shard int, stage, op string, d time.Duration, bytes int64) {
	s := shardLabel(shard)
	benchOpLatency.WithLabelValues(taskID, s, stage, op).Observe(d.Seconds())
	if bytes > 0 {
		benchOpBytes.WithLabelValues(taskID, s, stage, op).Add(float64(bytes))
	}
}

// IncErr increments the per-op error counter. kind is one of the canonical
// strings from ClassifyError; callers may pass arbitrary kinds but should
// stick to the canonical set to keep dashboards stable.
func IncErr(taskID string, shard int, stage, op, kind string) {
	benchOpErrors.WithLabelValues(taskID, shardLabel(shard), stage, op, kind).Inc()
}

// SetStageState updates the per-stage gauge.
func SetStageState(taskID string, shard int, stage string, state float64) {
	benchStageState.WithLabelValues(taskID, shardLabel(shard), stage).Set(state)
}

// ClassifyError maps an arbitrary error into a stable kind label used by the
// errors counter. Kinds follow the bench-platform plan:
//
//	throttle_4xx | server_5xx | timeout | network | checksum | cancel | other
//
// Best-effort classification by error string + interface checks; callers that
// want a more precise kind should pass it directly to IncErr.
func ClassifyError(err error) string {
	if err == nil {
		return "other"
	}
	if errors.Is(err, context.Canceled) {
		return "cancel"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timeout"
		}
		return "network"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "throttl") || strings.Contains(msg, "slowdown") ||
		strings.Contains(msg, "ratelimit") || strings.Contains(msg, "429"):
		return "throttle_4xx"
	case strings.Contains(msg, "checksum") || strings.Contains(msg, "etag mismatch") ||
		strings.Contains(msg, "bad digest"):
		return "checksum"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "connection reset") || strings.Contains(msg, "eof") ||
		strings.Contains(msg, "broken pipe") || strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "i/o timeout"):
		return "network"
	case strings.Contains(msg, "internalerror") || strings.Contains(msg, "503") ||
		strings.Contains(msg, "500") || strings.Contains(msg, "502") ||
		strings.Contains(msg, "504"):
		return "server_5xx"
	case strings.Contains(msg, "400") || strings.Contains(msg, "401") ||
		strings.Contains(msg, "403") || strings.Contains(msg, "404") ||
		strings.Contains(msg, "405"):
		return "throttle_4xx"
	}
	return "other"
}

// ---------------------------------------------------------------------------
// S3.2 Soak — append-only metrics. New gauges/counters MUST go below this
// anchor; do not insert above (avoids merge conflicts with S3.3 / S3.4 which
// also append at file tail). See docs/plan or 任务卡片 S3.2.
// ---------------------------------------------------------------------------

// Soak-specific gauges. Same isolated benchRegistry; same label conventions
// (task_id / shard / stage) as the other bench metrics so dashboards can join.
var (
	benchSoakRestartCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncnode_soak_restart_count",
		Help: "Soak stage restart counter — increments each time a stage callback errored and was restarted.",
	}, []string{"task_id", "shard", "stage"})

	benchSoakElapsedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncnode_soak_elapsed_seconds",
		Help: "Soak stage elapsed seconds — value from the most recent checkpoint.",
	}, []string{"task_id", "shard", "stage"})
)

func init() {
	benchRegistry.MustRegister(benchSoakRestartCount, benchSoakElapsedSeconds)
}

// soakObserveCheckpoint pushes the latest elapsed seconds gauge after each
// successful Save. Called from soak_runner.go.
func soakObserveCheckpoint(cp SoakCheckpoint) {
	benchSoakElapsedSeconds.WithLabelValues(cp.TaskID, shardLabel(cp.ShardID), cp.Stage).Set(float64(cp.ElapsedSec))
	// Restart count gauge is kept in lock-step with ElapsedSec so dashboards
	// see both update together even if no actual restart happened this tick.
	benchSoakRestartCount.WithLabelValues(cp.TaskID, shardLabel(cp.ShardID), cp.Stage).Set(float64(cp.RestartCount))
}

// soakObserveRestart bumps the restart-count gauge immediately when the
// runner has decided to restart a stage (before the back-off sleep). Saves
// dashboard latency vs. waiting for the next checkpoint tick.
func soakObserveRestart(taskID, stage string, shardID, restartCount int) {
	benchSoakRestartCount.WithLabelValues(taskID, shardLabel(shardID), stage).Set(float64(restartCount))
}

// ---------------------------------------------------------------------------
// S3.3 Mixed workload — append-only metrics. 新增的"按 size class 维度"指标
// 与已有 4 个 bench 指标完全并列、不修改原有 label 集合，保证旧 dashboard
// 100% 兼容；新 dashboard 可基于 class 维度做 small / large 拆分曲线。
// S3.4 应在本块末尾继续追加 `// S3.4 ... append-only metrics` 锚点。
// ---------------------------------------------------------------------------

var (
	// benchOpLatencyClass：新增 "class" 标签的 latency histogram。
	// 与 benchOpLatency 共用 bucket 配置以便跨 panel 对齐。
	benchOpLatencyClass = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "syncnode_bench_op_latency_class_seconds",
		Help:    "Bench per-op latency histogram bucketed by size class (seconds).",
		Buckets: geometricBuckets(100e-6, 10.0, 30),
	}, []string{"task_id", "shard", "stage", "op", "class"})

	// benchOpBytesClass：新增 "class" 标签的 bytes counter。
	benchOpBytesClass = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "syncnode_bench_op_bytes_class_total",
		Help: "Bench per-op bytes transferred bucketed by size class (cumulative).",
	}, []string{"task_id", "shard", "stage", "op", "class"})
)

func init() {
	benchRegistry.MustRegister(benchOpLatencyClass, benchOpBytesClass)
}

// ObserveBenchOpClass 与 ObserveBenchOp 并行调用，把 class 维度的延迟与
// 字节累计记录到 class 维度的指标。class 为空时记为 "default"，保证标签
// 永不缺失。bytes <= 0 时仅记录 latency。
func ObserveBenchOpClass(taskID string, shard int, stage, op, class string, latencyS float64, bytes int64) {
	if class == "" {
		class = "default"
	}
	s := shardLabel(shard)
	benchOpLatencyClass.WithLabelValues(taskID, s, stage, op, class).Observe(latencyS)
	if bytes > 0 {
		benchOpBytesClass.WithLabelValues(taskID, s, stage, op, class).Add(float64(bytes))
	}
}

// ---------------------------------------------------------------------------
// S3.4 Warmup/CacheDrop/ErrorAttr — append-only metrics. 与已有 bench 指标
// 完全并列、不修改既有 label 集合，旧 dashboard 100% 兼容。新 dashboard
// 通过新 metric 名进行下钻（warmup ops / cache drop / error attribution）。
// 后续 sprint 应在本块末尾继续追加 `// S3.5 ... append-only metrics` 锚点。
// ---------------------------------------------------------------------------

var (
	// 预热阶段的"通过/拒绝"计数（拒绝 = 来不及预热完）
	benchWarmupOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cubefs_bench_warmup_ops_total",
		Help: "Number of warmup operations executed (not counted toward stage metrics)",
	}, []string{"task", "shard", "stage", "result"})

	// cache drop 计数 + 失败原因
	benchCacheDropTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cubefs_bench_cache_drop_total",
		Help: "Number of cache drop attempts",
	}, []string{"task", "where", "result"})

	// 错误归因分桶（粒度更细的 error kind）
	benchErrorAttrTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cubefs_bench_error_attr_total",
		Help: "Error counts bucketed by attribution category (timeout/refused/network/permission/server_5xx/client_4xx/other)",
	}, []string{"task", "shard", "stage", "op", "category"})
)

func init() {
	benchRegistry.MustRegister(benchWarmupOpsTotal, benchCacheDropTotal, benchErrorAttrTotal)
}

func observeWarmupOp(task, shard, stage, result string) {
	benchWarmupOpsTotal.WithLabelValues(task, shard, stage, result).Inc()
}

func observeCacheDrop(task, where string, err error) {
	res := "ok"
	if err != nil {
		res = "fail"
	}
	benchCacheDropTotal.WithLabelValues(task, where, res).Inc()
}

func observeErrorAttr(task, shard, stage, op, category string) {
	benchErrorAttrTotal.WithLabelValues(task, shard, stage, op, category).Inc()
}
