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
	"sync"
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

// SetStageState updates the per-stage gauge. Done/Failed transitions also
// trigger cleanup of any per-stage append-only series (currently fio interval
// gauges/counters + cum state) so cardinality stays bounded over long-running
// syncnode processes.
func SetStageState(taskID string, shard int, stage string, state float64) {
	benchStageState.WithLabelValues(taskID, shardLabel(shard), stage).Set(state)
	if state == StageStateDone || state == StageStateFailed {
		cleanupFIOInterval(taskID, shard, stage)
	}
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
//
// 设计约束：仅用于真 per-op emit 场景（如 bench_s3.go 的 PUT/GET/DELETE 单次调用）。
// 不要在 fio 子进程路径调用——子进程边界外 syncnode 不感知 per-op，
// 调用此函数会让 histogram 退化成 gauge（仅一个 stage 聚合样本）。
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

// ---------------------------------------------------------------------------
// S3.5 FIO interval — append-only metrics. 给 fio 路径补 per-interval 中间趋势
// （latency p50/p95/p99 / throughput MB/s / IOPS）以及累积 IO/bytes/errors。
// 与既有 bench 指标完全并列、不修改原有 label 集合，旧 dashboard 100% 兼容。
// 后续 sprint 应在本块末尾继续追加 `// S3.6 ... append-only metrics` 锚点。
//
// 设计要点：
//   - Gauge 5 个（每 interval Set 当前快照）；Counter 3 个（累积 IO/bytes/errors）。
//   - fio interval JSON 报告的是 stage 内累积值，Counter 不能 Set，故采用
//     delta-based Add：维护 fioIntervalCumState[key] 累积状态，进入 helper 时
//     与上次状态 diff，diff > 0 才 Add；首次出现或 diff < 0（fio 重启 / stage
//     重跑 / overflow）只记录新基准不 Add，保证 Counter 单调。
//   - stage Done/Failed 时通过 SetStageState 触发 cleanupFIOInterval 清理
//     cumState + DeleteLabelValues，防止 long-running syncnode 上 cardinality
//     无限增长。
//
// 详细设计：docs/plan/syncnode/bench-live-trend.md §3.1
// ---------------------------------------------------------------------------

var (
	benchFIOIntervalLatP50Us = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncnode_bench_fio_interval_lat_p50_us",
		Help: "FIO interval-level clat p50 latency (microseconds).",
	}, []string{"task_id", "shard", "stage", "op"})

	benchFIOIntervalLatP95Us = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncnode_bench_fio_interval_lat_p95_us",
		Help: "FIO interval-level clat p95 latency (microseconds).",
	}, []string{"task_id", "shard", "stage", "op"})

	benchFIOIntervalLatP99Us = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncnode_bench_fio_interval_lat_p99_us",
		Help: "FIO interval-level clat p99 latency (microseconds).",
	}, []string{"task_id", "shard", "stage", "op"})

	benchFIOIntervalThroughputMBs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncnode_bench_fio_interval_throughput_mbs",
		Help: "FIO interval-level throughput (MB/s).",
	}, []string{"task_id", "shard", "stage", "op"})

	benchFIOIntervalIOPS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncnode_bench_fio_interval_iops",
		Help: "FIO interval-level IOPS.",
	}, []string{"task_id", "shard", "stage", "op"})

	benchFIOIntervalTotalIOs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "syncnode_bench_fio_interval_total_ios_total",
		Help: "Cumulative IO count reported by fio interval snapshots (delta-applied).",
	}, []string{"task_id", "shard", "stage", "op"})

	benchFIOIntervalTotalBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "syncnode_bench_fio_interval_total_bytes_total",
		Help: "Cumulative bytes transferred reported by fio interval snapshots (delta-applied).",
	}, []string{"task_id", "shard", "stage", "op"})

	benchFIOIntervalErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "syncnode_bench_fio_interval_errors_total",
		Help: "Cumulative IO errors reported by fio interval snapshots (delta-applied).",
	}, []string{"task_id", "shard", "stage", "op"})
)

func init() {
	benchRegistry.MustRegister(
		benchFIOIntervalLatP50Us,
		benchFIOIntervalLatP95Us,
		benchFIOIntervalLatP99Us,
		benchFIOIntervalThroughputMBs,
		benchFIOIntervalIOPS,
		benchFIOIntervalTotalIOs,
		benchFIOIntervalTotalBytes,
		benchFIOIntervalErrors,
	)
}

// fioIntervalCum 保存某 (taskID, shard, stage, op) 上一次观察到的 fio 累计计数，
// 用于把 fio cumulative 值转成 Counter 单调增量。
type fioIntervalCum struct {
	totalIOs   int64
	totalBytes int64
	errors     int64
}

var (
	fioIntervalCumMu    sync.Mutex
	fioIntervalCumState = make(map[string]fioIntervalCum)
)

func fioCumKey(taskID string, shard int, stage, op string) string {
	return taskID + "|" + shardLabel(shard) + "|" + stage + "|" + op
}

// ObserveFIOInterval 在 fio --status-interval 解析到一个 interval 快照时调用。
// latency / throughput / IOPS 直接 Set 到 gauge；total_ios / total_bytes /
// errors 是 fio stage 内累计值，本函数做 delta 计算后 Add 到 Counter，保证
// Counter 单调。
//
// 首次见到某 (taskID, shard, stage, op)：仅记录基线，不 Add；否则只在 delta>0
// 时 Add（delta<0 视为 fio 重启 / stage 重跑 / overflow，重置基线不 Add）。
func ObserveFIOInterval(
	taskID string, shard int, stage, op string,
	latP50Us, latP95Us, latP99Us float64,
	thrMBs, iops float64,
	totalIOs, totalBytes, errs int64,
) {
	s := shardLabel(shard)
	benchFIOIntervalLatP50Us.WithLabelValues(taskID, s, stage, op).Set(latP50Us)
	benchFIOIntervalLatP95Us.WithLabelValues(taskID, s, stage, op).Set(latP95Us)
	benchFIOIntervalLatP99Us.WithLabelValues(taskID, s, stage, op).Set(latP99Us)
	benchFIOIntervalThroughputMBs.WithLabelValues(taskID, s, stage, op).Set(thrMBs)
	benchFIOIntervalIOPS.WithLabelValues(taskID, s, stage, op).Set(iops)

	key := fioCumKey(taskID, shard, stage, op)
	fioIntervalCumMu.Lock()
	prev, ok := fioIntervalCumState[key]
	fioIntervalCumState[key] = fioIntervalCum{
		totalIOs:   totalIOs,
		totalBytes: totalBytes,
		errors:     errs,
	}
	fioIntervalCumMu.Unlock()

	if !ok {
		// 首次观察：仅记录基线，避免把 stage 启动前的累计回填到 Counter。
		return
	}
	if d := totalIOs - prev.totalIOs; d > 0 {
		benchFIOIntervalTotalIOs.WithLabelValues(taskID, s, stage, op).Add(float64(d))
	}
	if d := totalBytes - prev.totalBytes; d > 0 {
		benchFIOIntervalTotalBytes.WithLabelValues(taskID, s, stage, op).Add(float64(d))
	}
	if d := errs - prev.errors; d > 0 {
		benchFIOIntervalErrors.WithLabelValues(taskID, s, stage, op).Add(float64(d))
	}
}

// cleanupFIOInterval 删除某 (taskID, shard, stage) 下所有 op 的 fio interval
// 累计状态与对应 metric label 序列。由 SetStageState 在 Done/Failed 切换时调用。
// 多次调用幂等：DeleteLabelValues 对不存在的序列静默返回 false。
func cleanupFIOInterval(taskID string, shard int, stage string) {
	s := shardLabel(shard)
	for _, op := range []string{"read", "write"} {
		benchFIOIntervalLatP50Us.DeleteLabelValues(taskID, s, stage, op)
		benchFIOIntervalLatP95Us.DeleteLabelValues(taskID, s, stage, op)
		benchFIOIntervalLatP99Us.DeleteLabelValues(taskID, s, stage, op)
		benchFIOIntervalThroughputMBs.DeleteLabelValues(taskID, s, stage, op)
		benchFIOIntervalIOPS.DeleteLabelValues(taskID, s, stage, op)
		benchFIOIntervalTotalIOs.DeleteLabelValues(taskID, s, stage, op)
		benchFIOIntervalTotalBytes.DeleteLabelValues(taskID, s, stage, op)
		benchFIOIntervalErrors.DeleteLabelValues(taskID, s, stage, op)

		fioIntervalCumMu.Lock()
		delete(fioIntervalCumState, fioCumKey(taskID, shard, stage, op))
		fioIntervalCumMu.Unlock()
	}
}
