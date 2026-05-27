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

package spec

// BenchShardResult holds the outcome of a single shard execution for a bench
// task. Each node running a shard reports one BenchShardResult back to the
// master for aggregation into the full task result.
type BenchShardResult struct {
	ShardIdx  int                `json:"shardIdx"`
	NodeAddr  string             `json:"nodeAddr"`
	Status    string             `json:"status"` // "running" | "done" | "failed" | "skip"
	StartedAt int64              `json:"startedAt"`
	DoneAt    int64              `json:"doneAt"`
	Stages    []BenchStageResult `json:"stages"`
	Error     string             `json:"error,omitempty"`
}

// BenchSLAResult is the master-evaluated outcome of a rule's SLA checklist.
// Pass is the AND of every Items[*].Pass: any single failing item fails the
// whole task. It is omitted from the task record when the rule has no SLA
// configured, so dashboards can render a "no SLA" badge instead of forcing
// green/red on a missing field.
type BenchSLAResult struct {
	Pass  bool           `json:"pass"`
	Items []BenchSLAItem `json:"items,omitempty"`
}

// BenchSLAItem is the outcome of one BenchSLA × one matched stage. A single
// BenchSLA may produce multiple Items when its AppliesTo glob matches
// multiple stages. Index points back into BenchRule.SLA so the dashboard
// can correlate the failed criterion with what the user configured.
//
// When AppliesTo matches no stage, exactly one Item is emitted with Pass
// false, Stage "" and Reasons carrying the stage-missing message.
type BenchSLAItem struct {
	Index     int      `json:"index"`               // position in BenchRule.SLA
	AppliesTo string   `json:"appliesTo,omitempty"` // copied from the BenchSLA
	Stage     string   `json:"stage,omitempty"`     // matched stage name; "" when AppliesTo matched nothing
	Pass      bool     `json:"pass"`
	Reasons   []string `json:"reasons,omitempty"`
}

// BenchStageResult holds the aggregated metrics for one stage within a shard.
//
// HDRBuckets carries gzip+base64 HDR snapshots keyed by op name (e.g. "put",
// "get", "delete"). Each shard produces its own snapshots; the master merges
// across shards via syncnode/hist.MergeSnapshots and recomputes percentiles
// onto the parent record. Populated only for storage types that have a
// per-op hook (S3/SDK); fio/mdtest paths leave it empty.
//
// MixedComponents carries the per-component breakdown for FIO mixed stages
// (stage.Mixed != nil). Each entry mirrors one FIOMixedComponent's outcome
// before the stage-level aggregation collapses them into Total* / Latency.
// Empty for non-mixed stages (fio single, s3, ior, mdtest) — the `omitempty`
// tag keeps the JSON payload identical to rc7 for those paths so dashboards
// can detect the field's presence to decide whether to render a per-component
// breakdown.
type BenchStageResult struct {
	Name            string                 `json:"name"`
	DurationSec     float64                `json:"durationSec"`
	ThroughputMBs   float64                `json:"throughputMBs"`
	OpsPerSec       float64                `json:"opsPerSec"`
	TotalOps        int64                  `json:"totalOps"`
	TotalBytes      int64                  `json:"totalBytes"`
	Errors          int64                  `json:"errors"`
	Latency         BenchLatencyResult     `json:"latency"`
	HDRBuckets      map[string][]byte      `json:"hdrBuckets,omitempty"`
	MixedComponents []BenchComponentResult `json:"mixedComponents,omitempty"`
}

// BenchComponentResult is the per-component breakdown of a FIO mixed stage.
// One entry per FIOMixedComponent that actually ran (Weight > 0). The
// stage-level BenchStageResult still carries the aggregated Total* / Latency
// for SLA evaluation and the headline dashboard tile; this slice is what the
// dashboard's "small vs large" detail panel reads from.
//
// SizeClass is the resolved label (FIOMixedComponent.SizeClass.ClassLabel()),
// not the raw enum, so consumers don't need to repeat the "" → "default"
// fallback. Weight is copied verbatim so dashboards can show the configured
// time-share alongside the realised throughput.
type BenchComponentResult struct {
	Name          string             `json:"name"`
	SizeClass     string             `json:"sizeClass"`
	Weight        int                `json:"weight"`
	DurationSec   float64            `json:"durationSec"`
	ThroughputMBs float64            `json:"throughputMBs"`
	OpsPerSec     float64            `json:"opsPerSec"`
	TotalOps      int64              `json:"totalOps"`
	TotalBytes    int64              `json:"totalBytes"`
	Errors        int64              `json:"errors"`
	Latency       BenchLatencyResult `json:"latency"`
}

// BenchLatencyResult carries latency percentiles + mean + max for a stage,
// expressed in microseconds. P9999 / Max are populated by the HDR path on
// S3/SDK shards and by the master-side merge; legacy fio/mdtest paths leave
// them zero.
type BenchLatencyResult struct {
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	P999  float64 `json:"p999"`
	P9999 float64 `json:"p9999,omitempty"`
	Max   float64 `json:"max,omitempty"`
	Mean  float64 `json:"mean"`
}
