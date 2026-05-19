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

// BenchStageResult holds the aggregated metrics for one stage within a shard.
type BenchStageResult struct {
	Name          string             `json:"name"`
	DurationSec   float64            `json:"durationSec"`
	ThroughputMBs float64            `json:"throughputMBs"`
	OpsPerSec     float64            `json:"opsPerSec"`
	TotalOps      int64              `json:"totalOps"`
	TotalBytes    int64              `json:"totalBytes"`
	Errors        int64              `json:"errors"`
	Latency       BenchLatencyResult `json:"latency"`
}

// BenchLatencyResult carries latency percentiles and mean for a stage,
// expressed in microseconds.
type BenchLatencyResult struct {
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Mean float64 `json:"mean"`
}
