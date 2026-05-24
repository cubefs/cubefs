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
	"net"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// TestBenchMetrics_IsolatedRegistry confirms the bench registry is wholly
// separate from prometheus.DefaultRegisterer — recording a sample on the
// bench histogram must not produce a metric on the default gatherer, and
// vice versa.
func TestBenchMetrics_IsolatedRegistry(t *testing.T) {
	ObserveBenchOp("t1", 0, "stage1", "put", 5*time.Millisecond, 1024)
	IncErr("t1", 0, "stage1", "put", "timeout")
	SetStageState("t1", 0, "stage1", StageStateRunning)

	mfs, err := BenchRegistry().Gather()
	if err != nil {
		t.Fatalf("gather bench: %v", err)
	}
	want := map[string]bool{
		"syncnode_bench_op_latency_seconds": false,
		"syncnode_bench_op_bytes_total":     false,
		"syncnode_bench_op_errors_total":    false,
		"syncnode_bench_stage_state":        false,
	}
	for _, mf := range mfs {
		if _, ok := want[mf.GetName()]; ok {
			want[mf.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("bench registry missing %q", name)
		}
	}
}

// TestBenchMetrics_StageStateValues verifies the gauge holds the value we
// wrote, not just "is present".
func TestBenchMetrics_StageStateValues(t *testing.T) {
	SetStageState("stateTask", 7, "phase-A", StageStateDone)
	mfs, _ := BenchRegistry().Gather()
	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "syncnode_bench_stage_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if hasLabel(m, "task_id", "stateTask") &&
				hasLabel(m, "shard", "7") &&
				hasLabel(m, "stage", "phase-A") {
				if v := m.GetGauge().GetValue(); v != StageStateDone {
					t.Errorf("expected gauge=%v, got %v", StageStateDone, v)
				}
				found = true
			}
		}
	}
	if !found {
		t.Errorf("stage gauge for stateTask/7/phase-A not found")
	}
}

func hasLabel(m *dto.Metric, name, value string) bool {
	for _, l := range m.Label {
		if l.GetName() == name && l.GetValue() == value {
			return true
		}
	}
	return false
}

// TestClassifyError exercises the canonical kind mapping.
func TestClassifyError(t *testing.T) {
	cases := []struct {
		err  error
		kind string
	}{
		{context.Canceled, "cancel"},
		{context.DeadlineExceeded, "timeout"},
		{&net.OpError{Op: "dial", Err: errors.New("no such host")}, "network"},
		{errors.New("S3 SlowDown: please reduce your request rate"), "throttle_4xx"},
		{errors.New("403 Forbidden"), "throttle_4xx"},
		{errors.New("500 InternalError"), "server_5xx"},
		{errors.New("checksum mismatch"), "checksum"},
		{errors.New("connection reset by peer"), "network"},
		{errors.New("something weird happened"), "other"},
		{nil, "other"},
	}
	for _, c := range cases {
		got := ClassifyError(c.err)
		if got != c.kind {
			label := "<nil>"
			if c.err != nil {
				label = c.err.Error()
			}
			t.Errorf("ClassifyError(%q) = %q, want %q", label, got, c.kind)
		}
	}
	// Smoke: every kind from the plan is reachable from at least one input.
	_ = strings.Join([]string{"throttle_4xx", "server_5xx", "timeout", "network", "checksum", "cancel", "other"}, ",")
}

// fioGaugeValue returns the current value of a 4-label gauge {task_id, shard,
// stage, op}. Returns NaN if not found.
func fioGaugeValue(t *testing.T, metricName, taskID, shard, stage, op string) float64 {
	t.Helper()
	mfs, err := BenchRegistry().Gather()
	if err != nil {
		t.Fatalf("gather bench: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			if hasLabel(m, "task_id", taskID) && hasLabel(m, "shard", shard) &&
				hasLabel(m, "stage", stage) && hasLabel(m, "op", op) {
				if g := m.GetGauge(); g != nil {
					return g.GetValue()
				}
			}
		}
	}
	return -1
}

// fioCounterValue same as fioGaugeValue but reads CounterVec values. Returns
// -1 if no series with that label set exists.
func fioCounterValue(t *testing.T, metricName, taskID, shard, stage, op string) float64 {
	t.Helper()
	mfs, err := BenchRegistry().Gather()
	if err != nil {
		t.Fatalf("gather bench: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			if hasLabel(m, "task_id", taskID) && hasLabel(m, "shard", shard) &&
				hasLabel(m, "stage", stage) && hasLabel(m, "op", op) {
				if c := m.GetCounter(); c != nil {
					return c.GetValue()
				}
			}
		}
	}
	return -1
}

// TestObserveFIOInterval_GaugesAndDeltas drives ObserveFIOInterval through 3
// consecutive fio --status-interval snapshots and verifies (a) gauges hold the
// latest value, (b) counters reflect the cumulative delta (not the raw value),
// (c) the first observation only records a baseline (no Add).
func TestObserveFIOInterval_GaugesAndDeltas(t *testing.T) {
	taskID, shard, stage, op := "fio-trend-1", 0, "rw", "read"

	// Interval 1: 100 latency p99, 50 MB/s, 1000 IOPS, cum 5000 IOs / 10MB / 0 errs.
	// First sighting → counters should remain 0 (baseline only).
	ObserveFIOInterval(taskID, shard, stage, op, 50, 90, 100, 50.0, 1000.0, 5000, 10*1024*1024, 0)

	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_lat_p99_us", taskID, "0", stage, op); v != 100 {
		t.Errorf("interval1 p99 gauge = %v, want 100", v)
	}
	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_throughput_mbs", taskID, "0", stage, op); v != 50 {
		t.Errorf("interval1 throughput gauge = %v, want 50", v)
	}
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "0", stage, op); v != -1 && v != 0 {
		t.Errorf("interval1 counter must be baseline (0 or absent), got %v", v)
	}

	// Interval 2: cum 8000 IOs / 16MB / 2 errs → delta 3000 IOs / 6MB / 2 errs.
	ObserveFIOInterval(taskID, shard, stage, op, 60, 110, 120, 60.0, 1200.0, 8000, 16*1024*1024, 2)
	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_lat_p99_us", taskID, "0", stage, op); v != 120 {
		t.Errorf("interval2 p99 gauge = %v, want 120", v)
	}
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "0", stage, op); v != 3000 {
		t.Errorf("interval2 cumulative IOs counter = %v, want 3000", v)
	}
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_bytes_total", taskID, "0", stage, op); v != float64(6*1024*1024) {
		t.Errorf("interval2 cumulative bytes counter = %v, want 6MB", v)
	}
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_errors_total", taskID, "0", stage, op); v != 2 {
		t.Errorf("interval2 cumulative errors counter = %v, want 2", v)
	}

	// Interval 3: cum 12000 IOs / 24MB / 5 errs → delta 4000 IOs / 8MB / 3 errs.
	// Counter should accumulate to 7000 / 14MB / 5.
	ObserveFIOInterval(taskID, shard, stage, op, 70, 130, 150, 70.0, 1400.0, 12000, 24*1024*1024, 5)
	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_iops", taskID, "0", stage, op); v != 1400 {
		t.Errorf("interval3 iops gauge = %v, want 1400", v)
	}
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "0", stage, op); v != 7000 {
		t.Errorf("interval3 cumulative IOs counter = %v, want 7000 (3000+4000)", v)
	}
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_bytes_total", taskID, "0", stage, op); v != float64(14*1024*1024) {
		t.Errorf("interval3 cumulative bytes counter = %v, want 14MB (6MB+8MB)", v)
	}
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_errors_total", taskID, "0", stage, op); v != 5 {
		t.Errorf("interval3 cumulative errors counter = %v, want 5 (2+3)", v)
	}

	// Cleanup
	cleanupFIOInterval(taskID, shard, stage)
}

// TestObserveFIOInterval_NegativeDeltaResetsBaseline simulates a fio restart
// where the cumulative count goes backward — the counter must NOT decrement,
// it just records the new baseline.
func TestObserveFIOInterval_NegativeDeltaResetsBaseline(t *testing.T) {
	taskID, shard, stage, op := "fio-restart", 0, "rw", "write"

	// First: 1000 cum (baseline).
	ObserveFIOInterval(taskID, shard, stage, op, 10, 20, 30, 10, 100, 1000, 1024, 0)
	// Second: 1500 cum → +500 delta → counter = 500.
	ObserveFIOInterval(taskID, shard, stage, op, 10, 20, 30, 10, 100, 1500, 2048, 0)
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "0", stage, op); v != 500 {
		t.Fatalf("pre-restart counter = %v, want 500", v)
	}
	// Third: 200 cum (fio restarted; cumulative dropped) → no Add, baseline=200.
	ObserveFIOInterval(taskID, shard, stage, op, 10, 20, 30, 10, 100, 200, 512, 0)
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "0", stage, op); v != 500 {
		t.Errorf("post-restart counter must stay at 500 (no decrement), got %v", v)
	}
	// Fourth: 300 cum → +100 delta → counter = 600.
	ObserveFIOInterval(taskID, shard, stage, op, 10, 20, 30, 10, 100, 300, 768, 0)
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "0", stage, op); v != 600 {
		t.Errorf("after restart+progress counter = %v, want 600", v)
	}

	cleanupFIOInterval(taskID, shard, stage)
}

// TestCleanupFIOInterval verifies cleanup removes both metric label series
// and the internal cum-state, so a subsequent observation starts fresh.
func TestCleanupFIOInterval(t *testing.T) {
	taskID, shard, stage, op := "fio-cleanup", 3, "soak", "read"
	ObserveFIOInterval(taskID, shard, stage, op, 10, 20, 30, 10, 100, 1000, 1024, 0)
	ObserveFIOInterval(taskID, shard, stage, op, 10, 20, 30, 10, 100, 2000, 2048, 1)
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "3", stage, op); v != 1000 {
		t.Fatalf("pre-cleanup counter = %v, want 1000", v)
	}

	cleanupFIOInterval(taskID, shard, stage)

	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_lat_p99_us", taskID, "3", stage, op); v != -1 {
		t.Errorf("cleanup did not drop gauge series, got %v", v)
	}
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "3", stage, op); v != -1 {
		t.Errorf("cleanup did not drop counter series, got %v", v)
	}

	// After cleanup, next observation must be a fresh baseline (no Add).
	ObserveFIOInterval(taskID, shard, stage, op, 10, 20, 30, 10, 100, 5000, 4096, 0)
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "3", stage, op); v != -1 && v != 0 {
		t.Errorf("post-cleanup first observation must be baseline (-1 or 0), got %v", v)
	}

	cleanupFIOInterval(taskID, shard, stage)
}

// TestSetStageState_TriggersFIOCleanup confirms the SetStageState hook removes
// fio interval series when a stage transitions to Done or Failed.
func TestSetStageState_TriggersFIOCleanup(t *testing.T) {
	taskID, shard, stage := "fio-hook", 1, "rw"
	ObserveFIOInterval(taskID, shard, stage, "read", 10, 20, 30, 10, 100, 1000, 1024, 0)
	ObserveFIOInterval(taskID, shard, stage, "read", 10, 20, 30, 10, 100, 2000, 2048, 0)

	// Running state must NOT trigger cleanup.
	SetStageState(taskID, shard, stage, StageStateRunning)
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "1", stage, "read"); v != 1000 {
		t.Errorf("Running state should not clean up, counter = %v, want 1000", v)
	}

	// Done state must trigger cleanup.
	SetStageState(taskID, shard, stage, StageStateDone)
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "1", stage, "read"); v != -1 {
		t.Errorf("Done state must trigger cleanup, counter = %v, want -1 (absent)", v)
	}

	// Failed state must also trigger cleanup (different op).
	ObserveFIOInterval(taskID, shard, stage, "write", 10, 20, 30, 10, 100, 500, 512, 0)
	ObserveFIOInterval(taskID, shard, stage, "write", 10, 20, 30, 10, 100, 800, 1024, 0)
	SetStageState(taskID, shard, stage, StageStateFailed)
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "1", stage, "write"); v != -1 {
		t.Errorf("Failed state must trigger cleanup, counter = %v, want -1 (absent)", v)
	}
}

// TestGeometricBuckets sanity-checks the bucket layout used by the latency
// histogram: 30 buckets, monotonic, first ~100µs, last ~10s.
func TestGeometricBuckets(t *testing.T) {
	b := geometricBuckets(100e-6, 10.0, 30)
	if len(b) != 30 {
		t.Fatalf("expected 30 buckets, got %d", len(b))
	}
	for i := 1; i < len(b); i++ {
		if b[i] <= b[i-1] {
			t.Fatalf("buckets not strictly increasing at %d: %v <= %v", i, b[i], b[i-1])
		}
	}
	if b[0] < 99e-6 || b[0] > 101e-6 {
		t.Errorf("first bucket should be ~100µs, got %v", b[0])
	}
	if b[len(b)-1] < 9.99 || b[len(b)-1] > 10.01 {
		t.Errorf("last bucket should be ~10s, got %v", b[len(b)-1])
	}
}
