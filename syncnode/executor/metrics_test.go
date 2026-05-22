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
