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
	"strconv"
	"strings"
	"testing"
)

// fioIntervalJSON returns a minimal fio JSON+ status-interval snapshot with
// the given read/write activity and cumulative counters. Empty side is
// emitted with zeros so parseFIOInterval's "active op" heuristic filters it
// out. clat_ns percentiles are populated; lat_ns is left empty so tests also
// exercise the clat_ns→µs path in pickFIOPercentile.
func fioIntervalJSON(t *testing.T, read, write fioStatsFixture) string {
	t.Helper()
	return `{
		"jobs": [{
			"jobname": "j",
			"read": ` + read.encode() + `,
			"write": ` + write.encode() + `
		}]
	}`
}

type fioStatsFixture struct {
	iops     float64
	bwBytes  int64
	totalIOs int64
	ioBytes  int64
	totalErr int64
	p50ns    float64
	p95ns    float64
	p99ns    float64
}

func (f fioStatsFixture) encode() string {
	if f == (fioStatsFixture{}) {
		return `{
			"iops": 0,
			"bw_bytes": 0,
			"total_ios": 0,
			"io_bytes": 0,
			"total_err": 0,
			"lat_ns": {"mean": 0, "percentile": {}},
			"clat_ns": {"mean": 0, "percentile": {}}
		}`
	}
	return `{
		"iops": ` + ftoa(f.iops) + `,
		"bw_bytes": ` + itoa64(f.bwBytes) + `,
		"total_ios": ` + itoa64(f.totalIOs) + `,
		"io_bytes": ` + itoa64(f.ioBytes) + `,
		"total_err": ` + itoa64(f.totalErr) + `,
		"lat_ns": {"mean": 0, "percentile": {}},
		"clat_ns": {
			"mean": ` + ftoa(f.p99ns) + `,
			"percentile": {
				"50.000000": ` + ftoa(f.p50ns) + `,
				"95.000000": ` + ftoa(f.p95ns) + `,
				"99.000000": ` + ftoa(f.p99ns) + `
			}
		}
	}`
}

// TestParseFIOInterval_ReadOnly feeds an object where only the read side is
// active and verifies the observer is invoked exactly once with op="read"
// and matching field values. The write side has all zeros and must be
// filtered by parseFIOInterval's active-op heuristic.
func TestParseFIOInterval_ReadOnly(t *testing.T) {
	obj := fioIntervalJSON(t, fioStatsFixture{
		iops: 1234, bwBytes: 50 * 1024 * 1024, totalIOs: 9001, ioBytes: 90 * 1024 * 1024,
		totalErr: 0, p50ns: 50_000, p95ns: 90_000, p99ns: 100_000,
	}, fioStatsFixture{})

	calls := []string{}
	var got fioJobStats
	err := parseFIOInterval([]byte(obj), func(op string, s fioJobStats) {
		calls = append(calls, op)
		if op == "read" {
			got = s
		}
	})
	if err != nil {
		t.Fatalf("parseFIOInterval: %v", err)
	}
	if len(calls) != 1 || calls[0] != "read" {
		t.Fatalf("calls = %v, want [read]", calls)
	}
	if got.IOPS != 1234 || got.BWBytes != 50*1024*1024 || got.TotalIOs != 9001 ||
		got.IOBytes != 90*1024*1024 || got.TotalErr != 0 {
		t.Errorf("read stats mismatch: %+v", got)
	}
	if got.ClatNs.Percentile["99.000000"] != 100_000 {
		t.Errorf("p99 ns = %v, want 100000", got.ClatNs.Percentile["99.000000"])
	}
}

// TestParseFIOInterval_ReadWrite verifies both sides emit when active. Order
// is read-then-write per parseFIOInterval's implementation.
func TestParseFIOInterval_ReadWrite(t *testing.T) {
	obj := fioIntervalJSON(t,
		fioStatsFixture{iops: 100, bwBytes: 1 << 20, totalIOs: 10, p99ns: 1000},
		fioStatsFixture{iops: 200, bwBytes: 2 << 20, totalIOs: 20, p99ns: 2000},
	)

	calls := []string{}
	stats := map[string]fioJobStats{}
	err := parseFIOInterval([]byte(obj), func(op string, s fioJobStats) {
		calls = append(calls, op)
		stats[op] = s
	})
	if err != nil {
		t.Fatalf("parseFIOInterval: %v", err)
	}
	if len(calls) != 2 || calls[0] != "read" || calls[1] != "write" {
		t.Fatalf("calls = %v, want [read write]", calls)
	}
	if stats["read"].IOPS != 100 || stats["write"].IOPS != 200 {
		t.Errorf("iops mismatch: read=%v write=%v", stats["read"].IOPS, stats["write"].IOPS)
	}
}

// TestParseFIOInterval_BothInactive verifies the observer is never called
// when both sides report zero activity (defensive: drainer should still
// emit nothing rather than zero-rate noise).
func TestParseFIOInterval_BothInactive(t *testing.T) {
	obj := fioIntervalJSON(t, fioStatsFixture{}, fioStatsFixture{})
	calls := 0
	err := parseFIOInterval([]byte(obj), func(op string, s fioJobStats) { calls++ })
	if err != nil {
		t.Fatalf("parseFIOInterval: %v", err)
	}
	if calls != 0 {
		t.Fatalf("calls = %d, want 0 (all-zero stats must be filtered)", calls)
	}
}

// TestDrainFIOStdout_TwoAdjacentObjects feeds two adjacent fio JSON objects
// (with banner text between them, simulating fio's `Starting ...` lines and
// blank lines) and verifies:
//   - drainFIOStdout returns the bytes of the SECOND object (the final
//     summary candidate)
//   - both objects' per-op observations land in /metrics/bench (verifies the
//     drainer wires through parseFIOInterval → emitFIOInterval → Prometheus)
func TestDrainFIOStdout_TwoAdjacentObjects(t *testing.T) {
	taskID, shard, stage, op := "fio-stream-1", 0, "rwmix", "read"

	first := fioIntervalJSON(t, fioStatsFixture{
		iops: 1000, bwBytes: 10 * 1024 * 1024, totalIOs: 5000, ioBytes: 10 * 1024 * 1024,
		p50ns: 40_000, p95ns: 90_000, p99ns: 100_000,
	}, fioStatsFixture{})

	second := fioIntervalJSON(t, fioStatsFixture{
		iops: 1500, bwBytes: 15 * 1024 * 1024, totalIOs: 8000, ioBytes: 16 * 1024 * 1024,
		totalErr: 3, p50ns: 50_000, p95ns: 110_000, p99ns: 130_000,
	}, fioStatsFixture{})

	// Banner text between objects mimics fio's interleaved progress lines
	// outside any object — drainer must skip these.
	stream := "Starting 1 process\n" + first + "\nfio-3.16\n  Jobs: 1 (f=1)\n" + second + "\n"

	last, err := drainFIOStdout(strings.NewReader(stream), taskID, shard, stage)
	if err != nil {
		t.Fatalf("drainFIOStdout: %v", err)
	}

	// `last` must be the SECOND object's bytes. Sanity check by re-parsing it
	// with parseFIOResultBytes (the same path runFIOStage uses as the final
	// summary feed).
	sr, err := parseFIOResultBytes(last, stage)
	if err != nil {
		t.Fatalf("parseFIOResultBytes(last): %v", err)
	}
	if sr.TotalOps != 8000 {
		t.Errorf("last object TotalOps = %d, want 8000 (second object's total_ios)", sr.TotalOps)
	}

	// Both intervals must have produced /metrics/bench observations. After
	// the first object: counter is in baseline (state recorded, no Add). After
	// the second: counter += (8000 - 5000) = 3000.
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_ios_total", taskID, "0", stage, op); v != 3000 {
		t.Errorf("total_ios counter = %v, want 3000 after two intervals", v)
	}
	// Bytes counter: second cum (16MB) - first cum (10MB) = 6MB.
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_total_bytes_total", taskID, "0", stage, op); v != float64(6*1024*1024) {
		t.Errorf("total_bytes counter = %v, want %v", v, 6*1024*1024)
	}
	// Errors: 3 - 0 = 3.
	if v := fioCounterValue(t, "syncnode_bench_fio_interval_errors_total", taskID, "0", stage, op); v != 3 {
		t.Errorf("errors counter = %v, want 3", v)
	}
	// Gauges hold the LATEST snapshot values.
	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_iops", taskID, "0", stage, op); v != 1500 {
		t.Errorf("iops gauge = %v, want 1500", v)
	}
	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_lat_p99_us", taskID, "0", stage, op); v != 130 {
		t.Errorf("p99 gauge = %v µs, want 130 (130000ns)", v)
	}

	// Cleanup so this test's labels don't bleed into other tests sharing the
	// global benchRegistry.
	cleanupFIOInterval(taskID, shard, stage)
}

// TestDrainFIOStdout_BraceInsideString verifies the brace-balance state
// machine correctly handles `{` and `}` characters embedded inside JSON
// string values (e.g. an fio job that names its filename "/tmp/{wild}.0").
// Without string-aware tracking the parser would think depth dipped to 0
// mid-object and emit a truncated blob.
func TestDrainFIOStdout_BraceInsideString(t *testing.T) {
	taskID, shard, stage, op := "fio-brace-1", 0, "rw", "read"

	// Embed `{wild}` inside a string value. Use an unrelated field
	// ("global_options.filename" is a real fio json field) so parseFIOInterval
	// still succeeds — the test is about depth tracking, not field semantics.
	obj := `{
		"global options": {"filename": "/tmp/{wild}.0", "rw": "read"},
		"jobs": [{
			"jobname": "j",
			"read": ` + (fioStatsFixture{iops: 500, bwBytes: 5 << 20, totalIOs: 100, ioBytes: 5 << 20, p99ns: 50_000}).encode() + `,
			"write": ` + (fioStatsFixture{}).encode() + `
		}]
	}`

	last, err := drainFIOStdout(strings.NewReader(obj), taskID, shard, stage)
	if err != nil {
		t.Fatalf("drainFIOStdout: %v", err)
	}
	// Returned object must equal the full input modulo leading whitespace —
	// drainer trims only pre-object banner bytes, so byte-equal of the {…}
	// region is the right check. Easier: re-parse and confirm shape.
	sr, err := parseFIOResultBytes(last, stage)
	if err != nil {
		t.Fatalf("parseFIOResultBytes(last): %v (last=%q)", err, string(last))
	}
	if sr.TotalOps != 100 {
		t.Errorf("TotalOps = %d, want 100 (object truncated by brace-in-string bug?)", sr.TotalOps)
	}

	// First sighting → counter in baseline; gauges populated.
	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_iops", taskID, "0", stage, op); v != 500 {
		t.Errorf("iops gauge = %v, want 500", v)
	}

	cleanupFIOInterval(taskID, shard, stage)
}

// TestDrainFIOStdout_EscapedQuoteInString verifies `\"` inside a string
// doesn't terminate the string-tracking state (i.e. the `esc` flag works).
// If broken, the parser would treat the rest of the buffer as outside-string
// and miscount braces.
func TestDrainFIOStdout_EscapedQuoteInString(t *testing.T) {
	taskID, shard, stage, op := "fio-esc-1", 0, "rw", "read"

	obj := `{
		"comment": "she said \"hi\" then {laughed}",
		"jobs": [{
			"jobname": "j",
			"read": ` + (fioStatsFixture{iops: 7, bwBytes: 1024, totalIOs: 7, ioBytes: 1024, p99ns: 1_000}).encode() + `,
			"write": ` + (fioStatsFixture{}).encode() + `
		}]
	}`

	last, err := drainFIOStdout(strings.NewReader(obj), taskID, shard, stage)
	if err != nil {
		t.Fatalf("drainFIOStdout: %v", err)
	}
	if _, err := parseFIOResultBytes(last, stage); err != nil {
		t.Fatalf("parseFIOResultBytes: %v (last=%q)", err, string(last))
	}
	if v := fioGaugeValue(t, "syncnode_bench_fio_interval_iops", taskID, "0", stage, op); v != 7 {
		t.Errorf("iops gauge = %v, want 7", v)
	}

	cleanupFIOInterval(taskID, shard, stage)
}

// TestDrainFIOStdout_EmptyStream verifies an empty reader returns no error
// and an empty `last` (runFIOStage's caller handles empty as "empty fio
// json" via parseFIOResultBytes).
func TestDrainFIOStdout_EmptyStream(t *testing.T) {
	last, err := drainFIOStdout(strings.NewReader(""), "fio-empty", 0, "rw")
	if err != nil {
		t.Fatalf("drainFIOStdout(empty): %v", err)
	}
	if len(last) != 0 {
		t.Errorf("last = %q, want empty", string(last))
	}
}

// TestParseFIOResultBytes_Empty exercises the explicit empty guard added in
// the refactor — runFIOStage routes drainFIOStdout's empty return through
// parseFIOResultBytes which must surface a clear error rather than panic on
// json.Unmarshal of `nil`.
func TestParseFIOResultBytes_Empty(t *testing.T) {
	_, err := parseFIOResultBytes(nil, "rw")
	if err == nil {
		t.Fatalf("expected error for empty input, got nil")
	}
	if !strings.Contains(err.Error(), "empty fio json") {
		t.Errorf("error = %q, want it to contain 'empty fio json'", err)
	}
}

func ftoa(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func itoa64(i int64) string {
	return strconv.FormatInt(i, 10)
}
