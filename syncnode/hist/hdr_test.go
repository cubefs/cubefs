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

package hist

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

// TestRecorder_RecordAndSnapshot exercises the basic shard-side path:
// record N samples, snapshot, decode, check the histogram round-trips with
// the same total count and approximately the same percentile values.
func TestRecorder_RecordAndSnapshot(t *testing.T) {
	r := NewRecorder()
	for i := 1; i <= 1000; i++ {
		r.RecordLatency("stage1", "put", time.Duration(i)*time.Microsecond)
	}
	snaps := r.SnapshotStage("stage1")
	if _, ok := snaps["put"]; !ok {
		t.Fatalf("expected snapshot for put op, got %v", snaps)
	}
	s, err := DecodeSnapshot(snaps["put"])
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	merged, _, err := MergeSnapshots([][]byte{snaps["put"]})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if merged.TotalCount() != 1000 {
		t.Fatalf("expected 1000 total count, got %d (snapshot counts len=%d)", merged.TotalCount(), len(s.Counts))
	}
	// p99 should be ~990µs (with HDR's 0.1% precision tolerance).
	p99 := merged.ValueAtPercentile(99)
	if p99 < 970 || p99 > 1020 {
		t.Fatalf("p99 out of expected range: got %d, expected ~990", p99)
	}
}

// TestMergeSnapshots_Precision validates the hard requirement from the plan:
// merged-percentile error vs full-sample percentile must be <1% at p99 and
// <5% at p999 across N shards × M samples each.
func TestMergeSnapshots_Precision(t *testing.T) {
	const (
		numShards    = 8
		samplesEach  = 50_000
		seed         = int64(42)
	)
	rng := rand.New(rand.NewSource(seed))

	// Ground truth: record every sample into one histogram for full-sample
	// percentiles, AND distribute the same samples across N shards for the
	// merge path. Use a lognormal-ish distribution so percentiles are
	// meaningful (not uniform).
	full := NewRecorder()
	shards := make([]*Recorder, numShards)
	for i := range shards {
		shards[i] = NewRecorder()
	}
	for i := 0; i < numShards*samplesEach; i++ {
		// Lognormal-shaped: most samples 50µs - 5ms, long tail to ~1s.
		v := time.Duration(math.Exp(rng.NormFloat64()*1.2+8)) * time.Microsecond
		if v < time.Microsecond {
			v = time.Microsecond
		}
		if v > 600*time.Second {
			v = 600 * time.Second
		}
		full.RecordLatency("s", "op", v)
		shards[i%numShards].RecordLatency("s", "op", v)
	}

	// Full-sample reference percentiles.
	fullSnap := full.SnapshotStage("s")
	fullMerged, _, _ := MergeSnapshots([][]byte{fullSnap["op"]})
	refP50 := fullMerged.ValueAtPercentile(50)
	refP99 := fullMerged.ValueAtPercentile(99)
	refP999 := fullMerged.ValueAtPercentile(99.9)

	// Shard snapshots + merge.
	blobs := make([][]byte, 0, numShards)
	for _, sh := range shards {
		snap := sh.SnapshotStage("s")
		blobs = append(blobs, snap["op"])
	}
	merged, dropped, err := MergeSnapshots(blobs)
	if err != nil {
		t.Fatalf("merge snapshots: %v", err)
	}
	if dropped != 0 {
		t.Fatalf("expected 0 dropped snapshots, got %d", dropped)
	}
	if merged.TotalCount() != int64(numShards*samplesEach) {
		t.Fatalf("expected %d total samples, got %d", numShards*samplesEach, merged.TotalCount())
	}

	mergedP50 := merged.ValueAtPercentile(50)
	mergedP99 := merged.ValueAtPercentile(99)
	mergedP999 := merged.ValueAtPercentile(99.9)

	check := func(name string, ref, got int64, maxRel float64) {
		t.Helper()
		if ref == 0 {
			t.Fatalf("%s reference is zero", name)
		}
		diff := math.Abs(float64(got-ref)) / float64(ref)
		if diff > maxRel {
			t.Errorf("%s relative error %.4f exceeds %.4f (ref=%d got=%d)", name, diff, maxRel, ref, got)
		} else {
			t.Logf("%s ref=%d got=%d rel=%.4f", name, ref, got, diff)
		}
	}
	// HDR is deterministic across identical inputs, but use slightly relaxed
	// thresholds to keep the test robust if hdr-go's internals change.
	check("p50", refP50, mergedP50, 0.01)
	check("p99", refP99, mergedP99, 0.01)
	check("p999", refP999, mergedP999, 0.05)
}

// TestMergeSnapshots_Empty verifies graceful behaviour with no input.
func TestMergeSnapshots_Empty(t *testing.T) {
	merged, dropped, err := MergeSnapshots(nil)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if dropped != 0 {
		t.Fatalf("expected 0 dropped, got %d", dropped)
	}
	if merged == nil {
		t.Fatalf("expected non-nil histogram")
	}
	if merged.TotalCount() != 0 {
		t.Fatalf("expected 0 total count, got %d", merged.TotalCount())
	}
}

// TestRecorder_MultiStage ensures stage isolation: a snapshot for stage A
// must not include op data from stage B.
func TestRecorder_MultiStage(t *testing.T) {
	r := NewRecorder()
	r.RecordLatency("A", "put", 100*time.Microsecond)
	r.RecordLatency("A", "get", 200*time.Microsecond)
	r.RecordLatency("B", "put", 300*time.Microsecond)

	snapA := r.SnapshotStage("A")
	if _, ok := snapA["put"]; !ok {
		t.Errorf("stage A missing put op")
	}
	if _, ok := snapA["get"]; !ok {
		t.Errorf("stage A missing get op")
	}
	if len(snapA) != 2 {
		t.Errorf("stage A should have exactly 2 ops, got %d", len(snapA))
	}

	snapB := r.SnapshotStage("B")
	if len(snapB) != 1 {
		t.Errorf("stage B should have exactly 1 op, got %d", len(snapB))
	}
	if _, ok := snapB["put"]; !ok {
		t.Errorf("stage B missing put op")
	}
}
