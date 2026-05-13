// Copyright 2018 The CubeFS Authors.
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

package stream

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrExtentNotPhaseAEligible is returned by the lookup path when the
// server reported OpNotExistErr for an extent (e.g. orphan zero-size
// file on disk left over from a write-recovery cycle). The Phase A
// path treats this as a definitive "this extent will never serve a
// one-sided read", so callers should fall back to two-sided AND
// (eventually) negative-cache the result to avoid hammering the
// server with the same hopeless lookup every TTL window.
var ErrExtentNotPhaseAEligible = errors.New("rdma Phase A: extent not eligible (server reported NotExist)")

// phaseASlowChunkThreshold is the per-chunk RDMA Read latency above
// which the chunk is counted in slowChunks and a sampled WARN log is
// emitted (in readChunkViaRDMARead). 500 ms is well above the healthy
// expectation (a 4-MiB Phase A Read on a 25-Gbps RoCE link should
// complete in single-digit milliseconds, with mlx5 buffer pressure
// adding tens) but well below the 2-second tail observed in
// production. The threshold catches the bad tail without spamming
// during normal warmup variance.
const phaseASlowChunkThreshold = 500 * time.Millisecond

// phaseASlowChunkWarnEvery samples the SLOW WARN log. Tighter than
// phaseAWarnEvery (256) on purpose: slow chunks are themselves the
// rare-tail events we're diagnosing, so 1/256 leaves the operator
// with one log line per ~thousand chunks — not enough to spot the
// pattern of which connIdx / datanode clusters they on. 1/10
// trades a small bump in log volume for actually usable evidence.
const phaseASlowChunkWarnEvery = 10

// phaseAStalledChunkThreshold is the secondary bar above which EVERY
// occurrence logs (no sampling). 2 s is the readViaRDMAReadTimeout
// default — at this point the WR was effectively in queueing / fault
// territory rather than slow execution, and the operator wants every
// instance to surface so they can correlate with TCP fallback events.
const phaseAStalledChunkThreshold = 2 * time.Second

// Phase A observability counters live in this build-tag-free file so
// the (also build-tag-free) phase_a_stats.go logger can read them in
// both the RDMA and non-RDMA builds. The actual RDMA path
// (extent_reader_rdma_read.go behind //go:build linux && rdma) is the
// only writer; on stub builds these counters stay at zero, which the
// stats logger renders as a quiet "no traffic" line — exactly what we
// want for visibility into "is RDMA even being attempted in this
// binary?"

// phaseACounters holds the atomic counters incremented by the Phase A
// hot path. Reads are via *Snapshot helpers below so the logger gets a
// consistent (not mid-update) view of one counter at a time — across
// counters the snapshot is best-effort, which is fine for periodic
// stats. All fields are int64 + Load/Add helpers (Go 1.17 compat —
// atomic.Int64 is Go 1.19+).
var phaseACounters struct {
	attempt     int64 // tryReadViaRDMARead called at all
	success     int64 // returned (n>0, nil)
	noCacheInit int64 // cache was never initialised (returned 0,nil)
	lookupErr   int64 // cache.Get returned err
	boundsErr   int64 // read range outside lease.Size
	connErr     int64 // rdmaConnPool.ConnForKey failed
	wrErr       int64 // PostRDMAReadAndWait failed
	bytes       int64 // total bytes transferred via RDMA Read (client-side)
	// chunk-count distribution per tryReadViaRDMARead call. Buckets:
	//   chunks1     — single-chunk fast path
	//   chunks2to4  — 2..4 chunks parallel
	//   chunks5plus — ≥ 5 chunks parallel
	// Verifies whether ObjectNode's GET buffer size is large enough to
	// trigger multi-chunk parallelism on the Phase A path.
	chunks1     int64
	chunks2to4  int64
	chunks5plus int64
	// slowChunks counts per-chunk RDMA Reads whose end-to-end time
	// exceeded phaseASlowChunkThreshold. Diagnostic for the long-tail
	// "max sdkRead 2 s" observation: pairs with the sampled WARN log
	// in readChunkViaRDMARead so the cumulative bad-chunk rate is
	// visible at the periodic stats interval rather than only as
	// individual log lines.
	slowChunks int64
}

// phaseAConnIdxMax bounds the per-conn-index hit array. Set high enough
// to cover any reasonable maxConns. If maxConns exceeds this the higher
// indices just don't get instrumented — the metric undercounts but
// doesn't corrupt. A startup log line in objectnode/rdma_init records
// the cap so an operator knows when they've outgrown it.
const phaseAConnIdxMax = 64

// phaseAConnIdxHits is per-conn-index hit count. Index = fnvHash32(key)
// mod maxConns — same as slot_pool.ConnIfReadyForKey, so the histogram
// lines up exactly with which conn slot got dialed.
var phaseAConnIdxHits [phaseAConnIdxMax]int64

// PhaseAStatsSnapshot returns the atomic counter values for the stats
// logger to compute deltas. Always available (build-tag-free) so the
// logger compiles in both RDMA and stub builds. In stub builds every
// counter stays zero.
func PhaseAStatsSnapshot() (attempt, success, noCache, lookup, bounds, conn, wr, bytes int64) {
	return atomic.LoadInt64(&phaseACounters.attempt),
		atomic.LoadInt64(&phaseACounters.success),
		atomic.LoadInt64(&phaseACounters.noCacheInit),
		atomic.LoadInt64(&phaseACounters.lookupErr),
		atomic.LoadInt64(&phaseACounters.boundsErr),
		atomic.LoadInt64(&phaseACounters.connErr),
		atomic.LoadInt64(&phaseACounters.wrErr),
		atomic.LoadInt64(&phaseACounters.bytes)
}

// PhaseAChunkBucketsSnapshot returns the cumulative chunk-count
// distribution buckets. Used by the stats logger to verify multi-chunk
// parallelism is actually being exercised — when ObjectNode reads in
// small (e.g. 256 KiB) buffers, every Phase A call is a single chunk
// regardless of how high readPrefetchDepth is set.
func PhaseAChunkBucketsSnapshot() (chunks1, chunks2to4, chunks5plus int64) {
	return atomic.LoadInt64(&phaseACounters.chunks1),
		atomic.LoadInt64(&phaseACounters.chunks2to4),
		atomic.LoadInt64(&phaseACounters.chunks5plus)
}

// PhaseAConnIdxHitsSnapshot returns a copy of the per-conn-index hit
// counters. Lets the stats logger detect skewed hash distribution
// (everything piling on index 0 means hash routing isn't spreading
// load across the configured maxConns).
func PhaseAConnIdxHitsSnapshot() [phaseAConnIdxMax]int64 {
	var out [phaseAConnIdxMax]int64
	for i := range phaseAConnIdxHits {
		out[i] = atomic.LoadInt64(&phaseAConnIdxHits[i])
	}
	return out
}

// PhaseASlowChunksSnapshot returns the cumulative count of chunks
// whose RDMA Read exceeded phaseASlowChunkThreshold. Surfaces the
// long-tail rate in periodic logs even when individual WARN lines
// have been sampled out.
func PhaseASlowChunksSnapshot() int64 {
	return atomic.LoadInt64(&phaseACounters.slowChunks)
}

// recordPhaseASlowChunk increments the slow-chunk counter and returns
// the post-increment value so the caller can decide whether to also
// emit a sampled WARN log (matching the phaseAShouldWarn cadence).
func recordPhaseASlowChunk() int64 {
	return atomic.AddInt64(&phaseACounters.slowChunks, 1)
}

// recordPhaseAChunkBucket increments the chunk-count distribution
// bucket matching the chunks count. Called once per successful Phase A
// read.
func recordPhaseAChunkBucket(chunks int) {
	switch {
	case chunks <= 1:
		atomic.AddInt64(&phaseACounters.chunks1, 1)
	case chunks <= 4:
		atomic.AddInt64(&phaseACounters.chunks2to4, 1)
	default:
		atomic.AddInt64(&phaseACounters.chunks5plus, 1)
	}
}

// recordPhaseAConnIdx bumps the bucket for the conn index chosen for
// this read. Out-of-range indices (when maxConns > phaseAConnIdxMax)
// are silently dropped.
func recordPhaseAConnIdx(idx int) {
	if idx < 0 || idx >= phaseAConnIdxMax {
		return
	}
	atomic.AddInt64(&phaseAConnIdxHits[idx], 1)
}

// phaseAWarnEvery samples failure logs so a permanent fault doesn't
// spam the log file — but the first occurrence is always logged so the
// first deploy surfaces the issue. Lives in this build-tag-free file
// so phaseAShouldWarn below (also tag-free) compiles in both builds.
const phaseAWarnEvery = 256

// phaseAShouldWarn returns true on count==1 (first occurrence) and
// every phaseAWarnEvery-th thereafter. Cheap atomic Add — fine on the
// hot path. Build-tag-free so both RDMA and stub builds can sample
// warnings consistently (stubs don't currently warn, but keeping the
// helper here means a future stub path could without dragging the
// counter declaration around).
func phaseAShouldWarn(counter *int64) bool {
	n := atomic.AddInt64(counter, 1)
	return n == 1 || n%phaseAWarnEvery == 0
}
