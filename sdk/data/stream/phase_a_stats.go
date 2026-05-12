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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// StartPhaseAStatsLogger spawns a single 60s loop that prints a one-line
// summary of Phase A (one-sided RDMA Read) traffic. Mirrors the format
// of util/rdma's StartStatsLogger but lives here because it reads
// SDK-package counters and cannot live below the import boundary.
//
// Output format (grep-friendly):
//
//	Phase A stats[OBJECT_NODE] attempt=+1234 success=+1200 (hit=97.2%)
//	  fail: noCache=0 lookup=10 bounds=0 conn=0 wr=24
//
// Counters are cumulative since process start; per-window deltas come
// from comparing successive snapshots. The "+" prefix marks deltas;
// raw cumulative totals print only when an operator runs the line
// through grep -A 1 (they live on the optional second line below the
// summary).
//
// Idempotent: callers can invoke this once per binary role
// (ObjectNode / cfs-client / DataNode) and the most-recent name wins.
func StartPhaseAStatsLogger(callerName string) {
	phaseAStatsName.Store(callerName)
	phaseAStatsOnce.Do(func() {
		go phaseAStatsLoop()
	})
}

var (
	phaseAStatsOnce sync.Once
	phaseAStatsName atomicString
)

// atomicString is a tiny atomic.Value wrapper restricted to string —
// avoids the runtime type assertion at every load by giving the load
// site a typed accessor.
type atomicString struct {
	v sync.Mutex
	s string
}

func (a *atomicString) Store(s string) {
	a.v.Lock()
	a.s = s
	a.v.Unlock()
}
func (a *atomicString) Load() string {
	a.v.Lock()
	defer a.v.Unlock()
	return a.s
}

const phaseAStatsInterval = 60 * time.Second

func phaseAStatsLoop() {
	ticker := time.NewTicker(phaseAStatsInterval)
	defer ticker.Stop()
	var prev struct {
		attempt, success, noCache, lookup, bounds, conn, wr, bytes int64
		chunks1, chunks2to4, chunks5plus                           int64
		slowChunks                                                 int64
	}
	for range ticker.C {
		attempt, success, noCache, lookup, bounds, conn, wr, bytes := PhaseAStatsSnapshot()
		c1, c2to4, c5plus := PhaseAChunkBucketsSnapshot()
		idxHits := PhaseAConnIdxHitsSnapshot()
		slow := PhaseASlowChunksSnapshot()
		dAttempt := attempt - prev.attempt
		dSuccess := success - prev.success
		dNoCache := noCache - prev.noCache
		dLookup := lookup - prev.lookup
		dBounds := bounds - prev.bounds
		dConn := conn - prev.conn
		dWr := wr - prev.wr
		dBytes := bytes - prev.bytes
		dC1 := c1 - prev.chunks1
		dC2to4 := c2to4 - prev.chunks2to4
		dC5plus := c5plus - prev.chunks5plus
		dSlow := slow - prev.slowChunks
		prev.attempt, prev.success = attempt, success
		prev.noCache, prev.lookup = noCache, lookup
		prev.bounds, prev.conn, prev.wr = bounds, conn, wr
		prev.bytes = bytes
		prev.chunks1, prev.chunks2to4, prev.chunks5plus = c1, c2to4, c5plus
		prev.slowChunks = slow

		// Publish to Prometheus so Grafana can show RDMA Read bandwidth.
		// Called from a single goroutine — no data race on the counter.
		if dBytes > 0 {
			exporter.NewCounter("phaseAReadBytes").Add(dBytes)
		}

		if dAttempt == 0 {
			// Quiet line so absence of Phase A traffic is itself a
			// signal — without this, "no log" could mean either
			// "stats off" or "no traffic". The cum= part lets an
			// operator confirm process uptime. Pool health follows
			// on the same line so operators don't need to grep
			// elsewhere to know "did Phase A's dedicated conns
			// even come up?"
			log.LogInfof("Phase A stats[%s] attempt=+0 (cum attempt=%d success=%d) — no one-sided traffic; %s",
				phaseAStatsName.Load(), attempt, success, phaseAPoolHealth())
			continue
		}
		hit := 100.0 * float64(dSuccess) / float64(dAttempt)
		mbps := float64(dBytes) / float64(phaseAStatsInterval/time.Second) / 1e6
		log.LogInfof("Phase A stats[%s] attempt=+%d success=+%d (hit=%.1f%%) bytes=+%d (%.1f MB/s) fail: noCache=%d lookup=%d bounds=%d conn=%d wr=%d (cum attempt=%d success=%d); %s",
			phaseAStatsName.Load(), dAttempt, dSuccess, hit, dBytes, mbps,
			dNoCache, dLookup, dBounds, dConn, dWr,
			attempt, success, phaseAPoolHealth())
		// Chunk-count distribution: confirms whether the multi-chunk
		// parallel path is being exercised. If everything is in chunks1
		// the upper layer's buffer is too small to split — see
		// objectnode/get_object_bufsize.go.
		log.LogInfof("Phase A chunks[%s] +chunks1=%d +chunks2to4=%d +chunks5plus=%d slowChunks=+%d (cum=%d, threshold=%v)",
			phaseAStatsName.Load(), dC1, dC2to4, dC5plus, dSlow, slow, phaseASlowChunkThreshold)
		// Per-conn-index hit distribution. Shows whether hash routing
		// is spreading load across the configured maxConns. Skip zero
		// buckets so the line stays short on small-maxConns deployments.
		log.LogInfof("Phase A connIdx[%s] %s", phaseAStatsName.Load(), formatPhaseAConnIdx(idxHits))
	}
}

// formatPhaseAConnIdx renders the per-index hit array compactly,
// omitting indices with zero hits. e.g. "idx0=12345 idx3=23456 idx7=8901"
// — that pattern flags clear hash skew at a glance.
func formatPhaseAConnIdx(hits [phaseAConnIdxMax]int64) string {
	var b strings.Builder
	first := true
	for i, n := range hits {
		if n == 0 {
			continue
		}
		if !first {
			b.WriteByte(' ')
		}
		first = false
		fmt.Fprintf(&b, "idx%d=%d", i, n)
	}
	if first {
		return "(no hits yet)"
	}
	return b.String()
}
