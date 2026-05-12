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

package objectnode

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/rdma"
)

// Observability shim for the GET-object hot loop. Records, for every
// iteration of Volume.read's "Read N from SDK → Write N to HTTP"
// lockstep loop:
//   - how long the SDK Read call took
//   - how long the HTTP Write call took
//   - how many bytes moved
//
// Plus a process-global gauge of concurrent in-flight GET handlers.
// A periodic logger prints averages, max latency, and the in-flight
// gauge alongside the existing Phase A stats so all of the GET-path
// signals show up in the same place when triaging throughput plateaus.

// getObjectStats accumulates aggregate counters since the last stats
// dump. Per-iteration timing is tracked as sum/count/max — enough to
// compute mean and surface the slow tail without the memory cost of
// a real histogram. If we later need percentiles we'll add buckets.
//
// All fields are accessed via atomic helpers (Go 1.17 compat — no
// atomic.Int64 yet). Reset to zero at every dump.
var getObjectStats struct {
	// iteration accumulators
	iterations  int64 // count of Volume.read inner loops completed
	totalBytes  int64 // total bytes copied to writer
	sdkReadNs   int64 // sum of SDK Read durations
	httpWriteNs int64 // sum of HTTP Write durations
	sdkReadMax  int64 // worst single SDK Read duration
	httpWriteMx int64 // worst single HTTP Write duration

	// in-flight gauge: incremented when a getObject reaches readFile,
	// decremented when readFile returns. Peak in current interval is
	// what's interesting — track separately so the periodic logger can
	// reset peak without disturbing the live counter.
	inFlight   int64
	peakInFlt  int64
	totalCalls int64 // number of readFile entries since process start
}

// recordGetObjectIteration is called from Volume.read once per inner
// loop iteration. nanos arguments are time.Since deltas; bytes is the
// payload size of this iteration.
func recordGetObjectIteration(sdkReadNs, httpWriteNs, bytes int64) {
	atomic.AddInt64(&getObjectStats.iterations, 1)
	atomic.AddInt64(&getObjectStats.totalBytes, bytes)
	atomic.AddInt64(&getObjectStats.sdkReadNs, sdkReadNs)
	atomic.AddInt64(&getObjectStats.httpWriteNs, httpWriteNs)
	// CAS-update max. Loop bounded — contention is rare on int64 max.
	for {
		cur := atomic.LoadInt64(&getObjectStats.sdkReadMax)
		if sdkReadNs <= cur {
			break
		}
		if atomic.CompareAndSwapInt64(&getObjectStats.sdkReadMax, cur, sdkReadNs) {
			break
		}
	}
	for {
		cur := atomic.LoadInt64(&getObjectStats.httpWriteMx)
		if httpWriteNs <= cur {
			break
		}
		if atomic.CompareAndSwapInt64(&getObjectStats.httpWriteMx, cur, httpWriteNs) {
			break
		}
	}
}

// enterGetObject records that a GET handler entered readFile. Returns
// a release function that the caller must defer-call so the in-flight
// gauge stays correct on every exit path.
func enterGetObject() func() {
	atomic.AddInt64(&getObjectStats.totalCalls, 1)
	now := atomic.AddInt64(&getObjectStats.inFlight, 1)
	for {
		cur := atomic.LoadInt64(&getObjectStats.peakInFlt)
		if now <= cur {
			break
		}
		if atomic.CompareAndSwapInt64(&getObjectStats.peakInFlt, cur, now) {
			break
		}
	}
	return func() { atomic.AddInt64(&getObjectStats.inFlight, -1) }
}

// StartGetObjectStatsLogger spawns a single 10s loop printing one line
// of GET-path stats. Idempotent. 10s (vs the Phase A logger's 60s) is
// chosen deliberately: when triaging a throughput plateau the operator
// often runs a 30-60s s3bench burst and wants several data points
// across the burst, not one summary at the end. Production deployments
// past the diagnosis phase can flip the interval up.
func StartGetObjectStatsLogger() {
	getObjectStatsOnce.Do(func() {
		log.LogInfof("ObjectNode GET stats: logger started, interval=%s; "+
			"getObjectBufSize=%d bytes (default 256 KiB; raise via %q to enable Phase A multi-chunk)",
			getObjectStatsInterval, getObjectBufSize(), configGetObjectBufSize)
		go getObjectStatsLoop()
	})
}

var (
	getObjectStatsOnce     sync.Once
	getObjectStatsInterval = 10 * time.Second
)

func getObjectStatsLoop() {
	ticker := time.NewTicker(getObjectStatsInterval)
	defer ticker.Stop()
	var prev struct {
		iter, bytes, sdkNs, httpNs, calls int64
	}
	for range ticker.C {
		iter := atomic.LoadInt64(&getObjectStats.iterations)
		bytes := atomic.LoadInt64(&getObjectStats.totalBytes)
		sdkNs := atomic.LoadInt64(&getObjectStats.sdkReadNs)
		httpNs := atomic.LoadInt64(&getObjectStats.httpWriteNs)
		calls := atomic.LoadInt64(&getObjectStats.totalCalls)
		// Reset peak/max each interval — operator wants "what was the
		// worst in the last window", not "ever". inFlight is a live
		// gauge, not reset.
		sdkMax := atomic.SwapInt64(&getObjectStats.sdkReadMax, 0)
		httpMx := atomic.SwapInt64(&getObjectStats.httpWriteMx, 0)
		peak := atomic.SwapInt64(&getObjectStats.peakInFlt, 0)
		inFlt := atomic.LoadInt64(&getObjectStats.inFlight)
		// Bring the peak floor up to the current live count so the next
		// interval can detect a strictly higher peak. Without this the
		// gauge "forgets" the in-progress GETs at every reset.
		atomic.CompareAndSwapInt64(&getObjectStats.peakInFlt, 0, inFlt)

		dIter := iter - prev.iter
		dBytes := bytes - prev.bytes
		dSdkNs := sdkNs - prev.sdkNs
		dHttpNs := httpNs - prev.httpNs
		dCalls := calls - prev.calls
		prev.iter, prev.bytes = iter, bytes
		prev.sdkNs, prev.httpNs = sdkNs, httpNs
		prev.calls = calls

		// Pool snapshots taken once per interval (locks the pool's
		// mutex briefly — not the hot path).
		connsPerAddr := rdmaPhaseAConnsByAddr()

		if dIter == 0 && dCalls == 0 {
			log.LogInfof("ObjectNode GET stats: idle (no iterations; cum iter=%d bytes=%d calls=%d inFlight=%d); phaseAConns=%s",
				iter, bytes, calls, inFlt, formatConnsMap(connsPerAddr))
			continue
		}
		var avgSdkUs, avgHttpUs, mbps float64
		if dIter > 0 {
			avgSdkUs = float64(dSdkNs) / float64(dIter) / 1000.0
			avgHttpUs = float64(dHttpNs) / float64(dIter) / 1000.0
		}
		if dt := getObjectStatsInterval.Seconds(); dt > 0 {
			mbps = float64(dBytes) / dt / 1e6
		}
		log.LogInfof("ObjectNode GET stats: +iter=%d +bytes=%d (%.1f MB/s) avg sdkRead=%.0fus httpWrite=%.0fus max sdkRead=%.0fus httpWrite=%.0fus +calls=%d inFlight=%d peak=%d; phaseAConns=%s",
			dIter, dBytes, mbps,
			avgSdkUs, avgHttpUs,
			float64(sdkMax)/1000.0, float64(httpMx)/1000.0,
			dCalls, inFlt, peak,
			formatConnsMap(connsPerAddr))
	}
}

// rdmaPhaseAConnsByAddr snapshots the Phase A pool's live conn-per-addr
// counts. Returns nil when Phase A is disabled — the logger renders
// that as an empty map.
func rdmaPhaseAConnsByAddr() map[string]int {
	pool := stream.GetPhaseAConnPool()
	if pool == nil {
		return nil
	}
	return pool.ActiveConnsByAddr()
}

func formatConnsMap(m map[string]int) string {
	if len(m) == 0 {
		return "(none)"
	}
	// Order-insensitive; logger output is for humans — Go map iteration
	// is fine.
	var b []byte
	first := true
	for addr, n := range m {
		if !first {
			b = append(b, ' ')
		}
		first = false
		b = append(b, addr...)
		b = append(b, '=')
		b = append(b, []byte(itoa(n))...)
	}
	return string(b)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// rdma import guard — ensures Go doesn't drop the import when only the
// type-level use above is present. (We use rdma.RDMAConnPool indirectly
// through stream.GetPhaseAConnPool's return type.) Without this Go's
// import resolution would still keep it, but the explicit reference
// makes the dependency obvious to a reader.
var _ = rdma.HashKeyToConnIndex
