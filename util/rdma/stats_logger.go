//go:build linux && rdma

package rdma

import (
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// StartStatsLogger spawns a single 60 s periodic logger that prints
// aggregated RDMA stats for the calling process. Safe across multiple
// callers in one process (init is sync.Once-guarded) — repeated calls
// only refresh the name label.
//
// The implementation reads existing Prometheus counters every interval,
// so the call adds ZERO per-request overhead. It does an O(N) walk of
// metric children at log time (N = number of {addr, role, reason}
// label combinations, typically < 30) which costs micros every
// 60 seconds and is invisible at scale.
//
// Output format (single INFO line, easy to grep):
//
//	RDMA stats[OBJECT_NODE] req=+12450 fb=+283 hit=97.78% (cum req=82193 fb=1241)
//	  fallback by reason: small_payload=210 large_payload=73
//
// "+" prefixed numbers are per-window deltas. Cumulative totals at the
// end show lifetime counts. The reason breakdown is only printed when
// non-zero, to keep the line short in healthy state.
//
// Operator usage: grep '^.* RDMA stats' /var/log/... to confirm the
// pool is actually getting traffic and what proportion is falling
// back. Replaces the need for ad-hoc per-request logging.
func StartStatsLogger(callerName string) {
	statsLoggerName.Store(callerName)
	statsLoggerOnce.Do(func() {
		go statsLoggerLoop()
	})
}

var (
	statsLoggerOnce sync.Once
	statsLoggerName sync.Map // single key "name" → string; sync.Map for atomic store
)

func loadStatsLoggerName() string {
	v, ok := statsLoggerName.Load("name")
	if !ok {
		return "rdma"
	}
	return v.(string)
}

const statsLoggerInterval = 60 * time.Second

func statsLoggerLoop() {
	ticker := time.NewTicker(statsLoggerInterval)
	defer ticker.Stop()
	var prevReq, prevFb uint64
	for range ticker.C {
		req, fb, byReason := readRDMAStatsSnapshot()
		dReq, dFb := req-prevReq, fb-prevFb
		prevReq, prevFb = req, fb
		if dReq+dFb == 0 {
			continue // quiet: don't spam logs when nothing is happening
		}
		var hit float64
		if dReq+dFb > 0 {
			hit = 100 * float64(dReq) / float64(dReq+dFb)
		}
		name := loadStatsLoggerName()
		// Emit one INFO line — easy to grep, no per-request churn.
		if dFb > 0 {
			log.LogInfof("RDMA stats[%s] req=+%d fb=+%d hit=%.2f%% (cum req=%d fb=%d)  fallback: %s",
				name, dReq, dFb, hit, req, fb, formatReasonMap(byReason))
		} else {
			log.LogInfof("RDMA stats[%s] req=+%d fb=+%d hit=%.2f%% (cum req=%d fb=%d)",
				name, dReq, dFb, hit, req, fb)
		}
	}
}

// readRDMAStatsSnapshot walks the existing Prometheus counter vectors
// (registered in init()) and returns aggregated request count,
// fallback count, and fallback breakdown by reason. Pure read — does
// not touch metric collectors with anything more than a Collect()
// channel send.
func readRDMAStatsSnapshot() (req, fb uint64, byReason map[string]uint64) {
	byReason = make(map[string]uint64, 4)
	req = sumCollector(metricRequestsTotal)
	fb, byReason = sumFallbackCollector(metricFallbackTotal)
	return
}

func sumCollector(c prometheus.Collector) uint64 {
	ch := make(chan prometheus.Metric, 64)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	var total uint64
	var dtoM dto.Metric
	for m := range ch {
		dtoM.Reset()
		if err := m.Write(&dtoM); err != nil {
			continue
		}
		if dtoM.Counter != nil {
			total += uint64(dtoM.Counter.GetValue())
		}
	}
	return total
}

func sumFallbackCollector(c prometheus.Collector) (uint64, map[string]uint64) {
	ch := make(chan prometheus.Metric, 64)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	var total uint64
	byReason := make(map[string]uint64, 4)
	var dtoM dto.Metric
	for m := range ch {
		dtoM.Reset()
		if err := m.Write(&dtoM); err != nil {
			continue
		}
		if dtoM.Counter == nil {
			continue
		}
		v := uint64(dtoM.Counter.GetValue())
		total += v
		var reason string
		for _, lp := range dtoM.Label {
			if lp.GetName() == "reason" {
				reason = lp.GetValue()
				break
			}
		}
		if reason == "" {
			reason = "unknown"
		}
		byReason[reason] += v
	}
	return total, byReason
}

func formatReasonMap(m map[string]uint64) string {
	if len(m) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var s string
	for i, k := range keys {
		if i > 0 {
			s += " "
		}
		s += k + "=" + uintToString(m[k])
	}
	return s
}

func uintToString(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
