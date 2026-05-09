//go:build linux && rdma

// Prometheus metrics for the RDMA transport (P3 of
// docs/plan/rdma-optimization-spec.md).
//
// All metrics carry at least {role, addr} labels so operators can split
// observations across SDK clients, replication followers, and DataNode
// servers — and identify which peer is involved when fallback / stall
// patterns appear. Metrics are registered against the default Prometheus
// registry at package init time, matching the cubefs convention used by
// util/exporter; they show up at the standard /metrics endpoint without
// further wiring.
//
// On non-RDMA builds (default), metrics_stub.go provides no-op shims so
// nothing is registered with Prometheus and call sites compile cleanly.

package rdma

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// RoleClient identifies SDK-originated RDMA traffic.
	RoleClient = "client"
	// RoleFollower identifies leader→follower replication traffic.
	RoleFollower = "follower"
	// RoleServer identifies DataNode-side accepted connections.
	RoleServer = "server"
)

var (
	metricRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cubefs_rdma_requests_total",
			Help: "Total RDMA send attempts (after credit acquired, before reply).",
		},
		[]string{"role", "addr"},
	)
	metricFallbackTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cubefs_rdma_fallback_total",
			Help: "Total fallbacks from RDMA to TCP, with reason for the fallback.",
		},
		[]string{"role", "addr", "reason"},
	)
	metricLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cubefs_rdma_latency_seconds",
			Help: "Single RDMA round-trip latency in seconds (acquire+send+wait+ack).",
			// Microsecond-friendly buckets matching expected RC RDMA RTTs.
			Buckets: []float64{
				1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4,
				1e-3, 5e-3, 5e-2, 5e-1, 1, 5,
			},
		},
		[]string{"role", "addr"},
	)
	metricSlotWaitSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cubefs_rdma_slot_wait_seconds",
			Help:    "Time spent in AcquireSlot waiting for a free slot.",
			Buckets: []float64{1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1, 10},
		},
		[]string{"role", "addr"},
	)
	metricPollSpinTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cubefs_rdma_poll_spin_total",
			Help: "Adaptive poll iterations by phase (busy / yield / sleep).",
		},
		[]string{"role", "addr", "phase"},
	)
	metricCreditStallTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cubefs_rdma_credit_stall_total",
			Help: "Number of WritePacket calls that had to wait for credit to refill.",
		},
		[]string{"role", "addr"},
	)
	metricActiveSlots = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cubefs_rdma_active_slots",
			Help: "Currently-borrowed slots across all conns in a slot pool.",
		},
		[]string{"role", "addr"},
	)
)

// registerOnce ensures we only register collectors a single time even if
// the package is initialised twice (e.g. test harnesses). The standard
// prometheus.MustRegister panics on duplicate registration; the wrapper
// below treats AlreadyRegisteredError as success so re-imports in tests
// don't blow up.
var registerOnce sync.Once

func init() {
	registerOnce.Do(func() {
		registerOrIgnore(metricRequestsTotal)
		registerOrIgnore(metricFallbackTotal)
		registerOrIgnore(metricLatencySeconds)
		registerOrIgnore(metricSlotWaitSeconds)
		registerOrIgnore(metricPollSpinTotal)
		registerOrIgnore(metricCreditStallTotal)
		registerOrIgnore(metricActiveSlots)
	})
}

func registerOrIgnore(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return
		}
		// Other errors are programming bugs (e.g. metric name collision);
		// fail loudly so they surface in development.
		panic(err)
	}
}

// MetricsObserveRequest counts one RDMA send attempt and records the
// observed RTT in seconds. role / addr are the per-conn labels.
func MetricsObserveRequest(role, addr string, latency time.Duration) {
	if role == "" {
		return
	}
	metricRequestsTotal.WithLabelValues(role, addr).Inc()
	metricLatencySeconds.WithLabelValues(role, addr).Observe(latency.Seconds())
}

// MetricsObserveFallback counts one fallback to TCP. reason should be a
// short stable string (e.g. "init_failed", "wr_error", "no_slot").
func MetricsObserveFallback(role, addr, reason string) {
	if role == "" {
		return
	}
	metricFallbackTotal.WithLabelValues(role, addr, reason).Inc()
}

// MetricsObserveSlotWait records the time AcquireSlot blocked before
// returning a handle. Only called when wait was non-trivial — callers
// should skip the observation on fast-path acquires to avoid scrubbing
// histogram buckets with zero values.
func MetricsObserveSlotWait(role, addr string, wait time.Duration) {
	if role == "" {
		return
	}
	metricSlotWaitSeconds.WithLabelValues(role, addr).Observe(wait.Seconds())
}

// MetricsIncPollSpin increments the per-phase poll counter. phase must be
// one of "busy", "yield", "sleep". Call from each adaptive-poll iteration
// at the level of granularity the caller wants exposed.
func MetricsIncPollSpin(role, addr, phase string) {
	if role == "" {
		return
	}
	metricPollSpinTotal.WithLabelValues(role, addr, phase).Inc()
}

// MetricsIncCreditStall increments the per-conn credit-stall counter.
// Call once per WritePacket whose credit acquisition blocked
// non-trivially (the caller times it).
func MetricsIncCreditStall(role, addr string) {
	if role == "" {
		return
	}
	metricCreditStallTotal.WithLabelValues(role, addr).Inc()
}

// MetricsSetActiveSlots updates the active-slots gauge for one
// {role, addr} pair. Call from the slot pool whenever the count changes
// (acquire / release).
func MetricsSetActiveSlots(role, addr string, n int) {
	if role == "" {
		return
	}
	metricActiveSlots.WithLabelValues(role, addr).Set(float64(n))
}

// Internal aliases for use within the rdma package without forcing
// callers outside the package to learn the lowercase names.
var (
	metricsObserveRequest  = MetricsObserveRequest
	metricsObserveFallback = MetricsObserveFallback
	metricsObserveSlotWait = MetricsObserveSlotWait
	metricsIncPollSpin     = MetricsIncPollSpin
	metricsIncCreditStall  = MetricsIncCreditStall
	metricsSetActiveSlots  = MetricsSetActiveSlots
)
