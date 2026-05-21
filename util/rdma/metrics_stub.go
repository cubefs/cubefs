//go:build !(linux && rdma)

// No-op metric shims for non-RDMA builds. Spec requirement: when the
// transport isn't compiled in, no cubefs_rdma_* metrics should appear at
// the /metrics endpoint, and call sites must keep compiling without
// build-tag guards.

package rdma

import "time"

const (
	RoleClient   = "client"
	RoleFollower = "follower"
	RoleServer   = "server"
)

func MetricsObserveRequest(role, addr string, latency time.Duration) {}
func MetricsObserveFallback(role, addr, reason string)               {}
func MetricsObserveSlotWait(role, addr string, wait time.Duration)   {}
func MetricsIncPollSpin(role, addr, phase string)                    {}
func MetricsIncCreditStall(role, addr string)                        {}
func MetricsSetActiveSlots(role, addr string, n int)                 {}

// StartStatsLogger is a no-op in non-RDMA builds. Stub exists so
// callers (FUSE / ObjectNode / cfs-sync init) don't need build-tag
// guards.
func StartStatsLogger(_ string) {}

// Internal aliases mirror those in metrics.go so package-internal callers
// can share the lowercase names across builds.
var (
	metricsObserveRequest  = MetricsObserveRequest
	metricsObserveFallback = MetricsObserveFallback
	metricsObserveSlotWait = MetricsObserveSlotWait
	metricsIncPollSpin     = MetricsIncPollSpin
	metricsIncCreditStall  = MetricsIncCreditStall
	metricsSetActiveSlots  = MetricsSetActiveSlots
)
