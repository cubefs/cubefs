package rdma

import (
	"testing"
	"time"
)

// TestMetricsHelpers_NoOpOnEmptyRole ensures the public metric helpers
// silently no-op when role is empty. This is the contract that lets call
// sites drop labels without paying registration cost.
func TestMetricsHelpers_NoOpOnEmptyRole(t *testing.T) {
	// Each helper must accept zero-role labels without panic. We can't
	// directly inspect counter values without round-tripping through the
	// prometheus default registry; this test guards against the trivial
	// cases (panics, missing labels in the matcher).
	MetricsObserveRequest("", "addr", time.Microsecond)
	MetricsObserveFallback("", "addr", "reason")
	MetricsObserveSlotWait("", "addr", time.Microsecond)
	MetricsIncPollSpin("", "addr", "busy")
	MetricsIncCreditStall("", "addr")
	MetricsSetActiveSlots("", "addr", 5)
}

// TestMetricsHelpers_AcceptValidLabels ensures the helpers work for
// well-formed inputs. On rdma builds these increment real counters; on
// stub builds they no-op.
func TestMetricsHelpers_AcceptValidLabels(t *testing.T) {
	MetricsObserveRequest(RoleClient, "10.0.0.1:6000", 100*time.Microsecond)
	MetricsObserveFallback(RoleClient, "10.0.0.1:6000", "wr_error")
	MetricsObserveSlotWait(RoleClient, "10.0.0.1:6000", 50*time.Microsecond)
	MetricsIncPollSpin(RoleClient, "10.0.0.1:6000", "yield")
	MetricsIncCreditStall(RoleClient, "10.0.0.1:6000")
	MetricsSetActiveSlots(RoleClient, "10.0.0.1:6000", 12)

	MetricsObserveRequest(RoleFollower, "10.0.0.2:6000", 80*time.Microsecond)
	MetricsObserveRequest(RoleServer, "client@10.0.0.3", 80*time.Microsecond)
}

// TestRoleConstants pins the spec-mandated role label values so a typo
// in a future refactor doesn't silently shift them.
func TestRoleConstants(t *testing.T) {
	cases := map[string]string{
		RoleClient:   "client",
		RoleFollower: "follower",
		RoleServer:   "server",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("role constant: got %q want %q", got, want)
		}
	}
}
