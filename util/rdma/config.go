// Build-tag-free configuration types for the RDMA transport.
//
// Earlier revisions duplicated these structs across stub.go (non-RDMA
// builds) and conn.go (linux+rdma). That worked but made adding a field
// risky — drift between the two definitions would silently compile only
// in one build. Centralising here removes that hazard and lets both
// build-tagged files share the type.

package rdma

import "time"

// RDMAConnConfig holds per-connection sizing and behaviour parameters.
type RDMAConnConfig struct {
	NumSlots int
	SlotSize int
	// CreditAckMode controls whether the receiver waits for the
	// credit-return RDMA Write's CQE before processing the next slot.
	CreditAckMode CreditAckMode
	// Poll governs the busy → yield → sleep behaviour of every polling
	// site owned by this connection. Zero value means "use
	// DefaultPollConfig".
	Poll PollConfig
	// Role labels the connection for Prometheus metrics. Set to
	// RoleClient (SDK), RoleFollower (leader→follower replication), or
	// RoleServer (DataNode-side accepted conn). Empty string disables
	// metric emission for the conn.
	Role string
}

// RDMAPoolConfig configures a slot pool for one or more remote addresses.
type RDMAPoolConfig struct {
	Device        string
	Port          int
	NumSlots      int
	SlotSize      int
	MaxConns      int
	IdleTimeout   time.Duration
	CreditAckMode CreditAckMode
	Poll          PollConfig
	// Role propagates to every conn the pool dials; see
	// RDMAConnConfig.Role.
	Role string
	// MinPayloadBytes is the threshold below which the SDK should skip
	// the RDMA path and use TCP. Below ~4 KB the two-WR overhead of an
	// RDMA round trip outweighs the latency benefit; small control
	// packets and meta updates traverse TCP unconditionally. Zero means
	// "no threshold" — every payload tries RDMA. (P6)
	MinPayloadBytes int
}
