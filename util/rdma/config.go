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
	// ReadSlotCount and ReadSlotSize size the Phase A (one-sided
	// RDMA Read) scratch pool — independent from NumSlots/SlotSize,
	// which only serve the two-sided send/recv path. ReadSlots are
	// allocated lazily on the first PostRDMAReadAndWait call. Zero
	// values fall back to defaults defined alongside read_waiter.go.
	//
	// Larger ReadSlotSize cuts the number of WRs per object (16 MB
	// object / 4 MB slot = 4 WRs vs 128 at 128 KB), which is the
	// dominant overhead on the Phase A hot path. The trade-off is
	// scratch memory: ReadSlotCount × ReadSlotSize per conn.
	ReadSlotCount int
	ReadSlotSize  int
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
	// RDMAPortShift translates the caller-supplied (typically TCP) port
	// to the peer's RDMA listen port. Callers pass the peer's data
	// address (e.g. host:17310) and the pool/SDK shifts the port before
	// dialing (e.g. host:17350). Zero means "no shift". Must be uniform
	// across the cluster — if peers run with different shifts, the
	// caller is responsible for passing pre-shifted addresses and
	// leaving this at 0.
	RDMAPortShift int

	// ReadSlotCount / ReadSlotSize are propagated to RDMAConnConfig
	// for every conn the pool dials. They size the Phase A read
	// scratch pool. Zero means "use defaults" (see read_waiter.go).
	ReadSlotCount int
	ReadSlotSize  int

	// OneSidedReadDisabled, when true, makes the SDK skip the Phase A
	// one-sided RDMA Read entry point entirely and use only the
	// two-sided path. Provides an instant rollback for one-sided
	// regressions without rebuilding the binary — set to true at
	// startup (e.g. from a mount option), no code change. Default
	// false = Phase A active.
	OneSidedReadDisabled bool

	// ReadTimeoutMs caps a single Phase A RDMA Read WR's wait for
	// completion before the SDK abandons and falls back to the two-
	// sided path. Healthy RoCE round-trips are ~100 μs and a busy
	// server adds at most tens of ms; 1000 ms (the default) gives
	// 10000× headroom over healthy RTT, plenty for transient queue
	// or scheduler hiccups, and dropped tail-latency dramatically vs
	// the original 5 s. Set lower for read-heavy workloads on quiet
	// fabrics where you'd rather fall back fast than wait. 0 = use
	// default.
	ReadTimeoutMs int

	// ReadPrefetchDepth caps how many ReadBlockSize-sized chunks
	// the SDK posts in parallel for one ExtentReader.Read call.
	// Higher = more in-flight WRs per object = better NIC pipelining
	// at the cost of memory pressure (each in-flight chunk holds
	// one Phase A read scratch slot). 0 = use SDK default
	// (defaultReadPrefetchDepth in sdk/data/stream/extent_reader.go).
	ReadPrefetchDepth int
}
