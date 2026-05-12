//go:build linux && rdma

package rdma

// NewRDMAConnPool creates a slot pool that dials real RDMA connections
// to remote DataNodes. The pool's slot accounting (round-robin allocation,
// blocking on full, dirty exclusion) is in slot_pool.go and shared with
// non-rdma builds; this file just wires the Dial dependency.
func NewRDMAConnPool(cfg RDMAPoolConfig) (*RDMAConnPool, error) {
	return newPool(cfg, Dial)
}

// NewReadOnlyConnPool creates a Phase A dedicated pool that dials real
// RDMA connections. Same Dial dependency injection pattern as
// NewRDMAConnPool — the pool logic in readonly_pool.go is tag-free so
// unit tests on darwin can run with a mock dial.
func NewReadOnlyConnPool(cfg RDMAPoolConfig) (*ReadOnlyConnPool, error) {
	return newReadOnlyConnPool(cfg, Dial)
}
