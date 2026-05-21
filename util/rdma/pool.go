//go:build linux && rdma

package rdma

// NewRDMAConnPool creates a slot pool that dials real RDMA connections
// to remote DataNodes. The pool's slot accounting (round-robin allocation,
// blocking on full, dirty exclusion) is in slot_pool.go and shared with
// non-rdma builds; this file just wires the Dial dependency.
func NewRDMAConnPool(cfg RDMAPoolConfig) (*RDMAConnPool, error) {
	return newPool(cfg, Dial)
}
