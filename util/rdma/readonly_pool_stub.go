//go:build !(linux && rdma)

package rdma

import "errors"

// ReadOnlyConnPool stub for non-RDMA builds. Phase A is RDMA-only,
// so the SDK never instantiates this on a non-RDMA build — but the
// type and Stub Close exist so the call sites compile cleanly.

type ReadOnlyConnPool struct{}

type readOnlyConnStats struct {
	Tracked int
	Alive   int
	Faulted int
}

func NewReadOnlyConnPool(_ RDMAPoolConfig) (*ReadOnlyConnPool, error) {
	return nil, errors.New("rdma: ReadOnlyConnPool unavailable in non-RDMA build")
}

func (p *ReadOnlyConnPool) ConnIfReady(_ string) (*RDMAConn, bool) {
	return nil, false
}

func (p *ReadOnlyConnPool) Close() {}

func (p *ReadOnlyConnPool) Stats() readOnlyConnStats {
	return readOnlyConnStats{}
}
