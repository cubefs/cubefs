//go:build !(linux && rdma)

package stream

// rdmaEnabled is always false in non-RDMA builds; stream_conn.go guards on rdmaConnPool != nil.
var rdmaConnPool *struct{}

// InitRDMAConnPool is a no-op in non-RDMA builds.
func InitRDMAConnPool(_, _ int) error { return nil }

// sendPacketViaRDMA is never called in non-RDMA builds (rdmaConnPool stays nil).
func sendPacketViaRDMA(_ string, _ *Packet) error { return nil }
