//go:build !(linux && rdma)

package stream

import "github.com/cubefs/cubefs/util/rdma"

// rdmaEnabled is always false in non-RDMA builds; stream_conn.go guards on rdmaConnPool != nil.
var rdmaConnPool *struct{}

// InitRDMAConnPool is a no-op in non-RDMA builds. Signature mirrors the
// rdma-tagged version so callers can drop conditional build-tag guards.
func InitRDMAConnPool(_ rdma.RDMAPoolConfig) error { return nil }

// sendPacketViaRDMA is never called in non-RDMA builds (rdmaConnPool stays nil).
func sendPacketViaRDMA(_ string, _ *Packet) error { return nil }
