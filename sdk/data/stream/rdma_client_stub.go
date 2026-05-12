//go:build !(linux && rdma)

package stream

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/rdma"
)

// rdmaEnabled is always false in non-RDMA builds; stream_conn.go guards on rdmaConnPool != nil.
var rdmaConnPool *struct{}

// InitRDMAConnPool is a no-op in non-RDMA builds. Signature mirrors the
// rdma-tagged version so callers can drop conditional build-tag guards.
func InitRDMAConnPool(_ rdma.RDMAPoolConfig) error { return nil }

// sendPacketViaRDMA is never called in non-RDMA builds (rdmaConnPool stays nil).
func sendPacketViaRDMA(_ string, _ *Packet) error { return nil }

// recvPacketViaRDMA is never called in non-RDMA builds (rdmaConnPool stays nil).
// Signature mirrors the rdma-tagged version so the read path in
// extent_reader.go compiles unconditionally.
func recvPacketViaRDMA(_ string, _ *Packet) (*proto.Packet, error) { return nil, nil }

// rdmaRoundTrip is never called in non-RDMA builds (rdmaTryForSize returns
// false so the call site is unreachable). Stub exists so the
// build-tag-free ExtentHandler.sender compiles.
func rdmaRoundTrip(_ string, _ *Packet) (*proto.Packet, error) { return nil, nil }

// rdmaTryForSize always returns false on non-RDMA builds: the path is
// not compiled in, so callers must use TCP.
func rdmaTryForSize(_ string, _ int) bool { return false }

// phaseAPoolHealth returns a static placeholder on non-RDMA builds —
// the Phase A stats goroutine still runs (logger init is build-tag-
// free) but reports the path is disabled. The string format matches
// the rdma-build version so log post-processors can rely on it.
func phaseAPoolHealth() string { return "pool=disabled" }

// GetPhaseAConnPool returns nil on non-RDMA builds. Mirrors the
// rdma-tagged accessor so external instrumentation (objectnode
// get_object_stats.go) compiles unconditionally; callers must
// nil-check before using the returned pool.
func GetPhaseAConnPool() *rdma.RDMAConnPool { return nil }
