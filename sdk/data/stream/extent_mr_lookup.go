//go:build linux && rdma

package stream

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/rdma"
)

// Production wiring of extentMRCache (Sprint A.5a) — binds the
// pluggable lookupFn / renewFn callbacks to actual RDMA round-trips
// against an OpExtentMRLookup / OpExtentMRRenew handler on the
// DataNode.

// lookupExtentMR sends OpExtentMRLookup over RDMA and returns the
// parsed LeaseInfo on success. The transport round-trip reuses
// rdmaRoundTrip — the same primitive that backs the existing
// sendPacketViaRDMA / recvPacketViaRDMA wrappers — so lookups
// participate in the same slot pool, hash routing, and timeout
// behaviour as ordinary RDMA traffic.
func lookupExtentMR(addr string, pid, extentID uint64, ttlHint time.Duration) (*LeaseInfo, error) {
	req := &Packet{}
	req.Packet = proto.Packet{
		Magic:    proto.ProtoMagic,
		Opcode:   proto.OpExtentMRLookup,
		ReqID:    proto.GenerateRequestID(),
		ExtentType: proto.NormalExtentType,
	}
	argBuf := make([]byte, rdma.ExtentMRLookupRequestSize)
	argStruct := rdma.ExtentMRLookupRequest{
		PartitionID:      pid,
		ExtentID:         extentID,
		LeaseSecondsHint: uint32(ttlHint.Seconds()),
	}
	if err := argStruct.Marshal(argBuf); err != nil {
		return nil, err
	}
	req.Arg = argBuf
	req.ArgLen = uint32(len(argBuf))
	// PartitionID + ExtentID also stamped on the packet header so the
	// server-side hash routing places the lookup on the same conn as
	// subsequent reads (and so any audit log lines correlate).
	req.PartitionID = pid
	req.ExtentID = extentID

	// Route the lookup through the Phase A pool so the server registers
	// the extent MR against the same PD that will serve the subsequent
	// RDMA Read WRs. Using the two-sided pool here was the cause of the
	// previous "RDMA Read timed out" failure mode: server's lookup conn
	// PD ≠ read conn PD ⟹ rkey didn't decode on the Phase A QP.
	resp, err := rdmaRoundTripVia(rdmaPhaseAConnPool, addr, req)
	if err != nil {
		return nil, err
	}
	if resp.ResultCode != proto.OpOk {
		// OpNotExistErr is the expected response for orphan zero-size
		// extents (left behind by an SDK write-recovery cycle that
		// abandoned the extent before any write landed). The SDK
		// caller correctly falls back to two-sided; surface this as
		// ErrExtentNotPhaseAEligible so the cache layer can choose to
		// negative-cache it and stop hammering the server with the
		// same hopeless lookup every TTL window.
		if resp.ResultCode == proto.OpNotExistErr {
			return nil, ErrExtentNotPhaseAEligible
		}
		return nil, fmt.Errorf("rdma extent MR lookup: server rc=%d", resp.ResultCode)
	}
	if int(resp.ArgLen) < rdma.ExtentMRLookupReplySize {
		return nil, fmt.Errorf("rdma extent MR lookup: short reply arg %d < %d",
			resp.ArgLen, rdma.ExtentMRLookupReplySize)
	}
	var reply rdma.ExtentMRLookupReply
	if err := reply.Unmarshal(resp.Arg[:resp.ArgLen]); err != nil {
		return nil, err
	}
	info := &LeaseInfo{
		Addr:        addr,
		PartitionID: pid,
		ExtentID:    extentID,
		LeaseID:     reply.LeaseID,
		Rkey:        reply.Rkey,
		VA:          reply.VA,
		Size:        reply.Size,
	}
	deadline := time.Now().Add(time.Duration(reply.GrantedSeconds) * time.Second)
	atomic.StoreInt64(&info.expiresAtNanos, deadline.UnixNano())
	return info, nil
}

// renewExtentMR sends OpExtentMRRenew and returns the new granted
// TTL in seconds. Server's OpNotExistErr (lease expired / unknown)
// is surfaced as an error so the cache's renewer can invalidate the
// entry and force a fresh lookup on the next read.
func renewExtentMR(addr string, leaseID uint64, ttlHint time.Duration) (uint32, error) {
	req := &Packet{}
	req.Packet = proto.Packet{
		Magic:    proto.ProtoMagic,
		Opcode:   proto.OpExtentMRRenew,
		ReqID:    proto.GenerateRequestID(),
		ExtentType: proto.NormalExtentType,
	}
	argBuf := make([]byte, rdma.ExtentMRRenewRequestSize)
	argStruct := rdma.ExtentMRRenewRequest{
		LeaseID:          leaseID,
		LeaseSecondsHint: uint32(ttlHint.Seconds()),
	}
	if err := argStruct.Marshal(argBuf); err != nil {
		return 0, err
	}
	req.Arg = argBuf
	req.ArgLen = uint32(len(argBuf))

	// Renew goes through the Phase A pool same as the original lookup
	// — keeps PD consistency for the lease's rkey.
	resp, err := rdmaRoundTripVia(rdmaPhaseAConnPool, addr, req)
	if err != nil {
		return 0, err
	}
	if resp.ResultCode != proto.OpOk {
		return 0, fmt.Errorf("rdma extent MR renew: server rc=%d", resp.ResultCode)
	}
	if int(resp.ArgLen) < rdma.ExtentMRRenewReplySize {
		return 0, errors.New("rdma extent MR renew: short reply arg")
	}
	var reply rdma.ExtentMRRenewReply
	if err := reply.Unmarshal(resp.Arg[:resp.ArgLen]); err != nil {
		return 0, err
	}
	return reply.GrantedSeconds, nil
}

// newProductionExtentMRCache constructs the SDK-wide cache wired to
// the production RDMA round-trip helpers. Callers (extent reader
// initialisation) hold a single instance for the lifetime of the
// SDK process — the cache is concurrent-safe and survives transient
// DataNode reconnects (cache hits with expired-server-side leases
// surface as RDMA Read failures → caller invalidates → re-lookup).
func newProductionExtentMRCache() (*extentMRCache, error) {
	return newExtentMRCache(defaultExtentMRCacheConfig(), lookupExtentMR, renewExtentMR)
}
