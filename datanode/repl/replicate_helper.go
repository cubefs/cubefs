package repl

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

// RDMA replication call counters, dumped periodically via log so
// operators can confirm the RDMA receive path is actually replicating.
var (
	rdmaReplCallCount         atomic.Uint64
	rdmaReplNotForwardCount   atomic.Uint64
	rdmaReplDispatchOK        atomic.Uint64
	rdmaReplDispatchSkipCarry atomic.Uint64
	rdmaReplDispatchSendErr   atomic.Uint64
	rdmaReplWaitErr           atomic.Uint64

	rdmaReplStatsOnce sync.Once
)

func startRDMAReplicateStatsLoop() {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		var prevCalls uint64
		for range ticker.C {
			calls := rdmaReplCallCount.Load()
			if calls == prevCalls {
				continue
			}
			prevCalls = calls
			log.LogInfof("RDMA repl stats: calls=%d notForward=%d dispatchOK=%d skipCanCarry=%d sendErr=%d waitErr=%d",
				calls,
				rdmaReplNotForwardCount.Load(),
				rdmaReplDispatchOK.Load(),
				rdmaReplDispatchSkipCarry.Load(),
				rdmaReplDispatchSendErr.Load(),
				rdmaReplWaitErr.Load())
		}
	}()
}

// PrepareRDMAReplicate parses the packet's Arg into followersAddrs and
// asynchronously dispatches a copy of the packet to every follower via
// the existing followerRDMASend transport. After this returns nil the
// caller may run its local operator in parallel with follower
// processing; per-follower responses arrive on the followerPackets'
// respCh and are collected by WaitForRDMAReplicate.
//
// This mirrors ReplProtocol.sendRequestToAllFollowers' dispatch half
// but lives outside ReplProtocol so callers that receive packets via
// transports other than TCP (specifically the RDMA receive path in
// datanode/rdma_server.go) can reuse the same follower-replication
// infrastructure. Without this helper, RDMA-received writes apply only
// on the receiving node — leaving the other replicas stale and causing
// subsequent reads to fail with OpArgMismatchErr after any leader
// switch.
//
// Returns a non-nil error when prerequisites for forwarding are
// unmet (Arg parse failure, or followerRDMASend not registered).
// Per-follower send/transport errors are NOT returned here — they are
// pushed onto each follower's respCh so they surface uniformly in
// WaitForRDMAReplicate alongside genuine remote-side rejections.
func PrepareRDMAReplicate(p *Packet) error {
	rdmaReplStatsOnce.Do(startRDMAReplicateStatsLoop)
	n := rdmaReplCallCount.Add(1)
	if err := p.resolveFollowersAddr(); err != nil {
		return err
	}
	if !p.IsForwardPacket() {
		rdmaReplNotForwardCount.Add(1)
		// Surface first few non-forward cases so operators can confirm
		// whether IsForwardPacket=false (e.g. RemainingFollowers got
		// stripped along the wire) is the reason follower replication
		// never fires.
		if n < 20 || n%5000 == 0 {
			log.LogInfof("RDMA repl: skip (not forward) reqId=%d op=0x%x rf=%d argLen=%d",
				p.ReqID, p.Opcode, p.RemainingFollowers, p.ArgLen)
		}
		return nil
	}
	if followerRDMASend == nil {
		return fmt.Errorf("repl: follower RDMA transport not enabled; cannot replicate from RDMA receive path")
	}
	for index := 0; index < len(p.followersAddrs); index++ {
		fp := NewFollowerPacket()
		copyPacket(p, fp)
		fp.RemainingFollowers = 0
		p.followerPackets[index] = fp

		if followerRDMACanCarry != nil && !followerRDMACanCarry(fp) {
			rdmaReplDispatchSkipCarry.Add(1)
			fp.respCh <- fmt.Errorf("repl: follower packet exceeds RDMA slot (size=%d arg=%d)", fp.Size, fp.ArgLen)
			continue
		}
		addr := p.followersAddrs[index]
		if err := followerRDMASend(addr, fp); err != nil {
			rdmaReplDispatchSendErr.Add(1)
			fp.respCh <- err
			continue
		}
		rdmaReplDispatchOK.Add(1)
	}
	return nil
}

// WaitForRDMAReplicate blocks on each follower's respCh and returns
// the first non-nil follower error (or nil if all followers acked
// successfully). The caller is responsible for translating that error
// into a response packet body via PackErrorBody(ActionReceiveFromFollower, ...).
//
// Mirrors checkLocalResultAndReciveAllFollowerResponse's wait loop
// without the per-ReplProtocol packet-list bookkeeping.
func WaitForRDMAReplicate(p *Packet) error {
	if !p.IsForwardPacket() {
		return nil
	}
	var firstErr error
	for index := 0; index < len(p.followersAddrs); index++ {
		fp := p.followerPackets[index]
		if fp == nil {
			continue
		}
		if err := <-fp.respCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		rdmaReplWaitErr.Add(1)
	}
	return firstErr
}
