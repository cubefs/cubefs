package repl

import (
	"fmt"
)

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
	if err := p.resolveFollowersAddr(); err != nil {
		return err
	}
	if !p.IsForwardPacket() {
		// Single-replica DP, or the special-replica-count sentinel:
		// nothing to forward.
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
			fp.respCh <- fmt.Errorf("repl: follower packet exceeds RDMA slot (size=%d arg=%d)", fp.Size, fp.ArgLen)
			continue
		}
		addr := p.followersAddrs[index]
		if err := followerRDMASend(addr, fp); err != nil {
			fp.respCh <- err
		}
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
	return firstErr
}
