// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"sort"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
)

func (r *raftFsm) becomeLeader() {
	if r.state == stateFollower {
		panic(AppPanicError(fmt.Sprintf("[raft->becomeLeader][%v] invalid transition [follower -> leader].", r.id)))
	}
	r.recoverCommit()
	lasti := r.raftLog.lastIndex()
	r.step = stepLeader
	r.reset(r.term, lasti, true)
	r.tick = r.tickHeartbeat
	r.leader = r.config.NodeID
	r.state = stateLeader
	r.acks = nil
	if pr, ok := r.replicas[r.config.NodeID]; ok {
		pr.active = true
	}

	ents, err := r.raftLog.entries(r.raftLog.committed+1, noLimit)
	if err != nil {
		errMsg := fmt.Sprintf("[raft->becomeLeader][%v] unexpected error getting uncommitted entries (%v).", r.id, err)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic(AppPanicError(fmt.Sprintf("[raft->becomeLeader][%v] unexpected double uncommitted config entry.", r.id)))
	}
	if nconf == 1 {
		r.pendingConf = true
	}

	r.appendEntry(&proto.Entry{Term: r.term, Index: lasti + 1, Data: nil})
	if logger.IsEnableDebug() {
		logger.Debug("raft[%v,%v] became leader at term %d.index:%d", r.id, r.config.ReplicateAddr, r.term, lasti+1)
	}
}

func stepLeader(r *raftFsm, m *proto.Message) {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case proto.LocalMsgProp:
		if _, ok := r.replicas[r.config.NodeID]; !ok || len(m.Entries) == 0 {
			return
		}

		for i, e := range m.Entries {
			if e.Type == proto.EntryConfChange {
				if r.pendingConf {
					m.Entries[i] = &proto.Entry{Term: e.Term, Index: e.Index, Type: proto.EntryNormal}
				}
				r.pendingConf = true
			}
		}
		r.appendEntry(m.Entries...)
		r.bcastAppend()
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		if logger.IsEnableDebug() {
			logger.Debug("[raft->stepLeader][%v logterm: %d, index: %d, vote: %v] rejected vote from %v [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.From, m.LogTerm, m.Index, r.term)
		}
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgVote
		nmsg.To = m.From
		nmsg.Reject = true
		r.send(nmsg)
		proto.ReturnMessage(m)
		return
	}

	// All other message types require a progress for m.From (pr).
	pr, prOk := r.replicas[m.From]
	if !prOk {
		if logger.IsEnableDebug() {
			logger.Debug("[raft->stepLeader][%v] no progress available for %v.", r.id, m.From)
		}
		return
	}
	switch m.Type {
	case proto.RespMsgAppend:
		pr.active = true
		pr.lastActive = time.Now()

		if m.Reject {
			if logger.IsEnableDebug() {
				logger.Debug("raft[%v, %v, %v, %v] received msgApp rejection(lastindex: %d) from %v for index %d commit %v. replica info [%v,%v,%v,%v]",
					r.id, r.raftLog.firstIndex(), r.raftLog.lastIndex(), r.raftLog.committed, m.RejectHint, m.From, m.Index, m.Commit, pr.state, pr.next, pr.committed, pr.match)
			}
			nextProbeIdx := m.RejectHint
			if m.LogTerm > 0 {
				// If the follower has an uncommitted log tail, we would end up
				// probing one by one until we hit the common prefix.
				//
				// For example, if the leader has:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 5 5 5 5 5
				//   term (F)   1 1 1 1 2 2
				//
				// Then, after sending an append anchored at (idx=9,term=5) we
				// would receive a RejectHint of 6 and LogTerm of 2. Without the
				// code below, we would try an append at index 6, which would
				// fail again.
				//
				// However, looking only at what the leader knows about its own
				// log and the rejection hint, it is clear that a probe at index
				// 6, 5, 4, 3, and 2 must fail as well:
				//
				// For all of these indexes, the leader's log term is larger than
				// the rejection's log term. If a probe at one of these indexes
				// succeeded, its log term at that index would match the leader's,
				// i.e. 3 or 5 in this example. But the follower already told the
				// leader that it is still at term 2 at index 9, and since the
				// log term only ever goes up (within a log), this is a contradiction.
				//
				// At index 1, however, the leader can draw no such conclusion,
				// as its term 1 is not larger than the term 2 from the
				// follower's rejection. We thus probe at 1, which will succeed
				// in this example. In general, with this approach we probe at
				// most once per term found in the leader's log.
				//
				// There is a similar mechanism on the follower (implemented in
				// handleAppendEntries via a call to findConflictByTerm) that is
				// useful if the follower has a large divergent uncommitted log
				// tail[1], as in this example:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 3 3 3 3 7
				//   term (F)   1 3 3 4 4 5 5 5 6
				//
				// Naively, the leader would probe at idx=9, receive a rejection
				// revealing the log term of 6 at the follower. Since the leader's
				// term at the previous index is already smaller than 6, the leader-
				// side optimization discussed above is ineffective. The leader thus
				// probes at index 8 and, naively, receives a rejection for the same
				// index and log term 5. Again, the leader optimization does not improve
				// over linear probing as term 5 is above the leader's term 3 for that
				// and many preceding indexes; the leader would have to probe linearly
				// until it would finally hit index 3, where the probe would succeed.
				//
				// Instead, we apply a similar optimization on the follower. When the
				// follower receives the probe at index 8 (log term 3), it concludes
				// that all of the leader's log preceding that index has log terms of
				// 3 or below. The largest index in the follower's log with a log term
				// of 3 or below is index 3. The follower will thus return a rejection
				// for index=3, log term=3 instead. The leader's next probe will then
				// succeed at that index.
				//
				// [1]: more precisely, if the log terms in the large uncommitted
				// tail on the follower are larger than the leader's. At first,
				// it may seem unintuitive that a follower could even have such
				// a large tail, but it can happen:
				//
				// 1. Leader appends (but does not commit) entries 2 and 3, crashes.
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 2 2     [crashes]
				//   term (F)   1
				//   term (F)   1
				//
				// 2. a follower becomes leader and appends entries at term 3.
				//              -----------------
				//   term (x)   1 2 2     [down]
				//   term (F)   1 3 3 3 3
				//   term (F)   1
				//
				// 3. term 3 leader goes down, term 2 leader returns as term 4
				//    leader. It commits the log & entries at term 4.
				//
				//              -----------------
				//   term (L)   1 2 2 2
				//   term (x)   1 3 3 3 3 [down]
				//   term (F)   1
				//              -----------------
				//   term (L)   1 2 2 2 4 4 4
				//   term (F)   1 3 3 3 3 [gets probed]
				//   term (F)   1 2 2 2 4 4 4
				//
				// 4. the leader will now probe the returning follower at index
				//    7, the rejection points it at the end of the follower's log
				//    which is at a higher log term than the actually committed
				//    log.
				nextProbeIdx = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.maybeDecrTo(m.Index, nextProbeIdx, m.Commit) {
				if logger.IsEnableDebug() {
					logger.Debug("[%v] decreased progress of [%v] to [%s]", r.id, m.From, pr)
				}
				if pr.state == replicaStateReplicate {
					pr.becomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.isPaused()
			if pr.maybeUpdate(m.Index, m.Commit) {
				switch {
				case pr.state == replicaStateProbe:
					pr.becomeReplicate()
				case pr.state == replicaStateSnapshot && pr.needSnapshotAbort():
					if logger.IsEnableWarn() {
						logger.Warn("raft[%v] snapshot aborted, resumed sending replication messages to %v.", r.id, m.From)
					}
					pr.becomeProbe()
				case pr.state == replicaStateReplicate:
					pr.inflight.freeTo(m.Index)
				}

				if r.maybeCommit() {
					r.bcastAppend()
				} else if oldPaused {
					r.sendAppend(m.From)
				}
			}
		}
		proto.ReturnMessage(m)
		return

	case proto.RespMsgHeartBeat:
		if pr.state == replicaStateReplicate && pr.inflight.full() {
			pr.inflight.freeFirstOne()
		}
		if !pr.pending && (pr.match < r.raftLog.lastIndex() || pr.committed < r.raftLog.committed) {
			r.sendAppend(m.From)
		}

		pr.active = true
		pr.lastActive = time.Now()
		if pr.state != replicaStateSnapshot {
			pr.pending = false
		}
		return

	case proto.LeaseMsgOffline:
		for id := range r.replicas {
			if id == r.config.NodeID {
				continue
			}
			nmsg := proto.GetMessage()
			nmsg.Type = proto.LeaseMsgTimeout
			nmsg.To = id
			r.send(nmsg)
		}
		logger.Debug("[raft][%v] LeaseMsgOffline at term[%d] leader[%d].", r.id, r.term, r.leader)
		r.becomeFollower(r.term, NoLeader)
		proto.ReturnMessage(m)
		return

	case proto.RespMsgSnapShot:
		if pr.state != replicaStateSnapshot {
			return
		}

		if m.Reject {
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] send snapshot to [%v] failed.", r.id, m.From)
			}
			pr.snapshotFailure()
			pr.becomeProbe()
		} else {
			pr.active = true
			pr.lastActive = time.Now()
			pr.becomeProbe()
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] send snapshot to [%v] succeeded, resumed replication [%s]", r.id, m.From, pr)
			}
		}

		// If snapshot finish, wait for the RespMsgAppend from the remote node before sending out the next ReqMsgAppend.
		// If snapshot failure, wait for a heartbeat interval before next try.
		pr.pause()
		proto.ReturnMessage(m)
		return

	case proto.RespCheckQuorum:
		// TODO: remove this when stable
		if logger.IsEnableDebug() {
			logger.Debug("raft[%d] recv check quorum resp from %d, index=%d", r.id, m.From, m.Index)
		}
		r.readOnly.recvAck(m.Index, m.From, r.quorum())
		proto.ReturnMessage(m)
		return
	}
}

func (r *raftFsm) becomePreCandidate() {
	r.acks = make(map[uint64]bool)
	r.acks[r.config.NodeID] = true
	logger.Debug("raft[%v] became preCandidate at term %d.", r.id, r.term)

	r.step = stepPreCandidate
	r.reset(r.term, 0, false)
	r.tick = r.tickElectionAck
	r.state = statePreCandidate
}

func stepPreCandidate(r *raftFsm, m *proto.Message) {
	switch m.Type {
	case proto.LocalMsgProp:
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] no leader at term %d; dropping proposal", r.id, r.term)
		}
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgAppend:
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] PreCandidate receive append in term %d; become follower.", r.id, r.term)
		}
		r.becomeFollower(r.term, m.From)
		r.handleAppendEntries(m)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgHeartBeat:
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] PreCandidate receive heartbeat in term %d; become follower.", r.id, r.term)
		}
		r.becomeFollower(r.term, m.From)
		return

	case proto.ReqMsgPreVote:
		r.becomeFollower(r.term, m.From)
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgPreVote
		nmsg.To = m.From
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.RespCheckQuorum:
		// TODO: remove this when stable
		if logger.IsEnableDebug() {
			logger.Debug("raft[%d] recv check quorum resp from %d, index=%d", r.id, m.From, m.Index)
		}
		r.readOnly.recvAck(m.Index, m.From, r.quorum())
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgVote
		nmsg.To = m.From
		nmsg.Reject = true
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.RespMsgPreVote:
		gr := r.poll(m.From, !m.Reject)
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] [q:%d] stepPreCandidate has received %d votes and %d vote rejections.", r.id, r.quorum(), gr, len(r.votes)-gr)
		}
		switch r.quorum() {
		case gr:
			r.campaign(false, campaignElection)
		case len(r.votes) - gr:
			r.becomeFollower(r.term, NoLeader)
		}
		return
	}
}

func (r *raftFsm) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		if r.config.LeaseCheck && !r.checkLeaderLease() {
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] stepped down to follower since quorum is not active.", r.id)
			}
			logger.Debug("[raft][%v] heartbeat election timeout at term[%d] leader[%d].", r.id, r.term, r.leader)
			r.becomeFollower(r.term, NoLeader)
		}
	}

	if r.state != stateLeader {
		return
	}

	if r.heartbeatElapsed >= r.config.HeartbeatTick {
		r.heartbeatElapsed = 0
		for id := range r.replicas {
			if id == r.config.NodeID {
				continue
			}
			if r.replicas[id].state != replicaStateSnapshot {
				r.replicas[id].resume()
			}
		}
		r.bcastReadOnly()
	}
}

func (r *raftFsm) tickElectionAck() {
	r.electionElapsed++
	if r.electionElapsed >= r.config.ElectionTick {
		r.electionElapsed = 0

		m := proto.GetMessage()
		m.Type = proto.LocalMsgHup
		m.From = r.config.NodeID
		r.Step(m)
	}
}

func (r *raftFsm) checkLeaderLease() bool {
	var act int
	for id, peer := range r.replicas {
		if id == r.config.NodeID || peer.state == replicaStateSnapshot {
			act++
			continue
		}

		if peer.active {
			peer.active = false
			act++
		} else {
			r.monitorZombie(peer)
		}
	}

	return act >= r.quorum()
}

func (r *raftFsm) maybeCommit() bool {
	mis := make(util.Uint64Slice, 0, len(r.replicas))
	for _, rp := range r.replicas {
		mis = append(mis, rp.match)
	}
	sort.Sort(sort.Reverse(mis))
	mci := mis[r.quorum()-1]
	isCommit := r.raftLog.maybeCommit(mci, r.term)
	if r.state == stateLeader && r.replicas[r.config.NodeID] != nil {
		r.replicas[r.config.NodeID].committed = r.raftLog.committed
	}

	if r.state == stateLeader && !r.readOnly.committed && isCommit {
		if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) == r.term {
			r.readOnly.commit(r.raftLog.committed)
		}
		r.bcastReadOnly()
	}

	return isCommit
}

func (r *raftFsm) bcastAppend() {
	for id := range r.replicas {
		if id == r.config.NodeID {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *raftFsm) sendAppend(to uint64) {
	pr := r.replicas[to]
	if pr.isPaused() {
		return
	}

	var (
		term       uint64
		ents       []*proto.Entry
		errt, erre error
		m          *proto.Message
	)
	fi := r.raftLog.firstIndex()
	if pr.next >= fi {
		term, errt = r.raftLog.term(pr.next - 1)
		ents, erre = r.raftLog.entries(pr.next, r.config.MaxSizePerMsg)
	}
	if pr.next < fi || errt != nil || erre != nil {
		if !pr.active {
			if logger.IsEnableDebug() {
				logger.Debug("[raft->sendAppend][%v]ignore sending snapshot to %v since it is not recently active.", r.id, to)
			}
			return
		}

		snapshot, err := r.sm.Snapshot()
		if err != nil || snapshot.ApplyIndex() < fi-1 {
			panic(AppPanicError(fmt.Sprintf("[raft->sendAppend][%v]failed to send snapshot[%d] to %v because snapshot is unavailable, error is: \r\n%v", r.id, snapshot.ApplyIndex(), to, err)))
		}

		m = proto.GetMessage()
		m.Type = proto.ReqMsgSnapShot
		m.To = to
		m.Snapshot = snapshot
		snapMeta := proto.SnapshotMeta{Index: snapshot.ApplyIndex(), Peers: make([]proto.Peer, 0, len(r.replicas))}
		if snapTerm, err := r.raftLog.term(snapMeta.Index); err != nil {
			panic(AppPanicError(fmt.Sprintf("[raft->sendAppend][%v]failed to send snapshot to %v because snapshot is unavailable, error is: \r\n%v", r.id, to, err)))
		} else {
			snapMeta.Term = snapTerm
		}
		for _, p := range r.replicas {
			snapMeta.Peers = append(snapMeta.Peers, p.peer)
		}
		m.SnapshotMeta = snapMeta
		pr.becomeSnapshot(snapMeta.Index)

		logger.Debug("[raft->sendAppend][%v][firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to [%v][%s]",
			r.id, fi, r.raftLog.committed, snapMeta.Index, snapMeta.Term, to, pr)

	} else {
		m = proto.GetMessage()
		m.Type = proto.ReqMsgAppend
		m.To = to
		m.Index = pr.next - 1
		m.LogTerm = term
		m.Commit = r.raftLog.committed
		m.Entries = append(m.Entries, ents...)

		if n := len(m.Entries); n != 0 {
			switch pr.state {
			case replicaStateReplicate:
				last := m.Entries[n-1].Index
				pr.update(last)
				pr.inflight.add(last)
			case replicaStateProbe:
				pr.pause()
			default:
				errMsg := fmt.Sprintf("[repl->sendAppend][%v] is sending append in unhandled state %s.", r.id, pr.state)
				logger.Error(errMsg)
				panic(AppPanicError(errMsg))
			}
		}
	}
	pr.pending = true
	r.send(m)
}

func (r *raftFsm) appendEntry(es ...*proto.Entry) {
	r.raftLog.append(es...)
	r.replicas[r.config.NodeID].maybeUpdate(r.raftLog.lastIndex(), r.raftLog.committed)
	r.maybeCommit()
}

func (r *raftFsm) bcastReadOnly() {
	index := r.readOnly.lastPending()
	if index == 0 {
		return
	}
	if logger.IsEnableDebug() {
		logger.Debug("raft[%d] bcast readonly index: %d", r.id, index)
	}
	for id := range r.replicas {
		if id == r.config.NodeID {
			continue
		}
		msg := proto.GetMessage()
		msg.Type = proto.ReqCheckQuorum
		msg.To = id
		msg.Index = index
		r.send(msg)
	}
}
