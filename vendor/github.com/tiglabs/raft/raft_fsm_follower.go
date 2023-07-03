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
	"context"
	"math"

	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
)

func (r *raftFsm) becomeFollower(ctx context.Context, term, lead uint64) {
	if r.maybeChangeState(UnstableState) && logger.IsEnableDebug() {
		logger.Debug("raft[%v] change rist state to %v cause become follower", r.id, UnstableState)
	}
	r.step = stepFollower
	r.reset(term, 0, false, false)
	r.tick = r.tickElection
	r.leader = lead
	r.state = stateFollower
	if logger.IsEnableDebug() {
		logger.Debug("raft[%v] became follower at term[%d] leader[%d].", r.id, r.term, r.leader)
	}
}

func stepFollower(r *raftFsm, m *proto.Message) {
	switch m.Type {
	case proto.LocalMsgProp:
		if r.leader == NoLeader {
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] no leader at term %d; dropping proposal.", r.id, r.term)
			}
			return
		}
		m.To = r.leader
		r.send(m)
		return

	case proto.ReqMsgAppend:
		r.electionElapsed = 0
		r.leader = m.From
		r.handleAppendEntries(m)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgHeartBeat:
		r.electionElapsed = 0
		r.leader = m.From
		if entry, exist := m.HeartbeatContext.Get(r.id); exist {
			var newState = StableState
			if entry.IsUnstable {
				newState = UnstableState
			}
			if r.maybeChangeState(newState) {
				if logger.IsEnableDebug() {
					logger.Debug("raft[%v] recv risk state change to [%v] from leader [%v].", r.id, newState, m.From)
				}
			}
		}
		return

	case proto.ReqMsgElectAck:
		r.electionElapsed = 0
		r.leader = m.From
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgElectAck
		nmsg.To = m.From
		nmsg.Index = r.raftLog.lastIndex()
		nmsg.Commit = r.raftLog.committed
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.ReqCheckQuorum:
		// TODO: remove this
		if logger.IsEnableDebug() {
			logger.Debug("raft[%d] recv check quorum from %d, index=%d", r.id, m.From, m.Index)
		}
		r.electionElapsed = 0
		r.leader = m.From
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespCheckQuorum
		nmsg.Index = m.Index
		nmsg.To = m.From
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		fpri, lpri := uint16(math.MaxUint16), uint16(0)
		if pr, ok := r.replicas[m.From]; ok {
			fpri = pr.peer.Priority
		}
		if pr, ok := r.replicas[r.config.NodeID]; ok {
			lpri = pr.peer.Priority
		}

		if (!r.config.LeaseCheck || r.leader == NoLeader) && (r.vote == NoLeader || r.vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm, fpri, lpri) && m.Index >= r.startCommit {
			r.electionElapsed = 0
			if logger.IsEnableDebug() {
				logger.Debug("raft[%v] [logterm: %d, index: %d, vote: %v startCommit: %d] voted for %v [logterm: %d, index: %d] at term %d.", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, r.startCommit, m.From, m.LogTerm, m.Index, r.term)
			}
			r.vote = m.From
			nmsg := proto.GetMessage()
			nmsg.Type = proto.RespMsgVote
			nmsg.To = m.From
			nmsg.Index = r.raftLog.lastIndex()
			nmsg.Commit = r.raftLog.committed
			nmsg.SetCtx(m.Ctx())
			r.send(nmsg)
		} else {
			if logger.IsEnableDebug() {
				logger.Debug("raft[%v] [logterm: %d, index: %d, vote: %v startCommit: %d] rejected vote from %v [logterm: %d, index: %d] at term %d.", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, r.startCommit, m.From, m.LogTerm, m.Index, r.term)
			}
			nmsg := proto.GetMessage()
			nmsg.Type = proto.RespMsgVote
			nmsg.To = m.From
			nmsg.Reject = true
			nmsg.SetCtx(m.Ctx())
			r.send(nmsg)
		}
		proto.ReturnMessage(m)
		return

	case proto.LeaseMsgTimeout:
		if r.leader == m.From {
			r.electionElapsed = 0
			nmsg := proto.GetMessage()
			nmsg.Type = proto.LocalMsgHup
			nmsg.From = r.config.NodeID
			nmsg.SetCtx(m.Ctx())
			r.Step(nmsg)
		}
		proto.ReturnMessage(m)
		return
	}
}

func (r *raftFsm) tickElection() {
	if !r.promotable() {
		r.electionElapsed = 0
		return
	}

	r.electionElapsed++

	var stableElapsed = r.config.ElectionTick
	if r.leader != NoLeader && r.state == stateFollower && r.electionElapsed >= stableElapsed && r.maybeChangeState(UnstableState) {
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] risk state change to [%v] cause election elapsed [%v] larger than stable elapsed [%v].", r.id, UnstableState, r.electionElapsed, stableElapsed)
		}
	}

	timeout := false
	// check follower lease (2 * electiontimeout)
	if r.config.LeaseCheck && r.leader != NoLeader && r.state == stateFollower {
		timeout = r.electionElapsed >= (r.config.ElectionTick << 1)
	} else {
		timeout = r.pastElectionTimeout()
	}
	if timeout {
		r.electionElapsed = 0
		m := proto.GetMessage()
		m.Type = proto.LocalMsgHup
		m.From = r.config.NodeID
		r.Step(m)

	}
}

func (r *raftFsm) handleAppendEntries(m *proto.Message) {

	if m.Index < r.raftLog.committed {
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgAppend
		nmsg.To = m.From
		nmsg.Index = r.raftLog.committed
		nmsg.Commit = r.raftLog.committed
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgAppend
		nmsg.To = m.From
		nmsg.Index = mlastIndex
		nmsg.Commit = r.raftLog.committed
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
	} else {
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %v",
				r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		}
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgAppend
		nmsg.To = m.From
		nmsg.Index = m.Index
		nmsg.Commit = r.raftLog.committed
		nmsg.Reject = true
		nmsg.RejectIndex = r.raftLog.lastIndex()
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
	}
}

func (r *raftFsm) promotable() bool {
	pr, ok := r.replicas[r.config.NodeID]
	return ok && !pr.isLearner
}
