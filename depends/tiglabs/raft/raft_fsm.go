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
	"math/rand"
	"strings"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"time"
)

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// NoLeader is a placeholder nodeID used when there is no leader.
const NoLeader uint64 = 0

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
)

type stepFunc func(r *raftFsm, m *proto.Message)

type raftFsm struct {
	id               uint64
	term             uint64
	vote             uint64
	leader           uint64
	electionElapsed  int
	heartbeatElapsed int
	// randElectionTick is a random number between[electiontimetick, 2 * electiontimetick - 1].
	// It gets reset when raft changes its state to follower or candidate.
	randElectionTick int
	// New configuration is ignored if there exists unapplied configuration.
	pendingConf bool
	state       fsmState
	sm          StateMachine
	config      *Config
	raftLog     *raftLog
	rand        *rand.Rand
	votes       map[uint64]bool
	acks        map[uint64]bool
	replicas    map[uint64]*replica
	readOnly    *readOnly
	msgs        []*proto.Message
	step        stepFunc
	tick        func()
	stopCh      chan struct{}

	mo Monitor
	//electionFirstBegin is used to mark the begin time of continuous election
	//It is valid if and only if mo != nil.
	electionFirstBegin time.Time
}

func (fsm *raftFsm) getReplicas() (m string) {
	for id := range fsm.replicas {
		m += fmt.Sprintf(" [%v] ,", id)
	}
	return m
}

func newRaftFsm(config *Config, raftConfig *RaftConfig) (*raftFsm, error) {
	raftlog, err := newRaftLog(raftConfig.Storage)
	if err != nil {
		return nil, err
	}
	hs, err := raftConfig.Storage.InitialState()
	if err != nil {
		return nil, err
	}

	r := &raftFsm{
		id:       raftConfig.ID,
		sm:       raftConfig.StateMachine,
		mo:       raftConfig.Monitor,
		config:   config,
		leader:   NoLeader,
		raftLog:  raftlog,
		replicas: make(map[uint64]*replica),
		readOnly: newReadOnly(raftConfig.ID, config.ReadOnlyOption),
	}
	r.rand = rand.New(rand.NewSource(int64(config.NodeID + r.id)))
	for _, p := range raftConfig.Peers {
		r.replicas[p.ID] = newReplica(p, 0)
	}

	if !hs.IsEmpty() {
		if raftConfig.Applied > r.raftLog.lastIndex() {
			logger.Info("newRaft[%v] update [applied: %d, to lastindex: %d]", r.id, raftConfig.Applied, raftlog.lastIndex())
			raftConfig.Applied = r.raftLog.lastIndex()
		}
		if hs.Commit > r.raftLog.lastIndex() {
			logger.Info("newRaft[%v] update [hardState commit: %d, to lastindex: %d]", r.id, hs.Commit, raftlog.lastIndex())
			hs.Commit = r.raftLog.lastIndex()
		}
		if err := r.loadState(hs); err != nil {
			return nil, err
		}
	}

	logger.Info("newRaft[%v] [commit: %d, applied: %d, lastindex: %d]", r.id, raftlog.committed, raftConfig.Applied, raftlog.lastIndex())

	if raftConfig.Applied > 0 {
		lasti := raftlog.lastIndex()
		if lasti == 0 {
			// If there is application data but no raft log, then restore to initial state.
			raftlog.committed = 0
			raftConfig.Applied = 0
		} else if lasti < raftConfig.Applied {
			// If lastIndex<appliedIndex, then the log as the standard.
			raftlog.committed = lasti
			raftConfig.Applied = lasti
		} else if raftlog.committed < raftConfig.Applied {
			raftlog.committed = raftConfig.Applied
		}
		raftlog.appliedTo(raftConfig.Applied)
	}

	// recover committed
	if err := r.recoverCommit(); err != nil {
		return nil, err
	}
	if raftConfig.Leader == config.NodeID {
		if raftConfig.Term != 0 && r.term <= raftConfig.Term {
			r.term = raftConfig.Term
			r.state = stateLeader
			r.becomeLeader()
			r.bcastAppend()
		} else {
			r.becomeFollower(r.term, NoLeader)
		}
	} else {
		if raftConfig.Leader == NoLeader {
			r.becomeFollower(r.term, NoLeader)
		} else {
			r.becomeFollower(raftConfig.Term, raftConfig.Leader)
		}
	}

	if logger.IsEnableDebug() {
		peerStrs := make([]string, 0)
		for _, p := range r.peers() {
			peerStrs = append(peerStrs, fmt.Sprintf("%v", p.String()))
		}
		logger.Debug("newRaft[%v] [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
			r.id, strings.Join(peerStrs, ","), r.term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	}
	r.stopCh = make(chan struct{}, 1)
	go r.doRandomSeed()
	return r, nil
}

func (r *raftFsm) doRandomSeed() {
	ticker := time.Tick(time.Duration(rand.Intn(5)) * time.Second)
	for {
		select {
		case <-ticker:
			r.rand.Seed(time.Now().UnixNano())
		case <-r.stopCh:
			return
		}
	}
}

func (r *raftFsm) StopFsm() {
	peers := make([]proto.Peer, len(r.replicas))
	for _, r := range r.replicas {
		peers = append(peers, r.peer)
	}
	if r.mo != nil {
		r.mo.RemovePartition(r.id, peers)
	}
	close(r.stopCh)
}

// raft main method
func (r *raftFsm) Step(m *proto.Message) {
	if m.Type == proto.LocalMsgHup {
		if r.state != stateLeader && r.promotable() {
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				errMsg := fmt.Sprintf("[raft->Step][%v]unexpected error getting unapplied entries:[%v]", r.id, err)
				logger.Error(errMsg)
				panic(AppPanicError(errMsg))
			}
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				if logger.IsEnableWarn() {
					logger.Warn("[raft->Step][%v] cannot campaign at term %d since there are still %d pending configuration changes to apply.", r.id, r.term, n)
				}
				return
			}

			if logger.IsEnableInfo() {
				logger.Info("[raft->Step][%v] is starting a new election at term[%d].", r.id, r.term)
			}
			// only transfer leader will set forceVote=true.
			// Leadership transfers never use pre-vote even if r.preVote is true; we
			// know we are not recovering from a partition so there is no need for the
			// extra round trip.
			if r.config.PreVote && !m.ForceVote {
				r.campaign(m.ForceVote, campaignPreElection)
			} else {
				r.campaign(m.ForceVote, campaignElection)
			}

		} else if logger.IsEnableDebug() && r.state == stateLeader {
			logger.Debug("[raft->Step][%v] ignoring LocalMsgHup because already leader.", r.id)
		} else if logger.IsEnableDebug() {
			var replicas []uint64
			for id := range r.replicas {
				replicas = append(replicas, id)
			}
			logger.Debug("[raft->Step][%v] state %v, replicas %v.", r.id, r.state, replicas)
		}
		return
	}
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.term:
		if logger.IsEnableDebug() {
			logger.Debug("[raft->Step][%v term: %d] received a [%s] message with higher term from [%v term: %d],ForceVote[%v].",
				r.id, r.term, m.Type, m.From, m.Term, m.ForceVote)
		}
		if m.Type == proto.ReqMsgVote || m.Type == proto.ReqMsgPreVote {
			inLease := r.config.LeaseCheck && r.leader != NoLeader
			if r.leader != m.From && inLease && !m.ForceVote && r.electionElapsed < r.randElectionTick {
				if logger.IsEnableWarn() {
					logger.Warn("[raft->Step][%v logterm: %d, index: %d, vote: %v] ignored %v from %v [logterm: %d, index: %d] at term %d: lease is not expired.",
						r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.Type, m.From, m.LogTerm, m.Index, r.term)
				}

				return
			}
		}
		switch {
		case m.Type == proto.ReqMsgPreVote:
			// Never change our term in response to a PreVote
		case m.Type == proto.RespMsgPreVote && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			if logger.IsEnableDebug() {
				logger.Debug("[raft->Step][%x,%d] [term: %d] received a %s message with higher term from %x [term: %d]",
					r.id, r.config.ReplicateAddr, r.term, m.Type, m.From, m.Term)
			}
			if m.Type == proto.ReqMsgAppend || m.Type == proto.ReqMsgHeartBeat || m.Type == proto.ReqMsgSnapShot {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, NoLeader)
			}
		}

	case m.Term < r.term:
		if (r.config.LeaseCheck || r.config.PreVote) && (m.Type == proto.ReqMsgHeartBeat || m.Type == proto.ReqMsgAppend) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "proto.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election. This can be prevented with Pre-Vote phase.
			r.send(&proto.Message{To: m.From, Term: r.term, Type: proto.RespMsgAppend})
		} else if m.Type == proto.ReqMsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			if logger.IsEnableInfo() {
				logger.Info("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.Type, m.From, m.LogTerm, m.Index, r.term)
			}
			r.send(&proto.Message{To: m.From, Term: r.term, Type: proto.RespMsgPreVote, Reject: true})
		} else {
			// ignore other cases
			if logger.IsEnableInfo() {
				logger.Info("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
					r.id, r.term, m.Type, m.From, m.Term)
			}
		}

		return
	}

	if m.Type == proto.ReqMsgPreVote || m.Type == proto.ReqMsgVote {
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.vote == NoLeader && r.leader == NoLeader) ||
			// ...or this is a PreVote for a future term...
			(m.Type == proto.ReqMsgPreVote && m.Term > r.term)
		// ...and we believe the candidate is up to date.
		var respType proto.MsgType
		if m.Type == proto.ReqMsgPreVote {
			respType = proto.RespMsgPreVote
		} else {
			respType = proto.RespMsgVote
		}
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm, 0, 0) {
			// Note: it turns out that that learners must be allowed to cast votes.
			// This seems counter- intuitive but is necessary in the situation in which
			// a learner has been promoted (i.e. is now a voter) but has not learned
			// about this yet.
			// For example, consider a group in which id=1 is a learner and id=2 and
			// id=3 are voters. A configuration change promoting 1 can be committed on
			// the quorum `{2,3}` without the config change being appended to the
			// learner's log. If the leader (say 2) fails, there are de facto two
			// voters remaining. Only 3 can win an election (due to its log containing
			// all committed entries), but to do so it will need 1 to vote. But 1
			// considers itself a learner and will continue to do so until 3 has
			// stepped up as leader, replicates the conf change to 1, and 1 applies it.
			// Ultimately, by receiving a request to vote, the learner realizes that
			// the candidate believes it to be a voter, and that it should act
			// accordingly. The candidate's config may be stale, too; but in that case
			// it won't win the election, at least in the absence of the bug discussed
			// in:
			// https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
			if logger.IsEnableDebug() {
				logger.Info("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.Type, m.From, m.LogTerm, m.Index, r.term)
			}
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why, consider the
			// case where a single node was previously partitioned away and
			// it's local term is now out of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			r.send(&proto.Message{To: m.From, Term: m.Term, Type: respType})
			if m.Type == proto.ReqMsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.vote = m.From
			}
		} else {
			if logger.IsEnableDebug() {
				logger.Info("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.Type, m.From, m.LogTerm, m.Index, r.term)
			}
			r.send(&proto.Message{To: m.From, Term: r.term, Type: respType, Reject: true})
		}
		return
	}
	r.step(r, m)
}

func (r *raftFsm) loadState(state proto.HardState) error {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		return fmt.Errorf("[raft->loadState][%v] state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}

	r.term = state.Term
	r.vote = state.Vote
	r.raftLog.committed = state.Commit
	return nil
}

func (r *raftFsm) recoverCommit() error {
	for r.raftLog.applied <= r.raftLog.committed {
		committedEntries := r.raftLog.nextEnts(64 * MB)
		for _, entry := range committedEntries {
			r.raftLog.appliedTo(entry.Index)

			switch entry.Type {
			case proto.EntryNormal:
				if entry.Data == nil || len(entry.Data) == 0 {
					continue
				}
				if _, err := r.sm.Apply(entry.Data, entry.Index); err != nil {
					return err
				}

			case proto.EntryConfChange:
				cc := new(proto.ConfChange)
				cc.Decode(entry.Data)
				if _, err := r.sm.ApplyMemberChange(cc, entry.Index); err != nil {
					return err
				}
				r.applyConfChange(cc)
			}
		}
		if r.raftLog.applied == r.raftLog.committed {
			break
		}
	}
	return nil
}

func (r *raftFsm) applyConfChange(cc *proto.ConfChange) (ok bool) {
	if cc.Peer.ID == NoLeader {
		r.pendingConf = false
		return
	}

	switch cc.Type {
	case proto.ConfAddNode:
		r.addPeer(cc.Peer)
	case proto.ConfRemoveNode:
		return r.removePeer(cc.Peer)
	case proto.ConfUpdateNode:
		r.updatePeer(cc.Peer)
	}
	return
}

func (r *raftFsm) addPeer(peer proto.Peer) {
	r.pendingConf = false
	if _, ok := r.replicas[peer.ID]; !ok {
		if r.state == stateLeader {
			r.replicas[peer.ID] = newReplica(peer, r.config.MaxInflightMsgs)
			r.replicas[peer.ID].next = r.raftLog.lastIndex() + 1
		} else {
			r.replicas[peer.ID] = newReplica(peer, 0)
		}
	}
}

func (r *raftFsm) removePeer(peer proto.Peer) (ok bool) {
	r.pendingConf = false

	replica, ok := r.replicas[peer.ID]
	if !ok {
		return
	} else if replica.peer.PeerID != peer.PeerID {
		if logger.IsEnableInfo() {
			logger.Info("raft[%v] ignore remove peer[%v], current[%v]", r.id, peer.String(), replica.peer.String())
		}
		return
	}

	delete(r.replicas, peer.ID)
	ok = true

	if peer.ID == r.config.NodeID {
		r.becomeFollower(r.term, NoLeader)
	} else if r.state == stateLeader && len(r.replicas) > 0 {
		if r.maybeCommit() {
			r.bcastAppend()
		}
	}
	return
}

func (r *raftFsm) updatePeer(peer proto.Peer) {
	r.pendingConf = false
	if _, ok := r.replicas[peer.ID]; ok {
		r.replicas[peer.ID].peer = peer
	}
}

func (r *raftFsm) quorum() int {
	return len(r.replicas)/2 + 1
}

func (r *raftFsm) send(m *proto.Message) {
	m.ID = r.id
	m.From = r.config.NodeID
	// ReqMsgPreVote's message should add one
	if m.Type != proto.LocalMsgProp && m.Type != proto.ReqMsgPreVote {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

func (r *raftFsm) reset(term, lasti uint64, isLeader bool) {
	if r.term != term {
		r.term = term
		r.vote = NoLeader
	}
	r.leader = NoLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	r.pendingConf = false
	r.readOnly.reset(ErrNotLeader)

	if isLeader {
		r.randElectionTick = r.config.ElectionTick - 1
		for id, p := range r.replicas {
			r.replicas[id] = newReplica(p.peer, r.config.MaxInflightMsgs)
			r.replicas[id].next = lasti + 1
			if id == r.config.NodeID {
				r.replicas[id].match = lasti
				r.replicas[id].committed = r.raftLog.committed
			}
		}
	} else {
		r.resetRandomizedElectionTimeout()
		for id, p := range r.replicas {
			r.replicas[id] = newReplica(p.peer, 0)
		}
	}
}

func (r *raftFsm) resetRandomizedElectionTimeout() {
	randTick := r.rand.Intn(r.config.ElectionTick)
	r.randElectionTick = r.config.ElectionTick + randTick
	logger.Debug("raft[%v,%v] random election timeout randElectionTick=%v, config.ElectionTick=%v, randTick=%v",
		r.id, r.config.ReplicateAddr, r.randElectionTick, r.config.ElectionTick, randTick)
}

func (r *raftFsm) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randElectionTick
}

func (r *raftFsm) peers() []proto.Peer {
	peers := make([]proto.Peer, 0, len(r.replicas))
	for _, p := range r.replicas {
		peers = append(peers, p.peer)
	}
	return peers
}

func (r *raftFsm) checkSnapshot(meta proto.SnapshotMeta) bool {
	if meta.Index <= r.raftLog.committed {
		return false
	}
	if r.raftLog.matchTerm(meta.Index, meta.Term) {
		r.raftLog.commitTo(meta.Index)
		return false
	}
	return true
}

func (r *raftFsm) restore(meta proto.SnapshotMeta) {
	if logger.IsEnableWarn() {
		logger.Warn("raft [%v, commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d,term:%d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), meta.Index, meta.Term)
	}

	r.raftLog.restore(meta.Index)
	r.replicas = make(map[uint64]*replica)
	for _, p := range meta.Peers {
		r.replicas[p.ID] = newReplica(p, 0)
	}
}

func (r *raftFsm) addReadIndex(futures []*Future) {
	// not leader
	if r.leader != r.config.NodeID {
		respondReadIndex(futures, ErrNotLeader)
		return
	}

	// check leader commit in current term
	if !r.readOnly.committed {
		if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) == r.term {
			r.readOnly.commit(r.raftLog.committed)
		}
	}
	r.readOnly.add(r.raftLog.committed, futures)
	r.bcastReadOnly()
}

func numOfPendingConf(ents []*proto.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == proto.EntryConfChange {
			n++
		}
	}
	return n
}

func (r *raftFsm) monitorElection() {
	if r.mo == nil {
		return
	}
	now := time.Now()
	if r.electionFirstBegin.IsZero() || r.state != stateCandidate {
		//Record the time of the most recent lost of leader.
		r.electionFirstBegin = now
		return
	}
	//call r.mo.MonitorElection when r.leader==NoLeader continuously
	r.mo.MonitorElection(r.id, r.getReplicas(), now.Sub(r.electionFirstBegin))
}

func (r *raftFsm) monitorZombie(peer *replica) {
	if r.mo == nil {
		return
	}
	now := time.Now()
	if peer.lastZombie.Before(peer.lastActive) {
		peer.lastZombie = now
	}
	if du := now.Sub(peer.lastZombie); du > 2*r.config.TickInterval {
		r.mo.MonitorZombie(r.id, peer.peer, r.getReplicas(), du)
	}
}
