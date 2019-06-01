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

	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"time"
)

// NoLeader is a placeholder nodeID used when there is no leader.
const NoLeader uint64 = 0

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
	go r.doRandomSeed()
	return r, nil
}

func (r *raftFsm) doRandomSeed() {
	ticker := time.Tick(time.Duration(rand.Intn(5)) * time.Second)
	for {
		select {
		case <-ticker:
			r.rand.Seed(time.Now().UnixNano())
		}
	}
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

			if logger.IsEnableDebug() {
				logger.Debug("[raft->Step][%v] is starting a new election at term[%d].", r.id, r.term)
			}
			r.campaign(m.ForceVote)
		} else if logger.IsEnableDebug() && r.state == stateLeader {
			logger.Debug("[raft->Step][%v] ignoring LocalMsgHup because already leader.", r.id)
		}
		return
	}

	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.term:
		if logger.IsEnableDebug() {
			logger.Debug("[raft->Step][%v term: %d] received a [%s] message with higher term from [%v term: %d].", r.id, r.term, m.Type, m.From, m.Term)
		}
		lead := m.From
		if m.Type == proto.ReqMsgVote {
			lead = NoLeader
			inLease := r.config.LeaseCheck && r.state == stateFollower && r.leader != NoLeader
			if r.leader != m.From && inLease && !m.ForceVote {
				if logger.IsEnableWarn() {
					logger.Warn("[raft->Step][%v logterm: %d, index: %d, vote: %v] ignored vote from %v [logterm: %d, index: %d] at term %d: lease is not expired.",
						r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.From, m.LogTerm, m.Index, r.term)
				}

				nmsg := proto.GetMessage()
				nmsg.Type = proto.LeaseMsgOffline
				nmsg.To = r.leader
				r.send(nmsg)
				return
			}
		}
		r.becomeFollower(m.Term, lead)

	case m.Term < r.term:
		if logger.IsEnableDebug() {
			logger.Debug("[raft->Step][%v term: %d] ignored a %s message with lower term from [%v term: %d].", r.id, r.term, m.Type, m.From, m.Term)
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
	for r.raftLog.applied < r.raftLog.committed {
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
	}
	return nil
}

func (r *raftFsm) applyConfChange(cc *proto.ConfChange) {
	if cc.Peer.ID == NoLeader {
		r.pendingConf = false
		return
	}

	switch cc.Type {
	case proto.ConfAddNode:
		r.addPeer(cc.Peer)
	case proto.ConfRemoveNode:
		r.removePeer(cc.Peer)
	case proto.ConfUpdateNode:
		r.updatePeer(cc.Peer)
	}
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

func (r *raftFsm) removePeer(peer proto.Peer) {
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

	if peer.ID == r.config.NodeID {
		r.becomeFollower(r.term, NoLeader)
	} else if r.state == stateLeader && len(r.replicas) > 0 {
		if r.maybeCommit() {
			r.bcastAppend()
		}
	}
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
	if m.Type != proto.LocalMsgProp {
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
	logger.Debug("raft[%v] random election timeout randElectionTick=%v, config.ElectionTick=%v, randTick=%v", r.id,
		r.randElectionTick, r.config.ElectionTick, randTick)
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
