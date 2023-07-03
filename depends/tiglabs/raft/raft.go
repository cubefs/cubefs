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
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
)

type proposal struct {
	cmdType proto.EntryType
	future  *Future
	data    []byte
}

type apply struct {
	term        uint64
	index       uint64
	future      *Future
	command     interface{}
	readIndexes []*Future
}

// handle user's get log entries request
type entryRequest struct {
	future     *Future
	index      uint64
	maxSize    uint64
	onlyCommit bool
}

type softState struct {
	leader uint64
	term   uint64
}

type peerState struct {
	peers map[uint64]proto.Peer
	mu    sync.RWMutex
}

type monitorStatus struct {
	conErrCount    uint8
	replicasErrCnt map[uint64]uint8
}

func (s *peerState) change(c *proto.ConfChange) {
	s.mu.Lock()
	switch c.Type {
	case proto.ConfAddNode:
		s.peers[c.Peer.ID] = c.Peer
	case proto.ConfRemoveNode:
		delete(s.peers, c.Peer.ID)
	case proto.ConfUpdateNode:
		s.peers[c.Peer.ID] = c.Peer
	}
	s.mu.Unlock()
}

func (s *peerState) replace(peers []proto.Peer) {
	s.mu.Lock()
	s.peers = nil
	s.peers = make(map[uint64]proto.Peer)
	for _, p := range peers {
		s.peers[p.ID] = p
	}
	s.mu.Unlock()
}

func (s *peerState) get() (nodes []uint64) {
	s.mu.RLock()
	for n := range s.peers {
		nodes = append(nodes, n)
	}
	s.mu.RUnlock()
	return
}

type raft struct {
	raftFsm           *raftFsm
	config            *Config
	raftConfig        *RaftConfig
	restoringSnapshot util.AtomicBool
	curApplied        util.AtomicUInt64
	curSoftSt         unsafe.Pointer
	prevSoftSt        softState
	prevHardSt        proto.HardState
	peerState         peerState
	pending           map[uint64]*Future
	snapping          map[uint64]*snapshotStatus
	mStatus           *monitorStatus
	propc             chan *proposal
	applyc            chan *apply
	recvc             chan *proto.Message
	snapRecvc         chan *snapshotRequest
	truncatec         chan uint64
	readIndexC        chan *Future
	statusc           chan chan *Status
	entryRequestC     chan *entryRequest
	readyc            chan struct{}
	tickc             chan struct{}
	electc            chan struct{}
	stopc             chan struct{}
	done              chan struct{}
	mu                sync.Mutex
}

func newRaft(config *Config, raftConfig *RaftConfig) (*raft, error) {
	defer util.HandleCrash()

	if err := raftConfig.validate(); err != nil {
		return nil, err
	}

	r, err := newRaftFsm(config, raftConfig)
	if err != nil {
		return nil, err
	}

	mStatus := &monitorStatus{
		conErrCount:    0,
		replicasErrCnt: make(map[uint64]uint8),
	}
	raft := &raft{
		raftFsm:       r,
		config:        config,
		raftConfig:    raftConfig,
		mStatus:       mStatus,
		pending:       make(map[uint64]*Future),
		snapping:      make(map[uint64]*snapshotStatus),
		recvc:         make(chan *proto.Message, config.ReqBufferSize),
		applyc:        make(chan *apply, config.AppBufferSize),
		propc:         make(chan *proposal, 256),
		snapRecvc:     make(chan *snapshotRequest, 1),
		truncatec:     make(chan uint64, 1),
		readIndexC:    make(chan *Future, 256),
		statusc:       make(chan chan *Status, 1),
		entryRequestC: make(chan *entryRequest, 16),
		tickc:         make(chan struct{}, 64),
		readyc:        make(chan struct{}, 1),
		electc:        make(chan struct{}, 1),
		stopc:         make(chan struct{}),
		done:          make(chan struct{}),
	}
	raft.curApplied.Set(r.raftLog.applied)
	raft.peerState.replace(raftConfig.Peers)

	util.RunWorker(raft.runApply, raft.handlePanic)
	util.RunWorker(raft.run, raft.handlePanic)
	return raft, nil
}

func (s *raft) stop() {
	select {
	case <-s.done:
		return
	default:
		s.doStop()
	}
	<-s.done

}

func (s *raft) doStop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.stopc:
		return
	default:
		s.raftFsm.StopFsm()
		close(s.stopc)
		s.restoringSnapshot.Set(false)
	}
}

func (s *raft) runApply() {
	defer func() {
		s.doStop()
		s.resetApply()
	}()

	loopCount := 0
	for {
		loopCount = loopCount + 1
		if loopCount > 16 {
			loopCount = 0
			runtime.Gosched()
		}

		select {
		case <-s.stopc:
			return

		case apply := <-s.applyc:
			if apply.index <= s.curApplied.Get() {
				if len(apply.readIndexes) > 0 {
					respondReadIndex(apply.readIndexes, nil)
				}
				continue
			}

			var (
				err  error
				resp interface{}
			)
			switch cmd := apply.command.(type) {
			case *proto.ConfChange:
				resp, err = s.raftConfig.StateMachine.ApplyMemberChange(cmd, apply.index)
				if cmd.Type == proto.ConfRemoveNode && err == nil {
					s.raftFsm.mo.RemovePeer(s.raftFsm.id, cmd.Peer)
				}

			case []byte:
				resp, err = s.raftConfig.StateMachine.Apply(cmd, apply.index)
			}

			if apply.future != nil {
				apply.future.respond(resp, err)
			}
			if len(apply.readIndexes) > 0 {
				respondReadIndex(apply.readIndexes, nil)
			}
			s.curApplied.Set(apply.index)
			pool.returnApply(apply)
		}
	}
}

func (s *raft) run() {
	defer func() {
		s.doStop()
		s.resetPending(ErrStopped)
		s.raftFsm.readOnly.reset(ErrStopped)
		s.stopSnapping()
		s.raftConfig.Storage.Close()
		close(s.done)
	}()

	s.prevHardSt.Term = s.raftFsm.term
	s.prevHardSt.Vote = s.raftFsm.vote
	s.prevHardSt.Commit = s.raftFsm.raftLog.committed
	s.maybeChange(true)

	loopCount := 0
	var readyc chan struct{}
	for {
		if readyc == nil && s.containsUpdate() {
			readyc = s.readyc
			readyc <- struct{}{}
		}

		select {
		case <-s.stopc:
			return

		case <-s.tickc:
			s.raftFsm.tick()
			s.maybeChange(true)

		case pr := <-s.propc:
			if s.raftFsm.leader != s.config.NodeID {
				pr.future.respond(nil, ErrNotLeader)
				pool.returnProposal(pr)
				break
			}

			msg := proto.GetMessage()
			msg.Type = proto.LocalMsgProp
			msg.From = s.config.NodeID
			starti := s.raftFsm.raftLog.lastIndex() + 1
			s.pending[starti] = pr.future
			msg.Entries = append(msg.Entries, &proto.Entry{Term: s.raftFsm.term, Index: starti, Type: pr.cmdType, Data: pr.data})
			pool.returnProposal(pr)

			flag := false
			for i := 1; i < 64; i++ {
				starti = starti + 1
				select {
				case pr := <-s.propc:
					s.pending[starti] = pr.future
					msg.Entries = append(msg.Entries, &proto.Entry{Term: s.raftFsm.term, Index: starti, Type: pr.cmdType, Data: pr.data})
					pool.returnProposal(pr)
				default:
					flag = true
				}
				if flag {
					break
				}
			}
			s.raftFsm.Step(msg)

		case m := <-s.recvc:
			if _, ok := s.raftFsm.replicas[m.From]; ok || (!m.IsResponseMsg() && m.Type != proto.ReqMsgVote) ||
				(m.Type == proto.ReqMsgVote && s.raftFsm.raftLog.isUpToDate(m.Index, m.LogTerm, 0, 0)) {
				switch m.Type {
				case proto.ReqMsgHeartBeat:
					if s.raftFsm.leader == m.From && m.From != s.config.NodeID {
						s.raftFsm.Step(m)
					}
				case proto.RespMsgHeartBeat:
					if s.raftFsm.leader == s.config.NodeID && m.From != s.config.NodeID {
						s.raftFsm.Step(m)
					}
				default:
					s.raftFsm.Step(m)
				}
				var respErr = true
				if m.Type == proto.RespMsgAppend && m.Reject != true {
					respErr = false
				}
				s.maybeChange(respErr)
			} else if logger.IsEnableWarn() && m.Type != proto.RespMsgHeartBeat {
				logger.Warn(" [raft] [%v term: %d] raftFm[%p] raftReplicas[%v] ignored a %s message "+
					"without the replica from [%v term: %d].",
					s.raftFsm.id, s.raftFsm.term, s.raftFsm, s.raftFsm.getReplicas(), m.Type, m.From, m.Term)
			}

		case snapReq := <-s.snapRecvc:
			s.handleSnapshot(snapReq)

		case <-readyc:
			s.persist()
			s.apply()
			s.advance()
			// Send all messages.
			for _, msg := range s.raftFsm.msgs {
				if msg.Type == proto.ReqMsgSnapShot {
					s.sendSnapshot(msg)
					continue
				}
				s.sendMessage(msg)
			}
			s.raftFsm.msgs = nil
			readyc = nil
			loopCount = loopCount + 1
			if loopCount >= 2 {
				loopCount = 0
				runtime.Gosched()
			}

		case <-s.electc:
			msg := proto.GetMessage()
			msg.Type = proto.LocalMsgHup
			msg.From = s.config.NodeID
			msg.ForceVote = true
			logger.Debug("raft[%v] node %v try to leader", s.raftFsm.id, s.config.NodeID)
			s.raftFsm.Step(msg)
			s.maybeChange(true)

		case c := <-s.statusc:
			c <- s.getStatus()

		case truncIndex := <-s.truncatec:
			func() {
				defer util.HandleCrash()

				if lasti, err := s.raftConfig.Storage.LastIndex(); err != nil {
					logger.Error("raft[%v] truncate failed to get last index from storage: %v", s.raftFsm.id, err)
				} else if lasti > s.config.RetainLogs {
					maxIndex := util.Min(truncIndex, lasti-s.config.RetainLogs)
					if err = s.raftConfig.Storage.Truncate(maxIndex); err != nil {
						logger.Error("raft[%v] truncate failed,error is: %v", s.raftFsm.id, err)
					}
				}
			}()

		case future := <-s.readIndexC:
			futures := []*Future{future}
			// handle in batch
			var flag bool
			for i := 1; i < 64; i++ {
				select {
				case f := <-s.readIndexC:
					futures = append(futures, f)
				default:
					flag = true
				}
				if flag {
					break
				}
			}
			s.raftFsm.addReadIndex(futures)

		case req := <-s.entryRequestC:
			s.getEntriesInLoop(req)
		}
	}
}

func (s *raft) tick() {
	if s.restoringSnapshot.Get() {
		return
	}

	select {
	case <-s.stopc:
	case s.tickc <- struct{}{}:
	default:
		return
	}
}

func (s *raft) propose(cmd []byte, future *Future) {
	if !s.isLeader() {
		future.respond(nil, ErrNotLeader)
		return
	}

	pr := pool.getProposal()
	pr.cmdType = proto.EntryNormal
	pr.data = cmd
	pr.future = future

	select {
	case <-s.stopc:
		future.respond(nil, ErrStopped)
	case s.propc <- pr:
	}
}

func (s *raft) proposeMemberChange(cc *proto.ConfChange, future *Future) {
	if !s.isLeader() {
		future.respond(nil, ErrNotLeader)
		return
	}

	pr := pool.getProposal()
	pr.cmdType = proto.EntryConfChange
	pr.future = future
	pr.data = cc.Encode()

	select {
	case <-s.stopc:
		future.respond(nil, ErrStopped)
	case s.propc <- pr:
	}
}

func (s *raft) reciveMessage(m *proto.Message) {
	if s.restoringSnapshot.Get() {
		return
	}

	select {
	case <-s.stopc:
	case s.recvc <- m:
	default:
		logger.Warn(fmt.Sprintf("raft[%v] discard message(%v)", s.raftConfig.ID, m.ToString()))
		return
	}
}

func (s *raft) reciveSnapshot(m *snapshotRequest) {
	if s.restoringSnapshot.Get() {
		m.respond(ErrSnapping)
		return
	}

	select {
	case <-s.stopc:
		m.respond(ErrStopped)
		return
	case s.snapRecvc <- m:
	}
}

func (s *raft) status() *Status {
	if s.restoringSnapshot.Get() {
		return &Status{
			ID:                s.raftFsm.id,
			NodeID:            s.config.NodeID,
			RestoringSnapshot: true,
			State:             stateFollower.String(),
		}
	}

	c := make(chan *Status, 1)
	select {
	case <-s.stopc:
		return nil
	case s.statusc <- c:
		return <-c
	}
}

func (s *raft) truncate(index uint64) {
	logger.Debug("raft[%v] truncate index %v", s.raftFsm.id, index)
	if s.restoringSnapshot.Get() {
		return
	}

	select {
	case <-s.stopc:
	case s.truncatec <- index:
	default:
		return
	}
}

func (s *raft) tryToLeader(future *Future) {
	if s.restoringSnapshot.Get() {
		future.respond(nil, nil)
		return
	}

	select {
	case <-s.stopc:
		future.respond(nil, ErrStopped)
	case s.electc <- struct{}{}:
		future.respond(nil, nil)
	}
}

func (s *raft) leaderTerm() (leader, term uint64) {
	st := (*softState)(atomic.LoadPointer(&s.curSoftSt))
	if st == nil {
		return NoLeader, 0
	}
	return st.leader, st.term
}

func (s *raft) isLeader() bool {
	leader, _ := s.leaderTerm()
	return leader == s.config.NodeID
}

func (s *raft) applied() uint64 {
	return s.curApplied.Get()
}

func (s *raft) committed() uint64 {
	return s.raftFsm.raftLog.committed
}

func (s *raft) sendMessage(m *proto.Message) {
	s.config.transport.Send(m)
}

func (s *raft) maybeChange(respErr bool) {
	updated := false
	if s.prevSoftSt.term != s.raftFsm.term {
		updated = true
		s.prevSoftSt.term = s.raftFsm.term
		s.resetTick()
	}
	preLeader := s.prevSoftSt.leader
	if preLeader != s.raftFsm.leader {
		updated = true
		s.prevSoftSt.leader = s.raftFsm.leader
		if s.raftFsm.leader != s.config.NodeID {
			if respErr == true || preLeader != s.config.NodeID {
				s.resetPending(ErrNotLeader)
			}
			s.stopSnapping()
		}
		if logger.IsEnableWarn() {
			if s.raftFsm.leader != NoLeader {
				if preLeader == NoLeader {
					logger.Warn("raft:[%v] elected leader %v at term %d.", s.raftFsm.id, s.raftFsm.leader, s.raftFsm.term)
				} else {
					logger.Warn("raft:[%v] changed leader from %v to %v at term %d.", s.raftFsm.id, preLeader, s.raftFsm.leader, s.raftFsm.term)
				}
			} else {
				logger.Warn("raft:[%v] lost leader %v at term %d.", s.raftFsm.id, preLeader, s.raftFsm.term)
			}
		}

		s.raftConfig.StateMachine.HandleLeaderChange(s.raftFsm.leader)
	}
	if updated {
		atomic.StorePointer(&s.curSoftSt, unsafe.Pointer(&softState{leader: s.raftFsm.leader, term: s.raftFsm.term}))
	}
}

func (s *raft) persist() {
	unstableEntries := s.raftFsm.raftLog.unstableEntries()
	if len(unstableEntries) > 0 {
		if err := s.raftConfig.Storage.StoreEntries(unstableEntries); err != nil {
			panic(AppPanicError(fmt.Sprintf("[raft->persist][%v] storage storeEntries err: [%v].", s.raftFsm.id, err)))
		}
	}
	if s.raftFsm.raftLog.committed != s.prevHardSt.Commit || s.raftFsm.term != s.prevHardSt.Term || s.raftFsm.vote != s.prevHardSt.Vote {
		hs := proto.HardState{Term: s.raftFsm.term, Vote: s.raftFsm.vote, Commit: s.raftFsm.raftLog.committed}
		if err := s.raftConfig.Storage.StoreHardState(hs); err != nil {
			panic(AppPanicError(fmt.Sprintf("[raft->persist][%v] storage storeHardState err: [%v].", s.raftFsm.id, err)))
		}
		s.prevHardSt = hs
	}
}

func (s *raft) apply() {
	committedEntries := s.raftFsm.raftLog.nextEnts(noLimit)
	// check ready read index
	if len(committedEntries) == 0 {
		readIndexes := s.raftFsm.readOnly.getReady(s.curApplied.Get())
		if len(readIndexes) == 0 {
			return
		}
		apply := pool.getApply()
		apply.readIndexes = readIndexes
		select {
		case <-s.stopc:
			respondReadIndex(readIndexes, ErrStopped)
		case s.applyc <- apply:
		}
		return
	}

	for _, entry := range committedEntries {
		apply := pool.getApply()
		apply.term = entry.Term
		apply.index = entry.Index
		if future, ok := s.pending[entry.Index]; ok {
			apply.future = future
			delete(s.pending, entry.Index)
		}
		apply.readIndexes = s.raftFsm.readOnly.getReady(entry.Index)

		switch entry.Type {
		case proto.EntryNormal:
			if len(entry.Data) > 0 {
				apply.command = entry.Data
			}

		case proto.EntryConfChange:
			cc := new(proto.ConfChange)
			cc.Decode(entry.Data)
			apply.command = cc
			// repl apply
			peerChange := cc.Peer
			worked := s.raftFsm.applyConfChange(cc)
			if cc.Type == proto.ConfRemoveNode && worked {
				if _, ok := s.raftFsm.replicas[peerChange.PeerID]; !ok {
					if logger.IsEnableWarn() {
						logger.Warn("raft[%v] applying configuration peer [%v] be removed and stop snapshot", s.raftFsm.id, peerChange)
					}
					s.removeSnapping(peerChange.PeerID)
				}
			}

			s.peerState.change(cc)
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] applying configuration change %v.", s.raftFsm.id, cc)
			}
		}
		select {
		case <-s.stopc:
			if apply.future != nil {
				apply.future.respond(nil, ErrStopped)
			}
			if len(apply.readIndexes) > 0 {
				respondReadIndex(apply.readIndexes, ErrStopped)
			}
		case s.applyc <- apply:
		}
	}
}

func (s *raft) advance() {
	s.raftFsm.raftLog.appliedTo(s.raftFsm.raftLog.committed)
	entries := s.raftFsm.raftLog.unstableEntries()
	if len(entries) > 0 {
		s.raftFsm.raftLog.stableTo(entries[len(entries)-1].Index, entries[len(entries)-1].Term)
	}
}

func (s *raft) containsUpdate() bool {
	return len(s.raftFsm.raftLog.unstableEntries()) > 0 || s.raftFsm.raftLog.committed > s.raftFsm.raftLog.applied || len(s.raftFsm.msgs) > 0 ||
		s.raftFsm.raftLog.committed != s.prevHardSt.Commit || s.raftFsm.term != s.prevHardSt.Term || s.raftFsm.vote != s.prevHardSt.Vote ||
		s.raftFsm.readOnly.containsUpdate(s.curApplied.Get())
}

func (s *raft) resetPending(err error) {
	if len(s.pending) > 0 {
		for k, v := range s.pending {
			v.respond(nil, err)
			delete(s.pending, k)
		}
	}
}

func (s *raft) resetTick() {
	for {
		select {
		case <-s.tickc:
		default:
			return
		}
	}
}

func (s *raft) resetApply() {
	for {
		select {
		case apply := <-s.applyc:
			if apply.future != nil {
				apply.future.respond(nil, ErrStopped)
			}
			if len(apply.readIndexes) > 0 {
				respondReadIndex(apply.readIndexes, ErrStopped)
			}
			pool.returnApply(apply)
		default:
			return
		}
	}
}

func (s *raft) getStatus() *Status {
	stopped := false
	select {
	case <-s.stopc:
		stopped = true
	default:
	}

	st := &Status{
		ID:                s.raftFsm.id,
		NodeID:            s.config.NodeID,
		Leader:            s.raftFsm.leader,
		Term:              s.raftFsm.term,
		Index:             s.raftFsm.raftLog.lastIndex(),
		Commit:            s.raftFsm.raftLog.committed,
		Applied:           s.curApplied.Get(),
		Vote:              s.raftFsm.vote,
		State:             s.raftFsm.state.String(),
		RestoringSnapshot: s.restoringSnapshot.Get(),
		PendQueue:         len(s.pending),
		RecvQueue:         len(s.recvc),
		AppQueue:          len(s.applyc),
		Stopped:           stopped,
	}
	if s.raftFsm.state == stateLeader {
		st.Replicas = make(map[uint64]*ReplicaStatus)
		for id, p := range s.raftFsm.replicas {
			st.Replicas[id] = &ReplicaStatus{
				Match:       p.match,
				Commit:      p.committed,
				Next:        p.next,
				State:       p.state.String(),
				Snapshoting: p.state == replicaStateSnapshot,
				Paused:      p.paused,
				Active:      p.active,
				LastActive:  p.lastActive,
				Inflight:    p.count,
			}
		}
	}
	return st
}

func (s *raft) handlePanic(err interface{}) {
	fatalStopc <- s.raftFsm.id

	fatal := &FatalError{
		ID:  s.raftFsm.id,
		Err: fmt.Errorf("raft[%v] occur panic error: [%v]", s.raftFsm.id, err),
	}
	s.raftConfig.StateMachine.HandleFatalEvent(fatal)
}

func (s *raft) getPeers() (peers []uint64) {
	return s.peerState.get()
}

func (s *raft) readIndex(future *Future) {
	if !s.isLeader() {
		future.respond(nil, ErrNotLeader)
		return
	}

	select {
	case <-s.stopc:
		future.respond(nil, ErrStopped)
	case s.readIndexC <- future:
	}
}

func (s *raft) getEntries(future *Future, startIndex uint64, maxSize uint64) {
	req := &entryRequest{
		future:  future,
		index:   startIndex,
		maxSize: maxSize,
	}
	select {
	case <-s.stopc:
		future.respond(nil, ErrStopped)
	case s.entryRequestC <- req:
	}
}

func (s *raft) getEntriesInLoop(req *entryRequest) {
	select {
	case <-s.stopc:
		req.future.respond(nil, ErrStopped)
		return
	default:
	}

	if !s.isLeader() {
		req.future.respond(nil, ErrNotLeader)
		return
	}
	if req.index > s.raftFsm.raftLog.lastIndex() {
		req.future.respond(nil, nil)
		return
	}
	if req.index < s.raftFsm.raftLog.firstIndex() {
		req.future.respond(nil, ErrCompacted)
		return
	}
	entries, err := s.raftFsm.raftLog.entries(req.index, req.maxSize)
	req.future.respond(entries, err)
}
