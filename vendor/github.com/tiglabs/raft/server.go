// Copyright 2018 The tiglabs raft Authors.
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
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util"
)

var (
	fatalStopc = make(chan uint64)
)

type RaftServer struct {
	config   *Config
	ticker   *time.Ticker
	pmTicker *time.Ticker
	heartc   chan *proto.Message
	stopc    chan struct{}
	mu       sync.RWMutex
	rafts    map[uint64]*raft
}

func NewRaftServer(config *Config) (*RaftServer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	rs := &RaftServer{
		config:   config,
		ticker:   time.NewTicker(config.TickInterval),
		pmTicker: time.NewTicker(config.TickInterval * time.Duration(config.PromoteTick)),
		rafts:    make(map[uint64]*raft),
		heartc:   make(chan *proto.Message, 512),
		stopc:    make(chan struct{}),
	}
	if transport, err := NewMultiTransport(rs, &config.TransportConfig); err != nil {
		return nil, err
	} else {
		rs.config.transport = transport
	}

	util.RunWorkerUtilStop(rs.run, rs.stopc)
	return rs, nil
}

func (rs *RaftServer) run() {
	ticks := 0
	//entriesTicks:=time.NewTicker(time.Second)
	randomSeedTicker := time.NewTicker(time.Minute)
	for {
		select {
		case <-rs.stopc:
			randomSeedTicker.Stop()
			return

		case id := <-fatalStopc:
			rs.mu.Lock()
			delete(rs.rafts, id)
			rs.mu.Unlock()
		//case <-entriesTicks.C	:
		//	fmt.Println(fmt.Sprintf("GetEntriesCnt(%v),PutEntriesCnt(%v)",proto.LoadGetEntryCnt(),proto.LoadPutEntryCnt()))

		case m := <-rs.heartc:
			switch m.Type {
			case proto.ReqMsgHeartBeat:
				rs.handleHeartbeat(m)
			case proto.RespMsgHeartBeat:
				rs.handleHeartbeatResp(m)
			}

		case <-rs.ticker.C:
			ticks++
			if ticks >= rs.config.HeartbeatTick {
				ticks = 0
				rs.sendHeartbeat()
			}

			rs.mu.RLock()
			for _, raft := range rs.rafts {
				raft.tick()
			}
			rs.mu.RUnlock()
		case <-randomSeedTicker.C:
			rand.Seed(time.Now().UnixNano())
		case <-rs.pmTicker.C:
			rs.mu.RLock()
			for _, raft := range rs.rafts {
				raft.promote()
			}
			rs.mu.RUnlock()
		}
	}
}

func (rs *RaftServer) Stop() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	select {
	case <-rs.stopc:
		return

	default:
		close(rs.stopc)
		rs.ticker.Stop()
		rs.pmTicker.Stop()
		wg := new(sync.WaitGroup)
		for id, s := range rs.rafts {
			delete(rs.rafts, id)
			wg.Add(1)
			go func(r *raft) {
				defer wg.Done()
				r.stop()
			}(s)
		}
		wg.Wait()
		rs.config.transport.Stop()
	}
}

func (rs *RaftServer) SetSyncWALOnUnstable(enable bool) {
	if enable != rs.config.SyncWALOnUnstable {
		rs.config.SyncWALOnUnstable = enable
		logger.Debug("server side [SyncWALOnUnstable] set to [%v]", enable)
	}
}

func (rs *RaftServer) IsSyncWALOnUnstable() (enabled bool) {
	enabled = rs.config.SyncWALOnUnstable
	return
}

func (rs *RaftServer) CreateRaft(raftConfig *RaftConfig) error {
	var (
		raft *raft
		err  error
	)

	defer func() {
		if err != nil {
			logger.Error("CreateRaft [%v] failed, error is:\r\n %s", raftConfig.ID, err.Error())
		}
	}()

	if raft, err = newRaft(rs.config, raftConfig); err != nil {
		return err
	}
	if raft == nil {
		err = errors.New("CreateRaft return nil, maybe occur panic.")
		return err
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.rafts[raftConfig.ID]; ok {
		raft.stop()
		err = ErrRaftExists
		return err
	}
	rs.rafts[raftConfig.ID] = raft
	return nil
}

func (rs *RaftServer) RemoveRaft(id uint64) error {
	rs.mu.Lock()
	raft, ok := rs.rafts[id]
	delete(rs.rafts, id)
	rs.mu.Unlock()

	if ok {
		raft.stop()
	}
	return nil
}

func (rs *RaftServer) Submit(ctx context.Context, id uint64, cmd []byte) (future *Future) {

	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture()
	future.ctx = ctx
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.propose(cmd, future)
	return
}

func (rs *RaftServer) ChangeMember(id uint64, changeType proto.ConfChangeType, peer proto.Peer, context []byte) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture()
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}

	if changeType == proto.ConfPromoteLearner {
		raft.proposePromoteLearnerMemberChange(&proto.ConfChange{Type: changeType, Peer: peer, Context: context}, future, false)
	} else {
		raft.proposeMemberChange(&proto.ConfChange{Type: changeType, Peer: peer, Context: context}, future)
	}
	return
}

func (rs *RaftServer) ResetMember(id uint64, peers []proto.Peer, context []byte) (err error) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()
	if !ok {
		err = ErrRaftNotExists
		return
	}
	if peers == nil || len(peers) == 0 {
		err = ErrPeersEmpty
		return
	}

	raft.raftFsm.applyResetPeer(&proto.ResetPeers{NewPeers: peers, Context: context})
	raft.peerState.reset(peers)
	return nil
}

func (rs *RaftServer) Status(id uint64) (status *Status) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		status = raft.status()
	}
	if status == nil {
		status = &Status{
			ID:      id,
			NodeID:  rs.config.NodeID,
			Stopped: true,
		}
	}
	return
}

func (rs *RaftServer) LeaderTerm(id uint64) (leader, term uint64) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.leaderTerm()
	}
	return NoLeader, 0
}

func (rs *RaftServer) IsLeader(id uint64) bool {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.isLeader()
	}
	return false
}

func (rs *RaftServer) AppliedIndex(id uint64) uint64 {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.applied()
	}
	return 0
}

func (rs *RaftServer) CommittedIndex(id uint64) uint64 {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.committed()
	}
	return 0
}

func (rs *RaftServer) FirstCommittedIndex(id uint64) uint64 {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.raftFsm.raftLog.firstIndex()
	}
	return 0
}

func (rs *RaftServer) TryToLeader(id uint64) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture()
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.tryToLeader(future)
	return
}

func (rs *RaftServer) Truncate(id uint64, index uint64) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if !ok {
		return
	}
	raft.truncate(index)
}

func (rs *RaftServer) GetUnreachable(id uint64) (nodes []uint64) {
	downReplicas := rs.GetDownReplicas(id)
	for _, r := range downReplicas {
		nodes = append(nodes, r.NodeID)
	}
	return
}

// GetDownReplicas 获取down的副本
func (rs *RaftServer) GetDownReplicas(id uint64) (downReplicas []DownReplica) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if !ok {
		return nil
	}

	status := raft.status()
	if status != nil && len(status.Replicas) > 0 {
		for n, r := range status.Replicas {
			if n == rs.config.NodeID || r.IsLearner {
				continue
			}
			since := time.Since(r.LastActive)
			// 两次心跳内没活跃就视为Down
			downDuration := since - time.Duration(2*rs.config.HeartbeatTick)*rs.config.TickInterval
			if downDuration > 0 {
				downReplicas = append(downReplicas, DownReplica{
					NodeID:      n,
					DownSeconds: int(downDuration / time.Second),
				})
			}
		}
	}
	return
}

// GetPendingReplica get snapshot pending followers
func (rs *RaftServer) GetPendingReplica(id uint64) (peers []uint64) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if !ok {
		return nil
	}

	status := raft.status()
	if status != nil && len(status.Replicas) > 0 {
		for n, r := range status.Replicas {
			if n == rs.config.NodeID {
				continue
			}
			if r.Snapshoting {
				peers = append(peers, n)
			}
		}
	}
	return
}

// ReadIndex read index
func (rs *RaftServer) ReadIndex(id uint64) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture()
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.readIndex(future)
	return
}

// GetEntries get raft log entries
func (rs *RaftServer) GetEntries(id uint64, startIndex uint64, maxSize uint64) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture()
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.getEntries(future, startIndex, maxSize)
	return
}

func (rs *RaftServer) sendHeartbeat() {
	// key: sendto nodeId; value: range ids
	nodes := make(map[uint64]proto.HeartbeatContext)
	rs.mu.RLock()
	for id, raft := range rs.rafts {
		if !raft.isLeader() {
			continue
		}
		peers := raft.getPeers()
		for _, p := range peers {
			nodes[p] = append(nodes[p], proto.ContextInfo{ID: id, IsUnstable: raft.getRiskState() != stateStable})
		}
	}
	rs.mu.RUnlock()

	for to, ctx := range nodes {
		if to == rs.config.NodeID {
			continue
		}

		msg := proto.GetMessage()
		msg.Type = proto.ReqMsgHeartBeat
		msg.From = rs.config.NodeID
		msg.To = to
		msg.Context = proto.EncodeHBContext(ctx)
		rs.config.transport.Send(msg)
	}
}

func (rs *RaftServer) handleHeartbeat(m *proto.Message) {
	ctx := proto.DecodeHBContext(m.Context)
	m.HeartbeatContext = ctx
	var respCtx proto.HeartbeatContext
	rs.mu.RLock()
	for _, ent := range ctx {
		if raft, ok := rs.rafts[ent.ID]; ok {
			raft.reciveMessage(m)
			respCtx = append(respCtx, ent)
		}
	}
	rs.mu.RUnlock()

	msg := proto.GetMessage()
	msg.Type = proto.RespMsgHeartBeat
	msg.From = rs.config.NodeID
	msg.To = m.From
	msg.Context = proto.EncodeHBContext(respCtx)
	rs.config.transport.Send(msg)
}

func (rs *RaftServer) handleHeartbeatResp(m *proto.Message) {
	ctx := proto.DecodeHBContext(m.Context)

	rs.mu.RLock()
	defer rs.mu.RUnlock()

	for _, ent := range ctx {
		if raft, ok := rs.rafts[ent.ID]; ok {
			raft.reciveMessage(m)
		}
	}
}

func (rs *RaftServer) reciveMessage(m *proto.Message) {
	if m.Type == proto.ReqMsgHeartBeat || m.Type == proto.RespMsgHeartBeat {
		rs.heartc <- m
		return
	}

	rs.mu.RLock()
	raft, ok := rs.rafts[m.ID]
	rs.mu.RUnlock()
	if ok {
		raft.reciveMessage(m)
	}
}

func (rs *RaftServer) reciveSnapshot(req *snapshotRequest) {
	rs.mu.RLock()
	raft, ok := rs.rafts[req.header.ID]
	rs.mu.RUnlock()

	if !ok {
		req.respond(ErrRaftNotExists)
		return
	}
	raft.reciveSnapshot(req)
}

// wait: 等待至Flush完成
func (rs *RaftServer) Flush(id uint64, wait bool) (err error) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		err = raft.flush(wait)
	}
	return
}

func (rs *RaftServer) Config() *Config {
	return rs.config
}
