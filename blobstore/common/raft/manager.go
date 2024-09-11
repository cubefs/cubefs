// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package raft

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	stateQueued uint8 = 1 << iota
	stateProcessReady
	stateProcessRaftRequestMsg
	stateProcessTick
)

const (
	defaultGroupStateMuShardCount = 256
	defaultTickIntervalMS         = 200
	defaultHeartbeatTickInterval  = 10
	defaultElectionTickInterval   = 5 * defaultHeartbeatTickInterval
	defaultWorkerNum              = 96
	defaultWorkerBufferSize       = 32
	defaultSnapshotWorkerNum      = 16
	defaultSnapshotTimeoutS       = 3600
	defaultSizePerMsg             = uint64(1 << 20)
	defaultInflightMsg            = 128
	defaultProposeMsgNum          = 256
	defaultSnapshotNum            = 3
	defaultConnectionClassNum     = 3
	defaultProposeTimeoutMS       = 5000
	defaultReadIndexTimeoutMS     = defaultProposeTimeoutMS
)

var defaultConnectionClassList []connectionClass

type (
	Manager interface {
		CreateRaftGroup(ctx context.Context, cfg *GroupConfig) (Group, error)
		GetRaftGroup(id uint64) (Group, error)
		RemoveRaftGroup(ctx context.Context, id uint64, clearRaftLog bool) error
		RestartTickLoop(intervalMS int)
		Close()
	}

	groupHandler interface {
		HandlePropose(ctx context.Context, id uint64, req proposalRequest) error
		HandleSnapshot(ctx context.Context, id uint64, message raftpb.Message) error
		HandleSendRaftMessageRequest(ctx context.Context, req *RaftMessageRequest, class connectionClass) error
		HandleSendRaftSnapshotRequest(ctx context.Context, snapshot *outgoingSnapshot, req *RaftMessageRequest) error
		HandleSignalToWorker(ctx context.Context, id uint64)
		HandleMaybeCoalesceHeartbeat(ctx context.Context, groupId uint64, msg *raftpb.Message) bool
		HandleNextID() uint64
	}

	groupProcessor interface {
		ID() uint64
		NodeID() uint64
		Tick()
		WithRaftRawNodeLocked(f func(rn *raft.RawNode) error) error
		ProcessSendRaftMessage(ctx context.Context, messages []raftpb.Message)
		ProcessSendSnapshot(ctx context.Context, m raftpb.Message)
		ProcessRaftMessageRequest(ctx context.Context, req *RaftMessageRequest) error
		ProcessRaftSnapshotRequest(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error
		SaveHardStateAndEntries(ctx context.Context, hs raftpb.HardState, entries []raftpb.Entry) error
		ApplyLeaderChange(nodeID uint64) error
		ApplySnapshot(ctx context.Context, snap raftpb.Snapshot) error
		ApplyCommittedEntries(ctx context.Context, entries []raftpb.Entry) (err error)
		ApplyReadIndex(ctx context.Context, readState raft.ReadState)
		AddUnreachableRemoteReplica(remote uint64)
	}
)

type (
	Config struct {
		NodeID uint64
		// TickIntervalMs is the millisecond interval of timer which check heartbeat and election timeout.
		// The default value is 200ms.
		TickIntervalMS int `json:"tick_interval_ms"`
		// HeartbeatTick is the heartbeat interval. A leader sends heartbeat
		// message to maintain the leadership every heartbeat interval.
		// The default value is 10x of TickInterval.
		HeartbeatTick int `json:"heartbeat_tick"`
		// ElectionTick is the election timeout. If a follower does not receive any message
		// from the leader of current term during ElectionTick, it will become candidate and start an election.
		// ElectionTick must be greater than HeartbeatTick.
		// We suggest to use ElectionTick = 10 * HeartbeatTick to avoid unnecessary leader switching.
		// The default value is 10x of HeartbeatTick.
		ElectionTick int `json:"election_tick"`
		// CoalescedHeartbeatsIntervalMS specifies the coalesced heartbeat intervals
		// The default value is the half of TickInterval.
		CoalescedHeartbeatsIntervalMS int `json:"coalesced_heartbeats_interval_ms"`
		// MaxSizePerMsg limits the max size of each append message.
		// The default value is 1M.
		MaxSizePerMsg uint64 `json:"max_size_per_msg"`
		// MaxInflightMsg limits the max number of in-flight append messages during optimistic replication phase.
		// The application transportation layer usually has its own sending buffer over TCP/UDP.
		// Setting MaxInflightMsgs to avoid overflowing that sending buffer.
		// The default value is 128.
		MaxInflightMsg int `json:"max_inflight_msg"`
		// ReadOnlyOption specifies how the read only request is processed.
		//
		// ReadOnlySafe guarantees the linearizability of the read only request by
		// communicating with the quorum. It is the default and suggested option.
		//
		// ReadOnlyLeaseBased ensures linearizability of the read only request by
		// relying on the leader lease. It can be affected by clock drift.
		// If the clock drift is unbounded, leader might keep the lease longer than it
		// should (clock can move backward/pause without any bound). ReadIndex is not safe
		// in that case.
		// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
		ReadOnlyOption raft.ReadOnlyOption `json:"read_only_option"`
		// MaxProposeMsgNum specifies the max raft step msg num
		// The default value is 256.
		MaxProposeMsgNum int `json:"max_propose_msg_num"`
		// MaxWorkerNum specifies the max worker num of raft group processing
		// The default value is 96.
		MaxWorkerNum int `json:"max_worker_num"`
		// MaxWorkerNum specifies the max queue buffer size for one one worker
		// The default value is 32.
		MaxWorkerBufferSize int `json:"max_worker_buffer_size"`
		// MaxSnapshotWorkerNum specifies the max snapshot worker num of sending raft snapshot
		// The default value is 16.
		MaxSnapshotWorkerNum int `json:"max_snapshot_worker_num"`
		// MaxSnapshotNum limits the max number of snapshot num per raft group.
		// The default value is 3.
		MaxSnapshotNum int `json:"max_snapshot_num"`
		// MaxConnectionClassNum limits the default client connection class num
		// The default value is 3 and the max value can't exceed 3 or the through output may decline.
		MaxConnectionClassNum int `json:"max_connection_class_num"`
		// SnapshotTimeoutS limits the max expire time of snapshot
		// The default value is 1 hour.
		SnapshotTimeoutS int `json:"snapshot_timeout_s"`
		// ProposeTimeout specifies the proposal timeout interval
		// The default value is 5s.
		ProposeTimeoutMS int `json:"propose_timeout_ms"`
		// ReadIndexTimeout specifies the read index timeout interval
		// The default value is 5s.
		ReadIndexTimeoutMS int `json:"read_index_timeout_ms"`

		TransportConfig TransportConfig `json:"transport_config"`
		Transport       *Transport      `json:"-"`
		Logger          raft.Logger     `json:"-"`
		Storage         Storage         `json:"-"`
		Resolver        AddressResolver `json:"-"`
	}
	GroupConfig struct {
		ID      uint64
		Applied uint64
		Members []Member
		SM      StateMachine
	}

	groupState struct {
		state uint8
		id    uint64
	}
	snapshotMessage struct {
		groupID uint64
		message raftpb.Message
	}
)

func NewManager(cfg *Config) (Manager, error) {
	initConfig(cfg)
	if cfg.Logger != nil {
		raft.SetLogger(cfg.Logger)
	}

	manager := &manager{
		idGenerator:   newIDGenerator(cfg.NodeID, time.Now()),
		snapshotQueue: make(chan snapshotMessage, 1024),
		cfg:           cfg,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	manager.coalescedMu.heartbeats = make(map[uint64][]RaftHeartbeat)
	manager.coalescedMu.heartbeatResponses = make(map[uint64][]RaftHeartbeat)
	for i := range manager.groupStateMu {
		manager.groupStateMu[i] = &struct {
			sync.RWMutex
			state map[uint64]groupState
		}{state: make(map[uint64]groupState)}
	}

	transport := cfg.Transport
	if transport == nil {
		cfg.TransportConfig.Resolver = &cacheAddressResolver{resolver: cfg.Resolver}
		transport = NewTransport(&cfg.TransportConfig)
	}
	transport.RegisterHandler((*internalTransportHandler)(manager))
	manager.transport = transport

	for i := 0; i < cfg.MaxWorkerNum; i++ {
		workerCh := make(chan groupState, 20)
		manager.workerChs = append(manager.workerChs, workerCh)
	}
	for i, ch := range manager.workerChs {
		idx := i
		workerCh := ch
		go (*internalGroupHandler)(manager).worker(idx, workerCh)
	}

	for i := 0; i < cfg.MaxSnapshotWorkerNum; i++ {
		go (*internalGroupHandler)(manager).snapshotWorker()
	}

	go manager.raftTickLoop()
	go manager.coalescedHeartbeatsLoop()

	return manager, nil
}

type manager struct {
	groups            sync.Map
	proposalQueues    sync.Map
	raftMessageQueues sync.Map
	snapshotQueue     chan snapshotMessage
	workerChs         []chan groupState
	coalescedMu       struct {
		sync.Mutex
		heartbeats         map[uint64][]RaftHeartbeat
		heartbeatResponses map[uint64][]RaftHeartbeat
	}
	groupStateMu [defaultGroupStateMuShardCount]*struct {
		sync.RWMutex
		state map[uint64]groupState
	}
	workerRoundRobinCount uint32
	done                  chan struct{}
	stop                  chan struct{}

	idGenerator *idGenerator
	transport   *Transport
	cfg         *Config
}

func (m *manager) CreateRaftGroup(ctx context.Context, cfg *GroupConfig) (Group, error) {
	span := trace.SpanFromContextSafe(ctx)

	storage, err := newStorage(storageConfig{
		id:              cfg.ID,
		maxSnapshotNum:  m.cfg.MaxSnapshotNum,
		snapshotTimeout: time.Duration(m.cfg.SnapshotTimeoutS) * time.Second,
		members:         cfg.Members,
		raw:             m.cfg.Storage,
		sm:              cfg.SM,
	})
	if err != nil {
		return nil, errors.Info(err, "mew raft storage failed")
	}

	hs, cs, _ := storage.InitialState()
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		return nil, err
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil, err
	}
	span.Debugf("hard state: %+v, conf state: %+v, first index: %d, last index: %d", hs, cs, firstIndex, lastIndex)

	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              m.cfg.NodeID,
		ElectionTick:    m.cfg.ElectionTick,
		HeartbeatTick:   m.cfg.HeartbeatTick,
		Storage:         storage,
		Applied:         cfg.Applied,
		MaxSizePerMsg:   m.cfg.MaxSizePerMsg,
		MaxInflightMsgs: m.cfg.MaxInflightMsg,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          m.cfg.Logger,
	})
	if err != nil {
		return nil, err
	}

	g := &group{
		id:           cfg.ID,
		appliedIndex: cfg.Applied,

		cfg: groupConfig{
			nodeID:           m.cfg.NodeID,
			proposeTimeout:   time.Duration(m.cfg.ProposeTimeoutMS) * time.Millisecond,
			readIndexTimeout: time.Duration(m.cfg.ReadIndexTimeoutMS) * time.Millisecond,
		},
		sm:      cfg.SM,
		storage: storage,
		handler: (*internalGroupHandler)(m),
	}
	g.rawNodeMu.rawNode = rawNode

	_, loaded := m.groups.LoadOrStore(cfg.ID, g)
	if loaded {
		return nil, errors.New("group already exists")
	}

	queue := newProposalQueue(m.cfg.MaxProposeMsgNum)
	m.proposalQueues.Store(cfg.ID, queue)
	m.raftMessageQueues.Store(cfg.ID, &raftMessageQueue{})
	return g, nil
}

func (m *manager) GetRaftGroup(id uint64) (Group, error) {
	v, ok := m.groups.Load(id)
	if !ok {
		return nil, errors.New("group not found")
	}
	return v.(Group), nil
}

func (m *manager) RemoveRaftGroup(ctx context.Context, id uint64, clearRaftData bool) error {
	v, ok := m.groups.LoadAndDelete(id)
	m.proposalQueues.Delete(id)
	m.raftMessageQueues.Delete(id)

	if ok {
		v.(Group).Close()

		// clear raft group's raft log
		if clearRaftData {
			return v.(Group).Clear()
		}
	}

	return nil
}

func (m *manager) Close() {
	m.transport.Close()
	close(m.done)
}

func (h *internalGroupHandler) processTick(ctx context.Context, g groupProcessor) {
	g.Tick()
}

func (h *internalGroupHandler) processRaftRequestMsg(ctx context.Context, g groupProcessor) bool {
	span := trace.SpanFromContext(ctx)
	value, ok := h.raftMessageQueues.Load(g.ID())
	if !ok {
		return false
	}

	q := value.(*raftMessageQueue)
	infos, ok := q.drain()
	if !ok {
		return false
	}
	defer q.recycle(infos)

	var hadError bool
	for i := range infos {
		info := &infos[i]
		if pErr := g.ProcessRaftMessageRequest(ctx, info.req); pErr != nil {
			span.Errorf("process raft message request failed: %s", pErr)

			hadError = true
			if err := info.respStream.Send(newRaftMessageResponse(info.req, newError(ErrCodeGroupHandleRaftMessage, pErr.Error()))); err != nil {
				// Seems excessive to log this on every occurrence as the other side
				// might have closed.
				span.Errorf("req[%+v] sending response error: %s", info.req, err)
			}
		}
	}

	if hadError {
		// dropping the request queue to free up space when group has been deleted
		if _, exists := h.groups.Load(g.ID()); !exists {
			q.Lock()
			if len(q.infos) == 0 {
				h.raftMessageQueues.Delete(g.ID())
			}
			q.Unlock()
		}
	}

	return true
}

// process group ready
//  1. get all proposal message and send request msg
//  2. apply soft state
//  3. send raft request messages
//  4. save hard state and entries into wal log before apply committed entries,
//     they should be packed into one write batch with hard state
//  5. apply committed entries (include conf state and normal entry)
func (h *internalGroupHandler) processReady(ctx context.Context, g groupProcessor) {
	span := trace.SpanFromContext(ctx)

	if err := h.processProposal(ctx, g); err != nil {
		span.Fatalf("process proposal msg failed: %s", err)
	}

	var (
		hasReady bool
		rd       raft.Ready
	)
	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		hasReady = rn.HasReady()
		if !hasReady {
			return nil
		}
		rd = rn.Ready()
		return nil
	})
	if !hasReady {
		return
	}

	if rd.SoftState != nil {
		if err := g.ApplyLeaderChange(rd.SoftState.Lead); err != nil {
			span.Fatalf("leader change notify failed: %s", err)
		}
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := g.ApplySnapshot(ctx, rd.Snapshot); err != nil {
			span.Fatalf("apply raft snapshot failed: %s", err)
		}
	}

	if len(rd.Messages) > 0 {
		span.Debugf("node[%d] send request: %+v", h.cfg.NodeID, rd.Messages)
		g.ProcessSendRaftMessage(ctx, rd.Messages)
	}

	if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
		// todo: don't fatal but return error to upper application
		if err := g.SaveHardStateAndEntries(ctx, rd.HardState, rd.Entries); err != nil {
			span.Fatalf("save hard state and entries failed: %s", err)
		}
	}

	if len(rd.CommittedEntries) > 0 {
		// todo: don't fatal but return error to upper application
		if err := g.ApplyCommittedEntries(ctx, rd.CommittedEntries); err != nil {
			span.Fatalf("apply committed entries failed: %s", err)
		}
	}
	for i := range rd.ReadStates {
		g.ApplyReadIndex(ctx, rd.ReadStates[i])
	}

	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.Advance(rd)
		return nil
	})
}

func (h *internalGroupHandler) processProposal(ctx context.Context, g groupProcessor) (err error) {
	v, ok := h.proposalQueues.Load(g.ID())
	if !ok {
		span := trace.SpanFromContext(ctx)
		span.Warnf("proposal queue has been deleted, group id: %d", g.ID())
		return nil
	}
	queue := v.(proposalQueue)

	entries := make([]raftpb.Entry, 0, queue.Len())
	queue.Iter(func(p proposalRequest) bool {
		entries = append(entries, raftpb.Entry{
			Type: p.entryType,
			Data: p.data,
		})
		return true
	})

	if len(entries) == 0 {
		return nil
	}

	return g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		return rn.Step(raftpb.Message{
			Type:    raftpb.MsgProp,
			From:    g.NodeID(),
			Entries: entries,
		})
	})
}

func (m *manager) raftTickLoop() {
	ticker := time.NewTicker(time.Duration(m.cfg.TickIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	var groupIDs []uint64

	for {
		select {
		case <-ticker.C:
			groupIDs = groupIDs[:0]
			m.groups.Range(func(key, value interface{}) bool {
				groupIDs = append(groupIDs, key.(uint64))
				return true
			})

			for _, id := range groupIDs {
				(*internalGroupHandler)(m).signalToWorker(id, stateProcessTick)
			}
		case <-m.stop:
			return
		case <-m.done:
			return
		}
	}
}

func (m *manager) RestartTickLoop(intervalMS int) {
	close(m.stop)
	m.cfg.TickIntervalMS = intervalMS
	time.Sleep(1 * time.Second)
	m.stop = make(chan struct{})
	go m.raftTickLoop()
}

func (m *manager) coalescedHeartbeatsLoop() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "coalescedHeartbeatsLoop")
	ticker := time.NewTicker(time.Duration(m.cfg.CoalescedHeartbeatsIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.sendQueuedHeartbeats(ctx)
		case <-m.done:
			return
		}
	}
}

func (m *manager) sendQueuedHeartbeats(ctx context.Context) {
	m.coalescedMu.Lock()
	heartbeats := m.coalescedMu.heartbeats
	heartbeatResponses := m.coalescedMu.heartbeatResponses
	m.coalescedMu.heartbeats = map[uint64][]RaftHeartbeat{}
	m.coalescedMu.heartbeatResponses = map[uint64][]RaftHeartbeat{}
	m.coalescedMu.Unlock()

	var beatsSent int

	for toNodeID, beats := range heartbeats {
		beatsSent += m.sendQueuedHeartbeatsToNode(ctx, beats, nil, toNodeID)
	}
	for toNodeID, resps := range heartbeatResponses {
		beatsSent += m.sendQueuedHeartbeatsToNode(ctx, nil, resps, toNodeID)
	}
}

// sendQueuedHeartbeatsToNode requires that the s.coalescedMu lock is held. It
// returns the number of heartbeats that were sent.
func (m *manager) sendQueuedHeartbeatsToNode(
	ctx context.Context, beats, resps []RaftHeartbeat, toNodeID uint64,
) int {
	var (
		msgType raftpb.MessageType
		span    = trace.SpanFromContext(ctx)
	)

	if len(beats) == 0 && len(resps) == 0 {
		return 0
	}
	if len(beats) > 0 && len(resps) > 0 {
		span.Fatalf("can't send heartbeat request and response both")
	}

	if len(resps) == 0 {
		msgType = raftpb.MsgHeartbeat
	}
	if len(beats) == 0 {
		msgType = raftpb.MsgHeartbeatResp
	}

	chReq := newRaftMessageRequest()
	*chReq = RaftMessageRequest{
		GroupID: 0,
		To:      toNodeID,
		From:    m.cfg.NodeID,
		Message: raftpb.Message{
			Type: msgType,
		},
		Heartbeats:         beats,
		HeartbeatResponses: resps,
	}

	if err := m.transport.SendAsync(ctx, chReq, systemConnectionClass); err != nil {
		for i := range beats {
			if value, ok := m.groups.Load(beats[i].GroupID); ok {
				value.(*internalGroupProcessor).AddUnreachableRemoteReplica(chReq.To)
			}
		}
		for i := range resps {
			if value, ok := m.groups.Load(resps[i].GroupID); ok {
				value.(*internalGroupProcessor).AddUnreachableRemoteReplica(chReq.To)
			}
		}

		span.Errorf("send queued heartbeats failed: %s", err)
		return 0
	}
	return len(beats) + len(resps)
}

type internalGroupHandler manager

func (h *internalGroupHandler) HandlePropose(ctx context.Context, id uint64, req proposalRequest) error {
	v, ok := h.proposalQueues.Load(id)
	if !ok {
		span := trace.SpanFromContext(ctx)
		span.Warnf("proposal queue has been deleted, group id: %d", id)
		return nil
	}
	queue := v.(proposalQueue)

	if err := queue.Push(ctx, req); err != nil {
		return err
	}

	h.signalToWorker(id, stateProcessReady)
	return nil
}

func (h *internalGroupHandler) HandleSnapshot(ctx context.Context, id uint64, message raftpb.Message) error {
	select {
	case h.snapshotQueue <- snapshotMessage{
		groupID: id,
		message: message,
	}:
		return nil
	default:
		return errors.New("snapshot queue full")
	}
}

func (h *internalGroupHandler) HandleMaybeCoalesceHeartbeat(ctx context.Context, groupID uint64, msg *raftpb.Message) bool {
	// Note: ReadIndex with safe option on leader will broadcast heartbeat request to other nodes
	// We can not coalesce these request and should return false
	if len(msg.Context) > 0 {
		return false
	}

	var hbMap map[uint64][]RaftHeartbeat

	switch msg.Type {
	case raftpb.MsgHeartbeat:
		h.coalescedMu.Lock()
		hbMap = h.coalescedMu.heartbeats
	case raftpb.MsgHeartbeatResp:
		h.coalescedMu.Lock()
		hbMap = h.coalescedMu.heartbeatResponses
	default:
		return false
	}
	hb := RaftHeartbeat{
		GroupID: groupID,
		To:      msg.To,
		From:    msg.From,
		Term:    msg.Term,
		Commit:  msg.Commit,
	}

	if hbMap[msg.To] == nil {
		hbMap[msg.To] = *raftHeartbeatPool.Get().(*[]RaftHeartbeat)
	}
	hbMap[msg.To] = append(hbMap[msg.To], hb)
	h.coalescedMu.Unlock()
	return true
}

func (h *internalGroupHandler) HandleSendRaftMessageRequest(ctx context.Context, req *RaftMessageRequest, class connectionClass) error {
	return h.transport.SendAsync(ctx, req, class)
}

func (h *internalGroupHandler) HandleSendRaftSnapshotRequest(ctx context.Context, snapshot *outgoingSnapshot, req *RaftMessageRequest) error {
	return h.transport.SendSnapshot(ctx, snapshot, req)
}

func (h *internalGroupHandler) HandleSignalToWorker(ctx context.Context, id uint64) {
	h.signalToWorker(id, stateProcessReady)
}

func (h *internalGroupHandler) HandleNextID() uint64 {
	return h.idGenerator.Next()
}

func (h *internalGroupHandler) signalToWorker(groupID uint64, state uint8) {
	if !h.enqueueGroupState(groupID, state) {
		return
	}
	// log.Infof("do signal to group id[%d], state: %d, node id: %d", groupID, state, h.cfg.NodeID)

	count := atomic.AddUint32(&h.workerRoundRobinCount, 1)
	for {
		select {
		case h.workerChs[int(count)%len(h.workerChs)] <- groupState{state: state, id: groupID}:
			// log.Infof("do signal to worker[%d] success, group id: %d, state: %d,  node id: %d", int(count)%len(h.workerChs), groupID, state, h.cfg.NodeID)
			return
		case <-h.done:
			return
		default:
			// log.Infof("do signal to worker[%d] failed, retry again. group id: %d, state: %d, node id: %d", int(count)%len(h.workerChs), groupID, state, h.cfg.NodeID)
			count = count + uint32(rand.Int31n(int32(len(h.workerChs))))
		}
	}
}

// worker do raft state processing job, one raft group will be run in the worker pool with one worker only.
func (h *internalGroupHandler) worker(wid int, ch chan groupState) {
	for {
		select {
		case in := <-ch:
			span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "worker-"+strconv.Itoa(wid)+"/"+strconv.FormatUint(in.id, 10))
			v, ok := h.groups.Load(in.id)
			if !ok {
				span.Warnf("group[%d] has been removed or not created yet", in.id)
				// remove the group state as group has been removed
				h.removeGroupStateForce(in.id)
				continue
			}
			group := (*internalGroupProcessor)(v.(*group))

		AGAIN:
			// span.Debugf("start raft state processing, group state: %+v", in)

			// reset group state into queued, avoid raft group processing currently in worker pool
			// group state may be updated after we get it from input channel, so we need to get the latest state before
			// set stateQueued state into this group
			state := h.setGroupStateForce(group.ID(), stateQueued)
			if state != in.state {
				in.state = state
			}

			// process group request msg
			if in.state&stateProcessRaftRequestMsg != 0 {
				if h.processRaftRequestMsg(ctx, group) {
					in.state |= stateProcessReady
				}
			}

			// process group tick
			if in.state&stateProcessTick != 0 {
				h.processTick(ctx, group)
				in.state |= stateProcessReady
			}

			// process group ready
			if in.state&stateProcessReady != 0 {
				h.processReady(ctx, group)
			}

			state = h.findAndRemoveGroupState(group.ID(), stateQueued)
			// span.Debugf("worker done, find and remove group state, new state: %d", state)
			if state == stateQueued {
				continue
			}
			// new state signal coming, we do the group state processing job again in this worker
			in.state = state
			goto AGAIN

			/*state := h.getGroupState(group.ID())
			if state == stateQueued {
				continue
			}

			in.state = state
			goto AGAIN*/

		case <-h.done:
			return
		}
	}
}

func (h *internalGroupHandler) snapshotWorker() {
	for {
		select {
		case m := <-h.snapshotQueue:
			span, ctx := trace.StartSpanFromContext(context.Background(), "")
			v, ok := h.groups.Load(m.groupID)
			if !ok {
				span.Warnf("group[%d] has been removed", m.groupID)
				continue
			}
			group := (*internalGroupProcessor)(v.(*group))
			group.ProcessSendSnapshot(ctx, m.message)
		case <-h.done:
			return
		}
	}
}

func (h *internalGroupHandler) enqueueGroupState(groupID uint64, state uint8) (enqueued bool) {
	muIndex := groupID % uint64(len(h.groupStateMu))
	stateMu := h.groupStateMu[muIndex]

	stateMu.Lock()
	defer stateMu.Unlock()

	prevState := stateMu.state[groupID]

	if prevState.state&state == state {
		return false
	}

	newState := prevState
	newState.state = newState.state | state
	if newState.state&stateQueued == 0 {
		newState.state |= stateQueued
		enqueued = true
	}
	stateMu.state[groupID] = newState
	return
}

func (h *internalGroupHandler) setGroupStateForce(groupID uint64, state uint8) (currentState uint8) {
	muIndex := groupID % uint64(len(h.groupStateMu))
	stateMu := h.groupStateMu[muIndex]

	stateMu.Lock()
	defer stateMu.Unlock()

	currentState = stateMu.state[groupID].state
	stateMu.state[groupID] = groupState{
		state: state,
		id:    groupID,
	}

	return
}

func (h *internalGroupHandler) findAndRemoveGroupState(groupID uint64, oldState uint8) (newState uint8) {
	muIndex := groupID % uint64(len(h.groupStateMu))
	stateMu := h.groupStateMu[muIndex]

	stateMu.Lock()
	defer stateMu.Unlock()

	groupState := stateMu.state[groupID].state
	if groupState == oldState {
		delete(stateMu.state, groupID)
		return oldState
	}
	return groupState
}

func (h *internalGroupHandler) removeGroupStateForce(groupID uint64) {
	muIndex := groupID % uint64(len(h.groupStateMu))
	stateMu := h.groupStateMu[muIndex]

	stateMu.Lock()
	defer stateMu.Unlock()

	delete(stateMu.state, groupID)
}

type internalTransportHandler manager

func (t *internalTransportHandler) HandleRaftRequest(
	ctx context.Context, req *RaftMessageRequest, respStream MessageResponseStream,
) error {
	span := trace.SpanFromContext(ctx)
	span.Debugf("node[%d] receive request: %+v", t.cfg.NodeID, req)

	if req.IsCoalescedHeartbeat() {
		t.uncoalesceBeats(ctx, req.Heartbeats, raftpb.MsgHeartbeat, respStream)
		t.uncoalesceBeats(ctx, req.HeartbeatResponses, raftpb.MsgHeartbeatResp, respStream)
		return nil
	}

	enqueue := t.handleRaftUncoalescedRequest(ctx, req, respStream)
	if enqueue {
		(*internalGroupHandler)(t).signalToWorker(req.GroupID, stateProcessRaftRequestMsg)
	}

	return nil
}

func (t *internalTransportHandler) HandleRaftResponse(ctx context.Context, resp *RaftMessageResponse) error {
	// todo: dial with these response error return
	if resp.Err != nil {
		span := trace.SpanFromContext(ctx)
		span.Error("HandleRaftResponse with error: %s", resp.Err)

		switch resp.Err {
		case ErrRaftGroupDeleted:
		case ErrReplicaTooOld:
		case ErrGroupHandleRaftMessage:
			return resp.Err
		case ErrGroupNotFound:
			return resp.Err
		}
	}
	return nil
}

func (t *internalTransportHandler) HandleRaftSnapshot(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error {
	span := trace.SpanFromContext(ctx)
	span.Debugf("receive raft snapshot request: %+v", req)

	raftMessage := req.Header.RaftMessageRequest
	v, ok := t.groups.Load(raftMessage.GroupID)
	if !ok {
		return stream.Send(&RaftSnapshotResponse{
			Status:  RaftSnapshotResponse_ERROR,
			Message: ErrGroupNotFound.ErrorMsg,
		})
	}

	// send accepted response first before send snapshot data
	// we must ensure all validation is finished before this send.
	if err := stream.Send(&RaftSnapshotResponse{
		Status:  RaftSnapshotResponse_ACCEPTED,
		Message: "snapshot sender error: no header in the first snapshot request",
	}); err != nil {
		return err
	}

	g := (*internalGroupProcessor)(v.(*group))
	if err := g.ProcessRaftSnapshotRequest(ctx, req, stream); err != nil {
		return stream.Send(&RaftSnapshotResponse{
			Status:  RaftSnapshotResponse_ERROR,
			Message: err.Error(),
		})
	}

	return stream.Send(&RaftSnapshotResponse{Status: RaftSnapshotResponse_APPLIED})
}

func (t *internalTransportHandler) UniqueID() uint64 {
	return t.cfg.NodeID
}

func (t *internalTransportHandler) uncoalesceBeats(
	ctx context.Context,
	beats []RaftHeartbeat,
	msgT raftpb.MessageType,
	respStream MessageResponseStream,
) {
	if len(beats) == 0 {
		return
	}
	span := trace.SpanFromContext(ctx)
	start := time.Now()

	beatReqs := make([]RaftMessageRequest, len(beats))
	makeReqsCost := time.Since(start)
	start = time.Now()

	var groupIDs []uint64
	for i, beat := range beats {
		beatReqs[i] = RaftMessageRequest{
			GroupID: beat.GroupID,
			From:    beat.From,
			To:      beat.To,
			Message: raftpb.Message{
				Type:   msgT,
				From:   beat.From,
				To:     beat.To,
				Term:   beat.Term,
				Commit: beat.Commit,
			},
		}

		enqueue := t.handleRaftUncoalescedRequest(ctx, &beatReqs[i], respStream)
		if enqueue {
			groupIDs = append(groupIDs, beat.GroupID)
		}
	}
	queueCost := time.Since(start)
	start = time.Now()

	for _, id := range groupIDs {
		(*internalGroupHandler)(t).signalToWorker(id, stateProcessRaftRequestMsg)
	}
	signalCost := time.Since(start)

	span.Infof("uncoalesce heartbeats, make request slice cost: %dus, queue cost: %dus, signal cost: %dus",
		makeReqsCost/time.Microsecond, queueCost/time.Microsecond, signalCost/time.Microsecond)
}

func (t *internalTransportHandler) handleRaftUncoalescedRequest(
	ctx context.Context, req *RaftMessageRequest, respStream MessageResponseStream,
) (enqueue bool) {
	span := trace.SpanFromContext(ctx)
	if len(req.Heartbeats)+len(req.HeartbeatResponses) > 0 {
		span.Fatalf("handleRaftUncoalescedRequest can not handle heartbeats and heartbeat responses, but received %+v", req)
	}

	value, ok := t.raftMessageQueues.Load(req.GroupID)
	if !ok {
		span.Warnf("group[%d] has been removed or not created yet", req.GroupID)
		return false
	}

	q := value.(*raftMessageQueue)
	queueLen := q.add(raftMessageInfo{
		req:        req,
		respStream: respStream,
	})

	return queueLen == 1
}

type raftMessageInfo struct {
	req        *RaftMessageRequest
	respStream MessageResponseStream
}

type raftMessageQueue struct {
	infos []raftMessageInfo
	sync.Mutex
}

func (q *raftMessageQueue) add(msg raftMessageInfo) int {
	q.Lock()
	defer q.Unlock()

	q.infos = append(q.infos, msg)
	return len(q.infos)
}

func (q *raftMessageQueue) drain() ([]raftMessageInfo, bool) {
	q.Lock()
	defer q.Unlock()

	if len(q.infos) == 0 {
		return nil, false
	}
	infos := q.infos
	q.infos = nil
	return infos, true
}

func (q *raftMessageQueue) recycle(processed []raftMessageInfo) {
	/*if cap(processed) > 32 {
		return
	}*/
	q.Lock()
	defer q.Unlock()

	if q.infos == nil {
		for i := range processed {
			processed[i] = raftMessageInfo{}
		}
		q.infos = processed[:0]
	}
}

func initConfig(cfg *Config) {
	initialDefaultConfig(&cfg.TickIntervalMS, defaultTickIntervalMS)
	initialDefaultConfig(&cfg.HeartbeatTick, defaultHeartbeatTickInterval)
	initialDefaultConfig(&cfg.ElectionTick, defaultElectionTickInterval)
	initialDefaultConfig(&cfg.CoalescedHeartbeatsIntervalMS, cfg.TickIntervalMS/2)
	initialDefaultConfig(&cfg.MaxWorkerNum, defaultWorkerNum)
	initialDefaultConfig(&cfg.MaxWorkerBufferSize, defaultWorkerBufferSize)
	initialDefaultConfig(&cfg.MaxSnapshotWorkerNum, defaultSnapshotWorkerNum)
	initialDefaultConfig(&cfg.SnapshotTimeoutS, defaultSnapshotTimeoutS)
	initialDefaultConfig(&cfg.MaxInflightMsg, defaultInflightMsg)
	initialDefaultConfig(&cfg.MaxSnapshotNum, defaultSnapshotNum)
	initialDefaultConfig(&cfg.MaxConnectionClassNum, defaultConnectionClassNum)
	initialDefaultConfig(&cfg.MaxSizePerMsg, defaultSizePerMsg)
	initialDefaultConfig(&cfg.ProposeTimeoutMS, defaultProposeTimeoutMS)
	initialDefaultConfig(&cfg.ReadIndexTimeoutMS, defaultReadIndexTimeoutMS)
	initialDefaultConfig(&cfg.MaxProposeMsgNum, defaultProposeMsgNum)

	num := 0
	for i := defaultConnectionClass1; i < systemConnectionClass; i++ {
		num++
		defaultConnectionClassList = append(defaultConnectionClassList, i)
		if num >= cfg.MaxConnectionClassNum {
			break
		}
	}
}

func initialDefaultConfig(t interface{}, defaultValue interface{}) {
	switch t.(type) {
	case *int:
		if *(t.(*int)) <= 0 {
			*(t.(*int)) = defaultValue.(int)
		}
	case *uint32:
		if *(t.(*uint32)) <= 0 {
			*(t.(*uint32)) = defaultValue.(uint32)
		}

	case *uint64:
		if *(t.(*uint64)) <= 0 {
			*(t.(*uint64)) = defaultValue.(uint64)
		}
	default:
	}
}

var raftHeartbeatPool = sync.Pool{
	New: func() interface{} {
		rh := make([]RaftHeartbeat, 0)
		return &rh
	},
}
