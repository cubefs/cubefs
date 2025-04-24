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

package raftserver

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/raftserver/wal"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	applyChCapacity = 128
)

type RaftServer interface {
	Stop()
	Propose(ctx context.Context, data []byte) error
	ReadIndex(ctx context.Context) error
	TransferLeadership(ctx context.Context, leader, transferee uint64)
	AddMember(ctx context.Context, member Member) error
	RemoveMember(ctx context.Context, nodeID uint64) error
	IsLeader() bool
	Status() Status

	// In order to prevent log expansion, the application needs to call this method.
	Truncate(index uint64) error
}

// apply contains entries, snapshot to be applied. Once
// an apply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type apply struct {
	entries  []pb.Entry
	snapshot pb.Snapshot
}

type raftServer struct {
	cfg                Config
	proposeTimeout     time.Duration
	tickInterval       time.Duration
	snapTimeout        time.Duration
	lead               uint64
	n                  raft.Node
	shotter            *snapshotter
	store              *raftStorage
	idGen              *Generator
	sm                 StateMachine
	readNotifier       atomic.Value
	notifiers          sync.Map
	tr                 Transport
	applyWait          WaitTime
	leaderChangeMu     sync.RWMutex
	leaderChangeClosed bool
	leaderChangeC      chan struct{}
	readStateC         chan raft.ReadState
	applyc             chan apply
	snapshotC          chan Snapshot
	snapMsgc           chan pb.Message
	readwaitc          chan struct{}
	stopc              chan struct{}
	once               sync.Once
}

func NewRaftServer(cfg *Config) (RaftServer, error) {
	if err := cfg.Verify(); err != nil {
		return nil, err
	}
	tickInterval := time.Duration(cfg.TickInterval) * time.Second
	if cfg.TickIntervalMs > 0 {
		tickInterval = time.Duration(cfg.TickIntervalMs) * time.Millisecond
	}
	proposeTimeout := time.Duration(cfg.ProposeTimeout) * time.Second
	snapTimeout := time.Duration(cfg.SnapshotTimeout) * time.Second
	rs := &raftServer{
		cfg:                *cfg,
		proposeTimeout:     proposeTimeout,
		tickInterval:       tickInterval,
		snapTimeout:        snapTimeout,
		shotter:            newSnapshotter(cfg.MaxSnapConcurrency, snapTimeout),
		idGen:              NewGenerator(cfg.NodeId, time.Now()),
		sm:                 cfg.SM,
		applyWait:          NewTimeList(),
		leaderChangeClosed: false,
		leaderChangeC:      make(chan struct{}, 1),
		readStateC:         make(chan raft.ReadState, 64),
		applyc:             make(chan apply, applyChCapacity),
		snapshotC:          make(chan Snapshot),
		snapMsgc:           make(chan pb.Message, cfg.MaxSnapConcurrency),
		readwaitc:          make(chan struct{}, 1),
		stopc:              make(chan struct{}),
	}
	rs.readNotifier.Store(newReadIndexNotifier())

	begin := time.Now()
	store, err := NewRaftStorage(cfg.WalDir, cfg.WalSync, cfg.UseRocksdb, cfg.NodeId, rs.sm, rs.shotter)
	if err != nil {
		return nil, err
	}
	lastIndex, _ := store.LastIndex()
	firstIndex, _ := store.FirstIndex()
	hs, _, err := store.InitialState()
	if err != nil {
		return nil, err
	}

	log.Infof("load raft wal success, total: %dus, firstIndex: %d, lastIndex: %d, members: %v",
		time.Since(begin).Microseconds(), firstIndex, lastIndex, cfg.Members)

	rs.store = store
	raftCfg := &raft.Config{
		ID:              cfg.NodeId,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         store,
		MaxSizePerMsg:   64 * 1024 * 1024,
		MaxInflightMsgs: 1024,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          log.DefaultLogger,

		DisableProposalForwarding: cfg.DisableProposalForwarding,
	}
	rs.tr = NewTransport(cfg.ListenPort, cfg.ServerTimeoutMin, rs)
	for _, m := range cfg.Members {
		rs.addMember(m)
	}
	if hs.Commit < cfg.Applied {
		cfg.Applied = hs.Commit
	}
	raftCfg.Applied = cfg.Applied
	store.SetApplied(cfg.Applied)
	rs.n = raft.RestartNode(raftCfg)

	go rs.raftStart()
	go rs.raftApply()
	go rs.linearizableReadLoop()
	return rs, nil
}

func (s *raftServer) Stop() {
	s.once.Do(func() {
		s.tr.Stop()
		s.n.Stop()
		close(s.stopc)
		s.shotter.Stop()
		s.store.Close()
	})
}

func (s *raftServer) Propose(ctx context.Context, data []byte) (err error) {
	id := s.idGen.Next()
	return s.propose(ctx, id, pb.EntryNormal, normalEntryEncode(id, data))
}

func (s *raftServer) propose(ctx context.Context, id uint64, entryType pb.EntryType, data []byte) (err error) {
	nr := newNotifier()
	s.notifiers.Store(id, nr)
	var cancel context.CancelFunc

	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, s.proposeTimeout)
		defer cancel()
	}
	defer func() {
		s.notifiers.Delete(id)
	}()
	msg := pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: entryType, Data: data}}}
	if err = s.n.Step(ctx, msg); err != nil {
		return
	}

	return nr.wait(ctx, s.stopc)
}

func (s *raftServer) IsLeader() bool {
	return atomic.LoadUint64(&s.lead) == s.cfg.NodeId
}

func (s *raftServer) ReadIndex(ctx context.Context) error {
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, s.proposeTimeout)
		defer cancel()
	}
	// wait for read state notification
	nr := s.readNotifier.Load().(*readIndexNotifier)
	select {
	case s.readwaitc <- struct{}{}:
	default:
	}
	return nr.Wait(ctx, s.stopc)
}

func (s *raftServer) TransferLeadership(ctx context.Context, leader, transferee uint64) {
	s.n.TransferLeadership(ctx, leader, transferee)
}

func (s *raftServer) changeMember(ctx context.Context, cc pb.ConfChange) (err error) {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return s.propose(ctx, cc.ID, pb.EntryConfChange, data)
}

func (s *raftServer) AddMember(ctx context.Context, member Member) (err error) {
	body, err := member.Marshal()
	if err != nil {
		return err
	}
	addType := pb.ConfChangeAddNode
	if member.Learner {
		addType = pb.ConfChangeAddLearnerNode
	}
	id := s.idGen.Next()
	cc := pb.ConfChange{
		ID:      id,
		Type:    addType,
		NodeID:  member.NodeID,
		Context: body,
	}
	return s.changeMember(ctx, cc)
}

func (s *raftServer) RemoveMember(ctx context.Context, peerId uint64) (err error) {
	id := s.idGen.Next()
	cc := pb.ConfChange{
		ID:     id,
		Type:   pb.ConfChangeRemoveNode,
		NodeID: peerId,
	}
	return s.changeMember(ctx, cc)
}

func (s *raftServer) Status() Status {
	st := s.n.Status()
	status := Status{
		Id:             st.ID,
		Term:           st.Term,
		Vote:           st.Vote,
		Commit:         st.Commit,
		Leader:         st.Lead,
		RaftState:      st.RaftState.String(),
		Applied:        s.store.Applied(),
		RaftApplied:    st.Applied,
		ApplyingLength: len(s.applyc),
		LeadTransferee: st.LeadTransferee,
	}
	for id, pr := range st.Progress {
		var host string
		if m, ok := s.store.GetMember(id); ok {
			host = m.Host
		}
		peer := Peer{
			Id:              id,
			Host:            host,
			Match:           pr.Match,
			Next:            pr.Next,
			State:           pr.State.String(),
			Paused:          pr.IsPaused(),
			PendingSnapshot: pr.PendingSnapshot,
			RecentActive:    pr.RecentActive,
			IsLearner:       pr.IsLearner,
			InflightFull:    pr.Inflights.Full(),
			InflightCount:   pr.Inflights.Count(),
		}
		status.Peers = append(status.Peers, peer)
	}
	return status
}

func (s *raftServer) Truncate(index uint64) error {
	return s.store.Truncate(index)
}

func (s *raftServer) notify(id uint64, err error) {
	val, hit := s.notifiers.Load(id)
	if !hit {
		return
	}
	val.(notifier).notify(err)
}

func (s *raftServer) getSnapshot(name string) *snapshot {
	return s.shotter.Get(name)
}

func (s *raftServer) reportSnapshot(to uint64, status raft.SnapshotStatus) {
	s.n.ReportSnapshot(to, status)
}

func (s *raftServer) deleteSnapshot(name string) {
	s.shotter.Delete(name)
}

func (s *raftServer) raftApply() {
	for {
		select {
		case ap := <-s.applyc:
			entries := ap.entries
			snap := ap.snapshot
			n := len(s.applyc)
			for i := 0; i < n && raft.IsEmptySnap(snap); i++ {
				ap = <-s.applyc
				entries = append(entries, ap.entries...)
				snap = ap.snapshot
			}
			s.applyEntries(entries)
			s.applySnapshotFinish(snap)
			s.applyWait.Trigger(s.store.Applied())
		case snapMsg := <-s.snapMsgc:
			go s.processSnapshotMessage(snapMsg)
		case snap := <-s.snapshotC:
			s.applySnapshot(snap)
		case <-s.stopc:
			return
		}
	}
}

func (s *raftServer) applyConfChange(entry pb.Entry) {
	var cc pb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		log.Panicf("unmarshal confchange error: %v", err)
		return
	}
	if entry.Index <= s.store.Applied() {
		s.notify(cc.ID, nil)
		return
	}
	switch cc.Type {
	case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
		var member Member
		if err := member.Unmarshal(cc.Context); err != nil {
			log.Panicf("failed to unmarshal context that in conf change, error: %v", err)
		}
		s.addMember(member)
	case pb.ConfChangeRemoveNode:
		s.removeMember(cc.NodeID)
	default:
	}
	s.n.ApplyConfChange(cc)
	if err := s.sm.ApplyMemberChange(ConfChange(cc), entry.Index); err != nil {
		log.Panicf("application sm apply member change error: %v", err)
	}

	s.notify(cc.ID, nil)
}

func (s *raftServer) applyEntries(entries []pb.Entry) {
	var (
		prIds        []uint64
		pendinsDatas [][]byte
		lastIndex    uint64
	)
	if len(entries) == 0 {
		return
	}
	for _, ent := range entries {
		switch ent.Type {
		case pb.EntryConfChange:
			if len(pendinsDatas) > 0 {
				if err := s.sm.Apply(pendinsDatas, lastIndex); err != nil {
					log.Panicf("StateMachine apply error: %v", err)
				}
				for i := 0; i < len(prIds); i++ {
					s.notify(prIds[i], nil)
				}
				pendinsDatas = pendinsDatas[0:0]
				prIds = prIds[0:0]
			}
			s.applyConfChange(ent)
		case pb.EntryNormal:
			if len(ent.Data) == 0 {
				continue
			}
			id, data := normalEntryDecode(ent.Data)
			if ent.Index <= s.store.Applied() { // this message should be ignored
				s.notify(id, nil)
				continue
			}
			pendinsDatas = append(pendinsDatas, data)
			prIds = append(prIds, id)
			lastIndex = ent.Index
		default:
		}
	}

	if len(pendinsDatas) > 0 {
		if err := s.sm.Apply(pendinsDatas, lastIndex); err != nil {
			log.Panicf("StateMachine apply error: %v", err)
		}
		for i := 0; i < len(prIds); i++ {
			s.notify(prIds[i], nil)
		}
	}

	if len(entries) > 0 {
		// save applied id
		s.store.SetApplied(entries[len(entries)-1].Index)
	}
}

func (s *raftServer) applySnapshotFinish(st pb.Snapshot) {
	if raft.IsEmptySnap(st) {
		return
	}
	log.Infof("node[%d] apply snapshot[meta: %s, name: %s]", s.cfg.NodeId, st.Metadata.String(), string(st.Data))
	walSnap := wal.Snapshot{Index: st.Metadata.Index, Term: st.Metadata.Term}
	if err := s.store.ApplySnapshot(walSnap); err != nil {
		log.Panicf("apply snapshot error: %v", err)
	}
}

func (s *raftServer) applySnapshot(snap Snapshot) {
	meta := snap.(*applySnapshot).meta
	nr := snap.(*applySnapshot).nr
	log.Infof("apply snapshot(%s) data......", meta.Name)
	// read snapshot data
	if err := s.sm.ApplySnapshot(meta, snap); err != nil {
		log.Errorf("apply snapshot(%s) error: %v", meta.Name, err)
		nr.notify(err)
		return
	}
	log.Infof("apply snapshot(%s) success", meta.Name)
	s.updateMembers(meta.Mbs)
	s.store.SetApplied(meta.Index)
	nr.notify(nil)
}

func (s *raftServer) processSnapshotMessage(m pb.Message) {
	name := string(m.Snapshot.Data)
	st := s.getSnapshot(name)
	if st == nil {
		log.Errorf("not found snapshot(%s)", name)
		s.reportSnapshot(m.To, raft.SnapshotFailure)
		return
	}
	defer s.deleteSnapshot(name)
	if err := s.tr.SendSnapshot(m.To, st); err != nil {
		s.reportSnapshot(m.To, raft.SnapshotFailure)
		log.Errorf("send snapshot(%s) to node(%d) error: %v", name, m.To, err)
		return
	}
	s.reportSnapshot(m.To, raft.SnapshotFinish)
	// send snapshot message to m.TO
	s.tr.Send([]pb.Message{m})
}

func (s *raftServer) raftStart() {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopc:
			return
		case <-ticker.C:
			s.n.Tick()
		case rd := <-s.n.Ready():
			if rd.SoftState != nil {
				leader := atomic.SwapUint64(&s.lead, rd.SoftState.Lead)
				if rd.SoftState.Lead != leader {
					var leaderHost string
					if m, ok := s.store.GetMember(rd.SoftState.Lead); ok {
						leaderHost = m.Host
					}
					if rd.SoftState.Lead == raft.None {
						s.leaderChangeMu.Lock()
						s.leaderChangeC = make(chan struct{}, 1)
						s.leaderChangeClosed = false
						s.leaderChangeMu.Unlock()
					} else {
						if !s.leaderChangeClosed {
							close(s.leaderChangeC)
							s.leaderChangeClosed = true
						}
					}
					s.sm.LeaderChange(rd.SoftState.Lead, leaderHost)
				}
			}

			if len(rd.ReadStates) != 0 {
				select {
				case s.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-s.stopc:
					return
				default:
					log.Warn("read state chan is not ready!!!")
				}
			}

			ap := apply{
				entries:  rd.CommittedEntries,
				snapshot: rd.Snapshot,
			}

			select {
			case s.applyc <- ap:
			case <-s.stopc:
				return
			}
			s.tr.Send(s.processMessages(rd.Messages))

			err := s.store.Save(rd.HardState, rd.Entries)
			if err != nil {
				log.Panicf("save raft entries error: %v", err)
			}

			s.n.Advance()
		}
	}
}

func (s *raftServer) processMessages(ms []pb.Message) []pb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if _, hit := s.store.GetMember(ms[i].To); !hit {
			ms[i].To = 0
		}
		if ms[i].Type == pb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == pb.MsgSnap {
			select {
			case s.snapMsgc <- ms[i]:
			default:
				s.shotter.Delete(string(ms[i].Snapshot.Data))
				s.n.ReportSnapshot(ms[i].To, raft.SnapshotFailure)
			}
			ms[i].To = 0
		}
	}
	return ms
}

func (s *raftServer) readIndexOnce() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.proposeTimeout)
	defer cancel()
	readId := strconv.AppendUint([]byte{}, s.idGen.Next(), 10)
	s.leaderChangeMu.RLock()
	leaderChangeC := s.leaderChangeC
	s.leaderChangeMu.RUnlock()
	select {
	case <-leaderChangeC:
	case <-s.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	err := s.n.ReadIndex(ctx, readId)
	if err != nil {
		log.Errorf("read index error: %v", err)
		return err
	}

	done := false
	var rs raft.ReadState
	for !done {
		select {
		case rs = <-s.readStateC:
			done = bytes.Equal(rs.RequestCtx, readId)
			if !done {
				log.Warn("ignored out-of-date read index response")
			}
		case <-ctx.Done():
			log.Warnf("raft read index timeout, the length of applyC is %d", len(s.applyc))
			return ctx.Err()
		case <-s.stopc:
			return ErrStopped
		}
	}
	if s.store.Applied() < rs.Index {
		select {
		case <-s.applyWait.Wait(rs.Index):
		case <-s.stopc:
			return ErrStopped
		}
	}
	return nil
}

func (s *raftServer) linearizableReadLoop() {
	for {
		select {
		case <-s.readwaitc:
		case <-s.stopc:
			return
		}
		nextnr := newReadIndexNotifier()
		nr := s.readNotifier.Load().(*readIndexNotifier)

		var err error
		for {
			err = s.readIndexOnce()
			if err == nil || err == ErrStopped {
				break
			}
		}

		nr.Notify(err)
		s.readNotifier.Store(nextnr)
	}
}

func (s *raftServer) addMember(member Member) {
	s.store.AddMembers(member)
	if member.NodeID != s.cfg.NodeId {
		s.tr.AddMember(member)
	}
}

func (s *raftServer) removeMember(id uint64) {
	s.store.RemoveMember(id)
	s.tr.RemoveMember(id)
}

func (s *raftServer) updateMembers(mbs []*Member) {
	s.store.SetMembers(mbs)
	s.tr.SetMembers(mbs)
}

func (s *raftServer) handleMessage(msgs raftMsgs) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.proposeTimeout)
	defer cancel()
	for i := 0; i < msgs.Len(); i++ {
		if err := s.n.Step(ctx, msgs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *raftServer) handleSnapshot(st Snapshot) error {
	select {
	case s.snapshotC <- st:
	case <-s.stopc:
		return ErrStopped
	}

	return st.(*applySnapshot).nr.wait(context.TODO(), s.stopc)
}
