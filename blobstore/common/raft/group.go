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
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/cubefs/blobstore/util"
)

type Group interface {
	Propose(ctx context.Context, msg *ProposalData) (ProposalResponse, error)
	LeaderTransfer(ctx context.Context, peerID uint64) error
	ReadIndex(ctx context.Context) error
	Campaign(ctx context.Context) error
	Truncate(ctx context.Context, index uint64) error
	MemberChange(ctx context.Context, mc *Member) error
	Stat() (*Stat, error)
	Close() error
	Clear() error
}

type groupConfig struct {
	nodeID           uint64
	proposeTimeout   time.Duration
	readIndexTimeout time.Duration
}

type group struct {
	id            uint64
	robinCount    int
	appliedIndex  uint64
	unreachableMu struct {
		sync.Mutex
		remotes map[uint64]struct{}
	}
	rawNodeMu struct {
		sync.RWMutex
		rawNode *raft.RawNode
	}
	notifies sync.Map

	cfg     groupConfig
	sm      StateMachine
	handler groupHandler
	storage *storage
}

func (g *group) Propose(ctx context.Context, pdata *ProposalData) (resp ProposalResponse, err error) {
	pdata.notifyID = g.handler.HandleNextID()
	raw, err := pdata.Marshal()
	if err != nil {
		return
	}
	// set propose data context with trace id when context is nil
	if pdata.Context == nil {
		if span := trace.SpanFromContext(ctx); span == nil {
			span, ctx = trace.StartSpanFromContext(ctx, "")
			pdata.Context = util.StringToBytes(span.TraceID())
		}
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), g.cfg.proposeTimeout)
	defer cancel()
	n := newNotify(timeoutCtx)
	g.addNotify(pdata.notifyID, n)

	err = g.handler.HandlePropose(ctx, g.id, proposalRequest{
		data: raw,
	})
	if err != nil {
		return
	}

	ret, err := n.Wait(ctx)
	if err != nil {
		return
	}
	if ret.err != nil {
		return resp, ret.err
	}

	n.Release()
	return ProposalResponse{Data: ret.reply}, nil
}

func (g *group) LeaderTransfer(ctx context.Context, nodeID uint64) error {
	stat, err := g.Stat()
	if err != nil {
		return err
	}
	nodeFound := false
	for _, pr := range stat.Peers {
		if pr.NodeID == nodeID {
			nodeFound = true
			break
		}
	}
	if !nodeFound {
		return fmt.Errorf("node[%d] not found in node list[%+v]", nodeID, stat.Peers)
	}

	(*internalGroupProcessor)(g).WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.TransferLeader(nodeID)
		return nil
	})
	g.handler.HandleSignalToWorker(ctx, g.id)
	return nil
}

func (g *group) ReadIndex(ctx context.Context) error {
	notifyID := g.handler.HandleNextID()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), g.cfg.readIndexTimeout)
	defer cancel()
	n := newNotify(timeoutCtx)
	g.addNotify(notifyID, n)

	(*internalGroupProcessor)(g).WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.ReadIndex(notifyIDToBytes(notifyID))
		return nil
	})
	span := trace.SpanFromContext(ctx)
	span.Debug("do signal to worker")
	g.handler.HandleSignalToWorker(ctx, g.id)

	ret, err := n.Wait(ctx)
	if err != nil {
		return err
	}
	if ret.err != nil {
		return ret.err
	}

	n.Release()
	return nil
}

func (g *group) Campaign(ctx context.Context) error {
	err := (*internalGroupProcessor)(g).WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		return rn.Campaign()
	})
	if err != nil {
		return err
	}
	span := trace.SpanFromContext(ctx)
	span.Debug("do signal to worker")
	g.handler.HandleSignalToWorker(ctx, g.id)
	return nil
}

func (g *group) Truncate(ctx context.Context, index uint64) error {
	return g.storage.Truncate(index)
}

func (g *group) MemberChange(ctx context.Context, m *Member) error {
	cs, err := memberToConfChange(m)
	if err != nil {
		return err
	}
	cs.ID = g.handler.HandleNextID()
	data, err := cs.Marshal()
	if err != nil {
		return err
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), g.cfg.proposeTimeout)
	defer cancel()
	n := newNotify(timeoutCtx)
	g.addNotify(cs.ID, n)

	if err = g.handler.HandlePropose(ctx, g.id, proposalRequest{
		entryType: raftpb.EntryConfChange,
		data:      data,
	}); err != nil {
		return err
	}

	ret, err := n.Wait(ctx)
	if err != nil {
		return err
	}
	if ret.err != nil {
		return ret.err
	}

	n.Release()
	return nil
}

func (g *group) Stat() (*Stat, error) {
	raftStatus := g.raftStatusLocked()
	peers := make([]Peer, 0, len(raftStatus.Progress))
	for id, pr := range raftStatus.Progress {
		var host string
		if m, ok := g.storage.GetMember(id); ok {
			host = m.Host
		}
		peer := Peer{
			NodeID:          id,
			Host:            host,
			Match:           pr.Match,
			Next:            pr.Next,
			RaftState:       pr.State.String(),
			Paused:          pr.IsPaused(),
			PendingSnapshot: pr.PendingSnapshot,
			RecentActive:    pr.RecentActive,
			IsLearner:       pr.IsLearner,
			InflightFull:    pr.Inflights.Full(),
			InflightCount:   int64(pr.Inflights.Count()),
		}
		peers = append(peers, peer)
	}
	return &Stat{
		ID:             g.id,
		NodeID:         raftStatus.ID,
		Term:           raftStatus.Term,
		Vote:           raftStatus.Vote,
		Commit:         raftStatus.Commit,
		Leader:         raftStatus.Lead,
		RaftState:      raftStatus.RaftState.String(),
		Applied:        g.storage.AppliedIndex(),
		RaftApplied:    raftStatus.Applied,
		LeadTransferee: raftStatus.LeadTransferee,
		Peers:          peers,
	}, nil
}

func (g *group) Close() error {
	g.storage.Close()
	return nil
}

func (g *group) Clear() error {
	return g.storage.Clear()
}

func (g *group) raftStatusLocked() raft.Status {
	g.rawNodeMu.RLock()
	status := g.rawNodeMu.rawNode.Status()
	g.rawNodeMu.RUnlock()

	return status
}

func (g *group) addNotify(notifyID uint64, n notify) {
	g.notifies.Store(notifyID, n)
}

func (g *group) doNotify(notifyID uint64, ret proposalResult) {
	n, ok := g.notifies.LoadAndDelete(notifyID)
	if !ok {
		return
	}
	n.(notify).Notify(ret)
}

type internalGroupProcessor group

func (g *internalGroupProcessor) ID() uint64 {
	return g.id
}

func (g *internalGroupProcessor) NodeID() uint64 {
	return g.cfg.nodeID
}

func (g *internalGroupProcessor) WithRaftRawNodeLocked(f func(rn *raft.RawNode) error) error {
	g.rawNodeMu.Lock()
	defer g.rawNodeMu.Unlock()

	return f(g.rawNodeMu.rawNode)
}

func (g *internalGroupProcessor) ProcessSendRaftMessage(ctx context.Context, messages []raftpb.Message) {
	span := trace.SpanFromContext(ctx)
	sentAppResp := false
	for i := range messages {
		msg := &messages[i]

		// filter repeated MsgAppResp to leader
		if msg.Type == raftpb.MsgAppResp {
			if sentAppResp {
				msg.To = 0
			} else {
				sentAppResp = true
			}
		}

		// add into async queue to process snapshot
		if msg.Type == raftpb.MsgSnap {
			if err := g.handler.HandleSnapshot(ctx, g.id, *msg); err != nil {
				span.Errorf("handle snapshot failed: %s", err)
				// delete snapshot and report failed when handle snapshot into queue failed
				g.storage.DeleteSnapshot(string(msg.Snapshot.Data))
				g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
					rn.ReportSnapshot(msg.To, raft.SnapshotFailure)
					return nil
				})
			}

			// redirect snapshot message's to into zero, don't send snapshot message
			msg.To = 0
		}

		if g.handler.HandleMaybeCoalesceHeartbeat(ctx, g.id, msg) {
			continue
		}

		if msg.To == 0 {
			continue
		}

		req := newRaftMessageRequest()
		*req = RaftMessageRequest{
			GroupID: g.id,
			To:      msg.To,
			From:    msg.From,
			Message: *msg,
		}

		// idx := atomic.AddUint64(&defaultConnectionClassRobinCount, 1) % uint64(len(defaultConnectionClassList))
		g.robinCount++
		idx := g.robinCount % len(defaultConnectionClassList)
		if err := g.handler.HandleSendRaftMessageRequest(ctx, req, defaultConnectionClassList[idx]); err != nil {
			// if err := g.handler.HandleSendRaftMessageRequest(ctx, req, defaultConnectionClass); err != nil {
			g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
				rn.ReportUnreachable(msg.To)
				return nil
			})
			span.Warnf("handle send raft message request failed: %s", err)
			continue
		}
	}
}

func (g *internalGroupProcessor) ProcessSendSnapshot(ctx context.Context, m raftpb.Message) {
	span := trace.SpanFromContext(ctx)
	id := string(m.Snapshot.Data)
	snapshot := g.storage.GetSnapshot(id)
	if snapshot == nil {
		span.Errorf("not found outgoingSnapshot(%s)", id)
		g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
			rn.ReportSnapshot(m.To, raft.SnapshotFailure)
			return nil
		})
		return
	}
	defer g.storage.DeleteSnapshot(id)

	req := &RaftMessageRequest{
		GroupID: g.id,
		From:    m.From,
		To:      m.To,
		Message: raftpb.Message{
			Type:     m.Type,
			To:       m.To,
			From:     m.From,
			Term:     (*group)(g).raftStatusLocked().Term,
			Snapshot: m.Snapshot,
		},
	}
	if err := g.handler.HandleSendRaftSnapshotRequest(ctx, snapshot, req); err != nil {
		span.Errorf("handle send raft outgoingSnapshot[%s] request failed: %s", id, errors.Detail(err))
		g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
			rn.ReportSnapshot(m.To, raft.SnapshotFailure)
			return nil
		})
		return
	}

	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.ReportSnapshot(m.To, raft.SnapshotFinish)
		return nil
	})
}

func (g *internalGroupProcessor) ProcessRaftMessageRequest(ctx context.Context, req *RaftMessageRequest) error {
	span := trace.SpanFromContext(ctx)

	if req.Message.Type == raftpb.MsgSnap {
		span.Fatalf("receive unexpected outgoingSnapshot request: %+v", req)
	}

	if req.To == 0 {
		return errors.New("to node id can't be 0")
	}

	// drop := maybeDropMsgApp(ctx, (*replicaMsgAppDropper)(r), &req.Message, req.RangeStartKey)
	return g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		return rn.Step(req.Message)
	})
}

func (g *internalGroupProcessor) ProcessRaftSnapshotRequest(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error {
	snapshot := newIncomingSnapshot(req.Header, g.storage, stream)
	if err := g.sm.ApplySnapshot(snapshot.Header(), snapshot); err != nil {
		return err
	}

	if err := g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		return rn.Step(req.Header.RaftMessageRequest.Message)
	}); err != nil {
		return err
	}
	// raise worker signal
	span := trace.SpanFromContext(ctx)
	span.Debug("do signal to worker")
	g.handler.HandleSignalToWorker(ctx, g.id)
	return nil
}

func (g *internalGroupProcessor) SaveHardStateAndEntries(ctx context.Context, hs raftpb.HardState, entries []raftpb.Entry) error {
	return g.storage.SaveHardStateAndEntries(hs, entries)
}

func (g *internalGroupProcessor) ApplyLeaderChange(nodeID uint64) error {
	return g.sm.LeaderChange(nodeID)
}

func (g *internalGroupProcessor) ApplySnapshot(ctx context.Context, snap raftpb.Snapshot) error {
	// save hard state
	if err := g.storage.SaveHardStateAndEntries(raftpb.HardState{
		Commit: snap.Metadata.Index,
		Term:   snap.Metadata.Term,
	}, nil); err != nil {
		return err
	}
	// set applied index
	g.storage.SetAppliedIndex(snap.Metadata.Index)

	// no need to truncate the old raft log, as the Truncate function will remove
	// all oldest raft log after state machine call the Truncate API
	/*if err := g.storage.Truncate(snap.Metadata.Index + 1); err != nil {
		return err
	}*/
	return nil
}

func (g *internalGroupProcessor) ApplyCommittedEntries(ctx context.Context, entries []raftpb.Entry) error {
	allProposalData := make([]ProposalData, 0, len(entries))
	latestIndex := uint64(0)

	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryConfChange:
			// apply the previous committed entries first before apply conf change
			if len(allProposalData) > 0 {
				rets, err := g.sm.Apply(ctx, allProposalData, latestIndex)
				if err != nil {
					return errors.Info(err, "apply to state machine failed")
				}
				for j, ret := range rets {
					(*group)(g).doNotify(allProposalData[j].notifyID, proposalResult{
						reply: ret,
						err:   nil,
					})
				}
				allProposalData = allProposalData[:0]
			}
			if err := g.applyConfChange(ctx, entries[i]); err != nil {
				return errors.Info(err, "apply conf change to state machine failed")
			}
		case raftpb.EntryNormal:
			// initial raft state may propose empty data after leader change
			// just ignore it and don't call apply to state machine
			if len(entries[i].Data) == 0 {
				continue
			}
			allProposalData = append(allProposalData, ProposalData{})
			proposalData := &allProposalData[len(allProposalData)-1]
			if err := proposalData.Unmarshal(entries[i].Data); err != nil {
				return errors.Info(err, "unmarshal proposal data failed")
			}
		}

		latestIndex = entries[i].Index
	}

	if len(allProposalData) > 0 {
		rets, err := g.sm.Apply(ctx, allProposalData, latestIndex)
		if err != nil {
			return errors.Info(err, "apply to state machine failed")
		}

		for i, ret := range rets {
			(*group)(g).doNotify(allProposalData[i].notifyID, proposalResult{
				reply: ret,
				err:   nil,
			})
		}
	}

	// always set storage's applied index into entries last one
	if len(entries) > 0 {
		g.storage.SetAppliedIndex(entries[len(entries)-1].Index)
	}

	return nil
}

func (g *internalGroupProcessor) ApplyReadIndex(ctx context.Context, readState raft.ReadState) {
	notifyID := bytesToNotifyID(readState.RequestCtx)
	(*group)(g).doNotify(notifyID, proposalResult{})
}

func (g *internalGroupProcessor) Tick() {
	g.unreachableMu.Lock()
	remotes := g.unreachableMu.remotes
	g.unreachableMu.remotes = nil
	g.unreachableMu.Unlock()

	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		for remote := range remotes {
			rn.ReportUnreachable(remote)
		}
		rn.Tick()
		return nil
	})
}

func (g *internalGroupProcessor) AddUnreachableRemoteReplica(remote uint64) {
	g.unreachableMu.Lock()
	if g.unreachableMu.remotes == nil {
		g.unreachableMu.remotes = make(map[uint64]struct{})
	}
	g.unreachableMu.remotes[remote] = struct{}{}
	g.unreachableMu.Unlock()
}

func (g *internalGroupProcessor) applyConfChange(ctx context.Context, entry raftpb.Entry) error {
	var (
		cc   raftpb.ConfChange
		span = trace.SpanFromContext(ctx)
	)
	if err := cc.Unmarshal(entry.Data); err != nil {
		span.Fatalf("unmarshal conf change failed: %s", err)
		return err
	}

	// apply conf change to raft state machine
	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.ApplyConfChange(cc)
		return nil
	})

	member := &Member{}
	if err := member.Unmarshal(cc.Context); err != nil {
		return err
	}
	if err := g.sm.ApplyMemberChange(member, entry.Index); err != nil {
		return err
	}
	g.storage.MemberChange(member)

	(*group)(g).doNotify(cc.ID, proposalResult{})
	return nil
}

func notifyIDToBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return b
}

func bytesToNotifyID(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func memberToConfChange(m *Member) (cs raftpb.ConfChange, err error) {
	raw, err := m.Marshal()
	if err != nil {
		return
	}

	cs = raftpb.ConfChange{
		NodeID:  m.NodeID,
		Context: raw,
	}
	switch m.Type {
	case MemberChangeType_AddMember:
		if m.Learner {
			cs.Type = raftpb.ConfChangeAddLearnerNode
			break
		}
		cs.Type = raftpb.ConfChangeAddNode
	case MemberChangeType_RemoveMember:
		cs.Type = raftpb.ConfChangeRemoveNode
	}

	return
}
