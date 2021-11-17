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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util"
)

type snapshotStatus struct {
	respErr
	stopCh chan struct{}
}

func newSnapshotStatus() *snapshotStatus {
	f := &snapshotStatus{
		stopCh: make(chan struct{}),
	}
	f.init()
	return f
}

type snapshotRequest struct {
	respErr
	snapshotReader
	header *proto.Message
}

func newSnapshotRequest(m *proto.Message, r *util.BufferReader) *snapshotRequest {
	f := &snapshotRequest{
		header:         m,
		snapshotReader: snapshotReader{reader: r},
	}
	f.init()
	return f
}

func (r *snapshotRequest) response() error {
	return <-r.error()
}

type snapshotReader struct {
	reader *util.BufferReader
	err    error
}

func (r *snapshotReader) Next() ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}

	// read size header
	var buf []byte
	if buf, r.err = r.reader.ReadFull(4); r.err != nil {
		return nil, r.err
	}
	size := uint64(binary.BigEndian.Uint32(buf))
	if size == 0 {
		r.err = io.EOF
		return nil, r.err
	}

	// read data
	if buf, r.err = r.reader.ReadFull(int(size)); r.err != nil {
		return nil, r.err
	}

	return buf, nil
}

func (s *raft) addSnapping(nodeID uint64, rs *snapshotStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snap, ok := s.snapping[nodeID]; ok {
		close(snap.stopCh)
	}
	s.snapping[nodeID] = rs
}

func (s *raft) removeSnapping(nodeID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snap, ok := s.snapping[nodeID]; ok {
		close(snap.stopCh)
		delete(s.snapping, nodeID)
	}
}

func (s *raft) stopSnapping() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, snap := range s.snapping {
		close(snap.stopCh)
		delete(s.snapping, id)
	}
}

func (s *raft) sendSnapshot(m *proto.Message) {
	util.RunWorker(func() {
		defer func() {
			s.removeSnapping(m.To)
			m.Snapshot.Close()
			proto.ReturnMessage(m)
		}()

		// send snapshot
		rs := newSnapshotStatus()
		s.addSnapping(m.To, rs)
		s.config.transport.SendSnapshot(m, rs)
		select {
		case <-s.stopc:
			return
		case <-rs.stopCh:
			return
		case err := <-rs.error():
			nmsg := proto.GetMessage()
			nmsg.Type = proto.RespMsgSnapShot
			nmsg.ID = m.ID
			nmsg.From = m.To
			nmsg.Reject = (err != nil)
			s.recvc <- nmsg
		}
	}, func(err interface{}) {
		s.doStop()
		s.handlePanic(err)
	})
}

func (s *raft) handleSnapshot(req *snapshotRequest) {
	s.restoringSnapshot.Set(true)
	var err error
	defer func() {
		req.respond(err)
		s.resetTick()
		s.restoringSnapshot.Set(false)
		proto.ReturnMessage(req.header)
	}()

	// validate snapshot
	if req.header.Term < s.raftFsm.term {
		err = fmt.Errorf("raft %v [term: %d] ignored a snapshot message with lower term from %v [term: %d]", s.raftFsm.id, s.raftFsm.term, req.header.From, req.header.Term)
		return
	}
	if req.header.Term > s.raftFsm.term || s.raftFsm.state != stateFollower {
		s.raftFsm.becomeFollower(req.header.Term, req.header.From)
		s.maybeChange(true)
	}
	if !s.raftFsm.checkSnapshot(req.header.SnapshotMeta) {
		if logger.IsEnableWarn() {
			logger.Warn("raft %v [commit: %d] ignored snapshot [index: %d, term: %d].", s.raftFsm.id, s.raftFsm.raftLog.committed, req.header.SnapshotMeta.Index, req.header.SnapshotMeta.Term)
		}
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgAppend
		nmsg.To = req.header.From
		nmsg.Index = s.raftFsm.raftLog.committed
		nmsg.Commit = s.raftFsm.raftLog.committed
		s.raftFsm.send(nmsg)
		return
	}

	// restore snapshot
	s.raftConfig.Storage.ApplySnapshot(proto.SnapshotMeta{})
	if err = s.raftConfig.StateMachine.ApplySnapshot(req.header.SnapshotMeta.Peers, req); err != nil {
		return
	}
	if err = s.raftConfig.Storage.ApplySnapshot(req.header.SnapshotMeta); err != nil {
		return
	}
	s.raftFsm.restore(req.header.SnapshotMeta)
	s.peerState.replace(req.header.SnapshotMeta.Peers)
	s.curApplied.Set(req.header.SnapshotMeta.Index)

	// send snapshot response message
	if logger.IsEnableDebug() {
		logger.Warn("raft %v [commit: %d] restored snapshot [index: %d, term: %d]",
			s.raftFsm.id, s.raftFsm.raftLog.committed, req.header.SnapshotMeta.Index, req.header.SnapshotMeta.Term)
	}
	nmsg := proto.GetMessage()
	nmsg.Type = proto.RespMsgAppend
	nmsg.To = req.header.From
	nmsg.Index = s.raftFsm.raftLog.lastIndex()
	nmsg.Commit = s.raftFsm.raftLog.committed
	s.raftFsm.send(nmsg)
}
