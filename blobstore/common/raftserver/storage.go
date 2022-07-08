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
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/cubefs/blobstore/common/raftserver/wal"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type raftStorage struct {
	nodeId    uint64
	walMu     sync.RWMutex
	wal       *wal.Wal
	shotter   *snapshotter
	sm        StateMachine
	cs        pb.ConfState
	memberMu  sync.RWMutex
	members   map[uint64]Member
	applied   uint64
	snapIndex uint64
}

func NewRaftStorage(walDir string, sync bool, nodeId uint64, sm StateMachine, shotter *snapshotter) (*raftStorage, error) {
	rs := &raftStorage{
		nodeId:  nodeId,
		shotter: shotter,
		members: make(map[uint64]Member),
		sm:      sm,
	}

	wal, err := wal.OpenWal(walDir, sync)
	if err != nil {
		return nil, err
	}
	rs.wal = wal

	return rs, nil
}

func (s *raftStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	hs := s.wal.InitialState()
	return hs, s.cs, nil
}

func (s *raftStorage) Term(index uint64) (uint64, error) {
	s.walMu.RLock()
	defer s.walMu.RUnlock()
	return s.wal.Term(index)
}

func (s *raftStorage) LastIndex() (uint64, error) {
	s.walMu.RLock()
	defer s.walMu.RUnlock()
	return s.wal.LastIndex(), nil
}

func (s *raftStorage) FirstIndex() (uint64, error) {
	s.walMu.RLock()
	defer s.walMu.RUnlock()
	return s.wal.FirstIndex(), nil
}

func (s *raftStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	s.walMu.RLock()
	defer s.walMu.RUnlock()
	return s.wal.Entries(lo, hi, maxSize)
}

func (s *raftStorage) Snapshot() (pb.Snapshot, error) {
	var members []*Member
	s.memberMu.RLock()
	cs := s.cs
	for _, m := range s.members {
		member := &Member{}
		*member = m
		members = append(members, member)
	}
	s.memberMu.RUnlock()

	st, err := s.sm.Snapshot()
	if err != nil {
		return pb.Snapshot{}, err
	}
	snapIndex := st.Index()
	snapTerm, err := s.Term(snapIndex)
	if err != nil {
		st.Close()
		return pb.Snapshot{}, err
	}
	if snapIndex > s.Applied() {
		st.Close()
		return pb.Snapshot{}, fmt.Errorf("snapIndex(%d) greater than applied(%d)", snapIndex, s.Applied())
	}
	name := st.Name()
	snap := &snapshot{
		st: st,
		meta: SnapshotMeta{
			Name:     name,
			Index:    snapIndex,
			Term:     snapTerm,
			Mbs:      members,
			Voters:   cs.Voters,
			Learners: cs.Learners,
		},
	}
	runtime.SetFinalizer(snap, func(snap *snapshot) {
		snap.Close()
	})
	if err = s.shotter.Set(snap); err != nil {
		log.Errorf("set snapshot(%s) error: %v", name, err)
		return pb.Snapshot{}, err
	}
	log.Infof("generator a snapshot(%s)", name)
	return pb.Snapshot{
		Data: []byte(name),
		Metadata: pb.SnapshotMetadata{
			ConfState: cs,
			Index:     snapIndex,
			Term:      snapTerm,
		},
	}, nil
}

func (s *raftStorage) SaveEntries(entries []pb.Entry) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	return s.wal.SaveEntries(entries)
}

func (s *raftStorage) SaveHardState(hs pb.HardState) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	return s.wal.SaveHardState(hs)
}

func (s *raftStorage) SetApplied(applied uint64) {
	atomic.StoreUint64(&s.applied, applied)
}

func (s *raftStorage) SetSnapIndex(index uint64) {
	atomic.StoreUint64(&s.snapIndex, index)
}

func (s *raftStorage) Applied() uint64 {
	return atomic.LoadUint64(&s.applied)
}

func (s *raftStorage) Truncate(index uint64) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	return s.wal.Truncate(index)
}

func (s *raftStorage) ApplySnapshot(st wal.Snapshot) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	if err := s.wal.ApplySnapshot(st); err != nil {
		return err
	}
	s.SetApplied(st.Index)
	return nil
}

func (s *raftStorage) confState() pb.ConfState {
	var cs pb.ConfState

	for _, m := range s.members {
		if m.Learner {
			cs.Learners = append(cs.Learners, m.NodeID)
		} else {
			cs.Voters = append(cs.Voters, m.NodeID)
		}
	}

	return cs
}

func (s *raftStorage) AddMembers(m Member) {
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	s.members[m.NodeID] = m
	s.cs = s.confState()
}

func (s *raftStorage) RemoveMember(id uint64) {
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	delete(s.members, id)
	s.cs = s.confState()
}

func (s *raftStorage) SetMembers(members []*Member) {
	mbs := make(map[uint64]Member)
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	for i := 0; i < len(members); i++ {
		mbs[members[i].NodeID] = *members[i]
	}
	s.members = mbs
	s.cs = s.confState()
}

func (s *raftStorage) GetMember(id uint64) (Member, bool) {
	s.memberMu.RLock()
	defer s.memberMu.RUnlock()
	m, hit := s.members[id]
	return m, hit
}

func (s *raftStorage) Close() {
	s.wal.Close()
}
