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
	"math"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewStorage_RaftStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := NewMockStorage(ctrl)
	mockIter := NewMockIterator(ctrl)
	mockHardState := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 1,
	}
	rawHardState, err := mockHardState.Marshal()
	require.NoError(t, err)
	mockValueGetter := NewMockValGetter(ctrl)
	mockValueGetter.EXPECT().Value().Return(rawHardState)
	mockValueGetter.EXPECT().Close().Return()

	mockStorage.EXPECT().Get(gomock.Any()).Return(mockValueGetter, nil)
	mockSM := NewMockStateMachine(ctrl)

	cfg := storageConfig{
		id:              1,
		maxSnapshotNum:  10,
		snapshotTimeout: 3600,
		members: []Member{
			{NodeID: 1, Host: "127.0.0.1", Type: MemberChangeType_AddMember, Learner: false},
			{NodeID: 2, Host: "127.0.0.2", Type: MemberChangeType_AddMember, Learner: true},
		},
		raw: mockStorage,
		sm:  mockSM,
	}
	s, err := newStorage(cfg)
	require.NoError(t, err)
	require.NotNil(t, s)

	// test InitialState
	{
		hs, cs, err := s.InitialState()
		require.NoError(t, err)
		require.Equal(t, mockHardState, hs)
		require.Len(t, cs.Voters, 1)
		require.Len(t, cs.Learners, 1)
		require.Equal(t, cfg.members[0].NodeID, cs.Voters[0])
		require.Equal(t, cfg.members[1].NodeID, cs.Learners[0])
	}

	// test Entries
	{
		entry := raftpb.Entry{Term: 1, Index: 1}
		rawEntry, err := entry.Marshal()
		require.NoError(t, err)
		mockValueGetter := NewMockValGetter(ctrl)
		mockValueGetter.EXPECT().Value().Return(rawEntry)
		mockValueGetter.EXPECT().Close().Return()
		mockKeyGetter := NewMockKeyGetter(ctrl)
		mockKeyGetter.EXPECT().Key().Return(encodeIndexLogKey(cfg.id, entry.Index)).AnyTimes()
		// mockKeyGetter.EXPECT().Close().Return()
		mockIter.EXPECT().SeekTo(gomock.Any()).Return()
		mockIter.EXPECT().ReadNext().Times(1).Return(mockKeyGetter, mockValueGetter, nil)
		mockIter.EXPECT().ReadNext().Times(1).Return(nil, nil, nil)
		mockIter.EXPECT().Close().Return()

		mockStorage.EXPECT().Iter(gomock.Any()).Return(mockIter)
		entris, err := s.Entries(1, 100, 100)
		require.NoError(t, err)
		require.Len(t, entris, 1)
		require.Equal(t, entris[0].Term, entry.Term)
		require.Equal(t, entris[0].Index, entry.Index)
	}

	// test FirstIndex
	{
		mockIter := NewMockIterator(ctrl)
		entry := raftpb.Entry{Term: 1, Index: 1}
		rawEntry, err := entry.Marshal()
		require.NoError(t, err)
		mockValueGetter := NewMockValGetter(ctrl)
		mockValueGetter.EXPECT().Value().Return(rawEntry)
		mockValueGetter.EXPECT().Close().Return()

		mockIter.EXPECT().Close().Return()
		mockIter.EXPECT().ReadNext().Times(1).Return(nil, mockValueGetter, nil)

		mockStorage.EXPECT().Iter(gomock.Any()).Return(mockIter)

		index, err := s.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, entry.Index, index)
		index, err = s.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, entry.Index, index)
	}

	// test Term
	{
		entry := &raftpb.Entry{
			Term:  1,
			Index: 2,
		}
		rawEntry, err := entry.Marshal()
		require.NoError(t, err)
		mockValueGetter := NewMockValGetter(ctrl)
		mockValueGetter.EXPECT().Value().Return(rawEntry)

		key := encodeIndexLogKey(cfg.id, entry.Index)
		mockStorage.EXPECT().Get(key).Return(mockValueGetter, nil)
		term, err := s.Term(entry.Index)
		require.NoError(t, err)
		require.Equal(t, entry.Term, term)
	}

	// test lastIndex
	{
		entry := raftpb.Entry{Term: 1, Index: 1}
		rawEntry, err := entry.Marshal()
		require.NoError(t, err)

		mockIter := NewMockIterator(ctrl)
		mockValueGetter := NewMockValGetter(ctrl)
		mockValueGetter.EXPECT().Value().Return(rawEntry)
		mockValueGetter.EXPECT().Close().Return()
		mockKeyGetter := NewMockKeyGetter(ctrl)
		mockKeyGetter.EXPECT().Key().Return(encodeIndexLogKey(s.id, entry.Index))
		mockKeyGetter.EXPECT().Close().Return()

		mockIter.EXPECT().Close().Return()
		mockIter.EXPECT().ReadPrev().Times(1).Return(mockKeyGetter, mockValueGetter, nil)

		key := encodeIndexLogKey(cfg.id, math.MaxUint64)
		mockIter.EXPECT().SeekForPrev(key).Times(1).Return(nil)

		mockStorage.EXPECT().Iter(nil).Return(mockIter)

		index, err := s.LastIndex()
		require.NoError(t, err)
		require.Equal(t, entry.Index, index)
		index, err = s.LastIndex()
		require.NoError(t, err)
		require.Equal(t, entry.Index, index)
	}

	// test Snapshot
	{
		entry := &raftpb.Entry{
			Term:  1,
			Index: 2,
		}
		s.SetAppliedIndex(entry.Index)

		mockSnap := NewMockSnapshot(ctrl)
		mockSnap.EXPECT().Index().Return(entry.Index)
		mockSnap.EXPECT().Close().AnyTimes().Return(nil)

		rawEntry, err := entry.Marshal()
		require.NoError(t, err)
		mockValueGetter := NewMockValGetter(ctrl)
		mockValueGetter.EXPECT().Value().Return(rawEntry)
		mockStorage.EXPECT().Get(gomock.Any()).Return(mockValueGetter, nil)

		mockSM.EXPECT().Snapshot().Return(mockSnap)
		snap, err := s.Snapshot()
		require.NoError(t, err)
		require.NotNil(t, snap)
		require.Equal(t, entry.Index, snap.Metadata.Index)
		require.Equal(t, entry.Term, snap.Metadata.Term)
		require.Equal(t, s.membersMu.confState, snap.Metadata.ConfState)

		outgoingSnap := s.GetSnapshot(string(snap.Data))
		require.NotNil(t, outgoingSnap)
		require.Equal(t, string(snap.Data), outgoingSnap.id)

		// mockSnap.EXPECT().Close()
		s.DeleteSnapshot(string(snap.Data))
	}
}

func TestStorage_SaveHardStateAndEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s := initStorage(t, ctrl)

	hs := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 1,
	}
	rawHs, _ := hs.Marshal()
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: nil},
		{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: nil},
	}
	rawEntries := make([][]byte, len(entries))
	for i := range entries {
		rawEntries[i], _ = entries[i].Marshal()
	}

	mockBatch := NewMockBatch(ctrl)
	mockBatch.EXPECT().Put(encodeHardStateKey(s.id), rawHs).Return()
	for i := range entries {
		mockBatch.EXPECT().Put(encodeIndexLogKey(s.id, entries[i].Index), rawEntries[i]).Return()
	}
	mockBatch.EXPECT().Close().Return()

	mockStorage := s.rawStg.(*MockStorage)
	mockStorage.EXPECT().NewBatch().Return(mockBatch)
	mockStorage.EXPECT().Write(gomock.Any()).Return(nil)

	err := s.SaveHardStateAndEntries(hs, entries)
	require.NoError(t, err)
}

func TestStorage_Truncate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s := initStorage(t, ctrl)

	s.SetAppliedIndex(1002)
	index := uint64(1001)

	mockBatch := NewMockBatch(ctrl)
	mockBatch.EXPECT().DeleteRange(encodeIndexLogKey(s.id, 0), encodeIndexLogKey(s.id, index)).Return()
	mockBatch.EXPECT().Close().Return()

	mockStorage := s.rawStg.(*MockStorage)
	mockStorage.EXPECT().NewBatch().Return(mockBatch)
	mockStorage.EXPECT().Write(gomock.Any()).Return(nil)

	err := s.Truncate(index)
	require.NoError(t, err)
}

func TestStorage_MemberChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s := initStorage(t, ctrl)

	s.MemberChange(&Member{
		NodeID: 1,
		Host:   "127.0.0.1",
		Type:   MemberChangeType_AddMember,
	})

	s.MemberChange(&Member{
		NodeID: 1,
		Host:   "127.0.0.1",
		Type:   MemberChangeType_RemoveMember,
	})
}

func TestStorage_DecodeHardState(t *testing.T) {
	key := []byte{103, 0, 0, 0, 0, 0, 255, 255, 255, 104}
	id, infix := decodeHardStateKey(key)
	t.Log("id: ", id, "infix: ", string(infix))

	value := []byte{8, 1, 16, 3, 24, 5}
	hs := &raftpb.HardState{}
	err := hs.Unmarshal(value)
	t.Log("hs: ", hs, err)
}

func initStorage(t *testing.T, ctrl *gomock.Controller) *storage {
	mockStorage := NewMockStorage(ctrl)
	mockHardState := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 1,
	}
	rawHardState, err := mockHardState.Marshal()
	require.NoError(t, err)
	mockValueGetter := NewMockValGetter(ctrl)
	mockValueGetter.EXPECT().Value().Return(rawHardState)
	mockValueGetter.EXPECT().Close().Return()

	mockStorage.EXPECT().Get(gomock.Any()).Return(mockValueGetter, nil)
	mockSM := NewMockStateMachine(ctrl)

	cfg := storageConfig{
		id:              1,
		maxSnapshotNum:  10,
		snapshotTimeout: 3600,
		members: []Member{
			{NodeID: 1, Host: "127.0.0.1", Type: MemberChangeType_AddMember, Learner: false},
			{NodeID: 2, Host: "127.0.0.2", Type: MemberChangeType_AddMember, Learner: true},
		},
		raw: mockStorage,
		sm:  mockSM,
	}
	s, err := newStorage(cfg)
	require.NoError(t, err)
	require.NotNil(t, s)

	return s
}
