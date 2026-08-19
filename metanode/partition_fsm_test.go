// Copyright 2026 The CubeFS Authors.
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

package metanode

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

type sliceSnapshotIterator struct {
	items [][]byte
	index int
}

func (iter *sliceSnapshotIterator) Next() ([]byte, error) {
	if iter.index >= len(iter.items) {
		return nil, io.EOF
	}
	item := iter.items[iter.index]
	iter.index++
	return item, nil
}

func marshalSnapshotItem(t *testing.T, item *MetaItem) []byte {
	t.Helper()
	data, err := item.MarshalBinary()
	require.NoError(t, err)
	return data
}

func TestApplySnapshotRebuildsFreeLists(t *testing.T) {
	manager := &metadataManager{
		metaNode: &MetaNode{raftSyncSnapFormatVersion: SnapFormatVersion_1},
	}
	mp := newMetaPartition(1, manager)
	mp.multiVersionList = &proto.VolVersionInfoList{}
	mp.storedApplyId = 1
	mp.extReset = make(chan struct{}, 1)

	// Entries from the state replaced by the snapshot must not survive.
	mp.freeList.Push(999)
	mp.freeHybridList.Push(998)

	deleted := NewInode(101, 0)
	deleted.SetDeleteMark()
	migration := NewInode(102, 0)
	migration.SetDeleteMigrationExtentKeyImmediately()
	live := NewInode(103, 0)

	formatVersion := make([]byte, 8)
	binary.BigEndian.PutUint32(formatVersion, SnapFormatVersion_1)
	applyID := make([]byte, 8)
	binary.BigEndian.PutUint64(applyID, 1)

	iter := &sliceSnapshotIterator{items: [][]byte{
		marshalSnapshotItem(t, NewMetaItem(opFSMSnapFormatVersion, nil, formatVersion)),
		marshalSnapshotItem(t, NewMetaItem(opFSMApplyId, nil, applyID)),
		marshalSnapshotItem(t, NewMetaItem(opFSMCreateInode, deleted.MarshalKey(), deleted.MarshalValue())),
		marshalSnapshotItem(t, NewMetaItem(opFSMCreateInode, migration.MarshalKey(), migration.MarshalValue())),
		marshalSnapshotItem(t, NewMetaItem(opFSMCreateInode, live.MarshalKey(), live.MarshalValue())),
	}}

	require.NoError(t, mp.ApplySnapshot(nil, iter))
	require.NotNil(t, mp.inodeTree.Get(NewInode(live.Inode, 0)))
	require.Equal(t, 1, mp.freeList.Len())
	require.Equal(t, deleted.Inode, mp.freeList.Pop())
	require.Equal(t, 1, mp.freeHybridList.Len())
	require.Equal(t, migration.Inode, mp.freeHybridList.Pop())
}

func TestApplySnapshotKeepsFreeListsOnError(t *testing.T) {
	manager := &metadataManager{
		metaNode: &MetaNode{raftSyncSnapFormatVersion: SnapFormatVersion_1},
	}
	mp := newMetaPartition(1, manager)
	mp.freeList.Push(999)
	mp.freeHybridList.Push(998)

	deleted := NewInode(101, 0)
	deleted.SetDeleteMark()
	formatVersion := make([]byte, 8)
	binary.BigEndian.PutUint32(formatVersion, SnapFormatVersion_1)

	iter := &sliceSnapshotIterator{items: [][]byte{
		marshalSnapshotItem(t, NewMetaItem(opFSMSnapFormatVersion, nil, formatVersion)),
		marshalSnapshotItem(t, NewMetaItem(opFSMCreateInode, deleted.MarshalKey(), deleted.MarshalValue())),
		[]byte("invalid snapshot item"),
	}}

	require.Error(t, mp.ApplySnapshot(nil, iter))
	require.Equal(t, 1, mp.freeList.Len())
	require.Equal(t, uint64(999), mp.freeList.Pop())
	require.Equal(t, 1, mp.freeHybridList.Len())
	require.Equal(t, uint64(998), mp.freeHybridList.Pop())
}
