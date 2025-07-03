// Copyright 2024 The CubeFS Authors.
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

package metanode

import (
	"fmt"
	"os"
	"testing"
	"time"

	raftstoremock "github.com/cubefs/cubefs/metanode/mocktest/raftstore"
	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const RocksdbInodeTestDir = "/tmp/cfs/fsm_inode_test"

func getMpConfigForFsmInodeTest(storeMode proto.StoreMode) (config *MetaPartitionConfig) {
	config = &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	if config.StoreMode == proto.StoreModeRocksDb {
		config.RocksDBDir = fmt.Sprintf("%v/%v_%v", RocksdbInodeTestDir, partitionId, time.Now().UnixMilli())
	}
	return
}

func newMpForFsmInodeTest(t *testing.T, storeMode proto.StoreMode) (mp *metaPartition) {
	var _ interface{} = t
	config := getMpConfigForFsmInodeTest(storeMode)
	mp = newPartition(config, newManager())
	return
}

func mockPartitionRaftForFsmInodeTest(t *testing.T, ctrl *gomock.Controller, storeMode proto.StoreMode) *metaPartition {
	partition := newMpForFsmInodeTest(t, storeMode)
	raft := raftstoremock.NewMockPartition(ctrl)
	idx := uint64(0)
	raft.EXPECT().Submit(gomock.Any()).DoAndReturn(func(cmd []byte) (resp interface{}, err error) {
		idx++
		return partition.Apply(cmd, idx)
	}).AnyTimes()

	raft.EXPECT().IsRaftLeader().DoAndReturn(func() bool {
		return true
	}).AnyTimes()

	raft.EXPECT().LeaderTerm().Return(uint64(1), uint64(1)).AnyTimes()
	partition.raftPartition = raft
	return partition
}

func prepareInodeForFsmInodeTest(t *testing.T, mp *metaPartition, ino uint64) {
	inode := NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status := mp.fsmCreateInode(inode)
	require.EqualValues(t, proto.OpOk, status)
}

func prepareDirInodeForFsmInodeTest(t *testing.T, mp *metaPartition, ino uint64) {
	inode := NewInode(ino, DirModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status := mp.fsmCreateInode(inode)
	require.EqualValues(t, proto.OpOk, status)
}

func checkInodeLinkForFsmInodeTest(t *testing.T, mp *metaPartition, ino uint64, link uint64) {
	inode, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	if inode == nil {
		require.EqualValues(t, 0, link)
		return
	}
	require.EqualValues(t, link, inode.NLink)
}

func testFsmCreateInode(t *testing.T, mp *metaPartition) {
	const ino = 1000
	prepareInodeForFsmInodeTest(t, mp, ino)

	inode, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, inode)
	require.EqualValues(t, ino, inode.Inode)
}

func TestFsmCreateInode(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmCreateInode(t, mp)
}

func TestFsmCreateInode_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmCreateInode(t, mp)
}

func testFsmLinkInode(t *testing.T, mp *metaPartition) {
	const ino = 1000
	prepareInodeForFsmInodeTest(t, mp, ino)

	inode := NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp := mp.fsmCreateLinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)

	checkInodeLinkForFsmInodeTest(t, mp, ino, 2)

	const dirIno = 1001
	prepareDirInodeForFsmInodeTest(t, mp, dirIno)
	inode = NewInode(dirIno, DirModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp = mp.fsmCreateLinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)
	checkInodeLinkForFsmInodeTest(t, mp, dirIno, 3)
}

func TestFsmLinkInode(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmLinkInode(t, mp)
}

func TestFsmLinkInode_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmLinkInode(t, mp)
}

func testFsmUnlinkInode(t *testing.T, mp *metaPartition) {
	const ino = 1000
	const dirIno = 1001
	prepareInodeForFsmInodeTest(t, mp, ino)
	prepareDirInodeForFsmInodeTest(t, mp, dirIno)

	inode := NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp := mp.fsmCreateLinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)
	inode = NewInode(dirIno, DirModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp = mp.fsmCreateLinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)

	checkInodeLinkForFsmInodeTest(t, mp, ino, 2)
	checkInodeLinkForFsmInodeTest(t, mp, dirIno, 3)

	inode = NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp = mp.fsmUnlinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)
	inode = NewInode(dirIno, DirModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp = mp.fsmUnlinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)

	checkInodeLinkForFsmInodeTest(t, mp, ino, 1)
	checkInodeLinkForFsmInodeTest(t, mp, dirIno, 2)

	inode = NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp = mp.fsmUnlinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)
	inode = NewInode(dirIno, DirModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp = mp.fsmUnlinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)

	// NOTE: unlink empty dir, will delete it
	checkInodeLinkForFsmInodeTest(t, mp, ino, 0)
	checkInodeLinkForFsmInodeTest(t, mp, dirIno, 0)
}

func TestFsmUnlinkInode(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmUnlinkInode(t, mp)
}

func TestFsmUnlinkInode_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmUnlinkInode(t, mp)
}

func testFsmAppendInode(t *testing.T, mp *metaPartition) {
	const ino = 1000
	prepareInodeForFsmInodeTest(t, mp, ino)

	inode := NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status := mp.fsmAppendExtentsWithCheck(inode, false)
	require.EqualValues(t, proto.OpOk, status)

	var err error
	inode, err = mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)

	// NOTE: random write to hole
	status = mp.fsmAppendExtentsWithCheck(inode, false)
	require.EqualValues(t, proto.OpOk, status)

	_, err = mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
}

func TestFsmAppendInode(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmAppendInode(t, mp)
}

func TestFsmAppendInode_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmAppendInode(t, mp)
}

func testFsmAppendInodeRandomWrite(t *testing.T, mp *metaPartition) {
	const ino = 1000
	prepareInodeForFsmInodeTest(t, mp, ino)

	inode := NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status := mp.fsmAppendExtentsWithCheck(inode, false)
	require.EqualValues(t, proto.OpOk, status)

	var err error
	_, err = mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)

	inode = NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status = mp.fsmAppendExtentsWithCheck(inode, false)
	require.EqualValues(t, proto.OpOk, status)

	_, err = mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)

	// NOTE: random write to first extent
	inode = NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status = mp.fsmAppendExtentsWithCheck(inode, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)

	_, err = mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
}

func TestFsmAppendInodeRandomWrite(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmAppendInodeRandomWrite(t, mp)
}

func TestFsmAppendInodeRandomWrite_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmAppendInodeRandomWrite(t, mp)
}

func testFsmUnlinkFileInode(t *testing.T, mp *metaPartition) {
	const ino = 1000
	prepareInodeForFsmInodeTest(t, mp, ino)

	inode := NewInode(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status := mp.fsmAppendExtentsWithCheck(inode, false)
	require.EqualValues(t, proto.OpOk, status)

	resp := mp.fsmUnlinkInode(inode, 0)
	require.EqualValues(t, proto.OpOk, resp.Status)
}

func TestFsmUnlinkFileInode(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmUnlinkFileInode(t, mp)
}

func TestFsmUnlinkFileInode_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmUnlinkFileInode(t, mp)
}

func TestCleanRocksdbInodeTestDir(t *testing.T) {
	os.RemoveAll(RocksdbInodeTestDir)
}
