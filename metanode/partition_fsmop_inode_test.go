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
	mp.uniqChecker = newUniqChecker()
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
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInodeTest(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	inode.PoolId = proto.DefaultSSDPoolId
	status, err := mp.fsmCreateInode(handle, inode)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func prepareDirInodeForFsmInodeTest(t *testing.T, mp *metaPartition, ino uint64) {
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInodeTest(ino, DirModeType)
	status, err := mp.fsmCreateInode(handle, inode)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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

	inode := NewInodeTest(ino, FileModeType)
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	resp, err := mp.fsmCreateLinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	checkInodeLinkForFsmInodeTest(t, mp, ino, 2)

	const dirIno = 1001
	prepareDirInodeForFsmInodeTest(t, mp, dirIno)
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode = NewInodeTest(dirIno, DirModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp, err = mp.fsmCreateLinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

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

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInodeTest(ino, FileModeType)
	resp, err := mp.fsmCreateLinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	inode = NewInodeTest(dirIno, DirModeType)
	resp, err = mp.fsmCreateLinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	checkInodeLinkForFsmInodeTest(t, mp, ino, 2)
	checkInodeLinkForFsmInodeTest(t, mp, dirIno, 3)

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode = NewInodeTest(ino, FileModeType)
	resp, err = mp.fsmUnlinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	inode = NewInodeTest(dirIno, DirModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp, err = mp.fsmUnlinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	checkInodeLinkForFsmInodeTest(t, mp, ino, 1)
	checkInodeLinkForFsmInodeTest(t, mp, dirIno, 2)

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode = NewInodeTest(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp, err = mp.fsmUnlinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	inode = NewInodeTest(dirIno, DirModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	resp, err = mp.fsmUnlinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

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

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInodeTest(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status, err := mp.fsmAppendExtentsWithCheck(handle, inode, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	inode, err = mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)

	// NOTE: random write to hole
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = mp.fsmAppendExtentsWithCheck(handle, inode, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

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

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInodeTest(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status, err := mp.fsmAppendExtentsWithCheck(handle, inode, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	_, err = mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode = NewInodeTest(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status, err = mp.fsmAppendExtentsWithCheck(handle, inode, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	_, err = mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)

	// NOTE: random write to first extent
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode = NewInodeTest(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status, err = mp.fsmAppendExtentsWithCheck(handle, inode, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

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

func testFsmLinkInodeUniqIDIdempotent(t *testing.T, mp *metaPartition) {
	const ino = 2000
	prepareInodeForFsmInodeTest(t, mp, ino)
	mp.applyID = 100

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInodeTest(ino, FileModeType)
	resp, err := mp.fsmCreateLinkInode(handle, inode, 111)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	checkInodeLinkForFsmInodeTest(t, mp, ino, 2)

	mp.applyID = 101
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode = NewInodeTest(ino, FileModeType)
	resp, err = mp.fsmCreateLinkInode(handle, inode, 111)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	checkInodeLinkForFsmInodeTest(t, mp, ino, 2)
}

func TestFsmLinkInodeUniqIDIdempotent(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmLinkInodeUniqIDIdempotent(t, mp)
}

func TestFsmLinkInodeUniqIDIdempotent_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmLinkInodeUniqIDIdempotent(t, mp)
}

func testFsmUnlinkInodeUniqIDIdempotent(t *testing.T, mp *metaPartition) {
	const ino = 3000
	prepareInodeForFsmInodeTest(t, mp, ino)
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInodeTest(ino, FileModeType)
	resp, err := mp.fsmCreateLinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	checkInodeLinkForFsmInodeTest(t, mp, ino, 2)

	mp.applyID = 200

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode = NewInodeTest(ino, FileModeType)
	resp, err = mp.fsmUnlinkInode(handle, inode, 222)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	checkInodeLinkForFsmInodeTest(t, mp, ino, 1)

	mp.applyID = 201
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode = NewInodeTest(ino, FileModeType)
	resp, err = mp.fsmUnlinkInode(handle, inode, 222)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	checkInodeLinkForFsmInodeTest(t, mp, ino, 1)
}

func TestFsmUnlinkInodeUniqIDIdempotent(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmUnlinkInodeUniqIDIdempotent(t, mp)
}

func TestFsmUnlinkInodeUniqIDIdempotent_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmUnlinkInodeUniqIDIdempotent(t, mp)
}

func testFsmUnlinkFileInode(t *testing.T, mp *metaPartition) {
	const ino = 1000
	prepareInodeForFsmInodeTest(t, mp, ino)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInodeTest(ino, FileModeType)
	inode.StorageClass = proto.StorageClass_Replica_SSD
	status, err := mp.fsmAppendExtentsWithCheck(handle, inode, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	resp, err := mp.fsmUnlinkInode(handle, inode, 0)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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

func TestFsmUpdateExtentKeyAfterMigrationRejectsGenerationMismatch(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	const ino = 8801
	prepareInodeForFsmInodeTest(t, mp, ino)

	param := NewInode(ino, 0)
	param.LeaseExpireTime = 0
	param.Generation = 2
	resp := mp.fsmUpdateExtentKeyAfterMigration(param)
	require.EqualValues(t, proto.OpLeaseOccupiedByOthers, resp.Status)
}
