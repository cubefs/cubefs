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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	raftstoremock "github.com/cubefs/cubefs/metanode/mocktest/raftstore"
	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const fsmInodeQuotaID uint32 = 42

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

func TestFsmUpdateExtentKeyAfterMigrationRejectsLeaseExpireMismatch(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	const ino = 8801
	prepareInodeForFsmInodeTest(t, mp, ino)

	param := NewInode(ino, 0)
	param.LeaseExpireTime = 999
	param.Generation = 2
	resp := mp.fsmUpdateExtentKeyAfterMigration(param)
	require.EqualValues(t, proto.OpLeaseOccupiedByOthers, resp.Status)
}

func TestFsmUpdateExtentKeyAfterMigrationBumpsGeneration(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	const ino = 8802
	prepareInodeForFsmInodeTest(t, mp, ino)

	before, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, before)
	genBefore := before.Generation

	param := NewInode(ino, FileModeType)
	param.UpdateHybridCloudParams(before)
	param.HybridCloudExtentsMigration.storageClass = proto.StorageClass_Replica_HDD
	param.HybridCloudExtentsMigration.poolId = proto.DefaultHDDPoolId
	param.HybridCloudExtentsMigration.sortedEks = NewSortedExtents()
	param.HybridCloudExtentsMigration.expiredTime = time.Now().Add(time.Hour).Unix()

	resp := mp.fsmUpdateExtentKeyAfterMigration(param)
	require.EqualValues(t, proto.OpOk, resp.Status)

	after, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, after)
	require.EqualValues(t, genBefore+1, after.Generation,
		"successful migration must bump generation so clients refresh stale extent cache")
}

func applyUpdateInodeMetaForTest(t *testing.T, mp *metaPartition, req *UpdateInodeMetaRequest, index uint64) (resp interface{}, err error) {
	t.Helper()
	data, err := json.Marshal(req)
	require.NoError(t, err)
	item := NewMetaItem(0, nil, data)
	item.Op = opFSMUpdateInodeMeta
	cmd, err := item.MarshalJson()
	require.NoError(t, err)
	return mp.Apply(cmd, index)
}

func testFsmUpdateInodeMetaSuccess(t *testing.T, mp *metaPartition) {
	const ino = 20001
	prepareInodeForFsmInodeTest(t, mp, ino)
	before, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, before)
	genBefore := before.Generation

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status := mp.fsmUpdateInodeMeta(handle, &UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	})
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	after, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, after)
	require.EqualValues(t, genBefore+1, after.Generation)
}

func TestFsmUpdateInodeMetaSuccess(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmUpdateInodeMetaSuccess(t, mp)
}

func TestFsmUpdateInodeMetaSuccess_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmUpdateInodeMetaSuccess(t, mp)
}

func testFsmUpdateInodeMetaInodeNotExist(t *testing.T, mp *metaPartition) {
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status := mp.fsmUpdateInodeMeta(handle, &UpdateInodeMetaRequest{
		Inode:       99999,
		PartitionID: mp.config.PartitionId,
	})
	require.EqualValues(t, proto.OpNotExistErr, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func TestFsmUpdateInodeMetaInodeNotExist(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmUpdateInodeMetaInodeNotExist(t, mp)
}

func TestFsmUpdateInodeMetaInodeNotExist_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmUpdateInodeMetaInodeNotExist(t, mp)
}

func testFsmUpdateInodeMetaMarkedDelete(t *testing.T, mp *metaPartition) {
	const ino = 20002
	prepareInodeForFsmInodeTest(t, mp, ino)
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, inode)
	inode.Flag |= DeleteMarkFlag
	err = mp.inodeTree.Update(handle, inode)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status := mp.fsmUpdateInodeMeta(handle, &UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	})
	require.EqualValues(t, proto.OpNotExistErr, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func TestFsmUpdateInodeMetaMarkedDelete(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmUpdateInodeMetaMarkedDelete(t, mp)
}

func TestFsmUpdateInodeMetaApplyInodeNotExist(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	resp, err := applyUpdateInodeMetaForTest(t, mp, &UpdateInodeMetaRequest{
		Inode:       88888,
		PartitionID: mp.config.PartitionId,
	}, 1)
	require.NoError(t, err)
	msg, ok := resp.(*InodeResponse)
	require.True(t, ok)
	require.EqualValues(t, proto.OpNotExistErr, msg.Status)
}

func TestFsmUpdateInodeMetaApplySuccess(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	const ino = 20003
	prepareInodeForFsmInodeTest(t, mp, ino)

	resp, err := applyUpdateInodeMetaForTest(t, mp, &UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	}, 2)
	require.NoError(t, err)
	msg, ok := resp.(*InodeResponse)
	require.True(t, ok)
	require.EqualValues(t, proto.OpOk, msg.Status)
}

func TestFsmUpdateInodeMetaApplySuccess_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	const ino = 20004
	prepareInodeForFsmInodeTest(t, mp, ino)
	before, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, before)

	resp, err := applyUpdateInodeMetaForTest(t, mp, &UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	}, 2)
	require.NoError(t, err)
	msg, ok := resp.(*InodeResponse)
	require.True(t, ok)
	require.EqualValues(t, proto.OpOk, msg.Status)

	after, err := mp.inodeTree.Get(&Inode{Inode: ino})
	require.NoError(t, err)
	require.NotNil(t, after)
	require.EqualValues(t, before.Generation+1, after.Generation)
}

func TestFsmUpdateInodeMetaCopyGetError(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	const ino = 20010
	prepareInodeForFsmInodeTest(t, mp, ino)
	base := mp.inodeTree
	mp.inodeTree = &errInjectInodeTree{
		InodeTree:  base,
		copyGetErr: fmt.Errorf("copyget failed"),
	}
	defer func() { mp.inodeTree = base }()

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status := mp.fsmUpdateInodeMeta(handle, &UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	})
	require.EqualValues(t, proto.OpErr, status)
}

func TestFsmUpdateInodeMetaUpdateError(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	const ino = 20011
	prepareInodeForFsmInodeTest(t, mp, ino)
	base := mp.inodeTree
	mp.inodeTree = &errInjectInodeTree{
		InodeTree: base,
		updateErr: fmt.Errorf("update failed"),
	}
	defer func() { mp.inodeTree = base }()

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status := mp.fsmUpdateInodeMeta(handle, &UpdateInodeMetaRequest{
		Inode:       ino,
		PartitionID: mp.config.PartitionId,
	})
	require.EqualValues(t, proto.OpErr, status)
}

func prepareInodeWithExtentsForFsmInodeTest(t *testing.T, mp *metaPartition, ino uint64, size uint64) {
	t.Helper()
	prepareInodeForFsmInodeTest(t, mp, ino)
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	i, err := mp.inodeTree.CopyGet(NewInode(ino, 0))
	require.NoError(t, err)
	require.NotNil(t, i)
	se := NewSortedExtents()
	se.Append(proto.ExtentKey{FileOffset: 0, Size: uint32(size), ExtentId: 1, PartitionId: 1})
	i.HybridCloudExtents.sortedEks = se
	i.Size = size
	require.NoError(t, mp.inodeTree.Put(handle, i))
	require.NoError(t, mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false))
}

func inodeSizeFromSnap(t *testing.T, snap Snapshot, ino uint64) (size uint64, found bool) {
	t.Helper()
	err := snap.Range(InodeType, func(item interface{}) bool {
		inode := item.(*Inode)
		if inode.Inode == ino {
			size = inode.Size
			found = true
		}
		return true
	})
	require.NoError(t, err)
	return size, found
}

func testFsmExtentsTruncateCopyGetSnapshot(t *testing.T, mp *metaPartition) {
	const ino = 21001
	const beforeSize = uint64(2048)
	const afterSize = uint64(1024)

	if mp.multiVersionList == nil {
		mp.multiVersionList = &proto.VolVersionInfoList{
			TemporaryVerMap: make(map[uint64]*proto.VolVersionInfo),
		}
	}
	prepareInodeWithExtentsForFsmInodeTest(t, mp, ino, beforeSize)
	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
	defer snap.Close()

	snapSize, found := inodeSizeFromSnap(t, snap, ino)
	require.True(t, found)
	require.Equal(t, beforeSize, snapSize)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	req := NewInode(ino, 0)
	req.Size = afterSize
	req.ModifyTime = time.Now().Unix()
	resp, err := mp.fsmExtentsTruncate(handle, req)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	require.NoError(t, mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false))

	live, err := mp.inodeTree.Get(NewInode(ino, 0))
	require.NoError(t, err)
	require.NotNil(t, live)
	require.Equal(t, afterSize, live.Size)

	snapSizeAfter, _ := inodeSizeFromSnap(t, snap, ino)
	require.Equal(t, beforeSize, snapSizeAfter,
		"mem snapshot must not observe truncate after CopyGet")
}

func TestFsmExtentsTruncateCopyGetSnapshot(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmExtentsTruncateCopyGetSnapshot(t, mp)
}

func TestFsmExtentsTruncateCopyGetSnapshot_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmExtentsTruncateCopyGetSnapshot(t, mp)
}

func TestFsmExtentsTruncateCopyGetError(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	const ino = 21002
	prepareInodeForFsmInodeTest(t, mp, ino)
	base := mp.inodeTree
	mp.inodeTree = &errInjectInodeTree{
		InodeTree:  base,
		copyGetErr: fmt.Errorf("copyget failed"),
	}
	defer func() { mp.inodeTree = base }()

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	req := NewInode(ino, 0)
	req.Size = 512
	resp, _ := mp.fsmExtentsTruncate(handle, req)
	require.EqualValues(t, proto.OpErr, resp.Status)
}

func testFsmSetInodeQuotaBatchCopyGet(t *testing.T, mp *metaPartition) {
	const ino = 21010
	mp.mqMgr = NewQuotaManager(mp.config.VolName, mp.config.PartitionId)
	prepareInodeForFsmInodeTest(t, mp, ino)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	resp := mp.fsmSetInodeQuotaBatch(handle, &proto.BatchSetMetaserverQuotaReuqest{
		QuotaId: fsmInodeQuotaID,
		Inodes:  []uint64{ino},
		IsRoot:  true,
	})
	require.EqualValues(t, proto.OpOk, resp.InodeRes[ino])
	require.NoError(t, mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false))

	extend, err := mp.extendTree.Get(NewExtendWithQuota(ino))
	require.NoError(t, err)
	require.NotNil(t, extend)
	require.NotEmpty(t, extend.Quota)

	var quotaMap map[uint32]*proto.MetaQuotaInfo
	require.NoError(t, json.Unmarshal(extend.Quota, &quotaMap))
	require.NotNil(t, quotaMap[fsmInodeQuotaID])
}

func TestFsmSetInodeQuotaBatchCopyGet(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmSetInodeQuotaBatchCopyGet(t, mp)
}

func TestFsmSetInodeQuotaBatchCopyGet_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmSetInodeQuotaBatchCopyGet(t, mp)
}

func testFsmDeleteInodeQuotaBatchCopyGet(t *testing.T, mp *metaPartition) {
	const ino = 21011
	mp.mqMgr = NewQuotaManager(mp.config.VolName, mp.config.PartitionId)
	prepareInodeForFsmInodeTest(t, mp, ino)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	setResp := mp.fsmSetInodeQuotaBatch(handle, &proto.BatchSetMetaserverQuotaReuqest{
		QuotaId: fsmInodeQuotaID,
		Inodes:  []uint64{ino},
		IsRoot:  true,
	})
	require.EqualValues(t, proto.OpOk, setResp.InodeRes[ino])
	require.NoError(t, mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false))

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_ = mp.fsmDeleteInodeQuotaBatch(handle, &proto.BatchDeleteMetaserverQuotaReuqest{
		QuotaId: fsmInodeQuotaID,
		Inodes:  []uint64{ino},
	})
	require.NoError(t, mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false))

	extend, err := mp.extendTree.Get(NewExtendWithQuota(ino))
	require.NoError(t, err)
	if extend != nil {
		require.Nil(t, extend.Quota)
	}
}

func TestFsmDeleteInodeQuotaBatchCopyGet(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeMem)
	testFsmDeleteInodeQuotaBatchCopyGet(t, mp)
}

func TestFsmDeleteInodeQuotaBatchCopyGet_Rocksdb(t *testing.T) {
	mp := newMpForFsmInodeTest(t, proto.StoreModeRocksDb)
	testFsmDeleteInodeQuotaBatchCopyGet(t, mp)
}
