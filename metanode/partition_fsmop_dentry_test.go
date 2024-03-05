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

func getMpConfigForFsmDentryTest(storeMode proto.StoreMode) (config *MetaPartitionConfig) {
	config = &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	if config.StoreMode == proto.StoreModeRocksDb {
		config.RocksDBDir = fmt.Sprintf("%v/%v_%v", "/tmp/cfs/fsm_dentry_test", partitionId, time.Now().UnixMilli())
		os.RemoveAll(config.RocksDBDir)
	}
	return
}

func newMpForFsmDentryTest(t *testing.T, storeMode proto.StoreMode) (mp *metaPartition) {
	var _ interface{} = t
	config := getMpConfigForFsmDentryTest(storeMode)
	mp = newPartition(config, newManager())
	return
}

func mockPartitionRaftForFsmDentryTest(t *testing.T, ctrl *gomock.Controller, storeMode proto.StoreMode) *metaPartition {
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

func prepareInodeForFsmDentryTest(t *testing.T, mp *metaPartition, ino uint64, mode uint32) {
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInode(ino, mode)
	status, err := mp.fsmCreateInode(handle, inode)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func getDentryForFsmDentryTest(t *testing.T, mp *metaPartition, parent uint64, name string) (den *Dentry) {
	den, err := mp.dentryTree.Get(parent, name)
	require.NoError(t, err)
	return
}

func getInodeForFsmDentryTest(t *testing.T, mp *metaPartition, ino uint64) (inode *Inode) {
	inode, err := mp.inodeTree.Get(ino)
	require.NoError(t, err)
	return
}

func testFsmCreateDentry(t *testing.T, mp *metaPartition) {
	const dirIno = 1000
	const ino = 1001
	prepareInodeForFsmDentryTest(t, mp, dirIno, DirModeType)
	prepareInodeForFsmDentryTest(t, mp, ino, FileModeType)

	handle, err := mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	den := &Dentry{
		ParentId: dirIno,
		Name:     "test",
		Inode:    ino,
	}
	status, err := mp.fsmCreateDentry(handle, den, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	den = getDentryForFsmDentryTest(t, mp, dirIno, "test")
	require.EqualValues(t, ino, den.Inode)
	inode := getInodeForFsmDentryTest(t, mp, dirIno)
	require.EqualValues(t, 3, inode.NLink)
}

func TestFsmCreateDentry(t *testing.T) {
	mp := newMpForFsmDentryTest(t, proto.StoreModeMem)
	testFsmCreateDentry(t, mp)
}

func TestFsmCreateDentry_Rocksdb(t *testing.T) {
	mp := newMpForFsmDentryTest(t, proto.StoreModeRocksDb)
	testFsmCreateDentry(t, mp)
}

func testFsmDeleteDentry(t *testing.T, mp *metaPartition) {
	const dirIno = 1000
	const ino = 1001
	prepareInodeForFsmDentryTest(t, mp, dirIno, DirModeType)
	prepareInodeForFsmDentryTest(t, mp, ino, FileModeType)

	handle, err := mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	den := &Dentry{
		ParentId: dirIno,
		Name:     "test",
		Inode:    ino,
	}
	status, err := mp.fsmCreateDentry(handle, den, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	den = getDentryForFsmDentryTest(t, mp, dirIno, "test")
	require.EqualValues(t, ino, den.Inode)

	handle, err = mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	den = &Dentry{
		ParentId: dirIno,
		Name:     "test",
	}
	resp, err := mp.fsmDeleteDentry(handle, den, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	inode := getInodeForFsmDentryTest(t, mp, dirIno)
	require.EqualValues(t, 2, inode.NLink)
}

func TestFsmDeleteDentry(t *testing.T) {
	mp := newMpForFsmDentryTest(t, proto.StoreModeMem)
	testFsmDeleteDentry(t, mp)
}

func TestFsmDeleteDentry_Rocksdb(t *testing.T) {
	mp := newMpForFsmDentryTest(t, proto.StoreModeRocksDb)
	testFsmDeleteDentry(t, mp)
}

func testFsmUpdateDentry(t *testing.T, mp *metaPartition) {
	const dirIno = 1000
	const ino = 1001
	const otherIno = 1002
	prepareInodeForFsmDentryTest(t, mp, dirIno, DirModeType)
	prepareInodeForFsmDentryTest(t, mp, ino, FileModeType)
	prepareInodeForFsmDentryTest(t, mp, otherIno, FileModeType)

	handle, err := mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	den := &Dentry{
		ParentId: dirIno,
		Name:     "test",
		Inode:    ino,
	}
	status, err := mp.fsmCreateDentry(handle, den, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	handle, err = mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	den = &Dentry{
		ParentId: dirIno,
		Name:     "test",
		Inode:    otherIno,
	}
	resp, err := mp.fsmUpdateDentry(handle, den)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, resp.Status)
	err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	den = getDentryForFsmDentryTest(t, mp, dirIno, "test")
	require.NotNil(t, den)
	require.EqualValues(t, den.Inode, otherIno)
}

func TestFsmUpdateDentry(t *testing.T) {
	mp := newMpForFsmDentryTest(t, proto.StoreModeMem)
	testFsmUpdateDentry(t, mp)
}

func TestFsmUpdateDentry_Rocksdb(t *testing.T) {
	mp := newMpForFsmDentryTest(t, proto.StoreModeRocksDb)
	testFsmUpdateDentry(t, mp)
}
