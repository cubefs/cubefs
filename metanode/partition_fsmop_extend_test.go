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

func getMpConfigForFsmExtendTest(storeMode proto.StoreMode) (config *MetaPartitionConfig) {
	config = &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	if config.StoreMode == proto.StoreModeRocksDb {
		config.RocksDBDir = fmt.Sprintf("%v/%v_%v", "/tmp/cfs/fsm_extend_test", partitionId, time.Now().UnixMilli())
		os.RemoveAll(config.RocksDBDir)
	}
	return
}

func newMpForFsmExtendTest(t *testing.T, storeMode proto.StoreMode) (mp *metaPartition) {
	var _ interface{} = t
	config := getMpConfigForFsmExtendTest(storeMode)
	mp = newPartition(config, newManager())
	return
}

func mockPartitionRaftForFsmExtendTest(t *testing.T, ctrl *gomock.Controller, storeMode proto.StoreMode) *metaPartition {
	partition := newMpForFsmExtendTest(t, storeMode)
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

func prepareInoForFsmExtendTest(t *testing.T, mp *metaPartition, ino uint64) {
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInode(ino, FileModeType)
	err = mp.inodeTree.Put(handle, inode)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func checkExtendForFsmExtendTest(t *testing.T, mp *metaPartition, ino uint64, key string, expectedValue []byte) {
	extend, err := mp.extendTree.Get(ino)
	require.NoError(t, err)
	require.NotNil(t, extend)
	value, ok := extend.Get([]byte(key))
	require.True(t, ok)
	require.ElementsMatch(t, value, expectedValue)
}

func checkExtendNotExistsForFsmExtendTest(t *testing.T, mp *metaPartition, ino uint64, key string) {
	extend, err := mp.extendTree.Get(ino)
	require.NoError(t, err)
	if extend == nil {
		return
	}
	_, ok := extend.Get([]byte(key))
	require.False(t, ok)
}

func testFsmSetXAttr(t *testing.T, mp *metaPartition) {
	const ino = 1000
	prepareInoForFsmExtendTest(t, mp, ino)

	handle, err := mp.extendTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	extend := NewExtend(ino)
	extend.dataMap["Key"] = []byte("Value")
	_, err = mp.fsmSetXAttr(handle, extend)
	require.NoError(t, err)
	err = mp.extendTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	extend = NewExtend(ino)
	checkExtendForFsmExtendTest(t, mp, ino, "Key", []byte("Value"))

	extend.dataMap["Key2"] = []byte("Value2")
	handle, err = mp.extendTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = mp.fsmSetXAttr(handle, extend)
	require.NoError(t, err)
	err = mp.extendTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	checkExtendForFsmExtendTest(t, mp, ino, "Key2", []byte("Value2"))
	checkExtendForFsmExtendTest(t, mp, ino, "Key", []byte("Value"))
}

func TestFsmSetXAttr(t *testing.T) {
	mp := newMpForFsmExtendTest(t, proto.StoreModeMem)
	testFsmSetXAttr(t, mp)
}

func TestFsmSetXAttr_Rocksdb(t *testing.T) {
	mp := newMpForFsmExtendTest(t, proto.StoreModeRocksDb)
	testFsmSetXAttr(t, mp)
}

func testFsmRemoveXAttr(t *testing.T, mp *metaPartition) {
	const ino = 1000
	prepareInoForFsmExtendTest(t, mp, ino)

	handle, err := mp.extendTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	extend := NewExtend(ino)
	extend.dataMap["Key"] = []byte("Value")
	_, err = mp.fsmSetXAttr(handle, extend)
	require.NoError(t, err)
	err = mp.extendTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	extend = NewExtend(ino)
	checkExtendForFsmExtendTest(t, mp, ino, "Key", []byte("Value"))

	extend.dataMap["Key"] = []byte{}
	handle, err = mp.extendTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = mp.fsmRemoveXAttr(handle, extend)
	require.NoError(t, err)
	err = mp.extendTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	checkExtendNotExistsForFsmExtendTest(t, mp, ino, "Key")
}

func TestFsmRemoveXAttr(t *testing.T) {
	mp := newMpForFsmExtendTest(t, proto.StoreModeMem)
	testFsmRemoveXAttr(t, mp)
}

func TestFsmRemoveXAttr_Rocksdb(t *testing.T) {
	mp := newMpForFsmExtendTest(t, proto.StoreModeRocksDb)
	testFsmRemoveXAttr(t, mp)
}
