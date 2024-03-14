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

func getMpConfigForFsmMultipartTest(storeMode proto.StoreMode) (config *MetaPartitionConfig) {
	config = &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	if config.StoreMode == proto.StoreModeRocksDb {
		config.RocksDBDir = fmt.Sprintf("%v/%v_%v", "/tmp/cfs/fsm_multipart_test", partitionId, time.Now().UnixMilli())
		os.RemoveAll(config.RocksDBDir)
	}
	return
}

func newMpForFsmMultipartTest(t *testing.T, storeMode proto.StoreMode) (mp *metaPartition) {
	var _ interface{} = t
	config := getMpConfigForFsmMultipartTest(storeMode)
	mp = newPartition(config, newManager())
	return
}

func mockPartitionRaftForFsmMultipartTest(t *testing.T, ctrl *gomock.Controller, storeMode proto.StoreMode) *metaPartition {
	partition := newMpForFsmMultipartTest(t, storeMode)
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

func getMultipartForFsmMultipartTest(t *testing.T, mp *metaPartition, key, id string) (multipart *Multipart) {
	multipart, err := mp.multipartTree.Get(key, id)
	require.NoError(t, err)
	return
}

func testFsmCreateMutlipart(t *testing.T, mp *metaPartition) {
	handle, err := mp.multipartTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	multipart := &Multipart{
		key: "test",
		id:  "id1",
	}
	status, err := mp.fsmCreateMultipart(handle, multipart)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.multipartTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	gotMultipart := getMultipartForFsmMultipartTest(t, mp, "test", "id1")
	require.EqualValues(t, multipart.key, gotMultipart.key)
	require.EqualValues(t, multipart.id, gotMultipart.id)
}

func TestFsmCreateMultipart(t *testing.T) {
	mp := newMpForFsmMultipartTest(t, proto.StoreModeMem)
	testFsmCreateMutlipart(t, mp)
}

func TestFsmCreateMultipart_Rocksdb(t *testing.T) {
	mp := newMpForFsmMultipartTest(t, proto.StoreModeRocksDb)
	testFsmCreateMutlipart(t, mp)
}

func testFsmRemoveMultipart(t *testing.T, mp *metaPartition) {
	handle, err := mp.multipartTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	multipart := &Multipart{
		key: "test",
		id:  "id1",
	}
	status, err := mp.fsmCreateMultipart(handle, multipart)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.multipartTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	gotMultipart := getMultipartForFsmMultipartTest(t, mp, "test", "id1")
	require.EqualValues(t, multipart.key, gotMultipart.key)
	require.EqualValues(t, multipart.id, gotMultipart.id)

	handle, err = mp.multipartTree.CreateBatchWriteHandle()
	require.NoError(t, err)

	status, err = mp.fsmRemoveMultipart(handle, multipart)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	err = mp.multipartTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	gotMultipart = getMultipartForFsmMultipartTest(t, mp, "test", "id1")
	require.Nil(t, gotMultipart)

	handle, err = mp.multipartTree.CreateBatchWriteHandle()
	require.NoError(t, err)

	status, err = mp.fsmRemoveMultipart(handle, multipart)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpNotExistErr, status)

	err = mp.multipartTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func TestFsmRemoveMultipart(t *testing.T) {
	mp := newMpForFsmMultipartTest(t, proto.StoreModeMem)
	testFsmRemoveMultipart(t, mp)
}

func TestFsmRemoveMultipart_Rocksdb(t *testing.T) {
	mp := newMpForFsmMultipartTest(t, proto.StoreModeRocksDb)
	testFsmRemoveMultipart(t, mp)
}
