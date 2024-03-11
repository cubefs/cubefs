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

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func getMpConfigForFsmTest(storeMode proto.StoreMode) (config *MetaPartitionConfig) {
	config = &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	if config.StoreMode == proto.StoreModeRocksDb {
		config.RocksDBDir = fmt.Sprintf("%v/%v_%v", "/tmp/cfs/fsm_test", partitionId, time.Now().UnixMilli())
		os.RemoveAll(config.RocksDBDir)
	}
	config.RootDir = fmt.Sprintf("%v/%v_%v", "/tmp/cfs/fsm_test_root", partitionId, time.Now().UnixMilli())
	os.RemoveAll(config.RootDir)
	os.MkdirAll(config.RootDir, 0o755)
	return
}

func newMpForFsmTest(t *testing.T, storeMode proto.StoreMode) (mp *metaPartition) {
	var _ interface{} = t
	config := getMpConfigForFsmTest(storeMode)
	mp = newPartition(config, newManager())
	mp.manager.metaNode = &MetaNode{
		raftSyncSnapFormatVersion: SnapFormatVersion_1,
	}
	mp.uniqChecker = newUniqChecker()
	mp.multiVersionList = &proto.VolVersionInfoList{}
	// mp.storeChan = make(chan *storeMsg, 10000)
	return
}

func prepareDataForMpFsmTest(t *testing.T, mp *metaPartition) {
	prepareDataForMpTest(t, mp)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.inodeTree.SetApplyID(10)
	mp.applyID = 10
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, true)
	require.NoError(t, err)
}

func checkEmptyMpForMpFsmTest(t *testing.T, mp *metaPartition) {
	cnt, err := mp.GetInodeRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

	cnt, err = mp.GetDentryRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

	cnt, err = mp.GetExtendRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

	cnt, err = mp.GetMultipartRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

	cnt, err = mp.GetTxRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

	cnt, err = mp.GetTxRbInodeRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

	cnt, err = mp.GetTxRbDentryRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

	cnt, err = mp.GetDeletedExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)

	cnt, err = mp.GetDeletedObjExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)
}

func testApplySnapshot(t *testing.T, storeMode proto.StoreMode) {
	leaderMp := newMpForFsmTest(t, storeMode)
	followerMp := newMpForFsmTest(t, storeMode)
	prepareDataForMpFsmTest(t, leaderMp)
	checkTreeCntForMpTest(t, leaderMp)

	iter, err := leaderMp.Snapshot()
	require.NoError(t, err)

	require.EqualValues(t, 10, iter.ApplyIndex())

	go func() {
		sm := <-followerMp.storeChan
		err = followerMp.store(sm)
		require.NoError(t, err)
	}()
	err = followerMp.ApplySnapshot(nil, iter)
	require.NoError(t, err)

	iter.Close()

	require.EqualValues(t, 10, followerMp.getApplyID())

	checkTreeCntForMpTest(t, followerMp)

	err = leaderMp.Clear()
	require.NoError(t, err)

	handle, err := leaderMp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	leaderMp.inodeTree.SetApplyID(20)
	leaderMp.applyID = 20
	err = leaderMp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, true)
	require.NoError(t, err)

	checkEmptyMpForMpFsmTest(t, leaderMp)

	iter, err = leaderMp.Snapshot()
	require.NoError(t, err)

	require.EqualValues(t, 20, iter.ApplyIndex())

	go func() {
		sm := <-followerMp.storeChan
		err = followerMp.store(sm)
		require.NoError(t, err)
	}()
	err = followerMp.ApplySnapshot(nil, iter)
	require.NoError(t, err)

	iter.Close()

	require.EqualValues(t, 20, followerMp.getApplyID())

	checkEmptyMpForMpFsmTest(t, followerMp)
}

func TestApplySnapshot(t *testing.T) {
	testApplySnapshot(t, proto.StoreModeMem)
}

func TestApplySnapshot_Rocksdb(t *testing.T) {
	testApplySnapshot(t, proto.StoreModeRocksDb)
}
