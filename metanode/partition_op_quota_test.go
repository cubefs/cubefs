// Copyright 2018 The CubeFS Authors.
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
	"testing"
	"time"

	raftstoremock "github.com/cubefs/cubefs/metanode/mocktest/raftstore"
	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	PartitionIdForTest = 1
	VolNameForTest     = "test1"
)

func testBatchSetInodeQuota(t *testing.T, storeMode proto.StoreMode) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForQuotaTest(mockCtrl, storeMode)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)

	inode := NewInode(2, 0)
	inode.Size = 100
	mp.inodeTree.Put(handle, inode)
	inode = NewInode(3, 0)
	inode.Size = 200
	mp.inodeTree.Put(handle, inode)

	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	var quotaId1 uint32 = 1
	var inodes []uint64
	inodes = append(inodes, 2, 3)
	req := &proto.BatchSetMetaserverQuotaReuqest{
		PartitionId: PartitionIdForTest,
		Inodes:      inodes,
		QuotaId:     quotaId1,
		IsRoot:      false,
	}
	resp := &proto.BatchSetMetaserverQuotaResponse{}
	mp.batchSetInodeQuota(req, resp)
	size, files := mp.mqMgr.getUsedInfoForTest(quotaId1)
	require.Equal(t, int64(300), size)
	require.Equal(t, int64(2), files)

	var quotaId2 uint32 = 2
	req = &proto.BatchSetMetaserverQuotaReuqest{
		PartitionId: PartitionIdForTest,
		Inodes:      inodes,
		QuotaId:     quotaId2,
		IsRoot:      false,
	}
	resp = &proto.BatchSetMetaserverQuotaResponse{}
	mp.batchSetInodeQuota(req, resp)
	size, files = mp.mqMgr.getUsedInfoForTest(quotaId2)
	require.Equal(t, int64(300), size)
	require.Equal(t, int64(2), files)

	req2 := &proto.BatchDeleteMetaserverQuotaReuqest{
		PartitionId: PartitionIdForTest,
		Inodes:      inodes,
		QuotaId:     quotaId1,
	}
	resp2 := &proto.BatchDeleteMetaserverQuotaResponse{}
	mp.batchDeleteInodeQuota(req2, resp2)
	size, files = mp.mqMgr.getUsedInfoForTest(quotaId1)
	require.Equal(t, int64(0), size)
	require.Equal(t, int64(0), files)
	size, files = mp.mqMgr.getUsedInfoForTest(quotaId2)
	require.Equal(t, int64(300), size)
	require.Equal(t, int64(2), files)
}

func TestBatchSetInodeQuota(t *testing.T) {
	testBatchSetInodeQuota(t, proto.StoreModeMem)
}

func TestBatchSetInodeQuota_Rocksdb(t *testing.T) {
	testBatchSetInodeQuota(t, proto.StoreModeRocksDb)
}

func testQuotaHbInfo(t *testing.T, storeMode proto.StoreMode) {
	partition := NewMetaPartitionForQuotaTest(storeMode)
	var hbInfos []*proto.QuotaHeartBeatInfo
	var quotaId uint32 = 1
	var quotaId2 uint32 = 2
	hbInfo := &proto.QuotaHeartBeatInfo{
		VolName:     VolNameForTest,
		QuotaId:     quotaId,
		LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: true, LimitedBytes: true},
		Enable:      true,
	}
	hbInfos = append(hbInfos, hbInfo)
	partition.mqMgr.setQuotaHbInfo(hbInfos)
	require.Equal(t, true, partition.mqMgr.EnableQuota())
	require.Equal(t, proto.OpNoSpaceErr, partition.mqMgr.IsOverQuota(true, true, quotaId))

	hbInfo = &proto.QuotaHeartBeatInfo{
		VolName:     VolNameForTest,
		QuotaId:     quotaId2,
		LimitedInfo: proto.QuotaLimitedInfo{LimitedFiles: true, LimitedBytes: false},
		Enable:      false,
	}
	hbInfos = append(hbInfos, hbInfo)
	partition.mqMgr.setQuotaHbInfo(hbInfos)
	require.Equal(t, false, partition.mqMgr.EnableQuota())
	require.Equal(t, uint8(0), partition.mqMgr.IsOverQuota(true, true, quotaId2))
}

func TestQuotaHbInfo(t *testing.T) {
	testQuotaHbInfo(t, proto.StoreModeMem)
}

func TestQuotaHbInfo_Rocksdb(t *testing.T) {
	testQuotaHbInfo(t, proto.StoreModeRocksDb)
}

func testGetQuotaReportInfos(t *testing.T, storeMode proto.StoreMode) {
	partition := NewMetaPartitionForQuotaTest(storeMode)
	var quotaId uint32 = 1
	// var infos []*proto.QuotaReportInfo
	partition.mqMgr.updateUsedInfo(100, 1, quotaId)
	partition.mqMgr.updateUsedInfo(200, 2, quotaId)
	partition.mqMgr.limitedMap.Store(quotaId, proto.QuotaLimitedInfo{LimitedFiles: false, LimitedBytes: false})
	info := &proto.QuotaReportInfo{
		QuotaId:  quotaId,
		UsedInfo: proto.QuotaUsedInfo{UsedFiles: 3, UsedBytes: 300},
	}

	infos := partition.mqMgr.getQuotaReportInfos()
	require.Equal(t, info, infos[0])
}

func TestGetQuotaReportInfos(t *testing.T) {
	testGetQuotaReportInfos(t, proto.StoreModeMem)
}

func TestGetQuotaReportInfos_Rocksdb(t *testing.T) {
	testGetQuotaReportInfos(t, proto.StoreModeRocksDb)
}

func NewMetaPartitionForQuotaTest(storeMode proto.StoreMode) *metaPartition {
	mpC := &MetaPartitionConfig{
		PartitionId: PartitionIdForTest,
		VolName:     VolNameForTest,
		StoreMode:   storeMode,
	}
	mpC.RocksDBDir = fmt.Sprintf("%v/%v_%v", "/tmp/cfs/qt_test", partitionId, time.Now().UnixMilli())
	partition := NewMetaPartition(mpC, nil).(*metaPartition)
	if storeMode == proto.StoreModeRocksDb {
		partition.rocksdbManager = NewRocksdbManager()
		err := partition.rocksdbManager.Register(mpC.RocksDBDir)
		if err != nil {
			panic(err)
		}
	}
	err := partition.initObjects(true)
	if err != nil {
		panic(err)
	}
	partition.uniqChecker.keepTime = 1
	partition.uniqChecker.keepOps = 0
	partition.mqMgr = NewQuotaManager(VolNameForTest, 1)
	return partition
}

func mockPartitionRaftForQuotaTest(ctrl *gomock.Controller, storeMode proto.StoreMode) *metaPartition {
	partition := NewMetaPartitionForQuotaTest(storeMode)
	raft := raftstoremock.NewMockPartition(ctrl)
	idx := uint64(0)
	raft.EXPECT().Submit(gomock.Any()).DoAndReturn(func(cmd []byte) (resp interface{}, err error) {
		idx++
		return partition.Apply(cmd, idx)
	}).AnyTimes()
	raft.EXPECT().LeaderTerm().Return(uint64(1), uint64(1)).AnyTimes()
	partition.raftPartition = raft

	return partition
}
