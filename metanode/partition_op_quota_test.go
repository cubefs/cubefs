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
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

const (
	PartitionIdForTest = 1
	VolNameForTest     = "test1"
	RocksdbQuoTestDir  = "/tmp/cfs/qt_test"
)

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

func NewMetaPartitionForQuotaTest(storeMode proto.StoreMode) *metaPartition {
	mpC := &MetaPartitionConfig{
		PartitionId: PartitionIdForTest,
		VolName:     VolNameForTest,
		StoreMode:   storeMode,
	}
	mpC.RocksDBDir = fmt.Sprintf("%v/%v_%v", RocksdbQuoTestDir, partitionId, time.Now().UnixMilli())
	partition := NewMetaPartition(mpC, nil).(*metaPartition)
	if storeMode == proto.StoreModeRocksDb {
		partition.rocksdbManager = NewPerDiskRocksdbManager(0, 0, 0, 0, 0)
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

func TestCleanRocksdbOpQuotaTestDir(t *testing.T) {
	os.RemoveAll(RocksdbQuoTestDir)
}
