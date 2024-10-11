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

package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/cluster"
	"github.com/cubefs/cubefs/blobstore/clustermgr/mock"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var testConfig = Config{
	FlushIntervalS:               100,
	ShardConcurrentMapNum:        32,
	SpaceConcurrentMapNum:        32,
	ApplyConcurrency:             10,
	InitShardNum:                 10,
	CheckInitShardIntervalS:      10,
	RouteItemTruncateIntervalNum: 10000,
	CodeMode:                     codemode.Replica3,
	IDC:                          []string{"z0", "z1", "z2"},
}

func TestCatalogMgr_Loop(t *testing.T) {
	conf := testConfig
	conf.CheckInitShardIntervalS = 1
	conf.InitShardNum = 1
	mockCatalogMgr, clean := initMockCatalogMgr(t, conf)
	defer clean()
	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(true)
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(uint64(31), uint64(31), nil)

	mockDiskMgr := cluster.NewMockShardNodeManagerAPI(ctr)
	mockDiskMgr.EXPECT().AllocShards(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, policy cluster.AllocShardsPolicy) ([]proto.DiskID, []proto.Suid, error) {
		diskids := make([]proto.DiskID, len(policy.Suids))
		for i := range diskids {
			diskids[i] = 9999
		}
		return diskids, policy.Suids, nil
	})
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockCatalogMgr.scopeMgr = mockScopeMgr
	mockCatalogMgr.diskMgr = mockDiskMgr
	mockCatalogMgr.raftServer = mockRaftServer

	mockCatalogMgr.loop()
	time.Sleep(2 * time.Second)
}

func initMockCatalogMgr(t testing.TB, conf Config) (*CatalogMgr, func()) {
	dir := path.Join(os.TempDir(), fmt.Sprintf("catalogmgr-%d-%010d", time.Now().Unix(), rand.Intn(100000000)))
	catalogDBPPath := path.Join(dir, "sharddb")
	normalDBPath := path.Join(dir, "normaldb")
	succ := false
	defer func() {
		if !succ {
			os.RemoveAll(dir)
		}
	}()

	err := generateShard(catalogDBPPath, normalDBPath)
	require.NoError(t, err)

	catalogDB, err := catalogdb.Open(catalogDBPPath)
	require.NoError(t, err)

	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockScopeMgr := mock.NewMockScopeMgrAPI(ctr)
	mockDiskMgr := cluster.NewMockShardNodeManagerAPI(ctr)
	mockSharNodeAPI := cluster.NewMockShardNodeAPI(ctr)
	mockKvMgr := mock.NewMockKvMgrAPI(ctr)

	mockDiskMgr.EXPECT().Stat(gomock.Any(), proto.DiskTypeNVMeSSD).AnyTimes().Return(&clustermgr.SpaceStatInfo{TotalDisk: 35})
	mockDiskMgr.EXPECT().GetDiskInfo(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(mockGetDiskInfo)
	mockDiskMgr.EXPECT().IsDiskWritable(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	mockDiskMgr.EXPECT().AllocShards(gomock.Any(), gomock.Any()).AnyTimes().Return([]proto.DiskID{1}, proto.DiskSetID(0), nil)
	mockScopeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(uint64(31), uint64(31), nil)
	mockSharNodeAPI.EXPECT().GetShardUintInfo(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(clustermgr.ShardUnitInfo{DiskID: 31}, nil)
	mockKvMgr.EXPECT().Get(gomock.Any()).AnyTimes().Return([]byte("1"), nil)
	mockKvMgr.EXPECT().Set(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	mockCatalogMgr, err := NewCatalogMgr(conf, mockDiskMgr, mockScopeMgr, mockKvMgr, catalogDB)
	require.NoError(t, err)
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(false)
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, data []byte) interface{} {
		proposeInfo := base.DecodeProposeInfo(data)
		if proposeInfo.OperType == OperTypeAllocShardUnit {
			args := &allocShardUnitCtx{}
			err := json.Unmarshal(proposeInfo.Data, args)
			require.NoError(t, err)
			err = mockCatalogMgr.applyAllocShardUnit(ctx, args)
			require.NoError(t, err)
		}
		return nil
	})
	mockCatalogMgr.SetRaftServer(mockRaftServer)
	mockCatalogMgr.shardNodeClient = mockSharNodeAPI

	succ = true
	return mockCatalogMgr, func() {
		mockCatalogMgr.Close()
		os.RemoveAll(dir)
	}
}

func mockGetDiskInfo(_ context.Context, id proto.DiskID) (*clustermgr.ShardNodeDiskInfo, error) {
	return &clustermgr.ShardNodeDiskInfo{
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{DiskID: id},
		DiskInfo: clustermgr.DiskInfo{
			Idc:  "z0",
			Host: "127.0.0.1",
		},
	}, nil
}

func generateShard(catalogDBPath, normalDBPath string) error {
	var (
		unitCount = 3
		shards    []*catalogdb.ShardInfoRecord
		units     []*catalogdb.ShardUnitInfoRecord
		routes    []*catalogdb.RouteInfoRecord
		ranges    = sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 2, 10)
	)
	catalogDB, err := catalogdb.Open(catalogDBPath)
	if err != nil {
		return err
	}
	defer catalogDB.Close()
	normalDB, err := normaldb.OpenNormalDB(normalDBPath)
	if err != nil {
		return err
	}
	defer normalDB.Close()

	catalogTable, err := catalogdb.OpenCatalogTable(catalogDB)
	if err != nil {
		return err
	}

	for i := 1; i <= 10; i++ {
		suidPrefixes := make([]proto.SuidPrefix, unitCount)
		for j := 0; j < unitCount; j++ {
			suidPrefixes[j] = proto.EncodeSuidPrefix(proto.ShardID(i), uint8(j))
			units = append(units, &catalogdb.ShardUnitInfoRecord{
				SuidPrefix: suidPrefixes[j],
				Epoch:      1,
				NextEpoch:  2,
				DiskID:     proto.DiskID(j + 1),
				Learner:    false,
			})
		}
		shard := &catalogdb.ShardInfoRecord{
			ShardID:      proto.ShardID(i),
			SuidPrefixes: suidPrefixes,
			LeaderDiskID: proto.DiskID(i),
			Range:        *ranges[i-1],
			RouteVersion: proto.RouteVersion(i),
		}

		route := &catalogdb.RouteInfoRecord{
			RouteVersion: proto.RouteVersion(i),
			Type:         proto.CatalogChangeItemAddShard,
			ItemDetail:   &catalogdb.RouteInfoShardAdd{ShardID: proto.ShardID(i)},
		}

		fildMeta := clustermgr.FieldMeta{
			Name:        fmt.Sprintf("fildName%d", i),
			FieldType:   proto.FieldTypeBool,
			IndexOption: proto.IndexOptionIndexed,
		}
		space := &catalogdb.SpaceInfoRecord{
			SpaceID:    proto.SpaceID(i),
			Name:       fmt.Sprintf("spaceName%d", i),
			Status:     proto.SpaceStatusNormal,
			FieldMetas: []clustermgr.FieldMeta{fildMeta},
			AccessKey:  makeKey(),
			SecretKey:  makeKey(),
		}

		shards = append(shards, shard)
		routes = append(routes, route)

		err = catalogTable.CreateSpace(space)
		if err != nil {
			return err
		}
	}

	route := &catalogdb.RouteInfoRecord{
		RouteVersion: proto.RouteVersion(11),
		Type:         proto.CatalogChangeItemUpdateShard,
		ItemDetail:   &catalogdb.RouteInfoShardUpdate{SuidPrefix: proto.EncodeSuidPrefix(1, 1)},
	}
	routes = append(routes, route)

	err = catalogTable.PutShardsAndUnitsAndRouteItems(shards, units, routes)
	if err != nil {
		return err
	}

	nodeTable, err := normaldb.OpenShardNodeTable(normalDB)
	if err != nil {
		return err
	}

	diskTable, err := normaldb.OpenShardNodeDiskTable(normalDB, true)
	if err != nil {
		return err
	}
	for i := 1; i <= unitCount+24; i++ {
		dr := &normaldb.ShardNodeDiskInfoRecord{
			DiskInfoRecord: normaldb.DiskInfoRecord{
				Version:      normaldb.DiskInfoVersionNormal,
				DiskID:       proto.DiskID(i),
				ClusterID:    proto.ClusterID(1),
				Path:         "",
				Status:       proto.DiskStatusNormal,
				Readonly:     false,
				CreateAt:     time.Now(),
				LastUpdateAt: time.Now(),
				NodeID:       proto.NodeID(i),
			},
			Used:         0,
			Size:         100000,
			Free:         100000,
			MaxShardCnt:  10,
			FreeShardCnt: 10,
			UsedShardCnt: 0,
		}
		nr := &normaldb.ShardNodeInfoRecord{
			NodeInfoRecord: normaldb.NodeInfoRecord{
				Version:   normaldb.NodeInfoVersionNormal,
				ClusterID: proto.ClusterID(1),
				NodeID:    proto.NodeID(i),
				Idc:       "z0",
				Rack:      "rack1",
				Host:      "http://127.0.0." + strconv.Itoa(i) + ":80800",
				Role:      proto.NodeRoleShardNode,
				Status:    proto.NodeStatusNormal,
				DiskType:  proto.DiskTypeHDD,
			},
		}
		if i >= 9 && i < 18 {
			dr.Idc = "z1"
			nr.Idc = "z1"
		} else if i >= 18 {
			dr.Idc = "z2"
			nr.Idc = "z2"
		}
		err := diskTable.AddDisk(dr)
		if err != nil {
			return err
		}
		err = nodeTable.UpdateNode(nr)
		if err != nil {
			return err
		}
	}
	return nil
}
