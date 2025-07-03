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

package clustermgr

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
)

// generate 10 shards in db
func initServiceWithShardData() (*Service, func()) {
	cfg := *testServiceCfg

	cfg.DBPath = os.TempDir() + "/shard" + uuid.NewString() + strconv.FormatInt(rand.Int63n(math.MaxInt64), 10)
	cfg.ShardNodeDiskMgrConfig.HeartbeatExpireIntervalS = 600
	cfg.CatalogMgrConfig.InitShardNum = 10
	cfg.ClusterReportIntervalS = 3
	cfg.ShardCodeModeName = codemode.Replica3.Name()
	cfg.RaftConfig.ServerConfig.ListenPort = GetFreePort()
	cfg.RaftConfig.ServerConfig.Members = []raftserver.Member{
		{NodeID: 1, Host: fmt.Sprintf("127.0.0.1:%d", GetFreePort()), Learner: false},
	}

	err := os.Mkdir(cfg.DBPath, 0o755)
	if err != nil {
		panic("generate shard mkdir: " + err.Error())
	}
	err = generateShard(cfg.DBPath+"/catalogdb", cfg.DBPath+"/normaldb")
	if err != nil {
		panic("generate shard error: " + err.Error())
	}

	testService, _ := New(&cfg)
	return testService, func() {
		cleanTestService(testService)
	}
}

func TestService_ShardInfo(t *testing.T) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// get shardInfo
	{
		ret, err := cmClient.GetShardInfo(ctx, &clustermgr.GetShardArgs{ShardID: 1})
		require.NoError(t, err)
		require.NotNil(t, ret)
	}

	// list shardInfo
	{
		listArgs := &clustermgr.ListShardArgs{
			Marker: proto.ShardID(0),
			Count:  1,
		}
		list, err := cmClient.ListShard(ctx, listArgs)
		require.NoError(t, err)
		require.Equal(t, 1, len(list.Shards))
		require.Equal(t, proto.ShardID(1), list.Shards[0].ShardID)

		listArgs.Marker = proto.ShardID(1)
		listArgs.Count = 4
		list, err = cmClient.ListShard(ctx, listArgs)
		require.NoError(t, err)
		require.Equal(t, 4, len(list.Shards))
		require.Equal(t, proto.ShardID(2), list.Shards[0].ShardID)
		require.Equal(t, proto.ShardID(5), list.Shards[3].ShardID)
	}
}

func TestService_UpdateShard(t *testing.T) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// success case
	{
		oldSuid := proto.EncodeSuid(1, 1, 1)
		newSuid := proto.EncodeSuid(1, 1, 1)
		updateArgs := &clustermgr.UpdateShardArgs{
			NewSuid:     newSuid,
			OldSuid:     oldSuid,
			NewDiskID:   proto.DiskID(19),
			NewIsLeaner: false,
			OldIsLeaner: false,
		}
		err := cmClient.UpdateShard(context.Background(), updateArgs)
		require.NoError(t, err)
	}
	// failed case ,update unit next epoch not match
	{
		oldSuid := proto.EncodeSuid(1, 1, 1)
		newSuid := proto.EncodeSuid(1, 1, 2)
		updateArgs := &clustermgr.UpdateShardArgs{
			NewSuid:     newSuid,
			NewDiskID:   proto.DiskID(29),
			OldSuid:     oldSuid,
			NewIsLeaner: false,
			OldIsLeaner: false,
		}
		err := cmClient.UpdateShard(ctx, updateArgs)
		require.Error(t, err)
	}

	// alloc Shard unit
	{
		oldSuid := proto.EncodeSuid(1, 1, 1)
		args := &clustermgr.AllocShardUnitArgs{
			Suid: oldSuid,
		}
		// alloc shard unit failed case
		_, err := cmClient.AllocShardUnit(ctx, args)
		require.Error(t, err)
	}
}

func TestService_ShardUnitList(t *testing.T) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// list shardUnits
	{
		ret, err := cmClient.ListShardUnit(ctx, &clustermgr.ListShardUnitArgs{DiskID: proto.DiskID(2)})
		require.NoError(t, err)
		require.NotNil(t, ret)

		_, err = cmClient.ListShardUnit(ctx, &clustermgr.ListShardUnitArgs{DiskID: proto.DiskID(99)})
		require.NoError(t, err)
		require.Nil(t, err)
	}
}

func TestService_ShardReport(t *testing.T) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// shard report
	{
		var unitInfos []clustermgr.ShardUnitInfo
		for i := 1; i < 11; i++ {
			suid := proto.EncodeSuid(proto.ShardID(i), 2, 1)
			unitInfo := clustermgr.ShardUnitInfo{
				Suid:         suid,
				DiskID:       proto.DiskID(i),
				RouteVersion: proto.RouteVersion(i),
			}
			unitInfos = append(unitInfos, unitInfo)
		}
		args := &clustermgr.ShardReportArgs{Shards: unitInfos}
		_, err := cmClient.ReportShard(ctx, args)
		require.NoError(t, err)
	}
}

func TestService_AdminUpdateShard(t *testing.T) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()
	ranges := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 2, 10)
	args := &clustermgr.Shard{
		ShardID:      1,
		RouteVersion: 99,
		Range:        *ranges[0],
	}
	err := cmClient.AdminUpdateShard(ctx, args)
	require.NoError(t, err)
}

func TestService_AdminUpdateShardUnit(t *testing.T) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	args := &clustermgr.AdminUpdateShardUnitArgs{
		Epoch:     3,
		NextEpoch: 5,
		ShardUnit: clustermgr.ShardUnit{
			Suid:   proto.EncodeSuid(1, 1, 1),
			DiskID: 1,
			Status: proto.ShardUnitStatusNormal,
		},
	}
	err := cmClient.AdminUpdateShardUnit(ctx, args)
	require.NoError(t, err)
	shardInfo, err := cmClient.GetShardInfo(ctx, &clustermgr.GetShardArgs{ShardID: 1})
	require.NoError(t, err)
	require.Equal(t, shardInfo.Units[1].DiskID, args.DiskID)
	require.Equal(t, shardInfo.Units[1].Suid, proto.EncodeSuid(args.Suid.SuidPrefix().ShardID(), args.Suid.SuidPrefix().Index(), args.Epoch))

	// failed case, diskID not exist
	args.ShardUnit.DiskID = 88
	err = cmClient.AdminUpdateShardUnit(ctx, args)
	require.Error(t, err)

	// failed case, shardID not exist
	args.ShardUnit.Suid = proto.EncodeSuid(99, 1, 1)
	err = cmClient.AdminUpdateShardUnit(ctx, args)
	require.Error(t, err)
}

func generateShard(catalogDBPath, NormalDBPath string) error {
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
	normalDB, err := normaldb.OpenNormalDB(NormalDBPath)
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
				NextEpoch:  1,
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

		shards = append(shards, shard)
		routes = append(routes, route)
	}
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
				DiskSetID:    proto.DiskSetID(2),
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
				DiskType:  proto.DiskTypeNVMeSSD,
				NodeSetID: proto.NodeSetID(2),
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

func BenchmarkService_ShardList(b *testing.B) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	listArgs := &clustermgr.ListShardArgs{
		Marker: proto.ShardID(1),
		Count:  10,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cmClient.ListShard(ctx, listArgs)
		}
	})
}

func BenchmarkService_ShardReport(b *testing.B) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	var unitInfos []clustermgr.ShardUnitInfo
	for i := 1; i < 11; i++ {
		suid := proto.EncodeSuid(proto.ShardID(i), 2, 1)
		unitInfo := clustermgr.ShardUnitInfo{
			Suid:         suid,
			DiskID:       proto.DiskID(i),
			RouteVersion: proto.RouteVersion(i),
		}
		unitInfos = append(unitInfos, unitInfo)
	}
	args := &clustermgr.ShardReportArgs{Shards: unitInfos}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := cmClient.ReportShard(ctx, args)
			require.NoError(b, err)
		}
	})
}
