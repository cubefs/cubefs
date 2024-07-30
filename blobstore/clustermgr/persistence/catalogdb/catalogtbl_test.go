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

package catalogdb

import (
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

var catalogTable *CatalogTable

var (
	shardUnit1 = &ShardUnitInfoRecord{
		Version:    ShardUnitInfoVersionNormal,
		SuidPrefix: 4294967296,
		Epoch:      1432,
		NextEpoch:  1432,
		DiskID:     20,
		Status:     proto.ShardUnitStatusNormal,
	}
	shardUnit2 = &ShardUnitInfoRecord{
		Version:    ShardUnitInfoVersionNormal,
		SuidPrefix: 8589934592,
		Epoch:      1,
		NextEpoch:  1,
		DiskID:     32,
		Status:     proto.ShardUnitStatusNormal,
	}
	shardUnit3 = &ShardUnitInfoRecord{
		Version:    ShardUnitInfoVersionNormal,
		SuidPrefix: 12884901888,
		Epoch:      1,
		NextEpoch:  1,
		DiskID:     32,
		Status:     proto.ShardUnitStatusNormal,
	}

	rg     = sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 2, 3)
	shard1 = &ShardInfoRecord{
		Version:      ShardInfoVersionNormal,
		ShardID:      1,
		SuidPrefixes: []proto.SuidPrefix{4294967296, 4311744512, 4328521728, 4345298944, 4362076160, 4378853376, 4395630592, 4412407808, 4429185024, 4445962240, 4462739456, 4479516672, 4496293888, 4513071104, 4529848320, 4546625536, 4563402752, 4580179968, 4596957184, 4613734400, 4630511616, 4647288832, 4664066048, 4680843264, 4697620480, 4714397696, 4731174912},
		LeaderDiskID: 1,
		Range:        *rg[0],
		RouteVersion: proto.RouteVersion(1),
	}
	shard2 = &ShardInfoRecord{
		Version:      ShardInfoVersionNormal,
		ShardID:      2,
		SuidPrefixes: []proto.SuidPrefix{8589934592, 8606711808, 8623489024, 8640266240, 8657043456, 8673820672, 8690597888, 8707375104, 8724152320, 8740929536, 8757706752, 8774483968, 8791261184, 8808038400, 8824815616, 8841592832, 8858370048, 8875147264, 8891924480, 8908701696, 8925478912, 8942256128, 8959033344, 8975810560, 8992587776, 9009364992, 9026142208},
		LeaderDiskID: 1,
		Range:        *rg[1],
		RouteVersion: proto.RouteVersion(2),
	}
	shard3 = &ShardInfoRecord{
		Version:      ShardInfoVersionNormal,
		ShardID:      3,
		SuidPrefixes: []proto.SuidPrefix{12884901888, 12901679104, 12918456320, 12935233536, 12952010752, 12968787968, 12985565184, 13002342400, 13019119616, 13035896832, 13052674048, 13069451264, 13086228480, 13103005696, 13119782912, 13136560128, 13153337344, 13170114560, 13186891776, 13203668992, 13220446208, 13237223424, 13254000640, 13270777856, 13287555072, 13304332288, 13321109504},
		LeaderDiskID: 1,
		Range:        *rg[2],
		RouteVersion: proto.RouteVersion(3),
	}

	route1 = &RouteInfoRecord{
		Version:      RouteInfoVersionNormal,
		RouteVersion: shard1.RouteVersion,
		Type:         proto.CatalogChangeItemAddShard,
		ItemDetail:   &RouteInfoShardAdd{ShardID: shard1.ShardID},
	}

	route2 = &RouteInfoRecord{
		Version:      RouteInfoVersionNormal,
		RouteVersion: shard2.RouteVersion,
		Type:         proto.CatalogChangeItemAddShard,
		ItemDetail:   &RouteInfoShardAdd{ShardID: shard2.ShardID},
	}

	route3 = &RouteInfoRecord{
		Version:      RouteInfoVersionNormal,
		RouteVersion: shard3.RouteVersion,
		Type:         proto.CatalogChangeItemUpdateShard,
		ItemDetail:   &RouteInfoShardUpdate{SuidPrefix: shard3.SuidPrefixes[0]},
	}

	fildMeta = clustermgr.FieldMeta{
		ID:          1,
		Name:        "name1",
		FieldType:   proto.FieldTypeBool,
		IndexOption: proto.IndexOptionIndexed,
	}

	space1 = &SpaceInfoRecord{
		Version:    SpaceInfoVersionNormal,
		SpaceID:    proto.SpaceID(1),
		Name:       "space1",
		Status:     proto.SpaceStatusNormal,
		FieldMetas: []clustermgr.FieldMeta{fildMeta},
		AccessKey:  "ak1",
		SecretKey:  "sk1",
	}
	space2 = &SpaceInfoRecord{
		Version:    SpaceInfoVersionNormal,
		SpaceID:    proto.SpaceID(2),
		Name:       "space2",
		Status:     proto.SpaceStatusNormal,
		FieldMetas: []clustermgr.FieldMeta{fildMeta},
		AccessKey:  "ak2",
		SecretKey:  "sk2",
	}
	space3 = &SpaceInfoRecord{
		Version:    SpaceInfoVersionNormal,
		SpaceID:    proto.SpaceID(3),
		Name:       "space3",
		Status:     proto.SpaceStatusNormal,
		FieldMetas: []clustermgr.FieldMeta{fildMeta},
		AccessKey:  "ak3",
		SecretKey:  "sk3",
	}

	shards     = []*ShardInfoRecord{shard1, shard2, shard3}
	shardUnits = []*ShardUnitInfoRecord{shardUnit1, shardUnit2, shardUnit3}
	routes     = []*RouteInfoRecord{route1, route2, route3}
	spaces     = []*SpaceInfoRecord{space1, space2, space3}
)

func TestCatalogTable_PutShard(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	err := catalogTable.PutShardsAndUnitsAndRouteItems(shards, nil, nil)
	require.NoError(t, err)

	for _, shard := range shards {
		record, err := catalogTable.GetShard(shard.ShardID)
		require.NoError(t, err)
		require.Equal(t, record.ShardID, shard.ShardID)
		require.Equal(t, record.SuidPrefixes, shard.SuidPrefixes)
		require.Equal(t, record.LeaderDiskID, shard.LeaderDiskID)
		require.Equal(t, record.Range, shard.Range)
		require.Equal(t, record.RouteVersion, shard.RouteVersion)
	}
}

func TestCatalogTable_PutShardUnit(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	err := catalogTable.PutShardsAndUnitsAndRouteItems(nil, shardUnits, nil)
	require.NoError(t, err)
	for _, unit := range shardUnits {
		record, err := catalogTable.GetShardUnit(unit.SuidPrefix)
		require.NoError(t, err)
		require.Equal(t, record.SuidPrefix, unit.SuidPrefix)
		require.Equal(t, record.Epoch, unit.Epoch)
		require.Equal(t, record.NextEpoch, unit.NextEpoch)
		require.Equal(t, record.DiskID, unit.DiskID)
		require.Equal(t, record.Status, unit.Status)
		require.Equal(t, record.Learner, unit.Learner)
	}
	i := 0
	catalogTable.RangeShardUnits(func(unitRecord *ShardUnitInfoRecord) {
		i++
	})
	require.Equal(t, i, 3)
}

func TestCatalogTable_PutShardAndShardUnit(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	err := catalogTable.PutShardsAndUnitsAndRouteItems(shards, shardUnits, nil)
	require.NoError(t, err)
}

func TestCatalogTable_PutBatch(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	err := catalogTable.PutShardsAndUnitsAndRouteItems(shards, shardUnits, routes)
	require.NoError(t, err)

	ret, err := catalogTable.ListShardUnit(32)
	require.NoError(t, err)
	require.Equal(t, 2, len(ret))

	routeInfoRecords, err := catalogTable.ListRoute()
	require.NoError(t, err)
	require.Equal(t, 3, len(routeInfoRecords))
}

func TestCatalogTable_RangeShardInfoRecord(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	err := catalogTable.PutShardsAndUnitsAndRouteItems(shards, nil, nil)
	require.NoError(t, err)

	i := 0
	err = catalogTable.RangeShardRecord(func(Record *ShardInfoRecord) error {
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, i, len(shards))
}

func TestCatalogTable_ListShard(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	err := catalogTable.PutShardsAndUnitsAndRouteItems(shards, nil, nil)
	require.NoError(t, err)

	ret, err := catalogTable.ListShard(5, 0)
	require.NoError(t, err)
	require.Equal(t, len(ret), 3)

	ret, err = catalogTable.ListShard(5, 2)
	require.NoError(t, err)
	require.Equal(t, len(ret), 1)

	ret, err = catalogTable.ListShard(2, 1)
	require.NoError(t, err)
	require.Equal(t, len(ret), 2)

	ret, err = catalogTable.ListShard(1, 2)
	require.NoError(t, err)
	require.Equal(t, len(ret), 1)

	ret, err = catalogTable.ListShard(12, 44)
	require.NoError(t, err)
	require.Equal(t, len(ret), 0)
}

func TestCatalogTable_UpdateShardUnit(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	err := catalogTable.PutShardsAndUnitsAndRouteItems(nil, []*ShardUnitInfoRecord{shardUnit1}, nil)
	require.NoError(t, err)

	err = catalogTable.PutShardsAndUnitsAndRouteItems(nil, []*ShardUnitInfoRecord{shardUnit2}, nil)
	require.NoError(t, err)

	shardUnitInfo2 := *shardUnit2
	shardUnitInfo2.DiskID = 20
	err = catalogTable.PutShardsAndUnitsAndRouteItems(nil, []*ShardUnitInfoRecord{&shardUnitInfo2}, nil)
	require.NoError(t, err)

	ret, err := catalogTable.ListShardUnit(20)
	require.NoError(t, err)
	require.Equal(t, len(ret), 2)

	// repeat update shard unit
	err = catalogTable.PutShardsAndUnitsAndRouteItems(nil, []*ShardUnitInfoRecord{&shardUnitInfo2}, nil)
	require.NoError(t, err)
	ret, err = catalogTable.ListShardUnit(20)
	require.NoError(t, err)
	require.Equal(t, 2, len(ret))

	shardUnitInfo2.DiskID = 45
	err = catalogTable.UpdateUnitsAndPutShardsAndRouteItems(shards, []*ShardUnitInfoRecord{&shardUnitInfo2}, routes)
	require.NoError(t, err)

	ret, err = catalogTable.ListShardUnit(20)
	require.NoError(t, err)
	require.Equal(t, 1, len(ret))

	ret, err = catalogTable.ListShardUnit(45)
	require.NoError(t, err)
	require.Equal(t, 1, len(ret))
}

func TestCatalogTable_Route(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	err := catalogTable.PutShardsAndUnitsAndRouteItems(nil, nil, routes)
	require.NoError(t, err)

	routeInfoRecord, err := catalogTable.GetFirstRouteItem()
	require.NoError(t, err)
	require.Equal(t, route1.Version, routeInfoRecord.Version)
	require.Equal(t, route1.RouteVersion, routeInfoRecord.RouteVersion)
	require.Equal(t, route1.Type, routeInfoRecord.Type)
	require.Equal(t, route1.ItemDetail, routeInfoRecord.ItemDetail)

	routeInfoRecords, err := catalogTable.ListRoute()
	require.NoError(t, err)
	require.Equal(t, 3, len(routeInfoRecords))

	err = catalogTable.DeleteOldRoutes(route2.RouteVersion)
	require.NoError(t, err)

	routeInfoRecords, err = catalogTable.ListRoute()
	require.NoError(t, err)
	require.Equal(t, 2, len(routeInfoRecords))
}

func TestCatalogTable_Space(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	for _, space := range spaces {
		err := catalogTable.CreateSpace(space)
		require.NoError(t, err)
	}

	ret, err := catalogTable.ListSpace(5, 0)
	require.NoError(t, err)
	require.Equal(t, len(ret), 3)

	ret, err = catalogTable.ListSpace(5, 2)
	require.NoError(t, err)
	require.Equal(t, len(ret), 1)

	ret, err = catalogTable.ListSpace(2, 1)
	require.NoError(t, err)
	require.Equal(t, len(ret), 2)

	ret, err = catalogTable.ListSpace(1, 2)
	require.NoError(t, err)
	require.Equal(t, len(ret), 1)

	ret, err = catalogTable.ListSpace(12, 44)
	require.NoError(t, err)
	require.Equal(t, len(ret), 0)

	i := 0
	catalogTable.RangeSpaceRecord(func(record *SpaceInfoRecord) error {
		i++
		return nil
	})
	require.Equal(t, i, 3)
}
