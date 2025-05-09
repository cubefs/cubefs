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

	"github.com/stretchr/testify/require"
)

var transitedTable *TransitedTable

func TestTransitedTable_PutShardAndShardUnit(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	shardRecs := [][]*ShardInfoRecord{{shard1}, {shard2}, {shard3}}
	units := [][]*ShardUnitInfoRecord{{shardUnit1}, {shardUnit2}, {shardUnit3}}
	for i := range shards {
		err := transitedTable.PutShardsAndShardUnits(shardRecs[i], units[i])
		require.NoError(t, err)
	}
}

func TestTransitedTable_RangeShardRecord(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	shardRecs := [][]*ShardInfoRecord{{shard1}, {shard2}, {shard3}}
	units := [][]*ShardUnitInfoRecord{{shardUnit1}, {shardUnit2}, {shardUnit3}}
	for i := range shardRecs {
		err := transitedTable.PutShardsAndShardUnits(shardRecs[i], units[i])
		require.NoError(t, err)
	}

	i := 0
	err := transitedTable.RangeShard(func(record *ShardInfoRecord) error {
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, i, len(shards))
}

func TestTransitedTable_GetShardUnit(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	shardRecs := [][]*ShardInfoRecord{{shard1}, {shard2}, {shard3}}
	units := [][]*ShardUnitInfoRecord{{shardUnit1}, {shardUnit2}, {shardUnit3}}
	for i := range shardRecs {
		err := transitedTable.PutShardsAndShardUnits(shardRecs[i], units[i])
		require.NoError(t, err)
	}

	shardUnit, err := transitedTable.GetShardUnit(shardUnit1.SuidPrefix)
	require.NoError(t, err)
	require.Equal(t, shardUnit1.SuidPrefix, shardUnit.SuidPrefix)
	require.Equal(t, shardUnit1.Epoch, shardUnit.Epoch)
	require.Equal(t, shardUnit1.NextEpoch, shardUnit.NextEpoch)
	require.Equal(t, shardUnit1.DiskID, shardUnit.DiskID)
}

func TestTransitedTable_UpdateShardUnit(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	shardRecs := [][]*ShardInfoRecord{{shard1}, {shard2}, {shard3}}
	units := [][]*ShardUnitInfoRecord{{shardUnit1}, {shardUnit2}, {shardUnit3}}
	for i := range shardRecs {
		err := transitedTable.PutShardsAndShardUnits(shardRecs[i], units[i])
		require.NoError(t, err)
	}
	for _, shardUnit := range []*ShardUnitInfoRecord{shardUnit1, shardUnit2, shardUnit3} {
		newShardUnit := *shardUnit
		newShardUnit.Epoch += 10
		newShardUnit.NextEpoch += 10
		err := transitedTable.PutShardUnits([]*ShardUnitInfoRecord{&newShardUnit})
		require.NoError(t, err)
	}

	for _, shardUnit := range []*ShardUnitInfoRecord{shardUnit1, shardUnit2, shardUnit3} {
		unit, err := transitedTable.GetShardUnit(shardUnit.SuidPrefix)
		require.NoError(t, err)
		require.Equal(t, shardUnit.SuidPrefix, unit.SuidPrefix)
		require.Equal(t, shardUnit.Epoch+10, unit.Epoch)
		require.Equal(t, shardUnit.Epoch+10, unit.NextEpoch)
	}
}

func TestTransitedTable_DeleteShardAndShardUnit(t *testing.T) {
	initCatalogDB()
	defer closeCatalogDB()

	shardRecs := [][]*ShardInfoRecord{{shard1}, {shard2}, {shard3}}
	units := [][]*ShardUnitInfoRecord{{shardUnit1}, {shardUnit2}, {shardUnit3}}
	for i := range shardRecs {
		err := transitedTable.PutShardsAndShardUnits(shardRecs[i], units[i])
		require.NoError(t, err)
	}

	for i := range shardRecs {
		err := transitedTable.DeleteShardAndUnits(shards[i], units[i])
		require.NoError(t, err)
	}
}
