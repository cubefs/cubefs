// Copyright 2025 The CubeFS Authors.
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

package storage

import (
	"context"
	"testing"

	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/util/statistic"
	"github.com/stretchr/testify/require"
)

// TestNewShardMetaStatsRecorder tests the creation of a new shardMetaStatsRecorder
func TestNewShardMetaStatsRecorder(t *testing.T) {
	cfg := MetaStatsConfig{
		RangeCount:         5,
		RequestSampleRatio: 0.5,
		TDigestCompression: 100,
	}

	recorder := newShardMetaStatsRecorder(cfg)
	require.NotNil(t, recorder)
	require.Equal(t, 5, len(recorder.r))
	for i := range recorder.r {
		require.NotNil(t, recorder.r[i])
	}
}

// TestShardMetaStatsRecorder_Load tests loading stats from storage
func TestShardMetaStatsRecorder_Load(t *testing.T) {
	cfg := MetaStatsConfig{
		RangeCount:         3,
		RequestSampleRatio: 0.5,
		TDigestCompression: 100,
	}

	recorder := newShardMetaStatsRecorder(cfg)
	require.NotNil(t, recorder)

	// Test successful load
	stats := snproto.ShardMetaStats{
		ItemCount:    100,
		ItemSize:     1000,
		BlobCount:    50,
		BlobSize:     500,
		AppliedIndex: 10,
		PositiveTdigests: []statistic.TDigestSnapshot{
			statistic.NewTDigest(100).Snapshot(),
			statistic.NewTDigest(100).Snapshot(),
			statistic.NewTDigest(100).Snapshot(),
		},
		NegativeTdigests: []statistic.TDigestSnapshot{
			statistic.NewTDigest(100).Snapshot(),
			statistic.NewTDigest(100).Snapshot(),
			statistic.NewTDigest(100).Snapshot(),
		},
	}

	err := recorder.load(stats)
	require.NoError(t, err)

	loadedStats := recorder.get()
	require.Equal(t, uint64(100), loadedStats.ItemCount)
	require.Equal(t, uint64(1000), loadedStats.ItemSize)
	require.Equal(t, uint64(50), loadedStats.BlobCount)
	require.Equal(t, uint64(500), loadedStats.BlobSize)
	require.Equal(t, uint64(10), loadedStats.AppliedIndex)

	// Test load with mismatched tdigests count
	invalidStats := snproto.ShardMetaStats{
		ItemCount: 100,
		PositiveTdigests: []statistic.TDigestSnapshot{
			statistic.NewTDigest(100).Snapshot(),
		},
		NegativeTdigests: []statistic.TDigestSnapshot{
			statistic.NewTDigest(100).Snapshot(),
		},
	}

	err = recorder.load(invalidStats)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tdigests snapshot count mismatch")
}

// TestShardMetaStatsRecorder_Set tests setting stats
func TestShardMetaStatsRecorder_Set(t *testing.T) {
	cfg := MetaStatsConfig{
		RangeCount:         2,
		RequestSampleRatio: 0.5,
		TDigestCompression: 100,
	}

	recorder := newShardMetaStatsRecorder(cfg)
	require.NotNil(t, recorder)

	stats := snproto.ShardMetaStats{
		ItemCount:    200,
		ItemSize:     2000,
		BlobCount:    100,
		BlobSize:     1000,
		AppliedIndex: 20,
	}

	recorder.set(stats)

	retrievedStats := recorder.get()
	require.Equal(t, uint64(200), retrievedStats.ItemCount)
	require.Equal(t, uint64(2000), retrievedStats.ItemSize)
	require.Equal(t, uint64(100), retrievedStats.BlobCount)
	require.Equal(t, uint64(1000), retrievedStats.BlobSize)
	require.Equal(t, uint64(20), retrievedStats.AppliedIndex)
	require.Equal(t, 2, len(retrievedStats.PositiveTdigests))
	require.Equal(t, 2, len(retrievedStats.NegativeTdigests))
}

// TestShardMetaStatsRecorder_Get tests getting stats
func TestShardMetaStatsRecorder_Get(t *testing.T) {
	// Test with nil recorder
	var nilRecorder *shardMetaStatsRecorder
	stats := nilRecorder.get()
	require.Equal(t, snproto.ShardMetaStats{}, stats)

	// Test with empty value
	cfg := MetaStatsConfig{
		RangeCount:         2,
		RequestSampleRatio: 0.5,
		TDigestCompression: 100,
	}
	recorder := newShardMetaStatsRecorder(cfg)
	stats = recorder.get()
	require.Equal(t, snproto.ShardMetaStats{}, stats)

	// Test with SpaceMetaStats map
	statsWithSpace := snproto.ShardMetaStats{
		ItemCount:    100,
		ItemSize:     1000,
		AppliedIndex: 10,
		SpaceMetaStats: map[uint64]snproto.SpaceMetaStats{
			1: {ItemCount: 50, ItemSize: 500},
			2: {BlobCount: 30, BlobSize: 300},
		},
	}
	recorder.set(statsWithSpace)

	retrievedStats := recorder.get()
	require.Equal(t, uint64(100), retrievedStats.ItemCount)
	require.Equal(t, 2, len(retrievedStats.SpaceMetaStats))
	require.Equal(t, uint64(50), retrievedStats.SpaceMetaStats[1].ItemCount)
	require.Equal(t, uint64(30), retrievedStats.SpaceMetaStats[2].BlobCount)

	// Verify that the map is copied (not the same reference)
	retrievedStats.SpaceMetaStats[1] = snproto.SpaceMetaStats{ItemCount: 999}
	retrievedStats2 := recorder.get()
	require.Equal(t, uint64(50), retrievedStats2.SpaceMetaStats[1].ItemCount)
}

// TestShard_HandleInsertOp tests handling insert operations
func TestShard_HandleInsertOp(t *testing.T) {
	mockShard, cleanup := newMockShard(t)
	defer cleanup()

	s := mockShard.shard

	// Initialize metaStats
	s.metaStats = newShardMetaStatsRecorder(MetaStatsConfig{
		RangeCount:         5,
		RequestSampleRatio: 1.0, // Always sample for testing
		TDigestCompression: 100,
	})

	stats := snproto.ShardMetaStats{
		SpaceMetaStats: make(map[uint64]snproto.SpaceMetaStats),
	}

	// Test insert item
	mou := &metaOpUnit{
		op:           metaOpInsert,
		dataType:     metaDataTypeItem,
		spaceID:      100,
		keySize:      10,
		newValueSize: 100,
		hashValues:   []uint64{1, 2},
	}

	s.handleInsertOp(&stats, mou)
	require.Equal(t, uint64(1), stats.ItemCount)
	require.Equal(t, uint64(110), stats.ItemSize)
	require.Equal(t, uint64(1), stats.SpaceMetaStats[100].ItemCount)
	require.Equal(t, uint64(110), stats.SpaceMetaStats[100].ItemSize)

	// Test insert blob
	mou = &metaOpUnit{
		op:           metaOpInsert,
		dataType:     metaDataTypeBlob,
		spaceID:      200,
		keySize:      20,
		newValueSize: 200,
		hashValues:   []uint64{3, 4},
	}

	s.handleInsertOp(&stats, mou)
	require.Equal(t, uint64(1), stats.BlobCount)
	require.Equal(t, uint64(220), stats.BlobSize)
	require.Equal(t, uint64(1), stats.SpaceMetaStats[200].BlobCount)
	require.Equal(t, uint64(220), stats.SpaceMetaStats[200].BlobSize)

	// Test insert with spaceID = 0 (should be ignored)
	mou = &metaOpUnit{
		op:           metaOpInsert,
		dataType:     metaDataTypeItem,
		spaceID:      0,
		keySize:      10,
		newValueSize: 100,
		hashValues:   []uint64{},
	}

	prevItemCount := stats.ItemCount
	s.handleInsertOp(&stats, mou)
	require.Equal(t, prevItemCount, stats.ItemCount) // Should not change
}

// TestShard_HandleUpdateOp tests handling update operations
func TestShard_HandleUpdateOp(t *testing.T) {
	mockShard, cleanup := newMockShard(t)
	defer cleanup()

	s := mockShard.shard

	stats := snproto.ShardMetaStats{
		ItemCount: 1,
		ItemSize:  100,
		BlobCount: 1,
		BlobSize:  100,
		SpaceMetaStats: map[uint64]snproto.SpaceMetaStats{
			100: {ItemCount: 1, ItemSize: 100},
			200: {BlobCount: 1, BlobSize: 100},
		},
	}

	// Test update item with size increase
	mou := &metaOpUnit{
		op:           metaOpUpdate,
		dataType:     metaDataTypeItem,
		spaceID:      100,
		oldValueSize: 100,
		newValueSize: 150,
	}

	s.handleUpdateOp(&stats, mou)
	require.Equal(t, uint64(150), stats.SpaceMetaStats[100].ItemSize)

	// Test update item with size decrease
	mou = &metaOpUnit{
		op:           metaOpUpdate,
		dataType:     metaDataTypeItem,
		spaceID:      100,
		oldValueSize: 150,
		newValueSize: 50,
	}

	s.handleUpdateOp(&stats, mou)
	require.Equal(t, uint64(50), stats.SpaceMetaStats[100].ItemSize)

	// Test update blob with size increase
	mou = &metaOpUnit{
		op:           metaOpUpdate,
		dataType:     metaDataTypeBlob,
		spaceID:      200,
		oldValueSize: 100,
		newValueSize: 200,
	}

	s.handleUpdateOp(&stats, mou)
	require.Equal(t, uint64(200), stats.SpaceMetaStats[200].BlobSize)

	// Test update blob with size decrease
	mou = &metaOpUnit{
		op:           metaOpUpdate,
		dataType:     metaDataTypeBlob,
		spaceID:      200,
		oldValueSize: 200,
		newValueSize: 80,
	}

	s.handleUpdateOp(&stats, mou)
	require.Equal(t, uint64(80), stats.SpaceMetaStats[200].BlobSize)

	// Test update with spaceID = 0 (should be ignored)
	mou = &metaOpUnit{
		op:           metaOpUpdate,
		dataType:     metaDataTypeItem,
		spaceID:      0,
		oldValueSize: 100,
		newValueSize: 200,
	}

	s.handleUpdateOp(&stats, mou)
	// Should not panic or cause errors
}

// TestShard_HandleDeleteOp tests handling delete operations
func TestShard_HandleDeleteOp(t *testing.T) {
	mockShard, cleanup := newMockShard(t)
	defer cleanup()

	s := mockShard.shard

	// Initialize metaStats
	s.metaStats = newShardMetaStatsRecorder(MetaStatsConfig{
		RangeCount:         5,
		RequestSampleRatio: 1.0, // Always sample for testing
		TDigestCompression: 100,
	})

	stats := snproto.ShardMetaStats{
		ItemCount: 2,
		ItemSize:  200,
		BlobCount: 2,
		BlobSize:  200,
		SpaceMetaStats: map[uint64]snproto.SpaceMetaStats{
			100: {ItemCount: 2, ItemSize: 200},
			200: {BlobCount: 2, BlobSize: 200},
		},
	}

	// Test delete item
	mou := &metaOpUnit{
		op:           metaOpDelete,
		dataType:     metaDataTypeItem,
		spaceID:      100,
		keySize:      10,
		oldValueSize: 100,
		hashValues:   []uint64{1, 2},
	}

	s.handleDeleteOp(&stats, mou)
	require.Equal(t, uint64(1), stats.ItemCount)
	require.Equal(t, uint64(90), stats.ItemSize)
	require.Equal(t, uint64(1), stats.SpaceMetaStats[100].ItemCount)
	require.Equal(t, uint64(90), stats.SpaceMetaStats[100].ItemSize)

	// Test delete blob
	mou = &metaOpUnit{
		op:           metaOpDelete,
		dataType:     metaDataTypeBlob,
		spaceID:      200,
		keySize:      20,
		oldValueSize: 100,
		hashValues:   []uint64{3, 4},
	}

	s.handleDeleteOp(&stats, mou)
	require.Equal(t, uint64(1), stats.BlobCount)
	require.Equal(t, uint64(80), stats.BlobSize)
	require.Equal(t, uint64(1), stats.SpaceMetaStats[200].BlobCount)
	require.Equal(t, uint64(80), stats.SpaceMetaStats[200].BlobSize)

	// Test delete when count is already 0
	stats.ItemCount = 0
	stats.ItemSize = 50
	mou = &metaOpUnit{
		op:           metaOpDelete,
		dataType:     metaDataTypeItem,
		spaceID:      100,
		keySize:      10,
		oldValueSize: 100,
		hashValues:   []uint64{},
	}

	s.handleDeleteOp(&stats, mou)
	require.Equal(t, uint64(0), stats.ItemCount) // Should stay at 0
	require.Equal(t, uint64(0), stats.ItemSize)  // Should not underflow

	// Test delete with spaceID = 0 (should be ignored)
	prevBlobCount := stats.BlobCount
	mou = &metaOpUnit{
		op:           metaOpDelete,
		dataType:     metaDataTypeBlob,
		spaceID:      0,
		keySize:      10,
		oldValueSize: 100,
		hashValues:   []uint64{},
	}

	s.handleDeleteOp(&stats, mou)
	require.Equal(t, prevBlobCount, stats.BlobCount) // Should not change
}

// TestShard_GetOrCreateSpaceStats tests getting or creating space stats
func TestShard_GetOrCreateSpaceStats(t *testing.T) {
	mockShard, cleanup := newMockShard(t)
	defer cleanup()

	s := mockShard.shard

	// Test with nil SpaceMetaStats map
	stats := snproto.ShardMetaStats{}
	spaceStats := s.getOrCreateSpaceStats(&stats, 100)
	require.NotNil(t, spaceStats)
	require.NotNil(t, stats.SpaceMetaStats)

	// Test with existing space stats
	stats.SpaceMetaStats[100] = snproto.SpaceMetaStats{
		ItemCount: 10,
		ItemSize:  1000,
	}
	spaceStats = s.getOrCreateSpaceStats(&stats, 100)
	require.NotNil(t, spaceStats)
	require.Equal(t, uint64(10), spaceStats.ItemCount)
	require.Equal(t, uint64(1000), spaceStats.ItemSize)

	// Test with new space ID
	spaceStats = s.getOrCreateSpaceStats(&stats, 200)
	require.NotNil(t, spaceStats)
	require.Equal(t, uint64(0), spaceStats.ItemCount)
	require.Equal(t, uint64(0), spaceStats.ItemSize)

	// Test with spaceID = 0 (should return nil)
	spaceStats = s.getOrCreateSpaceStats(&stats, 0)
	require.Nil(t, spaceStats)
}

// TestSafeSubtract tests the safeSubtract helper function
func TestSafeSubtract(t *testing.T) {
	// Test normal subtraction
	result := safeSubtract(100, 50)
	require.Equal(t, uint64(50), result)

	// Test equal values
	result = safeSubtract(100, 100)
	require.Equal(t, uint64(0), result)

	// Test underflow prevention
	result = safeSubtract(50, 100)
	require.Equal(t, uint64(0), result)

	// Test with zero target
	result = safeSubtract(0, 100)
	require.Equal(t, uint64(0), result)

	// Test with zero value
	result = safeSubtract(100, 0)
	require.Equal(t, uint64(100), result)
}

// TestShard_UpdateMetaStatsByMetaOpUnit tests the main update function
func TestShard_UpdateMetaStatsByMetaOpUnit(t *testing.T) {
	mockShard, cleanup := newMockShard(t)
	defer cleanup()

	s := mockShard.shard

	// Initialize metaStats
	s.metaStats = newShardMetaStatsRecorder(MetaStatsConfig{
		RangeCount:         5,
		RequestSampleRatio: 0.1,
		TDigestCompression: 100,
	})

	initialStats := snproto.ShardMetaStats{
		AppliedIndex:   5,
		SpaceMetaStats: make(map[uint64]snproto.SpaceMetaStats),
	}
	s.metaStats.set(initialStats)

	ctx := context.Background()

	// Test with older index (should not update)
	metaOpUnits := []*metaOpUnit{
		{
			op:           metaOpInsert,
			dataType:     metaDataTypeItem,
			spaceID:      100,
			keySize:      10,
			newValueSize: 100,
			hashValues:   []uint64{1},
		},
	}

	s.updateMetaStatsByMetaOpUnit(ctx, metaOpUnits, 3)
	stats := s.metaStats.get()
	require.Equal(t, uint64(5), stats.AppliedIndex)
	require.Equal(t, uint64(0), stats.ItemCount) // Should not have changed

	// Test with newer index (should update)
	s.updateMetaStatsByMetaOpUnit(ctx, metaOpUnits, 10)
	stats = s.metaStats.get()
	require.Equal(t, uint64(10), stats.AppliedIndex)
	require.Equal(t, uint64(1), stats.ItemCount)
	require.Equal(t, uint64(110), stats.ItemSize)

	// Test with multiple operations
	metaOpUnits = []*metaOpUnit{
		{
			op:           metaOpInsert,
			dataType:     metaDataTypeBlob,
			spaceID:      200,
			keySize:      20,
			newValueSize: 200,
			hashValues:   []uint64{2},
		},
		{
			op:           metaOpUpdate,
			dataType:     metaDataTypeItem,
			spaceID:      100,
			oldValueSize: 100,
			newValueSize: 150,
		},
		nil, // Test nil handling
		{
			op:           metaOpDelete,
			dataType:     metaDataTypeItem,
			spaceID:      100,
			keySize:      10,
			oldValueSize: 150,
			hashValues:   []uint64{1},
		},
	}

	s.updateMetaStatsByMetaOpUnit(ctx, metaOpUnits, 15)
	stats = s.metaStats.get()
	require.Equal(t, uint64(15), stats.AppliedIndex)
	require.Equal(t, uint64(0), stats.ItemCount) // Inserted 1, deleted 1
	require.Equal(t, uint64(1), stats.BlobCount)
	require.Equal(t, uint64(220), stats.BlobSize)
}

// TestShard_HandleInsertOpWithDifferentDataTypes tests insert with unknown data type
func TestShard_HandleInsertOpWithDifferentDataTypes(t *testing.T) {
	mockShard, cleanup := newMockShard(t)
	defer cleanup()

	s := mockShard.shard

	stats := snproto.ShardMetaStats{
		SpaceMetaStats: make(map[uint64]snproto.SpaceMetaStats),
	}

	// Test with unknown data type (should not panic)
	mou := &metaOpUnit{
		op:           metaOpInsert,
		dataType:     metaDataType(99), // Unknown type
		spaceID:      100,
		keySize:      10,
		newValueSize: 100,
		hashValues:   []uint64{},
	}

	s.handleInsertOp(&stats, mou)
	require.Equal(t, uint64(0), stats.ItemCount) // Should not have changed
}

// TestShard_HandleUpdateOpWithDifferentDataTypes tests update with unknown data type
func TestShard_HandleUpdateOpWithDifferentDataTypes(t *testing.T) {
	mockShard, cleanup := newMockShard(t)
	defer cleanup()

	s := mockShard.shard

	stats := snproto.ShardMetaStats{
		SpaceMetaStats: map[uint64]snproto.SpaceMetaStats{
			100: {ItemCount: 1, ItemSize: 100},
		},
	}

	// Test with unknown data type (should not panic)
	mou := &metaOpUnit{
		op:           metaOpUpdate,
		dataType:     metaDataType(99), // Unknown type
		spaceID:      100,
		oldValueSize: 100,
		newValueSize: 200,
	}

	s.handleUpdateOp(&stats, mou)
	require.Equal(t, uint64(100), stats.SpaceMetaStats[100].ItemSize) // Should not have changed
}

// TestShard_HandleDeleteOpWithDifferentDataTypes tests delete with unknown data type
func TestShard_HandleDeleteOpWithDifferentDataTypes(t *testing.T) {
	mockShard, cleanup := newMockShard(t)
	defer cleanup()

	s := mockShard.shard

	// Initialize metaStats
	s.metaStats = newShardMetaStatsRecorder(MetaStatsConfig{
		RangeCount:         5,
		RequestSampleRatio: 0.1,
		TDigestCompression: 100,
	})

	stats := snproto.ShardMetaStats{
		ItemCount: 1,
		ItemSize:  100,
		SpaceMetaStats: map[uint64]snproto.SpaceMetaStats{
			100: {ItemCount: 1, ItemSize: 100},
		},
	}

	// Test with unknown data type (should not panic)
	mou := &metaOpUnit{
		op:           metaOpDelete,
		dataType:     metaDataType(99), // Unknown type
		spaceID:      100,
		keySize:      10,
		oldValueSize: 100,
		hashValues:   []uint64{},
	}

	s.handleDeleteOp(&stats, mou)
	require.Equal(t, uint64(1), stats.ItemCount) // Should not have changed
}
