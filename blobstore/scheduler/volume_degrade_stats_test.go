// Copyright 2022 The CubeFS Authors.
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

package scheduler

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func newDefaultDegradeStats(t *testing.T) *VolumeDegradeStatsMgr {
	ctrl := gomock.NewController(t)
	mockCli := NewMockClusterMgrAPI(ctrl)
	clusterID := proto.ClusterID(1)
	cfg := &VolumeDegradeStatsCfg{
		Enable: true,
		Batch:  3,
	}
	return newVolumeDegradeStatsMgr(clusterID, mockCli, cfg)
}

func genVolumeDegradeStats(batchSize uint32, numBatches int) *proto.VolumeDegradeStats {
	blobBatchs := make([]proto.VolumeDegradeBatch, numBatches)
	for i := 0; i < numBatches; i++ {
		batchIndex := uint32(i)
		codeModeStats := []proto.VolumeDegradeCodeMode{
			{
				Mode: codemode.EC6P6,
				DegradeStats: []proto.VolumeDegradeLevel{
					{Level: 1, Count: 123},
					{Level: 2, Count: 456},
				},
			},
			{
				Mode: codemode.EC3P3,
				DegradeStats: []proto.VolumeDegradeLevel{
					{Level: 3, Count: 789},
				},
			},
		}
		blobBatchs[i] = proto.VolumeDegradeBatch{
			BatchIndex:    batchIndex,
			CodeModeStats: codeModeStats,
		}
	}

	volBatchs := make([]proto.VolumeDegradeBatch, numBatches)
	for i := 0; i < numBatches; i++ {
		batchIndex := uint32(i)
		codeModeStats := []proto.VolumeDegradeCodeMode{
			{
				Mode: codemode.EC6P6,
				DegradeStats: []proto.VolumeDegradeLevel{
					{Level: 1, Count: 1},
					{Level: 2, Count: 2},
				},
			},
			{
				Mode: codemode.EC3P3,
				DegradeStats: []proto.VolumeDegradeLevel{
					{Level: 3, Count: 3},
				},
			},
		}
		volBatchs[i] = proto.VolumeDegradeBatch{
			BatchIndex:    batchIndex,
			CodeModeStats: codeModeStats,
		}
	}

	return &proto.VolumeDegradeStats{
		BatchSize: batchSize,
		BlobStats: blobBatchs,
		VolStats:  volBatchs,
	}
}

func genBidsMissed() map[proto.BlobID][]uint8 {
	bids := make(map[proto.BlobID][]uint8)

	{
		bid := proto.BlobID(1)
		shardCount := 1
		shards := make([]uint8, shardCount)
		for i := 0; i < shardCount; i++ {
			shards[i] = uint8(i)
		}
		bids[bid] = shards
	}

	{
		bid := proto.BlobID(2)
		shardCount := 2
		shards := make([]uint8, shardCount)
		for i := 0; i < shardCount; i++ {
			shards[i] = uint8(i)
		}
		bids[bid] = shards
	}

	{
		bid := proto.BlobID(3)
		shardCount := 3
		shards := make([]uint8, shardCount)
		for i := 0; i < shardCount; i++ {
			shards[i] = uint8(i)
		}
		bids[bid] = shards
	}

	{
		bid := proto.BlobID(4)
		shardCount := 4
		shards := make([]uint8, shardCount)
		for i := 0; i < shardCount; i++ {
			shards[i] = uint8(i)
		}
		bids[bid] = shards
	}
	return bids
}

func TestDegradeStatsInitialize(t *testing.T) {
	// not enabled
	{
		mgr := newDefaultDegradeStats(t)
		mgr.cfg.Enable = false
		err := mgr.initialize()
		require.NoError(t, err)
	}
	// get stats failed
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(nil, errMock)

		err := mgr.initialize()
		require.NotNil(t, err)
	}
	// not found
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(nil, errors.ErrNotFound)

		err := mgr.initialize()
		require.NoError(t, err)
		require.Equal(t, uint32(3), mgr.stats.batchSize)
		require.Equal(t, 0, len(mgr.stats.blobCounter))
		require.Equal(t, 0, len(mgr.stats.volCounter))
	}
	// delete stats failed
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(genVolumeDegradeStats(10, 3), nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteVolumeDegradeStats(any).AnyTimes().Return(errMock)

		err := mgr.initialize()
		require.NotNil(t, err)
	}
	// 3 batch stats
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(genVolumeDegradeStats(3, 3), nil)

		err := mgr.initialize()
		require.NoError(t, err)
		require.Equal(t, uint32(3), mgr.stats.batchSize)
		require.Equal(t, 3, len(mgr.stats.blobCounter))
		require.Equal(t, 3, len(mgr.stats.volCounter))
	}
	// new batch size
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(genVolumeDegradeStats(10, 3), nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteVolumeDegradeStats(any).AnyTimes().Return(nil)

		err := mgr.initialize()
		require.NoError(t, err)
		require.Equal(t, uint32(3), mgr.stats.batchSize)
		require.Equal(t, 0, len(mgr.stats.blobCounter))
		require.Equal(t, 0, len(mgr.stats.volCounter))
	}
}

func TestDegradeStatsUpdate(t *testing.T) {
	ctx := context.Background()
	// new cluster
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(nil, errors.ErrNotFound)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetVolumeDegradeStats(any, any).AnyTimes().Return(nil)

		err := mgr.initialize()
		require.NoError(t, err)
		require.Equal(t, uint32(3), mgr.stats.batchSize)
		require.Equal(t, 0, len(mgr.stats.blobCounter))
		require.Equal(t, 0, len(mgr.stats.volCounter))

		mgr.updateDegradeInfo(ctx, 0, codemode.EC3P3, genBidsMissed())
		mgr.updateDegradeInfo(ctx, 1, codemode.EC3P3, genBidsMissed())
		mgr.updateDegradeInfo(ctx, 2, codemode.EC3P3, genBidsMissed())
		require.Equal(t, 0, len(mgr.stats.blobCounter))
		require.Equal(t, 0, len(mgr.stats.volCounter))
		require.Equal(t, uint32(0), mgr.curBlobStats.batchIndex)

		mgr.updateDegradeInfo(ctx, 3, codemode.EC3P3, genBidsMissed())
		require.Equal(t, 1, len(mgr.stats.blobCounter))
		require.Equal(t, 1, len(mgr.stats.volCounter))
		require.Equal(t, uint32(1), mgr.curBlobStats.batchIndex)
	}
	// set stats failed
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(nil, errors.ErrNotFound)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetVolumeDegradeStats(any, any).AnyTimes().Return(errMock)

		err := mgr.initialize()
		require.NoError(t, err)
		require.Equal(t, uint32(3), mgr.stats.batchSize)
		require.Equal(t, 0, len(mgr.stats.blobCounter))
		require.Equal(t, 0, len(mgr.stats.volCounter))

		mgr.updateDegradeInfo(ctx, 0, codemode.EC3P3, genBidsMissed())
		mgr.updateDegradeInfo(ctx, 1, codemode.EC3P3, genBidsMissed())
		mgr.updateDegradeInfo(ctx, 2, codemode.EC3P3, genBidsMissed())
		require.Equal(t, 0, len(mgr.stats.blobCounter))
		require.Equal(t, 0, len(mgr.stats.volCounter))
		require.Equal(t, uint32(0), mgr.curBlobStats.batchIndex)

		mgr.updateDegradeInfo(ctx, 3, codemode.EC3P3, genBidsMissed())
		require.Equal(t, 1, len(mgr.stats.blobCounter))
		require.Equal(t, 1, len(mgr.stats.volCounter))
		require.Equal(t, uint32(1), mgr.curBlobStats.batchIndex)
	}
	// service restart
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(genVolumeDegradeStats(3, 3), nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetVolumeDegradeStats(any, any).AnyTimes().Return(nil)

		err := mgr.initialize()
		require.Nil(t, err)
		require.Equal(t, uint32(3), mgr.stats.batchSize)
		require.Equal(t, 3, len(mgr.stats.blobCounter))
		require.Equal(t, 3, len(mgr.stats.volCounter))

		mgr.updateDegradeInfo(ctx, 3000, codemode.EC3P3, genBidsMissed())
		mgr.updateDegradeInfo(ctx, 3001, codemode.EC3P3, genBidsMissed())
		mgr.updateDegradeInfo(ctx, 3002, codemode.EC3P3, genBidsMissed())
		require.Equal(t, 3, len(mgr.stats.blobCounter))
		require.Equal(t, 3, len(mgr.stats.volCounter))

		mgr.updateDegradeInfo(ctx, 30003, codemode.EC3P3, genBidsMissed())
		require.Equal(t, 4, len(mgr.stats.blobCounter))
		require.Equal(t, 4, len(mgr.stats.volCounter))
	}
	// service restart with new batchSize
	{
		mgr := newDefaultDegradeStats(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeDegradeStats(any).AnyTimes().Return(genVolumeDegradeStats(10, 3), nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteVolumeDegradeStats(any).AnyTimes().Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetVolumeDegradeStats(any, any).AnyTimes().Return(nil)

		err := mgr.initialize()
		require.Nil(t, err)
		require.Equal(t, uint32(3), mgr.stats.batchSize)
		require.Equal(t, 0, len(mgr.stats.blobCounter))
		require.Equal(t, 0, len(mgr.stats.volCounter))

		mgr.updateDegradeInfo(ctx, 3000, codemode.EC3P3, genBidsMissed())
		mgr.updateDegradeInfo(ctx, 3001, codemode.EC3P3, genBidsMissed())
		mgr.updateDegradeInfo(ctx, 3002, codemode.EC3P3, genBidsMissed())
		require.Equal(t, 0, len(mgr.stats.blobCounter))
		require.Equal(t, 0, len(mgr.stats.volCounter))

		mgr.updateDegradeInfo(ctx, 30003, codemode.EC3P3, genBidsMissed())
		require.Equal(t, 1, len(mgr.stats.blobCounter))
		require.Equal(t, 1, len(mgr.stats.volCounter))
	}
}

func TestDegradeStatsKVSize(t *testing.T) {
	volumeTotalCount := uint32(2000000)
	batchSize := uint32(10000)
	numBatches := volumeTotalCount / batchSize

	buildBatches := func() []proto.VolumeDegradeBatch {
		batches := make([]proto.VolumeDegradeBatch, 0, numBatches)
		for i := uint32(0); i < numBatches; i++ {
			var cm codemode.CodeMode
			if i < numBatches/2 {
				cm = codemode.EC15P12
			} else {
				cm = codemode.EC12P9
			}
			totalShard := cm.Tactic().N + cm.Tactic().M

			degradeStats := make([]proto.VolumeDegradeLevel, totalShard)
			for j := 1; j <= totalShard; j++ {
				degradeStats[j-1] = proto.VolumeDegradeLevel{
					Level: j,
					Count: math.MaxUint32,
				}
			}

			batches = append(batches, proto.VolumeDegradeBatch{
				BatchIndex: i,
				CodeModeStats: []proto.VolumeDegradeCodeMode{
					{
						Mode:         cm,
						DegradeStats: degradeStats,
					},
				},
			})
		}
		return batches
	}

	blobBatchs := buildBatches()
	volBatchs := buildBatches()

	stats := &proto.VolumeDegradeStats{
		BatchSize: batchSize,
		BlobStats: blobBatchs,
		VolStats:  volBatchs,
	}

	statsBytes, err := json.Marshal(stats)
	require.NoError(t, err)
	require.LessOrEqual(t, len(statsBytes), 1024*1024)
}
