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

package blobnode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func TestMigrateGenTasklets(t *testing.T) {
	mode := codemode.EC6P10L2
	replicas := genMockVol(100, codemode.CodeMode(mode))
	badi := 10
	godi := 11
	balanceTask := &proto.MigrateTask{
		TaskID:      "mock_balance_task_id",
		TaskType:    proto.TaskTypeBalance,
		CodeMode:    codemode.CodeMode(mode),
		Sources:     replicas,
		Destination: replicas[badi],
		SourceVuid:  replicas[badi].Vuid,
	}
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12, 50, 100, 2047}
	markDeleteBidIndex := 9
	bidsMap := make(map[proto.BlobID]int64, len(bids))
	for idx := range bids {
		bidsMap[bids[idx]] = sizes[idx]
	}

	workutils.TaskBufPool = workutils.NewBufPool(&workutils.BufConfig{
		MigrateBufSize:     2 * 1024,
		MigrateBufCapacity: 10,
		RepairBufSize:      1,
		RepairBufCapacity:  1,
	})
	getter := NewMockGetterWithBids(replicas, codemode.CodeMode(mode), bids, sizes)
	w := NewMigrateWorker(MigrateTaskEx{taskInfo: balanceTask, blobNodeCli: getter, downloadShardConcurrency: 1})

	tasklets, _ := w.GenTasklets(context.Background())
	require.Equal(t, 0, len(tasklets))

	getter.setFail(replicas[badi].Vuid, errors.New("fake error"))
	_, err := w.GenTasklets(context.Background())
	require.Equal(t, DstErr, err.errType)

	{
		getter.setWell(replicas[badi].Vuid)
		shards, _ := getter.ListShards(context.Background(), replicas[badi])
		for _, shard := range shards {
			getter.Delete(context.Background(), balanceTask.SourceVuid, shard.Bid)
		}
		tasklets, _ = w.GenTasklets(context.Background())
		require.Equal(t, 4, len(tasklets))
		var bids []*ShardInfoSimple
		for _, tasklet := range tasklets {
			var size int64 = 0
			for _, bid := range tasklet.bids {
				size += bid.Size
			}
			bids = append(bids, tasklet.bids...)
			require.LessOrEqual(t, size, int64(workutils.TaskBufPool.GetMigrateBufSize()))
		}

		for _, bid := range bids {
			require.Equal(t, bidsMap[bid.Bid], bid.Size)
		}
	}

	{
		getter.MarkDelete(context.Background(), replicas[godi].Vuid, bids[markDeleteBidIndex])
		tasklets, _ = w.GenTasklets(context.Background())
		require.Equal(t, 3, len(tasklets))
		var bids []*ShardInfoSimple
		for _, tasklet := range tasklets {
			var size int64 = 0
			for _, bid := range tasklet.bids {
				size += bid.Size
			}
			bids = append(bids, tasklet.bids...)
			require.LessOrEqual(t, size, int64(workutils.TaskBufPool.GetMigrateBufSize()))
		}

		for _, bid := range bids {
			require.Equal(t, bidsMap[bid.Bid], bid.Size)
		}
	}

	{
		getter.Delete(context.Background(), replicas[godi].Vuid, bids[markDeleteBidIndex])
		tasklets, _ = w.GenTasklets(context.Background())
		require.Equal(t, 4, len(tasklets))
		var bids []*ShardInfoSimple
		for _, tasklet := range tasklets {
			var size int64 = 0
			for _, bid := range tasklet.bids {
				size += bid.Size
			}
			bids = append(bids, tasklet.bids...)
			require.LessOrEqual(t, size, int64(workutils.TaskBufPool.GetMigrateBufSize()))
		}

		for _, bid := range bids {
			require.Equal(t, bidsMap[bid.Bid], bid.Size)
		}
	}
	{
		// test broken many
		codeInfo := mode.Tactic()
		n := codeInfo.N
		m := codeInfo.M
		allowFailCnt := n + m - codeInfo.PutQuorum
		minWellReplicasCnt := n + allowFailCnt
		globalIdxs, n, m := mode.T().GlobalStripe()
		brokenReplicasCnt := m + n - (minWellReplicasCnt) + 1
		if brokenReplicasCnt <= 0 {
			return
		}

		for idx := 0; idx < brokenReplicasCnt; idx++ {
			brokenIdx := globalIdxs[idx]
			replica := replicas[brokenIdx]
			getter.setFail(replica.Vuid, errors.New("fake error"))
		}

		_, err = w.GenTasklets(context.Background())
		if err != nil {
			require.EqualError(t, err.err, ErrNotEnoughWellReplicaCnt.Error())
		}
	}
	{
		for index, replica := range replicas {
			if index < balanceTask.CodeMode.Tactic().PutQuorum {
				getter.setVunitStatus(replica.Vuid, api.ChunkStatusNormal)
			}
		}
		_, err = w.GenTasklets(context.Background())
		if err != nil {
			require.EqualError(t, err.err, ErrNotReadyForMigrate.Error())
		}
	}
}

func TestMigrateExecTasklet(t *testing.T) {
	mode := codemode.EC16P20L2
	replicas := genMockVol(100, codemode.CodeMode(mode))
	badi := 10
	diskDropTask := &proto.MigrateTask{
		TaskID:      "mock_disk_drop_task_id",
		TaskType:    proto.TaskTypeDiskDrop,
		CodeMode:    codemode.CodeMode(mode),
		Sources:     replicas,
		Destination: replicas[badi],
		SourceVuid:  replicas[badi].Vuid,
	}
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12, 50, 100, 2047}
	crcMap := make(map[proto.BlobID]uint32)

	workutils.TaskBufPool = workutils.NewBufPool(&workutils.BufConfig{
		MigrateBufSize:     2 * 1024,
		MigrateBufCapacity: 100,
		RepairBufSize:      1,
		RepairBufCapacity:  1,
	})
	getter := NewMockGetterWithBids(replicas, codemode.CodeMode(mode), bids, sizes)
	w := NewMigrateWorker(MigrateTaskEx{taskInfo: diskDropTask, blobNodeCli: getter, downloadShardConcurrency: 1})

	{
		shards, _ := getter.ListShards(context.Background(), replicas[badi])

		for _, shard := range shards {
			crcMap[shard.Bid] = shard.Crc
			getter.Delete(context.Background(), replicas[badi].Vuid, shard.Bid)
		}
		tasklets, _ := w.GenTasklets(context.Background())
		require.Equal(t, 4, len(tasklets))

		for _, tasklet := range tasklets {
			t.Log(w.ExecTasklet(context.Background(), tasklet))
		}

		for _, shard := range shards {
			_, crc, err := getter.GetShard(context.Background(), replicas[badi], shard.Bid, api.InspectIO)
			require.NoError(t, err)
			require.Equal(t, crc, crcMap[shard.Bid])
		}
	}
	{
		shards, _ := getter.ListShards(context.Background(), replicas[badi])

		for index, shard := range shards {
			crcMap[shard.Bid] = shard.Crc
			if index%2 == 0 {
				getter.Delete(context.Background(), replicas[badi].Vuid, shard.Bid)
			}
		}
		tasklets, _ := w.GenTasklets(context.Background())
		require.LessOrEqual(t, len(tasklets), 4)

		for _, tasklet := range tasklets {
			err := w.ExecTasklet(context.Background(), tasklet)
			require.Nil(t, err)
		}

		for _, shard := range shards {
			_, crc, err := getter.GetShard(context.Background(), replicas[badi], shard.Bid, api.InspectIO)
			require.NoError(t, err)
			require.Equal(t, crc, crcMap[shard.Bid])
		}
	}
	workutils.TaskBufPool = nil
	_, err := w.GenTasklets(context.Background())
	require.Error(t, err)
}

func TestMigrateCheck(t *testing.T) {
	mode := codemode.EC16P20L2
	replicas := genMockVol(100, codemode.CodeMode(mode))
	badi := 10
	diskDropTask := &proto.MigrateTask{
		TaskID:      "mock_disk_drop_task_id",
		TaskType:    proto.TaskTypeDiskDrop,
		CodeMode:    codemode.CodeMode(mode),
		Sources:     replicas,
		Destination: replicas[badi],
		SourceVuid:  replicas[badi].Vuid,
	}
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12, 50, 100, 2047}
	crcMap := make(map[proto.BlobID]uint32)

	workutils.TaskBufPool = workutils.NewBufPool(&workutils.BufConfig{
		MigrateBufSize:     2 * 1024,
		MigrateBufCapacity: 100,
		RepairBufSize:      1,
		RepairBufCapacity:  1,
	})
	getter := NewMockGetterWithBids(replicas, codemode.CodeMode(mode), bids, sizes)
	w := NewMigrateWorker(MigrateTaskEx{taskInfo: diskDropTask, blobNodeCli: getter, downloadShardConcurrency: 1})

	shards, _ := getter.ListShards(context.Background(), replicas[badi])

	for _, shard := range shards {
		crcMap[shard.Bid] = shard.Crc
		getter.Delete(context.Background(), replicas[badi].Vuid, shard.Bid)
	}
	tasklets, werr := w.GenTasklets(context.Background())
	if werr != nil {
		require.NoError(t, werr.err)
	}
	require.Equal(t, 4, len(tasklets))

	for _, tasklet := range tasklets {
		werr = w.ExecTasklet(context.Background(), tasklet)
		if werr != nil {
			require.NoError(t, werr.err)
		}
	}

	werr = w.Check(context.Background())
	if werr != nil {
		require.NoError(t, werr.err)
	}
	migrateWorker := w.(*MigrateWorker)

	benchmarkBids := migrateWorker.benchmarkBids

	migrateWorker.benchmarkBids = append(benchmarkBids, &ShardInfoSimple{Bid: 1000, Size: 100})
	werr = w.Check(context.Background())
	if werr != nil {
		require.EqualError(t, ErrBidMissing, werr.err.Error())
	}

	migrateWorker.benchmarkBids = benchmarkBids
	migrateWorker.benchmarkBids[0].Size = 100000000
	werr = w.Check(context.Background())
	if werr != nil {
		require.EqualError(t, ErrBidNotMatch, werr.err.Error())
	}
}

func TestMigrateArgs(t *testing.T) {
	mode := codemode.EC15P12
	replicas := genMockVol(100, mode)
	badi := 15
	diskDropTask := &proto.MigrateTask{
		TaskID:      "mock_disk_drop_task_id",
		TaskType:    proto.TaskTypeDiskDrop,
		CodeMode:    codemode.CodeMode(mode),
		Sources:     replicas,
		Destination: replicas[badi],
		SourceVuid:  replicas[badi].Vuid,
	}
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12, 50, 100, 2047}

	getter := NewMockGetterWithBids(replicas, codemode.CodeMode(mode), bids, sizes)
	w := NewMigrateWorker(MigrateTaskEx{taskInfo: diskDropTask, blobNodeCli: getter, downloadShardConcurrency: 1})

	args := w.OperateArgs()
	require.Equal(t, diskDropTask.TaskID, args.TaskID)
	require.Equal(t, proto.TaskTypeDiskDrop, args.TaskType)
	require.Equal(t, diskDropTask.Sources, args.Src)
	require.Equal(t, diskDropTask.Destination, args.Dest)
}
