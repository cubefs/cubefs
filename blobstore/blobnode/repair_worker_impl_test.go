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

	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func TestGenTasklets(t *testing.T) {
	mode := codemode.EC6P10L2
	replicas, _ := genMockVol(1, codemode.CodeMode(mode))
	badi := 0
	taskInfo := proto.VolRepairTask{
		TaskID:      "mock_task_id",
		CodeMode:    codemode.CodeMode(mode),
		Sources:     replicas,
		Destination: replicas[badi],
		BadIdx:      uint8(badi),
	}
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12}
	bidsMap := make(map[proto.BlobID]int64)
	for idx := range bids {
		bidsMap[bids[idx]] = sizes[idx]
	}

	workutils.BigBufPool = workutils.NewByteBufferPool(2*1024, 10)
	getter := NewMockGetterWithBids(replicas, codemode.CodeMode(mode), bids, sizes)
	w := NewRepairWorker(VolRepairTaskEx{taskInfo: &taskInfo, blobNodeCli: getter, downloadShardConcurrency: 1})
	tasklets, _ := w.GenTasklets(context.Background())
	t.Logf("tasklets %+v", tasklets)
	// require.Equal(t, nil, err)
	require.Equal(t, 0, len(tasklets))

	getter.setFail(replicas[badi].Vuid, errors.New("fake error"))
	_, err := w.GenTasklets(context.Background())
	require.Equal(t, DstErr, err.errType)
	getter.setWell(replicas[badi].Vuid)
	shards, _ := getter.ListShards(context.Background(), replicas[badi])
	// todo  this code can delete
	for _, shard := range shards {
		getter.MarkDelete(context.Background(), replicas[badi].Vuid, shard.Bid)
	}
	tasklets, _ = w.GenTasklets(context.Background())
	require.Equal(t, 3, len(tasklets))
	bids2 := []*ShardInfoSimple{}
	for _, tasklet := range tasklets {
		var size int64 = 0
		for _, bid := range tasklet.bids {
			size += bid.Size
		}
		bids2 = append(bids2, tasklet.bids...)
		require.LessOrEqual(t, size, int64(workutils.BigBufPool.GetBufSize()))
	}

	for _, bid := range bids2 {
		require.Equal(t, bidsMap[bid.Bid], bid.Size)
	}

	workutils.BigBufPool = nil
	require.Panics(t, func() {
		_, err := w.GenTasklets(context.Background())
		require.Nil(t, err)
	})
}

func TestExecTasklet(t *testing.T) {
	mode := codemode.EC6P10L2
	replicas, _ := genMockVol(1, codemode.CodeMode(mode))
	badi := 0
	taskInfo := proto.VolRepairTask{
		TaskID:      "mock_task_id",
		CodeMode:    codemode.CodeMode(mode),
		Sources:     replicas,
		BadIdx:      uint8(badi),
		Destination: replicas[badi],
	}
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12}
	crcMap := make(map[proto.BlobID]uint32)

	workutils.BigBufPool = workutils.NewByteBufferPool(2*1024, 500)
	getter := NewMockGetterWithBids(replicas, codemode.CodeMode(mode), bids, sizes)
	w := NewRepairWorker(VolRepairTaskEx{taskInfo: &taskInfo, blobNodeCli: getter, downloadShardConcurrency: 1})

	shards, _ := getter.ListShards(context.Background(), replicas[badi])

	for _, shard := range shards {
		crcMap[shard.Bid] = shard.Crc
		getter.MarkDelete(context.Background(), replicas[badi].Vuid, shard.Bid)
	}
	tasklets, _ := w.GenTasklets(context.Background())
	require.Equal(t, 3, len(tasklets))

	for _, tasklet := range tasklets {
		werr := w.ExecTasklet(context.Background(), tasklet)
		t.Logf("recover tasklet.bids len %d, werr %+v", len(tasklet.bids), werr)
	}

	for _, shard := range shards {
		_, crc, err := getter.GetShard(context.Background(), replicas[badi], shard.Bid)
		require.NoError(t, err)
		require.Equal(t, crc, crcMap[shard.Bid])
	}
}

func TestCheck(t *testing.T) {
	mode := codemode.EC6P10L2
	replicas, _ := genMockVol(1, codemode.CodeMode(mode))
	badi := 0
	taskInfo := proto.VolRepairTask{
		TaskID:      "mock_task_id",
		CodeMode:    codemode.CodeMode(mode),
		Sources:     replicas,
		BadIdx:      uint8(badi),
		Destination: replicas[badi],
	}
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12}
	bidsMap := make(map[proto.BlobID]int64)
	for idx := range bids {
		bidsMap[bids[idx]] = sizes[idx]
	}

	workutils.BigBufPool = workutils.NewByteBufferPool(2*1024, 10)
	getter := NewMockGetterWithBids(replicas, codemode.CodeMode(mode), bids, sizes)
	w := NewRepairWorker(VolRepairTaskEx{taskInfo: &taskInfo, blobNodeCli: getter, downloadShardConcurrency: 1})

	repairWorker := w.(*RepairWorker)
	baseBids := []*ShardInfoSimple{}
	for idx := range bids {
		baseBids = append(baseBids, &ShardInfoSimple{Bid: bids[idx], Size: sizes[idx]})
	}
	repairWorker.benchmarkBids = baseBids
	werr := w.Check(context.Background())
	if werr != nil {
		require.NoError(t, werr.err)
	}

	repairWorker.benchmarkBids = append(repairWorker.benchmarkBids, &ShardInfoSimple{Bid: 100, Size: 100})
	werr = w.Check(context.Background())
	if werr != nil {
		require.EqualError(t, ErrBidMissing, werr.err.Error())
	}

	baseBids[0].Size++
	repairWorker.benchmarkBids = baseBids
	werr = w.Check(context.Background())
	if werr != nil {
		require.EqualError(t, ErrBidNotMatch, werr.err.Error())
	}
}

func TestRepairArgs(t *testing.T) {
	mode := codemode.EC6P10L2
	replicas, _ := genMockVol(1, codemode.CodeMode(mode))
	badi := 0
	taskInfo := proto.VolRepairTask{
		TaskID:      "mock_task_id",
		CodeMode:    codemode.CodeMode(mode),
		Sources:     replicas,
		Destination: replicas[badi],
	}
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12}

	workutils.BigBufPool = workutils.NewByteBufferPool(2*1024, 10)
	getter := NewMockGetterWithBids(replicas, codemode.CodeMode(mode), bids, sizes)
	w := NewRepairWorker(VolRepairTaskEx{taskInfo: &taskInfo, blobNodeCli: getter, downloadShardConcurrency: 1})

	taskID, taskType, src, dest := w.ReclaimArgs()
	require.Equal(t, taskInfo.TaskID, taskID)
	require.Equal(t, proto.RepairTaskType, taskType)
	require.Equal(t, taskInfo.Sources, src)
	require.Equal(t, taskInfo.Destination, dest)

	taskID, taskType, src, dest = w.CompleteArgs()
	require.Equal(t, taskInfo.TaskID, taskID)
	require.Equal(t, proto.RepairTaskType, taskType)
	require.Equal(t, taskInfo.Sources, src)
	require.Equal(t, taskInfo.Destination, dest)

	taskID, taskType, src, dest = w.CancelArgs()
	require.Equal(t, taskInfo.TaskID, taskID)
	require.Equal(t, proto.RepairTaskType, taskType)
	require.Equal(t, taskInfo.Sources, src)
	require.Equal(t, taskInfo.Destination, dest)
}
