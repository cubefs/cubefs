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
	"fmt"
	"sort"
	"sync"

	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// duties
// 1.get chunk shards meta list by assign chunk
// 2.get benchmark bids of volume(not allowed tp be miss any bid which has written success by user,
// but the rubbish bid which can recover can be allowed to be included in benchmark bids)

var (
	ErrNotEnoughWellReplicaCnt = errors.New("well locations cnt is not enough")
	ErrNotEnoughBidsInTasklet  = errors.New("check len of tasklet and bids is not equal")
	ErrTaskletSizeInvalid      = errors.New("tasklet size is invalid")
	ErrBidSizeOverTaskletSize  = errors.New("bid size is over tasklet size")
	ErrUnexpected              = errors.New("unexpected error when get bench bids")
)

// ShardInfoSimple with blob id and size
type ShardInfoSimple struct {
	Bid  proto.BlobID
	Size int64
}

// ShardInfoWithCrc with blob id and size and crc
type ShardInfoWithCrc struct {
	Bid   proto.BlobID
	Size  int64
	Crc32 uint32
}

// GetSingleVunitNormalBids returns single volume unit bids info
func GetSingleVunitNormalBids(ctx context.Context, cli client.IBlobNode, replica proto.VunitLocation) (bids []*ShardInfoWithCrc, err error) {
	shards, err := cli.ListShards(ctx, replica)
	if err != nil {
		return nil, err
	}
	for _, shardInfo := range shards {
		if !shardInfo.Normal() {
			continue
		}

		bidInfo := ShardInfoWithCrc{
			Bid:   shardInfo.Bid,
			Size:  shardInfo.Size,
			Crc32: shardInfo.Crc,
		}
		bids = append(bids, &bidInfo)
	}
	return bids, nil
}

// ReplicaBidsRet with bids info and error message
type ReplicaBidsRet struct {
	RetErr error
	Bids   map[proto.BlobID]*client.ShardInfo
}

// GetReplicasBids returns locations bids info
func GetReplicasBids(ctx context.Context, cli client.IBlobNode, replicas VunitLocations) map[proto.Vuid]*ReplicaBidsRet {
	result := make(map[proto.Vuid]*ReplicaBidsRet)
	wg := sync.WaitGroup{}
	var mu sync.Mutex
	for idx := range replicas {
		replica := replicas[idx]
		vuid := replica.Vuid
		wg.Add(1)
		_, tmpCtx := trace.StartSpanFromContext(ctx, "ListShard")

		go func() {
			defer wg.Done()
			bids, err := cli.ListShards(tmpCtx, replica)
			bidMap := make(map[proto.BlobID]*client.ShardInfo, len(bids))
			for _, bid := range bids {
				bidMap[bid.Bid] = bid
			}
			mu.Lock()
			defer mu.Unlock()
			result[vuid] = &ReplicaBidsRet{RetErr: err, Bids: bidMap}
		}()
	}
	wg.Wait()
	return result
}

// MergeBids merge bids
func MergeBids(replicasBids map[proto.Vuid]*ReplicaBidsRet) []*ShardInfoSimple {
	allBidsMap := make(map[proto.BlobID]*client.ShardInfo)
	for _, info := range replicasBids {
		if info.RetErr == nil {
			for _, bidInfo := range info.Bids {
				allBidsMap[bidInfo.Bid] = bidInfo
			}
		}
	}

	var allBidsList []*ShardInfoSimple
	for _, bid := range allBidsMap {
		bidInfo := ShardInfoSimple{Bid: bid.Bid, Size: bid.Size}
		allBidsList = append(allBidsList, &bidInfo)
	}
	return allBidsList
}

// GetRecoverableBids returns bench mark bids
func GetRecoverableBids(ctx context.Context, cli client.IBlobNode, replicas VunitLocations,
	mode codemode.CodeMode, badIdxs []uint8) (bidInfos []*ShardInfoSimple, err error) {
	span := trace.SpanFromContextSafe(ctx)

	globalReplicas := replicas.IntactGlobalSet(mode, badIdxs)
	replicasBids := GetReplicasBids(ctx, cli, globalReplicas)

	wellCnt := 0
	for _, replBids := range replicasBids {
		if replBids.RetErr == nil {
			wellCnt++
		}
	}
	if wellCnt < minWellReplicasCnt(mode) {
		span.Errorf("well locations cnt is not enough: wellCnt[%d], minWellReplicasCnt[%d]",
			wellCnt, minWellReplicasCnt(mode))
		return nil, ErrNotEnoughWellReplicaCnt
	}

	allBidsList := MergeBids(replicasBids)
	for _, bidInfo := range allBidsList {
		markDel := false
		existStatus := workutils.NewBidExistStatus(mode)
		notExistCnt := 0
		for vuid, replBids := range replicasBids {
			if replBids.RetErr != nil {
				continue
			}

			info, ok := replBids.Bids[bidInfo.Bid]
			if !ok {
				notExistCnt++
				continue
			}
			if info.MarkDeleted() {
				markDel = true
				break
			}

			existStatus.Exist(vuid.Index())
		}

		if markDel {
			workutils.DroppedBidRecorderInst().Write(
				ctx,
				replicas[0].Vuid.Vid(),
				bidInfo.Bid,
				"mark deleted",
			)
			continue
		}

		if existStatus.CanRecover() {
			bidInfos = append(bidInfos, bidInfo)
			continue
		}

		if notExistCnt > allowFailCnt(mode) {
			workutils.DroppedBidRecorderInst().Write(
				ctx,
				replicas[0].Vuid.Vid(),
				bidInfo.Bid,
				fmt.Sprintf("can't recover:notExist %d exist %d", notExistCnt, existStatus.ExistCnt()),
			)
			continue
		}

		span.Errorf("unexpect when get benchmark bids: vid[%d], bid[%d], existCnt[%d], notExistCnt[%d], allowFailCnt[%d]",
			replicas[0].Vuid.Vid(), bidInfo.Bid, existStatus.ExistCnt(), notExistCnt, allowFailCnt(mode))
		return nil, ErrUnexpected
	}

	return bidInfos, nil
}

// minWellReplicasCnt:It is the mini count of well locations which can determine
// whether a bid is repaired or discarded
func minWellReplicasCnt(mode codemode.CodeMode) int {
	return mode.Tactic().N + allowFailCnt(mode)
}

func allowFailCnt(mode codemode.CodeMode) int {
	modeInfo := mode.Tactic()
	return modeInfo.N + modeInfo.M - modeInfo.PutQuorum
}

// BidsSplit split bids list to many tasklets by taskletSize
func BidsSplit(ctx context.Context, bids []*ShardInfoSimple, taskletSize int) ([]Tasklet, *WorkError) {
	span := trace.SpanFromContextSafe(ctx)

	if len(bids) == 0 {
		return []Tasklet{}, nil
	}
	if taskletSize == 0 {
		span.Errorf("BidsSplit taskletSize size can not zero")
		return nil, OtherError(ErrTaskletSizeInvalid)
	}

	sortByBid(bids)
	var tasks []Tasklet
	var taskShardDataSize uint64 = 0
	task := Tasklet{}
	for _, bid := range bids {
		if bid.Size > int64(taskletSize) {
			span.Errorf("bid size is too big: bid[%d], bid size[%d], tasklet size[%d]", bid.Bid, bid.Size, taskletSize)
			return nil, OtherError(ErrBidSizeOverTaskletSize)
		}
		if taskShardDataSize+uint64(bid.Size) > uint64(taskletSize) {
			tasks = append(tasks, task)
			task = Tasklet{}
			taskShardDataSize = 0
		}
		taskShardDataSize += uint64(bid.Size)
		task.bids = append(task.bids, bid)
	}
	tasks = append(tasks, task)
	span.Debugf("generate %d tasks from bids[%v]", len(tasks), bids)

	return tasks, nil
}

func sortByBid(bids []*ShardInfoSimple) {
	sort.Slice(bids, func(i, j int) bool {
		return bids[i].Bid < bids[j].Bid
	})
}

// GetBids returns bids
func GetBids(shardMetas []*ShardInfoSimple) []proto.BlobID {
	bids := make([]proto.BlobID, len(shardMetas))
	for i, info := range shardMetas {
		bids[i] = info.Bid
	}
	return bids
}
