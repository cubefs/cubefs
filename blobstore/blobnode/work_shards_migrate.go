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
	"bytes"
	"context"

	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

var (
	// ErrBidMissing bid is missing
	ErrBidMissing = errors.New("bid is missing")
	// ErrBidNotMatch bid not match
	ErrBidNotMatch = errors.New("bid not match")
)

// GenMigrateBids generates migrate blob ids
func GenMigrateBids(ctx context.Context, blobnodeCli client.IBlobNode, srcReplicas []proto.VunitLocation,
	dst proto.VunitLocation, mode codemode.CodeMode, badIdxs []uint8,
) (migBids, benchmarkBids []*ShardInfoSimple, wErr *WorkError) {
	span := trace.SpanFromContextSafe(ctx)

	benchmarkBids, err := GetBenchmarkBids(ctx, blobnodeCli, srcReplicas, mode, badIdxs)
	if err != nil {
		span.Errorf("get benchmark bids failed: err[%v]", err)
		return nil, nil, SrcError(err)
	}
	span.Infof("get benchmark success: len[%d]", len(benchmarkBids))

	// get destination bids and check the meta info,if the bid is good,
	// which means we don’t need to migrate the corresponding bid
	destBids, err := GetSingleVunitNormalBids(ctx, blobnodeCli, dst)
	if err != nil {
		span.Errorf("get single vunit normal bids failed: dst[%+v], err[%+v]", dst, err)
		return nil, nil, DstError(err)
	}
	span.Infof("GetSingleVunitNormalBids success: destBids len[%d], idx[%d]", len(destBids), dst.Vuid.Index())

	existInDest := make(map[proto.BlobID]int64, len(destBids))
	for _, bid := range destBids {
		existInDest[bid.Bid] = bid.Size
	}

	for _, bid := range benchmarkBids {
		if size, ok := existInDest[bid.Bid]; ok && size == bid.Size {
			span.Debugf("benchmarkBids bid exist in dest: bid[%d]", bid.Bid)
			continue
		}
		span.Debugf("benchmarkBids append: bid[%d], size[%d]", bid.Bid, bid.Size)
		migBids = append(migBids, bid)
	}

	span.Infof("benchmarkBids: len[%d]", len(migBids))
	return migBids, benchmarkBids, nil
}

// MigrateBids migrate the bids data to destination
func MigrateBids(ctx context.Context, shardRecover *ShardRecover, badIdx uint8, destLocation proto.VunitLocation,
	direct bool, bids []*ShardInfoSimple, blobnodeCli client.IBlobNode) *WorkError {
	span := trace.SpanFromContextSafe(ctx)

	// step1 recover shards
	span.Infof("recover shard: len bids[%d]", len(bids))

	err := shardRecover.RecoverShards(ctx, []uint8{badIdx}, direct)
	if err != nil {
		return SrcError(err)
	}

	// put shards to dest
	span.Infof("put data to destination: dest[%+v]", destLocation)
	destIdx := destLocation.Vuid.Index()
	for _, bid := range bids {
		data, err := shardRecover.GetShard(destIdx, bid.Bid)
		if err != nil {
			return OtherError(err)
		}
		err = retry.Timed(3, 1000).On(func() error {
			return blobnodeCli.PutShard(ctx, destLocation, bid.Bid, bid.Size, bytes.NewReader(data), shardRecover.ioType)
		})
		if err != nil {
			return DstError(err)
		}
	}

	return nil
}

// CheckVunit checks volume unit info
func CheckVunit(ctx context.Context, expectBids []*ShardInfoSimple, dest proto.VunitLocation, blobnodeCli client.IBlobNode) *WorkError {
	span := trace.SpanFromContextSafe(ctx)

	// check dst shards
	destBids, err := GetSingleVunitNormalBids(ctx, blobnodeCli, dest)
	if err != nil {
		return DstError(err)
	}

	destBidsMap := make(map[proto.BlobID]*ShardInfoSimple, len(destBids))
	for _, bid := range destBids {
		info := ShardInfoSimple{Bid: bid.Bid, Size: bid.Size}
		destBidsMap[bid.Bid] = &info
	}

	for _, bid := range expectBids {
		info, ok := destBidsMap[bid.Bid]
		if !ok {
			span.Errorf("repair check destination failed: dest[%+v], bid[%d], err[%+v]", dest, bid.Bid, ErrBidMissing)
			return SrcError(ErrBidMissing)
		}
		if info.Size != bid.Size {
			span.Errorf("repair check failed: dest[%+v], bid[%d], size[%d], err[%+v]", dest, bid.Bid, bid.Size, ErrBidNotMatch)
			return SrcError(ErrBidNotMatch)
		}
	}
	return nil
}
