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
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func bidsEqual3(t *testing.T, bids1 map[proto.BlobID]*client.ShardInfo, bids2 []proto.BlobID, size2 []int64) {
	var bidsSimple []*ShardInfoSimple
	for _, bid := range bids1 {
		bidsSimple = append(bidsSimple, &ShardInfoSimple{Bid: bid.Bid, Size: bid.Size})
	}
	bidsEqual(t, bidsSimple, bids2, size2)
}

func bidsEqual2(t *testing.T, bids1 []*ShardInfoWithCrc, bids2 []proto.BlobID, size2 []int64) {
	var bidsSimple []*ShardInfoSimple
	for _, bid := range bids1 {
		bidsSimple = append(bidsSimple, &ShardInfoSimple{Bid: bid.Bid, Size: bid.Size})
	}
	bidsEqual(t, bidsSimple, bids2, size2)
}

func bidsEqual(t *testing.T, shardMetas []*ShardInfoSimple, bids []proto.BlobID, size2 []int64) {
	require.Equal(t, len(bids), len(shardMetas))
	m := make(map[proto.BlobID]int64)
	for _, bidInfo := range shardMetas {
		m[bidInfo.Bid] = bidInfo.Size
	}

	for idx := range bids {
		bid := bids[idx]
		size := size2[idx]
		size2, ok := m[bid]
		require.True(t, ok)
		require.Equal(t, size, size2)
	}
}

func TestGetSingleVunitNormalBids(t *testing.T) {
	testWithAllMode(t, testGetSingleVunitNormalBids)
}

func testGetSingleVunitNormalBids(t *testing.T, mode codemode.CodeMode) {
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{10, 1024, 1024, 1024, 1024, 1024, 1024}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	retBids, err := GetSingleVunitNormalBids(context.Background(), getter, replicas[0])
	require.NoError(t, err)
	bidsEqual2(t, retBids, bids, sizes)

	// test bid mark delete
	getter.MarkDelete(context.Background(), replicas[0].Vuid, 1)
	retBids, err = GetSingleVunitNormalBids(context.Background(), getter, replicas[0])
	require.NoError(t, err)
	bids2 := []proto.BlobID{2, 3, 4, 5, 6, 7}
	sizes2 := []int64{1024, 1024, 1024, 1024, 1024, 1024}
	bidsEqual2(t, retBids, bids2, sizes2)

	// test bid delete
	getter.Delete(context.Background(), replicas[0].Vuid, 2)
	retBids, err = GetSingleVunitNormalBids(context.Background(), getter, replicas[0])
	require.NoError(t, err)
	bids3 := []proto.BlobID{3, 4, 5, 6, 7}
	sizes3 := []int64{1024, 1024, 1024, 1024, 1024}
	bidsEqual2(t, retBids, bids3, sizes3)
}

func TestGetReplicasBids(t *testing.T) {
	testWithAllMode(t, testGetReplicasBids)
}

func testGetReplicasBids(t *testing.T, mode codemode.CodeMode) {
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{10, 1024, 1024, 1024, 1024, 1024, 1024}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	ret := GetReplicasBids(context.Background(), getter, replicas)
	require.Equal(t, len(replicas), len(ret))
	for _, replica := range replicas {
		shardsInfo := ret[replica.Vuid].Bids
		require.NoError(t, ret[replica.Vuid].RetErr)
		bidsEqual3(t, shardsInfo, bids, sizes)
	}

	badVuid := replicas[0].Vuid
	setFailerr := errors.New("fake error")
	getter.setFail(badVuid, setFailerr)
	ret1 := GetReplicasBids(context.Background(), getter, replicas)
	require.Equal(t, len(replicas), len(ret1))
	for _, replica := range replicas {
		if replica.Vuid == badVuid {
			require.EqualError(t, setFailerr, ret1[replica.Vuid].RetErr.Error())
		} else {
			shardsInfo := ret1[replica.Vuid].Bids
			require.NoError(t, ret1[replica.Vuid].RetErr)
			bidsEqual3(t, shardsInfo, bids, sizes)
		}
	}
}

func TestMergeBids(t *testing.T) {
	testWithAllMode(t, testMergeBids)
}

func testMergeBids(t *testing.T, mode codemode.CodeMode) {
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{10, 1024, 1024, 1024, 1024, 1024, 1024}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)

	setFailerr := errors.New("fake error")
	badVuid := replicas[0].Vuid
	remainVuid := replicas[1].Vuid
	getter.setFail(badVuid, setFailerr)
	for _, replica := range replicas {
		if replica.Vuid != remainVuid {
			getter.Delete(context.Background(), replica.Vuid, 1)
		}
	}
	ret := GetReplicasBids(context.Background(), getter, replicas)
	bidIfos := MergeBids(ret)
	bidsEqual(t, bidIfos, bids, sizes)

	getter.Delete(context.Background(), remainVuid, 1)
	ret1 := GetReplicasBids(context.Background(), getter, replicas)
	bidIfos1 := MergeBids(ret1)
	bids1 := []proto.BlobID{2, 3, 4, 5, 6, 7}
	sizes1 := []int64{1024, 1024, 1024, 1024, 1024, 1024}
	bidsEqual(t, bidIfos1, bids1, sizes1)
}

func TestGetBenchmarkBids(t *testing.T) {
	testWithAllMode(t, testGetBenchmarkBids)
}

func testGetBenchmarkBids(t *testing.T, mode codemode.CodeMode) {
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{10, 1024, 1024, 1024, 1024, 1024, 1024}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	benchmarkBids, err := GetBenchmarkBids(context.Background(), getter, replicas, mode, []uint8{})
	require.NoError(t, err)
	bidsEqual(t, benchmarkBids, bids, sizes)

	getter.Delete(context.Background(), replicas[0].Vuid, 1)
	benchmarkBids, err = GetBenchmarkBids(context.Background(), getter, replicas, mode, []uint8{})
	require.NoError(t, err)
	bidsEqual(t, benchmarkBids, bids, sizes)

	getter.MarkDelete(context.Background(), replicas[1].Vuid, 1)
	benchmarkBids, err = GetBenchmarkBids(context.Background(), getter, replicas, mode, []uint8{})
	require.NoError(t, err)
	bids1 := []proto.BlobID{2, 3, 4, 5, 6, 7}
	sizes1 := []int64{1024, 1024, 1024, 1024, 1024, 1024}
	bidsEqual(t, benchmarkBids, bids1, sizes1)

	codeInfo := mode.Tactic()
	n := codeInfo.N
	m := codeInfo.M

	for idx := 0; idx < m+1; idx++ {
		replica := replicas[idx]
		getter.Delete(context.Background(), replica.Vuid, 2)
	}
	benchmarkBids, err = GetBenchmarkBids(context.Background(), getter, replicas, mode, []uint8{})
	require.NoError(t, err)
	bids2 := []proto.BlobID{3, 4, 5, 6, 7}
	sizes2 := []int64{1024, 1024, 1024, 1024, 1024}
	bidsEqual(t, benchmarkBids, bids2, sizes2)

	// test broken many
	allowFailCnt := n + m - codeInfo.PutQuorum
	minWellReplicasCnt := n + allowFailCnt
	globalIdxs, n, m := workutils.GlobalStripe(mode)
	brokenReplicasCnt := m + n - (minWellReplicasCnt) + 1
	if brokenReplicasCnt <= 0 {
		return
	}

	for idx := 0; idx < brokenReplicasCnt; idx++ {
		brokenIdx := globalIdxs[idx]
		replica := replicas[brokenIdx]
		getter.setFail(replica.Vuid, errors.New("fake error"))
	}

	_, err = GetBenchmarkBids(context.Background(), getter, replicas, mode, []uint8{})
	require.Error(t, err)
	require.EqualError(t, ErrNotEnoughWellReplicaCnt, err.Error())
}

func TestBidsSplit(t *testing.T) {
	tasklets := BidsSplit(context.Background(), []*ShardInfoSimple{}, 1024)
	require.Equal(t, 0, len(tasklets))

	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{0, 1024, 1024, 512, 512, 0, 512}
	var bidInfos []*ShardInfoSimple
	bidsMap := make(map[proto.BlobID]int64)
	for idx := range bids {
		bidsMap[bids[idx]] = sizes[idx]
	}

	for idx := range bids {
		bidInfos = append(bidInfos, &ShardInfoSimple{Bid: bids[idx], Size: sizes[idx]})
	}
	tasklets = BidsSplit(context.Background(), bidInfos, 1024)
	require.Equal(t, 4, len(tasklets))

	bids2 := []*ShardInfoSimple{}
	for _, tasklet := range tasklets {
		var size int64 = 0
		for _, bid := range tasklet.bids {
			size += bid.Size
		}
		bids2 = append(bids2, tasklet.bids...)
		require.LessOrEqual(t, size, int64(1024))
	}

	for _, bid := range bids2 {
		require.Equal(t, bidsMap[bid.Bid], bid.Size)
	}
}
