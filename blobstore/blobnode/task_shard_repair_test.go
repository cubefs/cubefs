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
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestContain(t *testing.T) {
	testCases := []struct {
		name   string
		array  []int
		ele    int
		expect bool
	}{
		{"1", []int{1}, 1, true},
		{"2", nil, 1, false},
		{"3", []int{2, 3}, 1, false},
		{"4", []int{1}, 0, false},
		{"5", []int{}, 1, false},
		{"6", []int{1, 2, 4}, 1, true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			result := contains(testCase.ele, testCase.array)
			require.Equal(t, result, testCase.expect)
		})
	}
}

func TestGetRepairShards(t *testing.T) {
	testWithAllMode(t, testGetRepairShards)
}

func getRepairShardsTest(ctx context.Context,
	repairer *ShardRepairer,
	replicas []proto.VunitLocation,
	mode codemode.CodeMode,
	bid proto.BlobID,
	badIdxs []int) (repairIdx []int, shardSize int64, err error) {
	shardInfos := repairer.listShardsInfo(ctx, replicas, bid)

	for idx, shard := range shardInfos {
		if !contains(idx, badIdxs) {
			continue
		}

		if shard.IsBad() {
			return repairIdx, shardSize, errcode.ErrDestReplicaBad
		}

	}
	// get repair shards:
	return getRepairShards(ctx, shardInfos, mode, badIdxs)
}

func testGetRepairShards(t *testing.T, mode codemode.CodeMode) {
	codeInfo := mode.Tactic()
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1}
	sizes := []int64{1025}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	repairer := NewShardRepairer(getter)
	badi := []int{0, 1}

	shouldRepair, shardSize, err := getRepairShardsTest(context.Background(), repairer, replicas, mode, 1, badi)
	require.NoError(t, err)
	require.Equal(t, int64(1025), shardSize)
	require.Equal(t, 0, len(shouldRepair))

	getter.Delete(context.Background(), replicas[0].Vuid, 1)

	shouldRepair, shardSize, err = getRepairShardsTest(context.Background(), repairer, replicas, mode, 1, badi)
	require.NoError(t, err)
	require.Equal(t, int64(1025), shardSize)
	require.Equal(t, 1, len(shouldRepair))
	require.Equal(t, true, repairIdxsEqual(shouldRepair, []int{0}))

	getter.Delete(context.Background(), replicas[1].Vuid, 1)
	shouldRepair, shardSize, err = getRepairShardsTest(context.Background(), repairer, replicas, mode, 1, badi)
	require.NoError(t, err)
	require.Equal(t, int64(1025), shardSize)
	require.Equal(t, 2, len(shouldRepair))
	require.Equal(t, true, repairIdxsEqual(shouldRepair, []int{0, 1}))

	getter.Delete(context.Background(), replicas[2].Vuid, 1)
	shouldRepair, shardSize, err = getRepairShardsTest(context.Background(), repairer, replicas, mode, 1, badi)
	require.NoError(t, err)
	require.Equal(t, int64(1025), shardSize)
	require.Equal(t, 2, len(shouldRepair))
	require.Equal(t, true, repairIdxsEqual(shouldRepair, []int{0, 1}))

	getter.setFail(replicas[0].Vuid, errors.New("fake error"))
	_, _, err = getRepairShardsTest(context.Background(), repairer, replicas, mode, 1, badi)
	require.Error(t, err)
	require.EqualError(t, errcode.ErrDestReplicaBad, err.Error())

	getter2 := NewMockGetterWithBids(replicas, mode, bids, sizes)
	repairer2 := NewShardRepairer(getter2)
	badi2 := []int{0, 1}
	for i := 2; i < 2+codeInfo.M+1; i++ {
		getter2.setFail(replicas[i].Vuid, errors.New("fake error"))
	}
	_, _, err = getRepairShardsTest(context.Background(), repairer2, replicas, mode, 1, badi2)
	require.Error(t, err)
	require.EqualError(t, errcode.ErrShardMayBeLost, err.Error())

	getter3 := NewMockGetterWithBids(replicas, mode, bids, sizes)
	repairer3 := NewShardRepairer(getter3)
	badi3 := []int{0, 1}
	getter3.MarkDelete(context.Background(), replicas[0].Vuid, 1)
	shouldRepair, _, err = getRepairShardsTest(context.Background(), repairer3, replicas, mode, 1, badi3)
	require.NoError(t, err)
	require.Equal(t, 0, len(shouldRepair))

	getter4 := NewMockGetterWithBids(replicas, mode, bids, sizes)
	repairer4 := NewShardRepairer(getter4)
	badi4 := []int{0, 1}
	for i := 0; i < codeInfo.M+1; i++ {
		getter4.Delete(context.Background(), replicas[i].Vuid, 1)
	}
	_, _, err = getRepairShardsTest(context.Background(), repairer4, replicas, mode, 1, badi4)
	require.Error(t, err)
	require.EqualError(t, errcode.ErrShardMayBeLost, err.Error())

	for i := codeInfo.M + 1; i < codeInfo.M+codeInfo.N+codeInfo.L; i++ {
		getter4.Delete(context.Background(), replicas[i].Vuid, 1)
	}
	shouldRepair, _, err = getRepairShardsTest(context.Background(), repairer4, replicas, mode, 1, badi4)
	require.NoError(t, err)
	require.Equal(t, 0, len(shouldRepair))
}

func repairIdxsEqual(idxs1, idxs2 []int) bool {
	if len(idxs1) != len(idxs2) {
		return false
	}
	m := make(map[int]bool)
	for _, e := range idxs1 {
		m[e] = true
	}

	for _, e := range idxs2 {
		if _, ok := m[e]; !ok {
			return false
		}
	}
	return true
}

func TestShardRepair(t *testing.T) {
	testWithAllMode(t, testShardRepair)
}

func testShardRepair(t *testing.T, mode codemode.CodeMode) {
	codeInfo := mode.Tactic()
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1}
	sizes := []int64{1025}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	replicasCrc32 := make([]uint32, len(replicas))
	for _, replica := range replicas {
		replicasCrc32[replica.Vuid.Index()] = getter.getShardCrc32(replica.Vuid, 1)
	}

	workutils.TaskBufPool = workutils.NewBufPool(&workutils.BufConfig{
		MigrateBufSize:     4 * 1024,
		MigrateBufCapacity: 100,
		RepairBufSize:      2 * 1024,
		RepairBufCapacity:  100,
	})
	repairer := NewShardRepairer(getter)

	task := &proto.ShardRepairTask{
		Bid:      1,
		CodeMode: mode,
		Sources:  replicas,
		BadIdxs:  []uint8{0, 1},
	}
	err := repairer.RepairShard(context.Background(), task)
	require.NoError(t, err)

	getter.Delete(context.Background(), replicas[0].Vuid, 1)
	getter.Delete(context.Background(), replicas[1].Vuid, 1)
	err = repairer.RepairShard(context.Background(), task)
	require.NoError(t, err)
	checkRepairShardResult(t, getter, replicas, replicasCrc32)

	var localIdxs []int
	if codeInfo.L != 0 {
		localStripes, localN, localM := mode.T().AllLocalStripe()
		for _, stripe := range localStripes {
			localIdxs = append(localIdxs, stripe[localN:localN+localM]...)
		}
		// test local replica miss
		localReplica := task.Sources[localIdxs[0]]
		getter.Delete(context.Background(), localReplica.Vuid, 1)
		getter.Delete(context.Background(), replicas[1].Vuid, 1)
		err = repairer.RepairShard(context.Background(), task)
		log.Info("keno localIdxs", localIdxs)
		require.NoError(t, err)
		checkRepairShardResult(t, getter, replicas, replicasCrc32)
	}

	getter.Delete(context.Background(), replicas[0].Vuid, 1)
	getter.Delete(context.Background(), replicas[1].Vuid, 1)

	// test to many bid not exist
	var deleteCnt int
	for idx, replica := range task.Sources {
		if contains(idx, []int{0, 1}) {
			continue
		}
		getter.Delete(context.Background(), replica.Vuid, 1)
		deleteCnt++
		if deleteCnt == codeInfo.L+codeInfo.M {
			break
		}
	}

	err = repairer.RepairShard(context.Background(), task)
	require.Error(t, err)
	require.EqualError(t, errcode.ErrShardMayBeLost, err.Error())
}

func checkRepairShardResult(t *testing.T, getter *MockGetter, replicas []proto.VunitLocation, oldCrcs []uint32) {
	for _, replica := range replicas {
		newCrc32 := getter.getShardCrc32(replica.Vuid, 1)
		require.Equal(t, oldCrcs[replica.Vuid.Index()], newCrc32)
	}
}

func TestShardRepair2(t *testing.T) {
	testWithAllMode(t, testShardRepair2)
}

func testShardRepair2(t *testing.T, mode codemode.CodeMode) {
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1}
	sizes := []int64{1025}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	task := &proto.ShardRepairTask{
		Bid:      1,
		CodeMode: mode,
		Sources:  replicas,
		BadIdxs:  []uint8{0, 1},
	}
	for _, replica := range replicas {
		if contains(int(replica.Vuid.Index()), sliceUint8ToInt(task.BadIdxs)) {
			continue
		}
		getter.Delete(context.Background(), replica.Vuid, 1)
	}

	repairer := NewShardRepairer(getter)

	err := repairer.RepairShard(context.Background(), task)
	require.NoError(t, err)

	getter.Delete(context.Background(), replicas[0].Vuid, 1)
	err = repairer.RepairShard(context.Background(), task)
	require.NoError(t, err)

	getter.Delete(context.Background(), replicas[1].Vuid, 1)
	err = repairer.RepairShard(context.Background(), task)
	require.NoError(t, err)
}

func TestCheckOrphanShard(t *testing.T) {
	testWithAllMode(t, testCheckOrphanShard)
}

func testCheckOrphanShard(t *testing.T, mode codemode.CodeMode) {
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1}
	sizes := []int64{1025}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	task := proto.ShardRepairTask{
		Bid:      1,
		CodeMode: mode,
		Sources:  replicas,
		BadIdxs:  []uint8{0, 1},
	}

	repairer := NewShardRepairer(getter)

	isOrphanShard := repairer.checkOrphanShard(context.Background(), task.Sources, task.Bid, sliceUint8ToInt(task.BadIdxs))
	require.Equal(t, false, isOrphanShard)

	for _, replica := range replicas {
		if contains(int(replica.Vuid.Index()), sliceUint8ToInt(task.BadIdxs)) {
			continue
		}
		getter.Delete(context.Background(), replica.Vuid, 1)
	}
	isOrphanShard = repairer.checkOrphanShard(context.Background(), task.Sources, task.Bid, sliceUint8ToInt(task.BadIdxs))
	require.Equal(t, true, isOrphanShard)

	getter.Delete(context.Background(), replicas[0].Vuid, 1)
	isOrphanShard = repairer.checkOrphanShard(context.Background(), task.Sources, task.Bid, sliceUint8ToInt(task.BadIdxs))
	require.Equal(t, true, isOrphanShard)

	getter.Delete(context.Background(), replicas[0].Vuid, 1)
	isOrphanShard = repairer.checkOrphanShard(context.Background(), task.Sources, task.Bid, sliceUint8ToInt(task.BadIdxs))
	require.Equal(t, true, isOrphanShard)
}
