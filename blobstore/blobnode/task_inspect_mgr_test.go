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

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func newMockReporter(t *testing.T) scheduler.IScheduler {
	cli := mocks.NewMockIScheduler(C(t))
	cli.EXPECT().CompleteInspect(A, A).AnyTimes().Return(nil)
	return cli
}

func TestTaskInspectMgrDo(t *testing.T) {
	testWithAllMode(t, testTaskInspectMgrDo)
}

func testTaskInspectMgrDo(t *testing.T, mode codemode.CodeMode) {
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{10, 1024, 1024, 1024, 1024, 1024, 1024}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	mgr := NewInspectTaskMgr(1, getter, newMockReporter(t))
	task := proto.VolumeInspectTask{
		TaskId:   "InspectTask_XXX",
		Mode:     mode,
		Replicas: replicas,
	}
	ret := mgr.doInspect(context.Background(), &task)
	require.NoError(t, ret.Err())
	require.Equal(t, 0, len(ret.MissedShards))

	getter.MarkDelete(context.Background(), replicas[0].Vuid, 1)
	ret = mgr.doInspect(context.Background(), &task)
	require.NoError(t, ret.Err())
	require.Equal(t, 0, len(ret.MissedShards))

	// normal
	var expectMissedShard []*proto.MissedShard
	ret = mgr.doInspect(context.Background(), &task)
	require.NoError(t, ret.Err())
	verifyInspectResult(t, expectMissedShard, ret.MissedShards)

	// delete one bid
	getter.Delete(context.Background(), replicas[0].Vuid, 1)
	expectMissedShard = append(expectMissedShard, &proto.MissedShard{Vuid: replicas[0].Vuid, Bid: 1})
	ret = mgr.doInspect(context.Background(), &task)
	require.NoError(t, ret.Err())
	verifyInspectResult(t, expectMissedShard, ret.MissedShards)

	// delete  other bid 2
	getter.Delete(context.Background(), replicas[1].Vuid, 2)
	expectMissedShard = append(expectMissedShard, &proto.MissedShard{Vuid: replicas[1].Vuid, Bid: 2})
	ret = mgr.doInspect(context.Background(), &task)
	require.NoError(t, ret.Err())
	verifyInspectResult(t, expectMissedShard, ret.MissedShards)

	// delete m
	delStartIdx := 1
	delEndIdx := mode.Tactic().M
	for _, replica := range replicas[delStartIdx:delEndIdx] {
		getter.Delete(context.Background(), replica.Vuid, 1)
		expectMissedShard = append(expectMissedShard, &proto.MissedShard{Vuid: replica.Vuid, Bid: 1})
	}

	ret = mgr.doInspect(context.Background(), &task)
	require.NoError(t, ret.Err())
	verifyInspectResult(t, expectMissedShard, ret.MissedShards)

	delStartIdx = delEndIdx
	delEndIdx = delEndIdx + 1
	for _, replica := range replicas[delStartIdx:delEndIdx] {
		getter.Delete(context.Background(), replica.Vuid, 1)
		expectMissedShard = append(expectMissedShard, &proto.MissedShard{Vuid: replica.Vuid, Bid: 1})
	}
	ret = mgr.doInspect(context.Background(), &task)

	missBid := proto.MissedShard{Vuid: replicas[1].Vuid, Bid: 2}
	missBids := []*proto.MissedShard{&missBid}
	verifyInspectResult(t, missBids, ret.MissedShards)

	getter.setFail(replicas[0].Vuid, errors.New("fake error"))
	ret = mgr.doInspect(context.Background(), &task)
	require.Error(t, ret.Err())
	require.EqualError(t, errors.New("fake error"), ret.InspectErrStr)

	getter.setWell(replicas[0].Vuid)
	for _, replica := range replicas {
		getter.Delete(context.Background(), replica.Vuid, 2)
	}
	ret = mgr.doInspect(context.Background(), &task)
	require.NoError(t, ret.Err())
	require.Equal(t, 0, len(ret.MissedShards))
}

func verifyInspectResult(t *testing.T, expect, FailShards []*proto.MissedShard) {
	require.Equal(t, len(expect), len(FailShards))
	expectM := make(map[proto.MissedShard]bool)
	for _, shard := range expect {
		expectM[*shard] = true
	}
	for _, shard := range FailShards {
		_, ok := expectM[*shard]
		require.True(t, ok)
	}
}

func TestTaskInspectMgrAddTask(t *testing.T) {
	mode := codemode.EC6P10L2
	replicas := genMockVol(1, mode)
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{10, 1024, 1024, 1024, 1024, 1024, 1024}
	getter := NewMockGetterWithBids(replicas, mode, bids, sizes)
	mgr := NewInspectTaskMgr(1, getter, newMockReporter(t))
	task := proto.VolumeInspectTask{
		TaskId:   "InspectTask_XXX",
		Mode:     mode,
		Replicas: replicas,
	}
	err := mgr.AddTask(context.Background(), &task)
	require.NoError(t, err)

	err = mgr.AddTask(context.Background(), &task)
	require.Error(t, err)

	taskCnt := mgr.RunningTaskSize()
	require.Equal(t, 1, taskCnt)
}
