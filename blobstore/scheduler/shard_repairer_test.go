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
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

func newShardRepairMgr(t *testing.T) *ShardRepairMgr {
	ctr := gomock.NewController(t)

	clusterTopology := NewMockClusterTopology(ctr)
	clusterTopology.EXPECT().GetVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	clusterTopology.EXPECT().UpdateVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)

	selector := mocks.NewMockSelector(ctr)
	selector.EXPECT().GetRandomN(any).AnyTimes().Return([]string{"http://127.0.0.1:9600"})

	blobnode := NewMockBlobnodeAPI(ctr)
	blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(nil)

	orphanShardLog := mocks.NewMockRecordLogEncoder(ctr)
	orphanShardLog.EXPECT().Encode(any).AnyTimes().Return(nil)

	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)
	taskSwitch, _ := switchMgr.AddSwitch(proto.TaskTypeBlobDelete.String())

	return &ShardRepairMgr{
		clusterTopology:         clusterTopology,
		blobnodeSelector:        selector,
		blobnodeCli:             blobnode,
		orphanShardLogger:       orphanShardLog,
		taskSwitch:              taskSwitch,
		taskPool:                taskpool.New(10, 10),
		repairSuccessCounter:    base.NewCounter(1, ShardRepair, base.KindSuccess),
		repairFailedCounter:     base.NewCounter(1, ShardRepair, base.KindFailed),
		errStatsDistribution:    base.NewErrorStats(),
		repairSuccessCounterMin: &counter.Counter{},
		repairFailedCounterMin:  &counter.Counter{},
		cfg: &ShardRepairConfig{
			MessagePunishThreshold: defaultMessagePunishThreshold,
		},
	}
}

func TestTryRepair(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	volume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
	{
		// no host for shard repair
		mgr := newShardRepairMgr(t)
		selector := mocks.NewMockSelector(ctr)
		selector.EXPECT().GetRandomN(any).Return(nil)
		mgr.blobnodeSelector = selector
		doneVolume, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: proto.BlobID(1), Vid: proto.Vid(1), BadIdx: []uint8{0}})
		require.ErrorIs(t, err, ErrBlobnodeServiceUnavailable)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// repair success
		mgr := newShardRepairMgr(t)
		doneVolume, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: proto.BlobID(1), Vid: proto.Vid(1), BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// repair failed and update volume failed
		mgr := newShardRepairMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrDestReplicaBad)
		mgr.blobnodeCli = blobnode

		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, ErrFrequentlyUpdate)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: proto.BlobID(1), Vid: proto.Vid(1), BadIdx: []uint8{0}})
		require.ErrorIs(t, err, errcode.ErrDestReplicaBad)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// repair failed and update volume success, volume not change
		mgr := newShardRepairMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrDestReplicaBad)
		mgr.blobnodeCli = blobnode

		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, nil)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: proto.BlobID(1), Vid: proto.Vid(1), BadIdx: []uint8{0}})
		require.ErrorIs(t, err, errcode.ErrDestReplicaBad)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// repair failed and update volume success, volume change and repair success
		mgr := newShardRepairMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrDestReplicaBad)
		blobnode.EXPECT().RepairShard(any, any, any).Return(nil)
		mgr.blobnodeCli = blobnode

		clusterTopology := NewMockClusterTopology(ctr)
		newVolume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
		newVolume.VunitLocations[5].Vuid += 1
		clusterTopology.EXPECT().UpdateVolume(any).Return(newVolume, nil)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: proto.BlobID(1), Vid: proto.Vid(1), BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.False(t, doneVolume.EqualWith(volume))
		require.True(t, doneVolume.EqualWith(newVolume))
	}
}
