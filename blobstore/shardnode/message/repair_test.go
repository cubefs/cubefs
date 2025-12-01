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

package message

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	mock "github.com/cubefs/cubefs/blobstore/testing/mockshardnode"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/selector"
)

func TestNewSliceRepairMgr(t *testing.T) {
	cmClient := mocks.NewMockClientAPI(ctr(t))
	cmClient.EXPECT().GetConfig(any, any).Return("", nil).AnyTimes()
	taskSwitchMgr := taskswitch.NewSwitchMgr(cmClient)

	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sg := mock.NewMockMessageMgrShardGetter(ctr(t))
	sg.EXPECT().GetAllShards().Return([]storage.ShardHandler{sh}).AnyTimes()

	testDir, err := os.MkdirTemp(os.TempDir(), "repair_log")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	cfg := &SliceRepairMgrConfig{
		MessageMgrConfig: &MessageMgrConfig{
			TaskSwitchMgr: taskSwitchMgr,
			ShardGetter:   sg,
			BlobTransport: mocks.NewMockBlobTransport(ctr(t)),
			VolCache:      mocks.NewMockIVolumeCache(ctr(t)),
			MessageCfg: MessageCfg{
				FailedMsgChannelSize: 5,
				ProduceTaskPoolSize:  4,
				RateLimit:            100.0,
				RateLimitBurst:       10,
				MaxExecuteSliceNum:   1000,
				TierConfig:           newTestPriorityConfig(),
				MessageLog:           recordlog.Config{Dir: testDir},
			},
		},
	}
	mgr, err := NewSliceRepairMgr(cfg)
	require.Nil(t, err)

	// test stats
	ret := mgr.Stats()
	require.NotNil(t, ret)
	mgr.Close()
}

func TestSliceRepairMgr_ExecuteWithCheckVolConsistency(t *testing.T) {
	// mock blob transport
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().RepairSlice(any, any, any, any).DoAndReturn(func(ctx context.Context, host string, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) error {
		return nil
	}).AnyTimes()

	// mock blobnode selector
	blobNodeSelector := mocks.NewMockSelector(ctr(t))
	blobNodeSelector.EXPECT().GetRandomN(1).Return([]string{"blobnode-1"}).AnyTimes()

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	// mock volume cache transport
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	vc := base.NewVolumeCache(volTp, 1)
	mgr := newTestSliceRepairMgr(t, nil, blobTp, vc, nil, nil, blobNodeSelector)

	bid := proto.BlobID(1)
	msg := &repairMsgExt{
		msg: snproto.SliceRepairMsg{
			Bid:    bid,
			Vid:    vid,
			BadIdx: []uint32{0},
			Reason: "test",
		},
	}

	repairRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// repair
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, repairRet)
	require.Nil(t, err)
}

func TestSliceRepairMgr_ExecuteWithCheckVolConsistency_BlobnodeUnavailable(t *testing.T) {
	// mock blobnode selector - return empty list
	blobNodeSelector := mocks.NewMockSelector(ctr(t))
	blobNodeSelector.EXPECT().GetRandomN(1).Return([]string{}).AnyTimes()

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	// mock volume cache transport
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	vc := base.NewVolumeCache(volTp, 1)
	mgr := newTestSliceRepairMgr(t, nil, nil, vc, nil, nil, blobNodeSelector)

	bid := proto.BlobID(1)
	msg := &repairMsgExt{
		msg: snproto.SliceRepairMsg{
			Bid:    bid,
			Vid:    vid,
			BadIdx: []uint32{0},
			Reason: "test",
		},
	}

	repairRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// repair should fail with ErrBlobnodeServiceUnavailable
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, repairRet)
	require.NotNil(t, err)
	require.Equal(t, ErrBlobnodeServiceUnavailable, err)
}

func TestSliceRepairMgr_ExecuteWithCheckVolConsistency_OrphanShard(t *testing.T) {
	// mock blob transport - return orphan slice error
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().RepairSlice(any, any, any, any).DoAndReturn(func(ctx context.Context, host string, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) error {
		return rpc2.NewError(apierr.CodeOrphanShard, "orphan slice", "")
	}).AnyTimes()

	// mock blobnode selector
	blobNodeSelector := mocks.NewMockSelector(ctr(t))
	blobNodeSelector.EXPECT().GetRandomN(1).Return([]string{"blobnode-1"}).AnyTimes()

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	// mock volume cache transport
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	// mock execute logger
	executeLogger := mocks.NewMockRecordLogEncoder(ctr(t))
	executeLogger.EXPECT().Encode(any).Return(nil).AnyTimes()

	vc := base.NewVolumeCache(volTp, 1)
	mgr := newTestSliceRepairMgr(t, nil, blobTp, vc, nil, nil, blobNodeSelector)
	mgr.executeLogger = executeLogger

	bid := proto.BlobID(1)
	msg := &repairMsgExt{
		msg: snproto.SliceRepairMsg{
			Bid:    bid,
			Vid:    vid,
			BadIdx: []uint32{0},
			Reason: "test",
		},
	}

	repairRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	mgr.cfg.ClusterID = 1
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, repairRet)
	require.NotNil(t, err)
	require.Equal(t, apierr.CodeOrphanShard, rpc2.DetectStatusCode(err))
}

func TestSliceRepairMgr_ExecuteWithCheckVolConsistency_DiskNotFound(t *testing.T) {
	// mock blob transport - return disk not found error
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().RepairSlice(any, any, any, any).DoAndReturn(func(ctx context.Context, host string, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) error {
		return rpc2.NewError(apierr.CodeDiskNotFound, "disk not found", "")
	}).AnyTimes()

	// mock blobnode selector
	blobNodeSelector := mocks.NewMockSelector(ctr(t))
	blobNodeSelector.EXPECT().GetRandomN(1).Return([]string{"blobnode-1"}).AnyTimes()

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	// mock volume cache transport
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	// mock transport for GetBlobnodeDiskInfo and GetVolumeInfo
	transport := mocks.NewMockTransport(ctr(t))
	transport.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{
		DiskInfo: clustermgr.DiskInfo{
			Status: proto.DiskStatusNormal,
		},
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
			DiskID: volInfo.VunitLocations[0].DiskID,
		},
	}, nil).AnyTimes()
	transport.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	// mock scheduler client
	scClient := mocks.NewMockIScheduler(ctr(t))
	scClient.EXPECT().CheckTaskExist(any, any).Return(&scheduler.CheckTaskExistResp{Exist: false}, nil).AnyTimes()

	vc := base.NewVolumeCache(volTp, 1)
	mgr := newTestSliceRepairMgr(t, nil, blobTp, vc, transport, scClient, blobNodeSelector)

	bid := proto.BlobID(1)
	msg := &repairMsgExt{
		msg: snproto.SliceRepairMsg{
			Bid:    bid,
			Vid:    vid,
			BadIdx: []uint32{0},
			Reason: "test",
		},
	}

	repairRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// repair should fail with disk not found error
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, repairRet)
	require.NotNil(t, err)
	require.Equal(t, apierr.CodeDiskNotFound, rpc2.DetectStatusCode(err))
}

func TestSliceRepairMgr_ExecuteWithCheckVolConsistency_UpdateVolume(t *testing.T) {
	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)
	newVolInfo := newMockSimpleVolumeInfo(vid)
	newVolInfo.VunitLocations[0].DiskID = volInfo.VunitLocations[0].DiskID + 100

	// mock volume cache transport
	firstTime := true
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).DoAndReturn(func(ctx context.Context, vid proto.Vid) (*snproto.VolumeInfoSimple, error) {
		if firstTime {
			firstTime = false
			return volInfo, nil
		}
		return newVolInfo, nil
	}).Times(2)

	vc := base.NewVolumeCache(volTp, 1)

	// mock blob transport - first time fail, second time success
	firstTimeRepair := true
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().RepairSlice(any, any, any, any).DoAndReturn(func(ctx context.Context, host string, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) error {
		if firstTimeRepair {
			firstTimeRepair = false
			return rpc2.NewError(int32(apierr.CodeDiskBroken), "disk broken", "")
		}
		return nil
	}).Times(2)

	// mock blobnode selector
	blobNodeSelector := mocks.NewMockSelector(ctr(t))
	blobNodeSelector.EXPECT().GetRandomN(1).Return([]string{"blobnode-1"}).AnyTimes()

	mgr := newMinimalSliceRepairMgr(&SliceRepairMgrConfig{
		MessageMgrConfig: &MessageMgrConfig{
			BlobTransport: blobTp,
			VolCache:      vc,
		},
		BlobNodeSelector: blobNodeSelector,
	})

	bid := proto.BlobID(1)
	msg := &repairMsgExt{
		msg: snproto.SliceRepairMsg{
			Bid:    bid,
			Vid:    vid,
			BadIdx: []uint32{0},
			Reason: "test",
		},
	}

	repairRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// repair should succeed after volume update
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, repairRet)
	require.Nil(t, err)
}

func TestSliceRepairMgr_ExecuteWithCheckVolConsistency_UpdateVolume2(t *testing.T) {
	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)
	newVolInfo := newMockSimpleVolumeInfo(vid)
	newVolInfo.VunitLocations[0].DiskID = volInfo.VunitLocations[0].DiskID + 100

	// mock volume cache transport
	firstTime := true
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).DoAndReturn(func(ctx context.Context, vid proto.Vid) (*snproto.VolumeInfoSimple, error) {
		if firstTime {
			firstTime = false
			return volInfo, nil
		}
		return newVolInfo, nil
	}).Times(2)

	vc := base.NewVolumeCache(volTp, 1)

	// mock blob transport - always fail
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().RepairSlice(any, any, any, any).DoAndReturn(func(ctx context.Context, host string, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) error {
		return rpc2.NewError(int32(apierr.CodeDiskBroken), "disk broken", "")
	}).AnyTimes()

	// mock blobnode selector
	blobNodeSelector := mocks.NewMockSelector(ctr(t))
	blobNodeSelector.EXPECT().GetRandomN(1).Return([]string{"blobnode-1"}).AnyTimes()

	mgr := newMinimalSliceRepairMgr(&SliceRepairMgrConfig{
		MessageMgrConfig: &MessageMgrConfig{
			BlobTransport: blobTp,
			VolCache:      vc,
		},
		BlobNodeSelector: blobNodeSelector,
	})

	bid := proto.BlobID(1)
	msg := &repairMsgExt{
		msg: snproto.SliceRepairMsg{
			Bid:    bid,
			Vid:    vid,
			BadIdx: []uint32{0},
			Reason: "test",
		},
	}

	repairRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// repair should fail even after volume update (volume not changed)
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, repairRet)
	require.NotNil(t, err)
}

func TestSliceRepairMgr_RepairShard(t *testing.T) {
	// mock blob transport
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().RepairSlice(any, any, any, any).DoAndReturn(func(ctx context.Context, host string, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) error {
		return nil
	}).AnyTimes()

	// mock blobnode selector
	blobNodeSelector := mocks.NewMockSelector(ctr(t))
	blobNodeSelector.EXPECT().GetRandomN(1).Return([]string{"blobnode-1"}).AnyTimes()

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	mgr := newMinimalSliceRepairMgr(&SliceRepairMgrConfig{
		MessageMgrConfig: &MessageMgrConfig{
			BlobTransport: blobTp,
		},
		BlobNodeSelector: blobNodeSelector,
	})

	repairMsg := &snproto.SliceRepairMsg{
		Bid:    proto.BlobID(1),
		Vid:    vid,
		BadIdx: []uint32{0},
		Reason: "test",
	}

	// repair should succeed
	newVol, err := mgr.repairSlice(context.Background(), volInfo, repairMsg)
	require.Nil(t, err)
	require.Equal(t, volInfo, newVol)
}

func TestSliceRepairMgr_RepairShard_Error(t *testing.T) {
	// mock blob transport - return error
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().RepairSlice(any, any, any, any).DoAndReturn(func(ctx context.Context, host string, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) error {
		return errors.New("repair failed")
	}).AnyTimes()

	// mock blobnode selector
	blobNodeSelector := mocks.NewMockSelector(ctr(t))
	blobNodeSelector.EXPECT().GetRandomN(1).Return([]string{"blobnode-1"}).AnyTimes()

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	mgr := newMinimalSliceRepairMgr(&SliceRepairMgrConfig{
		MessageMgrConfig: &MessageMgrConfig{
			BlobTransport: blobTp,
		},
		BlobNodeSelector: blobNodeSelector,
	})

	repairMsg := &snproto.SliceRepairMsg{
		Bid:    proto.BlobID(1),
		Vid:    vid,
		BadIdx: []uint32{0},
		Reason: "test",
	}

	// repair should fail
	newVol, err := mgr.repairSlice(context.Background(), volInfo, repairMsg)
	require.NotNil(t, err)
	require.Equal(t, volInfo, newVol)
}

func TestSliceRepairMgr_InsertRepairMsg(t *testing.T) {
	// mock shard
	shard := mock.NewMockSpaceShardHandler(ctr(t))
	shard.EXPECT().InsertItem(any, any, any, any).Return(nil).AnyTimes()
	shard.EXPECT().ShardingSubRangeCount().Return(2).AnyTimes()

	// mock shard getter
	sg := mock.NewMockMessageMgrShardGetter(ctr(t))
	sg.EXPECT().GetShard(any, any).Return(shard, nil).AnyTimes()

	mgr := newTestSliceRepairMgr(t, sg, nil, nil, nil, nil, nil)

	args := &snapi.RepairSliceArgs{
		Header: snapi.ShardOpHeader{
			DiskID: 1,
			Suid:   1,
		},
		Bid:    proto.BlobID(1),
		Vid:    proto.Vid(1),
		BadIdx: []uint32{0, 1},
		Reason: "test",
	}
	err := mgr.Repair(context.Background(), args)
	require.Nil(t, err)
}

// newMinimalSliceRepairMgr creates a minimal SliceRepairMgr for testing
func newMinimalSliceRepairMgr(cfg *SliceRepairMgrConfig) *SliceRepairMgr {
	baseMgr := &messageMgr{
		cfg: cfg.MessageMgrConfig,
	}
	mgr := &SliceRepairMgr{
		messageMgr:       baseMgr,
		transport:        cfg.Transport,
		blobNodeSelector: cfg.BlobNodeSelector,
		scClient:         cfg.SCClient,
	}
	baseMgr.cfg.executor = mgr
	return mgr
}

func newTestSliceRepairMgr(t *testing.T, sg ShardGetter, tp base.BlobTransport, vc base.IVolumeCache, transport base.Transport, scClient scheduler.IScheduler, selector selector.Selector) *SliceRepairMgr {
	baseMgr := newTestBlobMessageMgr(t, sg, tp, vc, nil, snproto.MessageTypeRepair)
	mgr := &SliceRepairMgr{
		messageMgr:               baseMgr,
		transport:                transport,
		scClient:                 scClient,
		blobNodeSelector:         selector,
		chunkMissMigrateReporter: base.NewAbnormalReporter(0, base.ShardRepair, base.ChunkMissMigrateAbnormal),
	}
	// Set executor to ShardRepairMgr itself
	baseMgr.cfg.executor = mgr
	baseMgr.cfg.reporter = base.NewRepairSliceTaskReporter(0)
	return mgr
}
