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
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	mock "github.com/cubefs/cubefs/blobstore/testing/mockshardnode"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func TestNewTestBlobDeleteMgr(t *testing.T) {
	cmClient := mocks.NewMockClientAPI(ctr(t))
	cmClient.EXPECT().GetConfig(any, any).Return("", nil).AnyTimes()
	taskSwitchMgr := taskswitch.NewSwitchMgr(cmClient)

	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sg := mock.NewMockMessageMgrShardGetter(ctr(t))
	sg.EXPECT().GetAllShards().Return([]storage.ShardHandler{sh}).AnyTimes()

	testDir, err := os.MkdirTemp(os.TempDir(), "delete_log")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	cfg := &BlobDelMgrConfig{
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
	}
	mgr, err := NewBlobDeleteMgr(cfg)
	require.Nil(t, err)

	// test stats
	ret := mgr.Stats()
	require.NotNil(t, ret)
	mgr.Close()
}

func TestBlobDeleteMgr_DeleteWithCheckVolConsistency(t *testing.T) {
	//  mock blob transport
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().MarkDeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).AnyTimes()
	blobTp.EXPECT().DeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).AnyTimes()

	blobTp.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{}, nil).AnyTimes()

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	// mock volume cache transport
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	vc := base.NewVolumeCache(volTp, 1)
	mgr := newTestBlobDeleteMgr(t, nil, blobTp, vc)

	minBid := proto.BlobID(1)
	bidCnt := 5
	msg := &delMsgExt{
		suid: proto.Suid(123),
		msg: snproto.DeleteMsg{
			Slice: proto.Slice{
				MinSliceID: minBid,
				Vid:        vid,
				Count:      uint32(bidCnt),
				ValidSize:  10,
			},
			MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
		},
	}

	delRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// check delete stage before delete
	for i := 0; i < bidCnt; i++ {
		require.False(t, msg.hasMarkDel(proto.BlobID(minBid)+proto.BlobID(i)))
		require.False(t, msg.hasDelete(proto.BlobID(minBid+proto.BlobID(i))))
	}

	// delete
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.Nil(t, err)

	// check delete stage after delete
	for i := 0; i < bidCnt; i++ {
		require.True(t, msg.hasMarkDel(proto.BlobID(minBid)+proto.BlobID(i)))
		require.True(t, msg.hasDelete(proto.BlobID(minBid+proto.BlobID(i))))
	}
}

func TestBlobDeleteMgr_DeleteWithCheckVolConsistency2(t *testing.T) {
	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	// mock volume cache transport
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	uintCount := volInfo.CodeMode.GetShardNum()
	//  mock blob transport
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().MarkDeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).Times(uintCount)

	firstTime := true
	blobTp.EXPECT().DeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if firstTime {
			return errors.New("mock err")
		}
		return nil
	}).AnyTimes()

	blobTp.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{}, nil).AnyTimes()

	vc := base.NewVolumeCache(volTp, 1)
	mgr := newTestBlobDeleteMgr(t, nil, blobTp, vc)

	bid := proto.BlobID(1)
	msg := &delMsgExt{
		suid: proto.Suid(123),
		msg: snproto.DeleteMsg{
			Slice: proto.Slice{
				MinSliceID: bid,
				Vid:        vid,
				Count:      1,
				ValidSize:  10,
			},
			MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
		},
	}

	delRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// delete first time, markDelete success, delete failed
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.NotNil(t, err)

	require.True(t, msg.hasMarkDel(bid))
	require.False(t, msg.hasDelete(bid))

	firstTime = false
	// delete second time, skip markDelete
	err = mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.Nil(t, err)

	require.True(t, msg.hasMarkDel(bid))
	require.True(t, msg.hasDelete(bid))
}

func TestBlobDeleteMgr_DeleteWithCheckVolConsistency3(t *testing.T) {
	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	// mock volume cache transport
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	lastTime := false
	//  mock blob transport
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().MarkDeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if !lastTime && info.Vuid.Index() == 0 {
			return errors.New("mock err")
		}
		return nil
	}).AnyTimes()
	blobTp.EXPECT().DeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).AnyTimes()

	blobTp.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{}, nil).AnyTimes()

	vc := base.NewVolumeCache(volTp, 1)
	mgr := newTestBlobDeleteMgr(t, nil, blobTp, vc)

	bid := proto.BlobID(1)
	msg := &delMsgExt{
		suid: proto.Suid(123),
		msg: snproto.DeleteMsg{
			Slice: proto.Slice{
				MinSliceID: bid,
				Vid:        vid,
				Count:      1,
				ValidSize:  10,
			},
			MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
		},
	}

	delRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// first time
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.NotNil(t, err)

	require.False(t, msg.hasMarkDel(bid))
	require.False(t, msg.hasDelete(bid))

	// second time
	err = mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.NotNil(t, err)

	require.False(t, msg.hasMarkDel(bid))
	require.False(t, msg.hasDelete(bid))

	lastTime = true
	err = mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.Nil(t, err)

	require.True(t, msg.hasMarkDel(bid))
	require.True(t, msg.hasDelete(bid))
}

func TestBlobDeleteMgr_DeleteWithCheckVolConsistency4(t *testing.T) {
	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	// mock volume cache transport
	volTp := mocks.NewMockVolumeTransport(ctr(t))
	volTp.EXPECT().GetVolumeInfo(any, any).Return(volInfo, nil).AnyTimes()

	lastTime := false
	//  mock blob transport
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().MarkDeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).AnyTimes()
	blobTp.EXPECT().DeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if !lastTime && info.Vuid.Index() == 0 {
			return apierr.ErrShardNotMarkDelete
		}
		return nil
	}).AnyTimes()

	blobTp.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{}, nil).AnyTimes()

	vc := base.NewVolumeCache(volTp, 1)
	mgr := newTestBlobDeleteMgr(t, nil, blobTp, vc)

	bid := proto.BlobID(1)
	msg := &delMsgExt{
		suid: proto.Suid(123),
		msg: snproto.DeleteMsg{
			Slice: proto.Slice{
				MinSliceID: bid,
				Vid:        vid,
				Count:      1,
				ValidSize:  10,
			},
			MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
		},
	}

	delRet := &executeRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// first time
	err := mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.NotNil(t, err)

	require.False(t, msg.hasMarkDel(bid))
	require.False(t, msg.hasDelete(bid))

	lastTime = true
	err = mgr.ExecuteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.Nil(t, err)

	require.True(t, msg.hasMarkDel(bid))
	require.True(t, msg.hasDelete(bid))
}

func TestBlobDeleteMgr_DeleteSlice_UpdateVolume(t *testing.T) {
	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)
	newVolInfo := newMockSimpleVolumeInfo(vid)
	newVolInfo.VunitLocations[0].DiskID = volInfo.VunitLocations[0].DiskID + 100

	vc := mocks.NewMockIVolumeCache(ctr(t))
	vc.EXPECT().UpdateVolume(any).Return(&snproto.VolumeInfoSimple{Vid: vid, CodeMode: volInfo.CodeMode, VunitLocations: newVolInfo.VunitLocations}, nil)

	tp := mocks.NewMockBlobTransport(ctr(t))
	tp.EXPECT().MarkDeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if info.Vuid.Index()%2 == 0 {
			return apierr.ErrDiskBroken
		}
		return nil
	}).Times(6)
	tp.EXPECT().MarkDeleteSliceUnit(any, any, any).Return(nil).Times(3)
	tp.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{}, nil).AnyTimes()

	mgr := newMinimalBlobDeleteMgr(&MessageMgrConfig{
		BlobTransport: tp,
		VolCache:      vc,
	})

	msgExt := &delMsgExt{msg: snproto.DeleteMsg{
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}}

	bid := proto.BlobID(1)
	_, err := mgr.deleteSlice(context.Background(), volInfo, msgExt, bid, true)
	require.Nil(t, err)
	require.True(t, msgExt.hasMarkDel(bid))
}

func TestBlobDeleteMgr_DeleteSlice_UpdateVolume2(t *testing.T) {
	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	vc := mocks.NewMockIVolumeCache(ctr(t))
	vc.EXPECT().UpdateVolume(any).Return(&snproto.VolumeInfoSimple{Vid: vid, CodeMode: volInfo.CodeMode, VunitLocations: volInfo.VunitLocations}, nil)

	tp := mocks.NewMockBlobTransport(ctr(t))
	tp.EXPECT().MarkDeleteSliceUnit(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if info.Vuid.Index()%2 == 0 {
			return apierr.ErrDiskBroken
		}
		return nil
	}).Times(6)

	tp.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{}, nil).AnyTimes()

	mgr := newMinimalBlobDeleteMgr(&MessageMgrConfig{
		BlobTransport: tp,
		VolCache:      vc,
	})

	msgExt := &delMsgExt{msg: snproto.DeleteMsg{
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}}

	_, err := mgr.deleteSlice(context.Background(), volInfo, msgExt, proto.BlobID(1), true)
	require.NotNil(t, err)
}

func TestBlobDeleteMgr_DeleteShard_BackToInitStage(t *testing.T) {
	tp := mocks.NewMockBlobTransport(ctr(t))
	tp.EXPECT().DeleteSliceUnit(any, any, any).Return(apierr.ErrShardNotMarkDelete)
	tp.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{}, nil)

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	mgr := newMinimalBlobDeleteMgr(&MessageMgrConfig{
		BlobTransport: tp,
	})

	idx := 0
	bid := proto.BlobID(1)
	vunit := volInfo.VunitLocations[idx]

	msgExt := &delMsgExt{msg: snproto.DeleteMsg{
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}}
	msgExt.setSliceUnitDelStage(bid, vunit.Vuid, DeleteStageMarkDelete)

	err := mgr.deleteSliceUnit(context.Background(), vunit, bid, msgExt, false)
	require.Equal(t, err, apierr.ErrShardNotMarkDelete)
	require.False(t, msgExt.hasSliceUnitMarkDel(bid, vunit.Vuid))
}

func TestBlobDeleteMgr_DeleteShard_AssumeSuccess(t *testing.T) {
	tp := mocks.NewMockBlobTransport(ctr(t))
	tp.EXPECT().DeleteSliceUnit(any, any, any).Return(apierr.ErrNoSuchBid)
	tp.EXPECT().GetBlobnodeDiskInfo(any, any).Return(&clustermgr.BlobNodeDiskInfo{}, nil)

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	mgr := newMinimalBlobDeleteMgr(&MessageMgrConfig{
		BlobTransport: tp,
	})

	idx := 0
	bid := proto.BlobID(1)
	vunit := volInfo.VunitLocations[idx]

	msgExt := &delMsgExt{msg: snproto.DeleteMsg{
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}}
	msgExt.setSliceUnitDelStage(bid, vunit.Vuid, DeleteStageMarkDelete)

	err := mgr.deleteSliceUnit(context.Background(), vunit, bid, msgExt, false)
	require.Nil(t, err)
	require.True(t, msgExt.hasSliceUnitDelete(bid, vunit.Vuid))
}

func TestBlobDeleteMgr_InsertDeleteMsg(t *testing.T) {
	suid := proto.Suid(123)
	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().ShardingSubRangeCount().Return(2)
	sh.EXPECT().InsertItem(any, any, any, any).Return(nil)

	sg := mock.NewMockMessageMgrShardGetter(ctr(t))
	sg.EXPECT().GetShard(any, any).Return(sh, nil)

	mgr := newTestBlobDeleteMgr(t, sg, nil, nil)

	args := &shardnode.DeleteBlobRawArgs{
		Header: shardnode.ShardOpHeader{Suid: suid},
		Slice:  proto.Slice{Vid: proto.Vid(1), MinSliceID: proto.BlobID(456), Count: 3},
	}

	err := mgr.Delete(context.Background(), args)
	require.Nil(t, err)
}

func TestBlobDeleteMgr_SlicesToDeleteMsgItems(t *testing.T) {
	shardKeys := []string{"abc", "def"}

	mgr := newTestBlobDeleteMgr(t, nil, nil, nil)

	slice1 := proto.Slice{Vid: 1, MinSliceID: 456, Count: 3}
	slice2 := proto.Slice{Vid: 365, MinSliceID: 45542011, Count: 5}

	items, err := mgr.SlicesToDeleteMsgItems(context.Background(), []proto.Slice{slice1, slice2}, shardKeys)
	require.Nil(t, err)
	mk := newMsgKey()
	defer mk.release()
	for i := range items {
		mk.setKey([]byte(items[i].ID))
		err := mk.decode(len(shardKeys))
		require.Nil(t, err)
		require.Equal(t, shardKeys, mk.shardKeys)
		require.True(t, bytes.Contains([]byte(items[i].ID), snproto.DeleteMsgPrefix))
	}
}

// newMinimalBlobDeleteMgr creates a minimal BlobDeleteMgr for testing
func newMinimalBlobDeleteMgr(cfg *MessageMgrConfig) *BlobDeleteMgr {
	baseMgr := &messageMgr{
		cfg: cfg,
	}
	mgr := &BlobDeleteMgr{
		messageMgr: baseMgr,
	}
	baseMgr.cfg.executor = mgr
	return mgr
}

func newTestBlobDeleteMgr(t *testing.T, sg ShardGetter, tp base.BlobTransport, vc base.IVolumeCache) *BlobDeleteMgr {
	baseMgr := newTestBlobMessageMgr(t, sg, tp, vc, nil, snproto.MessageTypeDelete)

	mgr := &BlobDeleteMgr{
		messageMgr: baseMgr,
	}
	// Set executor to BlobDeleteMgr itself
	baseMgr.cfg.executor = mgr
	return mgr
}
