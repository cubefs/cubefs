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

package blobdeleter

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	mock "github.com/cubefs/cubefs/blobstore/testing/mockshardnode"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

func encodeRawDelMsgKey(ts base.Ts, vid proto.Vid, bid proto.BlobID, tagNum int) ([]byte, []string) {
	delMsg := shardnode.DeleteBlobRawArgs{Slice: proto.Slice{MinSliceID: bid, Vid: vid}}
	shardKeys := delMsg.GetShardKeys(tagNum)
	return encodeDelMsgKey(ts, vid, bid, shardKeys), shardKeys
}

func newTestBlobDeleteMgr(t *testing.T, sg ShardGetter, tp base.BlobTransport, vc base.IVolumeCache) *BlobDeleteMgr {
	cfg := &BlobDelMgrConfig{
		ShardGetter: sg,
		Transport:   tp,
		VolCache:    vc,
		BlobDelCfg: BlobDelCfg{
			MsgChannelNum:        2,
			MsgChannelSize:       10,
			FailedMsgChannelSize: 5,
			ProduceTaskPoolSize:  4,
			RateLimit:            100.0,
			RateLimitBurst:       10,
			MaxListMessageNum:    100,
			MaxExecuteBidNum:     1000,
			SafeDeleteTimeout:    util.Duration{Duration: time.Minute},
			PunishTimeout:        util.Duration{Duration: time.Minute},
		},
	}

	cmClient := mocks.NewMockClientAPI(ctr(t))
	cmClient.EXPECT().GetConfig(any, any).DoAndReturn(func(_ context.Context, name string) (string, error) {
		if name == snproto.ShardNodeBlobDeleteTask {
			return taskswitch.SwitchOpen, nil
		}
		return "", nil
	}).AnyTimes()
	taskSwitchMgr := taskswitch.NewSwitchMgr(cmClient)
	cfg.TaskSwitchMgr = taskSwitchMgr
	sw, err := taskSwitchMgr.AddSwitch(snproto.ShardNodeBlobDeleteTask)
	require.NoError(t, err)

	for {
		if sw.Enabled() {
			break
		}
	}

	msgChannels := make([]chan *delMsgExt, cfg.MsgChannelNum)
	for i := 0; i < cfg.MsgChannelNum; i++ {
		msgChannels[i] = make(chan *delMsgExt, cfg.MsgChannelSize)
	}

	delLogger, err := recordlog.NewEncoder(nil)
	require.NoError(t, err)

	return &BlobDeleteMgr{
		cfg:             cfg,
		taskSwitch:      sw,
		tsGen:           base.NewTsGenerator(base.Ts(0)),
		produceTaskPool: taskpool.New(cfg.ProduceTaskPoolSize, 1),
		consumeTaskPool: taskpool.New(cfg.ProduceTaskPoolSize, 1),

		msgChannels:   msgChannels,
		failedMsgChan: make(chan *delMsgExt, cfg.FailedMsgChannelSize),
		bidLimiter:    rate.NewLimiter(rate.Limit(cfg.RateLimit), cfg.RateLimitBurst),

		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},
		errStatsDistribution:   base.NewErrorStats(),
		delLogger:              delLogger,
		Closer:                 closer.New(),
	}
}

func newTestShardListReader(sh storage.ShardHandler) *shardListReader {
	reader := newShardListReader(sh)
	// add unprotected messages to cache, timestamp is 2 hours ago
	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	unprotectedMsg := &delMsgExt{
		msg: snproto.DeleteMsg{
			Time: oldTime,
		},
	}

	// add protected message to cache, timestamp is now
	now := time.Now().Unix()
	protectedMsg := &delMsgExt{
		msg: snproto.DeleteMsg{
			Time: now,
		},
	}
	reader.messages = append(reader.messages, unprotectedMsg, protectedMsg)
	return reader
}

func newMockSimpleVolumeInfo(vid proto.Vid) *snproto.VolumeInfoSimple {
	// mock volume info
	vunits := make([]proto.VunitLocation, 6)
	mode := codemode.EC3P3
	for i := 0; i < mode.GetShardNum(); i++ {
		vuid, _ := proto.NewVuid(vid, uint8(i), 1)
		vunits[i] = proto.VunitLocation{
			Vuid:   vuid,
			DiskID: proto.DiskID(i),
		}
	}
	return &snproto.VolumeInfoSimple{
		Vid:            vid,
		CodeMode:       codemode.EC3P3,
		VunitLocations: vunits,
	}
}

func TestNewBlobDeleteMgr(t *testing.T) {
	cmClient := mocks.NewMockClientAPI(ctr(t))
	cmClient.EXPECT().GetConfig(any, any).Return("", nil).AnyTimes()
	taskSwitchMgr := taskswitch.NewSwitchMgr(cmClient)

	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sg := mock.NewMockDelMgrShardGetter(ctr(t))
	sg.EXPECT().GetAllShards().Return([]storage.ShardHandler{sh}).AnyTimes()

	testDir, err := os.MkdirTemp(os.TempDir(), "delete_log")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	cfg := &BlobDelMgrConfig{
		TaskSwitchMgr: taskSwitchMgr,
		ShardGetter:   sg,
		Transport:     mocks.NewMockBlobTransport(ctr(t)),
		VolCache:      mocks.NewMockIVolumeCache(ctr(t)),
		BlobDelCfg: BlobDelCfg{
			MsgChannelNum:        2,
			MsgChannelSize:       10,
			FailedMsgChannelSize: 5,
			ProduceTaskPoolSize:  4,
			RateLimit:            100.0,
			RateLimitBurst:       10,
			MaxListMessageNum:    100,
			MaxExecuteBidNum:     1000,
			SafeDeleteTimeout:    util.Duration{Duration: time.Minute},
			PunishTimeout:        util.Duration{Duration: time.Minute},
			DeleteLog:            recordlog.Config{Dir: testDir},
		},
	}
	mgr, err := NewBlobDeleteMgr(cfg)
	require.Nil(t, err)

	stats := mgr.Stats()
	require.NotNil(t, stats)
	mgr.Close()
}

func TestBlobDeleteMgr_ListShardMsg(t *testing.T) {
	ctx := context.Background()

	diskID := proto.DiskID(1)
	suid := proto.Suid(123)

	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().IsLeader().Return(true)
	sh.EXPECT().GetSuid().Return(suid)

	sg := mock.NewMockDelMgrShardGetter(ctr(t))
	sg.EXPECT().GetShard(any, any).Return(sh, nil)

	mgr := newTestBlobDeleteMgr(t, sg, nil, nil)

	reader := newTestShardListReader(sh)
	_hasMsg, err := mgr.listShardMsg(ctx, diskID, reader)
	require.Nil(t, err)
	require.True(t, _hasMsg)
}

func TestBlobDeleteMgr_RunConsumeTask(t *testing.T) {
	suid := proto.Suid(123)
	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().ShardingSubRangeCount().Return(2).AnyTimes()
	sh.EXPECT().GetRouteVersion().Return(proto.RouteVersion(1)).AnyTimes()
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(nil).AnyTimes()

	// mock volume cache
	vc := mocks.NewMockIVolumeCache(ctr(t))
	vc.EXPECT().DoubleCheckedRun(any, any, any).
		DoAndReturn(func(ctx context.Context, vid proto.Vid,
			task func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error),
		) error {
			return nil
		}).AnyTimes()

	mgr := newTestBlobDeleteMgr(t, nil, nil, vc)
	mgr.cfg.MaxExecuteBidNum = 8
	reader := newTestShardListReader(sh)
	mgr.shardListReaderMap.Store(suid, reader)

	// mock messages
	j := uint32(0)
	for i := 0; i < 10; i++ {
		slice := proto.Slice{}
		slice.Vid = proto.Vid(1)
		slice.MinSliceID = proto.BlobID(j)
		cnt := uint32(rand.Int31n(10))
		slice.Count = cnt
		j += cnt
		ts := mgr.tsGen.GenerateTs()
		id, _ := encodeRawDelMsgKey(ts, slice.Vid, slice.MinSliceID, 2)
		mgr.msgChannels[0] <- &delMsgExt{
			suid: suid,
			msg: snproto.DeleteMsg{
				Slice: slice,
			},
			msgKey: id,
		}
	}
	go mgr.runConsumeTask(context.Background(), 0)
	for {
		if len(mgr.msgChannels[0]) == 0 {
			break
		}
	}
}

func TestBlobDeleteMgr_DeleteWithCheckVolConsistency(t *testing.T) {
	//  mock blob transport
	blobTp := mocks.NewMockBlobTransport(ctr(t))
	blobTp.EXPECT().MarkDeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).AnyTimes()
	blobTp.EXPECT().DeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).AnyTimes()

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

	delRet := &delBlobRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// check delete stage before delete
	for i := 0; i < bidCnt; i++ {
		require.False(t, msg.hasMarkDel(proto.BlobID(minBid)+proto.BlobID(i)))
		require.False(t, msg.hasDelete(proto.BlobID(minBid+proto.BlobID(i))))
	}

	// delete
	err := mgr.deleteWithCheckVolConsistency(context.Background(), vid, delRet)
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
	blobTp.EXPECT().MarkDeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).Times(uintCount)

	firstTime := true
	blobTp.EXPECT().DeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if firstTime {
			return errors.New("mock err")
		}
		return nil
	}).AnyTimes()

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

	delRet := &delBlobRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// delete first time, markDelete success, delete failed
	err := mgr.deleteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.NotNil(t, err)

	require.True(t, msg.hasMarkDel(bid))
	require.False(t, msg.hasDelete(bid))

	firstTime = false
	// delete second time, skip markDelete
	err = mgr.deleteWithCheckVolConsistency(context.Background(), vid, delRet)
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
	blobTp.EXPECT().MarkDeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if !lastTime && info.Vuid.Index() == 0 {
			return errors.New("mock err")
		}
		return nil
	}).AnyTimes()
	blobTp.EXPECT().DeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).AnyTimes()

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

	delRet := &delBlobRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// first time
	err := mgr.deleteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.NotNil(t, err)

	require.False(t, msg.hasMarkDel(bid))
	require.False(t, msg.hasDelete(bid))

	// second time
	err = mgr.deleteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.NotNil(t, err)

	require.False(t, msg.hasMarkDel(bid))
	require.False(t, msg.hasDelete(bid))

	lastTime = true
	err = mgr.deleteWithCheckVolConsistency(context.Background(), vid, delRet)
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
	blobTp.EXPECT().MarkDeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		return nil
	}).AnyTimes()
	blobTp.EXPECT().DeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if !lastTime && info.Vuid.Index() == 0 {
			return apierr.ErrShardNotMarkDelete
		}
		return nil
	}).AnyTimes()

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

	delRet := &delBlobRet{
		msgExt: msg,
		ctx:    context.Background(),
	}

	// first time
	err := mgr.deleteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.NotNil(t, err)

	require.False(t, msg.hasMarkDel(bid))
	require.False(t, msg.hasDelete(bid))

	lastTime = true
	err = mgr.deleteWithCheckVolConsistency(context.Background(), vid, delRet)
	require.Nil(t, err)

	require.True(t, msg.hasMarkDel(bid))
	require.True(t, msg.hasDelete(bid))
}

func TestBlobDeleteMgr_DeleteFailed_Retry(t *testing.T) {
	vc := mocks.NewMockIVolumeCache(ctr(t))
	vc.EXPECT().DoubleCheckedRun(any, any, any).Return(errors.New("mock err"))

	vid := proto.Vid(1)
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

	mgr := newTestBlobDeleteMgr(t, nil, nil, vc)

	// delete
	err := mgr.executeDel(context.Background(), []*delMsgExt{msg})
	// retry, no err will be returned
	require.Nil(t, err)

	select {
	case receivedMsg := <-mgr.failedMsgChan:
		require.Equal(t, msg, receivedMsg)
	default:
		t.Fatal("no msg received in failedMsgChan")
	}
}

func TestBlobDeleteMgr_DeleteFailed_Punish(t *testing.T) {
	suid := proto.Suid(123)
	vc := mocks.NewMockIVolumeCache(ctr(t))
	vc.EXPECT().DoubleCheckedRun(any, any, any).Return(errors.New("mock err"))
	mgr := newTestBlobDeleteMgr(t, nil, nil, vc)

	vid := proto.Vid(1)
	minBid := proto.BlobID(1)
	tagNum := 2
	bidCnt := 5
	ts := mgr.tsGen.GenerateTs()
	id, _ := encodeRawDelMsgKey(ts, vid, minBid, tagNum)
	msg := &delMsgExt{
		msgKey: id,
		suid:   suid,
		msg: snproto.DeleteMsg{
			Retry: 2,
			Slice: proto.Slice{
				MinSliceID: minBid,
				Vid:        vid,
				Count:      uint32(bidCnt),
				ValidSize:  10,
			},
			MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
		},
	}

	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().ShardingSubRangeCount().Return(tagNum)
	sh.EXPECT().GetRouteVersion().Return(proto.RouteVersion(1))
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(nil)

	reader := newTestShardListReader(sh)
	mgr.shardListReaderMap.Store(suid, reader)

	// delete
	err := mgr.executeDel(context.Background(), []*delMsgExt{msg})
	// retry, no err will be returned
	require.Nil(t, err)

	select {
	case receivedMsg := <-mgr.failedMsgChan:
		require.Equal(t, msg, receivedMsg)
	default:
		t.Log("no msg received in failedMsgChan")
	}
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
	tp.EXPECT().MarkDeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if info.Vuid.Index()%2 == 0 {
			return apierr.ErrDiskBroken
		}
		return nil
	}).Times(6)
	tp.EXPECT().MarkDeleteSlice(any, any, any).Return(nil).Times(3)

	mgr := &BlobDeleteMgr{
		cfg: &BlobDelMgrConfig{
			Transport: tp,
			VolCache:  vc,
		},
	}

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
	tp.EXPECT().MarkDeleteSlice(any, any, any).DoAndReturn(func(ctx context.Context, info proto.VunitLocation, bid proto.BlobID) error {
		if info.Vuid.Index()%2 == 0 {
			return apierr.ErrDiskBroken
		}
		return nil
	}).Times(6)

	mgr := &BlobDeleteMgr{
		cfg: &BlobDelMgrConfig{
			Transport: tp,
			VolCache:  vc,
		},
	}

	msgExt := &delMsgExt{msg: snproto.DeleteMsg{
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}}

	_, err := mgr.deleteSlice(context.Background(), volInfo, msgExt, proto.BlobID(1), true)
	require.NotNil(t, err)
}

func TestBlobDeleteMgr_DeleteShard_BackToInitStage(t *testing.T) {
	tp := mocks.NewMockBlobTransport(ctr(t))
	tp.EXPECT().DeleteSlice(any, any, any).Return(apierr.ErrShardNotMarkDelete)

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	mgr := &BlobDeleteMgr{
		cfg: &BlobDelMgrConfig{
			Transport: tp,
		},
	}

	idx := 0
	bid := proto.BlobID(1)
	vunit := volInfo.VunitLocations[idx]

	msgExt := &delMsgExt{msg: snproto.DeleteMsg{
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}}
	msgExt.setShardDelStage(bid, vunit.Vuid, DeleteStageMarkDelete)

	err := mgr.deleteShard(context.Background(), vunit, bid, msgExt, false)
	require.Equal(t, err, apierr.ErrShardNotMarkDelete)
	require.False(t, msgExt.hasShardMarkDel(bid, vunit.Vuid))
}

func TestBlobDeleteMgr_DeleteShard_AssumeSuccess(t *testing.T) {
	tp := mocks.NewMockBlobTransport(ctr(t))
	tp.EXPECT().DeleteSlice(any, any, any).Return(apierr.ErrNoSuchBid)

	// mock volume info
	vid := proto.Vid(1)
	volInfo := newMockSimpleVolumeInfo(vid)

	mgr := &BlobDeleteMgr{
		cfg: &BlobDelMgrConfig{
			Transport: tp,
		},
	}

	idx := 0
	bid := proto.BlobID(1)
	vunit := volInfo.VunitLocations[idx]

	msgExt := &delMsgExt{msg: snproto.DeleteMsg{
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}}
	msgExt.setShardDelStage(bid, vunit.Vuid, DeleteStageMarkDelete)

	err := mgr.deleteShard(context.Background(), vunit, bid, msgExt, false)
	require.Nil(t, err)
	require.True(t, msgExt.hasShardDelete(bid, vunit.Vuid))
}

func TestBlobDeleteMgr_Punish(t *testing.T) {
	suid := proto.Suid(123)
	tagNum := 2
	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().ShardingSubRangeCount().Return(tagNum).Times(2)
	sh.EXPECT().GetRouteVersion().Return(proto.RouteVersion(1)).Times(4)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(apierr.ErrShardRouteVersionNeedUpdate)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(apierr.ErrShardRouteVersionNeedUpdate)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(nil)
	mockErr := errors.New("mock error")
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(mockErr)

	mgr := newTestBlobDeleteMgr(t, nil, nil, nil)
	reader := newTestShardListReader(sh)
	mgr.shardListReaderMap.Store(suid, reader)

	// mock punish msg
	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	vid := proto.Vid(1)
	bid := proto.BlobID(100)
	ts := mgr.tsGen.GenerateTs()
	id, _ := encodeRawDelMsgKey(ts, vid, bid, tagNum)

	msg := &delMsgExt{
		suid: proto.Suid(123),
		msg: snproto.DeleteMsg{
			Slice: proto.Slice{
				MinSliceID: bid,
				Vid:        vid,
				Count:      1,
				ValidSize:  10,
			},
			Time: oldTime,
		},
		msgKey: id,
	}

	err := mgr.punish(context.Background(), msg)
	require.Nil(t, err)

	// punish failed
	err = mgr.punish(context.Background(), msg)
	require.Equal(t, errors.Cause(err), mockErr)
}

func TestBlobDeleteMgr_ClearShardMessages(t *testing.T) {
	suid := proto.Suid(123)
	tagNum := 2
	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().ShardingSubRangeCount().Return(tagNum)
	sh.EXPECT().GetRouteVersion().Return(proto.RouteVersion(1)).Times(3)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(apierr.ErrShardRouteVersionNeedUpdate)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(apierr.ErrShardRouteVersionNeedUpdate)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(nil)

	mgr := newTestBlobDeleteMgr(t, nil, nil, nil)
	reader := newTestShardListReader(sh)
	mgr.shardListReaderMap.Store(suid, reader)

	delItems := make([]storage.BatchItemElem, 3)
	for i := 0; i < 3; i++ {
		ts := mgr.tsGen.GenerateTs()
		id, _ := encodeRawDelMsgKey(ts, proto.Vid(i), proto.BlobID(i), tagNum)
		delItems[i] = storage.NewBatchItemElemDelete(string(id))
	}
	mgr.clearShardMessages(context.Background(), proto.Suid(123), delItems)
}

func TestBlobDeleteMgr_ProduceLoop(t *testing.T) {
	cmClient := mocks.NewMockClientAPI(ctr(t))
	cmClient.EXPECT().GetConfig(any, any).DoAndReturn(func(_ context.Context, name string) (string, error) {
		if name == snproto.ShardNodeBlobDeleteTask {
			return taskswitch.SwitchOpen, nil
		}
		return "", nil
	}).AnyTimes()
	taskSwitchMgr := taskswitch.NewSwitchMgr(cmClient)
	sw, err := taskSwitchMgr.AddSwitch(snproto.ShardNodeBlobDeleteTask)
	require.NoError(t, err)

	shardGetter := mock.NewMockDelMgrShardGetter(ctr(t))
	shardHandler := mock.NewMockSpaceShardHandler(ctr(t))

	cfg := &BlobDelMgrConfig{
		ShardGetter: shardGetter,
		BlobDelCfg: BlobDelCfg{
			MsgChannelNum: 1,
		},
	}

	mgr := &BlobDeleteMgr{
		taskSwitch:  sw,
		cfg:         cfg,
		msgChannels: make([]chan *delMsgExt, 1),
		Closer:      closer.New(),
	}
	mgr.msgChannels[0] = make(chan *delMsgExt, 1)

	shardGetter.EXPECT().GetAllShards().Return([]storage.ShardHandler{shardHandler}).AnyTimes()
	shardHandler.EXPECT().GetDiskID().Return(proto.DiskID(1)).AnyTimes()
	shardHandler.EXPECT().GetSuid().Return(proto.Suid(1)).AnyTimes()
	shardHandler.EXPECT().ListItem(any, any, any, any, any).Return(nil, nil, nil).AnyTimes()

	go mgr.produceLoop()

	time.Sleep(2 * time.Second)
	mgr.Close()
}

func TestBlobDeleteMgr_CleanupDeletedShards(t *testing.T) {
	shardGetter := mock.NewMockDelMgrShardGetter(ctr(t))
	shardHandler := mock.NewMockSpaceShardHandler(ctr(t))

	cfg := &BlobDelMgrConfig{
		ShardGetter: shardGetter,
	}

	mgr := &BlobDeleteMgr{
		cfg: cfg,
	}

	diskID := proto.DiskID(1)
	suid := proto.Suid(1)

	// mock not exist shard
	shardGetter.EXPECT().GetShard(diskID, suid).Return(nil, apierr.ErrShardDoesNotExist)

	shards := []storage.ShardHandler{shardHandler}
	shardHandler.EXPECT().GetDiskID().Return(diskID)
	shardHandler.EXPECT().GetSuid().Return(suid)

	ctx := context.Background()
	mgr.cleanupDeletedShards(ctx, shards)
}

func TestBlobDeleteMgr_GetChan(t *testing.T) {
	cfg := &BlobDelMgrConfig{
		BlobDelCfg: BlobDelCfg{
			MsgChannelNum: 4,
		},
	}

	mgr := &BlobDeleteMgr{
		cfg:         cfg,
		msgChannels: make([]chan *delMsgExt, 4),
	}

	// test get chan
	shardID := proto.ShardID(10)
	ch := mgr.getChan(shardID)
	expectedIdx := uint32(shardID) % 4
	require.Equal(t, mgr.msgChannels[expectedIdx], ch)
}

func TestBlobDeleteMgr_SentToFailedChan(t *testing.T) {
	cfg := &BlobDelMgrConfig{
		BlobDelCfg: BlobDelCfg{
			FailedMsgChannelSize: 1,
		},
	}

	mgr := &BlobDeleteMgr{
		cfg:           cfg,
		failedMsgChan: make(chan *delMsgExt, 1),
	}

	ctx := context.Background()
	msgExt := &delMsgExt{
		msg: snproto.DeleteMsg{
			Slice: proto.Slice{
				Vid:   proto.Vid(1),
				Count: 5,
			},
		},
	}

	mgr.sentToFailedChan(ctx, msgExt)

	select {
	case receivedMsg := <-mgr.failedMsgChan:
		require.Equal(t, msgExt, receivedMsg)
	default:
		t.Fatal("no msg received in failedMsgChan")
	}
}

func TestBlobDeleteMgr_RetryFailedLoop(t *testing.T) {
	mgr := &BlobDeleteMgr{
		failedMsgChan: make(chan *delMsgExt, 1),
		msgChannels:   make([]chan *delMsgExt, 1),
		Closer:        closer.New(),
	}
	mgr.msgChannels[0] = make(chan *delMsgExt, 1)

	// start retry failed loop
	go mgr.retryFailedLoop()

	// mock failed msg
	msgExt := &delMsgExt{
		suid: proto.Suid(1),
		msg: snproto.DeleteMsg{
			Slice: proto.Slice{
				Vid:   proto.Vid(1),
				Count: 1,
			},
		},
	}

	// sent
	mgr.failedMsgChan <- msgExt

	// wait received from msgChannels
	select {
	case retriedMsg := <-mgr.msgChannels[0]:
		require.Equal(t, msgExt, retriedMsg)
	case <-time.After(time.Second):
		t.Fatal("msg should by retried")
	}

	mgr.Close()
}

func TestBlobDeleteMgr_ConsumeLoop(t *testing.T) {
	cfg := &BlobDelMgrConfig{
		BlobDelCfg: BlobDelCfg{
			MsgChannelNum: 1,
		},
	}

	mgr := &BlobDeleteMgr{
		cfg:         cfg,
		msgChannels: make([]chan *delMsgExt, 1),
		Closer:      closer.New(),
	}
	mgr.msgChannels[0] = make(chan *delMsgExt, 1)

	go mgr.startConsume()

	time.Sleep(100 * time.Millisecond)
	mgr.Close()
}

func TestBlobDeleteMgr_InsertDeleteMsg(t *testing.T) {
	suid := proto.Suid(123)
	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().ShardingSubRangeCount().Return(2)
	sh.EXPECT().InsertItem(any, any, any, any).Return(nil)

	sg := mock.NewMockDelMgrShardGetter(ctr(t))
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
	for i := range items {
		_, _, _, _shardKeys, err := decodeDelMsgKey([]byte(items[i].ID), len(shardKeys))
		require.Nil(t, err)
		require.Equal(t, shardKeys, _shardKeys)
		require.True(t, bytes.Contains([]byte(items[i].ID), snproto.DeleteMsgPrefix))
	}
}
