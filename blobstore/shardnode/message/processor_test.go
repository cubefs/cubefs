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

func TestBlobMessageMgr_New(t *testing.T) {
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
	mgr.Close()
}

func TestBlobMessageMgr_ListShardMsg(t *testing.T) {
	ctx := context.Background()

	diskID := proto.DiskID(1)
	suid := proto.Suid(123)

	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().IsLeader().Return(true)
	sh.EXPECT().GetSuid().Return(suid).Times(2)

	sg := mock.NewMockMessageMgrShardGetter(ctr(t))
	sg.EXPECT().GetShard(any, any).Return(sh, nil)

	executor := mock.NewMockMessageExecutorMgr(ctr(t))
	executor.EXPECT().ItemToMessageExt(any).DoAndReturn(func(item interface{}) (interface{}, error) {
		return &delMsgExt{
			suid: suid,
			msg:  snproto.DeleteMsg{},
		}, nil
	}).AnyTimes()

	mgr := newTestBlobMessageMgr(t, sg, nil, nil, executor, snproto.MessageTypeDelete)

	reader := newTestShardListReader(sh, &mgr.cfg.TierConfig)
	_hasMsg, err := mgr.listShardMsg(ctx, diskID, reader)
	require.Nil(t, err)
	require.True(t, _hasMsg)
}

func TestBlobMessageMgr_RunConsumeTask(t *testing.T) {
	suid := proto.Suid(123)
	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().ShardingSubRangeCount().Return(2).AnyTimes()
	sh.EXPECT().GetRouteVersion().Return(proto.RouteVersion(1)).AnyTimes()
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(nil).AnyTimes()

	executor := mock.NewMockMessageExecutorMgr(ctr(t))
	executor.EXPECT().ExecuteWithCheckVolConsistency(any, any, any).Return(nil).AnyTimes()
	executor.EXPECT().ItemToMessageExt(any).DoAndReturn(func(item interface{}) (interface{}, error) {
		return &delMsgExt{
			suid: suid,
			msg:  snproto.DeleteMsg{},
		}, nil
	}).AnyTimes()

	mgr := newTestBlobMessageMgr(t, nil, nil, nil, executor, snproto.MessageTypeDelete)
	reader := newTestShardListReader(sh, &mgr.cfg.TierConfig)
	mgr.topShardReaderMap.Store(suid, reader)

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
		mgr.msgChannelsByTier[snproto.TierSingleIdx] <- &delMsgExt{
			suid: suid,
			msg: snproto.DeleteMsg{
				Slice: slice,
			},
			msgKey: id,
		}
	}
	go mgr.runConsumeTask(context.Background(), snproto.TierSingleIdx)
	for {
		if len(mgr.msgChannelsByTier[snproto.TierSingleIdx]) == 0 {
			break
		}
	}
}

func TestBlobMessageMgr_ExecuteFailed_Retry(t *testing.T) {
	executor := mock.NewMockMessageExecutorMgr(ctr(t))
	executor.EXPECT().ExecuteWithCheckVolConsistency(any, any, any).Return(errors.New("mock err")).AnyTimes()
	executor.EXPECT().ItemToMessageExt(any).DoAndReturn(func(item interface{}) (interface{}, error) {
		return &delMsgExt{
			suid: proto.Suid(123),
			msg:  snproto.DeleteMsg{},
		}, nil
	}).AnyTimes()

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

	mgr := newTestBlobMessageMgr(t, nil, nil, nil, executor, snproto.MessageTypeDelete)

	// delete
	err := mgr.execute(context.Background(), []snproto.MessageExt{msg})
	// retry, no err will be returned
	require.Nil(t, err)

	select {
	case receivedMsg := <-mgr.failedMsgChan:
		require.Equal(t, msg, receivedMsg)
	default:
		t.Fatal("no msg received in failedMsgChan")
	}
}

func TestBlobMessageMgr_ExecuteFailed_Punish(t *testing.T) {
	suid := proto.Suid(123)
	executor := mock.NewMockMessageExecutorMgr(ctr(t))
	executor.EXPECT().ExecuteWithCheckVolConsistency(any, any, any).Return(errors.New("mock err")).AnyTimes()
	executor.EXPECT().ItemToMessageExt(any).DoAndReturn(func(item interface{}) (interface{}, error) {
		return &delMsgExt{
			suid: suid,
			msg:  snproto.DeleteMsg{},
		}, nil
	}).AnyTimes()
	mgr := newTestBlobMessageMgr(t, nil, nil, nil, executor, snproto.MessageTypeDelete)

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

	reader := newTestShardListReader(sh, &mgr.cfg.TierConfig)
	mgr.topShardReaderMap.Store(suid, reader)

	// delete
	err := mgr.execute(context.Background(), []snproto.MessageExt{msg})
	// retry, no err will be returned
	require.Nil(t, err)

	select {
	case receivedMsg := <-mgr.failedMsgChan:
		require.Equal(t, msg, receivedMsg)
	default:
		t.Log("no msg received in failedMsgChan")
	}
}

func TestBlobMessageMgr_Punish(t *testing.T) {
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

	mgr := newTestBlobMessageMgr(t, nil, nil, nil, nil, snproto.MessageTypeDelete)
	reader := newTestShardListReader(sh, &mgr.cfg.TierConfig)
	mgr.topShardReaderMap.Store(suid, reader)

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

func TestBlobMessageMgr_ClearShardMessages(t *testing.T) {
	suid := proto.Suid(123)
	tagNum := 2
	sh := mock.NewMockSpaceShardHandler(ctr(t))
	sh.EXPECT().ShardingSubRangeCount().Return(tagNum)
	sh.EXPECT().GetRouteVersion().Return(proto.RouteVersion(1)).Times(3)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(apierr.ErrShardRouteVersionNeedUpdate)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(apierr.ErrShardRouteVersionNeedUpdate)
	sh.EXPECT().BatchWriteItem(any, any, any, any).Return(nil)

	mgr := newTestBlobMessageMgr(t, nil, nil, nil, nil, snproto.MessageTypeDelete)
	reader := newTestShardListReader(sh, &mgr.cfg.TierConfig)
	mgr.topShardReaderMap.Store(suid, reader)

	delItems := make([]storage.BatchItemElem, 3)
	for i := 0; i < 3; i++ {
		ts := mgr.tsGen.GenerateTs()
		id, _ := encodeRawDelMsgKey(ts, proto.Vid(i), proto.BlobID(i), tagNum)
		delItems[i] = storage.NewBatchItemElemDelete(string(id))
	}
	mgr.clearShardMessages(context.Background(), proto.Suid(123), delItems)
}

func TestBlobMessageMgr_ProduceLoop(t *testing.T) {
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

	shardGetter := mock.NewMockMessageMgrShardGetter(ctr(t))
	shardHandler := mock.NewMockSpaceShardHandler(ctr(t))

	cfg := &MessageMgrConfig{
		ShardGetter: shardGetter,
		MessageCfg:  MessageCfg{},
	}

	mgr := &messageMgr{
		taskSwitch:        sw,
		cfg:               cfg,
		msgChannelsByTier: make(map[snproto.MessageTier]chan snproto.MessageExt),
		Closer:            closer.New(),
	}

	shardGetter.EXPECT().GetAllShards().Return([]storage.ShardHandler{shardHandler}).AnyTimes()
	shardHandler.EXPECT().GetDiskID().Return(proto.DiskID(1)).AnyTimes()
	shardHandler.EXPECT().GetSuid().Return(proto.Suid(1)).AnyTimes()
	shardHandler.EXPECT().ListItem(any, any, any, any, any).Return(nil, nil, nil).AnyTimes()

	go mgr.produceLoop()

	time.Sleep(2 * time.Second)
	mgr.Close()
}

func TestBlobMessageMgr_CleanupDeletedShards(t *testing.T) {
	shardGetter := mock.NewMockMessageMgrShardGetter(ctr(t))
	shardHandler := mock.NewMockSpaceShardHandler(ctr(t))

	cfg := &MessageMgrConfig{
		ShardGetter: shardGetter,
	}

	mgr := &messageMgr{
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

func TestBlobMessageMgr_GetChan(t *testing.T) {
	cfg := &MessageMgrConfig{
		MessageCfg: MessageCfg{},
	}

	mgr := &messageMgr{
		cfg:               cfg,
		msgChannelsByTier: make(map[snproto.MessageTier]chan snproto.MessageExt),
	}
	mgr.msgChannelsByTier[snproto.TierSingleIdx] = make(chan snproto.MessageExt, 4)

	// test get chan
	ch := mgr.getChan(snproto.TierSingleIdx)
	require.Equal(t, mgr.msgChannelsByTier[snproto.TierSingleIdx], ch)
}

func TestBlobMessageMgr_SentToFailedChan(t *testing.T) {
	cfg := &MessageMgrConfig{
		MessageCfg: MessageCfg{
			FailedMsgChannelSize: 1,
		},
	}

	mgr := &messageMgr{
		cfg:           cfg,
		failedMsgChan: make(chan snproto.MessageExt, 1),
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

func TestBlobMessageMgr_RetryFailedLoop(t *testing.T) {
	mgr := &messageMgr{
		cfg: &MessageMgrConfig{
			MessageCfg: MessageCfg{
				RetryTimes: 3,
			},
		},
		failedMsgChan:     make(chan snproto.MessageExt, 1),
		msgChannelsByTier: make(map[snproto.MessageTier]chan snproto.MessageExt),
		Closer:            closer.New(),
	}

	mgr.msgChannelsByTier[snproto.TierSingleIdx] = make(chan snproto.MessageExt, 1)

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
	case retriedMsg := <-mgr.msgChannelsByTier[snproto.TierSingleIdx]:
		require.Equal(t, msgExt, retriedMsg)
	case <-time.After(time.Second):
		t.Fatal("msg should by retried")
	}

	mgr.Close()
}

func TestBlobDeleteMgr_ConsumeLoop(t *testing.T) {
	cfg := &MessageMgrConfig{
		MessageCfg: MessageCfg{},
	}

	mgr := &messageMgr{
		cfg:               cfg,
		msgChannelsByTier: make(map[snproto.MessageTier]chan snproto.MessageExt),
		Closer:            closer.New(),
	}

	mgr.msgChannelsByTier[snproto.TierSingleIdx] = make(chan snproto.MessageExt, 1)

	go mgr.startConsume()

	time.Sleep(100 * time.Millisecond)
	mgr.Close()
}

func encodeRawDelMsgKey(ts base.Ts, vid proto.Vid, bid proto.BlobID, tagNum int) ([]byte, []string) {
	delMsg := shardnode.DeleteBlobRawArgs{Slice: proto.Slice{MinSliceID: bid, Vid: vid}}
	shardKeys := delMsg.GetShardKeys(tagNum)
	mk := newMsgKey()
	defer mk.release()

	mk.setMsgType(snproto.MessageTypeDelete)
	mk.setTier(snproto.TierSingleIdx)
	mk.setTs(ts)
	mk.setVid(vid)
	mk.setBid(bid)
	mk.setShardKeys(shardKeys)
	encoded := mk.encode()

	result := make([]byte, len(encoded))
	copy(result, encoded)
	return result, shardKeys
}

func newTestPriorityConfig() TierConfig {
	return TierConfig{
		SafeMessageTimeout: util.Duration{Duration: time.Minute},
		PunishTimeout:      util.Duration{Duration: time.Minute},
		EnabledTiers: []snproto.MessageTier{
			snproto.TierSingleIdx,
			snproto.TierPunish,
		},
		tierArgs: map[snproto.MessageTier]tierArgs{
			snproto.TierSingleIdx: {
				count:        100,
				taskPoolSize: 2,
				channelSize:  10,
			},
			snproto.TierPunish: {
				count:        100,
				taskPoolSize: 2,
				channelSize:  10,
			},
		},
	}
}

func newTestBlobMessageMgr(t *testing.T, sg ShardGetter, tp base.BlobTransport, vc base.IVolumeCache, executor MessageExecutor, messageType snproto.MessageType) *messageMgr {
	cfg := &MessageMgrConfig{
		executor:      executor,
		ShardGetter:   sg,
		BlobTransport: tp,
		VolCache:      vc,
		MessageCfg: MessageCfg{
			RetryTimes:           3,
			FailedMsgChannelSize: 5,
			ProduceTaskPoolSize:  4,
			RateLimit:            100.0,
			RateLimitBurst:       10,
			MaxExecuteSliceNum:   1000,
			TierConfig:           newTestPriorityConfig(),
		},
	}
	cfg.MessageCfg.TierConfig.build(messageType)

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

	msgChannels := make(map[snproto.MessageTier]chan snproto.MessageExt)
	consumeTaskPools := make(map[snproto.MessageTier]taskpool.TaskPool)
	for tr := range cfg.TierConfig.tierArgs {
		_cfg, ok := cfg.TierConfig.tierArgs[tr]
		if !ok {
			t.Fatalf("tier[%d] concurrency config not found", tr)
		}
		msgChannels[tr] = make(chan snproto.MessageExt, _cfg.channelSize)
	}

	delLogger, err := recordlog.NewEncoder(nil)
	require.NoError(t, err)

	for tr := range cfg.TierConfig.tierArgs {
		_cfg, ok := cfg.TierConfig.tierArgs[tr]
		if !ok {
			t.Fatalf("priority[%d] concurrency config not found", tr)
		}
		consumeTaskPools[tr] = taskpool.New(_cfg.taskPoolSize, 1)
	}

	baseMgr := &messageMgr{
		cfg:                        cfg,
		taskSwitch:                 sw,
		tsGen:                      base.NewTsGenerator(base.Ts(0)),
		produceTaskPool:            taskpool.New(cfg.ProduceTaskPoolSize, 1),
		failedMsgChan:              make(chan snproto.MessageExt, cfg.FailedMsgChannelSize),
		limiter:                    rate.NewLimiter(rate.Limit(cfg.RateLimit), cfg.RateLimitBurst),
		msgChannelsByTier:          msgChannels,
		consumeTaskPoolsByTier:     consumeTaskPools,
		executeSuccessCounterByMin: &counter.Counter{},
		executeFailCounterByMin:    &counter.Counter{},
		errStatsDistribution:       base.NewErrorStats(),
		executeLogger:              delLogger,
		Closer:                     closer.New(),
	}
	return baseMgr
}

func newTestShardListReader(sh storage.ShardHandler, cfg *TierConfig) *tierShardListReader {
	cfg.build(snproto.MessageTypeDelete)
	reader := newTierShardListReader(sh, cfg)

	for t := range cfg.tierArgs {
		// add unprotected messages to cache, timestamp is 2 hours ago
		oldTime := time.Now().Add(-24 * time.Hour).Unix()
		unprotectedMsg := messageItem{
			time: oldTime,
			suid: proto.Suid(123),
		}

		// add protected message to cache, timestamp is now
		now := time.Now().Unix()
		protectedMsg := messageItem{
			time: now,
			suid: proto.Suid(123),
		}
		reader.readers[t].messages = append(reader.readers[t].messages, unprotectedMsg, protectedMsg)
	}
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
