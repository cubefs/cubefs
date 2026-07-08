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
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

// --- fixtures ---

func newBlobDeleteMgr(t *testing.T) *BlobDeleteMgr {
	t.Helper()
	ctr := gomock.NewController(t)
	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)

	clusterTopology := NewMockClusterTopology(ctr)
	clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
		func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
			return &client.VolumeInfoSimple{Vid: vid}, nil
		},
	)
	clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
	clusterTopology.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{}, true)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)
	taskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeBlobDelete.String())
	require.NoError(t, err)

	blobnodeCli := NewMockBlobnodeAPI(ctr)
	blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(nil)
	blobnodeCli.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	producer := NewMockProducer(ctr)
	producer.EXPECT().SendMessage(any).AnyTimes().Return(nil)

	delLogger := mocks.NewMockRecordLogEncoder(ctr)
	delLogger.EXPECT().Close().AnyTimes().Return(nil)
	delLogger.EXPECT().Encode(any).AnyTimes().Return(nil)
	tp := taskpool.New(2, 2)
	ftp := taskpool.New(2, 2)

	return &BlobDeleteMgr{
		taskSwitch:   taskSwitch,
		taskPool:     &tp,
		failTaskPool: &ftp,

		safeDelayTime:   time.Hour,
		clusterTopology: clusterTopology,
		punishTime:      time.Duration(defaultMessagePunishTimeM) * time.Minute,
		blobnodeCli:     blobnodeCli,
		failMsgSender:   producer,

		delSuccessCounter:    base.NewCounter(1, "delete", base.KindSuccess),
		delFailCounter:       base.NewCounter(1, "delete", base.KindFailed),
		errStatsDistribution: base.NewErrorStats(),
		delLogger:            delLogger,
		deleteLimiter:        rate.NewLimiter(10, 10),

		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},

		Closer: closer.New(),
		cfg: &BlobDeleteConfig{
			MessagePunishThreshold: defaultMessagePunishThreshold,
			MaxBatchSize:           defaultMaxBatchSize,
			BatchIntervalS:         1,
		},
	}
}

func defaultDeleteMsg() *proto.DeleteMsg {
	return &proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "123456"}
}

func deleteKafkaMsg(msg *proto.DeleteMsg) *sarama.ConsumerMessage {
	b, _ := json.Marshal(msg)
	return &sarama.ConsumerMessage{Value: b}
}

type deleteTopoOpts struct {
	useNewVuid bool
	brokenDisk bool
	diskFound  bool
	brokenAny  bool
	withUpdate bool
	updateFn   func(vid proto.Vid) (*client.VolumeInfoSimple, error)
	diskID     proto.DiskID
}

func mockDeleteTopology(t *testing.T, ctr *gomock.Controller, opts deleteTopoOpts) *MockClusterTopology {
	t.Helper()
	if !opts.diskFound && opts.diskID == 0 {
		opts.diskFound = true
	}
	topo := NewMockClusterTopology(ctr)
	if opts.diskFound {
		topo.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{}, true)
	} else {
		topo.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{}, false)
	}
	topo.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
		func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
			loc := proto.VunitLocation{Vuid: 1}
			if opts.useNewVuid {
				loc.Vuid, _ = proto.NewVuid(vid, 0, 1)
			}
			if opts.diskID != 0 {
				loc.DiskID = opts.diskID
			}
			return &client.VolumeInfoSimple{Vid: vid, VunitLocations: []proto.VunitLocation{loc}}, nil
		},
	)
	if opts.brokenAny {
		topo.EXPECT().IsBrokenDisk(any).AnyTimes().Return(opts.brokenDisk)
	} else if opts.brokenDisk {
		topo.EXPECT().IsBrokenDisk(any).Return(true)
	} else {
		topo.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
	}
	if opts.withUpdate {
		if opts.updateFn != nil {
			topo.EXPECT().UpdateVolume(any).DoAndReturn(opts.updateFn)
		} else {
			topo.EXPECT().UpdateVolume(any).AnyTimes().Return(nil, nil)
		}
	}
	return topo
}

func swapTopology(mgr *BlobDeleteMgr, topo IClusterTopology) func() {
	old := mgr.clusterTopology
	mgr.clusterTopology = topo
	return func() { mgr.clusterTopology = old }
}

func swapBlobnode(mgr *BlobDeleteMgr, cli client.BlobnodeAPI) func() {
	old := mgr.blobnodeCli
	mgr.blobnodeCli = cli
	return func() { mgr.blobnodeCli = old }
}

func consumeRet(mgr *BlobDeleteMgr, ctx context.Context, msg *proto.DeleteMsg, stop closer.Closer) delBlobRet {
	ret := delBlobRet{delMsg: msg, ctx: ctx}
	mgr.consume(&ret, stop)
	return ret
}

func requireDeleteDone(t *testing.T, msg *proto.DeleteMsg, ret delBlobRet) {
	t.Helper()
	require.Equal(t, DeleteStatusDone, ret.status)
	require.Len(t, msg.BlobDelStages.Stages, 1)
	for _, stage := range msg.BlobDelStages.Stages {
		require.Equal(t, proto.DeleteStageDelete, stage)
	}
}

// --- kafka consume path ---

func TestBlobDeleteConsumeKafka(t *testing.T) {
	ctr := gomock.NewController(t)
	mgr := newBlobDeleteMgr(t)
	stop := closer.New()
	defer stop.Close()

	t.Run("invalid_messages", func(t *testing.T) {
		msgs := []*sarama.ConsumerMessage{
			deleteKafkaMsg(&proto.DeleteMsg{}),
			{Value: []byte("123")},
		}
		require.True(t, mgr.Consume(msgs, stop))
	})

	t.Run("success", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restore()
		require.True(t, mgr.Consume([]*sarama.ConsumerMessage{deleteKafkaMsg(defaultDeleteMsg())}, stop))
	})

	t.Run("mark_delete_failed_still_returns_true", func(t *testing.T) {
		restoreTopo := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restoreTopo()
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(errMock)
		restoreCli := swapBlobnode(mgr, blobnode)
		defer restoreCli()

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "markDeleteFailed"}
		require.True(t, mgr.Consume([]*sarama.ConsumerMessage{deleteKafkaMsg(msg)}, stop))
	})

	t.Run("protected_message_cancelled", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restore()
		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "protected", Time: time.Now().Unix() - 1}
		cancel := closer.New()
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel.Close()
		}()
		oldDelay := mgr.safeDelayTime
		mgr.safeDelayTime = time.Hour
		defer func() { mgr.safeDelayTime = oldDelay }()
		require.False(t, mgr.Consume([]*sarama.ConsumerMessage{deleteKafkaMsg(msg)}, cancel))
	})

	t.Run("overload_triggers_slowdown", func(t *testing.T) {
		restoreTopo := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restoreTopo()
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(bloberr.ErrOverload)
		restoreCli := swapBlobnode(mgr, blobnode)
		defer restoreCli()

		oldSlow := mgr.slowDownTime
		mgr.slowDownTime = time.Second * defaultSlowDownTimeS
		defer func() { mgr.slowDownTime = oldSlow }()

		start := time.Now()
		require.True(t, mgr.Consume([]*sarama.ConsumerMessage{deleteKafkaMsg(defaultDeleteMsg())}, stop))
		require.GreaterOrEqual(t, time.Since(start), time.Duration(defaultSlowDownTimeS)*time.Second)
	})
}

// --- direct consume path ---

func TestBlobDeleteConsumeDirect(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	mgr := newBlobDeleteMgr(t)
	stop := closer.New()
	defer stop.Close()

	t.Run("punished_retry_success", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restore()
		msg := &proto.DeleteMsg{
			Bid: 1, Vid: 1, ReqId: "123456", Retry: 4,
			FailTime: time.Now().Unix() - int64(mgr.punishTime.Seconds()) + 1,
		}
		ret := consumeRet(mgr, ctx, msg, stop)
		requireDeleteDone(t, msg, ret)
	})

	t.Run("success", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restore()
		msg := defaultDeleteMsg()
		ret := consumeRet(mgr, ctx, msg, stop)
		requireDeleteDone(t, msg, ret)
	})

	t.Run("skip_when_mark_deleted", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{useNewVuid: true}))
		defer restore()
		msg := defaultDeleteMsg()
		msg.BlobDelStages = proto.BlobDeleteStage{Stages: map[uint8]proto.DeleteStage{0: proto.DeleteStageMarkDelete}}
		ret := consumeRet(mgr, ctx, msg, stop)
		requireDeleteDone(t, msg, ret)
	})

	t.Run("skip_when_already_deleted", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{useNewVuid: true}))
		defer restore()
		msg := defaultDeleteMsg()
		msg.BlobDelStages = proto.BlobDeleteStage{Stages: map[uint8]proto.DeleteStage{0: proto.DeleteStageDelete}}
		ret := consumeRet(mgr, ctx, msg, stop)
		requireDeleteDone(t, msg, ret)
	})

	t.Run("protected_within_delay", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restore()
		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "protected", Time: time.Now().Unix() - 1}
		oldDelay := mgr.safeDelayTime
		mgr.safeDelayTime = 2 * time.Second
		defer func() { mgr.safeDelayTime = oldDelay }()
		ret := consumeRet(mgr, ctx, msg, stop)
		require.Equal(t, DeleteStatusDone, ret.status)
	})

	t.Run("protected_cancelled", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restore()
		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "protected", Time: time.Now().Unix() - 1}
		cancel := closer.New()
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel.Close()
		}()
		oldDelay := mgr.safeDelayTime
		mgr.safeDelayTime = time.Hour
		defer func() { mgr.safeDelayTime = oldDelay }()
		ret := consumeRet(mgr, ctx, msg, cancel)
		require.Equal(t, DeleteStatusUndo, ret.status)
	})

	t.Run("mark_delete_failed", func(t *testing.T) {
		restoreTopo := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restoreTopo()
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(errMock)
		restoreCli := swapBlobnode(mgr, blobnode)
		defer restoreCli()

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "markDeleteFailed"}
		ret := consumeRet(mgr, ctx, msg, stop)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.ErrorIs(t, ret.err, errMock)
	})

	t.Run("disk_broken", func(t *testing.T) {
		restoreTopo := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{
			withUpdate: true,
			updateFn: func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{Vid: vid, VunitLocations: []proto.VunitLocation{{Vuid: 1}}}, nil
			},
		}))
		defer restoreTopo()
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrDiskBroken)
		restoreCli := swapBlobnode(mgr, blobnode)
		defer restoreCli()

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
		ret := consumeRet(mgr, ctx, msg, stop)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.Nil(t, msg.BlobDelStages.Stages)
		require.ErrorIs(t, ret.err, errcode.ErrDiskBroken)
	})

	t.Run("disk_broken_volume_unchanged", func(t *testing.T) {
		restoreTopo := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{
			withUpdate: true,
			updateFn: func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}, {Vuid: 2}},
				}, nil
			},
		}))
		defer restoreTopo()
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errcode.ErrDiskBroken)
		restoreCli := swapBlobnode(mgr, blobnode)
		defer restoreCli()

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
		ret := consumeRet(mgr, ctx, msg, stop)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.Nil(t, msg.BlobDelStages.Stages)

		restoreTopo2 := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{
			withUpdate: true,
			updateFn: func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{Vid: vid, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil
			},
		}))
		defer restoreTopo2()
		ret = consumeRet(mgr, ctx, msg, stop)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.ErrorIs(t, ret.err, errcode.ErrDiskBroken)
	})

	t.Run("broken_disk_skips_blobnode", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{
			brokenDisk: true,
			brokenAny:  true,
			diskID:     testDisk1.DiskID,
			withUpdate: true,
		}))
		defer restore()
		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
		ret := consumeRet(mgr, ctx, msg, stop)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.Nil(t, msg.BlobDelStages.Stages)
	})

	t.Run("punished_message_skips_delete", func(t *testing.T) {
		restore := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{}))
		defer restore()
		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed", Retry: defaultMessagePunishThreshold}
		oldPunish := mgr.punishTime
		mgr.punishTime = 10 * time.Millisecond
		defer func() { mgr.punishTime = oldPunish }()
		ret := consumeRet(mgr, ctx, msg, stop)
		requireDeleteDone(t, msg, ret)
	})

	t.Run("punished_message_cancelled", func(t *testing.T) {
		cancel := closer.New()
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel.Close()
		}()
		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed", Retry: defaultMessagePunishThreshold}
		ret := consumeRet(mgr, ctx, msg, cancel)
		require.Equal(t, DeleteStatusUndo, ret.status)
	})

	t.Run("repaired_disk_update_volume_failed", func(t *testing.T) {
		restoreTopo := swapTopology(mgr, mockDeleteTopology(t, ctr, deleteTopoOpts{
			diskFound:  false,
			diskID:     testDisk1.DiskID,
			withUpdate: true,
			updateFn: func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return nil, errMock
			},
		}))
		defer restoreTopo()
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(bloberr.ErrRequestTimeout)
		restoreCli := swapBlobnode(mgr, blobnode)
		defer restoreCli()

		msg := &proto.DeleteMsg{Bid: 10, Vid: 3, ReqId: "delete failed"}
		ret := consumeRet(mgr, ctx, msg, stop)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.Nil(t, msg.BlobDelStages.Stages)
	})
}

func TestNewDeleteMgr(t *testing.T) {
	ctr := gomock.NewController(t)
	broker0 := NewBroker(t)
	defer broker0.Close()

	testDir, err := os.MkdirTemp(os.TempDir(), "delete_log")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	blobCfg := &BlobDeleteConfig{
		ClusterID:    0,
		TaskPoolSize: 2,
		DeleteHourRange: HourRange{
			From: 0,
			To:   defaultDeleteHourRangeTo,
		},
		DeleteLog: recordlog.Config{
			Dir:       testDir,
			ChunkBits: 22,
		},
		Kafka: BlobDeleteKafkaConfig{
			BrokerList:             []string{broker0.Addr()},
			TopicNormal:            testTopic,
			TopicFailed:            testTopic,
			FailMsgSenderTimeoutMs: 0,
		},
	}

	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("", errMock)
	clusterMgrCli.EXPECT().GetConsumeOffset(any, any, any).AnyTimes().Return(int64(0), nil)
	clusterMgrCli.EXPECT().SetConsumeOffset(any, any, any, any).AnyTimes().Return(nil)

	clusterTopology := NewMockClusterTopology(ctr)
	blobnodeCli := NewMockBlobnodeAPI(ctr)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)

	kafkaClient := NewMockKafkaConsumer(ctr)
	consumer := NewMockGroupConsumer(ctr)
	consumer.EXPECT().Stop().AnyTimes().Return()
	kafkaClient.EXPECT().StartKafkaConsumer(any, any).AnyTimes().Return(consumer, nil)

	mgr, err := NewBlobDeleteMgr(blobCfg, clusterTopology, switchMgr, blobnodeCli, kafkaClient)
	require.NoError(t, err)
	require.False(t, mgr.Enabled())
	mgr.Run()
	require.NoError(t, mgr.startConsumer())
	mgr.stopConsumer()
	require.Nil(t, mgr.consumers)
	mgr.Close()
	mgr.GetTaskStats()
	mgr.GetErrorStats()
}

func TestAllowDeleting(t *testing.T) {
	now := time.Now()
	mgr := &BlobDeleteMgr{}
	testCases := []struct {
		hourRange HourRange
		now       time.Time
		ok        bool
		waitTime  time.Duration
	}{
		{hourRange: HourRange{0, 1}, now: time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()), ok: true},
		{hourRange: HourRange{0, 1}, now: time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, now.Location()), ok: false, waitTime: 23 * time.Hour},
		{hourRange: HourRange{0, 2}, now: time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, now.Location()), ok: true},
		{hourRange: HourRange{0, 23}, now: time.Date(now.Year(), now.Month(), now.Day(), 23, 10, 0, 0, now.Location()), ok: false, waitTime: 50 * time.Minute},
		{hourRange: HourRange{1, 2}, now: time.Date(now.Year(), now.Month(), now.Day(), 3, 0, 0, 0, now.Location()), ok: false, waitTime: (21 + 1) * time.Hour},
		{hourRange: HourRange{2, 5}, now: time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, now.Location()), ok: false, waitTime: 1 * time.Hour},
	}
	for _, test := range testCases {
		mgr.deleteHourRange = test.hourRange
		waitTime, ok := mgr.allowDeleting(test.now)
		require.Equal(t, test.ok, ok)
		require.Equal(t, test.waitTime, waitTime)
	}
}

func TestDeleteBlob(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	volume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)

	t.Run("mark_delete_failed", func(t *testing.T) {
		mgr := newBlobDeleteMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errMock)
		mgr.blobnodeCli = blobnode

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errMock)
		require.True(t, doneVolume.EqualWith(volume))
	})

	t.Run("shard_not_mark_delete_rollback_and_retry", func(t *testing.T) {
		mgr := newBlobDeleteMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().Delete(any, any, any).Return(errcode.ErrShardNotMarkDelete)
		mgr.blobnodeCli = blobnode

		stages := make(map[uint8]proto.DeleteStage)
		for i := 0; i < codemode.EC3P3.GetShardNum(); i++ {
			stages[uint8(i)] = proto.DeleteStageDelete
		}
		stages[1] = proto.DeleteStageMarkDelete
		msg := &proto.DeleteMsg{Bid: proto.BlobID(1), BlobDelStages: proto.BlobDeleteStage{Stages: stages}}

		doneVolume, err := mgr.deleteBlob(ctx, volume, msg)
		require.ErrorIs(t, err, errcode.ErrShardNotMarkDelete)
		require.True(t, doneVolume.EqualWith(volume))
		require.Equal(t, proto.InitStage, msg.BlobDelStages.Stages[1])

		blobnode.EXPECT().MarkDelete(any, any, any).Return(nil)
		blobnode.EXPECT().Delete(any, any, any).Return(nil)
		doneVolume, err = mgr.deleteBlob(ctx, volume, msg)
		require.NoError(t, err)
		require.True(t, doneVolume.EqualWith(volume))
	})

	t.Run("update_volume_cache_failed", func(t *testing.T) {
		mgr := newBlobDeleteMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		mgr.blobnodeCli = blobnode

		topo := NewMockClusterTopology(ctr)
		topo.EXPECT().UpdateVolume(any).Return(volume, errcode.ErrUpdateVolCacheFreq)
		topo.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{}, true)
		mgr.clusterTopology = topo

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errcode.ErrNoSuchVuid)
		require.True(t, doneVolume.EqualWith(volume))
	})

	t.Run("update_volume_unchanged", func(t *testing.T) {
		mgr := newBlobDeleteMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		mgr.blobnodeCli = blobnode

		topo := NewMockClusterTopology(ctr)
		topo.EXPECT().UpdateVolume(any).Return(volume, nil)
		topo.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{}, true)
		mgr.clusterTopology = topo

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errcode.ErrNoSuchVuid)
		require.True(t, doneVolume.EqualWith(volume))
	})

	t.Run("retry_after_volume_change", func(t *testing.T) {
		mgr := newBlobDeleteMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		blobnode.EXPECT().MarkDelete(any, any, any).Return(nil)
		blobnode.EXPECT().Delete(any, any, any).Times(6).Return(nil)
		mgr.blobnodeCli = blobnode

		newVolume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
		newVolume.VunitLocations[5].Vuid++
		topo := NewMockClusterTopology(ctr)
		topo.EXPECT().UpdateVolume(any).Return(newVolume, nil)
		topo.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{}, true)
		mgr.clusterTopology = topo

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.NoError(t, err)
		require.False(t, doneVolume.EqualWith(volume))
		require.True(t, doneVolume.EqualWith(newVolume))
	})
}
