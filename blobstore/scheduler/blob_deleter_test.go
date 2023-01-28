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
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/counter"
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

func newBlobDeleteMgr(t *testing.T) *BlobDeleteMgr {
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

	return &BlobDeleteMgr{
		taskSwitch: taskSwitch,
		taskPool:   &tp,

		safeDelayTime:   time.Hour,
		clusterTopology: clusterTopology,
		punishTime:      time.Duration(defaultMessagePunishTimeM) * time.Minute,
		blobnodeCli:     blobnodeCli,
		failMsgSender:   producer,

		delSuccessCounter:    base.NewCounter(1, "delete", base.KindSuccess),
		delFailCounter:       base.NewCounter(1, "delete", base.KindFailed),
		errStatsDistribution: base.NewErrorStats(),
		delLogger:            delLogger,

		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},

		Closer: closer.New(),
		cfg:    &BlobDeleteConfig{MessagePunishThreshold: defaultMessagePunishThreshold},
	}
}

func TestBlobDeleteConsume(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newBlobDeleteMgr(t)
	commonCloser := closer.New()
	defer commonCloser.Close()
	{
		// return invalid message
		msg := proto.DeleteMsg{}
		msgByte, _ := json.Marshal(msg)
		kafkaMsg := &sarama.ConsumerMessage{
			Value: msgByte,
		}
		success := mgr.Consume(kafkaMsg, commonCloser)
		require.True(t, success)

		kafkaMsg = &sarama.ConsumerMessage{
			Value: []byte("123"),
		}
		success = mgr.Consume(kafkaMsg, commonCloser)
		require.True(t, success)
	}
	{
		// consume success
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology
		msg := &proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "123456"}
		msgByte, _ := json.Marshal(msg)
		kafkaMsg := &sarama.ConsumerMessage{
			Value: msgByte,
		}
		success := mgr.Consume(kafkaMsg, commonCloser)
		require.True(t, success)
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// consume failed
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{Vid: vid, VunitLocations: []proto.VunitLocation{{Vuid: 1}}}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology

		oldBlobNode := mgr.blobnodeCli
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errMock)
		mgr.blobnodeCli = blobnodeCli

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "markDeleteFailed"}
		msgByte, _ := json.Marshal(msg)
		kafkaMsg := &sarama.ConsumerMessage{
			Value: msgByte,
		}
		success := mgr.Consume(kafkaMsg, commonCloser)
		require.True(t, success)
		mgr.clusterTopology = oldClusterTopology
		mgr.blobnodeCli = oldBlobNode
	}
	{
		// consume cancel
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology

		msg := &proto.DeleteMsg{
			Bid:   2,
			Vid:   2,
			ReqId: "protected",
			Time:  time.Now().Unix() - 1,
		}
		msgByte, _ := json.Marshal(msg)
		kafkaMsg := &sarama.ConsumerMessage{
			Value: msgByte,
		}
		mgr.safeDelayTime = 1 * time.Hour
		closer := closer.New()
		go func() {
			time.Sleep(10 * time.Millisecond)
			closer.Close()
		}()
		success := mgr.Consume(kafkaMsg, closer)
		require.False(t, success)
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// consume success
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology
		msg := &proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "123456"}

		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusDone, ret.status)
		require.Equal(t, 1, len(msg.BlobDelStages.Stages))
		for _, v := range msg.BlobDelStages.Stages {
			require.Equal(t, proto.DeleteStageDelete, v)
		}
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// has mark deleted and not send request to blobnode
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)

		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				vuid, _ := proto.NewVuid(vid, 0, 1)
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: vuid}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology
		stages := make(map[uint8]proto.DeleteStage)
		stages[0] = proto.DeleteStageMarkDelete
		msg := &proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "123456", BlobDelStages: proto.BlobDeleteStage{Stages: stages}}

		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusDone, ret.status)
		require.Equal(t, 1, len(msg.BlobDelStages.Stages))
		for _, v := range msg.BlobDelStages.Stages {
			require.Equal(t, proto.DeleteStageDelete, v)
		}
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// has deleted and not send request to blobnode
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)

		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				vuid, _ := proto.NewVuid(vid, 0, 1)
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: vuid}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology
		stages := make(map[uint8]proto.DeleteStage)
		stages[0] = proto.DeleteStageDelete
		msg := &proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "123456", BlobDelStages: proto.BlobDeleteStage{Stages: stages}}

		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusDone, ret.status)
		require.Equal(t, 1, len(msg.BlobDelStages.Stages))
		for _, v := range msg.BlobDelStages.Stages {
			require.Equal(t, proto.DeleteStageDelete, v)
		}
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// delete protected
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology

		msg := &proto.DeleteMsg{
			Bid:   2,
			Vid:   2,
			ReqId: "protected",
			Time:  time.Now().Unix() - 1,
		}
		mgr.safeDelayTime = 2 * time.Second
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusDone, ret.status)
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// delete protected and cancel
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology

		msg := &proto.DeleteMsg{
			Bid:   2,
			Vid:   2,
			ReqId: "protected",
			Time:  time.Now().Unix() - 1,
		}
		mgr.safeDelayTime = 1 * time.Hour
		closer := closer.New()
		go func() {
			time.Sleep(10 * time.Millisecond)
			closer.Close()
		}()
		ret := mgr.consume(ctx, msg, closer)
		require.Equal(t, DeleteStatusUndo, ret.status)
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// blobnode delete failed
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{Vid: vid, VunitLocations: []proto.VunitLocation{{Vuid: 1}}}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology

		oldBlobNode := mgr.blobnodeCli
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errMock)
		mgr.blobnodeCli = blobnodeCli

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "markDeleteFailed"}
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.ErrorIs(t, errMock, ret.err)
		mgr.clusterTopology = oldClusterTopology
		mgr.blobnodeCli = oldBlobNode
	}
	{
		// blobnode return ErrDiskBroken
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{Vid: vid, VunitLocations: []proto.VunitLocation{{Vuid: 1}}}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		clusterTopology.EXPECT().UpdateVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		mgr.clusterTopology = clusterTopology

		oldBlobNode := mgr.blobnodeCli
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errcode.ErrDiskBroken)
		mgr.blobnodeCli = blobnodeCli

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.Nil(t, msg.BlobDelStages.Stages)
		require.ErrorIs(t, errcode.ErrDiskBroken, ret.err)
		mgr.clusterTopology = oldClusterTopology
		mgr.blobnodeCli = oldBlobNode
	}
	{
		// blobnode return ErrDiskBroken, and clusterTopology update not eql
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		clusterTopology.EXPECT().UpdateVolume(any).DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}, {Vuid: 2}},
				}, nil
			},
		)
		mgr.clusterTopology = clusterTopology

		oldBlobNode := mgr.blobnodeCli
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errcode.ErrDiskBroken)
		mgr.blobnodeCli = blobnodeCli

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.Nil(t, msg.BlobDelStages.Stages)

		clusterTopology.EXPECT().UpdateVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 2}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology
		ret = mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.Nil(t, msg.BlobDelStages.Stages)
		require.ErrorIs(t, errcode.ErrDiskBroken, ret.err)

		mgr.clusterTopology = oldClusterTopology
		mgr.blobnodeCli = oldBlobNode
	}
	{
		// has broken disk and not send requests to blobnode
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1, DiskID: testDisk1.DiskID}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(true)
		clusterTopology.EXPECT().UpdateVolume(any).AnyTimes().Return(nil, nil)
		mgr.clusterTopology = clusterTopology

		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusFailed, ret.status)
		require.Nil(t, msg.BlobDelStages.Stages)
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// message punished and consume success
		oldClusterTopology := mgr.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mgr.clusterTopology = clusterTopology
		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed", Retry: defaultMessagePunishThreshold}
		oldPunishTime := mgr.punishTime
		mgr.punishTime = 10 * time.Millisecond
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, DeleteStatusDone, ret.status)
		require.Equal(t, 1, len(msg.BlobDelStages.Stages))
		for _, v := range msg.BlobDelStages.Stages {
			require.Equal(t, proto.DeleteStageDelete, v)
		}
		mgr.punishTime = oldPunishTime
		mgr.clusterTopology = oldClusterTopology
	}
	{
		// message punished for a while and cancel
		closer := closer.New()
		go func() {
			time.Sleep(10 * time.Millisecond)
			closer.Close()
		}()
		msg := &proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed", Retry: defaultMessagePunishThreshold}
		ret := mgr.consume(ctx, msg, closer)
		require.Equal(t, DeleteStatusUndo, ret.status)
	}
}

// comment temporary
func TestNewDeleteMgr(t *testing.T) {
	ctr := gomock.NewController(t)
	broker0 := NewBroker(t)
	defer broker0.Close()

	testDir, err := ioutil.TempDir(os.TempDir(), "delete_log")
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
	kafkaClient.EXPECT().StartKafkaConsumer(any, any, any).AnyTimes().Return(consumer, nil)

	mgr, err := NewBlobDeleteMgr(blobCfg, clusterTopology, switchMgr, blobnodeCli, kafkaClient)
	require.NoError(t, err)
	require.False(t, mgr.Enabled())
	// run task
	mgr.Run()
	err = mgr.startConsumer()
	require.NoError(t, err)
	mgr.stopConsumer()
	require.Nil(t, mgr.consumers)
	mgr.Close()

	// get stats
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
		{
			hourRange: HourRange{0, 1},
			now:       time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()),
			ok:        true,
		},
		{
			hourRange: HourRange{0, 1},
			now:       time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, now.Location()),
			ok:        false,
			waitTime:  23 * time.Hour,
		},
		{
			hourRange: HourRange{0, 2},
			now:       time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, now.Location()),
			ok:        true,
		},
		{
			hourRange: HourRange{0, 23},
			now:       time.Date(now.Year(), now.Month(), now.Day(), 23, 10, 0, 0, now.Location()),
			ok:        false,
			waitTime:  50 * time.Minute,
		},
		{
			hourRange: HourRange{1, 2},
			now:       time.Date(now.Year(), now.Month(), now.Day(), 3, 0, 0, 0, now.Location()),
			ok:        false,
			waitTime:  (21 + 1) * time.Hour,
		},
		{
			hourRange: HourRange{2, 5},
			now:       time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, now.Location()),
			ok:        false,
			waitTime:  1 * time.Hour,
		},
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
	{
		// mark delete failed
		mgr := newBlobDeleteMgr(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errMock)
		mgr.blobnodeCli = blobnodeCli

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errMock)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete failed and need update volume cache: update cache failed
		mgr := newBlobDeleteMgr(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		mgr.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, ErrFrequentlyUpdate)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errcode.ErrNoSuchVuid)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete failed and need update volume cache: update cache success but volume not change
		mgr := newBlobDeleteMgr(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		mgr.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, nil)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.ErrorIs(t, err, errcode.ErrNoSuchVuid)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete and delete success
		mgr := newBlobDeleteMgr(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(nil)
		blobnodeCli.EXPECT().Delete(any, any, any).Times(6).Return(nil)
		mgr.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		newVolume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
		newVolume.VunitLocations[5].Vuid += 1
		clusterTopology.EXPECT().UpdateVolume(any).Return(newVolume, nil)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.deleteBlob(ctx, volume, &proto.DeleteMsg{Bid: proto.BlobID(1)})
		require.NoError(t, err)
		require.False(t, doneVolume.EqualWith(volume))
		require.True(t, doneVolume.EqualWith(newVolume))
	}
}
