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
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

func newDeleteTopicConsumer(t *testing.T) *deleteTopicConsumer {
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
	consumer := NewMockConsumer(ctr)

	delLogger := mocks.NewMockRecordLogEncoder(ctr)
	delLogger.EXPECT().Close().AnyTimes().Return(nil)
	delLogger.EXPECT().Encode(any).AnyTimes().Return(nil)
	tp := taskpool.New(2, 2)

	return &deleteTopicConsumer{
		taskSwitch:     taskSwitch,
		topicConsumers: []base.IConsumer{consumer},
		taskPool:       &tp,

		consumeIntervalMs: time.Duration(0),
		safeDelayTime:     time.Hour,
		clusterTopology:   clusterTopology,
		blobnodeCli:       blobnodeCli,
		failMsgSender:     producer,

		delSuccessCounter:    base.NewCounter(1, "delete", base.KindSuccess),
		delFailCounter:       base.NewCounter(1, "delete", base.KindFailed),
		errStatsDistribution: base.NewErrorStats(),
		delLogger:            delLogger,

		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},
	}
}

func TestDeleteTopicConsumer(t *testing.T) {
	ctr := gomock.NewController(t)
	mockTopicConsumeDelete := newDeleteTopicConsumer(t)

	consumer := mockTopicConsumeDelete.topicConsumers[0].(*MockConsumer)
	consumer.EXPECT().CommitOffset(any).AnyTimes().Return(nil)

	{
		// nothing todo
		consumer.EXPECT().ConsumeMessages(any, any).Return([]*sarama.ConsumerMessage{})
		mockTopicConsumeDelete.consumeAndDelete(consumer, 0)
	}
	{
		// return one invalid message
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 1)
	}
	{
		// return 2 same messages and consume one time
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "123456"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs, kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
	}
	{
		// return 2 diff messages and consume success
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "msg1"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}

				msg2 := proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "msg2"}
				msgByte2, _ := json.Marshal(msg2)
				kafkaMgs2 := &sarama.ConsumerMessage{
					Value: msgByte2,
				}
				return []*sarama.ConsumerMessage{kafkaMgs, kafkaMgs2}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
	}
	{
		// return one message and delete protected
		oldClusterTopology := mockTopicConsumeDelete.clusterTopology
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
		mockTopicConsumeDelete.clusterTopology = clusterTopology

		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{
					Bid:   2,
					Vid:   2,
					ReqId: "msg with volume return",
					Time:  time.Now().Unix() - 1,
				}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.safeDelayTime = 2 * time.Second
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
		mockTopicConsumeDelete.clusterTopology = oldClusterTopology
	}
	{
		// return one message and blobnode delete failed
		oldClusterTopology := mockTopicConsumeDelete.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{Vid: vid}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mockTopicConsumeDelete.clusterTopology = clusterTopology

		oldBlobNode := mockTopicConsumeDelete.blobnodeCli
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errMock)
		mockTopicConsumeDelete.blobnodeCli = blobnodeCli

		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
		mockTopicConsumeDelete.clusterTopology = oldClusterTopology
		mockTopicConsumeDelete.blobnodeCli = oldBlobNode
	}
	{
		// return one message and blobnode return ErrDiskBroken
		oldClusterTopology := mockTopicConsumeDelete.clusterTopology
		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().GetVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{Vid: vid}, nil
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
		mockTopicConsumeDelete.clusterTopology = clusterTopology

		oldBlobNode := mockTopicConsumeDelete.blobnodeCli
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errcode.ErrDiskBroken)
		mockTopicConsumeDelete.blobnodeCli = blobnodeCli

		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
		mockTopicConsumeDelete.clusterTopology = oldClusterTopology
		mockTopicConsumeDelete.blobnodeCli = oldBlobNode
	}
	{
		// return one message, blobnode return ErrDiskBroken, and clusterTopology update not eql
		oldClusterTopology := mockTopicConsumeDelete.clusterTopology
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
		mockTopicConsumeDelete.clusterTopology = clusterTopology

		oldBlobNode := mockTopicConsumeDelete.blobnodeCli
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errcode.ErrDiskBroken)
		mockTopicConsumeDelete.blobnodeCli = blobnodeCli

		consumer.EXPECT().ConsumeMessages(any, any).AnyTimes().DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)

		clusterTopology.EXPECT().UpdateVolume(any).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolumeInfoSimple, error) {
				return &client.VolumeInfoSimple{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 2}},
				}, nil
			},
		)
		clusterTopology.EXPECT().IsBrokenDisk(any).AnyTimes().Return(false)
		mockTopicConsumeDelete.clusterTopology = clusterTopology
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)

		mockTopicConsumeDelete.clusterTopology = oldClusterTopology
		mockTopicConsumeDelete.blobnodeCli = oldBlobNode
	}
	{
		// has broken disk and not send requests to blobnode
		oldClusterTopology := mockTopicConsumeDelete.clusterTopology
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
		mockTopicConsumeDelete.clusterTopology = clusterTopology

		consumer.EXPECT().ConsumeMessages(any, any).AnyTimes().DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)

		mockTopicConsumeDelete.clusterTopology = oldClusterTopology
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
		ClusterID:            0,
		TaskPoolSize:         2,
		NormalHandleBatchCnt: 10,
		FailHandleBatchCnt:   10,
		DeleteHourRange: HourRange{
			From: 0,
			To:   defaultDeleteHourRangeTo,
		},
		DeleteLog: recordlog.Config{
			Dir:       testDir,
			ChunkBits: 22,
		},
		Kafka: BlobDeleteKafkaConfig{
			BrokerList: []string{broker0.Addr()},
			Normal: TopicConfig{
				Topic:      testTopic,
				Partitions: []int32{0},
			},
			Failed: TopicConfig{
				Topic:      testTopic,
				Partitions: []int32{0},
			},
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

	service, err := NewBlobDeleteMgr(blobCfg, clusterTopology, blobnodeCli, switchMgr, clusterMgrCli)
	require.NoError(t, err)

	// run task
	service.RunTask()

	// get stats
	service.GetTaskStats()
	service.GetErrorStats()
}

func TestAllowDeleting(t *testing.T) {
	now := time.Now()
	topicConsumer := &deleteTopicConsumer{}
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
		topicConsumer.deleteHourRange = test.hourRange
		waitTime, ok := topicConsumer.allowDeleting(test.now)
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
		mockTopicConsumeDelete := newDeleteTopicConsumer(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).AnyTimes().Return(errMock)
		mockTopicConsumeDelete.blobnodeCli = blobnodeCli

		doneVolume, err := mockTopicConsumeDelete.deleteBlob(ctx, volume, proto.BlobID(1))
		require.ErrorIs(t, err, errMock)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete failed and need update volume cache: update cache failed
		mockTopicConsumeDelete := newDeleteTopicConsumer(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		mockTopicConsumeDelete.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, ErrFrequentlyUpdate)
		mockTopicConsumeDelete.clusterTopology = clusterTopology

		doneVolume, err := mockTopicConsumeDelete.deleteBlob(ctx, volume, proto.BlobID(1))
		require.ErrorIs(t, err, errcode.ErrNoSuchVuid)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete failed and need update volume cache: update cache success but volume not change
		mockTopicConsumeDelete := newDeleteTopicConsumer(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		mockTopicConsumeDelete.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, nil)
		mockTopicConsumeDelete.clusterTopology = clusterTopology

		doneVolume, err := mockTopicConsumeDelete.deleteBlob(ctx, volume, proto.BlobID(1))
		require.ErrorIs(t, err, errcode.ErrNoSuchVuid)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// mark delete and delete success
		mockTopicConsumeDelete := newDeleteTopicConsumer(t)
		blobnodeCli := NewMockBlobnodeAPI(ctr)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Times(5).Return(nil)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(errcode.ErrNoSuchVuid)
		blobnodeCli.EXPECT().MarkDelete(any, any, any).Return(nil)
		blobnodeCli.EXPECT().Delete(any, any, any).Times(6).Return(nil)
		mockTopicConsumeDelete.blobnodeCli = blobnodeCli

		clusterTopology := NewMockClusterTopology(ctr)
		newVolume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
		newVolume.VunitLocations[5].Vuid += 1
		clusterTopology.EXPECT().UpdateVolume(any).Return(newVolume, nil)
		mockTopicConsumeDelete.clusterTopology = clusterTopology

		doneVolume, err := mockTopicConsumeDelete.deleteBlob(ctx, volume, proto.BlobID(1))
		require.NoError(t, err)
		require.False(t, doneVolume.EqualWith(volume))
		require.True(t, doneVolume.EqualWith(newVolume))
	}
}
