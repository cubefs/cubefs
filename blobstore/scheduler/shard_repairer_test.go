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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	"github.com/golang/mock/gomock"
)

func newShardRepairMgr(t *testing.T) *ShardRepairMgr {
	ctr := gomock.NewController(t)

	volCache := NewMockVolumeCache(ctr)
	volCache.EXPECT().Get(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	volCache.EXPECT().Update(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)

	selector := mocks.NewMockSelector(ctr)
	selector.EXPECT().GetRandomN(any).AnyTimes().Return([]string{"http://127.0.0.1:9600"})

	blobnode := NewMockBlobnodeAPI(ctr)
	blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(nil)

	sender := NewMockProducer(ctr)
	sender.EXPECT().SendMessage(any).AnyTimes().Return(nil)

	db := NewMockOrphanShardTable(ctr)
	db.EXPECT().Save(any).AnyTimes().Return(nil)

	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)
	taskSwitch, _ := switchMgr.AddSwitch(taskswitch.BlobDeleteSwitchName)

	consumer := NewMockConsumer(ctr)

	return &ShardRepairMgr{
		volCache:                volCache,
		blobnodeSelector:        selector,
		blobnodeCli:             blobnode,
		failMsgSender:           sender,
		orphanShardTable:        db,
		taskSwitch:              taskSwitch,
		failTopicConsumers:      []base.IConsumer{consumer},
		taskPool:                taskpool.New(1, 1),
		repairSuccessCounter:    base.NewCounter(1, ShardRepair, base.KindSuccess),
		repairFailedCounter:     base.NewCounter(1, ShardRepair, base.KindFailed),
		errStatsDistribution:    base.NewErrorStats(),
		repairSuccessCounterMin: &counter.Counter{},
		repairFailedCounterMin:  &counter.Counter{},
	}
}

func TestConsumerShardRepairMsg(t *testing.T) {
	ctr := gomock.NewController(t)
	service := newShardRepairMgr(t)
	consumer := NewMockConsumer(ctr)
	consumer.EXPECT().CommitOffset(any).AnyTimes().Return(nil)
	{
		// no messages
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				return []*sarama.ConsumerMessage{}
			},
		)
		service.consumerAndRepair(consumer, 0)
	}
	{
		// one message: message is invalid
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := struct{}{}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		service.consumerAndRepair(consumer, 1)
	}
	{
		// return one message and repair success
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		service.consumerAndRepair(consumer, 2)
	}
	{
		// return one message and repair failed because worker err
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		oldBlobnode := service.blobnodeCli
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(errMock)
		service.blobnodeCli = blobnode
		service.consumerAndRepair(consumer, 2)
		service.blobnodeCli = oldBlobnode
	}
	{
		// return one message and repair failed because worker err(should update volume map)
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		oldBlobnode := service.blobnodeCli
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(errcode.ErrDestReplicaBad)
		service.blobnodeCli = blobnode
		service.consumerAndRepair(consumer, 2)
		service.blobnodeCli = oldBlobnode
	}
	{

		// return one message and repair failed because worker return ErrOrphanShard err
		consumer.EXPECT().ConsumeMessages(any, any).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		oldBlobnode := service.blobnodeCli
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(errcode.ErrOrphanShard)
		service.blobnodeCli = blobnode
		service.consumerAndRepair(consumer, 2)
		service.blobnodeCli = oldBlobnode
	}
	{
		// get stats
		service.GetErrorStats()
		service.GetTaskStats()
	}
	{
		// run task
		service.RunTask()
	}
}

func TestNewShardRepairMgr(t *testing.T) {
	ctr := gomock.NewController(t)

	broker0 := NewBroker(t)
	defer broker0.Close()

	cfg := &ShardRepairConfig{
		Kafka: ShardRepairKafkaConfig{
			BrokerList: []string{broker0.Addr()},
			Normal:     TopicConfig{Topic: testTopic, Partitions: []int32{0}},
			Priority:   TopicConfig{Topic: testTopic, Partitions: []int32{0}},
			Failed:     TopicConfig{Topic: testTopic, Partitions: []int32{0}},
		},
	}

	volCache := NewMockVolumeCache(ctr)
	volCache.EXPECT().Get(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	volCache.EXPECT().Update(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)

	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)

	accessor := NewMockKafkaOffsetTable(ctr)
	accessor.EXPECT().Get(any, any).AnyTimes().Return(int64(0), nil)
	accessor.EXPECT().Set(any, any, any).AnyTimes().Return(nil)

	orphanShardTable := NewMockOrphanShardTable(ctr)
	orphanShardTable.EXPECT().Save(any).AnyTimes().Return(nil)

	blobnode := NewMockBlobnodeAPI(ctr)
	blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(nil)

	clusterCli := NewMockClusterMgrAPI(ctr)
	clusterCli.EXPECT().GetService(any, any, any).Return(nil, errMock)

	_, err := NewShardRepairMgr(cfg, volCache, switchMgr, accessor, orphanShardTable, blobnode, clusterCli)
	require.NoError(t, err)

	_, err = NewShardRepairMgr(cfg, volCache, switchMgr, accessor, orphanShardTable, blobnode, clusterCli)
	require.Error(t, err)
}
