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

func newShardRepairMgr(t *testing.T) *ShardRepairMgr {
	ctr := gomock.NewController(t)

	clusterTopology := NewMockClusterTopology(ctr)
	clusterTopology.EXPECT().GetVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	clusterTopology.EXPECT().UpdateVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)

	selector := mocks.NewMockSelector(ctr)
	selector.EXPECT().GetRandomN(any).AnyTimes().Return([]string{"http://127.0.0.1:9600"})

	blobnode := NewMockBlobnodeAPI(ctr)
	blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(nil)

	sender := NewMockProducer(ctr)
	sender.EXPECT().SendMessage(any).AnyTimes().Return(nil)
	kafkaClient := NewMockKafkaConsumer(ctr)
	consumer := NewMockGroupConsumer(ctr)
	consumer.EXPECT().Stop().AnyTimes().Return()
	kafkaClient.EXPECT().StartKafkaConsumer(any, any, any).AnyTimes().Return(consumer, nil)

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
		failMsgSender:           sender,
		kafkaConsumerClient:     kafkaClient,
		punishTime:              time.Duration(defaultMessagePunishTimeM) * time.Minute,
		orphanShardLogger:       orphanShardLog,
		taskSwitch:              taskSwitch,
		taskPool:                taskpool.New(10, 10),
		repairSuccessCounter:    base.NewCounter(1, ShardRepair, base.KindSuccess),
		repairFailedCounter:     base.NewCounter(1, ShardRepair, base.KindFailed),
		errStatsDistribution:    base.NewErrorStats(),
		repairSuccessCounterMin: &counter.Counter{},
		repairFailedCounterMin:  &counter.Counter{},
		cfg:                     &ShardRepairConfig{MessagePunishThreshold: defaultMessagePunishThreshold},
	}
}

func TestConsumerShardRepairMsg(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newShardRepairMgr(t)
	msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
	msgByte, _ := json.Marshal(msg)
	kafkaMsg := &sarama.ConsumerMessage{
		Value: msgByte,
	}
	commonCloser := closer.New()
	defer commonCloser.Close()
	{
		// message is invalid
		kafkaMsg := &sarama.ConsumerMessage{
			Value: []byte("123"),
		}
		require.True(t, mgr.Consume(kafkaMsg, commonCloser))

		msg := proto.ShardRepairMsg{}
		msgByte, _ := json.Marshal(msg)
		kafkaMsg = &sarama.ConsumerMessage{
			Value: msgByte,
		}
		require.True(t, mgr.Consume(kafkaMsg, commonCloser))
	}
	{
		// repair success
		require.True(t, mgr.Consume(kafkaMsg, commonCloser))
	}
	{
		// repair failed
		oldBlobnode := mgr.blobnodeCli
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(errMock)
		mgr.blobnodeCli = blobnode
		require.True(t, mgr.Consume(kafkaMsg, commonCloser))
		mgr.blobnodeCli = oldBlobnode
	}
	{
		// consume undo
		consuming := closer.New()
		consuming.Close()
		require.False(t, mgr.Consume(kafkaMsg, consuming))
	}
	{
		// repair success
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, ShardRepairStatusDone, ret.status)
	}
	{
		// repair failed because worker err
		oldBlobnode := mgr.blobnodeCli
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(errMock)
		mgr.blobnodeCli = blobnode
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, ShardRepairStatusFailed, ret.status)
		require.ErrorIs(t, errMock, ret.err)
		mgr.blobnodeCli = oldBlobnode
	}
	{
		// return one message and repair failed because worker err(should update volume map)
		oldBlobnode := mgr.blobnodeCli
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(errcode.ErrDestReplicaBad)
		mgr.blobnodeCli = blobnode
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, ShardRepairStatusFailed, ret.status)
		require.ErrorIs(t, errcode.ErrDestReplicaBad, ret.err)
		mgr.blobnodeCli = oldBlobnode
	}
	{
		// repair failed because worker return ErrOrphanShard err
		oldBlobnode := mgr.blobnodeCli
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(errcode.ErrOrphanShard)
		mgr.blobnodeCli = blobnode
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, ShardRepairStatusOrphan, ret.status)
		require.ErrorIs(t, errcode.ErrOrphanShard, ret.err)
		mgr.blobnodeCli = oldBlobnode
	}
	{
		// consume undo
		consuming := closer.New()
		consuming.Close()
		ret := mgr.consume(ctx, msg, consuming)
		require.Equal(t, ShardRepairStatusUndo, ret.status)
	}
	{
		// message punished and consume success
		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}, Retry: defaultMessagePunishThreshold}
		oldPunishTime := mgr.punishTime
		mgr.punishTime = 10 * time.Millisecond
		ret := mgr.consume(ctx, msg, commonCloser)
		require.Equal(t, ShardRepairStatusDone, ret.status)
		mgr.punishTime = oldPunishTime
	}
	{
		// message punished for a while and cancel
		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}, Retry: defaultMessagePunishThreshold}
		closer := closer.New()
		go func() {
			time.Sleep(10 * time.Millisecond)
			closer.Close()
		}()
		ret := mgr.consume(ctx, msg, closer)
		require.Equal(t, ShardRepairStatusUndo, ret.status)
	}
}

func TestNewShardRepairMgr(t *testing.T) {
	ctr := gomock.NewController(t)

	broker0 := NewBroker(t)
	defer broker0.Close()

	testDir, err := ioutil.TempDir(os.TempDir(), "orphan_shard_log")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	cfg := &ShardRepairConfig{
		Kafka: ShardRepairKafkaConfig{
			BrokerList:   []string{broker0.Addr()},
			TopicNormals: []string{testTopic},
			TopicFailed:  testTopic,
		},
		OrphanShardLog: recordlog.Config{
			Dir:       testDir,
			ChunkBits: 22,
		},
		MessagePunishTimeM:     defaultMessagePunishTimeM,
		MessagePunishThreshold: defaultMessagePunishThreshold,
	}

	clusterTopology := NewMockClusterTopology(ctr)
	clusterTopology.EXPECT().GetVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	clusterTopology.EXPECT().UpdateVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)

	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("false", nil)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)

	blobnode := NewMockBlobnodeAPI(ctr)
	blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(nil)

	clusterCli := NewMockClusterMgrAPI(ctr)
	clusterCli.EXPECT().GetService(any, any, any).Return(nil, errMock)
	clusterCli.EXPECT().GetConsumeOffset(any, any, any).AnyTimes().Return(int64(0), nil)
	clusterCli.EXPECT().SetConsumeOffset(any, any, any, any).AnyTimes().Return(nil)

	kafkaClient := NewMockKafkaConsumer(ctr)
	consumer := NewMockGroupConsumer(ctr)
	consumer.EXPECT().Stop().AnyTimes().Return()
	kafkaClient.EXPECT().StartKafkaConsumer(any, any, any).AnyTimes().Return(consumer, nil)

	mgr, err := NewShardRepairMgr(cfg, clusterTopology, switchMgr, blobnode, clusterCli, kafkaClient)
	require.NoError(t, err)
	require.False(t, mgr.Enabled())

	// get stats
	mgr.GetErrorStats()
	mgr.GetTaskStats()

	// run task
	mgr.Run()
	err = mgr.startConsumer()
	require.NoError(t, err)
	mgr.stopConsumer()
	require.Nil(t, mgr.consumers)
	mgr.Close()

	_, err = NewShardRepairMgr(cfg, clusterTopology, switchMgr, blobnode, clusterCli, kafkaClient)
	require.Error(t, err)
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
