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
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	clustermgr "github.com/cubefs/cubefs/blobstore/api/clustermgr"
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
	"github.com/cubefs/cubefs/blobstore/util/selector"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

func newShardRepairMgr(t *testing.T) *ShardRepairMgr {
	ctr := gomock.NewController(t)

	clusterTopology := NewMockClusterTopology(ctr)
	clusterTopology.EXPECT().GetVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	clusterTopology.EXPECT().UpdateVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	clusterTopology.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{}, true)

	mockSelector := mocks.NewMockSelector(ctr)
	mockSelector.EXPECT().GetRandomN(any).AnyTimes().Return([]string{"http://127.0.0.1:9600"})

	blobnode := NewMockBlobnodeAPI(ctr)
	blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(nil)

	sender := NewMockProducer(ctr)
	sender.EXPECT().SendMessage(any).AnyTimes().Return(nil)
	kafkaClient := NewMockKafkaConsumer(ctr)
	consumer := NewMockGroupConsumer(ctr)
	consumer.EXPECT().Stop().AnyTimes().Return()
	kafkaClient.EXPECT().StartKafkaConsumer(any, any).AnyTimes().Return(consumer, nil)

	orphanShardLog := mocks.NewMockRecordLogEncoder(ctr)
	orphanShardLog.EXPECT().Encode(any).AnyTimes().Return(nil)

	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	clusterMgrCli.EXPECT().GetService(any, any, any).AnyTimes().Return(nil, nil)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)
	taskSwitch, _ := switchMgr.AddSwitch(proto.TaskTypeBlobDelete.String())

	return &ShardRepairMgr{
		clusterTopology: clusterTopology,
		blobnodeSelector: &idcSelector{
			clusterMgrCli: clusterMgrCli,
			selectors:     map[string]selector.Selector{"": mockSelector},
		},
		clusterMgrCli:           clusterMgrCli,
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
		cfg: &ShardRepairConfig{
			MessagePunishThreshold: defaultMessagePunishThreshold,
		},
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
		kafkaMsgs := []*sarama.ConsumerMessage{kafkaMsg}
		require.True(t, mgr.Consume(kafkaMsgs, commonCloser))

		msg := proto.ShardRepairMsg{}
		msgByte, _ := json.Marshal(msg)
		kafkaMsg = &sarama.ConsumerMessage{
			Value: msgByte,
		}
		kafkaMsgs = []*sarama.ConsumerMessage{kafkaMsg}
		require.True(t, mgr.Consume(kafkaMsgs, commonCloser))
	}
	{
		// repair success
		kafkaMsgs := []*sarama.ConsumerMessage{kafkaMsg}
		require.True(t, mgr.Consume(kafkaMsgs, commonCloser))
	}
	{
		// repair failed
		oldBlobnode := mgr.blobnodeCli
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).AnyTimes().Return(errMock)
		mgr.blobnodeCli = blobnode
		kafkaMsgs := []*sarama.ConsumerMessage{kafkaMsg}
		require.True(t, mgr.Consume(kafkaMsgs, commonCloser))
		mgr.blobnodeCli = oldBlobnode
	}
	{
		// consume undo
		consuming := closer.New()
		consuming.Close()
		kafkaMsgs := []*sarama.ConsumerMessage{kafkaMsg}
		require.False(t, mgr.Consume(kafkaMsgs, consuming))
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
		require.ErrorIs(t, ret.err, errMock)
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
		require.ErrorIs(t, ret.err, errcode.ErrDestReplicaBad)
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
		require.ErrorIs(t, ret.err, errcode.ErrOrphanShard)
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

	testDir, err := os.MkdirTemp(os.TempDir(), "orphan_shard_log")
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
	clusterCli.EXPECT().GetConsumeOffset(any, any, any).AnyTimes().Return(int64(0), nil)
	clusterCli.EXPECT().SetConsumeOffset(any, any, any, any).AnyTimes().Return(nil)

	kafkaClient := NewMockKafkaConsumer(ctr)
	consumer := NewMockGroupConsumer(ctr)
	consumer.EXPECT().Stop().AnyTimes().Return()
	kafkaClient.EXPECT().StartKafkaConsumer(any, any).AnyTimes().Return(consumer, nil)

	mgr, err := NewShardRepairMgr(cfg, clusterTopology, switchMgr, blobnode, clusterCli, kafkaClient, nil)
	require.NoError(t, err)
	require.False(t, mgr.Enabled())

	// verify blobnodeSelector was initialized
	require.NotNil(t, mgr.blobnodeSelector)

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

	_, err = NewShardRepairMgr(cfg, clusterTopology, switchMgr, blobnode, clusterCli, kafkaClient, nil)
	require.Error(t, err)
}

func TestProcessDiskNotFoundErr(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)

	volume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
	repairMsg := &proto.ShardRepairMsg{Vid: proto.Vid(1), Bid: proto.BlobID(1), BadIdx: []uint8{0}}
	missVuid := volume.VunitLocations[0].Vuid
	missDisk := volume.VunitLocations[0].DiskID

	newMgr := func() *ShardRepairMgr {
		mgr := newShardRepairMgr(t)
		mgr.chunkMissMigrateReporter = base.NewAbnormalReporter(proto.ClusterID(2), ShardRepair, base.ChunkMissMigrateAbnormal)
		mgr.clusterMgrCli = NewMockClusterMgrAPI(ctr)
		mgr.taskCli = NewMockTaskAPI(ctr)
		return mgr
	}

	{
		// case 1: vuid already reported → skip all downstream calls
		mgr := newMgr()
		mgr.chunkMissMigrateReporter.SetVuidReported(missVuid)
		// GetDiskInfo must NOT be called
		mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
		require.True(t, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
	}
	{
		// case 2: GetDiskInfo fails → continue to next idx (no GetVolumeInfo)
		mgr := newMgr()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, missDisk).Return(nil, errMock)
		mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
		require.False(t, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
	}
	{
		// case 3: disk.Status <= DiskStatusRepairing → continue (no GetVolumeInfo)
		mgr := newMgr()
		repairingDisk := MockGenDiskInfo(missDisk, proto.DiskStatusRepairing)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, missDisk).Return(repairingDisk, nil)
		mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
		require.False(t, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
	}
	{
		// case 4: GetVolumeInfo fails → continue (no CheckTaskExist)
		mgr := newMgr()
		repairedDisk := MockGenDiskInfo(missDisk, proto.DiskStatusRepaired)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, missDisk).Return(repairedDisk, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(nil, errMock)
		mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
		require.False(t, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
	}
	{
		// case 5: vol changed (!vol.EqualWith) → continue (no CheckTaskExist)
		mgr := newMgr()
		repairedDisk := MockGenDiskInfo(missDisk, proto.DiskStatusRepaired)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, missDisk).Return(repairedDisk, nil)
		changedVol := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
		changedVol.VunitLocations[0].Vuid += 1 // different vuid → EqualWith returns false
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(changedVol, nil)
		mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
		require.False(t, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
	}
	{
		// case 6: CheckTaskExist fails → continue (no ReportAbnormal, not set reported)
		mgr := newMgr()
		repairedDisk := MockGenDiskInfo(missDisk, proto.DiskStatusRepaired)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, missDisk).Return(repairedDisk, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(volume, nil)
		mgr.taskCli.(*MockTaskAPI).EXPECT().CheckTaskExist(any, proto.TaskTypeManualMigrate, missDisk, missVuid).Return(false, errMock)
		mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
		require.False(t, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
	}
	{
		// case 7: task already exists → SetVuidReported, no ReportAbnormal
		mgr := newMgr()
		repairedDisk := MockGenDiskInfo(missDisk, proto.DiskStatusRepaired)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, missDisk).Return(repairedDisk, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(volume, nil)
		mgr.taskCli.(*MockTaskAPI).EXPECT().CheckTaskExist(any, proto.TaskTypeManualMigrate, missDisk, missVuid).Return(true, nil)
		mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
		// vuid is marked reported (task exists, will skip in future)
		require.True(t, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
	}
	{
		// case 8 (full path): disk Repaired, vol equal, task not exist → ReportAbnormal + SetVuidReported
		mgr := newMgr()
		repairedDisk := MockGenDiskInfo(missDisk, proto.DiskStatusRepaired)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, missDisk).Return(repairedDisk, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(volume, nil)
		mgr.taskCli.(*MockTaskAPI).EXPECT().CheckTaskExist(any, proto.TaskTypeManualMigrate, missDisk, missVuid).Return(false, nil)
		mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
		require.True(t, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
	}
}

func TestIDCSelectorInit(t *testing.T) {
	t.Run("should init without calling GetService", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cmCli := NewMockClusterMgrAPI(ctr)
		sel := newIDCSelector(cmCli, proto.ClusterID(1))
		require.NotNil(t, sel)
		require.Empty(t, sel.selectors)
	})

	t.Run("should build per-IDC selectors from worker GetService data", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cmCli := NewMockClusterMgrAPI(ctr)
		cmCli.EXPECT().GetService(gomock.Any(), proto.ServiceNameWorker, gomock.Any()).AnyTimes().
			Return([]clustermgr.ServiceNode{
				{ClusterID: 1, Host: "worker-a:9600", Idc: "az0"},
				{ClusterID: 1, Host: "worker-b:9600", Idc: "az1"},
			}, nil)

		sel := newIDCSelector(cmCli, proto.ClusterID(1))

		// az0 selector must return the az0 worker only
		host := sel.get(context.Background(), "az0")
		require.Equal(t, "worker-a:9600", host)
		// az1 selector must return the az1 worker only
		host = sel.get(context.Background(), "az1")
		require.Equal(t, "worker-b:9600", host)
		// global selector must return all workers
		host = sel.get(context.Background(), "")
		require.NotEmpty(t, host)
	})

	t.Run("should handle workers only", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cmCli := NewMockClusterMgrAPI(ctr)
		cmCli.EXPECT().GetService(gomock.Any(), proto.ServiceNameWorker, gomock.Any()).AnyTimes().
			Return([]clustermgr.ServiceNode{
				{ClusterID: 1, Host: "worker-a:9600", Idc: "az0"},
				{ClusterID: 1, Host: "worker-b:9600", Idc: "az1"},
			}, nil)

		sel := newIDCSelector(cmCli, proto.ClusterID(1))

		host := sel.get(context.Background(), "az0")
		require.NotEmpty(t, host)
		host = sel.get(context.Background(), "")
		require.NotEmpty(t, host)
	})
}

func TestIDCSelectorConcurrent(t *testing.T) {
	ctr := gomock.NewController(t)
	cmCli := NewMockClusterMgrAPI(ctr)
	cmCli.EXPECT().GetService(gomock.Any(), proto.ServiceNameWorker, gomock.Any()).AnyTimes().
		Return([]clustermgr.ServiceNode{
			{ClusterID: 1, Host: "worker-a:9600", Idc: "az0"},
			{ClusterID: 1, Host: "worker-b:9600", Idc: "az1"},
		}, nil)

	sel := newIDCSelector(cmCli, proto.ClusterID(1))

	const goroutines = 16
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			host := sel.get(context.Background(), "az0")
			if host == "" {
				t.Error("concurrent get az0 returned empty")
			}
			host = sel.get(context.Background(), "az1")
			if host == "" {
				t.Error("concurrent get az1 returned empty")
			}
			host = sel.get(context.Background(), "")
			if host == "" {
				t.Error("concurrent get global returned empty")
			}
		}()
	}
	wg.Wait()
}

func TestIDCSelectorRefreshError(t *testing.T) {
	ctr := gomock.NewController(t)
	cmCli := NewMockClusterMgrAPI(ctr)
	cmCli.EXPECT().GetService(gomock.Any(), proto.ServiceNameWorker, gomock.Any()).AnyTimes().
		Return(nil, errors.New("network error"))

	sel := newIDCSelector(cmCli, proto.ClusterID(1))

	// MakeSelector calls the getter immediately and ignores the error,
	// so the selector is created with empty cachedValues.
	host := sel.get(context.Background(), "az0")
	require.Empty(t, host, "should return empty when refresh fails")

	// Fallback to global should also fail since the same cmCli is used
	host = sel.get(context.Background(), "")
	require.Empty(t, host, "should return empty when global refresh also fails")
}

func TestTryRepair(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	volume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
	{
		// no host for shard repair
		mgr := newShardRepairMgr(t)
		mockSel := mocks.NewMockSelector(ctr)
		mockSel.EXPECT().GetRandomN(any).AnyTimes().Return(nil)
		mgr.blobnodeSelector.selectors[""] = mockSel
		doneVolume, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: proto.BlobID(1), Vid: proto.Vid(1), BadIdx: []uint8{0}})
		require.ErrorIs(t, err, ErrBlobnodeServiceUnavailable)
		require.True(t, doneVolume.EqualWith(volume))
	}
	{
		// repair success
		mgr := newShardRepairMgr(t)

		mgr.clusterTopology.(*MockClusterTopology).EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{
			Host: volume.VunitLocations[0].Host,
		}, true)

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
		clusterTopology.EXPECT().UpdateVolume(any).Return(volume, errcode.ErrUpdateVolCacheFreq)
		clusterTopology.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{Host: volume.VunitLocations[0].Host}, true)
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
		clusterTopology.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{Host: volume.VunitLocations[0].Host}, true)
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
		clusterTopology.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{Host: volume.VunitLocations[0].Host}, true)
		mgr.clusterTopology = clusterTopology

		doneVolume, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: proto.BlobID(1), Vid: proto.Vid(1), BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.False(t, doneVolume.EqualWith(volume))
		require.True(t, doneVolume.EqualWith(newVolume))
	}
	{
		// repair miss migrate chunk
		mgr := newShardRepairMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrNoSuchDisk).AnyTimes()
		mgr.blobnodeCli = blobnode

		clusterMgrCli := NewMockClusterMgrAPI(ctr)
		clusterMgrCli.EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli = clusterMgrCli

		taskCli := NewMockTaskAPI(ctr)
		taskCli.EXPECT().CheckTaskExist(any, any, any, any).Return(false, nil)
		mgr.taskCli = taskCli

		mgr.chunkMissMigrateReporter = base.NewAbnormalReporter(proto.ClusterID(1), ShardRepair, base.ChunkMissMigrateAbnormal)

		missIdx := uint8(0)
		newDisk := MockGenDiskInfo(volume.VunitLocations[missIdx].DiskID, proto.DiskStatusRepaired)
		clusterMgrCli.EXPECT().GetDiskInfo(any, any).Return(newDisk, nil)

		_, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: proto.BlobID(1), Vid: proto.Vid(1), BadIdx: []uint8{missIdx}})
		require.ErrorIs(t, err, errcode.ErrNoSuchDisk)
	}
}

func TestResolveRepairIDC(t *testing.T) {
	vol := MockGenVolInfo(1, codemode.EC3P3, proto.VolumeStatusActive)
	for i := range vol.VunitLocations {
		vol.VunitLocations[i].Host = fmt.Sprintf("http://host-%d:9600", i)
	}

	t.Run("should return empty when no bad index", func(t *testing.T) {
		mgr := newShardRepairMgr(t)
		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1}
		idc := mgr.resolveRepairIDC(context.Background(), msg, nil)
		require.Equal(t, "", idc)
	})

	t.Run("should return empty when bad index out of range", func(t *testing.T) {
		mgr := newShardRepairMgr(t)
		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{99}}
		idcByVunitIdx := make([]string, len(vol.VunitLocations))
		idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx)
		require.Equal(t, "", idc)
	})

	t.Run("should return AZ when disk found", func(t *testing.T) {
		mgr := newShardRepairMgr(t)
		idcByVunitIdx := make([]string, len(vol.VunitLocations))
		idcByVunitIdx[0] = "az0"
		idcByVunitIdx[1] = "az1"

		{
			msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
			idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx)
			require.Equal(t, "az0", idc)
		}
		{
			msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{1}}
			idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx)
			require.Equal(t, "az1", idc)
		}
	})

	t.Run("should return empty when no disk has IDC", func(t *testing.T) {
		mgr := newShardRepairMgr(t)
		idcByVunitIdx := make([]string, len(vol.VunitLocations))

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx)
		require.Equal(t, "", idc)
	})

	t.Run("should iterate bad indices to find valid IDC", func(t *testing.T) {
		mgr := newShardRepairMgr(t)
		idcByVunitIdx := make([]string, len(vol.VunitLocations))
		idcByVunitIdx[2] = "az2"

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0, 1, 2}}
		idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx)
		require.Equal(t, "az2", idc)
	})
}

func TestPickWorkerHost(t *testing.T) {
	vol := MockGenVolInfo(1, codemode.EC3P3, proto.VolumeStatusActive)
	for i := range vol.VunitLocations {
		vol.VunitLocations[i].Host = fmt.Sprintf("http://host-%d:9600", i)
	}

	t.Run("should select same AZ worker when available", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)

		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return([]string{"az0-worker"})
		mgr.blobnodeSelector.selectors["az0"] = mockAz0

		idcByVunitIdx := make([]string, len(vol.VunitLocations))
		idcByVunitIdx[0] = "az0"

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		host := mgr.blobnodeSelector.get(context.Background(), mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx))
		require.Equal(t, "az0-worker", host)
	})

	t.Run("should fallback to global random when target AZ has no worker", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)

		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return(nil)
		mgr.blobnodeSelector.selectors["az0"] = mockAz0

		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(1).Return([]string{"fallback-worker"})
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		idcByVunitIdx := make([]string, len(vol.VunitLocations))
		idcByVunitIdx[0] = "az0"

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		host := mgr.blobnodeSelector.get(context.Background(), mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx))
		require.Equal(t, "fallback-worker", host)
	})

	t.Run("should route different AZ damages to different AZ workers", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)

		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return([]string{"az0-worker"})
		mgr.blobnodeSelector.selectors["az0"] = mockAz0

		mockAz1 := mocks.NewMockSelector(ctr)
		mockAz1.EXPECT().GetRandomN(1).Return([]string{"az1-worker"})
		mgr.blobnodeSelector.selectors["az1"] = mockAz1

		idcByVunitIdx := make([]string, len(vol.VunitLocations))
		idcByVunitIdx[0] = "az0"
		idcByVunitIdx[3] = "az1"

		{
			msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
			host := mgr.blobnodeSelector.get(context.Background(), mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx))
			require.Equal(t, "az0-worker", host)
		}
		{
			msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{3}}
			host := mgr.blobnodeSelector.get(context.Background(), mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx))
			require.Equal(t, "az1-worker", host)
		}
	})

	t.Run("should return empty when all selectors have no hosts", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)

		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return(nil)
		mgr.blobnodeSelector.selectors["az0"] = mockAz0

		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(1).AnyTimes().Return(nil)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		idcByVunitIdx := make([]string, len(vol.VunitLocations))
		idcByVunitIdx[0] = "az0"

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		host := mgr.blobnodeSelector.get(context.Background(), mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx))
		require.Equal(t, "", host)
	})

	t.Run("should lazy refresh and find worker when fallback empty", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(any).AnyTimes().Return(nil)
		mgr.blobnodeSelector.selectors[""] = mockGlobal // global fallback always returns no hosts

		// getSelector on "az0" will lazily call GetService(Worker)
		cmCli := NewMockClusterMgrAPI(ctr)
		cmCli.EXPECT().GetService(gomock.Any(), proto.ServiceNameWorker, gomock.Any()).
			Return([]clustermgr.ServiceNode{
				{ClusterID: 1, Host: "lazy-worker:9600", Idc: "az0"},
			}, nil)
		mgr.blobnodeSelector.clusterMgrCli = cmCli

		idcByVunitIdx := make([]string, len(vol.VunitLocations))
		idcByVunitIdx[0] = "az0"

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		host := mgr.blobnodeSelector.get(context.Background(), mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx))
		require.Equal(t, "lazy-worker:9600", host)
	})
}

func TestRepairShardWithIDC(t *testing.T) {
	t.Run("should repair via same AZ worker when IDC routing enabled", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)

		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, gomock.Eq("az0-worker"), any).Return(nil)
		mgr.blobnodeCli = blobnode

		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return([]string{"az0-worker"})
		mgr.blobnodeSelector.selectors["az0"] = mockAz0

		vol := MockGenVolInfo(1, codemode.EC3P3, proto.VolumeStatusActive)

		newTopology := NewMockClusterTopology(ctr)
		newTopology.EXPECT().GetVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
		newTopology.EXPECT().UpdateVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
		for i := range vol.VunitLocations {
			host := fmt.Sprintf("http://host-%d:9600", i)
			vol.VunitLocations[i].Host = host
			diskInfo := &client.DiskInfoSimple{Host: host}
			if i == 0 {
				diskInfo.Idc = "az0"
			}
			newTopology.EXPECT().GetDisk(vol.VunitLocations[i].DiskID).Return(diskInfo, true).AnyTimes()
		}
		mgr.clusterTopology = newTopology

		doneVol, err := mgr.repairShard(context.Background(), vol,
			&proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.NotNil(t, doneVol)
	})

	t.Run("should fallback to global when unknown disk (empty IDC)", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		// GetDisk returns DiskInfoSimple without Idc, so resolveRepairIDC returns ""
		// Get(ctx, "") skips per-IDC and uses the global fallback

		vol := MockGenVolInfo(1, codemode.EC3P3, proto.VolumeStatusActive)
		newTopology := NewMockClusterTopology(ctr)
		newTopology.EXPECT().GetVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
		newTopology.EXPECT().UpdateVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
		for i := range vol.VunitLocations {
			host := fmt.Sprintf("http://host-%d:9600", i)
			vol.VunitLocations[i].Host = host
			newTopology.EXPECT().GetDisk(vol.VunitLocations[i].DiskID).Return(
				&client.DiskInfoSimple{Host: host}, true).AnyTimes()
		}
		mgr.clusterTopology = newTopology

		// Replace global selector with a mock to prove it was actually called
		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(1).Return([]string{"http://127.0.0.1:9600"}).Times(1)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		doneVol, err := mgr.repairShard(context.Background(), vol,
			&proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.NotNil(t, doneVol)
	})

	t.Run("should fallback to global when AZ has no worker", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		// selectors["az0"] is intentionally nil — triggers fallback

		vol := MockGenVolInfo(1, codemode.EC3P3, proto.VolumeStatusActive)
		newTopology := NewMockClusterTopology(ctr)
		newTopology.EXPECT().GetVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
		newTopology.EXPECT().UpdateVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
		for i := range vol.VunitLocations {
			host := fmt.Sprintf("http://host-%d:9600", i)
			vol.VunitLocations[i].Host = host
			diskInfo := &client.DiskInfoSimple{Host: host}
			if i == 0 {
				diskInfo.Idc = "az0"
			}
			newTopology.EXPECT().GetDisk(vol.VunitLocations[i].DiskID).Return(diskInfo, true).AnyTimes()
		}
		mgr.clusterTopology = newTopology

		// Replace global selector with a mock to prove it was hit after the nil az0 selector
		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(1).Return([]string{"http://127.0.0.1:9600"}).Times(1)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		doneVol, err := mgr.repairShard(context.Background(), vol,
			&proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.NotNil(t, doneVol)
	})

	t.Run("should return unavailable when no host at all", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(any).AnyTimes().Return(nil)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		vol := MockGenVolInfo(1, codemode.EC3P3, proto.VolumeStatusActive)
		for i := range vol.VunitLocations {
			vol.VunitLocations[i].Host = fmt.Sprintf("http://host-%d:9600", i)
		}

		doneVol, err := mgr.repairShard(context.Background(), vol,
			&proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.ErrorIs(t, err, ErrBlobnodeServiceUnavailable)
		require.NotNil(t, doneVol)
	})
}

func BenchmarkPickWorkerHostParallel(b *testing.B) {
	az0Sel := selector.MakeSelector(60*1000, func() ([]string, error) {
		return []string{"az0-worker:9600"}, nil
	})
	az1Sel := selector.MakeSelector(60*1000, func() ([]string, error) {
		return []string{"az1-worker:9600"}, nil
	})
	globalSel := selector.MakeSelector(60*1000, func() ([]string, error) {
		return []string{"any-worker:9600"}, nil
	})
	sel := &idcSelector{
		selectors: map[string]selector.Selector{
			"az0": az0Sel,
			"az1": az1Sel,
			"":    globalSel,
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			host := sel.get(context.Background(), "az0")
			if host == "" {
				b.Error("Get az0 returned empty")
			}
			host = sel.get(context.Background(), "az1")
			if host == "" {
				b.Error("Get az1 returned empty")
			}
			host = sel.get(context.Background(), "")
			if host == "" {
				b.Error("Get global returned empty")
			}
		}
	})
}
