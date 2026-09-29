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

// --- test fixtures ---

func defaultRepairMsg() *proto.ShardRepairMsg {
	return &proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
}

func repairKafkaMsg(msg *proto.ShardRepairMsg) *sarama.ConsumerMessage {
	b, _ := json.Marshal(msg)
	return &sarama.ConsumerMessage{Value: b}
}

func testVolume() *client.VolumeInfoSimple {
	return MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
}

func volumeWithHosts(vol *client.VolumeInfoSimple) *client.VolumeInfoSimple {
	for i := range vol.VunitLocations {
		vol.VunitLocations[i].Host = fmt.Sprintf("http://host-%d:9600", i)
	}
	return vol
}

func idcByVunitIdx(vol *client.VolumeInfoSimple, idcMap map[int]string) []string {
	idcByIdx := make([]string, len(vol.VunitLocations))
	for idx, idc := range idcMap {
		idcByIdx[idx] = idc
	}
	return idcByIdx
}

func mockTopologyWithDisks(t *testing.T, ctr *gomock.Controller, vol *client.VolumeInfoSimple, idcByIdx map[int]string) *MockClusterTopology {
	t.Helper()
	topo := NewMockClusterTopology(ctr)
	topo.EXPECT().GetVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	topo.EXPECT().UpdateVolume(any).AnyTimes().Return(&client.VolumeInfoSimple{}, nil)
	for i, loc := range vol.VunitLocations {
		disk := &client.DiskInfoSimple{Host: loc.Host}
		if idc, ok := idcByIdx[i]; ok {
			disk.Idc = idc
		}
		topo.EXPECT().GetDisk(loc.DiskID).Return(disk, true).AnyTimes()
	}
	return topo
}

func workerNodes() []clustermgr.ServiceNode {
	return []clustermgr.ServiceNode{
		{ClusterID: 1, Host: "worker-a:9600", Idc: "az0"},
		{ClusterID: 1, Host: "worker-b:9600", Idc: "az1"},
	}
}

func newShardRepairMgr(t *testing.T) *ShardRepairMgr {
	t.Helper()
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

func newDiskNotFoundMgr(t *testing.T, ctr *gomock.Controller) *ShardRepairMgr {
	t.Helper()
	mgr := newShardRepairMgr(t)
	mgr.chunkMissMigrateReporter = base.NewAbnormalReporter(proto.ClusterID(2), ShardRepair, base.ChunkMissMigrateAbnormal)
	mgr.clusterMgrCli = NewMockClusterMgrAPI(ctr)
	mgr.taskCli = NewMockTaskAPI(ctr)
	return mgr
}

// --- kafka consume path ---

func TestShardRepairConsumeKafka(t *testing.T) {
	mgr := newShardRepairMgr(t)
	stop := closer.New()
	defer stop.Close()

	validMsg := defaultRepairMsg()
	validKafka := repairKafkaMsg(validMsg)

	t.Run("invalid_json_returns_true", func(t *testing.T) {
		require.True(t, mgr.Consume([]*sarama.ConsumerMessage{{Value: []byte("123")}}, stop))
	})

	t.Run("empty_message_returns_true", func(t *testing.T) {
		empty := repairKafkaMsg(&proto.ShardRepairMsg{})
		require.True(t, mgr.Consume([]*sarama.ConsumerMessage{empty}, stop))
	})

	t.Run("repair_success", func(t *testing.T) {
		require.True(t, mgr.Consume([]*sarama.ConsumerMessage{validKafka}, stop))
	})

	t.Run("repair_failed_still_returns_true", func(t *testing.T) {
		ctr := gomock.NewController(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errMock)
		old := mgr.blobnodeCli
		mgr.blobnodeCli = blobnode
		defer func() { mgr.blobnodeCli = old }()

		require.True(t, mgr.Consume([]*sarama.ConsumerMessage{validKafka}, stop))
	})

	t.Run("closed_closer_returns_false", func(t *testing.T) {
		closed := closer.New()
		closed.Close()
		require.False(t, mgr.Consume([]*sarama.ConsumerMessage{validKafka}, closed))
	})
}

// --- direct consume path ---

func TestShardRepairConsumeDirect(t *testing.T) {
	ctx := context.Background()
	mgr := newShardRepairMgr(t)
	stop := closer.New()
	defer stop.Close()
	msg := defaultRepairMsg()

	t.Run("success", func(t *testing.T) {
		ret := mgr.consume(ctx, msg, stop)
		require.Equal(t, ShardRepairStatusDone, ret.status)
	})

	t.Run("worker_error", func(t *testing.T) {
		ctr := gomock.NewController(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errMock)
		old := mgr.blobnodeCli
		mgr.blobnodeCli = blobnode
		defer func() { mgr.blobnodeCli = old }()

		ret := mgr.consume(ctx, msg, stop)
		require.Equal(t, ShardRepairStatusFailed, ret.status)
		require.ErrorIs(t, ret.err, errMock)
	})

	t.Run("dest_replica_bad_triggers_volume_refresh", func(t *testing.T) {
		ctr := gomock.NewController(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrDestReplicaBad)
		old := mgr.blobnodeCli
		mgr.blobnodeCli = blobnode
		defer func() { mgr.blobnodeCli = old }()

		ret := mgr.consume(ctx, msg, stop)
		require.Equal(t, ShardRepairStatusFailed, ret.status)
		require.ErrorIs(t, ret.err, errcode.ErrDestReplicaBad)
	})

	t.Run("orphan_shard", func(t *testing.T) {
		ctr := gomock.NewController(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrOrphanShard)
		old := mgr.blobnodeCli
		mgr.blobnodeCli = blobnode
		defer func() { mgr.blobnodeCli = old }()

		ret := mgr.consume(ctx, msg, stop)
		require.Equal(t, ShardRepairStatusOrphan, ret.status)
		require.ErrorIs(t, ret.err, errcode.ErrOrphanShard)
	})

	t.Run("closed_closer_returns_undo", func(t *testing.T) {
		closed := closer.New()
		closed.Close()
		ret := mgr.consume(ctx, msg, closed)
		require.Equal(t, ShardRepairStatusUndo, ret.status)
	})

	t.Run("punished_message_skips_repair", func(t *testing.T) {
		punished := defaultRepairMsg()
		punished.Retry = defaultMessagePunishThreshold
		oldPunish := mgr.punishTime
		mgr.punishTime = 10 * time.Millisecond
		defer func() { mgr.punishTime = oldPunish }()

		ret := mgr.consume(ctx, punished, stop)
		require.Equal(t, ShardRepairStatusDone, ret.status)
	})

	t.Run("punished_message_cancelled_during_wait", func(t *testing.T) {
		punished := defaultRepairMsg()
		punished.Retry = defaultMessagePunishThreshold
		cancel := closer.New()
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel.Close()
		}()

		ret := mgr.consume(ctx, punished, cancel)
		require.Equal(t, ShardRepairStatusUndo, ret.status)
	})
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
	require.NotNil(t, mgr.blobnodeSelector)

	mgr.GetErrorStats()
	mgr.GetTaskStats()
	mgr.Run()
	require.NoError(t, mgr.startConsumer())
	mgr.stopConsumer()
	require.Nil(t, mgr.consumers)
	mgr.Close()

	_, err = NewShardRepairMgr(cfg, clusterTopology, switchMgr, blobnode, clusterCli, kafkaClient, nil)
	require.Error(t, err)
}

func TestProcessDiskNotFoundErr(t *testing.T) {
	ctx := context.Background()
	volume := testVolume()
	repairMsg := &proto.ShardRepairMsg{Vid: proto.Vid(1), Bid: proto.BlobID(1), BadIdx: []uint8{0}}
	missVuid := volume.VunitLocations[0].Vuid
	missDisk := volume.VunitLocations[0].DiskID

	tests := []struct {
		name         string
		setup        func(mgr *ShardRepairMgr, cm *MockClusterMgrAPI, task *MockTaskAPI)
		wantReported bool
	}{
		{
			name: "already_reported_skips_downstream",
			setup: func(mgr *ShardRepairMgr, _ *MockClusterMgrAPI, _ *MockTaskAPI) {
				mgr.chunkMissMigrateReporter.SetVuidReported(missVuid)
			},
			wantReported: true,
		},
		{
			name: "get_disk_info_error",
			setup: func(_ *ShardRepairMgr, cm *MockClusterMgrAPI, _ *MockTaskAPI) {
				cm.EXPECT().GetDiskInfo(any, missDisk).Return(nil, errMock)
			},
		},
		{
			name: "disk_still_repairing",
			setup: func(_ *ShardRepairMgr, cm *MockClusterMgrAPI, _ *MockTaskAPI) {
				cm.EXPECT().GetDiskInfo(any, missDisk).Return(MockGenDiskInfo(missDisk, proto.DiskStatusRepairing), nil)
			},
		},
		{
			name: "get_volume_info_error",
			setup: func(_ *ShardRepairMgr, cm *MockClusterMgrAPI, _ *MockTaskAPI) {
				cm.EXPECT().GetDiskInfo(any, missDisk).Return(MockGenDiskInfo(missDisk, proto.DiskStatusRepaired), nil)
				cm.EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(nil, errMock)
			},
		},
		{
			name: "volume_changed",
			setup: func(_ *ShardRepairMgr, cm *MockClusterMgrAPI, _ *MockTaskAPI) {
				cm.EXPECT().GetDiskInfo(any, missDisk).Return(MockGenDiskInfo(missDisk, proto.DiskStatusRepaired), nil)
				changed := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
				changed.VunitLocations[0].Vuid++
				cm.EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(changed, nil)
			},
		},
		{
			name: "check_task_exist_error",
			setup: func(_ *ShardRepairMgr, cm *MockClusterMgrAPI, task *MockTaskAPI) {
				cm.EXPECT().GetDiskInfo(any, missDisk).Return(MockGenDiskInfo(missDisk, proto.DiskStatusRepaired), nil)
				cm.EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(volume, nil)
				task.EXPECT().CheckTaskExist(any, proto.TaskTypeManualMigrate, missDisk, missVuid).Return(false, errMock)
			},
		},
		{
			name: "task_already_exists",
			setup: func(_ *ShardRepairMgr, cm *MockClusterMgrAPI, task *MockTaskAPI) {
				cm.EXPECT().GetDiskInfo(any, missDisk).Return(MockGenDiskInfo(missDisk, proto.DiskStatusRepaired), nil)
				cm.EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(volume, nil)
				task.EXPECT().CheckTaskExist(any, proto.TaskTypeManualMigrate, missDisk, missVuid).Return(true, nil)
			},
			wantReported: true,
		},
		{
			name: "full_path_reports_abnormal",
			setup: func(_ *ShardRepairMgr, cm *MockClusterMgrAPI, task *MockTaskAPI) {
				cm.EXPECT().GetDiskInfo(any, missDisk).Return(MockGenDiskInfo(missDisk, proto.DiskStatusRepaired), nil)
				cm.EXPECT().GetVolumeInfo(any, proto.Vid(1)).Return(volume, nil)
				task.EXPECT().CheckTaskExist(any, proto.TaskTypeManualMigrate, missDisk, missVuid).Return(false, nil)
			},
			wantReported: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctr := gomock.NewController(t)
			mgr := newDiskNotFoundMgr(t, ctr)
			cm := mgr.clusterMgrCli.(*MockClusterMgrAPI)
			task := mgr.taskCli.(*MockTaskAPI)
			tt.setup(mgr, cm, task)

			mgr.processDiskNotFoundErr(ctx, volume, repairMsg)
			require.Equal(t, tt.wantReported, mgr.chunkMissMigrateReporter.IsVuidReported(missVuid))
		})
	}
}

// --- idcSelector ---

func TestIDCSelectorInit(t *testing.T) {
	t.Run("lazy_init_without_get_service", func(t *testing.T) {
		ctr := gomock.NewController(t)
		sel := newIDCSelector(NewMockClusterMgrAPI(ctr), proto.ClusterID(1))
		require.NotNil(t, sel)
		require.Empty(t, sel.selectors)
	})

	t.Run("build_per_idc_selectors_from_workers", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cmCli := NewMockClusterMgrAPI(ctr)
		cmCli.EXPECT().GetService(gomock.Any(), proto.ServiceNameWorker, gomock.Any()).AnyTimes().
			Return(workerNodes(), nil)

		sel := newIDCSelector(cmCli, proto.ClusterID(1))
		require.Equal(t, "worker-a:9600", sel.get(context.Background(), "az0"))
		require.Equal(t, "worker-b:9600", sel.get(context.Background(), "az1"))
		require.NotEmpty(t, sel.get(context.Background(), ""))
	})
}

func TestIDCSelectorConcurrent(t *testing.T) {
	ctr := gomock.NewController(t)
	cmCli := NewMockClusterMgrAPI(ctr)
	cmCli.EXPECT().GetService(gomock.Any(), proto.ServiceNameWorker, gomock.Any()).AnyTimes().
		Return(workerNodes(), nil)

	sel := newIDCSelector(cmCli, proto.ClusterID(1))

	const goroutines = 16
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, idc := range []string{"az0", "az1", ""} {
				if sel.get(context.Background(), idc) == "" {
					t.Error("concurrent get returned empty for idc:", idc)
				}
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
	require.Empty(t, sel.get(context.Background(), "az0"))
	require.Empty(t, sel.get(context.Background(), ""))
}

func TestResolveRepairIDC(t *testing.T) {
	vol := volumeWithHosts(testVolume())

	tests := []struct {
		name    string
		badIdx  []uint8
		idcMap  map[int]string
		wantIDC string
	}{
		{name: "no_bad_index", badIdx: nil},
		{name: "bad_index_out_of_range", badIdx: []uint8{99}},
		{name: "first_bad_index_az0", badIdx: []uint8{0}, idcMap: map[int]string{0: "az0"}, wantIDC: "az0"},
		{name: "second_bad_index_az1", badIdx: []uint8{1}, idcMap: map[int]string{1: "az1"}, wantIDC: "az1"},
		{name: "no_idc_on_disk", badIdx: []uint8{0}, idcMap: map[int]string{}},
		{name: "iterate_to_find_valid_idc", badIdx: []uint8{0, 1, 2}, idcMap: map[int]string{2: "az2"}, wantIDC: "az2"},
	}

	mgr := newShardRepairMgr(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: tt.badIdx}
			idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx(vol, tt.idcMap))
			require.Equal(t, tt.wantIDC, idc)
		})
	}
}

func TestPickWorkerHost(t *testing.T) {
	vol := volumeWithHosts(testVolume())

	t.Run("same_az_worker", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return([]string{"az0-worker"})
		mgr.blobnodeSelector.selectors["az0"] = mockAz0

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx(vol, map[int]string{0: "az0"}))
		require.Equal(t, "az0-worker", mgr.blobnodeSelector.get(context.Background(), idc))
	})

	t.Run("fallback_when_az_empty", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return(nil)
		mgr.blobnodeSelector.selectors["az0"] = mockAz0
		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(1).Return([]string{"fallback-worker"})
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx(vol, map[int]string{0: "az0"}))
		require.Equal(t, "fallback-worker", mgr.blobnodeSelector.get(context.Background(), idc))
	})

	t.Run("route_different_az_to_different_workers", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return([]string{"az0-worker"})
		mgr.blobnodeSelector.selectors["az0"] = mockAz0
		mockAz1 := mocks.NewMockSelector(ctr)
		mockAz1.EXPECT().GetRandomN(1).Return([]string{"az1-worker"})
		mgr.blobnodeSelector.selectors["az1"] = mockAz1

		idcMap := map[int]string{0: "az0", 3: "az1"}
		{
			msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
			idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx(vol, idcMap))
			require.Equal(t, "az0-worker", mgr.blobnodeSelector.get(context.Background(), idc))
		}
		{
			msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{3}}
			idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx(vol, idcMap))
			require.Equal(t, "az1-worker", mgr.blobnodeSelector.get(context.Background(), idc))
		}
	})

	t.Run("empty_when_all_selectors_exhausted", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return(nil)
		mgr.blobnodeSelector.selectors["az0"] = mockAz0
		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(1).AnyTimes().Return(nil)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx(vol, map[int]string{0: "az0"}))
		require.Empty(t, mgr.blobnodeSelector.get(context.Background(), idc))
	})

	t.Run("lazy_refresh_on_az_miss", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(any).AnyTimes().Return(nil)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		cmCli := NewMockClusterMgrAPI(ctr)
		cmCli.EXPECT().GetService(gomock.Any(), proto.ServiceNameWorker, gomock.Any()).
			Return([]clustermgr.ServiceNode{{ClusterID: 1, Host: "lazy-worker:9600", Idc: "az0"}}, nil)
		mgr.blobnodeSelector.clusterMgrCli = cmCli

		msg := &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}}
		idc := mgr.resolveRepairIDC(context.Background(), msg, idcByVunitIdx(vol, map[int]string{0: "az0"}))
		require.Equal(t, "lazy-worker:9600", mgr.blobnodeSelector.get(context.Background(), idc))
	})
}

// --- repair execution ---

func TestTryRepair(t *testing.T) {
	ctx := context.Background()
	volume := testVolume()

	t.Run("no_worker_available", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockSel := mocks.NewMockSelector(ctr)
		mockSel.EXPECT().GetRandomN(any).AnyTimes().Return(nil)
		mgr.blobnodeSelector.selectors[""] = mockSel

		done, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.ErrorIs(t, err, ErrBlobnodeServiceUnavailable)
		require.True(t, done.EqualWith(volume))
	})

	t.Run("repair_success", func(t *testing.T) {
		mgr := newShardRepairMgr(t)
		mgr.clusterTopology.(*MockClusterTopology).EXPECT().GetDisk(any).AnyTimes().
			Return(&client.DiskInfoSimple{Host: volume.VunitLocations[0].Host}, true)

		done, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.True(t, done.EqualWith(volume))
	})

	t.Run("repair_failed_update_volume_failed", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrDestReplicaBad)
		mgr.blobnodeCli = blobnode

		topo := NewMockClusterTopology(ctr)
		topo.EXPECT().UpdateVolume(any).Return(volume, errcode.ErrUpdateVolCacheFreq)
		topo.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{Host: volume.VunitLocations[0].Host}, true)
		mgr.clusterTopology = topo

		done, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.ErrorIs(t, err, errcode.ErrDestReplicaBad)
		require.True(t, done.EqualWith(volume))
	})

	t.Run("repair_failed_volume_unchanged", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrDestReplicaBad)
		mgr.blobnodeCli = blobnode

		topo := NewMockClusterTopology(ctr)
		topo.EXPECT().UpdateVolume(any).Return(volume, nil)
		topo.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{Host: volume.VunitLocations[0].Host}, true)
		mgr.clusterTopology = topo

		done, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.ErrorIs(t, err, errcode.ErrDestReplicaBad)
		require.True(t, done.EqualWith(volume))
	})

	t.Run("repair_retry_after_volume_change", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrDestReplicaBad)
		blobnode.EXPECT().RepairShard(any, any, any).Return(nil)
		mgr.blobnodeCli = blobnode

		newVolume := MockGenVolInfo(proto.Vid(1), codemode.EC3P3, proto.VolumeStatusActive)
		newVolume.VunitLocations[5].Vuid++
		topo := NewMockClusterTopology(ctr)
		topo.EXPECT().UpdateVolume(any).Return(newVolume, nil)
		topo.EXPECT().GetDisk(any).AnyTimes().Return(&client.DiskInfoSimple{Host: volume.VunitLocations[0].Host}, true)
		mgr.clusterTopology = topo

		done, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.False(t, done.EqualWith(volume))
		require.True(t, done.EqualWith(newVolume))
	})

	t.Run("disk_not_found_triggers_miss_migrate_report", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, any, any).Return(errcode.ErrNoSuchDisk).AnyTimes()
		mgr.blobnodeCli = blobnode

		cmCli := NewMockClusterMgrAPI(ctr)
		cmCli.EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli = cmCli

		taskCli := NewMockTaskAPI(ctr)
		taskCli.EXPECT().CheckTaskExist(any, any, any, any).Return(false, nil)
		mgr.taskCli = taskCli
		mgr.chunkMissMigrateReporter = base.NewAbnormalReporter(proto.ClusterID(1), ShardRepair, base.ChunkMissMigrateAbnormal)

		missIdx := uint8(0)
		cmCli.EXPECT().GetDiskInfo(any, any).Return(
			MockGenDiskInfo(volume.VunitLocations[missIdx].DiskID, proto.DiskStatusRepaired), nil)

		_, err := mgr.tryRepair(ctx, volume, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{missIdx}})
		require.ErrorIs(t, err, errcode.ErrNoSuchDisk)
	})
}

func TestRepairShardWithIDC(t *testing.T) {
	t.Run("same_az_worker", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		vol := volumeWithHosts(testVolume())

		blobnode := NewMockBlobnodeAPI(ctr)
		blobnode.EXPECT().RepairShard(any, gomock.Eq("az0-worker"), any).Return(nil)
		mgr.blobnodeCli = blobnode

		mockAz0 := mocks.NewMockSelector(ctr)
		mockAz0.EXPECT().GetRandomN(1).Return([]string{"az0-worker"})
		mgr.blobnodeSelector.selectors["az0"] = mockAz0
		mgr.clusterTopology = mockTopologyWithDisks(t, ctr, vol, map[int]string{0: "az0"})

		done, err := mgr.repairShard(context.Background(), vol, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.NotNil(t, done)
	})

	t.Run("fallback_global_when_disk_has_no_idc", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		vol := volumeWithHosts(testVolume())
		mgr.clusterTopology = mockTopologyWithDisks(t, ctr, vol, nil)

		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(1).Return([]string{"http://127.0.0.1:9600"}).Times(1)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		done, err := mgr.repairShard(context.Background(), vol, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.NotNil(t, done)
	})

	t.Run("fallback_global_when_az_selector_missing", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		vol := volumeWithHosts(testVolume())
		mgr.clusterTopology = mockTopologyWithDisks(t, ctr, vol, map[int]string{0: "az0"})

		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(1).Return([]string{"http://127.0.0.1:9600"}).Times(1)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		done, err := mgr.repairShard(context.Background(), vol, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.NoError(t, err)
		require.NotNil(t, done)
	})

	t.Run("unavailable_when_no_host", func(t *testing.T) {
		ctr := gomock.NewController(t)
		mgr := newShardRepairMgr(t)
		mockGlobal := mocks.NewMockSelector(ctr)
		mockGlobal.EXPECT().GetRandomN(any).AnyTimes().Return(nil)
		mgr.blobnodeSelector.selectors[""] = mockGlobal

		vol := volumeWithHosts(testVolume())
		done, err := mgr.repairShard(context.Background(), vol, &proto.ShardRepairMsg{Bid: 1, Vid: 1, BadIdx: []uint8{0}})
		require.ErrorIs(t, err, ErrBlobnodeServiceUnavailable)
		require.NotNil(t, done)
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
			for _, idc := range []string{"az0", "az1", ""} {
				if sel.get(context.Background(), idc) == "" {
					b.Errorf("Get %q returned empty", idc)
				}
			}
		}
	})
}
