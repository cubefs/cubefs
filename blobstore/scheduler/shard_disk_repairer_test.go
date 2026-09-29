// Copyright 2024 The CubeFS Authors.
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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

// --- fixtures ---

var (
	brokenShardDisk = &client.ShardNodeDiskInfo{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		Status:       proto.DiskStatusBroken,
		DiskID:       1,
		FreeShardCnt: 100,
		UsedShardCnt: 7,
	}
	repairingShardDisk = &client.ShardNodeDiskInfo{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		Status:       proto.DiskStatusRepairing,
		DiskID:       1,
		FreeShardCnt: 2,
		UsedShardCnt: 1,
	}
	repairedShardDisk = &client.ShardNodeDiskInfo{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		Status:       proto.DiskStatusRepaired,
		DiskID:       1,
		FreeShardCnt: 100,
		UsedShardCnt: 7,
	}
	shardRepairTask = &proto.Task{
		ModuleType: proto.TypeShardNode,
		TaskID:     "mock_task_id",
		TaskType:   proto.TaskTypeShardDiskRepair,
	}
	shardUnitOnDisk1 = &client.ShardUnitInfoSimple{
		DiskID:  1,
		Learner: false,
		Suid:    proto.EncodeSuid(101, 0, 0),
	}
)

func newMockShardDiskRepairerMgr(t *testing.T) *ShardDiskRepairMgr {
	t.Helper()
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	migrator := NewMockShardMigrator(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)
	c := closer.New()

	migrator.EXPECT().Close().AnyTimes().DoAndReturn(c.Close)
	migrator.EXPECT().Done().AnyTimes().Return(c.Done())
	migrator.EXPECT().WaitEnable().AnyTimes().Return()
	migrator.EXPECT().Enabled().AnyTimes().Return(true)

	mgr := NewShardDiskRepairMgr(&ShardMigrateConfig{}, clusterMgr, taskSwitch)
	mgr.ShardMigrator = migrator
	return mgr
}

func (mgr *ShardDiskRepairMgr) clusterMgr() *MockClusterMgrAPI {
	return mgr.clusterMgrCli.(*MockClusterMgrAPI)
}

func (mgr *ShardDiskRepairMgr) migrator() *MockShardMigrator {
	return mgr.ShardMigrator.(*MockShardMigrator)
}

// --- lifecycle ---

func TestShardDiskRepairMgr_Run(t *testing.T) {
	mgr := newMockShardDiskRepairerMgr(t)
	defer mgr.Close()

	mgr.migrator().EXPECT().Run().Return()
	mgr.cfg.CollectTaskIntervalS = 1
	mgr.cfg.CheckTaskIntervalS = 1
	require.True(t, mgr.Enabled())
	mgr.Run()
}

func TestShardDiskRepairLoad(t *testing.T) {
	mgr := newMockShardDiskRepairerMgr(t)
	mgr.clusterMgr().EXPECT().ListRepairingShardDisk(any).Return(nil, nil)
	mgr.migrator().EXPECT().Load().Return(nil)
	require.NoError(t, mgr.Load())
}

func TestShardDiskRepairLoadTaskCallback(t *testing.T) {
	ctx := context.Background()

	t.Run("disk_already_in_repairing_set", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(repairingShardDisk.DiskID, repairingShardDisk)
		mgr.loadTaskCallback(ctx, repairingShardDisk.DiskID)
		require.Equal(t, 1, mgr.repairingDisks.size())
	})

	t.Run("get_disk_info_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, repairingShardDisk.DiskID).Return(nil, errMock)
		mgr.loadTaskCallback(ctx, repairingShardDisk.DiskID)
		require.Equal(t, 0, mgr.repairingDisks.size())
	})

	t.Run("add_repairing_disk_from_cm", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, repairingShardDisk.DiskID).Return(repairingShardDisk, nil)
		mgr.loadTaskCallback(ctx, repairingShardDisk.DiskID)
		require.Equal(t, 1, mgr.repairingDisks.size())
		got, ok := mgr.repairingDisks.get(repairingShardDisk.DiskID)
		require.True(t, ok)
		require.Equal(t, repairingShardDisk.Host, got.Host)
	})
}

// --- collection ---

func TestShardDiskRepairCollectionTask(t *testing.T) {
	t.Run("no_broken_disk", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		defer mgr.Close()
		mgr.cfg.DiskConcurrency = 1
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return(nil, nil)
		mgr.collectionTask()
	})

	t.Run("list_broken_disk_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.cfg.DiskConcurrency = 1
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return(nil, errMock)
		mgr.collectionTask()
		require.Equal(t, 0, mgr.repairingDisks.size())
	})

	t.Run("list_migrating_suid_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.cfg.DiskConcurrency = 1
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{brokenShardDisk}, nil)
		mgr.migrator().EXPECT().ListMigratingSuid(any, any).Return(nil, errMock)
		mgr.collectionTask()
		require.Equal(t, 0, mgr.repairingDisks.size())
	})

	t.Run("list_immigrated_suid_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.cfg.DiskConcurrency = 1
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{brokenShardDisk}, nil)
		mgr.migrator().EXPECT().ListMigratingSuid(any, any).Return(nil, nil)
		mgr.migrator().EXPECT().ListImmigratedSuid(any, any).Return(nil, errMock)
		mgr.collectionTask()
		require.Equal(t, 0, mgr.repairingDisks.size())
	})

	t.Run("success_adds_repairing_disk", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.cfg.DiskConcurrency = 1
		suid := proto.EncodeSuid(101, 0, 0)
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{brokenShardDisk}, nil)
		mgr.migrator().EXPECT().ListMigratingSuid(any, any).Return(nil, nil)
		mgr.migrator().EXPECT().ListImmigratedSuid(any, any).Return([]proto.Suid{suid}, nil)
		mgr.migrator().EXPECT().AddTask(any, any).Return()
		mgr.clusterMgr().EXPECT().SetShardDiskRepairing(any, any).Return(nil)
		mgr.collectionTask()
		require.Equal(t, 1, mgr.repairingDisks.size())
	})
}

func TestAcquireBrokenShardDisk(t *testing.T) {
	hostA1 := &client.ShardNodeDiskInfo{DiskID: 10, Host: "host-a", Status: proto.DiskStatusBroken}
	hostA2 := &client.ShardNodeDiskInfo{DiskID: 11, Host: "host-a", Status: proto.DiskStatusBroken}
	hostB1 := &client.ShardNodeDiskInfo{DiskID: 20, Host: "host-b", Status: proto.DiskStatusBroken}
	hostC1 := &client.ShardNodeDiskInfo{DiskID: 30, Host: "host-c", Status: proto.DiskStatusBroken}

	t.Run("no_broken_disks", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return(nil, nil)
		got, err := mgr.acquireBrokenDisk(context.Background())
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("all_candidates_already_repairing", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(hostA1.DiskID, hostA1)
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{hostA1}, nil)
		got, err := mgr.acquireBrokenDisk(context.Background())
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("prefer_idle_host", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(hostA1.DiskID, hostA1)
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{hostA2, hostB1}, nil)
		got, err := mgr.acquireBrokenDisk(context.Background())
		require.NoError(t, err)
		require.Equal(t, "host-b", got.Host)
	})

	t.Run("all_hosts_busy_fallback", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(hostA1.DiskID, hostA1)
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{hostA2}, nil)
		got, err := mgr.acquireBrokenDisk(context.Background())
		require.NoError(t, err)
		require.Equal(t, hostA2.DiskID, got.DiskID)
	})

	t.Run("multiple_idle_hosts", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(hostA1.DiskID, hostA1)
		mgr.clusterMgr().EXPECT().ListBrokenShardDisk(any).Return(
			[]*client.ShardNodeDiskInfo{hostA2, hostB1, hostC1}, nil)
		got, err := mgr.acquireBrokenDisk(context.Background())
		require.NoError(t, err)
		require.NotEqual(t, "host-a", got.Host)
	})
}

// --- generateTask / repairDisk ---

func TestShardDiskRepairGenerateTask(t *testing.T) {
	ctx := context.Background()

	t.Run("list_migrating_suid_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.migrator().EXPECT().ListMigratingSuid(any, brokenShardDisk.DiskID).Return(nil, errMock)
		require.Error(t, mgr.generateTask(ctx, brokenShardDisk))
	})

	t.Run("list_immigrated_suid_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.migrator().EXPECT().ListMigratingSuid(any, brokenShardDisk.DiskID).Return(nil, nil)
		mgr.migrator().EXPECT().ListImmigratedSuid(any, brokenShardDisk.DiskID).Return(nil, errMock)
		require.Error(t, mgr.generateTask(ctx, brokenShardDisk))
	})

	t.Run("add_task_for_remain_suids", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		suid := proto.EncodeSuid(101, 0, 0)
		mgr.migrator().EXPECT().ListMigratingSuid(any, brokenShardDisk.DiskID).Return(nil, nil)
		mgr.migrator().EXPECT().ListImmigratedSuid(any, brokenShardDisk.DiskID).Return([]proto.Suid{suid}, nil)
		mgr.migrator().EXPECT().AddTask(any, gomock.Any()).Return()
		require.NoError(t, mgr.generateTask(ctx, brokenShardDisk))
	})
}

func TestShardDiskRepairRepairDisk(t *testing.T) {
	ctx := context.Background()

	t.Run("get_disk_info_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, brokenShardDisk.DiskID).Return(nil, errMock)
		require.Error(t, mgr.repairDisk(ctx, brokenShardDisk.DiskID))
	})

	t.Run("generate_task_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, brokenShardDisk.DiskID).Return(brokenShardDisk, nil)
		mgr.migrator().EXPECT().ListMigratingSuid(any, any).Return(nil, errMock)
		require.Error(t, mgr.repairDisk(ctx, brokenShardDisk.DiskID))
	})

	t.Run("broken_disk_sets_repairing", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		suid := proto.EncodeSuid(101, 0, 0)
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, brokenShardDisk.DiskID).Return(brokenShardDisk, nil)
		mgr.migrator().EXPECT().ListMigratingSuid(any, any).Return(nil, nil)
		mgr.migrator().EXPECT().ListImmigratedSuid(any, any).Return([]proto.Suid{suid}, nil)
		mgr.migrator().EXPECT().AddTask(any, any).Return()
		mgr.clusterMgr().EXPECT().SetShardDiskRepairing(any, brokenShardDisk.DiskID).Return(nil)
		require.NoError(t, mgr.repairDisk(ctx, brokenShardDisk.DiskID))
	})

	t.Run("repairing_disk_skips_set_repairing", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.migrator().EXPECT().ListMigratingSuid(any, any).Return(nil, nil)
		mgr.migrator().EXPECT().ListImmigratedSuid(any, any).Return(nil, nil)
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, repairingShardDisk.DiskID).Return(repairingShardDisk, nil)
		require.NoError(t, mgr.repairDisk(ctx, repairingShardDisk.DiskID))
	})
}

// --- check repaired ---

func TestCheckRepaired(t *testing.T) {
	diskID := repairingShardDisk.DiskID

	t.Run("list_tasks_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(diskID, repairingShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.Equal(t, 1, mgr.repairingDisks.size())
	})

	t.Run("list_shard_units_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(diskID, repairingShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return([]*proto.Task{shardRepairTask}, nil)
		mgr.clusterMgr().EXPECT().ListDiskShardUnits(any, diskID).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.Equal(t, 1, mgr.repairingDisks.size())
	})

	t.Run("clear_junk_tasks_when_units_empty", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(diskID, repairingShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return([]*proto.Task{shardRepairTask}, nil)
		mgr.clusterMgr().EXPECT().ListDiskShardUnits(any, diskID).Return(nil, nil)
		mgr.clusterMgr().EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.checkRepairedAndClear()
		require.Equal(t, 1, mgr.repairingDisks.size())
	})

	t.Run("revise_when_units_remain_without_tasks", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(diskID, repairingShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return(nil, nil)
		mgr.clusterMgr().EXPECT().ListDiskShardUnits(any, diskID).Return([]*client.ShardUnitInfoSimple{shardUnitOnDisk1}, nil)
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, diskID).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.Equal(t, 1, mgr.repairingDisks.size())
	})

	t.Run("revise_success", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		suid := proto.EncodeSuid(101, 0, 0)
		mgr.repairingDisks.add(diskID, repairingShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return(nil, nil)
		mgr.clusterMgr().EXPECT().ListDiskShardUnits(any, diskID).Return([]*client.ShardUnitInfoSimple{shardUnitOnDisk1}, nil)
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, diskID).Return(repairingShardDisk, nil)
		mgr.migrator().EXPECT().ListMigratingSuid(any, any).Return(nil, nil)
		mgr.migrator().EXPECT().ListImmigratedSuid(any, any).Return([]proto.Suid{suid}, nil)
		mgr.migrator().EXPECT().AddTask(any, any).Return()
		mgr.checkRepairedAndClear()
		require.Equal(t, 1, mgr.repairingDisks.size())
	})

	t.Run("set_repaired_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(diskID, repairingShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return(nil, nil)
		mgr.clusterMgr().EXPECT().ListDiskShardUnits(any, diskID).Return(nil, nil)
		mgr.clusterMgr().EXPECT().SetShardDiskRepaired(any, diskID).Return(errMock)
		mgr.checkRepairedAndClear()
		require.Equal(t, 1, mgr.repairingDisks.size())
	})

	t.Run("repair_complete", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(diskID, repairingShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return(nil, nil)
		mgr.clusterMgr().EXPECT().ListDiskShardUnits(any, diskID).Return(nil, nil)
		mgr.clusterMgr().EXPECT().SetShardDiskRepaired(any, diskID).Return(nil)
		mgr.checkRepairedAndClear()
		require.Equal(t, 0, mgr.repairingDisks.size())
		require.Equal(t, 1, mgr.repairedDisks.size())
	})
}

func TestClearJunkTask(t *testing.T) {
	diskID := repairedShardDisk.DiskID

	t.Run("protection_window_not_expired", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairedDisks.add(diskID, time.Now())
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 1, mgr.repairedDisks.size())
	})

	t.Run("disk_still_repairing", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairedDisks.add(diskID, time.Now().Add(-junkMigrationTaskProtectionWindow))
		repairing := *repairedShardDisk
		repairing.Status = proto.DiskStatusRepairing
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, diskID).Return(&repairing, nil)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 1, mgr.repairedDisks.size())
	})

	t.Run("list_tasks_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairedDisks.add(diskID, time.Now().Add(-junkMigrationTaskProtectionWindow))
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, diskID).Return(repairedShardDisk, nil)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return(nil, errMock)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 1, mgr.repairedDisks.size())
	})

	t.Run("clear_junk_tasks_success", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairedDisks.add(diskID, time.Now().Add(-junkMigrationTaskProtectionWindow))
		mgr.clusterMgr().EXPECT().GetShardDiskInfo(any, diskID).Return(repairedShardDisk, nil)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, diskID).Return([]*proto.Task{shardRepairTask}, nil)
		mgr.clusterMgr().EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 0, mgr.repairedDisks.size())
	})
}

// --- progress ---

func TestShardDiskRepairProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("no_repairing_disks", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		disks, total, migrated := mgr.Progress(ctx)
		require.Empty(t, disks)
		require.Equal(t, 0, total)
		require.Equal(t, 0, migrated)
	})

	t.Run("list_tasks_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(brokenShardDisk.DiskID, brokenShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, brokenShardDisk.DiskID).Return(nil, errMock)
		disks, total, migrated := mgr.Progress(ctx)
		require.Empty(t, disks)
		require.Equal(t, 0, total)
		require.Equal(t, 0, migrated)
	})

	t.Run("single_disk_progress", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(brokenShardDisk.DiskID, brokenShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, brokenShardDisk.DiskID).
			Return([]*proto.Task{shardRepairTask, shardRepairTask}, nil)
		disks, total, migrated := mgr.Progress(ctx)
		require.Equal(t, []proto.DiskID{brokenShardDisk.DiskID}, disks)
		require.Equal(t, int(brokenShardDisk.UsedShardCnt), total)
		require.Equal(t, int(brokenShardDisk.UsedShardCnt)-2, migrated)
	})
}

func TestShardDiskRepairDiskProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("not_repairing_disk", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		_, err := mgr.DiskProgress(ctx, brokenShardDisk.DiskID)
		require.Error(t, err)
	})

	t.Run("list_tasks_error", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(brokenShardDisk.DiskID, brokenShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, brokenShardDisk.DiskID).Return(nil, errMock)
		_, err := mgr.DiskProgress(ctx, brokenShardDisk.DiskID)
		require.True(t, errors.Is(err, errMock))
	})

	t.Run("disk_stats", func(t *testing.T) {
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(brokenShardDisk.DiskID, brokenShardDisk)
		mgr.clusterMgr().EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair, brokenShardDisk.DiskID).
			Return([]*proto.Task{shardRepairTask, shardRepairTask, shardRepairTask}, nil)
		stats, err := mgr.DiskProgress(ctx, brokenShardDisk.DiskID)
		require.NoError(t, err)
		require.Equal(t, &api.DiskMigratingStats{
			TotalTasksCnt:    int(brokenShardDisk.UsedShardCnt),
			MigratedTasksCnt: int(brokenShardDisk.UsedShardCnt) - 3,
		}, stats)
	})
}
