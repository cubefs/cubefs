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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

func newMockShardDiskRepairerMgr(t *testing.T) *ShardDiskRepairMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	migrator := NewMockShardMigrator(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)
	cfg := &ShardMigrateConfig{}
	c := closer.New()

	migrator.EXPECT().Close().AnyTimes().DoAndReturn(c.Close)
	migrator.EXPECT().Done().AnyTimes().Return(c.Done())
	migrator.EXPECT().WaitEnable().AnyTimes().Return()
	migrator.EXPECT().Enabled().AnyTimes().Return(true)

	mgr := NewShardDiskRepairMgr(cfg, clusterMgr, taskSwitch)

	mgr.ShardMigrator = migrator

	return mgr
}

func TestShardDiskRepairMgr_Run(t *testing.T) {
	mgr := newMockShardDiskRepairerMgr(t)
	defer mgr.Close()

	mgr.ShardMigrator.(*MockShardMigrator).EXPECT().Run().Return()
	mgr.cfg.CollectTaskIntervalS = 1
	mgr.cfg.CheckTaskIntervalS = 1
	require.True(t, mgr.Enabled())
	mgr.Run()

	time.Sleep(1 * time.Second)
}

func TestShardDiskRepairLoad(t *testing.T) {
	mgr := newMockShardDiskRepairerMgr(t)
	mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListRepairingShardDisk(any).Return(nil, nil)
	mgr.ShardMigrator.(*MockShardMigrator).EXPECT().Load().Return(nil)
	err := mgr.Load()
	require.NoError(t, err)
}

func TestShardDiskRepairCollectionTask(t *testing.T) {
	disk1 := &client.ShardNodeDiskInfo{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		Status:       proto.DiskStatusBroken,
		DiskID:       1,
		FreeShardCnt: 100,
		UsedShardCnt: 7,
	}
	{
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.cfg.DiskConcurrency = 1
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenShardDisk(any).Return(nil, nil)
		mgr.collectionTask()
		mgr.Close()
	}
	{
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.cfg.DiskConcurrency = 1
		// list broken disk err
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenShardDisk(any).Return(nil, errMock)
		mgr.collectionTask()
		require.True(t, mgr.repairingDisks.size() == 0)

		// list migrating task failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{disk1}, nil)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().ListMigratingSuid(any, any).Return(nil, errMock)
		mgr.collectionTask()
		require.True(t, mgr.repairingDisks.size() == 0)

		// list immigrating suid failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{disk1}, nil)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().ListMigratingSuid(any, any).Return(nil, nil)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().ListImmigratedSuid(any, any).Return(nil, errMock)
		mgr.collectionTask()
		require.True(t, mgr.repairingDisks.size() == 0)

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenShardDisk(any).Return([]*client.ShardNodeDiskInfo{disk1}, nil)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().ListMigratingSuid(any, any).Return(nil, nil)
		suid := proto.EncodeSuid(101, 0, 0)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().ListImmigratedSuid(any, any).Return([]proto.Suid{suid}, nil)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().AddTask(any, any).Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetShardDiskRepairing(any, any).Return(nil)
		mgr.collectionTask()
		require.True(t, mgr.repairingDisks.size() == 1)
	}
}

func TestCheckRepaired(t *testing.T) {
	disk1 := &client.ShardNodeDiskInfo{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		Status:       proto.DiskStatusRepairing,
		DiskID:       1,
		FreeShardCnt: 2,
		UsedShardCnt: 1,
	}
	unitInfoSimple := &client.ShardUnitInfoSimple{
		DiskID:  1,
		Learner: false,
		Suid:    proto.EncodeSuid(101, 0, 0),
	}
	task := &proto.Task{ModuleType: proto.TypeShardNode, TaskID: "mock_task_id", TaskType: proto.TaskTypeShardDiskRepair}
	{
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairingDisks.add(disk1.DiskID, disk1)

		// list task failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.repairingDisks.size() == 1)

		// list shard units failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return([]*proto.Task{task}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any, disk1.DiskID).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.repairingDisks.size() == 1)

		// has junk task
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return([]*proto.Task{task}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any,
			disk1.DiskID).Return(nil, nil)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.repairingDisks.size() == 1)

		// should repair again and get disk info failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any, disk1.DiskID).Return([]*client.ShardUnitInfoSimple{unitInfoSimple}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardDiskInfo(any, any).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.repairingDisks.size() == 1)

		// should repair again success
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any, disk1.DiskID).Return([]*client.ShardUnitInfoSimple{unitInfoSimple}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardDiskInfo(any, any).Return(disk1, nil)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().ListMigratingSuid(any, any).Return(nil, nil)
		suid := proto.EncodeSuid(101, 0, 0)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().ListImmigratedSuid(any, any).Return([]proto.Suid{suid}, nil)
		mgr.ShardMigrator.(*MockShardMigrator).EXPECT().AddTask(any, any).Return()
		mgr.checkRepairedAndClear()
		require.True(t, mgr.repairingDisks.size() == 1)

		// check success and set repaired failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any, disk1.DiskID).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetShardDiskRepaired(any, disk1.DiskID).Return(errMock)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.repairingDisks.size() == 1)

		// success
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any, disk1.DiskID).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetShardDiskRepaired(any, disk1.DiskID).Return(nil)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.repairingDisks.size() == 0)
		require.True(t, mgr.repairedDisks.size() == 1)
	}
}

func TestClearJunkTask(t *testing.T) {
	disk1 := &client.ShardNodeDiskInfo{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		Status:       proto.DiskStatusRepaired,
		DiskID:       1,
		FreeShardCnt: 100,
		UsedShardCnt: 7,
	}
	task := &proto.Task{ModuleType: proto.TypeShardNode, TaskID: "mock_task_id", TaskType: proto.TaskTypeShardDiskRepair}
	{
		// time not satisfied
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairedDisks.add(disk1.DiskID, time.Now())
		mgr.checkAndClearJunkTasks()
		require.True(t, mgr.repairedDisks.size() == 1)
	}
	{
		// disk is repairing
		mgr := newMockShardDiskRepairerMgr(t)
		mgr.repairedDisks.add(disk1.DiskID, time.Now().Add(-junkMigrationTaskProtectionWindow))
		disk1.Status = proto.DiskStatusRepairing
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardDiskInfo(any, any).Return(disk1, nil)
		mgr.checkAndClearJunkTasks()
		disk1.Status = proto.DiskStatusRepaired
		require.True(t, mgr.repairedDisks.size() == 1)

		// list task failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardDiskInfo(any, any).Return(disk1, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return(nil, errMock)
		mgr.checkAndClearJunkTasks()
		require.True(t, mgr.repairedDisks.size() == 1)

		// success
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardDiskInfo(any, any).Return(disk1, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, proto.TaskTypeShardDiskRepair,
			disk1.DiskID).Return([]*proto.Task{task}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.checkAndClearJunkTasks()
		require.True(t, mgr.repairedDisks.size() == 0)
	}
}
