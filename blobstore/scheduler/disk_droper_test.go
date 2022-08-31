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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

func newDiskDroper(t *testing.T) *DiskDropMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	volumeUpdater := NewMockVolumeUpdater(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)
	taskLogger := mocks.NewMockRecordLogEncoder(ctr)
	c := closer.New()

	migrater := NewMockMigrater(ctr)
	migrater.EXPECT().StatQueueTaskCnt().AnyTimes().Return(0, 0, 0)
	migrater.EXPECT().Close().AnyTimes().DoAndReturn(c.Close)
	migrater.EXPECT().Done().AnyTimes().Return(c.Done())
	mgr := NewDiskDropMgr(clusterMgr, volumeUpdater, taskSwitch, taskLogger, &MigrateConfig{})
	mgr.cfg.DiskConcurrency = 1
	mgr.IMigrator = migrater
	return mgr
}

func TestDiskDropLoad(t *testing.T) {
	{
		mgr := newDiskDroper(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, errMock)
		err := mgr.Load()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskDroper(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().Load().Return(nil)
		err := mgr.Load()
		require.NoError(t, err)
	}
	{
		mgr := newDiskDroper(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{Disk: testDisk1}, {Disk: testDisk2}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().Load().Return(errMock)
		err := mgr.Load()
		require.Equal(t, 2, mgr.droppingDisks.size())
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskDroper(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(
			[]*client.MigratingDiskMeta{{Disk: &client.DiskInfoSimple{DiskID: proto.DiskID(1)}}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().Load().Return(errMock)
		err := mgr.Load()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskDroper(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(
			[]*client.MigratingDiskMeta{{Disk: &client.DiskInfoSimple{DiskID: proto.DiskID(1)}}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().Load().Return(nil)
		err := mgr.Load()
		require.NoError(t, err)
	}
}

func TestClearJunkTasksWhenLoading(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newDiskDroper(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskDrop, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		err := mgr.clearJunkTasksWhenLoading(ctx, []*proto.MigrateTask{t1})
		require.True(t, errors.Is(err, errMock))

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(
			&client.DiskInfoSimple{DiskID: t1.SourceDiskID, Status: proto.DiskStatusDropped}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		err = mgr.clearJunkTasksWhenLoading(ctx, []*proto.MigrateTask{t1})
		require.NoError(t, err)

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(
			&client.DiskInfoSimple{DiskID: t1.SourceDiskID, Status: proto.DiskStatusNormal}, nil)
		err = mgr.clearJunkTasksWhenLoading(ctx, []*proto.MigrateTask{t1})
		require.True(t, errors.Is(err, errcode.ErrUnexpectMigrationTask))
	}
}

func TestDiskDropRun(t *testing.T) {
	mgr := newDiskDroper(t)
	defer mgr.Close()

	mgr.IMigrator.(*MockMigrater).EXPECT().WaitEnable().AnyTimes().Return()
	mgr.IMigrator.(*MockMigrater).EXPECT().Enabled().AnyTimes().Return(true)
	mgr.IMigrator.(*MockMigrater).EXPECT().Run().Return()
	mgr.IMigrator.(*MockMigrater).EXPECT().GetMigratingDiskNum().AnyTimes().Return(1)
	mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).AnyTimes().Return(nil, errMock)
	mgr.cfg.CollectTaskIntervalS = 1
	mgr.cfg.CheckTaskIntervalS = 1
	require.True(t, mgr.Enabled())
	mgr.Run()

	time.Sleep(1 * time.Second)
}

func TestDiskDropCollectTask(t *testing.T) {
	{
		// reviseDropTask failed
		mgr := newDiskDroper(t)
		mgr.hasRevised = false
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		mgr.collectTask()
		require.False(t, mgr.hasRevised)
	}
	{
		// genDiskDropTasks failed
		mgr := newDiskDroper(t)
		mgr.hasRevised = false
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)

		// find in db failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(&client.DiskInfoSimple{DiskID: testDisk1.DiskID}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, errMock)
		mgr.collectTask()
		require.False(t, mgr.hasRevised)

		// find in cm failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(&client.DiskInfoSimple{DiskID: testDisk1.DiskID}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, errMock)
		mgr.collectTask()
		require.False(t, mgr.hasRevised)

		// genDiskDropTasks success
		volume := MockGenVolInfo(10005, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(&client.DiskInfoSimple{DiskID: testDisk1.DiskID}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return([]*proto.MigrateTask{{SourceVuid: units[0].Vuid}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).AnyTimes().Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.collectTask()
		require.True(t, mgr.hasRevised)
	}
	{
		mgr := newDiskDroper(t)
		mgr.hasRevised = true
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)
		// has disk drop task and do nothing
		mgr.collectTask()
	}
	{
		// acquireDropDisk
		mgr := newDiskDroper(t)
		mgr.hasRevised = true

		// list drop disk failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return(nil, errMock)
		mgr.collectTask()
		require.Equal(t, 0, mgr.droppingDisks.size())

		// no drop disk
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return(nil, nil)
		mgr.collectTask()
		require.Equal(t, 0, mgr.droppingDisks.size())
	}
	{
		mgr := newDiskDroper(t)
		mgr.hasRevised = true
		volume := MockGenVolInfo(10005, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).AnyTimes().Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return([]*client.DiskInfoSimple{testDisk1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigratingDisk(any, any).Return(errMock)

		// collect failed
		mgr.collectTask()
		require.Equal(t, 0, mgr.droppingDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).AnyTimes().Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return([]*client.DiskInfoSimple{testDisk1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigratingDisk(any, any).Return(nil)
		mgr.collectTask()
		require.Equal(t, 1, mgr.droppingDisks.size())
	}
	{
		mgr := newDiskDroper(t)
		mgr.hasRevised = true
		disk1 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z0",
			Rack:         "rack1",
			Host:         "127.0.0.1:8000",
			Status:       proto.DiskStatusNormal,
			DiskID:       1,
			FreeChunkCnt: 10,
			MaxChunkCnt:  700,
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return([]*client.DiskInfoSimple{disk1}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, errMock)
		mgr.collectTask()
		require.Equal(t, 0, mgr.droppingDisks.size())
	}
	{
		mgr := newDiskDroper(t)
		mgr.hasRevised = true
		mgr.cfg.DiskConcurrency = 2
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)

		volume := MockGenVolInfo(10006, codemode.EC6P10L2, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).AnyTimes().Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return([]*client.DiskInfoSimple{testDisk1, testDisk2}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigratingDisk(any, any).Return(nil)
		mgr.collectTask()
		require.Equal(t, 2, mgr.droppingDisks.size())
	}
}

func TestDiskDropCheckDroppedAndClear(t *testing.T) {
	{
		mgr := newDiskDroper(t)
		mgr.checkDroppedAndClear()
	}
	{
		// check dropped return false
		mgr := newDiskDroper(t)
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)

		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, errMock)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, errMock)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())

		task1 := &proto.MigrateTask{State: proto.MigrateStatePrepared, TaskType: proto.TaskTypeDiskDrop}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Times(3).Return([]*proto.MigrateTask{task1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return([]*client.VunitInfoSimple{{}, {}}, nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().IsDeletedTask(any).Return(false)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Times(2).Return(nil, nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().IsDeletedTask(any).Return(true)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())

		task2 := &proto.MigrateTask{State: proto.MigrateStateFinished, TaskType: proto.TaskTypeDiskDrop}
		task3 := &proto.MigrateTask{State: proto.MigrateStateFinishedInAdvance, TaskType: proto.TaskTypeDiskDrop}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2, task3}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return([]*client.VunitInfoSimple{{}, {}}, nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())
	}
	{
		// check dropped return true
		mgr := newDiskDroper(t)
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)
		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Times(5).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).Times(len(units)).Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Times(2).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(&client.DiskInfoSimple{DiskID: testDisk1.DiskID}, nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Times(2).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskDropped(any, any).Return(errMock)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.droppingDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().ClearDeletedTasks(any).Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskDropped(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigratingDisk(any, any, any).Return(nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 0, mgr.droppingDisks.size())
	}
}

func TestDiskDropCheckAndClearJunkTasks(t *testing.T) {
	{
		mgr := newDiskDroper(t)
		mgr.droppedDisks.add(proto.DiskID(1), time.Now())
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 1, mgr.droppedDisks.size())
		require.Equal(t, proto.DiskID(1), mgr.droppedDisks.list()[0].diskID)
	}
	{
		mgr := newDiskDroper(t)
		disk1 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z0",
			Rack:         "rack1",
			Host:         "127.0.0.1:8000",
			Status:       proto.DiskStatusNormal,
			DiskID:       proto.DiskID(1),
			FreeChunkCnt: 10,
			MaxChunkCnt:  700,
		}
		mgr.droppedDisks.add(disk1.DiskID, time.Now().Add(-junkMigrationTaskProtectionWindow))

		// get disk info failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 1, mgr.droppedDisks.size())
		require.Equal(t, disk1.DiskID, mgr.droppedDisks.list()[0].diskID)

		// disk not dropped
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Times(4).Return(disk1, nil)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 1, mgr.droppedDisks.size())
		require.Equal(t, disk1.DiskID, mgr.droppedDisks.list()[0].diskID)

		// disk is dropped and list task failed
		disk1.Status = proto.DiskStatusDropped
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, errMock)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 1, mgr.droppedDisks.size())
		require.Equal(t, disk1.DiskID, mgr.droppedDisks.list()[0].diskID)
		// no junk tasks
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 0, mgr.droppedDisks.size())

		// has junk task and clear
		mgr.droppedDisks.add(disk1.DiskID, time.Now().Add(-junkMigrationTaskProtectionWindow))
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{{TaskID: "test"}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 0, mgr.droppedDisks.size())
	}
}

func TestDiskDropAcquireTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().AcquireTask(any, any).Return(proto.MigrateTask{TaskType: proto.TaskTypeDiskDrop}, nil)
	_, err := mgr.AcquireTask(ctx, idc)
	require.NoError(t, err)
}

func TestDiskDropCancelTask(t *testing.T) {
	ctx := context.Background()
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CancelTask(any, any).Return(nil)
	err := mgr.CancelTask(ctx, &api.OperateTaskArgs{})
	require.NoError(t, err)
}

func TestDiskDropReclaimTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().ReclaimTask(any, any, any, any, any, any).Return(nil)
	t1 := mockGenMigrateTask(proto.TaskTypeDiskDrop, idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	err := mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
	require.NoError(t, err)
}

func TestDiskDropCompleteTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(nil)
	t1 := mockGenMigrateTask(proto.TaskTypeDiskDrop, idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	err := mgr.CompleteTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskID: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(errMock)
	err = mgr.CompleteTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskID: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
	require.True(t, errors.Is(err, errMock))
}

func TestDiskDropRenewalTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().RenewalTask(any, any, any).Return(nil)
	err := mgr.RenewalTask(ctx, idc, "")
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().RenewalTask(any, any, any).Return(errMock)
	err = mgr.RenewalTask(ctx, idc, "")
	require.True(t, errors.Is(err, errMock))
}

func TestDiskDropProgress(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newDiskDroper(t)
		disks, _, _ := mgr.Progress(ctx)
		require.Equal(t, 0, len(disks))
	}
	{
		mgr := newDiskDroper(t)
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, errMock)
		disks, _, _ := mgr.Progress(ctx)
		require.Equal(t, 0, len(disks))
	}
	{
		mgr := newDiskDroper(t)
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)
		task1 := &proto.MigrateTask{State: proto.MigrateStatePrepared, SourceDiskID: testDisk1.DiskID}
		task2 := &proto.MigrateTask{State: proto.MigrateStateInited, SourceDiskID: testDisk1.DiskID}
		task3 := &proto.MigrateTask{State: proto.MigrateStateWorkCompleted, SourceDiskID: testDisk1.DiskID}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2, task3}, nil)
		disks, tatal, dropped := mgr.Progress(ctx)
		require.Equal(t, 1, len(disks))
		require.Equal(t, testDisk1.DiskID, disks[0])
		require.Equal(t, testDisk1.UsedChunkCnt, int64(tatal))
		require.Equal(t, 17, dropped)
	}
	{
		mgr := newDiskDroper(t)
		mgr.droppingDisks.add(testDisk1.DiskID, testDisk1)
		mgr.droppingDisks.add(testDisk2.DiskID, testDisk2)
		task1 := &proto.MigrateTask{State: proto.MigrateStatePrepared, SourceDiskID: testDisk1.DiskID}
		task2 := &proto.MigrateTask{State: proto.MigrateStateInited, SourceDiskID: testDisk1.DiskID}
		task3 := &proto.MigrateTask{State: proto.MigrateStateWorkCompleted, SourceDiskID: testDisk1.DiskID}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2, task3}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2}, nil)
		disks, tatal, dropped := mgr.Progress(ctx)
		require.Equal(t, 2, len(disks))
		require.Equal(t, testDisk1.UsedChunkCnt+testDisk2.UsedChunkCnt, int64(tatal))
		require.Equal(t, testDisk1.UsedChunkCnt+testDisk2.UsedChunkCnt-2-3, int64(dropped))
	}
}
