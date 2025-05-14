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
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

func newDiskDroper(t *testing.T) *DiskDropMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	volumeUpdater := NewMockVolumeUpdater(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)
	taskLogger := mocks.NewMockRecordLogEncoder(ctr)
	topology := NewMockClusterTopology(ctr)
	c := closer.New()

	migrater := NewMockMigrater(ctr)
	migrater.EXPECT().StatQueueTaskCnt().AnyTimes().Return(0, 0, 0)
	migrater.EXPECT().Close().AnyTimes().DoAndReturn(c.Close)
	migrater.EXPECT().Done().AnyTimes().Return(c.Done())
	mgr := NewDiskDropMgr(clusterMgr, volumeUpdater, taskSwitch, taskLogger, &DropMgrConfig{
		TotalTaskLimit:   20,
		TaskLimitPerDisk: 20,
	}, topology)
	mgr.IMigrator = migrater
	return mgr
}

func TestDiskDropLoad(t *testing.T) {
	t1 := mockGenMigrateTask(proto.TaskTypeDiskDrop, "z0", 1, 110, proto.MigrateStateInited, MockMigrateVolInfoMap)
	task, err := t1.Task()
	require.NoError(t, err)
	// loopGenerateTask success
	volume := MockGenVolInfo(110, codemode.EC6P6, proto.VolumeStatusIdle)
	{
		mgr := newDiskDroper(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return(nil, errMock)
		err := mgr.Load()
		require.Equal(t, 0, mgr.allDisks.size())
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskDroper(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Times(1).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Times(1).Return([]*proto.Task{task}, nil)
		vuid := volume.VunitLocations[0].Vuid
		epoch := vuid.Epoch()
		volume.VunitLocations[0].Vuid = proto.EncodeVuid(vuid.VuidPrefix(), epoch+1)
		mgr.topologyMgr.(*MockClusterTopology).EXPECT().GetVolume(any).Times(1).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, t1.TaskID).Times(1).Return(nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().Load().Return(nil)
		err := mgr.Load()
		require.NoError(t, err)
		volume.VunitLocations[0].Vuid = vuid
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
	mgr.IMigrator.(*MockMigrater).EXPECT().Run().AnyTimes().Return()
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
		mgr.prepareTaskPool = taskpool.New(1, 1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return(nil, errMock)
		mgr.collectTask()
		require.Equal(t, 0, mgr.collectedDisks.size())
	}
	{
		mgr := newDiskDroper(t)
		mgr.prepareTaskPool = taskpool.New(1, 1)
		// loopGenerateTask success
		volume := MockGenVolInfo(10005, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		ctr := gomock.NewController(t)
		cli := NewMockClusterMgrAPI(ctr)
		cli.EXPECT().ListDropDisks(gomock.Any()).Return([]*client.DiskInfoSimple{testDisk1}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(gomock.Any(), testDisk1.DiskID).Return([]*proto.MigrateTask{{SourceVuid: units[0].Vuid}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(gomock.Any(), gomock.Any()).AnyTimes().Return()
		mgr.IMigrator.(*MockMigrater).EXPECT().WaitEnable().AnyTimes().Return()
		cli.EXPECT().AddMigratingDisk(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		cli.EXPECT().ListDiskVolumeUnits(any, testDisk1.DiskID).Return(units, nil)
		mgr.topologyMgr.(*MockClusterTopology).EXPECT().GetVolume(any).AnyTimes().Return(volume, nil)
		mgr.clusterMgrCli = cli
		mgr.collectTask()
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, 1, mgr.collectedDisks.size())
	}
}

func TestDiskDropCheckDroppedAndClear(t *testing.T) {
	{
		// check dropped return false
		mgr := newDiskDroper(t)
		mgr.collectedDisks.add(&dropDisk{DiskInfoSimple: testDisk1})

		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, errMock)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.collectedDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, errMock)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.collectedDisks.size())

		task1 := &proto.MigrateTask{State: proto.MigrateStatePrepared, TaskType: proto.TaskTypeDiskDrop}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Times(3).Return([]*proto.MigrateTask{task1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return([]*client.VunitInfoSimple{{}, {}}, nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.collectedDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().IsDeletedTask(any).Return(false)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Times(2).Return(nil, nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.collectedDisks.size())

		mgr.IMigrator.(*MockMigrater).EXPECT().IsDeletedTask(any).Return(true)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.collectedDisks.size())

		task2 := &proto.MigrateTask{State: proto.MigrateStateFinished, TaskType: proto.TaskTypeDiskDrop}
		task3 := &proto.MigrateTask{State: proto.MigrateStateFinishedInAdvance, TaskType: proto.TaskTypeDiskDrop}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2, task3}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return([]*client.VunitInfoSimple{{}, {}}, nil)
		mgr.checkDroppedAndClear()
		require.Equal(t, 1, mgr.collectedDisks.size())
	}
	{
		// check dropped return true
		mgr := newDiskDroper(t)
		mgr.collectedDisks.add(&dropDisk{DiskInfoSimple: testDisk1})
		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.IMigrator.(*MockMigrater).EXPECT().ListAllTaskByDiskID(any, any).AnyTimes().Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.allDisks.add(&dropDisk{wait: make(chan struct{}, 1), DiskInfoSimple: testDisk1})
		mgr.checkDroppedAndClear()
		require.Equal(t, 0, mgr.collectedDisks.size())

		mgr.collectedDisks.add(&dropDisk{DiskInfoSimple: testDisk1})
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).AnyTimes().Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Times(1).Return(units, nil)
		mgr.allDisks.add(&dropDisk{wait: make(chan struct{}, 1), DiskInfoSimple: testDisk1})
		mgr.checkDroppedAndClear()
		require.Equal(t, 0, mgr.collectedDisks.size())

		mgr.collectedDisks.add(&dropDisk{DiskInfoSimple: testDisk1})
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).AnyTimes().Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigratingDisk(any, any, any).Times(1).Return(nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ClearDeletedTasks(any).Times(1).Return()
		mgr.allDisks.add(&dropDisk{wait: make(chan struct{}, 1), DiskInfoSimple: testDisk1})
		dropped := mgr.checkDiskDropped(context.Background(), testDisk1.DiskID)
		require.True(t, dropped)
		mgr.clearTasksByDiskID(context.Background(), testDisk1.DiskID)
		require.Equal(t, 0, mgr.collectedDisks.size())
		require.Equal(t, 1, mgr.droppedDisks.size())
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
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.Task{{TaskID: "test"}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.checkAndClearJunkTasks()
		require.Equal(t, 0, mgr.droppedDisks.size())
	}
}

func TestDiskDropAcquireTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().AcquireTask(any, any).Return(&proto.Task{TaskType: proto.TaskTypeDiskDrop}, nil)
	_, err := mgr.AcquireTask(ctx, idc)
	require.NoError(t, err)
}

func TestDiskDropCancelTask(t *testing.T) {
	ctx := context.Background()
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CancelTask(any, any).Return(nil)
	err := mgr.CancelTask(ctx, &api.TaskArgs{})
	require.NoError(t, err)
}

func TestDiskDropReclaimTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().ReclaimTask(any, any).Return(nil)
	t1 := mockGenMigrateTask(proto.TaskTypeDiskDrop, idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)

	taskArgs := generateTaskArgs(t1, "")
	err := mgr.ReclaimTask(ctx, taskArgs)
	require.NoError(t, err)
}

func TestDiskDropCompleteTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(nil)
	t1 := mockGenMigrateTask(proto.TaskTypeDiskDrop, idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	taskArgs := generateTaskArgs(t1, "")
	err := mgr.CompleteTask(ctx, taskArgs)
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(errMock)

	err = mgr.CompleteTask(ctx, taskArgs)
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
		dDisk := dropDisk{DiskInfoSimple: testDisk1}
		mgr.allDisks.add(&dDisk)
		disks, tatal, dropped := mgr.Progress(ctx)
		require.Equal(t, 0, len(disks))
		require.Equal(t, 0, tatal)
		require.Equal(t, 0, dropped)
	}
	{
		mgr := newDiskDroper(t)
		dDisk := dropDisk{DiskInfoSimple: testDisk1}
		dDisk.setCollecting(true)
		mgr.allDisks.add(&dDisk)
		disks, tatal, dropped := mgr.Progress(ctx)
		require.Equal(t, 1, len(disks))
		require.Equal(t, testDisk1.DiskID, disks[0])
		require.Equal(t, testDisk1.UsedChunkCnt, int64(tatal))
		require.Equal(t, testDisk1.UsedChunkCnt-dDisk.undoneTaskCnt, int64(dropped))
	}
	{
		mgr := newDiskDroper(t)
		dDisk := dropDisk{
			DiskInfoSimple: testDisk1,
			undoneTaskCnt:  3,
		}
		dDisk.setCollecting(true)
		mgr.allDisks.add(&dDisk)
		disks, tatal, dropped := mgr.Progress(ctx)
		require.Equal(t, 1, len(disks))
		require.Equal(t, testDisk1.DiskID, disks[0])
		require.Equal(t, testDisk1.UsedChunkCnt, int64(tatal))
		require.Equal(t, testDisk1.UsedChunkCnt-dDisk.undoneTaskCnt, int64(dropped))
	}
	{
		mgr := newDiskDroper(t)
		dDisk1 := dropDisk{DiskInfoSimple: testDisk1, undoneTaskCnt: 3}
		dDisk2 := dropDisk{DiskInfoSimple: testDisk2, undoneTaskCnt: 2}
		dDisk1.setCollecting(true)
		dDisk2.setCollecting(true)
		mgr.allDisks.add(&dDisk1)
		mgr.allDisks.add(&dDisk2)
		disks, tatal, dropped := mgr.Progress(ctx)
		require.Equal(t, 2, len(disks))
		require.Equal(t, testDisk1.UsedChunkCnt+testDisk2.UsedChunkCnt, int64(tatal))
		require.Equal(t, testDisk1.UsedChunkCnt+testDisk2.UsedChunkCnt-2-3, int64(dropped))
	}
}

func TestDiskDropDiskProgress(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newDiskDroper(t)
		_, err := mgr.DiskProgress(ctx, testDisk1.DiskID)
		require.Error(t, err)
	}
	{
		mgr := newDiskDroper(t)
		mgr.allDisks.add(&dropDisk{DiskInfoSimple: testDisk1})
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, errMock)
		_, err := mgr.DiskProgress(ctx, testDisk1.DiskID)
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskDroper(t)
		mgr.allDisks.add(&dropDisk{DiskInfoSimple: testDisk1, undoneTaskCnt: 3})

		task1, _ := (&proto.MigrateTask{State: proto.MigrateStatePrepared, SourceDiskID: testDisk1.DiskID}).Task()
		task2, _ := (&proto.MigrateTask{State: proto.MigrateStateInited, SourceDiskID: testDisk1.DiskID}).Task()
		task3, _ := (&proto.MigrateTask{State: proto.MigrateStateWorkCompleted, SourceDiskID: testDisk1.DiskID}).Task()

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.Task{task1, task2, task3}, nil)
		stats, err := mgr.DiskProgress(ctx, testDisk1.DiskID)
		require.NoError(t, err)
		require.Equal(t, int(testDisk1.UsedChunkCnt), stats.TotalTasksCnt)
		require.Equal(t, int(testDisk1.UsedChunkCnt-3), stats.MigratedTasksCnt)
	}
}
