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
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

func newDiskDroper(t *testing.T) *DiskDropMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	volumeUpdater := NewMockVolumeUpdater(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)
	migrateTable := NewMockMigrateTaskTable(ctr)
	c := closer.New()

	migrater := NewMockMigrater(ctr)
	migrater.EXPECT().StatQueueTaskCnt().AnyTimes().Return(0, 0, 0)
	migrater.EXPECT().Close().AnyTimes().DoAndReturn(c.Close)
	migrater.EXPECT().Done().AnyTimes().Return(c.Done())
	mgr := NewDiskDropMgr(clusterMgr, volumeUpdater, taskSwitch, migrateTable, &MigrateConfig{})
	mgr.IMigrator = migrater
	return mgr
}

func TestDiskDropLoad(t *testing.T) {
	{
		mgr := newDiskDroper(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindAll(any).Return(nil, errMock)
		err := mgr.Load()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskDroper(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindAll(any).Return(nil, nil)
		err := mgr.Load()
		require.NoError(t, err)
	}
	{
		mgr := newDiskDroper(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindAll(any).Return([]*proto.MigrateTask{{SourceDiskID: proto.DiskID(1)}, {SourceDiskID: proto.DiskID(2)}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, errMock)
		err := mgr.Load()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskDroper(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindAll(any).Return([]*proto.MigrateTask{{SourceDiskID: proto.DiskID(1)}, {SourceDiskID: proto.DiskID(2)}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{{SourceDiskID: proto.DiskID(1)}}, nil)
		require.Panics(t, func() {
			mgr.Load()
		})
	}
	{
		mgr := newDiskDroper(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindAll(any).Return([]*proto.MigrateTask{{SourceDiskID: proto.DiskID(1)}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{{SourceDiskID: proto.DiskID(1)}}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().Load().Return(nil)
		err := mgr.Load()
		require.NoError(t, err)
	}
}

func TestDiskDropRun(t *testing.T) {
	mgr := newDiskDroper(t)
	defer mgr.Close()

	mgr.IMigrator.(*MockMigrater).EXPECT().WaitEnable().AnyTimes().Return()
	mgr.IMigrator.(*MockMigrater).EXPECT().Enabled().AnyTimes().Return(true)
	mgr.IMigrator.(*MockMigrater).EXPECT().Run().Return()
	mgr.IMigrator.(*MockMigrater).EXPECT().ClearTasksByStates(any, any).AnyTimes().Return()
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
		mgr.droppingDiskID = proto.DiskID(1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		mgr.collectTask()
	}
	{
		// genDiskDropTasks failed
		mgr := newDiskDroper(t)
		mgr.hasRevised = false
		mgr.droppingDiskID = proto.DiskID(1)

		// find in db failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(&client.DiskInfoSimple{DiskID: mgr.droppingDiskID}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, errMock)
		mgr.collectTask()

		// find in cm failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(&client.DiskInfoSimple{DiskID: mgr.droppingDiskID}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, errMock)
		mgr.collectTask()

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
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(&client.DiskInfoSimple{DiskID: mgr.droppingDiskID}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).AnyTimes().Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.collectTask()
	}
	{
		mgr := newDiskDroper(t)
		mgr.hasRevised = true
		mgr.droppingDiskID = proto.DiskID(1)
		mgr.collectTask()
	}
	{
		// acquireDropDisk
		mgr := newDiskDroper(t)
		mgr.hasRevised = true

		// list drop disk failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return(nil, errMock)
		mgr.collectTask()

		// no drop disk
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return(nil, nil)
		mgr.collectTask()
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
		volume := MockGenVolInfo(10005, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).AnyTimes().Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDropDisks(any).Return([]*client.DiskInfoSimple{disk1}, nil)
		mgr.collectTask()
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
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, errMock)
		mgr.collectTask()
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
		mgr.droppingDiskID = proto.DiskID(1)

		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, errMock)
		mgr.checkDroppedAndClear()
		require.True(t, mgr.hasDroppingDisk())

		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, errMock)
		mgr.checkDroppedAndClear()
		require.True(t, mgr.hasDroppingDisk())

		task1 := &proto.MigrateTask{State: proto.MigrateStatePrepared, TaskType: proto.TaskTypeDiskDrop}
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{task1}, nil)
		mgr.checkDroppedAndClear()
		require.True(t, mgr.hasDroppingDisk())

		task2 := &proto.MigrateTask{State: proto.MigrateStateFinished, TaskType: proto.TaskTypeDiskDrop}
		task3 := &proto.MigrateTask{State: proto.MigrateStateFinishedInAdvance, TaskType: proto.TaskTypeDiskDrop}
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2, task3}, nil)
		mgr.checkDroppedAndClear()
		require.True(t, mgr.hasDroppingDisk())
	}
	{
		// check dropped return true
		mgr := newDiskDroper(t)
		mgr.droppingDiskID = proto.DiskID(1)
		task1 := &proto.MigrateTask{State: proto.MigrateStateFinished, TaskType: proto.TaskTypeDiskDrop}
		task2 := &proto.MigrateTask{State: proto.MigrateStateFinishedInAdvance, TaskType: proto.TaskTypeDiskDrop}
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskDropped(any, any).Return(errMock)
		mgr.checkDroppedAndClear()
		require.True(t, mgr.hasDroppingDisk())

		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		mgr.checkDroppedAndClear()
		require.True(t, mgr.hasDroppingDisk())

		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ClearTasksByDiskID(any, any).Return()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskDropped(any, any).Return(nil)
		mgr.checkDroppedAndClear()
		require.False(t, mgr.hasDroppingDisk())
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
	err := mgr.CancelTask(ctx, &api.CancelTaskArgs{})
	require.NoError(t, err)
}

func TestDiskDropReclaimTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().ReclaimTask(any, any, any, any, any, any).Return(nil)
	t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	err := mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
	require.NoError(t, err)
}

func TestDiskDropCompleteTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newDiskDroper(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(nil)
	t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	err := mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(errMock)
	err = mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
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
		diskID, _, _ := mgr.Progress(ctx)
		require.Equal(t, base.EmptyDiskID, diskID)
	}
	{
		mgr := newDiskDroper(t)
		mgr.droppingDiskID = proto.DiskID(1)
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return(nil, errMock)
		diskID, _, _ := mgr.Progress(ctx)
		require.Equal(t, proto.DiskID(1), diskID)
	}
	{
		mgr := newDiskDroper(t)
		diskID := proto.DiskID(1)
		mgr.droppingDiskID = diskID
		task1 := &proto.MigrateTask{TaskType: proto.TaskTypeDiskDrop, State: proto.MigrateStatePrepared, SourceDiskID: diskID}
		task2 := &proto.MigrateTask{TaskType: proto.TaskTypeDiskDrop, State: proto.MigrateStateFinished, SourceDiskID: diskID}
		task3 := &proto.MigrateTask{TaskType: proto.TaskTypeDiskDrop, State: proto.MigrateStateFinishedInAdvance, SourceDiskID: diskID}
		mgr.IMigrator.(*MockMigrater).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{task1, task2, task3}, nil)
		doingDisk, tatal, dropped := mgr.Progress(ctx)
		require.Equal(t, diskID, doingDisk)
		require.Equal(t, 3, tatal)
		require.Equal(t, 2, dropped)
	}
}
