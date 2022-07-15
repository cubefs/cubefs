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
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

var MockMigrateVolInfoMap = map[proto.Vid]*client.VolumeInfoSimple{
	100: MockGenVolInfo(100, codemode.EC6P6, proto.VolumeStatusIdle),
	101: MockGenVolInfo(101, codemode.EC6P10L2, proto.VolumeStatusIdle),
	102: MockGenVolInfo(102, codemode.EC6P10L2, proto.VolumeStatusActive),
	103: MockGenVolInfo(103, codemode.EC6P6, proto.VolumeStatusLock),
	104: MockGenVolInfo(104, codemode.EC6P6, proto.VolumeStatusLock),
	105: MockGenVolInfo(105, codemode.EC6P6, proto.VolumeStatusActive),

	300: MockGenVolInfo(300, codemode.EC6P6, proto.VolumeStatusIdle),
	301: MockGenVolInfo(301, codemode.EC6P10L2, proto.VolumeStatusIdle),
	302: MockGenVolInfo(302, codemode.EC6P10L2, proto.VolumeStatusActive),

	400: MockGenVolInfo(400, codemode.EC6P6, proto.VolumeStatusIdle),
	401: MockGenVolInfo(401, codemode.EC6P10L2, proto.VolumeStatusIdle),
	402: MockGenVolInfo(402, codemode.EC6P10L2, proto.VolumeStatusActive),
}

func newMigrateMgr(t *testing.T) *MigrateMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)

	migrateTbl := NewMockMigrateTaskTable(ctr)
	volumeUpdater := NewMockVolumeUpdater(ctr)
	conf := &MigrateConfig{
		TaskCommonConfig: base.TaskCommonConfig{
			PrepareQueueRetryDelayS: 0,
			FinishQueueRetryDelayS:  0,
			CancelPunishDurationS:   0,
			WorkQueueSize:           3,
		},
	}
	mgr := NewMigrateMgr(clusterMgr, volumeUpdater, taskSwitch, migrateTbl, conf, proto.TaskTypeBalance, 0)
	mgr.SetLockFailHandleFunc(mgr.FinishTaskInAdvanceWhenLockFail)
	return mgr
}

func TestMigrateMigrateLoad(t *testing.T) {
	mgr := newMigrateMgr(t)

	{
		// load failed
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().FindAll(any).Return(nil, errMock)
		err := mgr.Load()
		require.True(t, errors.Is(err, errMock))
	}
	{
		t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStateInited, MockMigrateVolInfoMap)
		t2 := mockGenMigrateTask("z0", 5, 101, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		t3 := mockGenMigrateTask("z1", 6, 102, proto.MigrateStateWorkCompleted, MockMigrateVolInfoMap)
		t4 := mockGenMigrateTask("z2", 7, 103, proto.MigrateStateFinishedInAdvance, MockMigrateVolInfoMap)
		t5 := mockGenMigrateTask("z2", 8, 104, proto.MigrateStateFinished, MockMigrateVolInfoMap)
		t6 := mockGenMigrateTask("z0", 4, 105, proto.MigrateStateInited, MockMigrateVolInfoMap)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().FindAll(any).Return([]*proto.MigrateTask{t1, t2, t3, t4, t5, t6}, nil)
		err := mgr.Load()
		require.NoError(t, err)
	}
	{
		t2 := mockGenMigrateTask("z0", 5, 101, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		t3 := mockGenMigrateTask("z1", 6, 101, proto.MigrateStateWorkCompleted, MockMigrateVolInfoMap)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().FindAll(any).Return([]*proto.MigrateTask{t2, t3}, nil)
		// panic
		require.Panics(t, func() {
			mgr.Load()
		})
		t4 := mockGenMigrateTask("z2", 7, 103, 100, MockMigrateVolInfoMap)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().FindAll(any).Return([]*proto.MigrateTask{t4}, nil)
		require.Panics(t, func() {
			mgr.Load()
		})
	}
}

func TestPrepareMigrateTask(t *testing.T) {
	ctx := context.Background()
	{
		// no task
		mgr := newMigrateMgr(t)
		err := mgr.prepareTask()
		require.True(t, errors.Is(err, base.ErrNoTaskInQueue))
	}
	{
		// one task and finish in advance
		mgr := newMigrateMgr(t)
		t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStateInited, MockMigrateVolInfoMap)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Insert(any, any).Return(nil)
		mgr.AddTask(ctx, t1)

		// lock failed and send task to queue
		err := base.VolTaskLockerInst().TryLock(ctx, 100)
		require.NoError(t, err)
		err = mgr.prepareTask()
		require.True(t, errors.Is(err, base.ErrVolNotOnlyOneTask))
		base.VolTaskLockerInst().Unlock(ctx, 100)

		// get volume info failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)
		err = mgr.prepareTask()
		require.True(t, errors.Is(err, errMock))

		// finish task in advance because source chunk has moved
		// unlock failed
		volume := MockMigrateVolInfoMap[100]
		volume.VunitLocations[int(t1.SourceVuid.Index())].Vuid = volume.VunitLocations[int(t1.SourceVuid.Index())].Vuid + 1
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UnlockVolume(any, any).Return(errMock)
		err = mgr.prepareTask()
		require.True(t, errors.Is(err, errMock))
		// unlock success
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UnlockVolume(any, any).Return(nil)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
		err = mgr.prepareTask()
		require.NoError(t, err)
	}
	{
		// one task and finish in advance because  other migrate task is doing on this volume
		mgr := newMigrateMgr(t)
		t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStateInited, MockMigrateVolInfoMap)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Insert(any, any).Return(nil)
		mgr.AddTask(ctx, t1)

		// lock cm volume failed
		volume := MockMigrateVolInfoMap[100]
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().LockVolume(any, any).Return(errMock)
		err := mgr.prepareTask()
		require.True(t, errors.Is(err, errMock))

		// lock failed and call lockFailHandleFunc
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().LockVolume(any, any).Return(errcode.ErrLockNotAllow)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
		err = mgr.prepareTask()
		require.NoError(t, err)
	}
	{
		// one task and normal finish
		mgr := newMigrateMgr(t)
		t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStateInited, MockMigrateVolInfoMap)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Insert(any, any).Return(nil)
		mgr.AddTask(ctx, t1)

		// lock cm volume failed
		volume := MockMigrateVolInfoMap[100]
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().LockVolume(any, any).Return(nil)

		// alloc volume failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).Return(nil, errMock)
		err := mgr.prepareTask()
		require.True(t, errors.Is(err, errMock))

		// alloc success
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().LockVolume(any, any).Return(nil)
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).DoAndReturn(
			func(ctx context.Context, vuid proto.Vuid) (*client.AllocVunitInfo, error) {
				vid := vuid.Vid()
				idx := vuid.Index()
				epoch := vuid.Epoch()
				epoch++
				newVuid, _ := proto.NewVuid(vid, idx, epoch)
				return &client.AllocVunitInfo{
					VunitLocation: proto.VunitLocation{Vuid: newVuid},
				}, nil
			})
		err = mgr.prepareTask()
		require.NoError(t, err)
	}
}

func TestFinishMigrateTask(t *testing.T) {
	{
		// no task
		mgr := newMigrateMgr(t)
		err := mgr.finishTask()
		require.True(t, errors.Is(err, base.ErrNoTaskInQueue))
	}
	{
		// panic :status not eql proto.MigrateStateWorkCompleted
		mgr := newMigrateMgr(t)
		t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		require.Panics(t, func() {
			mgr.finishTask()
		})
	}
	{
		{
			// one task and redo success finally
			mgr := newMigrateMgr(t)
			t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStateWorkCompleted, MockMigrateVolInfoMap)
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			mgr.finishQueue.PushTask(t1.TaskID, t1)

			// update relationship failed
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errMock)
			err := mgr.finishTask()
			require.True(t, errors.Is(err, errMock))

			// update relationship failed and need redo
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errcode.ErrNewVuidNotMatch)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).Return(nil, errMock)
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			// alloc failed
			err = mgr.finishTask()
			require.True(t, errors.Is(err, errMock))

			// panic
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errcode.ErrOldVuidNotMatch)
			require.Panics(t, func() {
				mgr.finishTask()
			})

			// redo success
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errcode.ErrNewVuidNotMatch)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).DoAndReturn(
				func(ctx context.Context, vuid proto.Vuid) (*client.AllocVunitInfo, error) {
					vid := vuid.Vid()
					idx := vuid.Index()
					epoch := vuid.Epoch()
					epoch++
					newVuid, _ := proto.NewVuid(vid, idx, epoch)
					return &client.AllocVunitInfo{
						VunitLocation: proto.VunitLocation{Vuid: newVuid},
					}, nil
				})
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			err = mgr.finishTask()
			require.NoError(t, err)
		}
		{
			// one task and success normal
			mgr := newMigrateMgr(t)
			t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStateWorkCompleted, MockMigrateVolInfoMap)
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			mgr.finishQueue.PushTask(t1.TaskID, t1)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(nil)
			// release failed and update volume cache failed
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ReleaseVolumeUnit(any, any, any).Return(errMock)
			mgr.volumeUpdater.(*MockVolumeUpdater).EXPECT().UpdateLeaderVolumeCache(any, any).Return(errMock)
			err := mgr.finishTask()
			require.True(t, errors.Is(err, base.ErrUpdateVolumeCache))

			// release failed and update volume cache success
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(nil)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ReleaseVolumeUnit(any, any, any).Return(errMock)
			mgr.volumeUpdater.(*MockVolumeUpdater).EXPECT().UpdateLeaderVolumeCache(any, any).Return(nil)
			// unlock volume failed
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UnlockVolume(any, any).Return(errMock)
			err = mgr.finishTask()
			require.True(t, errors.Is(err, errMock))

			// update volume success
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(nil)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ReleaseVolumeUnit(any, any, any).Return(nil)
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UnlockVolume(any, any).Return(nil)
			err = mgr.finishTask()
			require.NoError(t, err)
		}
	}
}

func TestAcquireMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		// task switch is close
		mgr := newMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(false)
		_, err := mgr.AcquireTask(ctx, idc)
		require.True(t, errors.Is(err, proto.ErrTaskPaused))
	}
	{
		// no task in queue
		mgr := newMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		_, err := mgr.AcquireTask(ctx, idc)
		require.True(t, errors.Is(err, proto.ErrTaskEmpty))
	}
	{
		// one task in queue
		mgr := newMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		task, err := mgr.AcquireTask(ctx, idc)
		require.NoError(t, err)
		require.Equal(t, t1.TaskID, task.TaskID)
	}
}

func TestCancelMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		mgr := newMigrateMgr(t)
		err := mgr.CancelTask(ctx, &api.CancelTaskArgs{IDC: idc})
		require.Error(t, err)
	}
	{
		mgr := newMigrateMgr(t)
		t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)

		// no such task
		err := mgr.CancelTask(ctx, &api.CancelTaskArgs{IDC: idc})
		require.Error(t, err)

		err = mgr.CancelTask(ctx, &api.CancelTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
		require.NoError(t, err)
	}
}

func TestReclaimMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		// no task
		mgr := newMigrateMgr(t)
		err := mgr.ReclaimTask(ctx, idc, "", nil, proto.VunitLocation{}, &client.AllocVunitInfo{})
		require.Error(t, err)
	}
	{
		mgr := newMigrateMgr(t)
		t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)

		// update failed
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(errMock)
		err := mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
		require.True(t, errors.Is(err, errMock))

		// update success
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
		err = mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
		require.NoError(t, err)
	}
}

func TestCompleteMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		// no task
		mgr := newMigrateMgr(t)
		err := mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc})
		require.Error(t, err)
	}
	{
		mgr := newMigrateMgr(t)
		t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)

		// update failed
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(errMock)
		err := mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
		require.NoError(t, err)

		// no task in queue
		err = mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
		require.Error(t, err)

		// update success
		mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Update(any, any, any).Return(nil)
		t2 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t2.TaskID, t2)
		err = mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t2.TaskID, Src: t2.Sources, Dest: t2.Destination})
		require.NoError(t, err)
	}
}

func TestRenewalMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		// task switch is close
		mgr := newMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(false)
		err := mgr.RenewalTask(ctx, idc, "")
		require.True(t, errors.Is(err, proto.ErrTaskPaused))
	}
	{
		// no task
		mgr := newMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		err := mgr.RenewalTask(ctx, idc, "")
		require.Error(t, err)
	}
	{
		mgr := newMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		err := mgr.RenewalTask(ctx, idc, t1.TaskID)
		require.NoError(t, err)
	}
}

func TestAddMigrateTask(t *testing.T) {
	ctx := context.Background()
	mgr := newMigrateMgr(t)
	t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStateInited, MockMigrateVolInfoMap)
	mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Insert(any, any).Return(nil)
	mgr.AddTask(ctx, t1)
	require.True(t, mgr.IsMigratingDisk(proto.DiskID(4)))
	require.False(t, mgr.IsMigratingDisk(proto.DiskID(5)))
	require.Equal(t, 1, mgr.GetMigratingDiskNum())

	inited, prepared, completed := mgr.StatQueueTaskCnt()
	require.Equal(t, 1, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 0, completed)

	mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().FindAll(any).Return([]*proto.MigrateTask{t1}, nil)
	tasks, err := mgr.FindAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Find(any, any).Return(t1, nil)
	task, err := mgr.FindTask(ctx, t1.TaskID)
	require.NoError(t, err)
	require.Equal(t, t1.TaskID, task.TaskID)

	mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().FindByDiskID(any, any).Return([]*proto.MigrateTask{}, nil)
	_, err = mgr.FindByDiskID(ctx, proto.DiskID(1))
	require.NoError(t, err)
}

func TestClearMigrateTasks(t *testing.T) {
	ctx := context.Background()
	mgr := newMigrateMgr(t)

	mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().MarkDeleteByDiskID(any, any).Return(nil)
	mgr.ClearTasksByDiskID(ctx, proto.DiskID(1))

	mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().MarkDeleteByStates(any, any).Return(nil)
	mgr.ClearTasksByStates(ctx, []proto.MigrateState{proto.MigrateStateWorkCompleted})
}

func TestMigrateRun(t *testing.T) {
	mgr := newMigrateMgr(t)
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().WaitEnable().AnyTimes().Return()
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().AnyTimes().Return(true)
	mgr.Run()

	// wait to run
	time.Sleep(2 * time.Millisecond)
}

func TestMigrateQueryTask(t *testing.T) {
	ctx := context.Background()
	taskID := "task_id"
	mgr := newMigrateMgr(t)

	mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Find(any, any).Return(nil, errMock)
	_, err := mgr.QueryTask(ctx, taskID)
	require.ErrorIs(t, errMock, err)

	mgr.taskTbl.(*MockMigrateTaskTable).EXPECT().Find(any, any).Return(&proto.MigrateTask{TaskType: proto.TaskTypeDiskDrop}, nil)
	_, err = mgr.QueryTask(ctx, taskID)
	require.NoError(t, err)
}

func TestMigrateReportWorkerTaskStats(t *testing.T) {
	mgr := newMigrateMgr(t)
	mgr.ReportWorkerTaskStats(&api.TaskReportArgs{
		TaskId:               "task_id",
		IncreaseDataSizeByte: 1,
		IncreaseShardCnt:     1,
	})
}

func TestMigrateStatQueueTaskCnt(t *testing.T) {
	mgr := newMigrateMgr(t)
	inited, prepared, completed := mgr.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 0, completed)
}

func TestMigrateStats(t *testing.T) {
	mgr := newMigrateMgr(t)
	mgr.Stats()
}

func TestMigrateAction(t *testing.T) {
	mgr := newMigrateMgr(t)
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().WaitEnable().Return()
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)

	mgr.WaitEnable()
	require.True(t, mgr.Enabled())

	select {
	case <-mgr.Done():
		require.Fail(t, "cannot be there")
	default:
	}

	mgr.Close()

	select {
	case <-mgr.Done():
	default:
		require.Fail(t, "cannot be there")
	}

	mgr.Close()
}
