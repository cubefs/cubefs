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
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

// newShardMigrateMgrWithCallback creates a manager with loadTaskCallback set.
func newShardMigrateMgrWithCallback(t *testing.T, cb loadTaskCallback) *ShardMigrateMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)

	conf := &ShardMigrateConfig{
		ClusterID: 0,
		TaskCommonConfig: base.TaskCommonConfig{
			PrepareQueueRetryDelayS: 0,
			FinishQueueRetryDelayS:  0,
			CancelPunishDurationS:   0,
			WorkQueueSize:           3,
		},
		loadTaskCallback: cb,
	}

	mgr := NewShardMigrateMgr(clusterMgr, taskSwitch, conf, proto.TaskTypeShardDiskRepair)
	m, ok := mgr.(*ShardMigrateMgr)
	require.True(t, ok)
	return m
}

var MockMigrateShardInfoMap = map[proto.ShardID]*client.ShardInfoSimple{
	100: MockGenShardInfo(100, 0),
	101: MockGenShardInfo(101, 0),
	102: MockGenShardInfo(102, 0),
	103: MockGenShardInfo(103, 0),
	104: MockGenShardInfo(104, 0),
}

func newShardMigrateMgr(t *testing.T) *ShardMigrateMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)

	conf := &ShardMigrateConfig{
		ClusterID: 0,
		TaskCommonConfig: base.TaskCommonConfig{
			PrepareQueueRetryDelayS: 0,
			FinishQueueRetryDelayS:  0,
			CancelPunishDurationS:   0,
			WorkQueueSize:           3,
		},
	}

	mgr := NewShardMigrateMgr(clusterMgr, taskSwitch, conf, proto.TaskTypeShardDiskRepair)

	shardMigrateMgr, ok := mgr.(*ShardMigrateMgr)
	require.True(t, ok)

	return shardMigrateMgr
}

func TestShardMigrateLoad(t *testing.T) {
	mgr := newShardMigrateMgr(t)

	{
		// load success
		t1, _ := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateInited, MockMigrateShardInfoMap).ToTask()
		t2, _ := mockGenShardMigrateTask(101, proto.TaskTypeShardDiskRepair, "z1", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap).ToTask()
		t5, _ := mockGenShardMigrateTask(104, proto.TaskTypeShardDiskRepair, "z1", 4, proto.ShardTaskStateWorkCompleted, MockMigrateShardInfoMap).ToTask()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return([]*proto.Task{t1, t2, t5}, nil)
		err := mgr.Load()
		require.NoError(t, err)

		// task should not be in db
		t3, _ := mockGenShardMigrateTask(102, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateFinished, MockMigrateShardInfoMap).ToTask()
		t4, _ := mockGenShardMigrateTask(103, proto.TaskTypeShardDiskRepair, "z1", 4, proto.ShardTaskStateFinishedInAdvance, MockMigrateShardInfoMap).ToTask()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return([]*proto.Task{t3, t4}, nil)
		err = mgr.Load()
		require.Error(t, err)

		// task state wrong
		t6, _ := mockGenShardMigrateTask(103, proto.TaskTypeShardDiskRepair, "z1", 4, 7, MockMigrateShardInfoMap).ToTask()
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return([]*proto.Task{t6}, nil)
		err = mgr.Load()
		require.Error(t, err)

		// list task from cm error
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return(nil, errMock)
		err = mgr.Load()
		require.True(t, errors.Is(err, errMock))

		// list task with nil
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return([]*proto.Task{}, nil)
		err = mgr.Load()
		require.NoError(t, err)
	}
}

func TestPrepareShardMigrateTask(t *testing.T) {
	ctx := context.Background()
	{
		// no task
		mgr := newShardMigrateMgr(t)
		err := mgr.prepareTask()
		require.True(t, errors.Is(err, base.ErrNoTaskInQueue))
	}
	{
		// one task and finish in advance
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateInited, MockMigrateShardInfoMap)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigrateTask(any, any).Return(nil)
		mgr.AddTask(ctx, t1)

		// lock failed and send task to queue
		err := base.ShardTaskLockerInst().TryLock(ctx, 100)
		require.NoError(t, err)
		err = mgr.prepareTask()
		require.True(t, errors.Is(err, base.ErrShardNotOnlyOneTask))
		base.ShardTaskLockerInst().Unlock(ctx, 100)

		// get shard info failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(nil, errMock)
		err = mgr.prepareTask()
		require.True(t, errors.Is(err, errMock))

		// finish task in advance because source shard unit has moved
		shard := MockMigrateShardInfoMap[100]
		shard.ShardUnitInfos[int(t1.Source.Suid.Index())].Suid = shard.ShardUnitInfos[int(t1.Source.Suid.Index())].Suid + 1
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(shard, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		err = mgr.prepareTask()
		require.NoError(t, err)
	}
	{
		// one task and normal finish
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateInited, MockMigrateShardInfoMap)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigrateTask(any, any).Return(nil)
		mgr.AddTask(ctx, t1)

		// alloc shard unit failed
		shard := MockMigrateShardInfoMap[100]
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(shard, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocShardUnit(any, any, any).Return(nil, errMock)
		err := mgr.prepareTask()
		require.True(t, errors.Is(err, errMock))

		// alloc success
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(shard, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocShardUnit(any, any, any).DoAndReturn(
			func(ctx context.Context, vuid proto.Suid, excludes []proto.DiskID) (*client.AllocShardUnitInfo, error) {
				shardID := vuid.ShardID()
				idx := vuid.Index()
				epoch := vuid.Epoch()
				epoch++
				newSuid := proto.EncodeSuid(shardID, idx, epoch)
				return &client.AllocShardUnitInfo{
					ShardUnitInfoSimple: proto.ShardUnitInfoSimple{
						Suid:   newSuid,
						DiskID: shard.ShardUnitInfos[idx].DiskID + 3,
						Host:   shard.ShardUnitInfos[idx].Host,
					},
				}, nil
			})
		err = mgr.prepareTask()
		require.NoError(t, err)
	}
}

func TestFinishShardMigrateTask(t *testing.T) {
	{
		// no task
		mgr := newShardMigrateMgr(t)
		err := mgr.finishTask()
		require.True(t, errors.Is(err, base.ErrNoTaskInQueue))
	}
	{
		// panic :status not eql proto.MigrateStateWorkCompleted
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateInited, MockMigrateShardInfoMap)
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		require.Panics(t, func() {
			_ = mgr.finishTask()
		})
	}

	{
		// one task and redo success finally
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateWorkCompleted, MockMigrateShardInfoMap)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.finishQueue.PushTask(t1.TaskID, t1)

		// update relationship failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(MockMigrateShardInfoMap[100], nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateShard(any, any).Return(errMock)
		err := mgr.finishTask()
		require.True(t, errors.Is(err, errMock))

		// update relationship failed and get shard info failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(nil, errMock)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateShard(any, any).Return(errMock)
		err = mgr.finishTask()
		require.True(t, errors.Is(err, errMock))

		// update relationship failed and need redo
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(MockMigrateShardInfoMap[100], nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateShard(any, any).Return(errcode.ErrGetShardFailed)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocShardUnit(any, any, any).Return(nil, errMock)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		// alloc failed
		err = mgr.finishTask()
		require.True(t, errors.Is(err, errMock))

		// panic
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(MockMigrateShardInfoMap[100], nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateShard(any, any).Return(errcode.ErrOldSuidNotMatch)
		require.Panics(t, func() {
			_ = mgr.finishTask()
		})

		// redo success
		shard := MockMigrateShardInfoMap[100]
		oldSource := t1.Destination
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(shard, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateShard(any, any).Return(errcode.ErrNewSuidNotMatch)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocShardUnit(any, any, any).DoAndReturn(
			func(ctx context.Context, vuid proto.Suid, excludes []proto.DiskID) (*client.AllocShardUnitInfo, error) {
				shardID := vuid.ShardID()
				idx := vuid.Index()
				epoch := vuid.Epoch()
				epoch++
				newSuid := proto.EncodeSuid(shardID, idx, epoch)
				return &client.AllocShardUnitInfo{
					ShardUnitInfoSimple: proto.ShardUnitInfoSimple{
						Suid:   newSuid,
						DiskID: shard.ShardUnitInfos[idx].DiskID + 3,
						Host:   shard.ShardUnitInfos[idx].Host,
					},
				}, nil
			})
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Times(2).Return(nil)
		err = mgr.finishTask()
		require.NoError(t, err)
		newTask, err := mgr.workQueue.Query(t1.SourceIDC, t1.TaskID)
		require.NoError(t, err)
		require.Equal(t, newTask.GetBadDestination().Suid, oldSource.Suid)
	}
	{
		// one task and success normal
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateWorkCompleted, MockMigrateShardInfoMap)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateShard(any, any).Return(nil)
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		err := mgr.finishTask()
		require.NoError(t, err)
	}
}

func TestAcquireShardMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		// task switch is close
		mgr := newShardMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(false)
		_, err := mgr.AcquireTask(ctx, idc)
		require.True(t, errors.Is(err, proto.ErrTaskPaused))
	}
	{
		// no task in queue
		mgr := newShardMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		_, err := mgr.AcquireTask(ctx, idc)
		require.True(t, errors.Is(err, proto.ErrTaskEmpty))
	}
	{
		// one task in queue
		mgr := newShardMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		task, err := mgr.AcquireTask(ctx, idc)
		require.NoError(t, err)
		require.Equal(t, t1.TaskID, task.TaskID)
		require.Equal(t, t1.TaskType, task.TaskType)
	}
}

func TestCancelShardMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		mgr := newShardMigrateMgr(t)

		err := mgr.CancelTask(ctx, &api.TaskArgs{})
		require.Error(t, err)
	}
	{
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)

		// no such task
		err := mgr.CancelTask(ctx, &api.TaskArgs{})
		require.Error(t, err)
		taskArgs := genShardTaskArgs(t1, "", 0)
		err = mgr.CancelTask(ctx, taskArgs)
		require.NoError(t, err)

		// leader failed but leader not update
		shard := MockMigrateShardInfoMap[100]
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(shard, nil)
		taskArgs = genShardTaskArgs(t1, errcode.ErrShardNodeNotLeader.Error(), errcode.CodeShardNodeNotLeader)
		err = mgr.CancelTask(ctx, taskArgs)
		require.NoError(t, err)

		// leader failed and leader update
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(shard, nil)
		t1.Leader.Learner = true
		taskArgs = genShardTaskArgs(t1, errcode.ErrShardNodeNotLeader.Error(), errcode.CodeShardNodeNotLeader)
		err = mgr.CancelTask(ctx, taskArgs)
		require.NoError(t, err)
	}
}

func TestReclaimShardMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		// no task
		mgr := newShardMigrateMgr(t)
		err := mgr.ReclaimTask(ctx, &api.TaskArgs{})
		require.Error(t, err)
	}
	{
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
		location := t1.Destination
		location.Suid += 1
		location.DiskID += 1
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)

		// allocate shard unit failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocShardUnit(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errMock)
		taskArgs := genShardTaskArgs(t1, "", 0)
		err := mgr.ReclaimTask(ctx, taskArgs)
		require.True(t, errors.Is(err, errMock))

		// update failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocShardUnit(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			&client.AllocShardUnitInfo{ShardUnitInfoSimple: location}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(errMock)
		taskArgs = genShardTaskArgs(t1, "", 0)
		err = mgr.ReclaimTask(ctx, taskArgs)
		require.True(t, errors.Is(err, errMock))

		// update success
		task, err := mgr.workQueue.Query(t1.SourceIDC, t1.TaskID)
		require.NoError(t, err)
		t1 = task.(*proto.ShardMigrateTask)
		taskArgs = genShardTaskArgs(t1, "", 0)
		location = t1.Source
		location.Suid += 2
		location.DiskID += 2
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocShardUnit(gomock.Any(), gomock.Any(), gomock.Any()).Return(&client.AllocShardUnitInfo{ShardUnitInfoSimple: location}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		err = mgr.ReclaimTask(ctx, taskArgs)
		require.NoError(t, err)
	}
}

func TestCompleteShardMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		// no task
		mgr := newShardMigrateMgr(t)
		err := mgr.CompleteTask(ctx, &api.TaskArgs{})
		require.Error(t, err)
	}
	{
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)

		// update failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(errMock)
		taskArgs := genShardTaskArgs(t1, "", 0)
		err := mgr.CompleteTask(ctx, taskArgs)
		require.NoError(t, err)

		// no task in queue
		err = mgr.CompleteTask(ctx, taskArgs)
		require.Error(t, err)

		// update success
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		t2 := mockGenShardMigrateTask(101, proto.TaskTypeShardDiskRepair, "z0", 5, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
		mgr.workQueue.AddPreparedTask(idc, t2.TaskID, t2)
		args := genShardTaskArgs(t2, "", 0)
		err = mgr.CompleteTask(ctx, args)
		require.NoError(t, err)
	}
}

func TestRenewalShardMigrateTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		// task switch is close
		mgr := newShardMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(false)
		err := mgr.RenewalTask(ctx, idc, "")
		require.True(t, errors.Is(err, proto.ErrTaskPaused))
	}
	{
		// no task
		mgr := newShardMigrateMgr(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		err := mgr.RenewalTask(ctx, idc, "")
		require.Error(t, err)
	}
	{
		mgr := newShardMigrateMgr(t)
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0",
			4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		err := mgr.RenewalTask(ctx, idc, t1.TaskID)
		require.NoError(t, err)
	}
}

func TestShardMigrateRun(t *testing.T) {
	mgr := newShardMigrateMgr(t)
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().WaitEnable().AnyTimes().Return()
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().AnyTimes().Return(true)
	mgr.Run()

	// wait to run
	time.Sleep(2 * time.Millisecond)
}

func TestShardMigrateMgr_QueryTask(t *testing.T) {
	mgr := newShardMigrateMgr(t)
	t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0",
		4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
	t2, err := t1.ToTask()
	require.NoError(t, err)
	{
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(t2, nil)
		task, err := mgr.GetTask(context.Background(), t1.TaskID)
		require.NoError(t, err)
		require.EqualValues(t, t1, task)
	}
	{
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(nil, errMock)
		_, err := mgr.GetTask(context.Background(), t1.TaskID)
		require.True(t, errors.Is(err, errMock))
	}
	{
		// unmarshal error: GetMigrateTask returns task with invalid data
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(&proto.Task{Data: []byte("invalid-json")}, nil)
		_, err := mgr.GetTask(context.Background(), t1.TaskID)
		require.Error(t, err)
	}
}

func TestShardMigrateMgr_SimpleOps(t *testing.T) {
	ctx := context.Background()
	mgr := newShardMigrateMgr(t)

	// ReportTask
	err := mgr.ReportTask(ctx, nil)
	require.NoError(t, err)

	// StatQueueTaskCnt / Stats with empty queues
	preparing, doing, finishing := mgr.StatQueueTaskCnt()
	require.Equal(t, 0, preparing)
	require.Equal(t, 0, doing)
	require.Equal(t, 0, finishing)
	stats := mgr.Stats()
	require.Equal(t, 0, stats.PreparingCnt)
	require.Equal(t, 0, stats.WorkerDoingCnt)
	require.Equal(t, 0, stats.FinishingCnt)

	// StatQueueTaskCnt with tasks in queues
	t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateInited, MockMigrateShardInfoMap)
	mgr.prepareQueue.PushTask(t1.TaskID, t1)
	t2 := mockGenShardMigrateTask(101, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
	mgr.workQueue.AddPreparedTask("z0", t2.TaskID, t2)
	t3 := mockGenShardMigrateTask(102, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateWorkCompleted, MockMigrateShardInfoMap)
	mgr.finishQueue.PushTask(t3.TaskID, t3)
	preparing, doing, finishing = mgr.StatQueueTaskCnt()
	require.Greater(t, preparing, 0)
	require.Greater(t, doing, 0)
	require.Greater(t, finishing, 0)

	// Enabled
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
	require.True(t, mgr.Enabled())

	// WaitEnable
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().WaitEnable().Return()
	mgr.WaitEnable()

	// Done + Close
	done := mgr.Done()
	require.NotNil(t, done)
	mgr.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close should have closed Done channel")
	}
}

func TestShardMigrateMgr_QueryTaskFull(t *testing.T) {
	ctx := context.Background()
	mgr := newShardMigrateMgr(t)
	t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
	t2, err := t1.ToTask()
	require.NoError(t, err)

	// success
	mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(t2, nil)
	ret, err := mgr.QueryTask(ctx, t1.TaskID)
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Equal(t, t1.TaskType, ret.TaskType)

	// GetTask error
	mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(nil, errMock)
	_, err = mgr.QueryTask(ctx, t1.TaskID)
	require.True(t, errors.Is(err, errMock))
}

func TestShardMigrateSuids(t *testing.T) {
	ctx := context.Background()

	// ListMigratingSuid
	{
		mgr := newShardMigrateMgr(t)

		// error
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, errMock)
		_, err := mgr.ListMigratingSuid(ctx, 1)
		require.True(t, errors.Is(err, errMock))

		// success with task
		t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
		protoTask, err := t1.ToTask()
		require.NoError(t, err)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.Task{protoTask}, nil)
		suids, err := mgr.ListMigratingSuid(ctx, 4)
		require.NoError(t, err)
		require.Len(t, suids, 1)
		require.Equal(t, t1.Source.Suid, suids[0])

		// success with empty list
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.Task{}, nil)
		suids, err = mgr.ListMigratingSuid(ctx, 4)
		require.NoError(t, err)
		require.Len(t, suids, 0)
	}

	// ListImmigratedSuid
	{
		mgr := newShardMigrateMgr(t)

		// error
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any, any).Return(nil, errMock)
		_, err := mgr.ListImmigratedSuid(ctx, 1)
		require.True(t, errors.Is(err, errMock))

		// success with shard units
		suid := proto.EncodeSuid(100, 0, 1)
		sunits := []*client.ShardUnitInfoSimple{
			{Suid: suid, DiskID: 1},
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any, any).Return(sunits, nil)
		suids, err := mgr.ListImmigratedSuid(ctx, 1)
		require.NoError(t, err)
		require.Len(t, suids, 1)
		require.Equal(t, suid, suids[0])

		// success with empty list
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskShardUnits(any, any).Return([]*client.ShardUnitInfoSimple{}, nil)
		suids, err = mgr.ListImmigratedSuid(ctx, 1)
		require.NoError(t, err)
		require.Len(t, suids, 0)
	}
}

func TestMigratingShardDisks(t *testing.T) {
	m := newMigratingShardDisks()
	require.NotNil(t, m)

	diskInfo1 := &client.ShardNodeDiskInfo{}
	diskInfo2 := &client.ShardNodeDiskInfo{}
	diskID1 := proto.DiskID(1)
	diskID2 := proto.DiskID(2)

	require.Equal(t, 0, m.size())

	// add
	m.add(diskID1, diskInfo1)
	m.add(diskID2, diskInfo2)
	require.Equal(t, 2, m.size())

	// get existing
	got, exist := m.get(diskID1)
	require.True(t, exist)
	require.Equal(t, diskInfo1, got)

	// get non-existent
	_, exist = m.get(proto.DiskID(999))
	require.False(t, exist)

	// list
	list := m.list()
	require.Len(t, list, 2)

	// delete
	m.delete(diskID1)
	require.Equal(t, 1, m.size())

	_, exist = m.get(diskID1)
	require.False(t, exist)
}

func TestShardMigrateLoad_WithCallback(t *testing.T) {
	callbackDiskIDs := make([]proto.DiskID, 0)
	cb := func(ctx context.Context, diskID proto.DiskID) {
		callbackDiskIDs = append(callbackDiskIDs, diskID)
	}
	mgr := newShardMigrateMgrWithCallback(t, cb)

	t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStateInited, MockMigrateShardInfoMap)
	protoTask, _ := t1.ToTask()
	mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return([]*proto.Task{protoTask}, nil)
	err := mgr.Load()
	require.NoError(t, err)
	require.Len(t, callbackDiskIDs, 1)
	require.Equal(t, t1.Source.DiskID, callbackDiskIDs[0])
}

func TestDealCancelReason_GetShardInfoError(t *testing.T) {
	ctx := context.Background()
	idc := "z0"

	mgr := newShardMigrateMgr(t)
	t1 := mockGenShardMigrateTask(100, proto.TaskTypeShardDiskRepair, "z0", 4, proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
	mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)

	// code triggers GetShardInfo, but GetShardInfo fails → early return, no panic
	mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetShardInfo(any, any).Return(nil, errMock)
	taskArgs := genShardTaskArgs(t1, errcode.ErrShardNodeNotLeader.Error(), errcode.CodeShardNodeNotLeader)
	err := mgr.CancelTask(ctx, taskArgs)
	require.NoError(t, err)
}

func TestShardMigrateRun_QueueFull(t *testing.T) {
	mgr := newShardMigrateMgr(t)
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().WaitEnable().AnyTimes().Return()
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().AnyTimes().Return(true)

	// fill workQueue to capacity (WorkQueueSize=3) to trigger the "queue full" branch
	for i := proto.ShardID(200); i < 203; i++ {
		MockMigrateShardInfoMap[i] = MockGenShardInfo(i, 0)
		task := mockGenShardMigrateTask(i, proto.TaskTypeShardDiskRepair, "z0", proto.DiskID(i), proto.ShardTaskStatePrepared, MockMigrateShardInfoMap)
		mgr.workQueue.AddPreparedTask("z0", task.TaskID, task)
	}

	mgr.Run()
	// give the loop time to hit the "queue full" branch
	time.Sleep(5 * time.Millisecond)
}
