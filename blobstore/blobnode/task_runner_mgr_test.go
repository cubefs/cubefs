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

package blobnode

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func mockGenTasklet(bids ...proto.BlobID) (ret []*ShardInfoSimple) {
	for _, bid := range bids {
		ret = append(ret, &ShardInfoSimple{Bid: bid})
	}
	return
}

var mocktasklets = func() []Tasklet {
	lets := make([]Tasklet, 12)
	for idx := range [12]struct{}{} {
		lets[idx].bids = mockGenTasklet(proto.BlobID(idx + 1))
	}
	return lets
}()

type mockMigrateWorker struct {
	tasklet []Tasklet
}

func NewMockMigrateWorker(task MigrateTaskEx) ITaskWorker {
	return &mockMigrateWorker{tasklet: mocktasklets}
}

func (w *mockMigrateWorker) GenTasklets(ctx context.Context) ([]Tasklet, *WorkError) {
	time.Sleep(3600 * time.Second)
	return w.tasklet, nil
}

func (w *mockMigrateWorker) ExecTasklet(ctx context.Context, t Tasklet) *WorkError { return nil }
func (w *mockMigrateWorker) Check(ctx context.Context) *WorkError                  { return nil }
func (w *mockMigrateWorker) TaskType() proto.TaskType                              { return proto.TaskTypeBalance }
func (w *mockMigrateWorker) GetBenchmarkBids() []*ShardInfoSimple                  { return nil }
func (w *mockMigrateWorker) OperateArgs(reason string) *scheduler.TaskArgs {
	return &scheduler.TaskArgs{ModuleType: proto.TypeBlobNode, TaskType: w.TaskType()}
}

func initTestTaskRunnerMgr(t *testing.T, cli scheduler.IMigrator, taskCnt int, taskTypes ...proto.TaskType) *TaskRunnerMgr {
	tm := NewTaskRunnerMgr("Z0", getDefaultConfig().WorkerConfigMeter, NewMockMigrateWorker, cli, cli)

	ctx := context.Background()
	for _, typ := range taskTypes {
		for i := 0; i < taskCnt; i++ {
			taskID := fmt.Sprintf("%s_%d", typ, i+1)
			err := tm.AddTask(ctx, MigrateTaskEx{
				taskInfo: &proto.MigrateTask{TaskID: taskID, TaskType: typ, CodeMode: 200},
			})
			require.NoError(t, err)
		}
	}
	time.Sleep(200 * time.Millisecond) // wait task runnner started
	return tm
}

func initTestShardTaskRunnerMgr(t *testing.T, cli scheduler.IMigrator, taskCnt int, shardCli client.IShardNode,
	taskTypes ...proto.TaskType) *TaskRunnerMgr {
	tm := NewTaskRunnerMgr("Z0", getDefaultConfig().WorkerConfigMeter, NewMockMigrateWorker, cli, cli)

	ctx := context.Background()
	for _, typ := range taskTypes {
		for i := 0; i < taskCnt; i++ {
			taskID := fmt.Sprintf("%s_%d", typ, i+1)
			worker := NewShardWorker(&proto.ShardMigrateTask{
				TaskID:   taskID,
				TaskType: typ,
			}, shardCli, 0)
			worker.shardNodeCli.(*MockIShardNode).EXPECT().UpdateShard(any, any).Return(nil)
			err := tm.AddShardTask(ctx, worker)
			require.NoError(t, err)
		}
	}
	time.Sleep(200 * time.Millisecond) // wait task runnner started
	return tm
}

func TestTaskRunnerMgr(t *testing.T) {
	schedCli := mocks.NewMockIScheduler(C(t))
	shardCli := NewMockIShardNode(C(t))
	{
		tm := initTestTaskRunnerMgr(t, schedCli, 10)
		require.Equal(t, 0, len(tm.GetAliveTasks()))
		tm.StopAllAliveRunner()
		tm.TaskStats()
	}
	{
		tm := initTestTaskRunnerMgr(t, schedCli, 0, proto.TaskTypeBalance)
		require.Equal(t, 0, len(tm.GetAliveTasks()))
		tm.StopAllAliveRunner()
	}
	{
		tm := initTestTaskRunnerMgr(t, schedCli, 10, proto.TaskTypeBalance)
		tasks := tm.GetAliveTasks()
		require.Equal(t, 1, len(tasks))
		require.Equal(t, 10, len(tasks[proto.TaskTypeBalance]))
		tm.StopAllAliveRunner()
	}
	{
		tm := initTestTaskRunnerMgr(t, schedCli, 10, proto.TaskTypeBalance, proto.TaskTypeDiskDrop, proto.TaskTypeDiskRepair)
		tasks := tm.GetAliveTasks()
		require.Equal(t, 3, len(tasks))
		require.Equal(t, 10, len(tasks[proto.TaskTypeBalance]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeDiskDrop]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeDiskRepair]))
		require.Equal(t, 0, len(tasks[proto.TaskTypeManualMigrate]))
		tm.StopAllAliveRunner()
		tm.TaskStats()
	}
	{
		tm := initTestShardTaskRunnerMgr(t, schedCli, 1, shardCli, proto.TaskTypeShardDiskRepair)
		tm.schedulerCli.(*mocks.MockIScheduler).EXPECT().CancelTask(A, A).AnyTimes().Return(nil)
		tasks := tm.GetAliveTasks()
		require.Equal(t, 1, len(tasks))
		require.Equal(t, 1, len(tasks[proto.TaskTypeShardDiskRepair]))
		require.Equal(t, 0, len(tasks[proto.TaskTypeManualMigrate]))
		tm.StopAllAliveRunner()
		tm.TaskStats()
	}
}

func TestNewTaskRunnerMgr2(t *testing.T) {
	cli := mocks.NewMockIScheduler(C(t))
	tm := NewTaskRunnerMgr("Z0", getDefaultConfig().WorkerConfigMeter, NewMockMigrateWorker, cli, cli)
	ctx := context.Background()
	err := tm.AddTask(ctx, MigrateTaskEx{
		taskInfo: &proto.MigrateTask{CodeMode: codemode.Replica3, TaskID: "id", TaskType: proto.TaskTypeBalance},
	})
	require.Error(t, err, errcode.ErrUnsupportedTaskCodeMode)
}

func newMockRenewalCli(t *testing.T, mockFailTasks map[string]bool, mockErr error, times int) scheduler.IMigrator {
	cli := mocks.NewMockIScheduler(C(t))
	cli.EXPECT().RenewalTask(A, A).Times(times).DoAndReturn(
		func(_ context.Context, tasks *scheduler.TaskRenewalArgs) (*scheduler.TaskRenewalRet, error) {
			result := &scheduler.TaskRenewalRet{Errors: make(map[proto.TaskType]map[string]string)}
			for typ, ids := range tasks.IDs {
				errs := make(map[string]string)
				for _, taskID := range ids {
					if _, ok := mockFailTasks[taskID]; ok {
						errs[taskID] = "mock fail"
					}
				}
				result.Errors[typ] = errs
			}
			return result, mockErr
		})
	return cli
}

func TestWorkerTaskRenewal(t *testing.T) {
	// test renewal ok
	{
		cli := newMockRenewalCli(t, nil, nil, 1)
		tm := initTestTaskRunnerMgr(t, cli, 20, proto.TaskTypeDiskDrop, proto.TaskTypeDiskRepair)
		tm.renewalTask()
		tasks := tm.GetAliveTasks()
		require.Equal(t, 2, len(tasks))
		require.Equal(t, 20, len(tasks[proto.TaskTypeDiskDrop]))
		require.Equal(t, 20, len(tasks[proto.TaskTypeDiskRepair]))
	}

	// test few renewal fail
	{
		mockFailTasks := make(map[string]bool)
		cli := newMockRenewalCli(t, mockFailTasks, nil, 2)
		tm := initTestTaskRunnerMgr(t, cli, 11, proto.TaskTypeBalance, proto.TaskTypeDiskDrop,
			proto.TaskTypeDiskRepair, proto.TaskTypeManualMigrate)

		tm.renewalTask()
		tasks := tm.GetAliveTasks()
		require.Equal(t, 4, len(tasks))
		require.Equal(t, 11, len(tasks[proto.TaskTypeBalance]))

		mockFailTasks[proto.TaskTypeBalance.String()+"_1"] = true
		mockFailTasks[proto.TaskTypeBalance.String()+"_7"] = true
		mockFailTasks[proto.TaskTypeDiskDrop.String()+"_1"] = true
		mockFailTasks[proto.TaskTypeDiskRepair.String()+"_3"] = true
		mockFailTasks[proto.TaskTypeManualMigrate.String()+"_10"] = true

		tm.renewalTask()
		tasks = tm.GetAliveTasks()
		require.Equal(t, 4, len(tasks))
		require.Equal(t, 9, len(tasks[proto.TaskTypeBalance]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeDiskDrop]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeDiskRepair]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeManualMigrate]))
	}

	// test all renewal fail
	{
		cli := newMockRenewalCli(t, nil, errors.New("mock fail"), 1)
		tm := initTestTaskRunnerMgr(t, cli, 11, proto.TaskTypeBalance, proto.TaskTypeDiskDrop,
			proto.TaskTypeDiskRepair, proto.TaskTypeManualMigrate)

		tasks := tm.GetAliveTasks()
		require.Equal(t, 4, len(tasks))

		tm.renewalTask()
		tasks = tm.GetAliveTasks()
		require.Equal(t, 0, len(tasks))
	}
	// shard task
	{
		cli := newMockRenewalCli(t, nil, errors.New("mock fail"), 1)
		cli.(*mocks.MockIScheduler).EXPECT().CancelTask(A, A).Times(1).Return(nil)
		shardCli := NewMockIShardNode(C(t))
		tm := initTestShardTaskRunnerMgr(t, cli, 1, shardCli, proto.TaskTypeShardDiskRepair)
		tasks := tm.GetAliveTasks()
		require.Equal(t, 1, len(tasks))
		tm.renewalTask()
		tasks = tm.GetAliveTasks()
		require.Equal(t, 0, len(tasks))
		tm.StopAllAliveRunner()
	}
	// test renewal ok
	{
		cli := newMockRenewalCli(t, nil, nil, 1)
		shardCli := NewMockIShardNode(C(t))
		tm := initTestShardTaskRunnerMgr(t, cli, 1, shardCli, proto.TaskTypeShardDiskRepair)
		tm.renewalTask()
		tasks := tm.GetAliveTasks()
		require.Equal(t, 1, len(tasks))
		require.Equal(t, 1, len(tasks[proto.TaskTypeShardDiskRepair]))
	}
}
