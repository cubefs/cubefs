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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
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
	tasklet       []Tasklet
	taskletRetErr error
}

func NewmockMigrateWorker(task MigrateTaskEx) ITaskWorker {
	return &mockMigrateWorker{
		tasklet:       mocktasklets,
		taskletRetErr: nil,
	}
}

func (w *mockMigrateWorker) GenTasklets(ctx context.Context) ([]Tasklet, *WorkError) {
	time.Sleep(3600 * time.Second)
	return w.tasklet, nil
}

func (w *mockMigrateWorker) ExecTasklet(ctx context.Context, t Tasklet) *WorkError {
	return nil
}

func (w *mockMigrateWorker) Check(ctx context.Context) *WorkError {
	return nil
}

func (w *mockMigrateWorker) CancelArgs() (taskID string, taskType proto.TaskType, src []proto.VunitLocation, dest proto.VunitLocation) {
	return "test_mock_task", w.TaskType(), []proto.VunitLocation{}, proto.VunitLocation{}
}

func (w *mockMigrateWorker) CompleteArgs() (taskID string, taskType proto.TaskType, src []proto.VunitLocation, dest proto.VunitLocation) {
	return "test_mock_task", w.TaskType(), []proto.VunitLocation{}, proto.VunitLocation{}
}

func (w *mockMigrateWorker) ReclaimArgs() (taskID string, taskType proto.TaskType, src []proto.VunitLocation, dest proto.VunitLocation) {
	return "test_mock_task", w.TaskType(), []proto.VunitLocation{}, proto.VunitLocation{}
}

func (w *mockMigrateWorker) TaskType() proto.TaskType {
	return proto.TaskTypeBalance
}

func (w *mockMigrateWorker) GetBenchmarkBids() []*ShardInfoSimple {
	return nil
}

type mockWorkerFactory struct {
	newMigWorkerFn func(task MigrateTaskEx) ITaskWorker
}

func (mwf *mockWorkerFactory) NewMigrateWorker(task MigrateTaskEx) ITaskWorker {
	return mwf.newMigWorkerFn(task)
}

type mockScheCli struct {
	cancelRet   error
	completeRet error
	reclaimRet  error
	step        string
}

func (mock *mockScheCli) CancelTask(ctx context.Context, args *api.CancelTaskArgs) error {
	mock.step = "CancelOrReclaim"
	return mock.cancelRet
}

func (mock *mockScheCli) CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) error {
	mock.step = "Complete"
	return mock.completeRet
}

func (mock *mockScheCli) ReclaimTask(ctx context.Context, args *api.ReclaimTaskArgs) error {
	mock.step = "CancelOrReclaim"
	return mock.reclaimRet
}

func (mock *mockScheCli) ReportTask(ctx context.Context, args *api.TaskReportArgs) (err error) {
	return nil
}

func initTestTaskRunnerMgr(t *testing.T, taskCnt int, taskTypes ...proto.TaskType) *TaskRunnerMgr {
	cli := mockScheCli{}
	wf := mockWorkerFactory{newMigWorkerFn: NewmockMigrateWorker}
	tm := NewTaskRunnerMgr(getDefaultConfig().WorkerConfigMeter, &cli, &wf)

	ctx := context.Background()
	for _, typ := range taskTypes {
		for i := 0; i < taskCnt; i++ {
			taskID := fmt.Sprintf("%s_%d", typ, i+1)
			err := tm.AddTask(ctx, MigrateTaskEx{
				taskInfo: &proto.MigrateTask{TaskID: taskID, TaskType: typ},
			})
			require.NoError(t, err)
		}
	}
	time.Sleep(200 * time.Millisecond) // wait task runnner started
	return tm
}

func TestTaskRunnerMgr(t *testing.T) {
	{
		tm := initTestTaskRunnerMgr(t, 10)
		require.Equal(t, 0, len(tm.GetAliveTasks()))
		tm.StopAllAliveRunner()
	}
	{
		tm := initTestTaskRunnerMgr(t, 0, proto.TaskTypeBalance)
		require.Equal(t, 0, len(tm.GetAliveTasks()))
		tm.StopAllAliveRunner()
	}
	{
		tm := initTestTaskRunnerMgr(t, 10, proto.TaskTypeBalance)
		tasks := tm.GetAliveTasks()
		require.Equal(t, 1, len(tasks))
		require.Equal(t, 10, len(tasks[proto.TaskTypeBalance]))
		tm.StopAllAliveRunner()
	}
	{
		tm := initTestTaskRunnerMgr(t, 10, proto.TaskTypeBalance, proto.TaskTypeDiskDrop, proto.TaskTypeDiskRepair)
		tasks := tm.GetAliveTasks()
		require.Equal(t, 3, len(tasks))
		require.Equal(t, 10, len(tasks[proto.TaskTypeBalance]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeDiskDrop]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeDiskRepair]))
		require.Equal(t, 0, len(tasks[proto.TaskTypeManualMigrate]))
		tm.StopAllAliveRunner()
	}
}
