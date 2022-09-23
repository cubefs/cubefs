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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type mockWorker struct {
	blocking       chan struct{}
	taskRetErr     error
	checkRetErr    error
	genTaskletsErr error

	taskLetCntMu sync.Mutex
	taskLetCnt   int
}

func (w *mockWorker) GenTasklets(ctx context.Context) ([]Tasklet, *WorkError) {
	tasklets := make([]Tasklet, 0, 12)
	for id := proto.BlobID(1); id <= 12; id++ {
		tasklets = append(tasklets, Tasklet{bids: mockGenTasklet(id)})
	}
	if w.genTaskletsErr != nil {
		if err, ok := w.genTaskletsErr.(*WorkError); ok {
			return tasklets, err
		}
		return tasklets, SrcError(w.genTaskletsErr)
	}
	return tasklets, nil
}

func (w *mockWorker) ExecTasklet(ctx context.Context, t Tasklet) *WorkError {
	var ok bool
	if w.blocking != nil {
		_, ok = <-w.blocking
	}
	w.taskLetCntMu.Lock()
	defer w.taskLetCntMu.Unlock()
	w.taskLetCnt++
	if w.taskRetErr != nil && !ok {
		return OtherError(w.taskRetErr)
	}
	return nil
}

func (w *mockWorker) Check(ctx context.Context) *WorkError {
	return OtherError(w.checkRetErr)
}

func (w *mockWorker) OperateArgs() scheduler.OperateTaskArgs {
	return scheduler.OperateTaskArgs{TaskID: "test_mock_task", TaskType: w.TaskType()}
}

func (w *mockWorker) TaskType() proto.TaskType {
	return proto.TaskTypeDiskRepair
}

func (w *mockWorker) GetBenchmarkBids() (bids []*ShardInfoSimple) {
	bids = make([]*ShardInfoSimple, 0, 12)
	for id := proto.BlobID(1); id <= 12; id++ {
		bids = append(bids, &ShardInfoSimple{Size: 0, Bid: id})
	}
	return
}

type mockStats struct {
	wg   sync.WaitGroup
	step string
}

func newMockSchedulerCli(t *testing.T, stats *mockStats) scheduler.IMigrator {
	cli := mocks.NewMockIScheduler(C(t))
	cli.EXPECT().ReportTask(A, A).AnyTimes().Return(nil)
	cli.EXPECT().CancelTask(A, A).AnyTimes().DoAndReturn(
		func(context.Context, *scheduler.OperateTaskArgs) error {
			stats.step = "Cancel"
			stats.wg.Done()
			return errors.New("nothing")
		})
	cli.EXPECT().CompleteTask(A, A).AnyTimes().DoAndReturn(
		func(context.Context, *scheduler.OperateTaskArgs) error {
			stats.step = "Complete"
			stats.wg.Done()
			return errors.New("nothing")
		})
	cli.EXPECT().ReclaimTask(A, A).AnyTimes().DoAndReturn(
		func(context.Context, *scheduler.OperateTaskArgs) error {
			stats.step = "Reclaim"
			stats.wg.Done()
			return errors.New("nothing")
		})
	return cli
}

func TestTaskRunner(t *testing.T) {
	taskID := "test_mock_task"
	idc := "z0"
	stats := &mockStats{}
	cli := newMockSchedulerCli(t, stats)
	run := func(worker ITaskWorker) {
		runner := NewTaskRunner(context.Background(), taskID, worker, idc, 3, &taskCounter{}, cli)
		stats.step = ""
		stats.wg.Add(1)
		go runner.Run()
		stats.wg.Wait()
	}
	// test stop
	{
		log.Info("start test tasklet stop")
		blocking := make(chan struct{})
		worker := &mockWorker{blocking: blocking}
		runner := NewTaskRunner(context.Background(), taskID, worker, idc, 2, &taskCounter{}, cli)
		stats.step = ""
		stats.wg.Add(1)
		go runner.Run()
		for range [7]struct{}{} {
			blocking <- struct{}{}
		}
		runner.Stop()
		close(blocking)
		stats.wg.Wait()
		require.Equal(t, "Cancel", stats.step)
		require.True(t, worker.taskLetCnt < 12)
	}
	// test tasklet fail
	{
		log.Info("start test tasklet fail")
		blocking := make(chan struct{})
		worker := &mockWorker{blocking: blocking, taskRetErr: errors.New("mock fail")}
		runner := NewTaskRunner(context.Background(), taskID, worker, idc, 3, &taskCounter{}, cli)
		stats.step = ""
		stats.wg.Add(1)
		go runner.Run()
		for range [7]struct{}{} {
			blocking <- struct{}{}
		}
		close(blocking)
		stats.wg.Wait()
		require.Equal(t, "Cancel", stats.step)
		require.True(t, worker.taskLetCnt < 12)
	}
	// test check fail
	{
		log.Info("start test check fail")
		worker := &mockWorker{checkRetErr: errors.New("mock check fail")}
		run(worker)
		require.Equal(t, "Cancel", stats.step)
		require.Equal(t, 12, worker.taskLetCnt)
	}
	// test genTasklet fail
	{
		log.Info("start test genTasklet fail")
		worker := &mockWorker{genTaskletsErr: errors.New("mock check fail")}
		run(worker)
		require.Equal(t, "Cancel", stats.step)
		require.Equal(t, 0, worker.taskLetCnt)
	}
	// test genTasklet dest fail
	{
		log.Info("start test genTasklet dest fail")
		worker := &mockWorker{genTaskletsErr: DstError(errors.New("mock dest fail"))}
		run(worker)
		require.Equal(t, "Reclaim", stats.step)
		require.Equal(t, 0, worker.taskLetCnt)
	}
	// test tasklet complete
	{
		log.Info("start test tasklet complete")
		worker := &mockWorker{}
		run(worker)
		require.Equal(t, "Complete", stats.step)
		require.Equal(t, 12, worker.taskLetCnt)
	}
}

func TestTaskState(t *testing.T) {
	s := taskState{}
	s.set(TaskRunning)
	require.Equal(t, TaskRunning, s.state)
	s.set(TaskRunning)
	require.Equal(t, s.state, TaskRunning)
	require.Equal(t, true, s.alive())
	s.set(TaskStopping)
	require.Equal(t, s.state, TaskStopping)
	require.Equal(t, false, s.alive())
	require.Equal(t, false, s.stopped())

	s.set(TaskStopped)
	require.Equal(t, true, s.stopped())
	require.Equal(t, false, s.alive())
	s.set(TaskSuccess)
	require.Equal(t, false, s.alive())
	require.Equal(t, true, s.stopped())
}
