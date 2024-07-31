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
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// ShardWorker used to manager shard migrate task
type ShardWorker struct {
	t            *proto.ShardMigrateTask
	shardNodeCli client.IShardNode
}

func (s *ShardWorker) Do(ctx context.Context) {

}

func (s *ShardWorker) OperateArgs(reason string) *scheduler.TaskArgs {
	shardTaskArgs := &scheduler.ShardTaskArgs{
		TaskType: s.t.TaskType,
		IDC:      s.t.SourceIDC,
		TaskID:   s.t.TaskID,
		Source:   s.t.Source,
		Dest:     s.t.Destination,
		Leader:   s.t.Leader,
		Reason:   reason,
	}
	data, _ := shardTaskArgs.Marshal()
	ret := new(scheduler.TaskArgs)
	ret.Data = data
	ret.TaskType = shardTaskArgs.TaskType
	ret.ModuleType = proto.TypeShardNode
	return ret
}

// IShardWorker define for shard node task executor
type IShardWorker interface {
	Do(ctx context.Context)
	OperateArgs(reason string) *scheduler.TaskArgs
}

// ShardNodeTaskRunner used to manage task
type ShardNodeTaskRunner struct {
	taskID string
	w      IShardWorker
	idc    string
	state  *TaskState

	ctx    context.Context
	cancel context.CancelFunc
	span   trace.Span

	stopMu     sync.Mutex
	stopReason *WorkError

	schedulerCli scheduler.IMigrator
	stats        proto.TaskProgress // task progress statics
	taskCounter  *taskCounter
}

// NewShardNodeTaskRunner return task runner
func NewShardNodeTaskRunner(ctx context.Context, taskID string, w IShardWorker, idc string,
	taskCounter *taskCounter, schedulerCli scheduler.IMigrator) Runner {
	span, ctx := trace.StartSpanFromContext(ctx, "taskRunner")
	ctx, cancel := context.WithCancel(ctx)

	task := ShardNodeTaskRunner{
		taskID:       taskID,
		w:            w,
		idc:          idc,
		ctx:          ctx,
		cancel:       cancel,
		span:         span,
		schedulerCli: schedulerCli,
		stats:        proto.NewTaskProgress(),
		taskCounter:  taskCounter,
	}
	task.state.set(TaskInit)
	return &task
}

func (s *ShardNodeTaskRunner) Run() {
	span := s.span
	span.Infof("start run task: taskID[%s]", s.taskID)

	s.state.set(TaskRunning)

}

func (s *ShardNodeTaskRunner) Stop() {
	s.state.set(TaskStopping)
	s.stopWithFail(OtherError(errKilled))
}
func (s *ShardNodeTaskRunner) Stopped() bool {
	return s.state.stopped()
}
func (s *ShardNodeTaskRunner) Alive() bool {
	return s.state.alive()
}
func (s *ShardNodeTaskRunner) TaskID() string {
	return s.taskID
}
func (s *ShardNodeTaskRunner) State() *TaskState {
	return s.state
}

func (s *ShardNodeTaskRunner) stopWithFail(fail *WorkError) {
	s.span.Infof("stop task: taskID[%s], err_type[%d], err[%+v]", s.taskID, fail.errType, fail.err)
	s.stopMu.Lock()
	if s.stopReason == nil {
		s.stopReason = fail
	}
	s.stopMu.Unlock()
	s.cancel()
}

func (s *ShardNodeTaskRunner) cancelOrReclaim(retErr *WorkError) {
	defer s.state.set(TaskStopped)

}

func (s *ShardNodeTaskRunner) complete() {
	defer s.state.set(TaskSuccess)
}
