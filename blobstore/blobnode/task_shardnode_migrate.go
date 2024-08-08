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

package blobnode

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const defaultCheckShardStatusIntervalMS = 30 * 1000

var ErrShardSyncDataFailed = errors.New("shard sync data failed")

// ShardWorker used to manager shard migrate task
type ShardWorker struct {
	t          *proto.ShardMigrateTask
	intervalMS int64
	lastIndex  uint64
	retry      int

	shardNodeCli client.IShardNode
}

func NewShardWorker(task *proto.ShardMigrateTask, client client.IShardNode, interval int64) *ShardWorker {
	if interval <= 0 {
		interval = defaultCheckShardStatusIntervalMS
	}
	return &ShardWorker{
		t:            task,
		intervalMS:   interval,
		shardNodeCli: client,
	}
}

func (s *ShardWorker) AddShardMember(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start add member to shard[%d], disk[%d], host[%s]", s.t.Destination.Suid.ShardID(),
		s.t.Destination.DiskID, s.t.Destination.Host)

	destination := s.t.Destination
	destination.Learner = true
	err := retry.Timed(3, 100).RuptOn(func() (bool, error) {
		if err := s.shardNodeCli.UpdateShard(ctx, &client.UpdateShardArgs{
			Unit: destination,
			Type: proto.ShardUpdateTypeAddMember,
		}); err != nil {
			span.Warnf("add shard member failed, suid[%d], diskid[%d], err: %s", s.t.Destination.Suid,
				s.t.Destination.DiskID, err)
			return false, err
		}
		return true, nil
	})
	if err != nil {
		span.Errorf("add shard member failed,suid[%d], diskid[%d], err: %s", s.t.Destination.Suid,
			s.t.Destination.DiskID, err)
		return err
	}

	ticker := time.NewTicker(time.Duration(s.intervalMS) * time.Millisecond)
	defer ticker.Stop()
	var success bool
	for {
		select {
		case <-ctx.Done():
			span.Warnf("task has stoped, taskID[%s]", s.t.TaskID)
			return ctx.Err()
		case <-ticker.C:
			success = s.checkStatus(ctx)
		}
		if success {
			break
		}
		if s.retry >= 3 {
			return ErrShardSyncDataFailed
		}
	}
	return nil
}

func (s *ShardWorker) RemoveShardMember(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start remove member to shard[%d], disk[%d], host[%s]", s.t.Source.Suid.ShardID(),
		s.t.Source.DiskID, s.t.Source.Host)
	err := retry.Timed(3, 100).RuptOn(func() (bool, error) {
		if err := s.shardNodeCli.UpdateShard(ctx, &client.UpdateShardArgs{
			Unit: s.t.Source,
			Type: proto.ShardUpdateTypeRemoveMember,
		}); err != nil {
			span.Warnf("remove shard member failed, suid[%d], diskid[%d], err: %s", s.t.Source.Suid,
				s.t.Source.DiskID, err)
			return false, err
		}
		return true, nil
	})
	if err != nil {
		span.Errorf("remove shard member failed, suid[%d], diskid[%d], err: %s", s.t.Source.Suid,
			s.t.Source.DiskID, err)
		return err
	}
	return nil
}

func (s *ShardWorker) UpdateShardMember(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	destination := s.t.Destination
	if destination.Learner == s.t.Source.Learner {
		return nil
	}

	destination.Learner = s.t.Source.Learner
	err := s.shardNodeCli.UpdateShard(ctx, &client.UpdateShardArgs{
		Unit: destination,
		Type: proto.ShardUpdateTypeUpdateMember,
	})
	if err != nil {
		span.Errorf("remove shard member failed, suid[%d], diskid[%d], err: %s", s.t.Destination.Suid,
			s.t.Destination.DiskID, err)
		// todo retry other raft member
		return err
	}
	return nil
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

func (s *ShardWorker) checkStatus(ctx context.Context) bool {
	span := trace.SpanFromContextSafe(ctx)
	status, err := s.shardNodeCli.GetShardStatus(ctx, s.t.Destination.Suid, s.t.Destination.DiskID)
	if err != nil {
		span.Errorf("get shard status failed, shardID[%d], err[%s]", s.t.Destination.Suid.ShardID(), err)
		return false
	}
	if status.AppliedIndex+s.t.Threshold >= status.LeaderIndex {
		return true
	}
	if status.AppliedIndex <= s.lastIndex {
		s.retry += 1
	}
	s.lastIndex = status.AppliedIndex
	return false
}

// IShardWorker define for shard node task executor
type IShardWorker interface {
	AddShardMember(ctx context.Context) error
	RemoveShardMember(ctx context.Context) error
	UpdateShardMember(ctx context.Context) error
	OperateArgs(reason string) *scheduler.TaskArgs
}

// ShardNodeTaskRunner used to manage task
type ShardNodeTaskRunner struct {
	taskID string
	w      IShardWorker
	idc    string
	state  TaskState

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
	span, ctx := trace.StartSpanFromContext(ctx, "shardTaskRunner")
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

	err := s.w.AddShardMember(s.ctx)
	if err != nil {
		s.cancelOrReclaim(err)
		return
	}
	span.Infof("add shard member success, taskID[%s]", s.TaskID())
	err = s.w.RemoveShardMember(s.ctx)
	if err != nil {
		s.cancelOrReclaim(err)
		return
	}
	err = s.w.UpdateShardMember(s.ctx)
	if err != nil {
		s.cancelOrReclaim(err)
		return
	}
	s.complete()
	span.Infof("task Runner finish: taskID[%s]", s.TaskID())
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
	return &s.state
}

func (s *ShardNodeTaskRunner) ShouldReclaim(err error) bool {
	return err == ErrShardSyncDataFailed
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

func (s *ShardNodeTaskRunner) cancelOrReclaim(err error) {
	defer s.state.set(TaskStopped)
	span := s.span
	if s.ShouldReclaim(err) {
		args := s.w.OperateArgs(err.Error())
		span.Infof("reclaim shard task: taskID[%s], err[%s]", s.taskID, err.Error())
		if err := s.schedulerCli.ReclaimTask(s.newCtx(), args); err != nil {
			span.Errorf("reclaim shard task failed: taskID[%s], args[%+v], code[%d], err[%+v]",
				s.taskID, args, rpc.DetectStatusCode(err), err)
		}
		s.taskCounter.reclaim.Add()
		return
	}
	span.Infof("cancel shard task: taskID[%s], err[%+v]", s.taskID, err)

	args := s.w.OperateArgs(err.Error())
	if err := s.schedulerCli.CancelTask(s.newCtx(), args); err != nil {
		span.Errorf("cancel failed: taskID[%s], args[%+v], code[%d], err[%+v]",
			s.taskID, args, rpc.DetectStatusCode(err), err)
	}
	s.taskCounter.cancel.Add()
}

func (s *ShardNodeTaskRunner) complete() {
	defer s.state.set(TaskSuccess)

	s.span.Infof("complete shard task: taskID[%s]", s.taskID)
	args := s.w.OperateArgs("")
	if err := s.schedulerCli.CompleteTask(s.newCtx(), args); err != nil {
		s.span.Errorf("complete failed: taskID[%s], args[%+v], code[%d], err[%+v]",
			s.taskID, args, rpc.DetectStatusCode(err), err)
	}
}

func (s *ShardNodeTaskRunner) newCtx() context.Context {
	return trace.ContextWithSpan(context.Background(), s.span)
}
