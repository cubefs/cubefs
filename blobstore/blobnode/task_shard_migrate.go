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
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const defaultCheckShardStatusIntervalMS = 30 * 1000

var ErrShardSyncDataFailed = errors.New("shard sync data failed")

// IShardWorker define for shard node task executor
type IShardWorker interface {
	AddShardMember(ctx context.Context) error
	UpdateShardMember(ctx context.Context) error
	LeaderTransfer(ctx context.Context) error
	OperateArgs(reason string) *scheduler.ShardTaskArgs
}

// ShardWorker used to manager shard migrate task
type ShardWorker struct {
	task       *proto.ShardMigrateTask
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
		task:         task,
		intervalMS:   interval,
		shardNodeCli: client,
	}
}

func (s *ShardWorker) AddShardMember(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start add member to shard[%d], disk[%d], host[%s]", s.task.Destination.Suid.ShardID(),
		s.task.Destination.DiskID, s.task.Destination.Host)

	// set task destination into learner fist,
	// the next step of UpdateShardMember will fix learner state with source suid
	s.task.Destination.Learner = true
	destination := s.task.Destination

	err := retry.Timed(3, 1000).RuptOn(func() (bool, error) {
		err := s.shardNodeCli.UpdateShard(ctx, &client.UpdateShardArgs{
			Unit:   destination,
			Type:   proto.ShardUpdateTypeAddMember,
			Leader: s.task.Leader,
		})
		if err == nil {
			return true, nil
		}
		span.Warnf("add shard member failed, suid[%d], diskid[%d], err: %s", s.task.Destination.Suid,
			s.task.Destination.DiskID, err)
		if rpc.DetectStatusCode(err) == apierrors.CodeShardNodeNotLeader {
			s.checkLeaderChange(ctx)
		}
		return false, err
	})
	if err != nil {
		span.Errorf("add shard member failed,suid[%d], diskid[%d], err: %s", s.task.Destination.Suid,
			s.task.Destination.DiskID, err)
		return err
	}

	ticker := time.NewTicker(time.Duration(s.intervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			span.Warnf("task has been stopped, taskID[%s]", s.task.TaskID)
			return ctx.Err()
		case <-ticker.C:
		}
		if s.checkStatus(ctx) {
			return nil
		}
		if s.retry >= 3 {
			return ErrShardSyncDataFailed
		}
	}
}

func (s *ShardWorker) UpdateShardMember(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	if s.task.Destination.Learner == s.task.Source.Learner {
		return nil
	}
	// fix destination learner state
	s.task.Destination.Learner = s.task.Source.Learner
	destination := s.task.Destination

	err := retry.Timed(3, 3000).RuptOn(func() (bool, error) {
		err := s.shardNodeCli.UpdateShard(ctx, &client.UpdateShardArgs{
			Unit:   destination,
			Type:   proto.ShardUpdateTypeUpdateMember,
			Leader: s.task.Leader,
		})
		if err == nil {
			return true, nil
		}
		span.Warnf("update shard member failed, suid[%d], diskid[%d], err: %s", s.task.Destination.Suid,
			s.task.Destination.DiskID, err)
		if rpc.DetectStatusCode(err) == apierrors.CodeShardNodeNotLeader {
			s.checkLeaderChange(ctx)
		}
		return false, err
	})
	if err != nil {
		span.Errorf("update shard member failed, suid[%d], diskid[%d], err: %s", s.task.Destination.Suid,
			s.task.Destination.DiskID, err)
		return err
	}
	return nil
}

func (s *ShardWorker) LeaderTransfer(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	newLeader, err := s.shardNodeCli.GetShardLeader(ctx, s.task.Leader)
	if err != nil {
		span.Errorf("get shard status failed, err[%s]", err)
		return err
	}
	if !newLeader.Equal(&s.task.Source) {
		return nil
	}
	if newLeader.Equal(&s.task.Destination) {
		return nil
	}
	if !newLeader.Equal(&s.task.Leader) {
		s.task.Leader = *newLeader
	}

	err = retry.Timed(3, 3000).RuptOn(func() (bool, error) {
		err = s.shardNodeCli.LeaderTransfer(ctx, &client.LeaderTransferArgs{
			Leader: s.task.Leader,
			DiskID: s.task.Destination.DiskID,
		})
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		span.Errorf("transfer leader for shard[%d] failed, err[%s]", newLeader.Suid, err)
		return err
	}
	ticker := time.NewTicker(time.Duration(s.intervalMS) * time.Millisecond)
	defer ticker.Stop()
	retryTimes := 0
	for {
		newLeader, err = s.shardNodeCli.GetShardLeader(ctx, s.task.Leader)
		if err == nil {
			if newLeader.Equal(&s.task.Destination) {
				return nil
			}
			if !newLeader.Equal(&s.task.Leader) {
				s.task.Leader = *newLeader
			}
		}
		retryTimes++
		if retryTimes >= 3 {
			err = fmt.Errorf("transfer leader to suid[%d], disk[%d] failed too much times",
				s.task.Destination.Suid, s.task.Destination.DiskID)
			span.Warn(err)
			return err
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *ShardWorker) OperateArgs(reason string) *scheduler.ShardTaskArgs {
	return &scheduler.ShardTaskArgs{
		TaskType: s.task.TaskType,
		IDC:      s.task.SourceIDC,
		TaskID:   s.task.TaskID,
		Source:   s.task.Source,
		Dest:     s.task.Destination,
		Leader:   s.task.Leader,
		Reason:   reason,
	}
}

func (s *ShardWorker) checkStatus(ctx context.Context) bool {
	span := trace.SpanFromContextSafe(ctx)
	status, err := s.shardNodeCli.GetShardStatus(ctx, s.task.Destination.Suid, s.task.Leader)
	if err != nil {
		if errors.Is(err, client.LeaderOutdatedErr) {
			s.task.Leader = status.Leader
		}
		span.Errorf("get shard status failed, shardID[%d], err[%s]", s.task.Destination.Suid.ShardID(), err)
		return false
	}
	if status.AppliedIndex+s.task.Threshold >= status.LeaderAppliedIndex {
		return true
	}
	if status.AppliedIndex <= s.lastIndex {
		s.retry += 1
	}
	s.lastIndex = status.AppliedIndex
	return false
}

func (s *ShardWorker) checkLeaderChange(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	leader, err := s.shardNodeCli.GetShardLeader(ctx, s.task.Leader)
	if err != nil {
		span.Errorf("get shard status failed, err[%s]", err)
		return
	}
	if !leader.Equal(&s.task.Leader) {
		s.task.Leader = *leader
	}
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
	taskCounter  *taskCounter
}

// NewShardNodeTaskRunner return task runner
func NewShardNodeTaskRunner(ctx context.Context, taskID string, w IShardWorker, idc string,
	taskCounter *taskCounter, schedulerCli scheduler.IMigrator,
) Runner {
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
	err = s.w.UpdateShardMember(s.ctx)
	if err != nil {
		s.cancelOrReclaim(err)
		return
	}

	// if source is leader, transfer leader to new disk
	err = s.w.LeaderTransfer(s.ctx)
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
		if err := s.schedulerCli.ReclaimShardTask(s.newCtx(), args); err != nil {
			span.Errorf("reclaim shard task failed: taskID[%s], args[%+v], code[%d], err[%+v]",
				s.taskID, args, rpc.DetectStatusCode(err), err)
		}
		s.taskCounter.reclaim.Add()
		return
	}
	span.Infof("cancel shard task: taskID[%s], err[%+v]", s.taskID, err)

	args := s.w.OperateArgs(err.Error())
	if err := s.schedulerCli.CancelShardTask(s.newCtx(), args); err != nil {
		span.Errorf("cancel failed: taskID[%s], args[%+v], code[%d], err[%+v]",
			s.taskID, args, rpc.DetectStatusCode(err), err)
	}
	s.taskCounter.cancel.Add()
}

func (s *ShardNodeTaskRunner) complete() {
	defer s.state.set(TaskSuccess)

	s.span.Infof("complete shard task: taskID[%s]", s.taskID)
	args := s.w.OperateArgs("")
	if err := s.schedulerCli.CompleteShardTask(s.newCtx(), args); err != nil {
		s.span.Errorf("complete failed: taskID[%s], args[%+v], code[%d], err[%+v]",
			s.taskID, args, rpc.DetectStatusCode(err), err)
	}
}

func (s *ShardNodeTaskRunner) newCtx() context.Context {
	return trace.ContextWithSpan(context.Background(), s.span)
}
