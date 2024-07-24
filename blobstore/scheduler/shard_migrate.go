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
	"time"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

// ShardMigrateConfig migrate config
type ShardMigrateConfig struct {
	ClusterID proto.ClusterID `json:"-"` // fill in config.go
	base.TaskCommonConfig
}

type ShardMigrator interface {
	BaseMigrator

	Stats() api.ShardTaskStat

	taskswitch.ISwitcher
	closer.Closer
	Load() error
	Run()
}

type ShardMigrateMgr struct {
	closer.Closer

	taskType   proto.TaskType
	taskSwitch taskswitch.ISwitcher

	prepareQueue *base.TaskQueue      // store inited task
	workQueue    *base.ShardTaskQueue // store prepared task
	finishQueue  *base.TaskQueue      // store completed task

	clusterMgrCli client.ClusterMgrAPI

	finishTaskCounter counter.Counter
	taskStatsMgr      *base.TaskStatsMgr

	cfg *ShardMigrateConfig
}

func (mgr *ShardMigrateMgr) AcquireTask(ctx context.Context, idc string) (task *proto.Task, err error) {
	span := trace.SpanFromContextSafe(ctx)
	task = new(proto.Task)
	task.ModuleType = proto.TypeShardNode
	if !mgr.taskSwitch.Enabled() {
		return task, proto.ErrTaskPaused
	}
	_, migTask, _ := mgr.workQueue.Acquire(idc)
	if migTask != nil {
		t := *migTask.(*proto.ShardMigrateTask)
		data, err := t.Marshal()
		if err != nil {
			return task, err
		}
		task.Data = data
		span.Infof("acquire %s taskId: %s", mgr.taskType, t.TaskID)
		return task, nil
	}
	return task, proto.ErrTaskEmpty
}

func (mgr *ShardMigrateMgr) CancelTask(ctx context.Context, args *api.TaskArgs) error {
	mgr.taskStatsMgr.CancelTask()

	arg := &api.ShardTaskArgs{}
	err := arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}
	if !client.ValidMigrateTask(args.TaskType, arg.TaskID) {
		return errcode.ErrIllegalArguments
	}

	err = mgr.workQueue.Cancel(arg.IDC, arg.TaskID, arg.Source, arg.Dest)
	if err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Errorf("cancel shard migrate failed: task_type[%s], task_id[%s], err[%+v]", mgr.taskType, arg.TaskID, err)
	}
	return nil
}

func (mgr *ShardMigrateMgr) CompleteTask(ctx context.Context, args *api.TaskArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	arg := &api.ShardTaskArgs{}
	err := arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}
	if !client.ValidMigrateTask(args.TaskType, arg.TaskID) {
		return errcode.ErrIllegalArguments
	}

	completeTask, err := mgr.workQueue.Complete(arg.IDC, arg.TaskID, arg.Source, arg.Dest)
	if err != nil {
		span.Errorf("complete migrate task failed: task_id[%s], err[%+v]", arg.TaskID, err)
		return err
	}

	t := completeTask.(*proto.ShardMigrateTask)
	t.State = proto.ShardTaskStateWorkCompleted
	task, err := t.Task()
	if err != nil {
		return err
	}
	err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
	if err != nil {
		// there is no impact if we failed to update task state in db,
		// because we will do it in finishTask again, so assume complete success
		span.Errorf("complete migrate task into db failed: task_id[%s], err[%+v]", t.TaskID, err)
		err = nil
	}
	mgr.finishQueue.PushTask(arg.TaskID, t)
	return nil
}

func (mgr *ShardMigrateMgr) ReclaimTask(ctx context.Context, args *api.TaskArgs) error {
	return nil
}

func (mgr *ShardMigrateMgr) RenewalTask(ctx context.Context, idc, taskID string) error {
	return nil
}

func (mgr *ShardMigrateMgr) QueryTask(ctx context.Context, taskID string) (*api.TaskRet, error) {
	return nil, nil
}

func (mgr *ShardMigrateMgr) ReportTask(ctx context.Context, args *api.TaskArgs) (err error) {
	return nil
}

func (mgr *ShardMigrateMgr) Stats() api.ShardTaskStat {
	return api.ShardTaskStat{}
}

// Enabled task switch
func (mgr *ShardMigrateMgr) Enabled() bool {
	return mgr.taskSwitch.Enabled()
}

func (mgr *ShardMigrateMgr) WaitEnable() {
	mgr.taskSwitch.WaitEnable()
}

func (mgr *ShardMigrateMgr) Close() {
	mgr.Closer.Close()
}

// Done returns a channel that's closed when object was closed.
func (mgr *ShardMigrateMgr) Done() <-chan struct{} {
	return mgr.Closer.Done()
}

func (mgr *ShardMigrateMgr) Load() error {
	return nil
}

func (mgr *ShardMigrateMgr) Run() {
	go mgr.prepareTaskLoop()
	go mgr.finishTaskLoop()
}

func (mgr *ShardMigrateMgr) prepareTaskLoop() {
	go func() {
		for {
			mgr.taskSwitch.WaitEnable()
			todo, doing := mgr.workQueue.StatsTasks()
			if todo+doing >= mgr.cfg.WorkQueueSize {
				time.Sleep(prepareTaskPause)
				continue
			}
			err := mgr.prepareTask()
			if errors.Is(err, base.ErrNoTaskInQueue) {
				time.Sleep(prepareMigrateTaskInterval)
			}
		}
	}()
}

func (mgr *ShardMigrateMgr) finishTaskLoop() {
	for {
		err := mgr.finishTask()
		if errors.Is(err, base.ErrNoTaskInQueue) {
			time.Sleep(finishMigrateTaskInterval)
		}
	}
}

func (mgr *ShardMigrateMgr) prepareTask() error {
	return nil
}

func (mgr *ShardMigrateMgr) finishTask() error {
	return nil
}
