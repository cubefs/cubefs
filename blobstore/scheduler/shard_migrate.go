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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
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

	AppliedIndexThreshold uint64 `json:"applied_index_threshold"`

	loadTaskCallback loadTaskCallback
}

type ShardMigrator interface {
	BaseMigrator

	Stats() api.ShardTaskStat
	AddTask(ctx context.Context, task *proto.ShardMigrateTask)
	GetTask(ctx context.Context, taskID string) (*proto.ShardMigrateTask, error)
	ListMigratingSuid(ctx context.Context, diskID proto.DiskID) (suids []proto.Suid, err error)
	ListImmigratedSuid(ctx context.Context, diskID proto.DiskID) (suids []proto.Suid, err error)

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

type loadTaskCallback func(ctx context.Context, diskId proto.DiskID)

// NewShardMigrateMgr returns migrate manager
func NewShardMigrateMgr(
	clusterMgrCli client.ClusterMgrAPI,
	taskSwitch taskswitch.ISwitcher,
	conf *ShardMigrateConfig,
	taskType proto.TaskType,
) ShardMigrator {
	mgr := &ShardMigrateMgr{
		taskType:      taskType,
		taskSwitch:    taskSwitch,
		clusterMgrCli: clusterMgrCli,

		prepareQueue: base.NewTaskQueue(time.Duration(conf.PrepareQueueRetryDelayS) * time.Second),
		workQueue:    base.NewShardTaskQueue(time.Duration(conf.CancelPunishDurationS) * time.Second),
		finishQueue:  base.NewTaskQueue(time.Duration(conf.FinishQueueRetryDelayS) * time.Second),

		cfg:    conf,
		Closer: closer.New(),
	}
	mgr.taskStatsMgr = base.NewTaskStatsMgrAndRun(conf.ClusterID, taskType, mgr)
	return mgr
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
		task.TaskID = t.TaskID
		task.TaskType = t.TaskType
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
		return errcode.ErrIllegalArguments
	}
	if !client.ValidMigrateTask(args.TaskType, arg.TaskID) {
		return errcode.ErrIllegalArguments
	}

	mgr.dealCancelReason(ctx, arg)

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
		return errcode.ErrIllegalArguments
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
	task, err := t.ToTask()
	if err != nil {
		return err
	}
	err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
	if err != nil {
		// there is no impact if we failed to update task state in db,
		// because we will do it in finishTask again, so assume complete success
		span.Errorf("complete migrate task into db failed: task_id[%s], err[%+v]", t.TaskID, err)
	}
	mgr.finishQueue.PushTask(arg.TaskID, t)
	return nil
}

func (mgr *ShardMigrateMgr) ReclaimTask(ctx context.Context, args *api.TaskArgs) (err error) {
	mgr.taskStatsMgr.ReclaimTask()
	span := trace.SpanFromContextSafe(ctx)

	arg := &api.ShardTaskArgs{}
	err = arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}

	if !client.ValidMigrateTask(args.TaskType, arg.TaskID) {
		return errcode.ErrIllegalArguments
	}

	newDst, err := base.AllocShardUnitSafe(ctx, mgr.clusterMgrCli, arg.Source, arg.Dest, []proto.DiskID{arg.Dest.DiskID})
	if err != nil {
		span.Errorf("alloc volume unit from clustermgr failed, err: %s", err)
		return err
	}

	err = mgr.workQueue.Reclaim(arg.IDC, arg.TaskID, arg.Source, arg.Dest, newDst.ShardUnitInfoSimple, newDst.DiskID)
	if err != nil {
		span.Errorf("reclaim migrate task failed: task_type:[%s],task_id[%s], err[%+v]", mgr.taskType, arg.TaskID, err)
		return err
	}

	task, err := mgr.workQueue.Query(arg.IDC, arg.TaskID)
	if err != nil {
		span.Errorf("found task in workQueue failed: idc[%s], task_id[%s], err[%+v]", arg.IDC, arg.TaskID, err)
		return err
	}
	t, err := task.ToTask()
	if err != nil {
		return err
	}
	if err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, t); err != nil {
		span.Errorf("update reclaim task failed: task_id[%s], err[%+v]", arg.TaskID, err)
	}
	return
}

func (mgr *ShardMigrateMgr) RenewalTask(ctx context.Context, idc, taskID string) (err error) {
	if !mgr.taskSwitch.Enabled() {
		return proto.ErrTaskPaused
	}

	err = mgr.workQueue.Renewal(idc, taskID)
	if err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Warnf("renewal migrate task failed: task_type[%s], task_id[%s], err[%+v]", mgr.taskType, taskID, err)
	}
	return
}

func (mgr *ShardMigrateMgr) QueryTask(ctx context.Context, taskID string) (*api.TaskRet, error) {
	detail := &api.ShardTaskDetail{}
	task, err := mgr.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	detail.Task = *task

	// todo add statics

	data, err := json.Marshal(detail)
	if err != nil {
		return nil, err
	}

	return &api.TaskRet{TaskType: task.TaskType, Data: data}, nil
}

func (mgr *ShardMigrateMgr) ReportTask(ctx context.Context, args *api.TaskArgs) (err error) {
	return nil
}

func (mgr *ShardMigrateMgr) Stats() api.ShardTaskStat {
	preparing, doing, finishing := mgr.StatQueueTaskCnt()
	return api.ShardTaskStat{
		PreparingCnt:   preparing,
		WorkerDoingCnt: doing,
		FinishingCnt:   finishing,
	}
}

func (mgr *ShardMigrateMgr) StatQueueTaskCnt() (preparing, workerDoing, finishing int) {
	todo, doing := mgr.prepareQueue.StatsTasks()
	preparing = todo + doing

	todo, doing = mgr.workQueue.StatsTasks()
	workerDoing = todo + doing

	todo, doing = mgr.finishQueue.StatsTasks()
	finishing = todo + doing

	return
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

func (mgr *ShardMigrateMgr) Load() (err error) {
	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("start load shard node migrate task: task_type[%s]", mgr.taskType)
	// load task
	tasks, err := mgr.clusterMgrCli.ListAllMigrateTasks(ctx, mgr.taskType)
	if err != nil {
		span.Errorf("list tasks from clustermgr failed: err[%+v]", err)
		return
	}
	span.Infof("load task success: task_type[%s], tasks len[%d]", mgr.taskType, len(tasks))

	if len(tasks) == 0 {
		return
	}

	var junkTasks []*proto.ShardMigrateTask
	for i := range tasks {
		task := &proto.ShardMigrateTask{}
		err = task.Unmarshal(tasks[i].Data)
		if err != nil {
			return err
		}
		if mgr.isJunkTask(ctx, task) {
			junkTasks = append(junkTasks, task)
			continue
		}

		if mgr.cfg.loadTaskCallback != nil {
			mgr.cfg.loadTaskCallback(ctx, task.Source.DiskID)
		}

		if task.Running() {
			err = base.ShardTaskLockerInst().TryLock(ctx, uint32(task.Source.Suid.ShardID()))
			if err != nil {
				return fmt.Errorf("migrate task conflict: vid[%d], task[%+v], err[%+v]",
					task.Source.Suid.ShardID(), tasks[i], err)
			}
		}

		span.Infof("load task success: task_type[%s], task_id[%s], state[%d]", mgr.taskType, task.TaskID, task.State)
		switch task.State {
		case proto.ShardTaskStateInited:
			mgr.prepareQueue.PushTask(task.TaskID, task)
		case proto.ShardTaskStatePrepared:
			mgr.workQueue.AddPreparedTask(task.SourceIDC, task.TaskID, task)
		case proto.ShardTaskStateWorkCompleted:
			mgr.finishQueue.PushTask(task.TaskID, task)
		case proto.ShardTaskStateFinished, proto.ShardTaskStateFinishedInAdvance:
			return fmt.Errorf("task should be deleted from db: task[%+v]", task)
		default:
			return fmt.Errorf("unexpect migrate state: task[%+v]", task)
		}
	}
	return mgr.clearJunkTasks(ctx, junkTasks)
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

func (mgr *ShardMigrateMgr) prepareTask() (err error) {
	_, task, exist := mgr.prepareQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "shard.migrate.prepareTask")
	defer span.Finish()
	defer func() {
		if err != nil {
			mgr.prepareQueue.RetryTask(task.(*proto.ShardMigrateTask).TaskID)
		}
	}()

	taskC := *(task.(*proto.ShardMigrateTask))
	err = base.ShardTaskLockerInst().TryLock(ctx, uint32(taskC.Source.Suid.ShardID()))
	if err != nil {
		span.Warnf("lock shard failed: shard_id[%v], err[%+v]", taskC.Source.Suid.ShardID(), err)
		return base.ErrShardNotOnlyOneTask
	}
	defer func() {
		if err != nil {
			base.ShardTaskLockerInst().Unlock(ctx, uint32(taskC.Source.Suid.ShardID()))
		}
	}()

	shardInfo, err := mgr.clusterMgrCli.GetShardInfo(ctx, taskC.Source.Suid.ShardID())
	if err != nil {
		span.Errorf("prepare task failed: err[%v]", err)
		return err
	}

	if int(taskC.Source.Suid.Index()) >= len(shardInfo.ShardUnitInfos) || taskC.Source.Suid != shardInfo.ShardUnitInfos[taskC.Source.Suid.Index()].Suid {
		span.Infof("the source unit has been moved and finish task immediately: task_id[%s], task source suid[%v], current suid[%v]",
			taskC.TaskID, taskC.Source.Suid, shardInfo.ShardUnitInfos[taskC.Source.Suid.Index()].Suid)
		mgr.finishTaskInAdvance(ctx, &taskC)
		return
	}
	taskC.Source = shardInfo.ShardUnitInfos[taskC.Source.Suid.Index()]

	// alloc shard unit
	ret, err := base.AllocShardUnitSafe(ctx, mgr.clusterMgrCli, taskC.Source, taskC.Source, nil)
	if err != nil {
		span.Errorf("alloc shard unit failed: err[%+v]", err)
		return
	}

	taskC.Leader = shardInfo.ShardUnitInfos[shardInfo.Leader]
	taskC.Destination = ret.ShardUnitInfoSimple
	taskC.Destination.Learner = taskC.Source.Learner
	taskC.State = proto.ShardTaskStatePrepared
	taskC.Ctime = time.Now().String()

	// update db
	base.InsistOn(ctx, "shard migrate prepare task update task tbl", func() error {
		task, err := taskC.ToTask()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
	})

	mgr.workQueue.AddPreparedTask(taskC.SourceIDC, taskC.TaskID, &taskC)
	_ = mgr.prepareQueue.RemoveTask(taskC.TaskID)

	span.Infof("prepare task success: task_id[%s], state[%v]", taskC.TaskID, taskC.State)

	return nil
}

func (mgr *ShardMigrateMgr) finishTask() (err error) {
	_, task, exist := mgr.finishQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "migrate.finishTask")
	defer span.Finish()
	defer func() {
		if err != nil {
			mgr.finishQueue.RetryTask(task.(*proto.ShardMigrateTask).TaskID)
		}
	}()

	migrateTask := *(task.(*proto.ShardMigrateTask))
	span.Infof("finish task phase: task_id[%s], state[%v]", migrateTask.TaskID, migrateTask.State)
	if migrateTask.State != proto.ShardTaskStateWorkCompleted {
		span.Panicf("unexpect task state: task_id[%s], expect state[%d], actual state[%d]",
			migrateTask.TaskID, proto.ShardTaskStateWorkCompleted, migrateTask.State)
	}

	// because competed task did not persisted to the database, so in finish phase need to do it
	// the task maybe update more than once, which is allowed
	base.InsistOn(ctx, "migrate finish task update task tbl to state completed ", func() error {
		task, err := migrateTask.ToTask()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
	})

	// update shard mapping relationship
	err = mgr.clusterMgrCli.UpdateShard(ctx, &client.UpdateShardArgs{
		NewIsLearner: migrateTask.Destination.Learner,
		OldIsLearner: migrateTask.Source.Learner,
		NewDiskID:    migrateTask.Destination.DiskID,
		OldSuid:      migrateTask.Source.Suid,
		NewSuid:      migrateTask.Destination.Suid,
	})
	if err != nil {
		info, err_ := mgr.clusterMgrCli.GetShardInfo(ctx, migrateTask.Source.Suid.ShardID())
		if err_ != nil {
			span.Errorf("task[%s] get shard[%d] info from clustermgr failed, err: %s",
				migrateTask.TaskID, migrateTask.Source.Suid.ShardID(), err_)
			return err_
		}
		idx := migrateTask.Source.Suid.Index()
		if info.ShardUnitInfos[idx] != migrateTask.Destination {
			span.Errorf("change shard unit relationship failed: old suid[%d], new suid[%d], new diskId[%d], err[%+v]",
				migrateTask.Source.Suid,
				migrateTask.Destination.Suid,
				migrateTask.Destination.DiskID,
				err)
			err = mgr.handleUpdateShardMappingFail(ctx, &migrateTask, err)
			return
		}
	}

	// remove task from clustermgr
	migrateTask.State = proto.ShardTaskStateFinished
	base.InsistOn(ctx, "migrate finish task update task tbl", func() error {
		return mgr.clusterMgrCli.DeleteMigrateTask(ctx, migrateTask.TaskID)
	})

	_ = mgr.finishQueue.RemoveTask(migrateTask.TaskID)
	//_ = mgr.updateVolumeCache(ctx, migrateTask)

	base.ShardTaskLockerInst().Unlock(ctx, uint32(migrateTask.Source.Suid.ShardID()))
	// mgr.deleteMigratingVuid(migrateTask.SourceDiskID, migrateTask.SourceVuid)

	mgr.finishTaskCounter.Add()

	// add delete task and check it again
	// mgr.addDeletedTask(migrateTask)
	// mgr.finishTaskCallback(migrateTask.SourceDiskID)

	span.Infof("finish task phase success: task_id[%s], state[%v]", migrateTask.TaskID, migrateTask.State)
	return
}

func (mgr *ShardMigrateMgr) isJunkTask(ctx context.Context, task *proto.ShardMigrateTask) bool {
	return false
}

func (mgr *ShardMigrateMgr) clearJunkTasks(ctx context.Context, tasks []*proto.ShardMigrateTask) (err error) {
	return nil
}

func (mgr *ShardMigrateMgr) finishTaskInAdvance(ctx context.Context, task *proto.ShardMigrateTask) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("finish task in advance: task_id[%s], task[%+v]", task.TaskID, task)

	task.State = proto.ShardTaskStateFinishedInAdvance

	base.InsistOn(ctx, "migrate finish task in advance update tbl", func() error {
		return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
	})

	mgr.finishTaskCounter.Add()
	_ = mgr.prepareQueue.RemoveTask(task.TaskID)

	base.ShardTaskLockerInst().Unlock(ctx, uint32(task.Source.Suid.ShardID()))
}

func (mgr *ShardMigrateMgr) handleUpdateShardMappingFail(ctx context.Context, task *proto.ShardMigrateTask, err error) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("handle update shard mapping failed: task_id[%s], state[%d], dest suid[%d]",
		task.TaskID, task.State, task.Destination.Suid)

	code := rpc.DetectStatusCode(err)
	if code == errcode.CodeOldSuidNotMatch {
		span.Panicf("change shard unit relationship failed: old vuid not match")
	}

	if base.ShouldAllocShardUnitAndRedo(code) {
		span.Infof("realloc shard unit and redo: task_id[%s]", task.TaskID)
		newSunit, err := base.AllocShardUnitSafe(ctx, mgr.clusterMgrCli, task.Source, task.Destination, nil)
		if err != nil {
			span.Errorf("realloc failed: suid[%d], err[%+v]", task.Source.Suid, err)
			return err
		}
		task.SetDestination(newSunit.ShardUnitInfoSimple)
		task.State = proto.ShardTaskStatePrepared
		task.MTime = time.Now().String()

		base.InsistOn(ctx, "migrate redo task update task tbl", func() error {
			t, err := task.ToTask()
			if err != nil {
				return err
			}
			return mgr.clusterMgrCli.UpdateMigrateTask(ctx, t)
		})

		_ = mgr.finishQueue.RemoveTask(task.TaskID)
		mgr.workQueue.AddPreparedTask(task.SourceIDC, task.TaskID, task)
		span.Infof("task %+v redo again", task)

		return nil
	}

	return err
}

func (mgr *ShardMigrateMgr) dealCancelReason(ctx context.Context, args *api.ShardTaskArgs) {
	if !(strings.Contains(args.Reason, errcode.ErrShardNodeNotLeader.Error()) ||
		strings.Contains(args.Reason, errcode.ErrShardDoesNotExist.Error()) ||
		strings.Contains(args.Reason, errcode.ErrShardNodeDiskNotFound.Error())) {
		return
	}
	span := trace.SpanFromContextSafe(ctx)
	shardInfo, err := mgr.clusterMgrCli.GetShardInfo(ctx, args.Source.Suid.ShardID())
	if err != nil {
		span.Errorf("get shard info from clustermgr failed: err[%v]", err)
		return
	}
	if args.Leader.Equal(&shardInfo.ShardUnitInfos[shardInfo.Leader]) {
		return
	}
	task, err := mgr.workQueue.Update(args.IDC, args.TaskID, shardInfo.ShardUnitInfos[shardInfo.Leader])
	if err != nil {
		span.Errorf("update task  failed: err[%v]", err)
	}

	t, err := task.ToTask()
	if err != nil {
		span.Errorf("encode task failed: err[%v]", err)
	}
	err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, t)
	if err != nil {
		span.Errorf("update task  failed: err[%v]", err)
	}
}

func (mgr *ShardMigrateMgr) GetTask(ctx context.Context, id string) (task *proto.ShardMigrateTask, err error) {
	span := trace.SpanFromContextSafe(ctx)
	t, err := mgr.clusterMgrCli.GetMigrateTask(ctx, mgr.taskType, id)
	if err != nil {
		span.Errorf("get task[%s] from clustermgr failed, err: %s", id, err)
		return nil, err
	}
	task = new(proto.ShardMigrateTask)
	err = task.Unmarshal(t.Data)
	if err != nil {
		span.Errorf("unmarshal task[%s] from clustermgr failed, err: %s", id, err)
		return nil, err
	}
	return task, nil
}

// AddTask add shard migrate task, such as balance manualMigrate and drop task
func (mgr *ShardMigrateMgr) AddTask(ctx context.Context, task *proto.ShardMigrateTask) {
	// add task to db
	base.InsistOn(ctx, "migrate add task insert task to tbl", func() error {
		t, err := task.ToTask()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.AddMigrateTask(ctx, t)
	})

	// add task to prepare queue
	mgr.prepareQueue.PushTask(task.TaskID, task)

	// mgr.addMigratingVuid(task.SourceDiskID, task.SourceVuid, task.TaskID)
}

// ListMigratingSuid for disk drop and repair
func (mgr *ShardMigrateMgr) ListMigratingSuid(ctx context.Context, diskID proto.DiskID) (bads []proto.Suid, err error) {
	tasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, mgr.taskType, diskID)
	if err != nil {
		return nil, err
	}

	task := &proto.ShardMigrateTask{}
	for _, t := range tasks {
		err = task.Unmarshal(t.Data)
		if err != nil {
			return nil, err
		}
		bads = append(bads, task.Source.Suid)
	}
	return bads, nil
}

// ListImmigratedSuid for disk drop and repair
func (mgr *ShardMigrateMgr) ListImmigratedSuid(ctx context.Context, diskID proto.DiskID) (bads []proto.Suid, err error) {
	shardUnits, err := mgr.clusterMgrCli.ListDiskShardUnits(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, sunit := range shardUnits {
		bads = append(bads, sunit.Suid)
	}
	return bads, nil
}

type migratingShardDisks struct {
	disks map[proto.DiskID]*client.ShardNodeDiskInfo
	sync.RWMutex
}

func newMigratingShardDisks() *migratingShardDisks {
	return &migratingShardDisks{
		disks: make(map[proto.DiskID]*client.ShardNodeDiskInfo),
	}
}

func (m *migratingShardDisks) add(diskID proto.DiskID, disk *client.ShardNodeDiskInfo) {
	m.Lock()
	m.disks[diskID] = disk
	m.Unlock()
}

func (m *migratingShardDisks) delete(diskID proto.DiskID) {
	m.Lock()
	delete(m.disks, diskID)
	m.Unlock()
}

func (m *migratingShardDisks) get(diskID proto.DiskID) (disk *client.ShardNodeDiskInfo, exist bool) {
	m.RLock()
	disk, exist = m.disks[diskID]
	m.RUnlock()
	return
}

func (m *migratingShardDisks) list() (disks []*client.ShardNodeDiskInfo) {
	m.RLock()
	for _, disk := range m.disks {
		disks = append(disks, disk)
	}
	m.RUnlock()
	return
}

func (m *migratingShardDisks) size() (size int) {
	m.RLock()
	size = len(m.disks)
	m.RUnlock()
	return
}
