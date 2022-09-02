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
	"fmt"
	"sync"
	"time"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	prepareMigrateTaskInterval        = 1 * time.Second
	finishMigrateTaskInterval         = 1 * time.Second
	prepareTaskPause                  = 2 * time.Second
	clearJunkMigrationTaskInterval    = 1 * time.Hour
	junkMigrationTaskProtectionWindow = 1 * time.Hour
)

// MMigrator merged interfaces for mocking.
type MMigrator interface {
	IMigrator
	IDisKMigrator
	IManualMigrator
}

// Migrator base interface of migrate, balancer, disk_droper, manual_migrater.
type Migrator interface {
	AcquireTask(ctx context.Context, idc string) (proto.MigrateTask, error)
	CancelTask(ctx context.Context, args *api.OperateTaskArgs) error
	CompleteTask(ctx context.Context, args *api.OperateTaskArgs) error
	ReclaimTask(ctx context.Context, idc, taskID string,
		src []proto.VunitLocation, oldDst proto.VunitLocation, newDst *client.AllocVunitInfo) error
	RenewalTask(ctx context.Context, idc, taskID string) error
	QueryTask(ctx context.Context, taskID string) (*api.MigrateTaskDetail, error)
	// status
	ReportWorkerTaskStats(st *api.TaskReportArgs)
	StatQueueTaskCnt() (inited, prepared, completed int)
	Stats() api.MigrateTasksStat
	// control
	taskswitch.ISwitcher
	closer.Closer
	Load() error
	Run()
}

// IDisKMigrator base interface of disk migrate, such as disk repair and disk drop
type IDisKMigrator interface {
	Migrator
	Progress(ctx context.Context) (repairingDiskID proto.DiskID, total, repaired int)
}

// IManualMigrator interface of manual migrator
type IManualMigrator interface {
	Migrator
	AddManualTask(ctx context.Context, vuid proto.Vuid, forbiddenDirectDownload bool) (err error)
}

// IMigrator interface of common migrator
type IMigrator interface {
	Migrator
	// inner interface
	SetLockFailHandleFunc(lockFailHandleFunc func(ctx context.Context, task *proto.MigrateTask))
	GetMigratingDiskNum() int
	IsMigratingDisk(diskID proto.DiskID) bool
	ClearDeletedTasks(diskID proto.DiskID)
	IsDeletedTask(task *proto.MigrateTask) bool
	AddTask(ctx context.Context, task *proto.MigrateTask)
	GetTask(ctx context.Context, taskID string) (*proto.MigrateTask, error)
	ListAllTask(ctx context.Context) (tasks []*proto.MigrateTask, err error)
	ListAllTaskByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error)
	FinishTaskInAdvanceWhenLockFail(ctx context.Context, task *proto.MigrateTask)
}

// MigratingVuids record migrating vuid info
type MigratingVuids map[proto.Vuid]string

type diskMigratingVuids struct {
	vuids map[proto.DiskID]MigratingVuids
	lock  sync.RWMutex
}

func newDiskMigratingVuids() *diskMigratingVuids {
	return &diskMigratingVuids{
		vuids: make(map[proto.DiskID]MigratingVuids),
	}
}

func (m *diskMigratingVuids) addMigratingVuid(diskID proto.DiskID, vuid proto.Vuid, taskID string) {
	m.lock.Lock()
	if m.vuids[diskID] == nil {
		m.vuids[diskID] = make(MigratingVuids)
	}
	m.vuids[diskID][vuid] = taskID
	m.lock.Unlock()
}

func (m *diskMigratingVuids) deleteMigratingVuid(diskID proto.DiskID, vuid proto.Vuid) {
	m.lock.Lock()
	delete(m.vuids[diskID], vuid)
	if len(m.vuids[diskID]) == 0 {
		delete(m.vuids, diskID)
	}
	m.lock.Unlock()
}

func (m *diskMigratingVuids) getCurrMigratingDisksCnt() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.vuids)
}

func (m *diskMigratingVuids) isMigratingDisk(diskID proto.DiskID) (ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	_, ok = m.vuids[diskID]
	return
}

type migratedTasks map[string]struct{}

type diskMigratedTasks struct {
	tasks map[proto.DiskID]migratedTasks
	sync.RWMutex
}

func newDiskMigratedTasks() *diskMigratedTasks {
	return &diskMigratedTasks{
		tasks: make(map[proto.DiskID]migratedTasks),
	}
}

func (m *diskMigratedTasks) add(diskID proto.DiskID, taskID string) {
	m.Lock()
	if m.tasks[diskID] == nil {
		m.tasks[diskID] = make(migratedTasks)
	}
	m.tasks[diskID][taskID] = struct{}{}
	m.Unlock()
}

func (m *diskMigratedTasks) exits(diskID proto.DiskID, taskID string) (exist bool) {
	m.RLock()
	if _, ok := m.tasks[diskID]; ok {
		_, exist = m.tasks[diskID][taskID]
	}
	m.RUnlock()
	return exist
}

func (m *diskMigratedTasks) delete(diskID proto.DiskID) {
	m.Lock()
	delete(m.tasks, diskID)
	m.Unlock()
}

type migratedDisk struct {
	diskID       proto.DiskID
	finishedTime time.Time
}

type migratedDisks struct {
	disks map[proto.DiskID]time.Time
	sync.RWMutex
}

func newMigratedDisks() *migratedDisks {
	return &migratedDisks{
		disks: make(map[proto.DiskID]time.Time),
	}
}

func (m *migratedDisks) add(diskID proto.DiskID, finishedTime time.Time) {
	m.Lock()
	m.disks[diskID] = finishedTime
	m.Unlock()
}

func (m *migratedDisks) delete(diskID proto.DiskID) {
	m.Lock()
	delete(m.disks, diskID)
	m.Unlock()
}

func (m *migratedDisks) size() int {
	m.RLock()
	size := len(m.disks)
	m.RUnlock()
	return size
}

func (m *migratedDisks) list() (disks []*migratedDisk) {
	m.RLock()
	for k, v := range m.disks {
		disk := &migratedDisk{diskID: k, finishedTime: v}
		disks = append(disks, disk)
	}
	m.RUnlock()
	return disks
}

// MigrateConfig migrate config
type MigrateConfig struct {
	ClusterID proto.ClusterID `json:"-"` // fill in config.go
	base.TaskCommonConfig
}

// MigrateMgr migrate manager
type MigrateMgr struct {
	closer.Closer

	taskType           proto.TaskType
	diskMigratingVuids *diskMigratingVuids

	clusterMgrCli client.ClusterMgrAPI
	volumeUpdater client.IVolumeUpdater

	taskSwitch taskswitch.ISwitcher

	prepareQueue *base.TaskQueue       // store inited task
	workQueue    *base.WorkerTaskQueue // store prepared task
	finishQueue  *base.TaskQueue       // store completed task
	deletedTasks *diskMigratedTasks

	finishTaskCounter counter.Counter
	taskStatsMgr      *base.TaskStatsMgr

	cfg *MigrateConfig

	taskLogger recordlog.Encoder
	// handle func when lock volume fail
	lockFailHandleFunc func(ctx context.Context, task *proto.MigrateTask)
}

// NewMigrateMgr returns migrate manager
func NewMigrateMgr(
	clusterMgrCli client.ClusterMgrAPI,
	volumeUpdater client.IVolumeUpdater,
	taskSwitch taskswitch.ISwitcher,
	taskLogger recordlog.Encoder,
	conf *MigrateConfig,
	taskType proto.TaskType,
) *MigrateMgr {
	mgr := &MigrateMgr{
		taskType:           taskType,
		diskMigratingVuids: newDiskMigratingVuids(),

		taskSwitch: taskSwitch,

		clusterMgrCli: clusterMgrCli,
		volumeUpdater: volumeUpdater,

		prepareQueue: base.NewTaskQueue(time.Duration(conf.PrepareQueueRetryDelayS) * time.Second),
		workQueue:    base.NewWorkerTaskQueue(time.Duration(conf.CancelPunishDurationS) * time.Second),
		finishQueue:  base.NewTaskQueue(time.Duration(conf.FinishQueueRetryDelayS) * time.Second),
		deletedTasks: newDiskMigratedTasks(),

		cfg:        conf,
		taskLogger: taskLogger,

		Closer: closer.New(),
	}

	mgr.taskStatsMgr = base.NewTaskStatsMgrAndRun(conf.ClusterID, taskType, mgr)
	return mgr
}

// SetLockFailHandleFunc set lock failed func
func (mgr *MigrateMgr) SetLockFailHandleFunc(lockFailHandleFunc func(ctx context.Context, task *proto.MigrateTask)) {
	mgr.lockFailHandleFunc = lockFailHandleFunc
}

// Load load migrate task from database
func (mgr *MigrateMgr) Load() (err error) {
	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("start load migrate task: task_type[%s]", mgr.taskType)
	// load task
	tasks, err := mgr.clusterMgrCli.ListAllMigrateTasks(ctx, mgr.taskType)
	if err != nil {
		span.Errorf("find all tasks failed: err[%+v]", err)
		return
	}
	span.Infof("load task success: task_type[%s], tasks len[%d]", mgr.taskType, len(tasks))

	for i := range tasks {
		if tasks[i].Running() {
			err = base.VolTaskLockerInst().TryLock(ctx, tasks[i].SourceVuid.Vid())
			if err != nil {
				return fmt.Errorf("migrate task conflict: vid[%d], task[%+v], err[%+v]",
					tasks[i].SourceVuid.Vid(), tasks[i], err.Error())
			}
		}

		mgr.addMigratingVuid(tasks[i].SourceDiskID, tasks[i].SourceVuid, tasks[i].TaskID)

		span.Infof("load task success: task_type[%s], task_id[%s], state[%d]", mgr.taskType, tasks[i].TaskID, tasks[i].State)
		switch tasks[i].State {
		case proto.MigrateStateInited:
			mgr.prepareQueue.PushTask(tasks[i].TaskID, tasks[i])
		case proto.MigrateStatePrepared:
			mgr.workQueue.AddPreparedTask(tasks[i].SourceIDC, tasks[i].TaskID, tasks[i])
		case proto.MigrateStateWorkCompleted:
			mgr.finishQueue.PushTask(tasks[i].TaskID, tasks[i])
		case proto.MigrateStateFinished, proto.MigrateStateFinishedInAdvance:
			return fmt.Errorf("task should be deleted from db: task[%+v]", tasks[i])
		default:
			return fmt.Errorf("unexpect migrate state: task[%+v]", tasks[i])
		}
	}
	return
}

// Run run migrate task do prepare and finish task phase
func (mgr *MigrateMgr) Run() {
	go mgr.prepareTaskLoop()
	go mgr.finishTaskLoop()
}

func (mgr *MigrateMgr) prepareTaskLoop() {
	for {
		mgr.taskSwitch.WaitEnable()
		todo, doing := mgr.workQueue.StatsTasks()
		if todo+doing >= mgr.cfg.WorkQueueSize {
			time.Sleep(prepareTaskPause)
			continue
		}
		err := mgr.prepareTask()
		if err == base.ErrNoTaskInQueue {
			log.Debug("no task in prepare queue and sleep")
			time.Sleep(prepareMigrateTaskInterval)
		}
	}
}

func (mgr *MigrateMgr) prepareTask() (err error) {
	_, task, exist := mgr.prepareQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "migrate.prepareTask")
	defer span.Finish()

	defer func() {
		if err != nil {
			mgr.prepareQueue.RetryTask(task.(*proto.MigrateTask).TaskID)
		}
	}()

	migTask := task.(*proto.MigrateTask).Copy()

	span.Infof("prepare task phase: task_id[%s], state[%+v]", migTask.TaskID, migTask.State)

	err = base.VolTaskLockerInst().TryLock(ctx, migTask.SourceVuid.Vid())
	if err != nil {
		span.Warnf("lock volume failed: volume_id[%v], err[%+v]", migTask.SourceVuid.Vid(), err)
		return base.ErrVolNotOnlyOneTask
	}
	defer func() {
		if err != nil {
			base.VolTaskLockerInst().Unlock(ctx, task.(*proto.MigrateTask).SourceVuid.Vid())
		}
	}()

	volInfo, err := mgr.clusterMgrCli.GetVolumeInfo(ctx, migTask.SourceVuid.Vid())
	if err != nil {
		span.Errorf("prepare task failed: err[%v]", err)
		return err
	}

	// check necessity of generating current task
	if migTask.SourceVuid != volInfo.VunitLocations[migTask.SourceVuid.Index()].Vuid {
		span.Infof("the source unit has been moved and finish task immediately: task_id[%s], task source vuid[%v], current vuid[%v]",
			migTask.TaskID, migTask.SourceVuid, volInfo.VunitLocations[migTask.SourceVuid.Index()].Vuid)

		// volume may be locked, try unlock the volume
		// for example
		// 1. lock volume success
		// 2. alloc chunk failed and VolTaskLockerInst().Unlock
		// 3. this volume maybe execute other tasks, such as disk repair
		// 4. then enter this branch and volume status is locked
		err := mgr.clusterMgrCli.UnlockVolume(ctx, migTask.SourceVuid.Vid())
		if err != nil {
			span.Errorf("before finish in advance try unlock volume failed: vid[%d], err[%+v]",
				migTask.SourceVuid.Vid(), err)
			return err
		}

		mgr.finishTaskInAdvance(ctx, migTask, "volume has migrated")
		return nil
	}

	// lock volume
	err = mgr.clusterMgrCli.LockVolume(ctx, migTask.SourceVuid.Vid())
	if err != nil {
		if rpc.DetectStatusCode(err) == errors.CodeLockNotAllow && mgr.lockFailHandleFunc != nil {
			mgr.lockFailHandleFunc(ctx, migTask)
			return nil
		}
		span.Errorf("lock volume failed: volume_id[%v], err[%+v]", migTask.SourceVuid.Vid(), err)
		return err
	}

	// alloc volume unit
	ret, err := base.AllocVunitSafe(ctx, mgr.clusterMgrCli, migTask.SourceVuid, migTask.Sources)
	if err != nil {
		span.Errorf("alloc volume unit failed: err[%+v]", err)
		return
	}

	migTask.CodeMode = volInfo.CodeMode
	migTask.Sources = volInfo.VunitLocations
	migTask.SetDestination(ret.Location())
	migTask.State = proto.MigrateStatePrepared

	// update db
	base.InsistOn(ctx, "migrate prepare task update task tbl", func() error {
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, migTask)
	})

	// send task to worker queue and remove task in prepareQueue
	mgr.workQueue.AddPreparedTask(migTask.SourceIDC, migTask.TaskID, migTask)
	mgr.prepareQueue.RemoveTask(migTask.TaskID)

	span.Infof("prepare task success: task_id[%s], state[%v]", migTask.TaskID, migTask.State)
	return
}

func (mgr *MigrateMgr) finishTaskLoop() {
	for {
		mgr.taskSwitch.WaitEnable()
		err := mgr.finishTask()
		if err == base.ErrNoTaskInQueue {
			log.Debug("no task in finish queue and sleep")
			time.Sleep(finishMigrateTaskInterval)
		}
	}
}

func (mgr *MigrateMgr) finishTask() (err error) {
	_, task, exist := mgr.finishQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "migrate.finishTask")
	defer span.Finish()

	defer func() {
		if err != nil {
			mgr.finishQueue.RetryTask(task.(*proto.MigrateTask).TaskID)
		}
	}()

	migrateTask := task.(*proto.MigrateTask).Copy()
	span.Infof("finish task phase: task_id[%s], state[%v]", migrateTask.TaskID, migrateTask.State)

	if migrateTask.State != proto.MigrateStateWorkCompleted {
		span.Panicf("unexpect task state: task_id[%s], expect state[%d], actual state[%d]", proto.MigrateStateWorkCompleted, migrateTask.State)
	}

	// because competed task did not persisted to the database, so in finish phase need to do it
	// the task maybe update more than once, which is allowed
	base.InsistOn(ctx, "migrate finish task update task tbl to state completed ", func() error {
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, migrateTask)
	})

	// update volume mapping relationship
	err = mgr.clusterMgrCli.UpdateVolume(ctx, migrateTask.Destination.Vuid, migrateTask.SourceVuid, migrateTask.DestinationDiskID())
	if err != nil {
		span.Errorf("change volume unit relationship failed: old vuid[%d], new vuid[%d], new diskId[%d], err[%+v]",
			migrateTask.SourceVuid,
			migrateTask.Destination.Vuid,
			migrateTask.DestinationDiskID(),
			err)
		return mgr.handleUpdateVolMappingFail(ctx, migrateTask, err)
	}

	err = mgr.clusterMgrCli.ReleaseVolumeUnit(ctx, migrateTask.SourceVuid, migrateTask.SourceDiskID)
	if err != nil {
		span.Errorf("release volume unit failed: err[%+v]", err)
		// 1. CodeVuidNotFound means the volume unit dose not exist and ignore it
		// 2. CodeDiskBroken need ignore it
		// 3. Other err, all scheduler need to be notified to update the volume mapping relationship
		// to avoid affecting the deletion process due to caching the old mapping relationship.
		// If the update is successful, continue with the following process, and return err it fails.
		httpCode := rpc.DetectStatusCode(err)
		if httpCode != errors.CodeVuidNotFound && httpCode != errors.CodeDiskBroken {
			err = mgr.updateVolumeCache(ctx, migrateTask)
			if err != nil {
				return base.ErrUpdateVolumeCache
			}
		}
		err = nil
	}

	err = mgr.clusterMgrCli.UnlockVolume(ctx, migrateTask.SourceVuid.Vid())
	if err != nil {
		span.Errorf("unlock volume failed: err[%+v]", err)
		return
	}
	// remove task from clustermgr
	migrateTask.State = proto.MigrateStateFinished
	base.InsistOn(ctx, "migrate finish task update task tbl", func() error {
		return mgr.clusterMgrCli.DeleteMigrateTask(ctx, migrateTask.TaskID)
	})

	if recordErr := mgr.taskLogger.Encode(migrateTask); recordErr != nil {
		span.Errorf("record migrate task failed: task[%+v], err[%+v]", migrateTask, recordErr)
	}

	mgr.finishQueue.RemoveTask(migrateTask.TaskID)

	base.VolTaskLockerInst().Unlock(ctx, migrateTask.SourceVuid.Vid())
	mgr.deleteMigratingVuid(migrateTask.SourceDiskID, migrateTask.SourceVuid)

	mgr.finishTaskCounter.Add()

	// add delete task and check it again
	mgr.addDeletedTask(migrateTask)
	span.Infof("finish task phase success: task_id[%s], state[%v]", migrateTask.TaskID, migrateTask.State)
	return
}

func (mgr *MigrateMgr) updateVolumeCache(ctx context.Context, task *proto.MigrateTask) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("update volume cache: vid[%d], task_id[%s]", task.SourceVuid.Vid(), task.TaskID)
	return mgr.volumeUpdater.UpdateLeaderVolumeCache(ctx, task.SourceVuid.Vid())
}

// AddTask adds migrate task
func (mgr *MigrateMgr) AddTask(ctx context.Context, task *proto.MigrateTask) {
	// add task to db
	base.InsistOn(ctx, "migrate add task insert task to tbl", func() error {
		return mgr.clusterMgrCli.AddMigrateTask(ctx, task)
	})

	// add task to prepare queue
	mgr.prepareQueue.PushTask(task.TaskID, task)

	mgr.addMigratingVuid(task.SourceDiskID, task.SourceVuid, task.TaskID)
}

// FinishTaskInAdvanceWhenLockFail finish migrate task in advance when lock volume failed
func (mgr *MigrateMgr) FinishTaskInAdvanceWhenLockFail(ctx context.Context, task *proto.MigrateTask) {
	mgr.finishTaskInAdvance(ctx, task, "lock volume fail")
}

func (mgr *MigrateMgr) finishTaskInAdvance(ctx context.Context, task *proto.MigrateTask, reason string) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("finish task in advance: task_id[%s], task[%+v]", task.TaskID, task)

	task.State = proto.MigrateStateFinishedInAdvance
	task.FinishAdvanceReason = reason

	base.InsistOn(ctx, "migrate finish task in advance update tbl", func() error {
		return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
	})
	if recordErr := mgr.taskLogger.Encode(task); recordErr != nil {
		span.Errorf("record migrate task failed: task[%+v], err[%+v]", task, recordErr)
	}

	mgr.finishTaskCounter.Add()
	mgr.prepareQueue.RemoveTask(task.TaskID)
	mgr.addDeletedTask(task)
	base.VolTaskLockerInst().Unlock(ctx, task.SourceVuid.Vid())
}

func (mgr *MigrateMgr) handleUpdateVolMappingFail(ctx context.Context, task *proto.MigrateTask, err error) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("handle update vol mapping failed: task_id[%s], state[%d], dest vuid[%d]", task.TaskID, task.State, task.Destination.Vuid)

	code := rpc.DetectStatusCode(err)
	if code == errors.CodeOldVuidNotMatch {
		span.Panicf("change volume unit relationship failed: old vuid not match")
	}

	if base.ShouldAllocAndRedo(code) {
		span.Infof("realloc vunit and redo: task_id[%s]", task.TaskID)
		newVunit, err := base.AllocVunitSafe(ctx, mgr.clusterMgrCli, task.SourceVuid, task.Sources)
		if err != nil {
			span.Errorf("realloc failed: vuid[%d], err[%+v]", task.SourceVuid, err)
			return err
		}
		task.SetDestination(newVunit.Location())
		task.State = proto.MigrateStatePrepared
		task.WorkerRedoCnt++

		base.InsistOn(ctx, "migrate redo task update task tbl", func() error {
			return mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
		})

		mgr.finishQueue.RemoveTask(task.TaskID)
		mgr.workQueue.AddPreparedTask(task.SourceIDC, task.TaskID, task)
		span.Infof("task %+v redo again", task)

		return nil
	}

	return err
}

// ClearDeletedTasks clear tasks when disk is migrated
func (mgr *MigrateMgr) ClearDeletedTasks(diskID proto.DiskID) {
	switch mgr.taskType {
	case proto.TaskTypeDiskDrop: // only disk drop task need to add
		mgr.deletedTasks.delete(diskID)
	}
}

// IsDeletedTask return true if the task is deleted
func (mgr *MigrateMgr) IsDeletedTask(task *proto.MigrateTask) bool {
	return mgr.deletedTasks.exits(task.SourceDiskID, task.TaskID)
}

func (mgr *MigrateMgr) addMigratingVuid(diskID proto.DiskID, vuid proto.Vuid, taskID string) {
	switch mgr.taskType {
	case proto.TaskTypeBalance: // only balance task need to add
		mgr.diskMigratingVuids.addMigratingVuid(diskID, vuid, taskID)
	}
}

func (mgr *MigrateMgr) deleteMigratingVuid(diskID proto.DiskID, vuid proto.Vuid) {
	switch mgr.taskType {
	case proto.TaskTypeBalance: // only balance task need to add
		mgr.diskMigratingVuids.deleteMigratingVuid(diskID, vuid)
	}
}

func (mgr *MigrateMgr) addDeletedTask(task *proto.MigrateTask) {
	switch mgr.taskType {
	case proto.TaskTypeDiskDrop: // only disk drop task need to add
		mgr.deletedTasks.add(task.SourceDiskID, task.TaskID)
	}
}

// StatQueueTaskCnt returns queue task count
func (mgr *MigrateMgr) StatQueueTaskCnt() (inited, prepared, completed int) {
	todo, doing := mgr.prepareQueue.StatsTasks()
	inited = todo + doing

	todo, doing = mgr.workQueue.StatsTasks()
	prepared = todo + doing

	todo, doing = mgr.finishQueue.StatsTasks()
	completed = todo + doing
	return
}

// Stats implement migrator
func (mgr *MigrateMgr) Stats() api.MigrateTasksStat {
	preparing, workerDoing, finishing := mgr.StatQueueTaskCnt()
	finishedCnt := mgr.finishTaskCounter.Show()
	increaseDataSize, increaseShardCnt := mgr.taskStatsMgr.Counters()
	return api.MigrateTasksStat{
		PreparingCnt:   preparing,
		WorkerDoingCnt: workerDoing,
		FinishingCnt:   finishing,
		StatsPerMin: api.PerMinStats{
			FinishedCnt:    fmt.Sprint(finishedCnt),
			DataAmountByte: base.DataMountFormat(increaseDataSize),
			ShardCnt:       fmt.Sprint(increaseShardCnt),
		},
	}
}

// AcquireTask acquire migrate task
func (mgr *MigrateMgr) AcquireTask(ctx context.Context, idc string) (task proto.MigrateTask, err error) {
	span := trace.SpanFromContextSafe(ctx)

	if !mgr.taskSwitch.Enabled() {
		return task, proto.ErrTaskPaused
	}

	_, migTask, _ := mgr.workQueue.Acquire(idc)
	if migTask != nil {
		task = *migTask.(*proto.MigrateTask)
		span.Infof("acquire %s taskId: %s", mgr.taskType, task.TaskID)
		return task, nil
	}
	return task, proto.ErrTaskEmpty
}

// CancelTask cancel migrate task
func (mgr *MigrateMgr) CancelTask(ctx context.Context, args *api.OperateTaskArgs) (err error) {
	mgr.taskStatsMgr.CancelTask()

	err = mgr.workQueue.Cancel(args.IDC, args.TaskID, args.Src, args.Dest)
	if err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Errorf("cancel migrate failed: task_type[%s], task_id[%s], err[%+v]", mgr.taskType, args.TaskID, err)
	}
	return
}

// ReclaimTask reclaim migrate task
func (mgr *MigrateMgr) ReclaimTask(ctx context.Context, idc, taskID string,
	src []proto.VunitLocation, oldDst proto.VunitLocation, newDst *client.AllocVunitInfo) (err error) {
	mgr.taskStatsMgr.ReclaimTask()

	span := trace.SpanFromContextSafe(ctx)
	err = mgr.workQueue.Reclaim(idc, taskID, src, oldDst, newDst.Location(), newDst.DiskID)
	if err != nil {
		span.Errorf("reclaim migrate task failed: task_type:[%s],task_id[%s], err[%+v]", mgr.taskType, taskID, err)
		return err
	}

	task, err := mgr.workQueue.Query(idc, taskID)
	if err != nil {
		span.Errorf("found task in workQueue failed: idc[%s], task_id[%s], err[%+v]", idc, taskID, err)
		return err
	}
	err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, task.(*proto.MigrateTask))

	if err != nil {
		span.Errorf("update reclaim task failed: task_id[%s], err[%+v]", taskID, err)
	}
	return
}

// CompleteTask complete migrate task
func (mgr *MigrateMgr) CompleteTask(ctx context.Context, args *api.OperateTaskArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	completeTask, err := mgr.workQueue.Complete(args.IDC, args.TaskID, args.Src, args.Dest)
	if err != nil {
		span.Errorf("complete migrate task failed: task_id[%s], err[%+v]", args.TaskID, err)
		return err
	}

	t := completeTask.(*proto.MigrateTask)
	t.State = proto.MigrateStateWorkCompleted

	err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, t)
	if err != nil {
		// there is no impact if we failed to update task state in db,
		// because we will do it in finishTask again, so assume complete success
		span.Errorf("complete migrate task into db failed: task_id[%s], err[%+v]", t.TaskID, err)
		err = nil
	}
	mgr.finishQueue.PushTask(args.TaskID, t)
	return
}

// RenewalTask renewal migrate task
func (mgr *MigrateMgr) RenewalTask(ctx context.Context, idc, taskID string) (err error) {
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

// IsMigratingDisk returns true if disk is migrating
func (mgr *MigrateMgr) IsMigratingDisk(diskID proto.DiskID) bool {
	return mgr.diskMigratingVuids.isMigratingDisk(diskID)
}

// GetMigratingDiskNum returns migrating disk count
func (mgr *MigrateMgr) GetMigratingDiskNum() int {
	return mgr.diskMigratingVuids.getCurrMigratingDisksCnt()
}

// ListAllTask returns all migrate task
func (mgr *MigrateMgr) ListAllTask(ctx context.Context) (tasks []*proto.MigrateTask, err error) {
	return mgr.clusterMgrCli.ListAllMigrateTasks(ctx, mgr.taskType)
}

// ListAllTaskByDiskID return all task by diskID
func (mgr *MigrateMgr) ListAllTaskByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error) {
	return mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, mgr.taskType, diskID)
}

// GetTask returns task in db
func (mgr *MigrateMgr) GetTask(ctx context.Context, taskID string) (*proto.MigrateTask, error) {
	return mgr.clusterMgrCli.GetMigrateTask(ctx, mgr.taskType, taskID)
}

// QueryTask implement migrator
func (mgr *MigrateMgr) QueryTask(ctx context.Context, taskID string) (*api.MigrateTaskDetail, error) {
	detail := &api.MigrateTaskDetail{}
	taskInfo, err := mgr.GetTask(ctx, taskID)
	if err != nil {
		return detail, err
	}
	detail.Task = *taskInfo

	detailRunInfo, err := mgr.taskStatsMgr.QueryTaskDetail(taskID)
	if err != nil {
		return detail, nil
	}
	detail.Stat = detailRunInfo.Statistics
	return detail, nil
}

// ReportWorkerTaskStats implement migrator
func (mgr *MigrateMgr) ReportWorkerTaskStats(st *api.TaskReportArgs) {
	mgr.taskStatsMgr.ReportWorkerTaskStats(st.TaskID, st.TaskStats, st.IncreaseDataSizeByte, st.IncreaseShardCnt)
}

// Enabled returns enable or not.
func (mgr *MigrateMgr) Enabled() bool {
	return mgr.taskSwitch.Enabled()
}

// WaitEnable block to wait enable.
func (mgr *MigrateMgr) WaitEnable() {
	mgr.taskSwitch.WaitEnable()
}
