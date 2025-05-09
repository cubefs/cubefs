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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/closer"
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

var (
	_ BaseMigrator = (*DiskRepairMgr)(nil)
	_ BaseMigrator = (*MigrateMgr)(nil)
	_ BaseMigrator = (*ShardMigrateMgr)(nil)
)

// BaseMigrator base interface for shard and blobnode task.
type BaseMigrator interface {
	AcquireTask(ctx context.Context, idc string) (*proto.Task, error)
	RenewalTask(ctx context.Context, idc, taskID string) error
	CancelTask(ctx context.Context, args *api.TaskArgs) error
	CompleteTask(ctx context.Context, args *api.TaskArgs) error
	ReclaimTask(ctx context.Context, args *api.TaskArgs) error
	ReportTask(ctx context.Context, args *api.TaskArgs) (err error)
	QueryTask(ctx context.Context, taskID string) (*api.TaskRet, error)
}

// Migrator interface of blobnode migrate, balancer, disk_dropper, manual_migrator.
type Migrator interface {
	BaseMigrator
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
	Progress(ctx context.Context) (migratingDisks []proto.DiskID, total, migrated int)
	DiskProgress(ctx context.Context, diskID proto.DiskID) (stats *api.DiskMigratingStats, err error)
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
	GetMigratingDiskNum() int
	IsMigratingDisk(diskID proto.DiskID) bool
	ClearDeletedTasks(diskID proto.DiskID)
	ClearDeletedTaskByID(diskID proto.DiskID, taskID string)
	IsDeletedTask(task *proto.MigrateTask) bool
	DeletedTasks() []DeletedTask
	AddTask(ctx context.Context, task *proto.MigrateTask)
	GetTask(ctx context.Context, taskID string) (*proto.MigrateTask, error)
	ListAllTask(ctx context.Context) (tasks []*proto.MigrateTask, err error)
	ListAllTaskByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error)
}

type migratingDisks struct {
	disks map[proto.DiskID]*client.DiskInfoSimple
	sync.RWMutex
}

func newMigratingDisks() *migratingDisks {
	return &migratingDisks{
		disks: make(map[proto.DiskID]*client.DiskInfoSimple),
	}
}

func (m *migratingDisks) add(diskID proto.DiskID, disk *client.DiskInfoSimple) {
	m.Lock()
	m.disks[diskID] = disk
	m.Unlock()
}

func (m *migratingDisks) delete(diskID proto.DiskID) {
	m.Lock()
	delete(m.disks, diskID)
	m.Unlock()
}

func (m *migratingDisks) get(diskID proto.DiskID) (disk *client.DiskInfoSimple, exist bool) {
	m.RLock()
	disk, exist = m.disks[diskID]
	m.RUnlock()
	return
}

func (m *migratingDisks) list() (disks []*client.DiskInfoSimple) {
	m.RLock()
	for _, disk := range m.disks {
		disks = append(disks, disk)
	}
	m.RUnlock()
	return
}

func (m *migratingDisks) size() (size int) {
	m.RLock()
	size = len(m.disks)
	m.RUnlock()
	return
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

type migratedTasks map[string]time.Time

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
	m.tasks[diskID][taskID] = time.Now()
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

func (m *diskMigratedTasks) deleteByID(diskID proto.DiskID, taskID string) {
	m.Lock()
	tasks := m.tasks[diskID]
	delete(tasks, taskID)
	if len(tasks) == 0 {
		delete(m.tasks, diskID)
	}
	m.Unlock()
}

type DeletedTask struct {
	DiskID      proto.DiskID
	TaskID      string
	DeletedTime time.Time
}

func (m *diskMigratedTasks) list() (tasks []DeletedTask) {
	m.RLock()
	for diskID, deletedTasks := range m.tasks {
		for taskID, deletedTime := range deletedTasks {
			tasks = append(tasks, DeletedTask{
				DiskID:      diskID,
				TaskID:      taskID,
				DeletedTime: deletedTime,
			})
		}
	}
	m.RUnlock()
	return tasks
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

	lockFailHandleFunc lockFailFunc
	// clear junk tasks
	clearJunkTasksWhenLoadingFunc clearJunkTasksFunc
	// finish drop task
	finishTaskCallback taskLimitFunc
	// load drop task
	loadTaskCallback taskLimitFunc
}

type clearJunkTasksFunc func(ctx context.Context, tasks []*proto.MigrateTask) error

var defaultClearJunkTasksFunc = func(ctx context.Context, tasks []*proto.MigrateTask) error {
	return nil
}

type taskLimitFunc func(diskId proto.DiskID)

type lockFailFunc func(ctx context.Context, task *proto.MigrateTask) error

var defaultDiskTaskLimitFunc = func(diskId proto.DiskID) {
	_ = struct{}{}
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

	lockVolFailHandleFunc lockFailFunc
	// clear junk tasks
	clearJunkTasksCallBack clearJunkTasksFunc
	// load and finish drop task
	finishTaskCallback, loadTaskCallback taskLimitFunc
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
	checkMigrateConf(conf)
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

		clearJunkTasksCallBack: conf.clearJunkTasksWhenLoadingFunc,
		finishTaskCallback:     conf.finishTaskCallback,
		loadTaskCallback:       conf.loadTaskCallback,
		lockVolFailHandleFunc:  conf.lockFailHandleFunc,

		Closer: closer.New(),
	}
	if mgr.lockVolFailHandleFunc == nil {
		mgr.lockVolFailHandleFunc = mgr.handleLockVolFail
	}
	mgr.taskStatsMgr = base.NewTaskStatsMgrAndRun(conf.ClusterID, taskType, mgr)
	return mgr
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

	if len(tasks) == 0 {
		return
	}

	disks, err := mgr.listMigratingDisks(ctx)
	if err != nil {
		return
	}
	var junkTasks []*proto.MigrateTask
	for i := range tasks {
		task := &proto.MigrateTask{}
		err = task.Unmarshal(tasks[i].Data)
		if err != nil {
			return err
		}
		if mgr.isJunkTask(disks, task) {
			junkTasks = append(junkTasks, task)
			continue
		}
		if task.Running() {
			err = base.VolTaskLockerInst().TryLock(ctx, uint32(task.SourceVuid.Vid()))
			if err != nil {
				return fmt.Errorf("migrate task conflict: vid[%d], task[%+v], err[%+v]",
					task.SourceVuid.Vid(), tasks[i], err)
			}
		}

		mgr.loadTaskCallback(task.SourceDiskID)
		mgr.addMigratingVuid(task.SourceDiskID, task.SourceVuid, task.TaskID)

		span.Infof("load task success: task_type[%s], task_id[%s], state[%d]", mgr.taskType, task.TaskID, task.State)
		switch task.State {
		case proto.MigrateStateInited:
			mgr.prepareQueue.PushTask(task.TaskID, task)
		case proto.MigrateStatePrepared:
			mgr.workQueue.AddPreparedTask(task.SourceIDC, task.TaskID, task)
		case proto.MigrateStateWorkCompleted:
			mgr.finishQueue.PushTask(task.TaskID, task)
		case proto.MigrateStateFinished, proto.MigrateStateFinishedInAdvance:
			return fmt.Errorf("task should be deleted from db: task[%+v]", task)
		default:
			return fmt.Errorf("unexpect migrate state: task[%+v]", task)
		}
	}
	return mgr.clearJunkTasksCallBack(ctx, junkTasks)
}

func (mgr *MigrateMgr) isJunkTask(disks *migratingDisks, task *proto.MigrateTask) bool {
	switch mgr.taskType {
	case proto.TaskTypeDiskDrop:
		if _, ok := disks.get(task.SourceDiskID); !ok {
			return true
		}
		return false
	default:
		return false
	}
}

func (mgr *MigrateMgr) listMigratingDisks(ctx context.Context) (*migratingDisks, error) {
	switch mgr.taskType {
	case proto.TaskTypeDiskDrop:
		migratingDisks, err := mgr.clusterMgrCli.ListDropDisks(ctx)
		if err != nil {
			return nil, err
		}
		disks := newMigratingDisks()
		for _, disk := range migratingDisks {
			disks.add(disk.DiskID, disk)
		}
		return disks, nil
	default:
		return nil, nil
	}
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
		if errors.Is(err, base.ErrNoTaskInQueue) {
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

	err = base.VolTaskLockerInst().TryLock(ctx, uint32(migTask.SourceVuid.Vid()))
	if err != nil {
		span.Warnf("lock volume failed: volume_id[%v], err[%+v]", migTask.SourceVuid.Vid(), err)
		return base.ErrVolNotOnlyOneTask
	}
	defer func() {
		if err != nil {
			base.VolTaskLockerInst().Unlock(ctx, uint32(task.(*proto.MigrateTask).SourceVuid.Vid()))
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
		if rpc.DetectStatusCode(err) == errcode.CodeLockNotAllow {
			// disk drop lockVolFailHandleFunc is nil, and can not finished in advance
			return mgr.lockVolFailHandleFunc(ctx, migTask)
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
		task, err := migTask.Task()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
	})

	// send task to worker queue and remove task in prepareQueue
	mgr.workQueue.AddPreparedTask(migTask.SourceIDC, migTask.TaskID, migTask)
	_ = mgr.prepareQueue.RemoveTask(migTask.TaskID)

	span.Infof("prepare task success: task_id[%s], state[%v]", migTask.TaskID, migTask.State)
	return
}

func (mgr *MigrateMgr) finishTaskLoop() {
	for {
		err := mgr.finishTask()
		if errors.Is(err, base.ErrNoTaskInQueue) {
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
		task, err := migrateTask.Task()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
	})

	// update volume mapping relationship
	err = mgr.clusterMgrCli.UpdateVolume(ctx, migrateTask.Destination.Vuid, migrateTask.SourceVuid, migrateTask.DestinationDiskID())
	if err != nil {
		info, err_ := mgr.clusterMgrCli.GetVolumeInfo(ctx, migrateTask.Vid())
		if err_ != nil {
			span.Errorf("task[%s] get volume[%d] info from clustermgr failed, err: %s",
				migrateTask.TaskID, migrateTask.Vid(), err_)
			return err_
		}
		idx := migrateTask.SourceVuid.Index()
		if info.VunitLocations[idx] != migrateTask.Destination {
			span.Errorf("change volume unit relationship failed: old vuid[%d], new vuid[%d], new diskId[%d], err[%+v]",
				migrateTask.SourceVuid,
				migrateTask.Destination.Vuid,
				migrateTask.DestinationDiskID(),
				err)
			err = mgr.handleUpdateVolMappingFail(ctx, migrateTask, err)
			return
		}
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
		if httpCode != errcode.CodeVuidNotFound && httpCode != errcode.CodeDiskBroken {
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

	_ = mgr.finishQueue.RemoveTask(migrateTask.TaskID)
	_ = mgr.updateVolumeCache(ctx, migrateTask)

	base.VolTaskLockerInst().Unlock(ctx, uint32(migrateTask.SourceVuid.Vid()))
	mgr.deleteMigratingVuid(migrateTask.SourceDiskID, migrateTask.SourceVuid)

	mgr.finishTaskCounter.Add()

	// add delete task and check it again
	mgr.addDeletedTask(migrateTask)
	mgr.finishTaskCallback(migrateTask.SourceDiskID)

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
		t, err := task.Task()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.AddMigrateTask(ctx, t)
	})

	// add task to prepare queue
	mgr.prepareQueue.PushTask(task.TaskID, task)

	mgr.addMigratingVuid(task.SourceDiskID, task.SourceVuid, task.TaskID)
}

func (mgr *MigrateMgr) handleLockVolFail(ctx context.Context, task *proto.MigrateTask) error {
	mgr.finishTaskInAdvance(ctx, task, "lock volume fail")
	return nil
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
	_ = mgr.prepareQueue.RemoveTask(task.TaskID)
	mgr.addDeletedTask(task)

	mgr.finishTaskCallback(task.SourceDiskID)

	base.VolTaskLockerInst().Unlock(ctx, uint32(task.SourceVuid.Vid()))
}

func (mgr *MigrateMgr) handleUpdateVolMappingFail(ctx context.Context, task *proto.MigrateTask, err error) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("handle update vol mapping failed: task_id[%s], state[%d], dest vuid[%d]", task.TaskID, task.State, task.Destination.Vuid)

	code := rpc.DetectStatusCode(err)
	if code == errcode.CodeOldVuidNotMatch {
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
			t, err := task.Task()
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

// ClearDeletedTasks clear tasks when disk is migrated
func (mgr *MigrateMgr) ClearDeletedTasks(diskID proto.DiskID) {
	switch mgr.taskType {
	case proto.TaskTypeDiskDrop: // only disk drop task need to clear by disk
		mgr.deletedTasks.delete(diskID)
	default:
	}
}

// ClearDeletedTaskByID clear migrated task
func (mgr *MigrateMgr) ClearDeletedTaskByID(diskID proto.DiskID, taskID string) {
	switch mgr.taskType {
	case proto.TaskTypeBalance: // only balance task need to clear by id
		mgr.deletedTasks.deleteByID(diskID, taskID)
	default:
	}
}

// IsDeletedTask return true if the task is deleted
func (mgr *MigrateMgr) IsDeletedTask(task *proto.MigrateTask) bool {
	return mgr.deletedTasks.exits(task.SourceDiskID, task.TaskID)
}

// DeletedTasks returns deleted tasks
func (mgr *MigrateMgr) DeletedTasks() []DeletedTask {
	return mgr.deletedTasks.list()
}

func (mgr *MigrateMgr) addMigratingVuid(diskID proto.DiskID, vuid proto.Vuid, taskID string) {
	switch mgr.taskType {
	case proto.TaskTypeBalance: // only balance task need to add
		mgr.diskMigratingVuids.addMigratingVuid(diskID, vuid, taskID)
	default:
	}
}

func (mgr *MigrateMgr) deleteMigratingVuid(diskID proto.DiskID, vuid proto.Vuid) {
	switch mgr.taskType {
	case proto.TaskTypeBalance: // only balance task need to add
		mgr.diskMigratingVuids.deleteMigratingVuid(diskID, vuid)
	default:
	}
}

func (mgr *MigrateMgr) addDeletedTask(task *proto.MigrateTask) {
	switch mgr.taskType {
	case proto.TaskTypeDiskDrop, proto.TaskTypeBalance: // only disk drop and balance task need to add
		mgr.deletedTasks.add(task.SourceDiskID, task.TaskID)
	default:
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
func (mgr *MigrateMgr) AcquireTask(ctx context.Context, idc string) (task *proto.Task, err error) {
	span := trace.SpanFromContextSafe(ctx)
	task = &proto.Task{}
	task.ModuleType = proto.TypeBlobNode
	if !mgr.taskSwitch.Enabled() {
		return task, proto.ErrTaskPaused
	}

	_, migTask, _ := mgr.workQueue.Acquire(idc)
	if migTask != nil {
		t := *migTask.(*proto.MigrateTask)
		data, err := t.Marshal()
		if err != nil {
			return task, err
		}
		task.TaskID = t.TaskID
		task.Data = data
		span.Infof("acquire %s taskId: %s", mgr.taskType, t.TaskID)
		return task, nil
	}
	return task, proto.ErrTaskEmpty
}

// CancelTask cancel migrate task
func (mgr *MigrateMgr) CancelTask(ctx context.Context, args *api.TaskArgs) (err error) {
	mgr.taskStatsMgr.CancelTask()

	arg := &api.OperateTaskArgs{}
	err = arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}

	if !client.ValidMigrateTask(args.TaskType, arg.TaskID) {
		return errcode.ErrIllegalArguments
	}

	err = mgr.workQueue.Cancel(arg.IDC, arg.TaskID, arg.Src, arg.Dest)
	if err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Errorf("cancel migrate failed: task_type[%s], task_id[%s], err[%+v]", mgr.taskType, arg.TaskID, err)
	}
	return
}

// ReclaimTask reclaim migrate task
func (mgr *MigrateMgr) ReclaimTask(ctx context.Context, args *api.TaskArgs) (err error) {
	mgr.taskStatsMgr.ReclaimTask()
	span := trace.SpanFromContextSafe(ctx)

	arg := &api.OperateTaskArgs{}
	err = arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}

	if !client.ValidMigrateTask(args.TaskType, arg.TaskID) {
		return errcode.ErrIllegalArguments
	}

	newDst, err := base.AllocVunitSafe(ctx, mgr.clusterMgrCli, arg.Dest.Vuid, arg.Src)
	if err != nil {
		span.Errorf("alloc volume unit from clustermgr failed, err: %s", err)
		return err
	}

	err = mgr.workQueue.Reclaim(arg.IDC, arg.TaskID, arg.Src, arg.Dest, newDst.Location(), newDst.DiskID)
	if err != nil {
		span.Errorf("reclaim migrate task failed: task_type:[%s],task_id[%s], err[%+v]", mgr.taskType, arg.TaskID, err)
		return err
	}

	task, err := mgr.workQueue.Query(arg.IDC, arg.TaskID)
	if err != nil {
		span.Errorf("found task in workQueue failed: idc[%s], task_id[%s], err[%+v]", arg.IDC, arg.TaskID, err)
		return err
	}
	t, err := task.Task()
	if err != nil {
		return err
	}
	err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, t)

	if err != nil {
		span.Errorf("update reclaim task failed: task_id[%s], err[%+v]", arg.TaskID, err)
	}
	return
}

// CompleteTask complete migrate task
func (mgr *MigrateMgr) CompleteTask(ctx context.Context, args *api.TaskArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	arg := &api.OperateTaskArgs{}
	err = arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}
	if !client.ValidMigrateTask(args.TaskType, arg.TaskID) {
		return errcode.ErrIllegalArguments
	}

	completeTask, err := mgr.workQueue.Complete(arg.IDC, arg.TaskID, arg.Src, arg.Dest)
	if err != nil {
		span.Errorf("complete migrate task failed: task_id[%s], err[%+v]", arg.TaskID, err)
		return err
	}

	t := completeTask.(*proto.MigrateTask)
	t.State = proto.MigrateStateWorkCompleted
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
	ts, err := mgr.clusterMgrCli.ListAllMigrateTasks(ctx, mgr.taskType)
	if err != nil {
		return nil, err
	}
	for _, t := range ts {
		task := &proto.MigrateTask{}
		err = task.Unmarshal(t.Data)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return
}

// ListAllTaskByDiskID return all task by diskID
func (mgr *MigrateMgr) ListAllTaskByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error) {
	ts, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, mgr.taskType, diskID)
	if err != nil {
		return nil, err
	}
	for _, t := range ts {
		task := &proto.MigrateTask{}
		err = task.Unmarshal(t.Data)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return
}

// GetTask returns task in db
func (mgr *MigrateMgr) GetTask(ctx context.Context, taskID string) (*proto.MigrateTask, error) {
	task, err := mgr.clusterMgrCli.GetMigrateTask(ctx, mgr.taskType, taskID)
	if err != nil {
		return nil, err
	}
	ret := &proto.MigrateTask{}
	err = ret.Unmarshal(task.Data)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// QueryTask implement migrator
func (mgr *MigrateMgr) QueryTask(ctx context.Context, taskID string) (*api.TaskRet, error) {
	detail := &api.MigrateTaskDetail{}
	task, err := mgr.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	detail.Task = *task

	detailRunInfo, err := mgr.taskStatsMgr.QueryTaskDetail(taskID)
	if err == nil {
		detail.Stat = detailRunInfo.Statistics
	}
	data, err := json.Marshal(detail)
	if err != nil {
		return nil, err
	}

	return &api.TaskRet{TaskType: task.TaskType, Data: data}, nil
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

func checkMigrateConf(conf *MigrateConfig) {
	if conf.clearJunkTasksWhenLoadingFunc == nil {
		conf.clearJunkTasksWhenLoadingFunc = defaultClearJunkTasksFunc
	}
	if conf.finishTaskCallback == nil {
		conf.finishTaskCallback = defaultDiskTaskLimitFunc
	}
	if conf.loadTaskCallback == nil {
		conf.loadTaskCallback = defaultDiskTaskLimitFunc
	}
}

func (mgr *MigrateMgr) ReportTask(ctx context.Context, args *api.TaskArgs) (err error) {
	arg := &api.TaskReportArgs{}
	err = arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}

	if !client.ValidMigrateTask(arg.TaskType, arg.TaskID) {
		return errcode.ErrIllegalArguments
	}

	mgr.ReportWorkerTaskStats(arg)
	return nil
}
