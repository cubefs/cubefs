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

// DiskRepairMgr repair task manager
type DiskRepairMgr struct {
	closer.Closer

	prepareQueue   *base.TaskQueue
	workQueue      *base.WorkerTaskQueue
	finishQueue    *base.TaskQueue
	deletedTasks   *diskMigratedTasks
	repairedDisks  *migratedDisks
	repairingDisks *migratingDisks

	clusterMgrCli client.ClusterMgrAPI

	taskSwitch taskswitch.ISwitcher

	// for stats
	finishTaskCounter counter.Counter
	taskStatsMgr      *base.TaskStatsMgr

	hasRevised bool
	taskLogger recordlog.Encoder
	cfg        *MigrateConfig
}

// NewDiskRepairMgr returns repair manager
func NewDiskRepairMgr(clusterMgrCli client.ClusterMgrAPI, taskSwitch taskswitch.ISwitcher, taskLogger recordlog.Encoder, cfg *MigrateConfig) *DiskRepairMgr {
	mgr := &DiskRepairMgr{
		Closer:         closer.New(),
		prepareQueue:   base.NewTaskQueue(time.Duration(cfg.PrepareQueueRetryDelayS) * time.Second),
		workQueue:      base.NewWorkerTaskQueue(time.Duration(cfg.CancelPunishDurationS) * time.Second),
		finishQueue:    base.NewTaskQueue(time.Duration(cfg.FinishQueueRetryDelayS) * time.Second),
		deletedTasks:   newDiskMigratedTasks(),
		repairedDisks:  newMigratedDisks(),
		repairingDisks: newMigratingDisks(),

		clusterMgrCli: clusterMgrCli,
		taskSwitch:    taskSwitch,
		cfg:           cfg,
		taskLogger:    taskLogger,

		hasRevised: false,
	}
	mgr.taskStatsMgr = base.NewTaskStatsMgrAndRun(cfg.ClusterID, proto.TaskTypeDiskRepair, mgr)
	return mgr
}

// Load load repair task from database
func (mgr *DiskRepairMgr) Load() error {
	span, ctx := trace.StartSpanFromContext(context.Background(), "Load")

	repairingDisks, err := mgr.clusterMgrCli.ListMigratingDisks(ctx, proto.TaskTypeDiskRepair)
	if err != nil {
		return err
	}
	for _, disk := range repairingDisks {
		mgr.repairingDisks.add(disk.Disk.DiskID, disk.Disk)
	}

	tasks, err := mgr.clusterMgrCli.ListAllMigrateTasks(ctx, proto.TaskTypeDiskRepair)
	if err != nil {
		span.Errorf("find all tasks failed: err[%+v]", err)
		return err
	}
	if len(tasks) == 0 {
		return nil
	}

	var junkTasks []*proto.MigrateTask
	for _, t := range tasks {
		task := &proto.MigrateTask{}
		err = task.Unmarshal(t.Data)
		if err != nil {
			return err
		}
		if _, ok := mgr.repairingDisks.get(task.SourceDiskID); !ok {
			junkTasks = append(junkTasks, task)
			continue
		}
		if task.Running() {
			err = base.VolTaskLockerInst().TryLock(ctx, uint32(task.Vid()))
			if err != nil {
				return fmt.Errorf("repair task conflict: task[%+v], err[%+v]",
					t, err.Error())
			}
		}

		span.Infof("load task success: task_id[%s], state[%d]", t.TaskID, task.State)
		switch task.State {
		case proto.MigrateStateInited:
			mgr.prepareQueue.PushTask(t.TaskID, task)
		case proto.MigrateStatePrepared:
			mgr.workQueue.AddPreparedTask(task.SourceIDC, t.TaskID, task)
		case proto.MigrateStateWorkCompleted:
			mgr.finishQueue.PushTask(t.TaskID, task)
		case proto.MigrateStateFinished, proto.MigrateStateFinishedInAdvance:
			return fmt.Errorf("task should be deleted from db: task[%+v]", t)
		default:
			return fmt.Errorf("unexpect migrate state: task[%+v]", t)
		}
	}

	return mgr.clearJunkTasksWhenLoading(ctx, junkTasks)
}

func (mgr *DiskRepairMgr) clearJunkTasksWhenLoading(ctx context.Context, tasks []*proto.MigrateTask) error {
	span := trace.SpanFromContextSafe(ctx)

	disks := make(map[proto.DiskID]bool)
	for _, task := range tasks {
		if _, ok := disks[task.SourceDiskID]; !ok {
			diskInfo, err := mgr.clusterMgrCli.GetDiskInfo(ctx, task.SourceDiskID)
			if err != nil {
				return err
			}
			disks[task.SourceDiskID] = diskInfo.IsRepaired()
		}
		if !disks[task.SourceDiskID] {
			span.Errorf("has junk task but the disk is not repaired: disk_id[%d], task_id[%s]", task.SourceDiskID, task.TaskID)
			return errcode.ErrUnexpectMigrationTask
		}
		span.Warnf("loading delete junk task: task_id[%s]", task.TaskID)
		base.InsistOn(ctx, " loading delete junk task", func() error {
			return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
		})
	}
	return nil
}

// Run run repair task includes collect/prepare/finish/check phase
func (mgr *DiskRepairMgr) Run() {
	go mgr.collectTaskLoop()
	go mgr.prepareTaskLoop()
	go mgr.finishTaskLoop()
	go mgr.checkRepairedAndClearLoop()
	go mgr.checkAndClearJunkTasksLoop()
}

func (mgr *DiskRepairMgr) Enabled() bool {
	return mgr.taskSwitch.Enabled()
}

func (mgr *DiskRepairMgr) WaitEnable() {
	mgr.taskSwitch.WaitEnable()
}

func (mgr *DiskRepairMgr) collectTaskLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CollectTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.WaitEnable()
			mgr.collectTask()
		case <-mgr.Closer.Done():
			return
		}
	}
}

func (mgr *DiskRepairMgr) collectTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_repair.collectTask")
	defer span.Finish()

	// revise repair tasks to make sure data consistency when services start
	if !mgr.hasRevised {
		if err := mgr.reviseRepairDisks(ctx); err != nil {
			return
		}
		mgr.hasRevised = true
	}

	if mgr.repairingDisks.size() >= mgr.cfg.DiskConcurrency {
		return
	}

	brokenDisk, err := mgr.acquireBrokenDisk(ctx)
	if err != nil {
		span.Info("acquire broken disk failed: err[%+v]", err)
		return
	}

	if brokenDisk == nil {
		return
	}

	err = mgr.genDiskRepairTasks(ctx, brokenDisk, true)
	if err != nil {
		span.Errorf("generate disk repair tasks failed: err[%+v]", err)
		return
	}

	base.InsistOn(ctx, "set disk diskId %d repairing failed", func() error {
		return mgr.clusterMgrCli.SetDiskRepairing(ctx, brokenDisk.DiskID)
	})

	mgr.repairingDisks.add(brokenDisk.DiskID, brokenDisk)
}

func (mgr *DiskRepairMgr) reviseRepairDisks(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	for _, disk := range mgr.repairingDisks.list() {
		if err := mgr.reviseRepairDisk(ctx, disk.DiskID); err != nil {
			span.Errorf("revise repair tasks failed: disk_id[%d]", disk.DiskID)
			return err
		}
	}
	return nil
}

func (mgr *DiskRepairMgr) reviseRepairDisk(ctx context.Context, diskID proto.DiskID) error {
	span := trace.SpanFromContextSafe(ctx)

	diskInfo, err := mgr.clusterMgrCli.GetDiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("get disk info failed: err[%+v]", err)
		return err
	}

	if err = mgr.genDiskRepairTasks(ctx, diskInfo, false); err != nil {
		span.Errorf("generate disk repair tasks failed: err[%+v]", err)
		return err
	}

	if diskInfo.IsBroken() {
		execMsg := fmt.Sprintf("set disk diskId %d repairing", diskID)
		base.InsistOn(ctx, execMsg, func() error {
			return mgr.clusterMgrCli.SetDiskRepairing(ctx, diskID)
		})
	}
	return nil
}

func (mgr *DiskRepairMgr) acquireBrokenDisk(ctx context.Context) (*client.DiskInfoSimple, error) {
	repairingDisks, err := mgr.clusterMgrCli.ListBrokenDisks(ctx)
	if err != nil {
		return nil, err
	}
	if len(repairingDisks) == 0 {
		return nil, nil
	}
	return mgr.getUnRepairingDisk(repairingDisks), nil
}

func (mgr *DiskRepairMgr) getUnRepairingDisk(disks []*client.DiskInfoSimple) *client.DiskInfoSimple {
	for _, v := range disks {
		if _, ok := mgr.repairingDisks.get(v.DiskID); !ok {
			return v
		}
	}
	return nil
}

func (mgr *DiskRepairMgr) genDiskRepairTasks(ctx context.Context, disk *client.DiskInfoSimple, newRepairDisk bool) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start generate disk repair tasks: disk_id[%d], disk_idc[%s]", disk.DiskID, disk.Idc)

	migratingVuids, err := mgr.listMigratingVuid(ctx, disk.DiskID)
	if err != nil {
		span.Errorf("list repairing vuids failed: err[%+v]", err)
		return err
	}

	unmigratedvuids, err := mgr.listUnmigratedVuid(ctx, disk.DiskID)
	if err != nil {
		span.Errorf("list un repaired vuids failed: err[%+v]", err)
		return err
	}

	remain := base.Subtraction(unmigratedvuids, migratingVuids)
	span.Infof("should gen tasks remain: len[%d]", len(remain))
	if newRepairDisk {
		meta := &client.MigratingDiskMeta{
			TaskType: proto.TaskTypeDiskRepair,
			Disk:     disk,
		}
		if err := mgr.clusterMgrCli.AddMigratingDisk(ctx, meta); err != nil {
			return err
		}
	}
	for _, vuid := range remain {
		mgr.initOneTask(ctx, vuid, disk.DiskID, disk.Idc)
	}
	return nil
}

func (mgr *DiskRepairMgr) listMigratingVuid(ctx context.Context, diskID proto.DiskID) (bads []proto.Vuid, err error) {
	tasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskRepair, diskID)
	if err != nil {
		return nil, err
	}

	task := &proto.MigrateTask{}
	for _, t := range tasks {
		err = task.Unmarshal(t.Data)
		if err != nil {
			return nil, err
		}
		bads = append(bads, task.SourceVuid)
	}
	return bads, nil
}

func (mgr *DiskRepairMgr) listUnmigratedVuid(ctx context.Context, diskID proto.DiskID) (bads []proto.Vuid, err error) {
	vunits, err := mgr.clusterMgrCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, vunit := range vunits {
		bads = append(bads, vunit.Vuid)
	}
	return bads, nil
}

func (mgr *DiskRepairMgr) initOneTask(ctx context.Context, badVuid proto.Vuid, brokenDiskID proto.DiskID, brokenDiskIdc string) {
	span := trace.SpanFromContextSafe(ctx)

	t := proto.MigrateTask{
		TaskID:                  client.GenMigrateTaskID(proto.TaskTypeDiskRepair, brokenDiskID, uint32(badVuid.Vid())),
		TaskType:                proto.TaskTypeDiskRepair,
		State:                   proto.MigrateStateInited,
		SourceDiskID:            brokenDiskID,
		SourceVuid:              badVuid,
		SourceIDC:               brokenDiskIdc,
		ForbiddenDirectDownload: true,
		Ctime:                   time.Now().String(),
	}
	t.MTime = t.Ctime

	base.InsistOn(ctx, "repair init one task insert task to tbl", func() error {
		task, err := t.ToTask()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.AddMigrateTask(ctx, task)
	})

	mgr.prepareQueue.PushTask(t.TaskID, &t)
	span.Infof("init repair task success %+v", t)
}

func (mgr *DiskRepairMgr) prepareTaskLoop() {
	for {
		mgr.WaitEnable()
		todo, doing := mgr.workQueue.StatsTasks()
		if mgr.repairingDisks.size() == 0 || todo+doing >= mgr.cfg.WorkQueueSize {
			time.Sleep(time.Second)
			continue
		}

		err := mgr.popTaskAndPrepare()
		if err == base.ErrNoTaskInQueue {
			time.Sleep(time.Second)
		}
	}
}

func (mgr *DiskRepairMgr) popTaskAndPrepare() error {
	_, task, exist := mgr.prepareQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	var err error
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_repair.popTaskAndPrepare")
	defer span.Finish()

	defer func() {
		if err != nil {
			span.Errorf("prepare task failed  and retry task: task_id[%s], err[%+v]", task.(*proto.MigrateTask).TaskID, err)
			mgr.prepareQueue.RetryTask(task.(*proto.MigrateTask).TaskID)
		}
	}()

	//why:avoid to change task in queue
	t := task.(*proto.MigrateTask).Copy()
	span.Infof("pop task: task_id[%s], task[%+v]", t.TaskID, t)
	// whether vid has another running task
	err = base.VolTaskLockerInst().TryLock(ctx, uint32(t.Vid()))
	if err != nil {
		span.Warnf("tryLock failed: vid[%d]", t.Vid())
		return base.ErrVolNotOnlyOneTask
	}
	defer func() {
		if err != nil {
			span.Errorf("prepare task failed: task_id[%s], err[%+v]", t.TaskID, err)
			base.VolTaskLockerInst().Unlock(ctx, uint32(t.Vid()))
		}
	}()

	err = mgr.prepareTask(t)
	if err != nil {
		span.Errorf("prepare task failed: task_id[%s], err[%+v]", t.TaskID, err)
		return err
	}

	span.Infof("prepare task success: task_id[%s]", t.TaskID)
	return nil
}

func (mgr *DiskRepairMgr) prepareTask(t *proto.MigrateTask) error {
	span, ctx := trace.StartSpanFromContext(
		context.Background(),
		"DiskRepairMgr.prepareTask")
	defer span.Finish()

	span.Infof("start prepare repair task: task_id[%s], task[%+v]", t.TaskID, t)

	volInfo, err := mgr.clusterMgrCli.GetVolumeInfo(ctx, t.Vid())
	if err != nil {
		span.Errorf("prepare task get volume info failed: err[%+v]", err)
		return err
	}

	// 1.check necessity of generating current task
	badVuid := t.SourceVuid
	if volInfo.VunitLocations[badVuid.Index()].Vuid != badVuid {
		span.Infof("repair task finish in advance: task_id[%s]", t.TaskID)
		mgr.finishTaskInAdvance(ctx, t, "volume has migrated")
		return nil
	}

	// 2.generate src and destination for task & task persist
	allocDstVunit, err := base.AllocVunitSafe(ctx, mgr.clusterMgrCli, badVuid, t.Sources, nil)
	if err != nil {
		span.Errorf("repair alloc volume unit failed: err[%+v]", err)
		return err
	}

	t.CodeMode = volInfo.CodeMode
	t.Sources = volInfo.VunitLocations
	t.Destination = allocDstVunit.Location()
	t.State = proto.MigrateStatePrepared
	base.InsistOn(ctx, "repair prepare task update task tbl", func() error {
		task, err := t.ToTask()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
	})

	mgr.sendToWorkQueue(t)
	return nil
}

func (mgr *DiskRepairMgr) sendToWorkQueue(t *proto.MigrateTask) {
	mgr.workQueue.AddPreparedTask(t.SourceIDC, t.TaskID, t)
	mgr.prepareQueue.RemoveTask(t.TaskID)
}

func (mgr *DiskRepairMgr) finishTaskInAdvance(ctx context.Context, task *proto.MigrateTask, reason string) {
	task.State = proto.MigrateStateFinishedInAdvance
	task.FinishAdvanceReason = reason
	base.InsistOn(ctx, "repair finish task in advance update task tbl", func() error {
		return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
	})

	if recordErr := mgr.taskLogger.Encode(task); recordErr != nil {
		trace.SpanFromContextSafe(ctx).Errorf("record repair task failed: task[%+v], err[%+v]", task, recordErr)
	}

	mgr.finishTaskCounter.Add()
	mgr.prepareQueue.RemoveTask(task.TaskID)
	mgr.deletedTasks.add(task.SourceDiskID, task.TaskID)
	base.VolTaskLockerInst().Unlock(ctx, uint32(task.Vid()))
}

func (mgr *DiskRepairMgr) finishTaskLoop() {
	for {
		mgr.WaitEnable()
		err := mgr.popTaskAndFinish()
		if err == base.ErrNoTaskInQueue {
			time.Sleep(5 * time.Second)
		}
	}
}

func (mgr *DiskRepairMgr) popTaskAndFinish() error {
	_, task, exist := mgr.finishQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_repair.popTaskAndFinish")

	t := task.(*proto.MigrateTask).Copy()
	err := mgr.finishTask(ctx, t)
	if err != nil {
		span.Errorf("finish task failed: err[%+v]", err)
		return err
	}

	span.Infof("finish task success: task_id[%s]", t.TaskID)
	return nil
}

func (mgr *DiskRepairMgr) finishTask(ctx context.Context, task *proto.MigrateTask) (retErr error) {
	span := trace.SpanFromContextSafe(ctx)

	defer func() {
		if retErr != nil {
			mgr.finishQueue.RetryTask(task.TaskID)
		}
	}()

	if task.State != proto.MigrateStateWorkCompleted {
		span.Panicf("task state not expect: task_id[%s], expect state[%d], actual state[%d]", proto.MigrateStateWorkCompleted, task.State)
	}
	// complete stage can not make sure to save task info to db,
	// finish stage make sure to save task info to db
	// execute update volume mapping relation when can not save task with completed state is dangerous
	// because if process restart will reload task and redo by worker
	// worker will write data to chunk which is online
	base.InsistOn(ctx, "repair finish task update task state completed", func() error {
		t, err := task.ToTask()
		if err != nil {
			return err
		}
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, t)
	})

	newVuid := task.Destination.Vuid
	oldVuid := task.SourceVuid
	err := mgr.clusterMgrCli.UpdateVolume(ctx, newVuid, oldVuid, task.DestinationDiskID())
	if err != nil {
		span.Errorf("update volume failed: err[%+v]", err)
		return mgr.handleUpdateVolMappingFail(ctx, task, err)
	}

	task.State = proto.MigrateStateFinished
	base.InsistOn(ctx, "repair finish task update task state finished", func() error {
		return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
	})

	if recordErr := mgr.taskLogger.Encode(task); recordErr != nil {
		span.Errorf("record repair task failed: task[%+v], err[%+v]", task, recordErr)
	}

	mgr.finishTaskCounter.Add()
	// 1.remove task in memory
	// 2.release lock of volume task
	mgr.finishQueue.RemoveTask(task.TaskID)

	// add delete task and check it again
	mgr.deletedTasks.add(task.SourceDiskID, task.TaskID)

	base.VolTaskLockerInst().Unlock(ctx, uint32(task.Vid()))

	return nil
}

func (mgr *DiskRepairMgr) handleUpdateVolMappingFail(ctx context.Context, task *proto.MigrateTask, err error) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("handle update vol mapping failed: task_id[%s], state[%d], dest vuid[%d]", task.TaskID, task.State, task.Destination.Vuid)

	code := rpc.DetectStatusCode(err)
	if code == errcode.CodeOldVuidNotMatch {
		span.Panicf("change volume unit relationship got unexpected err")
	}

	if base.ShouldAllocAndRedo(code) {
		span.Infof("realloc vunit and redo: task_id[%s]", task.TaskID)

		newVunit, err := base.AllocVunitSafe(ctx, mgr.clusterMgrCli, task.SourceVuid, task.Sources, nil)
		if err != nil {
			span.Errorf("realloc failed: vuid[%d], err[%+v]", task.SourceVuid, err)
			return err
		}
		task.SetDestination(newVunit.Location())
		task.State = proto.MigrateStatePrepared
		task.WorkerRedoCnt++

		base.InsistOn(ctx, "repair redo task update task tbl", func() error {
			t, err := task.ToTask()
			if err != nil {
				return err
			}
			return mgr.clusterMgrCli.UpdateMigrateTask(ctx, t)
		})

		mgr.finishQueue.RemoveTask(task.TaskID)
		mgr.workQueue.AddPreparedTask(task.SourceIDC, task.TaskID, task)
		span.Infof("task redo again:  task_id[%v]", task.TaskID)
		return nil
	}

	return err
}

func (mgr *DiskRepairMgr) checkRepairedAndClearLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CheckTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.WaitEnable()
			mgr.checkRepairedAndClear()
		case <-mgr.Closer.Done():
			return
		}
	}
}

func (mgr *DiskRepairMgr) checkRepairedAndClear() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_repair.checkRepairedAndClear")

	for _, disk := range mgr.repairingDisks.list() {
		if !mgr.checkDiskRepaired(ctx, disk.DiskID) {
			continue
		}
		err := mgr.clusterMgrCli.SetDiskRepaired(ctx, disk.DiskID)
		if err != nil {
			return
		}
		span.Infof("disk repaired will start clear: disk_id[%d]", disk.DiskID)
		mgr.clearTasksByDiskID(ctx, disk.DiskID)
	}
}

func (mgr *DiskRepairMgr) clearTasksByDiskID(ctx context.Context, diskID proto.DiskID) {
	base.InsistOn(ctx, "delete migrating disk fail", func() error {
		return mgr.clusterMgrCli.DeleteMigratingDisk(ctx, proto.TaskTypeDiskRepair, diskID)
	})
	mgr.deletedTasks.delete(diskID)
	mgr.repairedDisks.add(diskID, time.Now())
	mgr.repairingDisks.delete(diskID)
}

func (mgr *DiskRepairMgr) checkDiskRepaired(ctx context.Context, diskID proto.DiskID) bool {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("check repaired: disk_id[%d]", diskID)

	tasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskRepair, diskID)
	if err != nil {
		span.Errorf("check repaired and find task failed: disk_iD[%d], err[%+v]", diskID, err)
		return false
	}
	vunitInfos, err := mgr.clusterMgrCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		span.Errorf("check repaired list disk volume units failed: disk_id[%s], err[%+v]", diskID, err)
		return false
	}
	if len(vunitInfos) == 0 && len(tasks) != 0 {
		// due to network timeout, it may lead to repeated insertion of deleted tasks, and need to delete it again
		mgr.clearJunkTasks(ctx, diskID, tasks)
		return false
	}
	if len(vunitInfos) != 0 && len(tasks) == 0 {
		// it may be occur when migration done and repair tasks generate concurrent, list volume units may not return the migrate unit
		span.Warnf("clustermgr has some volume unit not repair and revise again: disk_id[%d], volume units len[%d]", diskID, len(vunitInfos))
		if err = mgr.reviseRepairDisk(ctx, diskID); err != nil {
			span.Errorf("revise repair task failed: err[%+v]", err)
		}
		return false
	}
	return len(tasks) == 0 && len(vunitInfos) == 0
}

func (mgr *DiskRepairMgr) clearJunkTasks(ctx context.Context, diskID proto.DiskID, tasks []*proto.Task) {
	span := trace.SpanFromContextSafe(ctx)
	for _, task := range tasks {
		if !mgr.deletedTasks.exits(diskID, task.TaskID) {
			continue
		}
		span.Warnf("delete junk task: task_id[%s]", task.TaskID)
		base.InsistOn(ctx, "delete junk task", func() error {
			return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
		})
	}
}

// checkAndClearJunkTasksLoop due to network timeout, the repaired disk may still have some junk migrate tasks in clustermgr,
// and we need to clear those tasks later
func (mgr *DiskRepairMgr) checkAndClearJunkTasksLoop() {
	t := time.NewTicker(clearJunkMigrationTaskInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.checkAndClearJunkTasks()
		case <-mgr.Closer.Done():
			return
		}
	}
}

func (mgr *DiskRepairMgr) checkAndClearJunkTasks() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_repair.clearJunkTasks")

	for _, disk := range mgr.repairedDisks.list() {
		if time.Since(disk.finishedTime) < junkMigrationTaskProtectionWindow {
			continue
		}
		span.Debugf("check repaired disk: disk_id[%d], repaired time[%v]", disk.diskID, disk.finishedTime)
		diskInfo, err := mgr.clusterMgrCli.GetDiskInfo(ctx, disk.diskID)
		if err != nil {
			span.Errorf("get disk info failed: disk_id[%d], err[%+v]", disk.diskID, err)
			continue
		}
		if !diskInfo.IsRepaired() {
			continue
		}
		tasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskRepair, disk.diskID)
		if err != nil {
			continue
		}
		if len(tasks) != 0 {
			span.Warnf("clear junk tasks of repaired disk: disk_id[%d], tasks size[%d]", disk.diskID, len(tasks))
			for _, task := range tasks {
				span.Warnf("check and delete junk task: task_id[%s]", task.TaskID)
				base.InsistOn(ctx, "chek and delete junk task", func() error {
					return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
				})
			}
		}
		mgr.repairedDisks.delete(disk.diskID)
	}
}

// AcquireTask acquire repair task
func (mgr *DiskRepairMgr) AcquireTask(ctx context.Context, idc string) (task *proto.Task, err error) {
	if !mgr.taskSwitch.Enabled() {
		return task, proto.ErrTaskPaused
	}
	_, repairTask, _ := mgr.workQueue.Acquire(idc)
	if repairTask != nil {
		t := *repairTask.(*proto.MigrateTask)
		data, err := t.Marshal()
		if err != nil {
			return task, err
		}
		task = &proto.Task{}
		task.Data = data
		task.ModuleType = proto.TypeBlobNode
		return task, nil
	}
	return task, proto.ErrTaskEmpty
}

// CancelTask cancel repair task
func (mgr *DiskRepairMgr) CancelTask(ctx context.Context, args *api.TaskArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	arg := &api.BlobnodeTaskArgs{}
	err := arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}

	err = mgr.workQueue.Cancel(arg.IDC, arg.TaskID, arg.Src, arg.Dest)
	if err != nil {
		span.Errorf("cancel repair failed: task_id[%s], err[%+v]", arg.TaskID, err)
	}

	mgr.taskStatsMgr.CancelTask()

	return err
}

// ReclaimTask reclaim repair task
func (mgr *DiskRepairMgr) ReclaimTask(ctx context.Context, args *api.TaskArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	arg := &api.BlobnodeTaskArgs{}
	err := arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}
	if int(arg.Dest.Vuid.Index()) >= len(arg.Src) || arg.Src[arg.Dest.Vuid.Index()].Vuid.Index() != arg.Dest.Vuid.Index() {
		return errcode.ErrIllegalArguments
	}

	newDst, err := base.AllocVunitSafe(ctx, mgr.clusterMgrCli,
		arg.Src[arg.Dest.Vuid.Index()].Vuid, arg.Src, []proto.DiskID{arg.Dest.DiskID})
	if err != nil {
		span.Errorf("alloc volume unit from clustermgr failed, err: %s", err)
		return err
	}

	err = mgr.workQueue.Reclaim(arg.IDC, arg.TaskID, arg.Src, arg.Dest, newDst.VunitLocation, newDst.DiskID)
	if err != nil {
		span.Errorf("reclaim task in workQueue failed: idc[%s], task_id[%s], err[%+v]", arg.IDC, arg.TaskID, err)
		return err
	}

	task, err := mgr.workQueue.Query(arg.IDC, arg.TaskID)
	if err != nil {
		span.Errorf("found task in workQueue failed: idc[%s], task_id[%s], err[%+v]", arg.IDC, arg.TaskID, err)
		return err
	}
	t, _ := task.ToTask()
	err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, t)
	if err != nil {
		span.Warnf("update reclaim task failed: task_id[%s], err[%+v]", arg.TaskID, err)
	}

	mgr.taskStatsMgr.ReclaimTask()
	return nil
}

// CompleteTask complete repair task
func (mgr *DiskRepairMgr) CompleteTask(ctx context.Context, args *api.TaskArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	arg := &api.BlobnodeTaskArgs{}
	err := arg.Unmarshal(args.Data)
	if err != nil {
		return err
	}

	completeTask, err := mgr.workQueue.Complete(arg.IDC, arg.TaskID, arg.Src, arg.Dest)
	if err != nil {
		span.Errorf("complete repair task failed: task_id[%s], err[%+v]", arg.TaskID, err)
		return err
	}

	t := completeTask.(*proto.MigrateTask)
	t.State = proto.MigrateStateWorkCompleted

	mgr.finishQueue.PushTask(arg.TaskID, t)
	// as complete func is face to svr api, so can not loop save task
	// to db until success, it will make saving task info to be difficult,
	// that delay saving task info in finish stage is a simply way
	return nil
}

// RenewalTask renewal repair task
func (mgr *DiskRepairMgr) RenewalTask(ctx context.Context, idc, taskID string) error {
	if !mgr.taskSwitch.Enabled() {
		// renewal task stopping will touch off worker to stop task
		return proto.ErrTaskPaused
	}

	span := trace.SpanFromContextSafe(ctx)
	err := mgr.workQueue.Renewal(idc, taskID)
	if err != nil {
		span.Warnf("renewal repair task failed: task_id[%s], err[%+v]", taskID, err)
	}
	return err
}

func (mgr *DiskRepairMgr) ReportTask(ctx context.Context, args *api.TaskArgs) (err error) {
	arg := &api.BlobnodeTaskReportArgs{}
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

// ReportWorkerTaskStats reports task stats
func (mgr *DiskRepairMgr) ReportWorkerTaskStats(st *api.BlobnodeTaskReportArgs) {
	mgr.taskStatsMgr.ReportWorkerTaskStats(st.TaskID, st.TaskStats, st.IncreaseDataSizeByte, st.IncreaseShardCnt)
}

// QueryTask return task statistics
func (mgr *DiskRepairMgr) QueryTask(ctx context.Context, taskID string) (*api.TaskRet, error) {
	migTask := &api.MigrateTaskDetail{}
	task, err := mgr.clusterMgrCli.GetMigrateTask(ctx, proto.TaskTypeDiskRepair, taskID)
	if err != nil {
		return nil, err
	}
	migrateTask := &proto.MigrateTask{}
	err = migrateTask.Unmarshal(task.Data)
	if err != nil {
		return nil, err
	}
	migTask.Task = *migrateTask
	detailRunInfo, err := mgr.taskStatsMgr.QueryTaskDetail(taskID)
	if err != nil {
		return nil, nil
	}
	migTask.Stat = detailRunInfo.Statistics

	data, err := json.Marshal(migTask)
	if err != nil {
		return nil, err
	}
	return &api.TaskRet{TaskType: proto.TaskTypeDiskRepair, Data: data}, nil
}

// StatQueueTaskCnt returns task queue stats
func (mgr *DiskRepairMgr) StatQueueTaskCnt() (inited, prepared, completed int) {
	todo, doing := mgr.prepareQueue.StatsTasks()
	inited = todo + doing

	todo, doing = mgr.workQueue.StatsTasks()
	prepared = todo + doing

	todo, doing = mgr.finishQueue.StatsTasks()
	completed = todo + doing
	return
}

// Stats returns task stats
func (mgr *DiskRepairMgr) Stats() api.MigrateTasksStat {
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

// Progress repair manager progress
func (mgr *DiskRepairMgr) Progress(ctx context.Context) (migratingDisks []proto.DiskID, total, migrated int) {
	span := trace.SpanFromContextSafe(ctx)
	migratingDisks = make([]proto.DiskID, 0)

	for _, disk := range mgr.repairingDisks.list() {
		total += int(disk.UsedChunkCnt)
		remainTasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskRepair, disk.DiskID)
		if err != nil {
			span.Errorf("find all task failed: err[%+v]", err)
			return migratingDisks, 0, 0
		}
		migrated += int(disk.UsedChunkCnt) - len(remainTasks)
		migratingDisks = append(migratingDisks, disk.DiskID)
	}
	return
}

func (mgr *DiskRepairMgr) DiskProgress(ctx context.Context, diskID proto.DiskID) (stats *api.DiskMigratingStats, err error) {
	span := trace.SpanFromContextSafe(ctx)

	migratingDisk, ok := mgr.repairingDisks.get(diskID)
	if !ok {
		err = errors.New("not repairing disk")
		return
	}
	remainTasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskRepair, diskID)
	if err != nil {
		span.Errorf("find all task failed: err[%+v]", err)
		return
	}
	stats = &api.DiskMigratingStats{}
	stats.TotalTasksCnt = int(migratingDisk.UsedChunkCnt)
	stats.MigratedTasksCnt = stats.TotalTasksCnt - len(remainTasks)
	return
}
