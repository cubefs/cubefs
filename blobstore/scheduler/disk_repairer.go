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
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// DiskRepairMgr repair task manager
type DiskRepairMgr struct {
	closer.Closer
	repairingDiskID proto.DiskID // only supports repair one disk at the same time temporarily

	mu sync.Mutex

	prepareQueue *base.TaskQueue
	workQueue    *base.WorkerTaskQueue
	finishQueue  *base.TaskQueue

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
		Closer:       closer.New(),
		prepareQueue: base.NewTaskQueue(time.Duration(cfg.PrepareQueueRetryDelayS) * time.Second),
		workQueue:    base.NewWorkerTaskQueue(time.Duration(cfg.CancelPunishDurationS) * time.Second),
		finishQueue:  base.NewTaskQueue(time.Duration(cfg.FinishQueueRetryDelayS) * time.Second),

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
	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	repairingDisks, err := mgr.clusterMgrCli.ListMigratingDisks(ctx, proto.TaskTypeDiskRepair)
	if err != nil {
		return err
	}
	if len(repairingDisks) == 0 {
		tasks, err := mgr.clusterMgrCli.ListAllMigrateTasks(ctx, proto.TaskTypeDiskRepair)
		if err != nil {
			return err
		}
		if len(tasks) != 0 {
			span.Errorf("existing duplicate task: count[%d]", len(tasks))
			return errors.New("existing duplicate task")
		}
		return nil
	}
	if len(repairingDisks) > 1 {
		return errors.New("can not allow many disk repairing")
	}
	repairingDiskID := repairingDisks[0].Disk.DiskID
	tasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskRepair, repairingDiskID)
	if err != nil {
		return err
	}
	mgr.setRepairingDiskID(repairingDiskID)

	for _, t := range tasks {
		if t.Running() {
			err = base.VolTaskLockerInst().TryLock(ctx, t.Vid())
			if err != nil {
				return fmt.Errorf("repair task conflict: task[%+v], err[%+v]",
					t, err.Error())
			}
		}

		log.Infof("load task success: task_id[%s], state[%d]", t.TaskID, t.State)
		switch t.State {
		case proto.MigrateStateInited:
			mgr.prepareQueue.PushTask(t.TaskID, t)
		case proto.MigrateStatePrepared:
			mgr.workQueue.AddPreparedTask(t.SourceIDC, t.TaskID, t)
		case proto.MigrateStateWorkCompleted:
			mgr.finishQueue.PushTask(t.TaskID, t)
		case proto.MigrateStateFinished, proto.MigrateStateFinishedInAdvance:
			return fmt.Errorf("task should be deleted from db: task[%+v]", t)
		default:
			return fmt.Errorf("unexpect migrate state: task[%+v]", t)
		}
	}

	return nil
}

// Run run repair task includes collect/prepare/finish/check phase
func (mgr *DiskRepairMgr) Run() {
	go mgr.collectTaskLoop()
	go mgr.prepareTaskLoop()
	go mgr.finishTaskLoop()
	go mgr.checkRepairedAndClearLoop()
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
	if !mgr.hasRevised && mgr.hasRepairingDisk() {
		span.Infof("first collect task will revise repair task")
		err := mgr.reviseRepairTask(ctx, mgr.getRepairingDiskID())
		if err == nil {
			span.Infof("firstCollectTask finished")
			mgr.hasRevised = true
		}
		return
	}

	if mgr.hasRepairingDisk() {
		span.Infof("disk is repairing and skip collect task: disk_id[%d]", mgr.getRepairingDiskID())
		return
	}

	// it will retry the same disk when break in initOneTask phase,
	repairingDisks, err := mgr.clusterMgrCli.ListMigratingDisks(ctx, proto.TaskTypeDiskRepair)
	if err != nil {
		return
	}
	var brokenDisk *client.DiskInfoSimple

	switch len(repairingDisks) {
	case 1:
		brokenDisk = repairingDisks[0].Disk
	case 0:
		brokenDisk, err = mgr.acquireBrokenDisk(ctx)
		if err != nil {
			span.Info("acquire broken disk failed: err[%+v]", err)
			return
		}
		if brokenDisk == nil {
			return
		}
	default:
		span.Panicf("can not allow many disk repairing")
	}

	err = mgr.genDiskRepairTasks(ctx, brokenDisk, true)
	if err != nil {
		span.Errorf("generate disk repair tasks failed: err[%+v]", err)
		return
	}

	base.InsistOn(ctx, "set disk diskId %d repairing failed", func() error {
		return mgr.clusterMgrCli.SetDiskRepairing(ctx, brokenDisk.DiskID)
	})

	mgr.setRepairingDiskID(brokenDisk.DiskID)
}

func (mgr *DiskRepairMgr) reviseRepairTask(ctx context.Context, diskID proto.DiskID) error {
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
		execMsg := fmt.Sprintf("set disk diskId %d repairing", mgr.getRepairingDiskID())
		base.InsistOn(ctx, execMsg, func() error {
			return mgr.clusterMgrCli.SetDiskRepairing(ctx, diskID)
		})
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

	for _, t := range tasks {
		bads = append(bads, t.SourceVuid)
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
		TaskID:                  client.GenMigrateTaskID(proto.TaskTypeDiskRepair, brokenDiskID, badVuid.Vid()),
		TaskType:                proto.TaskTypeDiskRepair,
		State:                   proto.MigrateStateInited,
		SourceDiskID:            brokenDiskID,
		SourceVuid:              badVuid,
		SourceIDC:               brokenDiskIdc,
		ForbiddenDirectDownload: true,
	}
	base.InsistOn(ctx, "repair init one task insert task to tbl", func() error {
		return mgr.clusterMgrCli.AddMigrateTask(ctx, &t)
	})

	mgr.prepareQueue.PushTask(t.TaskID, &t)
	span.Infof("init repair task success %+v", t)
}

func (mgr *DiskRepairMgr) acquireBrokenDisk(ctx context.Context) (*client.DiskInfoSimple, error) {
	brokenDisks, err := mgr.clusterMgrCli.ListBrokenDisks(ctx, 1)
	if err != nil {
		return nil, err
	}
	if len(brokenDisks) == 0 {
		return nil, nil
	}

	return brokenDisks[0], nil
}

func (mgr *DiskRepairMgr) prepareTaskLoop() {
	for {
		mgr.WaitEnable()
		todo, doing := mgr.workQueue.StatsTasks()
		if !mgr.hasRepairingDisk() || todo+doing >= mgr.cfg.WorkQueueSize {
			time.Sleep(1 * time.Second)
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
	err = base.VolTaskLockerInst().TryLock(ctx, t.Vid())
	if err != nil {
		span.Warnf("tryLock failed: vid[%d]", t.Vid())
		return base.ErrVolNotOnlyOneTask
	}
	defer func() {
		if err != nil {
			span.Errorf("prepare task failed: task_id[%s], err[%+v]", t.TaskID, err)
			base.VolTaskLockerInst().Unlock(ctx, t.Vid())
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
	allocDstVunit, err := base.AllocVunitSafe(ctx, mgr.clusterMgrCli, badVuid, t.Sources)
	if err != nil {
		span.Errorf("repair alloc volume unit failed: err[%+v]", err)
		return err
	}

	t.CodeMode = volInfo.CodeMode
	t.Sources = volInfo.VunitLocations
	t.Destination = allocDstVunit.Location()
	t.State = proto.MigrateStatePrepared
	base.InsistOn(ctx, "repair prepare task update task tbl", func() error {
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, t)
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
	base.VolTaskLockerInst().Unlock(ctx, task.Vid())
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
	defer span.Finish()

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
		return mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
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
	base.VolTaskLockerInst().Unlock(ctx, task.Vid())

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

		newVunit, err := base.AllocVunitSafe(ctx, mgr.clusterMgrCli, task.SourceVuid, task.Sources)
		if err != nil {
			span.Errorf("realloc failed: vuid[%d], err[%+v]", task.SourceVuid, err)
			return err
		}
		task.SetDestination(newVunit.Location())
		task.State = proto.MigrateStatePrepared
		task.WorkerRedoCnt++

		base.InsistOn(ctx, "repair redo task update task tbl", func() error {
			return mgr.clusterMgrCli.UpdateMigrateTask(ctx, task)
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
	diskID := mgr.getRepairingDiskID()
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_repair.checkRepairedAndClear")
	defer span.Finish()

	if !mgr.hasRepairingDisk() {
		return
	}

	span.Infof("check repaired: disk_id[%d]", diskID)
	if mgr.checkRepaired(ctx, diskID) {
		err := mgr.clusterMgrCli.SetDiskRepaired(ctx, diskID)
		if err != nil {
			return
		}
		span.Infof("disk repaired will start clear: disk_id[%d]", diskID)
		base.InsistOn(ctx, "delete migrating disk fail", func() error {
			return mgr.clusterMgrCli.DeleteMigratingDisk(ctx, proto.TaskTypeDiskRepair, diskID)
		})
		mgr.emptyRepairingDiskID()
	}
}

func (mgr *DiskRepairMgr) checkRepaired(ctx context.Context, diskID proto.DiskID) bool {
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
		inited, prepared, finished := mgr.StatQueueTaskCnt()
		if inited+prepared+finished == 0 {
			// tasks may be inserted repeatedly due to network problems
			span.Panicf("there are duplicate tasks that require manual processing")
		}
		return false
	}
	if len(vunitInfos) != 0 && len(tasks) == 0 {
		// it may be occur when migration done and repair tasks generate concurrent, list volume units may not return the migrate unit
		span.Warnf("clustermgr has some volume unit not repair and revise again: disk_id[%d], volume units len[%d]", diskID, len(vunitInfos))
		if err = mgr.reviseRepairTask(ctx, diskID); err != nil {
			span.Errorf("revise repair task failed: err[%+v]", err)
		}
		return false
	}
	return len(tasks) == 0 && len(vunitInfos) == 0
}

func (mgr *DiskRepairMgr) setRepairingDiskID(diskID proto.DiskID) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.repairingDiskID = diskID
}

func (mgr *DiskRepairMgr) emptyRepairingDiskID() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.repairingDiskID = base.EmptyDiskID
}

func (mgr *DiskRepairMgr) getRepairingDiskID() proto.DiskID {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.repairingDiskID
}

func (mgr *DiskRepairMgr) hasRepairingDisk() bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.repairingDiskID != base.EmptyDiskID
}

// AcquireTask acquire repair task
func (mgr *DiskRepairMgr) AcquireTask(ctx context.Context, idc string) (task proto.MigrateTask, err error) {
	if !mgr.taskSwitch.Enabled() {
		return task, proto.ErrTaskPaused
	}

	_, repairTask, _ := mgr.workQueue.Acquire(idc)
	if repairTask != nil {
		task = *repairTask.(*proto.MigrateTask)
		return task, nil
	}
	return task, proto.ErrTaskEmpty
}

// CancelTask cancel repair task
func (mgr *DiskRepairMgr) CancelTask(ctx context.Context, args *api.OperateTaskArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	err := mgr.workQueue.Cancel(args.IDC, args.TaskID, args.Src, args.Dest)
	if err != nil {
		span.Errorf("cancel repair failed: task_id[%s], err[%+v]", args.TaskID, err)
	}

	mgr.taskStatsMgr.CancelTask()

	return err
}

// ReclaimTask reclaim repair task
func (mgr *DiskRepairMgr) ReclaimTask(ctx context.Context,
	idc, taskID string,
	src []proto.VunitLocation,
	oldDst proto.VunitLocation,
	newDst *client.AllocVunitInfo) error {
	span := trace.SpanFromContextSafe(ctx)

	err := mgr.workQueue.Reclaim(idc, taskID, src, oldDst, newDst.Location(), newDst.DiskID)
	if err != nil {
		// task has finished,because only complete will remove task from queue
		span.Errorf("reclaim repair task failed: task_id[%s], err[%+v]", taskID, err)
		return err
	}

	task, err := mgr.workQueue.Query(idc, taskID)
	if err != nil {
		span.Errorf("found task in workQueue failed: idc[%s], task_id[%s], err[%+v]", idc, taskID, err)
		return err
	}

	err = mgr.clusterMgrCli.UpdateMigrateTask(ctx, task.(*proto.MigrateTask))
	if err != nil {
		span.Warnf("update reclaim task failed: task_id[%s], err[%+v]", taskID, err)
	}

	mgr.taskStatsMgr.ReclaimTask()
	return nil
}

// CompleteTask complete repair task
func (mgr *DiskRepairMgr) CompleteTask(ctx context.Context, args *api.OperateTaskArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	completeTask, err := mgr.workQueue.Complete(args.IDC, args.TaskID, args.Src, args.Dest)
	if err != nil {
		span.Errorf("complete repair task failed: task_id[%s], err[%+v]", args.TaskID, err)
		return err
	}

	t := completeTask.(*proto.MigrateTask)
	t.State = proto.MigrateStateWorkCompleted

	mgr.finishQueue.PushTask(args.TaskID, t)
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

// ReportWorkerTaskStats reports task stats
func (mgr *DiskRepairMgr) ReportWorkerTaskStats(st *api.TaskReportArgs) {
	mgr.taskStatsMgr.ReportWorkerTaskStats(st.TaskID, st.TaskStats, st.IncreaseDataSizeByte, st.IncreaseShardCnt)
}

// QueryTask return task statistics
func (mgr *DiskRepairMgr) QueryTask(ctx context.Context, taskID string) (*api.MigrateTaskDetail, error) {
	detail := &api.MigrateTaskDetail{}
	taskInfo, err := mgr.clusterMgrCli.GetMigrateTask(ctx, proto.TaskTypeDiskRepair, taskID)
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
func (mgr *DiskRepairMgr) Progress(ctx context.Context) (repairingDiskID proto.DiskID, total, repaired int) {
	span := trace.SpanFromContextSafe(ctx)
	repairingDiskID = mgr.getRepairingDiskID()
	if repairingDiskID == base.EmptyDiskID {
		return base.EmptyDiskID, 0, 0
	}
	meta, err := mgr.clusterMgrCli.GetMigratingDisk(ctx, proto.TaskTypeDiskRepair, repairingDiskID)
	if err != nil {
		span.Errorf("find all task failed: err[%+v]", err)
		return repairingDiskID, 0, 0
	}
	total = int(meta.Disk.UsedChunkCnt)
	remainTasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskRepair, repairingDiskID)
	if err != nil {
		span.Errorf("find all task failed: err[%+v]", err)
		return repairingDiskID, 0, 0
	}
	repaired = total - len(remainTasks)
	return repairingDiskID, total, repaired
}
