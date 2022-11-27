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
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

// DiskDropMgr disk drop manager
type DiskDropMgr struct {
	IMigrator

	mu             sync.Mutex
	droppingDiskID proto.DiskID
	clusterMgrCli  client.ClusterMgrAPI
	hasRevised     bool

	migratedDisks *migratedDisks

	cfg *MigrateConfig
}

// NewDiskDropMgr returns disk drop manager
func NewDiskDropMgr(clusterMgrCli client.ClusterMgrAPI, volumeUpdater client.IVolumeUpdater,
	taskSwitch taskswitch.ISwitcher, taskLogger recordlog.Encoder, conf *MigrateConfig) *DiskDropMgr {
	mgr := &DiskDropMgr{
		clusterMgrCli: clusterMgrCli,
		cfg:           conf,
		migratedDisks: newMigratedDisks(),
	}
	mgr.IMigrator = NewMigrateMgr(clusterMgrCli, volumeUpdater, taskSwitch, taskLogger, conf, proto.TaskTypeDiskDrop)
	return mgr
}

// Load load disk drop task from database
func (mgr *DiskDropMgr) Load() (err error) {
	ctx := context.Background()
	span := trace.SpanFromContextSafe(ctx)

	droppingDisks, err := mgr.clusterMgrCli.ListMigratingDisks(ctx, proto.TaskTypeDiskDrop)
	if err != nil {
		return err
	}
	if len(droppingDisks) == 0 {
		// load task
		tasks, err := mgr.clusterMgrCli.ListAllMigrateTasks(ctx, proto.TaskTypeDiskDrop)
		if err != nil {
			span.Errorf("find all tasks failed: err[%+v]", err)
			return err
		}
		return mgr.clearJunkTasksWhenLoading(ctx, tasks)
	}

	if len(droppingDisks) > 1 {
		return errors.New("can not allow many disk dropping")
	}
	droppingDiskID := droppingDisks[0].Disk.DiskID
	mgr.setDroppingDiskID(droppingDiskID)

	return mgr.IMigrator.Load()
}

func (mgr *DiskDropMgr) clearJunkTasksWhenLoading(ctx context.Context, tasks []*proto.MigrateTask) error {
	span := trace.SpanFromContextSafe(ctx)

	disks := make(map[proto.DiskID]bool)
	for _, task := range tasks {
		if _, ok := disks[task.SourceDiskID]; !ok {
			diskInfo, err := mgr.clusterMgrCli.GetDiskInfo(ctx, task.SourceDiskID)
			if err != nil {
				return err
			}
			disks[task.SourceDiskID] = diskInfo.IsDropped()
		}
		if !disks[task.SourceDiskID] {
			span.Errorf("has junk task but the disk is not dropped: disk_id[%d], task_id[%s]", task.SourceDiskID, task.TaskID)
			return errors.New("unexpect migration task")
		}
		span.Warnf("delete junk task: task_id[%s]", task.TaskID)
		base.InsistOn(ctx, "delete junk task", func() error {
			return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
		})
	}
	return nil
}

// Run run disk drop task
func (mgr *DiskDropMgr) Run() {
	go mgr.collectTaskLoop()
	mgr.IMigrator.Run()
	go mgr.checkDroppedAndClearLoop()
	go mgr.checkAndClearJunkTasksLoop()
}

// collectTaskLoop collect disk drop task loop
func (mgr *DiskDropMgr) collectTaskLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CollectTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.IMigrator.WaitEnable()
			mgr.collectTask()
		case <-mgr.IMigrator.Done():
			return
		}
	}
}

func (mgr *DiskDropMgr) collectTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_drop.collectTask")
	defer span.Finish()

	if !mgr.hasRevised && mgr.hasDroppingDisk() {
		err := mgr.reviseDropTask(ctx, mgr.getDroppingDiskID())
		if err == nil {
			span.Infof("drop collect revise tasks success")
			mgr.hasRevised = true
			return
		}
		span.Errorf("drop collect revise task failed: err[%+v]", err)
		return
	}

	if mgr.hasDroppingDisk() {
		return
	}

	// it will retry the same disk when break in initOneTask phase,
	droppingDisks, err := mgr.clusterMgrCli.ListMigratingDisks(ctx, proto.TaskTypeDiskDrop)
	if err != nil {
		return
	}
	var dropDisk *client.DiskInfoSimple

	switch len(droppingDisks) {
	case 1:
		dropDisk = droppingDisks[0].Disk
	case 0:
		dropDisk, err = mgr.acquireDropDisk(ctx)
		if err != nil {
			span.Info("acquire drop disk failed: err[%+v]", err)
			return
		}
		if dropDisk == nil {
			return
		}
	default:
		span.Panicf("can not allow many disk dropping")
	}

	err = mgr.genDiskDropTasks(ctx, dropDisk, true)
	if err != nil {
		span.Errorf("drop collect drop task failed: err[%+v]", err)
		return
	}

	mgr.setDroppingDiskID(dropDisk.DiskID)
}

func (mgr *DiskDropMgr) reviseDropTask(ctx context.Context, diskID proto.DiskID) error {
	span := trace.SpanFromContextSafe(ctx)

	diskInfo, err := mgr.clusterMgrCli.GetDiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("get disk info failed: err[%+v]", err)
		return err
	}

	err = mgr.genDiskDropTasks(ctx, diskInfo, false)
	if err != nil {
		span.Errorf("gen disk drop tasks failed: err[%+v]", err)
		return err
	}
	return nil
}

func (mgr *DiskDropMgr) genDiskDropTasks(ctx context.Context, disk *client.DiskInfoSimple, newDropDisk bool) error {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("start generate disk drop tasks: disk_id[%d], disk_idc[%s]", disk.DiskID, disk.Idc)

	migratingVuids, err := mgr.listMigratingVuid(ctx, disk.DiskID)
	if err != nil {
		span.Errorf("list migrating vuids failed: err[%+v]", err)
		return err
	}

	unmigratedvuids, err := mgr.listUnmigratedVuid(ctx, disk.DiskID)
	if err != nil {
		span.Errorf("list un migrating vuids failed: err[%+v]", err)
		return err
	}

	remain := base.Subtraction(unmigratedvuids, migratingVuids)
	span.Infof("should gen tasks: remain len[%d]", len(remain))
	if newDropDisk {
		meta := &client.MigratingDiskMeta{
			TaskType: proto.TaskTypeDiskDrop,
			Disk:     disk,
		}
		if err := mgr.clusterMgrCli.AddMigratingDisk(ctx, meta); err != nil {
			return err
		}
	}
	for _, vuid := range remain {
		mgr.initOneTask(ctx, vuid, disk.DiskID, disk.Idc)
		span.Infof("init drop task success: vuid[%d]", vuid)
	}
	return nil
}

func (mgr *DiskDropMgr) listMigratingVuid(ctx context.Context, diskID proto.DiskID) (drops []proto.Vuid, err error) {
	tasks, err := mgr.IMigrator.ListAllTaskByDiskID(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, t := range tasks {
		drops = append(drops, t.SourceVuid)
	}
	return drops, nil
}

func (mgr *DiskDropMgr) listUnmigratedVuid(ctx context.Context, diskID proto.DiskID) (drops []proto.Vuid, err error) {
	vunits, err := mgr.clusterMgrCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, vunit := range vunits {
		drops = append(drops, vunit.Vuid)
	}
	return drops, nil
}

func (mgr *DiskDropMgr) initOneTask(ctx context.Context, src proto.Vuid, dropDiskID proto.DiskID, diskIDC string) {
	t := proto.MigrateTask{
		TaskID:       client.GenMigrateTaskID(proto.TaskTypeDiskDrop, dropDiskID, src.Vid()),
		TaskType:     proto.TaskTypeDiskDrop,
		State:        proto.MigrateStateInited,
		SourceDiskID: dropDiskID,
		SourceIDC:    diskIDC,
		SourceVuid:   src,
	}
	mgr.IMigrator.AddTask(ctx, &t)
}

func (mgr *DiskDropMgr) acquireDropDisk(ctx context.Context) (*client.DiskInfoSimple, error) {
	dropDisks, err := mgr.clusterMgrCli.ListDropDisks(ctx)
	if err != nil {
		return nil, err
	}
	if len(dropDisks) == 0 {
		return nil, nil
	}

	return dropDisks[0], nil
}

func (mgr *DiskDropMgr) checkDroppedAndClearLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CheckTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.IMigrator.WaitEnable()
			mgr.checkDroppedAndClear()
		case <-mgr.IMigrator.Done():
			return
		}
	}
}

func (mgr *DiskDropMgr) checkDroppedAndClear() {
	diskID := mgr.getDroppingDiskID()

	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_drop.checkDroppedAndClear")
	defer span.Finish()

	if !mgr.hasDroppingDisk() {
		return
	}
	if mgr.checkDropped(ctx, diskID) {
		err := mgr.clusterMgrCli.SetDiskDropped(ctx, diskID)
		if err != nil {
			span.Errorf("set disk dropped failed: err[%+v]", err)
			return
		}
		span.Infof("start clear dropped disk: disk_id[%d]", diskID)
		mgr.clearTasksByDiskID(ctx, diskID)
		mgr.emptyDroppingDiskID()
	}
}

func (mgr *DiskDropMgr) checkDropped(ctx context.Context, diskID proto.DiskID) bool {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("check dropped: disk_id[%d]", diskID)

	tasks, err := mgr.IMigrator.ListAllTaskByDiskID(ctx, diskID)
	if err != nil {
		span.Errorf("find all tasks failed: disk_id[%d], err[%+v]", diskID, err)
		return false
	}
	vunitInfos, err := mgr.clusterMgrCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		span.Errorf("list disk volume units failed: disk_id[%s], err[%+v]", diskID, err)
		return false
	}
	if len(vunitInfos) == 0 && len(tasks) != 0 {
		// due to network timeout, it may lead to repeated insertion of deleted tasks, and need to delete it again
		mgr.clearJunkTasks(ctx, diskID, tasks)
		return false
	}
	if len(tasks) == 0 && len(vunitInfos) != 0 {
		// it may be occur when migration done and disk drop tasks generate concurrent, list volume units may not return the migrate unit
		span.Warnf("clustermgr has some volume unit not migrate and revise again: disk_id[%d], volume units len[%d]", diskID, len(vunitInfos))
		if err = mgr.reviseDropTask(ctx, diskID); err != nil {
			span.Errorf("revise disk drop task failed: err[%+v]", err)
		}
		return false
	}
	return len(tasks) == 0 && len(vunitInfos) == 0
}

func (mgr *DiskDropMgr) clearJunkTasks(ctx context.Context, diskID proto.DiskID, tasks []*proto.MigrateTask) {
	span := trace.SpanFromContextSafe(ctx)
	for _, task := range tasks {
		if !mgr.IsDeletedTask(task) {
			continue
		}
		span.Warnf("delete junk task: task_id[%s]", task.TaskID)
		base.InsistOn(ctx, "delete junk task", func() error {
			return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
		})
	}
}

func (mgr *DiskDropMgr) clearTasksByDiskID(ctx context.Context, diskID proto.DiskID) {
	base.InsistOn(ctx, "delete migrating disk fail", func() error {
		return mgr.clusterMgrCli.DeleteMigratingDisk(ctx, proto.TaskTypeDiskDrop, diskID)
	})
	mgr.ClearDeletedTasks(diskID)
	mgr.migratedDisks.add(diskID, time.Now())
}

// checkAndClearJunkTasksLoop due to network timeout, the dropped disk may still have some junk migrate tasks in clustermgr,
// and we need to clear those tasks later
func (mgr *DiskDropMgr) checkAndClearJunkTasksLoop() {
	t := time.NewTicker(clearJunkMigrationTaskInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.checkAndClearJunkTasks()
		case <-mgr.IMigrator.Done():
			return
		}
	}
}

func (mgr *DiskDropMgr) checkAndClearJunkTasks() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_drop.clearJunkTasks")

	for _, disk := range mgr.migratedDisks.list() {
		if time.Since(disk.finishedTime) < junkMigrationTaskProtectionWindow {
			continue
		}
		span.Debugf("check dropped disk: disk_id[%d], dropped time[%v]", disk.diskID, disk.finishedTime)
		diskInfo, err := mgr.clusterMgrCli.GetDiskInfo(ctx, disk.diskID)
		if err != nil {
			span.Errorf("get disk info failed: disk_id[%d], err[%+v]", disk.diskID, err)
			continue
		}
		if !diskInfo.IsDropped() {
			continue
		}
		tasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskDrop, disk.diskID)
		if err != nil {
			continue
		}
		if len(tasks) != 0 {
			span.Warnf("clear junk tasks of dropped disk: disk_id[%d], tasks size[%d]", disk.diskID, len(tasks))
			for _, task := range tasks {
				span.Warnf("delete junk task: task_id[%s]", task.TaskID)
				base.InsistOn(ctx, "delete junk task", func() error {
					return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
				})
			}
		}
		mgr.migratedDisks.delete(disk.diskID)
	}
}

func (mgr *DiskDropMgr) setDroppingDiskID(diskID proto.DiskID) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.droppingDiskID = diskID
}

func (mgr *DiskDropMgr) emptyDroppingDiskID() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.droppingDiskID = base.EmptyDiskID
}

func (mgr *DiskDropMgr) getDroppingDiskID() proto.DiskID {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.droppingDiskID
}

func (mgr *DiskDropMgr) hasDroppingDisk() bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.droppingDiskID != base.EmptyDiskID
}

// Progress returns disk drop progress
func (mgr *DiskDropMgr) Progress(ctx context.Context) (dropDiskID proto.DiskID, total, dropped int) {
	span := trace.SpanFromContextSafe(ctx)

	dropDiskID = mgr.getDroppingDiskID()
	if dropDiskID == base.EmptyDiskID {
		return base.EmptyDiskID, 0, 0
	}

	meta, err := mgr.clusterMgrCli.GetMigratingDisk(ctx, proto.TaskTypeDiskDrop, dropDiskID)
	if err != nil {
		span.Errorf("find all task failed: err[%+v]", err)
		return dropDiskID, 0, 0
	}
	total = int(meta.Disk.UsedChunkCnt)
	remainTasks, err := mgr.IMigrator.ListAllTaskByDiskID(ctx, dropDiskID)
	if err != nil {
		span.Errorf("find remain task failed: err[%+v]", err)
		return dropDiskID, 0, 0
	}
	dropped = total - len(remainTasks)
	return dropDiskID, total, dropped
}
