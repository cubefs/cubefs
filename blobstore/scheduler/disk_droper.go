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
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

type DropMgrConfig struct {
	*MigrateConfig

	TotalTaskLimit   int `json:"total_task_limit"`
	TaskLimitPerDisk int `json:"task_limit_per_disk"`
}

type dropDisk struct {
	*client.DiskInfoSimple
	collect bool

	lock sync.RWMutex
	done chan struct{}
}

func (d *dropDisk) waitDone() {
	<-d.done
}

func (d *dropDisk) collecting() {
	d.lock.Lock()
	d.collect = true
	d.lock.Unlock()
}

func (d *dropDisk) collected() {
	d.lock.Lock()
	d.collect = false
	d.lock.Unlock()
}

func (d *dropDisk) isCollecting() bool {
	d.lock.RLock()
	res := d.collect
	d.lock.RUnlock()
	return res
}

type dropDiskMap struct {
	disks map[proto.DiskID]*dropDisk
	lock  sync.RWMutex
}

func (d *dropDiskMap) add(disk *dropDisk) {
	d.lock.Lock()
	d.disks[disk.DiskID] = disk
	d.lock.Unlock()
}

func (d *dropDiskMap) get(diskId proto.DiskID) *dropDisk {
	d.lock.RLock()
	disk := d.disks[diskId]
	d.lock.RUnlock()
	return disk
}
func (d *dropDiskMap) isCollecting(diskId proto.DiskID) bool {
	d.lock.RLock()
	disk := d.disks[diskId]
	d.lock.RUnlock()
	return disk.isCollecting()
}

func newDropDisks() *dropDiskMap {
	return &dropDiskMap{
		disks: make(map[proto.DiskID]*dropDisk),
		lock:  sync.RWMutex{},
	}
}

// DiskDropMgr disk drop manager
type DiskDropMgr struct {
	IMigrator

	allDisks        *dropDiskMap
	dropFailedDisks *migratingDisks
	droppingDisks   *migratingDisks
	droppedDisks    *migratedDisks

	totalTaskLimit   limit.Limiter
	taskLimitPerDisk limit.Limiter
	prepareTaskPool  taskpool.TaskPool
	clusterTopology  IClusterTopology

	clusterMgrCli client.ClusterMgrAPI
	hasRevised    bool

	cfg *DropMgrConfig
}

// NewDiskDropMgr returns disk drop manager
func NewDiskDropMgr(clusterMgrCli client.ClusterMgrAPI, volumeUpdater client.IVolumeUpdater,
	taskSwitch taskswitch.ISwitcher, taskLogger recordlog.Encoder, conf *DropMgrConfig, clusterTopology IClusterTopology) *DiskDropMgr {
	mgr := &DiskDropMgr{
		clusterMgrCli:   clusterMgrCli,
		cfg:             conf,
		allDisks:        newDropDisks(),
		dropFailedDisks: newMigratingDisks(),
		droppedDisks:    newMigratedDisks(),
		droppingDisks:   newMigratingDisks(),

		prepareTaskPool:  taskpool.New(conf.DiskConcurrency, conf.DiskConcurrency*2),
		totalTaskLimit:   count.NewBlockingCount(conf.TotalTaskLimit),
		taskLimitPerDisk: keycount.New(conf.TaskLimitPerDisk),
		clusterTopology:  clusterTopology,
	}
	mgr.IMigrator = NewMigrateMgr(clusterMgrCli, volumeUpdater, taskSwitch,
		taskLogger, conf.MigrateConfig, proto.TaskTypeDiskDrop, mgr.releaseTaskLimit)
	mgr.SetClearJunkTasksWhenLoadingFunc(mgr.clearJunkTasksWhenLoading)
	return mgr
}

// Load load disk drop task from database
func (mgr *DiskDropMgr) Load() (err error) {
	ctx := context.Background()

	toDropDisks, err := mgr.clusterMgrCli.ListMigratingDisks(ctx, proto.TaskTypeDiskDrop)
	if err != nil {
		return err
	}

	for _, disk := range toDropDisks {
		mgr.allDisks.add(&dropDisk{done: make(chan struct{}), DiskInfoSimple: disk.Disk})
	}

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
			return errcode.ErrUnexpectMigrationTask
		}
		span.Warnf("loading delete junk task: task_id[%s]", task.TaskID)
		base.InsistOn(ctx, "loading delete junk task", func() error {
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

	toDropDisks := mgr.dropFailedDisks.list()
	disks, err := mgr.clusterMgrCli.ListDropDisks(ctx)
	if err != nil {
		span.Errorf("getr drop disk list failed, err: %s", err)
	}
	for _, disk := range disks {
		if d := mgr.allDisks.get(disk.DiskID); d == nil {
			mgr.allDisks.add(&dropDisk{done: make(chan struct{}), DiskInfoSimple: disk})
		}
		if _, exist := mgr.dropFailedDisks.get(disk.DiskID); !exist {
			toDropDisks = append(toDropDisks, disk)
		}
	}
	if mgr.dropFailedDisks.size() != 0 {
		mgr.dropFailedDisks = newMigratingDisks()
	}

	if len(toDropDisks) == 0 {
		return
	}
	for _, disk := range toDropDisks {
		dDisk := mgr.allDisks.get(disk.DiskID)
		if dDisk.isCollecting() {
			continue
		}
		newDisk := disk
		mgr.prepareTaskPool.Run(func() {
			dDisk.collecting()
			taskSpan, ctx := trace.StartSpanFromContextWithTraceID(context.Background(),
				fmt.Sprintf("drop-disk[%d]", newDisk.DiskID), "")
			err := mgr.loopGenerateTask(ctx, newDisk)
			if err != nil {
				taskSpan.Errorf("loop generate task failed for disk[%d], err: %s", newDisk.DiskID, err)
				return
			}
			dDisk.waitDone()
			dDisk.collected()
		})
	}
}

func (mgr *DiskDropMgr) releaseTaskLimit(diskId proto.DiskID) {
	mgr.totalTaskLimit.Release()
	mgr.taskLimitPerDisk.Release(diskId)
}

func (mgr *DiskDropMgr) loopGenerateTask(ctx context.Context, disk *client.DiskInfoSimple) error {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("start generate disk drop tasks: disk_id[%d], disk_idc[%s]", disk.DiskID, disk.Idc)

	migratingVuids, err := mgr.listMigratingVuid(ctx, disk.DiskID)
	if err != nil {
		span.Errorf("list migrating vuids failed: err[%+v]", err)
		return err
	}

	unmigratedvuids, err := mgr.listUnMigratedVuid(ctx, disk.DiskID)
	if err != nil {
		span.Errorf("list un migrating vuids failed: err[%+v]", err)
		return err
	}

	remain := base.Subtraction(unmigratedvuids, migratingVuids)
	span.Infof("should gen tasks: remain len[%d]", len(remain))

	if len(unmigratedvuids) == len(remain) {
		meta := &client.MigratingDiskMeta{
			TaskType: proto.TaskTypeDiskDrop,
			Disk:     disk,
		}
		if err := mgr.clusterMgrCli.AddMigratingDisk(ctx, meta); err != nil {
			return err
		}
	}
	if len(remain) == 0 {
		return nil
	}

	var retryVuids []proto.Vuid

RETRY:
	if len(retryVuids) != 0 {
		remain = retryVuids
		retryVuids = nil
	}

	for _, vuid := range remain {
		migrated := false
		vid := vuid.Vid()
		volume, err := mgr.clusterTopology.GetVolume(vid)
		if err != nil {
			retryVuids = append(retryVuids, vuid)
			continue
		}
		for _, location := range volume.VunitLocations {
			if location.Vuid.Index() == vuid.Index() {
				if location.Vuid != vuid {
					migrated = true
				}
				break
			}
		}
		if migrated {
			continue
		}

		mgr.WaitEnable()
		_ = mgr.totalTaskLimit.Acquire()
		if err := mgr.taskLimitPerDisk.Acquire(disk.DiskID); err != nil {
			mgr.totalTaskLimit.Release()
			retryVuids = append(retryVuids, vuid)
			time.Sleep(time.Millisecond * 10)
			continue
		}

		mgr.initOneTask(ctx, vuid, disk.DiskID, disk.Idc)
		span.Infof("init drop task success: vuid[%d]", vuid)
	}
	if len(retryVuids) != 0 {
		goto RETRY
	}

	mgr.droppingDisks.add(disk.DiskID, disk)
	mgr.dropFailedDisks.delete(disk.DiskID)
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

func (mgr *DiskDropMgr) listUnMigratedVuid(ctx context.Context, diskID proto.DiskID) (drops []proto.Vuid, err error) {
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
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_drop.checkDroppedAndClear")

	for _, disk := range mgr.droppingDisks.list() {
		if !mgr.checkDiskDropped(ctx, disk.DiskID) {
			continue
		}
		err := mgr.clusterMgrCli.SetDiskDropped(ctx, disk.DiskID)
		if err != nil {
			span.Errorf("set disk dropped failed: err[%+v]", err)
			return
		}
		span.Infof("start clear dropped disk: disk_id[%d]", disk.DiskID)
		mgr.clearTasksByDiskID(ctx, disk.DiskID)
	}
}

func (mgr *DiskDropMgr) checkDiskDropped(ctx context.Context, diskID proto.DiskID) bool {
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
		span.Warnf("clustermgr has some volume unit not migrate and remigrate again: disk_id[%d], volume units len[%d]", diskID, len(vunitInfos))
		disk, _ := mgr.droppingDisks.get(diskID)
		mgr.dropFailedDisks.add(diskID, disk)
		mgr.droppingDisks.delete(diskID)
		// need exit colleting goroutine
		mgr.allDisks.get(diskID).done <- struct{}{}
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
	mgr.droppedDisks.add(diskID, time.Now())
	mgr.droppingDisks.delete(diskID)
	close(mgr.allDisks.get(diskID).done)
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

	for _, disk := range mgr.droppedDisks.list() {
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
				span.Warnf("check and delete junk task: task_id[%s]", task.TaskID)
				base.InsistOn(ctx, "check and delete junk task", func() error {
					return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
				})
			}
		}
		mgr.droppedDisks.delete(disk.diskID)
	}
}

func (mgr *DiskDropMgr) getUnDroppingDisk(disks []*client.DiskInfoSimple) *client.DiskInfoSimple {
	for _, v := range disks {
		if _, ok := mgr.droppingDisks.get(v.DiskID); !ok {
			return v
		}
	}
	return nil
}

// Progress returns disk drop progress
func (mgr *DiskDropMgr) Progress(ctx context.Context) (migratingDisks []proto.DiskID, total, migrated int) {
	span := trace.SpanFromContextSafe(ctx)
	migratingDisks = make([]proto.DiskID, 0)

	for _, disk := range mgr.droppingDisks.list() {
		total += int(disk.UsedChunkCnt)
		remainTasks, err := mgr.IMigrator.ListAllTaskByDiskID(ctx, disk.DiskID)
		if err != nil {
			span.Errorf("find remain task failed: disk_id[%d], err[%+v]", disk.DiskID, err)
			return migratingDisks, 0, 0
		}
		migrated += int(disk.UsedChunkCnt) - len(remainTasks)
		migratingDisks = append(migratingDisks, disk.DiskID)
	}
	return
}

func (mgr *DiskDropMgr) DiskProgress(ctx context.Context, diskID proto.DiskID) (stats *api.DiskMigratingStats, err error) {
	span := trace.SpanFromContextSafe(ctx)

	migratingDisk, ok := mgr.droppingDisks.get(diskID)
	if !ok {
		err = errors.New("not dropping disk")
		return
	}
	remainTasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeDiskDrop, diskID)
	if err != nil {
		span.Errorf("find all task failed: err[%+v]", err)
		return
	}
	stats = &api.DiskMigratingStats{}
	stats.TotalTasksCnt = int(migratingDisk.UsedChunkCnt)
	stats.MigratedTasksCnt = stats.TotalTasksCnt - len(remainTasks)
	return
}
