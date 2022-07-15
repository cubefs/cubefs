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
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// DiskDropMgrConfig disk drop manager config
type DiskDropMgrConfig struct {
	MigrateConfig
}

// DiskDropMgr disk drop manager
type DiskDropMgr struct {
	IMigrator

	mu             sync.Mutex
	dropDisk       *client.DiskInfoSimple
	droppingDiskID proto.DiskID
	clusterMgrCli  client.ClusterMgrAPI
	hasRevised     bool
	cfg            *DiskDropMgrConfig
}

// NewDiskDropMgr returns disk drop manager
func NewDiskDropMgr(
	clusterMgrCli client.ClusterMgrAPI,
	volumeUpdater client.IVolumeUpdater,
	taskSwitch taskswitch.ISwitcher,
	taskTbl db.IMigrateTaskTable,
	conf *DiskDropMgrConfig) *DiskDropMgr {
	mgr := &DiskDropMgr{
		clusterMgrCli: clusterMgrCli,
		cfg:           conf,
	}
	mgr.IMigrator = NewMigrateMgr(clusterMgrCli, volumeUpdater, taskSwitch, taskTbl,
		&conf.MigrateConfig, proto.TaskTypeDiskDrop, conf.ClusterID)
	return mgr
}

// Load load disk drop task from database
func (mgr *DiskDropMgr) Load() (err error) {
	ctx := context.Background()
	allTasks, err := mgr.IMigrator.FindAll(ctx)
	if err != nil {
		return err
	}
	if len(allTasks) == 0 {
		log.Infof("no drop tasks in db")
		return
	}

	droppingDiskID := allTasks[0].GetSourceDiskID()
	tasks, err := mgr.IMigrator.FindByDiskID(ctx, droppingDiskID)
	if err != nil {
		return err
	}
	if len(allTasks) != len(tasks) {
		panic("can not allow many disk dropping")
	}

	mgr.setDroppingDiskID(droppingDiskID)

	return mgr.IMigrator.Load()
}

// Run run disk drop task
func (mgr *DiskDropMgr) Run() {
	go mgr.collectTaskLoop()
	mgr.IMigrator.Run()
	go mgr.checkDroppedAndClearLoop()
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
			mgr.setDroppingDiskID(mgr.getDroppingDiskID())
			return
		}
		span.Errorf("drop collect revise task failed: err[%+v]", err)
		return
	}

	if mgr.hasDroppingDisk() {
		return
	}

	dropDisk, err := mgr.acquireDropDisk(ctx)
	if err != nil {
		span.Info("acquire drop disk failed: err[%+v]", err)
		return
	}

	if dropDisk == nil {
		return
	}

	err = mgr.genDiskDropTasks(ctx, dropDisk.DiskID, dropDisk.Idc)
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

	err = mgr.genDiskDropTasks(ctx, diskInfo.DiskID, diskInfo.Idc)
	if err != nil {
		span.Errorf("gen disk drop tasks failed: err[%+v]", err)
		return err
	}
	return nil
}

func (mgr *DiskDropMgr) genDiskDropTasks(ctx context.Context, diskID proto.DiskID, diskIdc string) error {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("start generate disk drop tasks: disk_id[%d], disk_idc[%s]", diskID, diskIdc)

	vuidsDb, err := mgr.dropVuidsFromDb(ctx, diskID)
	if err != nil {
		span.Errorf("get drop vuids from db failed: err[%+v]", err)
		return err
	}
	span.Infof("drop vuids from db success: len[%d]", len(vuidsDb))

	vuidsCm, err := mgr.dropVuidsFromCm(ctx, diskID)
	if err != nil {
		span.Errorf("get drop vuid from clustermgr failed: err[%+v]", err)
		return err
	}
	span.Infof("drop vuids from clustermgr success: len[%d]", len(vuidsCm))

	remain := base.Subtraction(vuidsCm, vuidsDb)
	span.Infof("should gen tasks: remain len[%d]", len(remain))
	for _, vuid := range remain {
		mgr.initOneTask(ctx, vuid, diskID, diskIdc)
		span.Infof("init drop task success: vuid[%d]", vuid)
	}
	return nil
}

func (mgr *DiskDropMgr) dropVuidsFromDb(ctx context.Context, diskID proto.DiskID) (drops []proto.Vuid, err error) {
	tasks, err := mgr.IMigrator.FindByDiskID(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, t := range tasks {
		drops = append(drops, t.SourceVuid)
	}
	return drops, nil
}

func (mgr *DiskDropMgr) dropVuidsFromCm(ctx context.Context, diskID proto.DiskID) (drops []proto.Vuid, err error) {
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
	vid := src.Vid()
	t := proto.MigrateTask{
		TaskID:       mgr.genUniqTaskID(vid),
		TaskType:     proto.TaskTypeDiskDrop,
		State:        proto.MigrateStateInited,
		SourceDiskID: dropDiskID,
		SourceIDC:    diskIDC,
		SourceVuid:   src,
	}
	mgr.IMigrator.AddTask(ctx, &t)
}

func (mgr *DiskDropMgr) acquireDropDisk(ctx context.Context) (*client.DiskInfoSimple, error) {
	// it will retry when break in collectTask,
	// should make sure acquire same disk
	if mgr.dropDisk != nil {
		return mgr.dropDisk, nil
	}

	dropDisks, err := mgr.clusterMgrCli.ListDropDisks(ctx)
	if err != nil {
		return nil, err
	}
	if len(dropDisks) == 0 {
		return nil, nil
	}

	mgr.dropDisk = dropDisks[0]
	return mgr.dropDisk, nil
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

	tasks, err := mgr.IMigrator.FindByDiskID(ctx, diskID)
	if err != nil {
		span.Errorf("find all tasks failed: disk_id[%d], err[%+v]", diskID, err)
		return false
	}
	for _, task := range tasks {
		if !task.Finished() {
			return false
		}
	}
	span.Infof("disk drop has finished: disk_id[%d], task len[%d]", diskID, len(tasks))

	vunitInfos, err := mgr.clusterMgrCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		span.Errorf("list disk volume units failed: disk_id[%s], err[%+v]", diskID, err)
		return false
	}
	if len(vunitInfos) != 0 {
		// it may be occur when migration done and repair tasks generate concurrent, list volume units may not return the migrate unit
		span.Warnf("clustermgr has some volume unit not repair and revise again: disk_id[%d], volume units len[%d]", diskID, len(vunitInfos))
		if err = mgr.reviseDropTask(ctx, diskID); err != nil {
			span.Errorf("revise repair task failed: err[%+v]", err)
		}
		return false
	}
	return true
}

func (mgr *DiskDropMgr) clearTasksByDiskID(ctx context.Context, diskID proto.DiskID) {
	mgr.IMigrator.ClearTasksByDiskID(ctx, diskID)
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
	mgr.dropDisk = nil
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

func (mgr *DiskDropMgr) genUniqTaskID(vid proto.Vid) string {
	return base.GenTaskID("disk_drop", vid)
}

// Progress returns disk drop progress
func (mgr *DiskDropMgr) Progress(ctx context.Context) (dropDiskID proto.DiskID, total, dropped int) {
	span := trace.SpanFromContextSafe(ctx)

	dropDiskID = mgr.getDroppingDiskID()
	if dropDiskID == base.EmptyDiskID {
		return base.EmptyDiskID, 0, 0
	}

	allTasks, err := mgr.IMigrator.FindByDiskID(ctx, dropDiskID)
	if err != nil {
		span.Errorf("find all task failed: err[%+v]", err)
		return dropDiskID, 0, 0
	}
	total = len(allTasks)
	for _, task := range allTasks {
		if task.Finished() {
			dropped++
		}
	}
	return dropDiskID, total, dropped
}
