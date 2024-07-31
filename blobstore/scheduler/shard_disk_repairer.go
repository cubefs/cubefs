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
	"fmt"
	"time"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

type ShardDiskMigrator interface {
	ShardMigrator

	Progress(ctx context.Context) (migratingDisks []proto.DiskID, total, migrated int)
	DiskProgress(ctx context.Context, diskID proto.DiskID) (stats *api.DiskMigratingStats, err error)
}

type ShardDiskRepairMgr struct {
	ShardMigrator

	repairedDisks  *migratedDisks
	repairingDisks *migratingShardDisks

	hasRevised bool

	clusterMgrCli client.ClusterMgrAPI

	cfg *ShardMigrateConfig
}

func NewShardDiskRepairMgr(
	cfg *ShardMigrateConfig,
	clusterMgrCli client.ClusterMgrAPI,
	taskSwitch taskswitch.ISwitcher) *ShardDiskRepairMgr {
	mgr := &ShardDiskRepairMgr{
		clusterMgrCli:  clusterMgrCli,
		repairingDisks: newMigratingShardDisks(),
		repairedDisks:  newMigratedDisks(),
	}
	cfg.loadTaskCallback = mgr.loadTaskCallback
	mgr.ShardMigrator = NewShardMigrateMgr(clusterMgrCli, taskSwitch, cfg, proto.TaskTypeShardDiskRepair)
	return mgr
}

// Load load repair task from database
func (mgr *ShardDiskRepairMgr) Load() error {
	span, ctx := trace.StartSpanFromContext(context.Background(), "shard_disk_repair.Load")
	diskInfos, err := mgr.clusterMgrCli.ListRepairingShardDisk(ctx)
	if err != nil {
		span.Errorf("list repairing shard disk failed, err: %s", err)
		return err
	}
	for _, disk := range diskInfos {
		mgr.repairingDisks.add(disk.DiskID, disk)
	}
	err = mgr.ShardMigrator.Load()
	if err != nil {
		return err
	}

	return nil
}

// Run shard disk repair task manager
func (mgr *ShardDiskRepairMgr) Run() {
	go mgr.collectTaskLoop()
	mgr.ShardMigrator.Run()
	go mgr.checkRepairedAndClearLoop()
	go mgr.checkAndClearJunkTasksLoop()
}

// Close shard disk repair task manager
func (mgr *ShardDiskRepairMgr) Close() {
	mgr.ShardMigrator.Close()
}

func (mgr *ShardDiskRepairMgr) Progress(ctx context.Context) (repairingDisk []proto.DiskID, total, migrated int) {
	span := trace.SpanFromContextSafe(ctx)
	repairingDisk = make([]proto.DiskID, 0)

	for _, disk := range mgr.repairingDisks.list() {
		total += int(disk.UsedShardCnt)
		remainTasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeShardDiskRepair, disk.DiskID)
		if err != nil {
			span.Errorf("find all task failed: err[%+v]", err)
			return repairingDisk, 0, 0
		}
		migrated += int(disk.UsedShardCnt) - len(remainTasks)
		repairingDisk = append(repairingDisk, disk.DiskID)
	}
	return
}

func (mgr *ShardDiskRepairMgr) DiskProgress(ctx context.Context, diskID proto.DiskID) (stats *api.DiskMigratingStats, err error) {
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
	stats.TotalTasksCnt = int(migratingDisk.UsedShardCnt)
	stats.MigratedTasksCnt = stats.TotalTasksCnt - len(remainTasks)
	return
}

func (mgr *ShardDiskRepairMgr) collectTaskLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CollectTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.ShardMigrator.WaitEnable()
			mgr.collectionTask()
		case <-mgr.ShardMigrator.Done():
			return
		}
	}
}

func (mgr *ShardDiskRepairMgr) collectionTask() {
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
	disk, err := mgr.acquireBrokenDisk(ctx)
	if err != nil {
		span.Warnf("acquire broken shard disk from clustermgr failed, err[%s]", err)
		return
	}
	if disk == nil {
		return
	}
	if err = mgr.generateTask(ctx, disk); err != nil {
		span.Errorf("generate shard disk repair tasks failed: err[%+v]", err)
		return
	}

	execMsg := fmt.Sprintf("set shard disk diskId %d repairing", disk.DiskID)
	base.InsistOn(ctx, execMsg, func() error {
		return mgr.clusterMgrCli.SetShardDiskRepairing(ctx, disk.DiskID)
	})

	mgr.repairingDisks.add(disk.DiskID, disk)
	span.Infof("init repair task for shard disk[%d] success", disk.DiskID)
}

func (mgr *ShardDiskRepairMgr) acquireBrokenDisk(ctx context.Context) (*client.ShardNodeDiskInfo, error) {
	diskInfos, err := mgr.clusterMgrCli.ListBrokenShardDisk(ctx)
	if err != nil {
		return nil, err
	}
	for _, disk := range diskInfos {
		if _, exist := mgr.repairingDisks.get(disk.DiskID); !exist {
			return disk, nil
		}
	}
	return nil, nil
}

// checkAndClearJunkTasksLoop due to network timeout, it may still have some junk migrate tasks in clustermgr,
// and we need to clear those tasks later
func (mgr *ShardDiskRepairMgr) checkAndClearJunkTasksLoop() {
	t := time.NewTicker(clearJunkMigrationTaskInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.checkAndClearJunkTasks()
		case <-mgr.ShardMigrator.Done():
			return
		}
	}
}

func (mgr *ShardDiskRepairMgr) checkAndClearJunkTasks() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "shard_disk_repair.clearJunkTasks")

	for _, disk := range mgr.repairedDisks.list() {
		if time.Since(disk.finishedTime) < junkMigrationTaskProtectionWindow {
			continue
		}
		span.Debugf("check repaired shard disk: disk_id[%d], repaired time[%v]", disk.diskID, disk.finishedTime)
		diskInfo, err := mgr.clusterMgrCli.GetShardDiskInfo(ctx, disk.diskID)
		if err != nil {
			span.Errorf("get disk info failed: disk_id[%d], err[%+v]", disk.diskID, err)
			continue
		}
		if !diskInfo.IsRepaired() {
			continue
		}
		tasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeShardDiskRepair, disk.diskID)
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

func (mgr *ShardDiskRepairMgr) checkRepairedAndClearLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CheckTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.WaitEnable()
			mgr.checkRepairedAndClear()
		case <-mgr.Done():
			return
		}
	}
}

func (mgr *ShardDiskRepairMgr) checkRepairedAndClear() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "disk_repair.checkRepairedAndClear")

	for _, disk := range mgr.repairingDisks.list() {
		if !mgr.checkDiskRepaired(ctx, disk.DiskID) {
			continue
		}
		err := mgr.clusterMgrCli.SetShardDiskRepaired(ctx, disk.DiskID)
		if err != nil {
			return
		}
		span.Infof("disk repaired will start clear: disk_id[%d]", disk.DiskID)
		// mgr.clearTasksByDiskID(ctx, disk.DiskID)
	}
}

func (mgr *ShardDiskRepairMgr) checkDiskRepaired(ctx context.Context, diskID proto.DiskID) bool {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("check repaired: disk_id[%d]", diskID)

	tasks, err := mgr.clusterMgrCli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeShardDiskRepair, diskID)
	if err != nil {
		span.Errorf("check repaired and find task failed: disk_iD[%d], err[%+v]", diskID, err)
		return false
	}
	sunitInfos, err := mgr.clusterMgrCli.ListDiskShardUnits(ctx, diskID)
	if err != nil {
		span.Errorf("check repaired list disk shard units failed: disk_id[%s], err[%+v]", diskID, err)
		return false
	}
	if len(sunitInfos) == 0 && len(tasks) != 0 {
		// due to network timeout, it may lead to repeated insertion of deleted tasks, and need to delete it again
		// mgr.clearJunkTasks(ctx, diskID, tasks)
		return false
	}
	if len(sunitInfos) != 0 && len(tasks) == 0 {
		// it may be occur when migration done and repair tasks generate concurrent, list volume units may not return the migrate unit
		span.Warnf("clustermgr has some shard unit not repair and revise again: disk_id[%d], shard units len[%d]", diskID, len(sunitInfos))
		if err = mgr.repairDisk(ctx, diskID); err != nil {
			span.Errorf("revise repair task failed: err[%+v]", err)
		}
		return false
	}
	return len(tasks) == 0 && len(sunitInfos) == 0
}

func (mgr *ShardDiskRepairMgr) loadTaskCallback(ctx context.Context, diskId proto.DiskID) {
	if _, exist := mgr.repairingDisks.get(diskId); exist {
		return
	}
	info, err := mgr.clusterMgrCli.GetShardDiskInfo(ctx, diskId)
	if err != nil {
		return
	}
	mgr.repairingDisks.add(diskId, &client.ShardNodeDiskInfo{
		ClusterID:    info.ClusterID,
		DiskID:       info.DiskID,
		Idc:          info.Idc,
		Rack:         info.Rack,
		Host:         info.Host,
		Status:       info.Status,
		Readonly:     info.Readonly,
		UsedShardCnt: info.UsedShardCnt,
		FreeShardCnt: info.FreeShardCnt,
	})
}

func (mgr *ShardDiskRepairMgr) reviseRepairDisks(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	for _, disk := range mgr.repairingDisks.list() {
		if err := mgr.repairDisk(ctx, disk.DiskID); err != nil {
			span.Errorf("revise repair shard tasks failed: disk_id[%d]", disk.DiskID)
			return err
		}
	}
	return nil
}

func (mgr *ShardDiskRepairMgr) repairDisk(ctx context.Context, diskID proto.DiskID) error {
	span := trace.SpanFromContextSafe(ctx)

	diskInfo, err := mgr.clusterMgrCli.GetShardDiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("get shard disk info failed: err[%+v]", err)
		return err
	}

	if err = mgr.generateTask(ctx, diskInfo); err != nil {
		span.Errorf("generate shard disk repair tasks failed: err[%+v]", err)
		return err
	}

	if diskInfo.IsBroken() {
		execMsg := fmt.Sprintf("set shard disk diskId %d repairing", diskID)
		base.InsistOn(ctx, execMsg, func() error {
			return mgr.clusterMgrCli.SetShardDiskRepairing(ctx, diskID)
		})
	}
	return nil
}

func (mgr *ShardDiskRepairMgr) generateTask(ctx context.Context, disk *client.ShardNodeDiskInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start generate shard disk repair tasks: disk_id[%d], disk_idc[%s]", disk.DiskID, disk.Idc)

	migratingSuids, err := mgr.ListMigratingSuid(ctx, disk.DiskID)
	if err != nil {
		span.Errorf("list repairing suids failed: err[%s]", err)
		return err
	}

	immigratedSuids, err := mgr.ListImmigratedSuid(ctx, disk.DiskID)
	if err != nil {
		span.Errorf("list un repaired suids failed: err[%s]", err)
		return err
	}

	remain := base.Sub(immigratedSuids, migratingSuids)
	span.Infof("should gen shard tasks remain: len[%d]", len(remain))
	for _, suid := range remain {
		mgr.initOneTask(ctx, suid, disk.DiskID, disk.Idc)
	}
	return nil
}

func (mgr *ShardDiskRepairMgr) initOneTask(ctx context.Context, suid proto.Suid, diskID proto.DiskID, idc string) {
	task := &proto.ShardMigrateTask{
		TaskID:    client.GenMigrateTaskID(proto.TaskTypeShardDiskRepair, diskID, uint32(suid.ShardID())),
		TaskType:  proto.TaskTypeShardDiskRepair,
		State:     proto.ShardTaskStateInited,
		SourceIDC: idc,
		Source:    proto.ShardUnitInfoSimple{Suid: suid, DiskID: diskID},
	}
	mgr.ShardMigrator.AddTask(ctx, task)
}
