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
	"net/http"
	"sort"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	collectBalanceTaskPauseS = 5
)

var (
	// ErrNoBalanceVunit no balance volume unit on disk
	ErrNoBalanceVunit = errors.New("no balance volume unit on disk")
	// ErrTooManyBalancingTasks too many balancing tasks
	ErrTooManyBalancingTasks = errors.New("too many balancing tasks")
)

// BalanceMgrConfig balance task manager config
type BalanceMgrConfig struct {
	MaxDiskFreeChunkCnt        int64   `json:"max_disk_free_chunk_cnt"`
	MinDiskFreeChunkCnt        int64   `json:"min_disk_free_chunk_cnt"`
	DiskUsageThreshold         float64 `json:"disk_usage_threshold"`           // 0 means disabled, e.g. 0.9
	CompactMigrateHoleRate     float64 `json:"compact_migrate_hole_rate"`      // hole rate threshold for large chunks, e.g. 0.6
	CompactMigrateMinLogicSize uint64  `json:"compact_migrate_min_logic_size"` // bytes, large chunk boundary, default 16GiB
	MigrateConfig
}

// BalanceMgr balance manager
type BalanceMgr struct {
	IMigrator

	clusterTopology IClusterTopology
	clusterMgrCli   client.ClusterMgrAPI
	priorityVuids   map[proto.Vuid]*client.DiskInfoSimple

	cfg *BalanceMgrConfig
}

// NewBalanceMgr returns balance manager
func NewBalanceMgr(clusterMgrCli client.ClusterMgrAPI, volumeUpdater client.TaskAPI, taskSwitch taskswitch.ISwitcher,
	clusterTopology IClusterTopology, taskLogger recordlog.Encoder, conf *BalanceMgrConfig,
) *BalanceMgr {
	mgr := &BalanceMgr{
		clusterTopology: clusterTopology,
		clusterMgrCli:   clusterMgrCli,
		cfg:             conf,
		priorityVuids:   make(map[proto.Vuid]*client.DiskInfoSimple),
	}
	conf.MigrateConfig.IsBalanceAlloc = true
	mgr.IMigrator = NewMigrateMgr(clusterMgrCli, volumeUpdater, taskSwitch, taskLogger,
		&conf.MigrateConfig, proto.TaskTypeBalance, clusterTopology)
	return mgr
}

// Run run balance task manager
func (mgr *BalanceMgr) Run() {
	go mgr.collectTaskLoop()
	mgr.IMigrator.Run()
	go mgr.checkAndClearJunkTasksLoop()
}

// Close close balance task manager
func (mgr *BalanceMgr) Close() {
	mgr.clusterTopology.Close()
	mgr.IMigrator.Close()
}

func (mgr *BalanceMgr) collectTaskLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CollectTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.IMigrator.WaitEnable()
			err := mgr.collectionTask()
			if err == ErrTooManyBalancingTasks || err == ErrNoBalanceVunit {
				log.Debugf("no task to collect and sleep: sleep second[%d], err[%+v]", collectBalanceTaskPauseS, err)
				time.Sleep(time.Duration(collectBalanceTaskPauseS) * time.Second)
			}
		case <-mgr.IMigrator.Done():
			return
		}
	}
}

func (mgr *BalanceMgr) collectionTask() (err error) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "balance_collectionTask")
	defer span.Finish()

	needBalanceDiskCnt := mgr.cfg.DiskConcurrency - mgr.IMigrator.GetMigratingDiskNum()
	if needBalanceDiskCnt <= 0 {
		span.Warnf("the number of balancing disk is greater than config: current[%d], conf[%d]",
			mgr.IMigrator.GetMigratingDiskNum(), mgr.cfg.DiskConcurrency)
		return ErrTooManyBalancingTasks
	}

	balanceDiskCnt := 0
	for vuid, disk := range mgr.priorityVuids {
		if mgr.IMigrator.IsMigratingDisk(disk.DiskID) {
			continue
		}
		volInfo, err := mgr.clusterMgrCli.GetVolumeInfo(ctx, vuid.Vid())
		if err != nil {
			span.Errorf("get volume info failed: vid[%d], err[%+v]", vuid.Vid(), err)
			continue
		}
		if !volInfo.IsIdle() {
			continue
		}
		if err = mgr.generateTask(ctx, vuid, disk); err != nil {
			continue
		}
		span.Debugf("add balance task from priority vuid[%d] success, disk[%d]", vuid, disk.DiskID)
		balanceDiskCnt++
		if balanceDiskCnt >= needBalanceDiskCnt {
			return nil
		}
	}

	// select balance disks
	disks := mgr.selectDisks(ctx, mgr.cfg.MaxDiskFreeChunkCnt, mgr.cfg.MinDiskFreeChunkCnt)
	span.Debugf("select balance disks: len[%d]", len(disks))

	for _, disk := range disks {
		if err = mgr.genOneBalanceTask(ctx, disk); err != nil {
			continue
		}
		balanceDiskCnt++
		if balanceDiskCnt >= needBalanceDiskCnt {
			return nil
		}
	}
	if balanceDiskCnt == 0 {
		span.Infof("select disks has no balance volume unit on disk: len[%d]", len(disks))
		return ErrNoBalanceVunit
	}
	return nil
}

func (mgr *BalanceMgr) selectDisks(ctx context.Context, maxFreeChunkCnt, minFreeChunkCnt int64) []*client.DiskInfoSimple {
	span := trace.SpanFromContextSafe(ctx)
	var allDisks []*client.DiskInfoSimple
	for idcName := range mgr.clusterTopology.GetIDCs() {
		maxFreeChunksDisk := mgr.clusterTopology.MaxFreeChunksDisk(idcName)
		if maxFreeChunksDisk != nil && maxFreeChunksDisk.FreeChunkCnt >= maxFreeChunkCnt {
			allDisks = append(allDisks, mgr.clusterTopology.GetIDCDisks(idcName)...)
		}
	}
	sortDiskByFreeChunkCnt(allDisks)

	var selected []*client.DiskInfoSimple
	for _, disk := range allDisks {
		if !disk.IsHealth() || mgr.IMigrator.IsMigratingDisk(disk.DiskID) {
			continue
		}
		if disk.FreeChunkCnt < minFreeChunkCnt {
			selected = append(selected, disk)
			span.Debugf("select balance disk for free chunk count, disk[%d], free[%d]", disk.DiskID, disk.FreeChunkCnt)
			continue
		}
		if mgr.cfg.DiskUsageThreshold > 0 && disk.UsageRatio() >= mgr.cfg.DiskUsageThreshold {
			selected = append(selected, disk)
			span.Debugf("select balance disk for disk usage, disk[%d], usage[%f]", disk.DiskID, disk.UsageRatio())
		}

	}
	return selected
}

func (mgr *BalanceMgr) genOneBalanceTask(ctx context.Context, diskInfo *client.DiskInfoSimple) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	vuid, err := mgr.selectBalanceVunit(ctx, diskInfo)
	if err != nil {
		span.Errorf("generate task source failed: disk_id[%d], err[%+v]", diskInfo.DiskID, err)
		return
	}
	return mgr.generateTask(ctx, vuid, diskInfo)
}

func (mgr *BalanceMgr) generateTask(ctx context.Context, vuid proto.Vuid, disk *client.DiskInfoSimple) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	if mgr.IMigrator.IsTaskExist(disk.DiskID, vuid) {
		delete(mgr.priorityVuids, vuid)
		return nil
	}
	span.Debugf("select balance volume unit; vuid[%d], volume_id[%v]", vuid, vuid.Vid())
	task := &proto.MigrateTask{
		TaskID:       client.GenMigrateTaskID(proto.TaskTypeBalance, disk.DiskID, uint32(vuid.Vid())),
		TaskType:     proto.TaskTypeBalance,
		State:        proto.MigrateStateInited,
		SourceIDC:    disk.Idc,
		SourceDiskID: disk.DiskID,
		SourceVuid:   vuid,
	}
	err = mgr.IMigrator.AddTask(ctx, task)
	if err == nil {
		delete(mgr.priorityVuids, vuid)
	}
	return
}

// meetsCompactMigrateThreshold reports whether vunit is a large chunk whose hole rate
// reaches the configured threshold and should be prioritised for migration.
func (mgr *BalanceMgr) meetsCompactMigrateThreshold(v *client.VunitInfoSimple) bool {
	if mgr.cfg.CompactMigrateHoleRate <= 0 || v.LogicSize < mgr.cfg.CompactMigrateMinLogicSize {
		return false
	}
	holeRate := 1.0 - float64(v.Used)/float64(v.LogicSize)
	return holeRate >= mgr.cfg.CompactMigrateHoleRate
}

// vunitLessHighUsage compares two vunits for ordering under high disk usage.
// Priority order (descending):
//  1. Large chunks (LogicSize >= minLogicSize) with hole rate >= holeRateThreshold, sorted by hole rate desc
//  2. Large chunks with hole rate < holeRateThreshold, sorted by LogicSize desc
//  3. Small chunks (LogicSize < minLogicSize), sorted by LogicSize desc
func (mgr *BalanceMgr) vunitLessHighUsage(vi, vj *client.VunitInfoSimple) bool {
	iLarge := mgr.cfg.CompactMigrateHoleRate > 0 && vi.LogicSize >= mgr.cfg.CompactMigrateMinLogicSize
	jLarge := mgr.cfg.CompactMigrateHoleRate > 0 && vj.LogicSize >= mgr.cfg.CompactMigrateMinLogicSize

	if iLarge != jLarge {
		return iLarge // large chunks always before small chunks
	}
	if !iLarge {
		return vi.LogicSize > vj.LogicSize // both small: larger size first
	}
	holeI := 1.0 - float64(vi.Used)/float64(vi.LogicSize)
	holeJ := 1.0 - float64(vj.Used)/float64(vj.LogicSize)
	iAbove := holeI >= mgr.cfg.CompactMigrateHoleRate
	jAbove := holeJ >= mgr.cfg.CompactMigrateHoleRate

	if iAbove != jAbove {
		return iAbove
	}
	if iAbove {
		return holeI > holeJ
	}
	return vi.LogicSize > vj.LogicSize
}

func (mgr *BalanceMgr) selectBalanceVunit(ctx context.Context, diskInfo *client.DiskInfoSimple) (vuid proto.Vuid, err error) {
	span := trace.SpanFromContextSafe(ctx)

	vunits, err := mgr.clusterMgrCli.ListDiskVolumeUnits(ctx, diskInfo.DiskID)
	if err != nil {
		return
	}

	highUsage := mgr.cfg.DiskUsageThreshold > 0 && diskInfo.UsageRatio() >= mgr.cfg.DiskUsageThreshold
	sort.SliceStable(vunits, func(i, j int) bool {
		if highUsage {
			return mgr.vunitLessHighUsage(vunits[i], vunits[j])
		}
		return vunits[i].Used < vunits[j].Used
	})

	first := true
	for _, v := range vunits {
		if v.Compacting {
			continue
		}
		volInfo, err := mgr.clusterMgrCli.GetVolumeInfo(ctx, v.Vuid.Vid())
		if err != nil {
			span.Errorf("get volume info failed: vid[%d], err[%+v]", v.Vuid.Vid(), err)
			continue
		}
		if volInfo.IsIdle() {
			return v.Vuid, nil
		}
		if first && mgr.meetsCompactMigrateThreshold(v) {
			mgr.priorityVuids[v.Vuid] = diskInfo
			first = false
		}
	}
	return vuid, ErrNoBalanceVunit
}

// checkAndClearJunkTasksLoop due to network timeout, it may still have some junk migrate tasks in clustermgr,
// and we need to clear those tasks later
func (mgr *BalanceMgr) checkAndClearJunkTasksLoop() {
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

func (mgr *BalanceMgr) checkAndClearJunkTasks() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "balance.clearJunkTasks")

	for _, task := range mgr.DeletedTasks() {
		if time.Since(task.DeletedTime) < junkMigrationTaskProtectionWindow {
			continue
		}
		_, err := mgr.clusterMgrCli.GetMigrateTask(ctx, proto.TaskTypeBalance, task.TaskID)
		if err != nil {
			if rpc.DetectStatusCode(err) != http.StatusNotFound {
				span.Errorf("get balance task from clustermanager failed: err[%+v]", err)
				continue
			}
			// means there is no junk task and only delete task from memory
		} else { // delete junk task when exists
			span.Warnf("delete junk task: task_id[%s]", task.TaskID)
			base.InsistOn(ctx, "delete junk task", func() error {
				return mgr.clusterMgrCli.DeleteMigrateTask(ctx, task.TaskID)
			})
		}

		mgr.ClearDeletedTaskByID(task.DiskID, task.TaskID)
	}
}
