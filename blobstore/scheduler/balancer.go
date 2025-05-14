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
	MaxDiskFreeChunkCnt int64 `json:"max_disk_free_chunk_cnt"`
	MinDiskFreeChunkCnt int64 `json:"min_disk_free_chunk_cnt"`
	MigrateConfig
}

// BalanceMgr balance manager
type BalanceMgr struct {
	IMigrator

	clusterTopology IClusterTopology
	clusterMgrCli   client.ClusterMgrAPI

	cfg *BalanceMgrConfig
}

// NewBalanceMgr returns balance manager
func NewBalanceMgr(clusterMgrCli client.ClusterMgrAPI, volumeUpdater client.IVolumeUpdater, taskSwitch taskswitch.ISwitcher,
	clusterTopology IClusterTopology, taskLogger recordlog.Encoder, conf *BalanceMgrConfig,
) *BalanceMgr {
	mgr := &BalanceMgr{
		clusterTopology: clusterTopology,
		clusterMgrCli:   clusterMgrCli,
		cfg:             conf,
	}
	mgr.IMigrator = NewMigrateMgr(clusterMgrCli, volumeUpdater, taskSwitch, taskLogger,
		&conf.MigrateConfig, proto.TaskTypeBalance)
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

	// select balance disks
	disks := mgr.selectDisks(mgr.cfg.MaxDiskFreeChunkCnt, mgr.cfg.MinDiskFreeChunkCnt)
	span.Debugf("select balance disks: len[%d]", len(disks))

	balanceDiskCnt := 0
	for _, disk := range disks {
		err = mgr.genOneBalanceTask(ctx, disk)
		if err != nil {
			continue
		}

		balanceDiskCnt++
		if balanceDiskCnt >= needBalanceDiskCnt {
			break
		}
	}
	// if balanceDiskCnt==0, means there is no balance volume unit on disk and need to do collect task later
	if balanceDiskCnt == 0 {
		span.Infof("select disks has no balance volume unit on disk: len[%d]", len(disks))
		return ErrNoBalanceVunit
	}

	return nil
}

func (mgr *BalanceMgr) selectDisks(maxFreeChunkCnt, minFreeChunkCnt int64) []*client.DiskInfoSimple {
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
		if !disk.IsHealth() {
			continue
		}
		if ok := mgr.IMigrator.IsMigratingDisk(disk.DiskID); ok {
			continue
		}
		if disk.FreeChunkCnt < minFreeChunkCnt {
			selected = append(selected, disk)
		}
	}
	return selected
}

func (mgr *BalanceMgr) genOneBalanceTask(ctx context.Context, diskInfo *client.DiskInfoSimple) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	vuid, err := mgr.selectBalanceVunit(ctx, diskInfo.DiskID)
	if err != nil {
		span.Errorf("generate task source failed: disk_id[%d], err[%+v]", diskInfo.DiskID, err)
		return
	}

	span.Debugf("select balance volume unit; vuid[%d], volume_id[%v]", vuid, vuid.Vid())
	task := &proto.MigrateTask{
		TaskID:       client.GenMigrateTaskID(proto.TaskTypeBalance, diskInfo.DiskID, uint32(vuid.Vid())),
		TaskType:     proto.TaskTypeBalance,
		State:        proto.MigrateStateInited,
		SourceIDC:    diskInfo.Idc,
		SourceDiskID: diskInfo.DiskID,
		SourceVuid:   vuid,
	}
	mgr.IMigrator.AddTask(ctx, task)
	return
}

func (mgr *BalanceMgr) selectBalanceVunit(ctx context.Context, diskID proto.DiskID) (vuid proto.Vuid, err error) {
	span := trace.SpanFromContextSafe(ctx)

	vunits, err := mgr.clusterMgrCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		return
	}

	sort.Slice(vunits, func(i, j int) bool {
		return vunits[i].Used < vunits[j].Used
	})

	for i := range vunits {
		volInfo, err := mgr.clusterMgrCli.GetVolumeInfo(ctx, vunits[i].Vuid.Vid())
		if err != nil {
			span.Errorf("get volume info failed: vid[%d], err[%+v]", vunits[i].Vuid.Vid(), err)
			continue
		}
		if volInfo.IsIdle() {
			return vunits[i].Vuid, nil
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
