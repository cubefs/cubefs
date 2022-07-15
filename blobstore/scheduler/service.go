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
	"net/http"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

// Service rpc service
type Service struct {
	ClusterID     proto.ClusterID
	leader        bool
	leaderHost    string
	followerHosts []string

	balanceMgr    Migrator
	diskDropMgr   IDisKMigrator
	diskRepairMgr IDisKMigrator
	manualMigMgr  IManualMigrator
	inspectMgr    IVolumeInspector
	archiveMgr    IArchiver

	shardRepairMgr ITaskRunner
	blobDeleteMgr  ITaskRunner
	volCache       IVolumeCache
	volumeUpdater  client.IVolumeUpdater

	clusterMgrCli client.ClusterMgrAPI
}

// HTTPTaskAcquire acquire task
func (svr *Service) HTTPTaskAcquire(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.AcquireArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	manualMigTask, err := svr.manualMigMgr.AcquireTask(ctx, args.IDC)
	if err == nil {
		ret := &api.WorkerTask{
			Task: manualMigTask,
		}
		c.RespondJSON(ret)
		return
	}

	repairTask, err := svr.diskRepairMgr.AcquireTask(ctx, args.IDC)
	if err == nil {
		ret := &api.WorkerTask{
			Task: repairTask,
		}
		c.RespondJSON(ret)
		return
	}

	diskDropTask, err := svr.diskDropMgr.AcquireTask(ctx, args.IDC)
	if err == nil {
		ret := &api.WorkerTask{
			Task: diskDropTask,
		}
		c.RespondJSON(ret)
		return
	}

	balanceTask, err := svr.balanceMgr.AcquireTask(ctx, args.IDC)
	if err == nil {
		ret := &api.WorkerTask{
			Task: balanceTask,
		}
		c.RespondJSON(ret)
		return
	}

	c.RespondError(errcode.ErrNothingTodo)
}

// HTTPTaskReclaim reclaim task
func (svr *Service) HTTPTaskReclaim(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.ReclaimTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if !args.TaskType.Valid() {
		c.RespondError(rpc.NewError(http.StatusBadRequest, "illegal_type", errcode.ErrIllegalTaskType))
		return
	}

	newDst, err := base.AllocVunitSafe(ctx, svr.clusterMgrCli, args.Dest.Vuid, args.Src)
	if err != nil {
		c.RespondError(err)
		return
	}

	switch args.TaskType {
	case proto.TaskTypeDiskRepair:
		err = svr.diskRepairMgr.ReclaimTask(ctx, args.IDC, args.TaskId, args.Src, args.Dest, newDst)
	case proto.TaskTypeBalance:
		err = svr.balanceMgr.ReclaimTask(ctx, args.IDC, args.TaskId, args.Src, args.Dest, newDst)
	case proto.TaskTypeDiskDrop:
		err = svr.diskDropMgr.ReclaimTask(ctx, args.IDC, args.TaskId, args.Src, args.Dest, newDst)
	case proto.TaskTypeManualMigrate:
		err = svr.manualMigMgr.ReclaimTask(ctx, args.IDC, args.TaskId, args.Src, args.Dest, newDst)
	}

	c.RespondError(err)
}

// HTTPTaskCancel cancel task
func (svr *Service) HTTPTaskCancel(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.CancelTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if !args.TaskType.Valid() {
		c.RespondError(rpc.NewError(http.StatusBadRequest, "illegal_type", errcode.ErrIllegalTaskType))
		return
	}

	var err error
	switch args.TaskType {
	case proto.TaskTypeDiskRepair:
		err = svr.diskRepairMgr.CancelTask(ctx, args)
	case proto.TaskTypeBalance:
		err = svr.balanceMgr.CancelTask(ctx, args)
	case proto.TaskTypeDiskDrop:
		err = svr.diskDropMgr.CancelTask(ctx, args)
	case proto.TaskTypeManualMigrate:
		err = svr.manualMigMgr.CancelTask(ctx, args)
	}

	c.RespondError(err)
}

// HTTPTaskComplete complete task
func (svr *Service) HTTPTaskComplete(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.CompleteTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if !args.TaskType.Valid() {
		c.RespondError(rpc.NewError(http.StatusBadRequest, "illegal_type", errcode.ErrIllegalTaskType))
		return
	}

	var err error
	switch args.TaskType {
	case proto.TaskTypeDiskRepair:
		err = svr.diskRepairMgr.CompleteTask(ctx, args)
	case proto.TaskTypeBalance:
		err = svr.balanceMgr.CompleteTask(ctx, args)
	case proto.TaskTypeDiskDrop:
		err = svr.diskDropMgr.CompleteTask(ctx, args)
	case proto.TaskTypeManualMigrate:
		err = svr.manualMigMgr.CompleteTask(ctx, args)
	}

	c.RespondError(err)
}

// HTTPInspectAcquire acquire inspect task
func (svr *Service) HTTPInspectAcquire(c *rpc.Context) {
	ctx := c.Request.Context()

	task, _ := svr.inspectMgr.AcquireInspect(ctx)
	if task != nil {
		c.RespondJSON(api.WorkerInspectTask{Task: task})
		return
	}

	c.RespondError(errcode.ErrNothingTodo)
}

// HTTPInspectComplete complete inspect task
func (svr *Service) HTTPInspectComplete(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.CompleteInspectArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	svr.inspectMgr.CompleteInspect(ctx, args.InspectRet)
	c.Respond()
}

// HTTPTaskRenewal renewal task
func (svr *Service) HTTPTaskRenewal(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.TaskRenewalArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	idc := args.IDC
	ret := &api.TaskRenewalRet{
		Repair:        make(map[string]string),
		Balance:       make(map[string]string),
		DiskDrop:      make(map[string]string),
		ManualMigrate: make(map[string]string),
	}

	for taskID := range args.Repair {
		err := svr.diskRepairMgr.RenewalTask(ctx, idc, taskID)
		ret.Repair[taskID] = getErrMsg(err)
	}

	for taskID := range args.Balance {
		err := svr.balanceMgr.RenewalTask(ctx, idc, taskID)
		ret.Balance[taskID] = getErrMsg(err)
	}

	for taskID := range args.DiskDrop {
		err := svr.diskDropMgr.RenewalTask(ctx, idc, taskID)
		ret.DiskDrop[taskID] = getErrMsg(err)
	}

	for taskID := range args.ManualMigrate {
		err := svr.manualMigMgr.RenewalTask(ctx, idc, taskID)
		ret.ManualMigrate[taskID] = getErrMsg(err)
	}

	c.RespondJSON(ret)
}

func getErrMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// HTTPTaskReport reports task stats
func (svr *Service) HTTPTaskReport(c *rpc.Context) {
	args := new(api.TaskReportArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	switch args.TaskType {
	case proto.TaskTypeDiskRepair:
		svr.diskRepairMgr.ReportWorkerTaskStats(args)
	case proto.TaskTypeBalance:
		svr.balanceMgr.ReportWorkerTaskStats(args)
	case proto.TaskTypeDiskDrop:
		svr.diskDropMgr.ReportWorkerTaskStats(args)
	case proto.TaskTypeManualMigrate:
		svr.manualMigMgr.ReportWorkerTaskStats(args)
	}

	c.Respond()
}

func respondTaskDetail(c *rpc.Context, mgr interface {
	QueryTask(context.Context, string) (*api.MigrateTaskDetail, error)
}) {
	args := new(api.TaskStatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	detail, err := mgr.QueryTask(c.Request.Context(), args.TaskId)
	if err != nil {
		c.RespondError(rpc.NewError(http.StatusNotFound, "NotFound", err))
		return
	}
	c.RespondJSON(detail)
}

// HTTPBalanceTaskDetail returns balance task detail stats
func (svr *Service) HTTPBalanceTaskDetail(c *rpc.Context) {
	respondTaskDetail(c, svr.balanceMgr)
}

// HTTPDropTaskDetail returns disk drop task detail stats
func (svr *Service) HTTPDropTaskDetail(c *rpc.Context) {
	respondTaskDetail(c, svr.diskDropMgr)
}

// HTTPManualMigrateTaskDetail returns manual migrate task detail stats
func (svr *Service) HTTPManualMigrateTaskDetail(c *rpc.Context) {
	respondTaskDetail(c, svr.manualMigMgr)
}

// HTTPRepairTaskDetail returns repair task detail stats
func (svr *Service) HTTPRepairTaskDetail(c *rpc.Context) {
	args := new(api.TaskStatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	detail, err := svr.diskRepairMgr.QueryTask(c.Request.Context(), args.TaskId)
	if err != nil {
		c.RespondError(rpc.NewError(http.StatusNotFound, "NotFound", err))
		return
	}
	c.RespondJSON(detail)
}

// HTTPStats returns service stats
func (svr *Service) HTTPStats(c *rpc.Context) {
	ctx := c.Request.Context()
	taskStats := api.TasksStat{}

	// delete stats
	deleteSuccessCounter, deleteFailedCounter := svr.blobDeleteMgr.GetTaskStats()
	delErrStats, delTotalErrCnt := svr.blobDeleteMgr.GetErrorStats()
	taskStats.BlobDelete = &api.RunnerStat{
		Enable:        svr.blobDeleteMgr.Enabled(),
		SuccessPerMin: fmt.Sprint(deleteSuccessCounter),
		FailedPerMin:  fmt.Sprint(deleteFailedCounter),
		TotalErrCnt:   delTotalErrCnt,
		ErrStats:      delErrStats,
	}

	// stats shard repair tasks
	repairSuccessCounter, repairFailedCounter := svr.shardRepairMgr.GetTaskStats()
	repairErrStats, repairTotalErrCnt := svr.shardRepairMgr.GetErrorStats()
	taskStats.ShardRepair = &api.RunnerStat{
		Enable:        svr.shardRepairMgr.Enabled(),
		SuccessPerMin: fmt.Sprint(repairSuccessCounter),
		FailedPerMin:  fmt.Sprint(repairFailedCounter),
		TotalErrCnt:   repairTotalErrCnt,
		ErrStats:      repairErrStats,
	}

	if !svr.leader {
		c.RespondJSON(taskStats)
		return
	}

	// stats repair tasks
	repairDiskID, totalTasksCnt, repairedTasksCnt := svr.diskRepairMgr.Progress(ctx)
	taskStats.DiskRepair = &api.DiskRepairTasksStat{
		Enable:           svr.diskRepairMgr.Enabled(),
		RepairingDiskID:  repairDiskID,
		TotalTasksCnt:    totalTasksCnt,
		RepairedTasksCnt: repairedTasksCnt,
		MigrateTasksStat: svr.diskRepairMgr.Stats(),
	}

	// stats drop tasks
	dropDiskID, totalTasksCnt, droppedTasksCnt := svr.diskDropMgr.Progress(ctx)
	taskStats.DiskDrop = &api.DiskDropTasksStat{
		Enable:           svr.diskDropMgr.Enabled(),
		DroppingDiskID:   dropDiskID,
		TotalTasksCnt:    totalTasksCnt,
		DroppedTasksCnt:  droppedTasksCnt,
		MigrateTasksStat: svr.diskDropMgr.Stats(),
	}

	// stats balance tasks
	taskStats.Balance = &api.BalanceTasksStat{
		Enable:           svr.balanceMgr.Enabled(),
		MigrateTasksStat: svr.balanceMgr.Stats(),
	}

	// stats manual migrate tasks
	taskStats.ManualMigrate = &api.ManualMigrateTasksStat{
		MigrateTasksStat: svr.manualMigMgr.Stats(),
	}

	// stats inspect tasks
	finished, timeout := svr.inspectMgr.GetTaskStats()
	taskStats.VolumeInspect = &api.VolumeInspectTasksStat{
		Enable:         svr.inspectMgr.Enabled(),
		FinishedPerMin: fmt.Sprint(finished),
		TimeOutPerMin:  fmt.Sprint(timeout),
	}

	c.RespondJSON(taskStats)
}

// HTTPManualMigrateTaskAdd adds manual migrate task
func (svr *Service) HTTPManualMigrateTaskAdd(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.AddManualMigrateArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if !args.Valid() {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	err := svr.manualMigMgr.AddManualTask(ctx, args.Vuid, !args.DirectDownload)
	c.RespondError(rpc.Error2HTTPError(err))
}

// HTTPUpdateVolume updates volume cache
func (svr *Service) HTTPUpdateVolume(c *rpc.Context) {
	args := new(api.UpdateVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	// update local cache of volume
	_, err := svr.volCache.Update(args.Vid)
	if err != nil {
		span.Errorf("local volume cache update failed: vid[%d], err[%+v]", args.Vid, err)
		c.RespondError(err)
		return
	}

	if !svr.leader {
		c.Respond()
		return
	}

	tasks := make([]func() error, 0, len(svr.followerHosts))
	for _, host := range svr.followerHosts {
		host := host
		tasks = append(tasks, func() error {
			return svr.volumeUpdater.UpdateFollowerVolumeCache(ctx, host, args.Vid)
		})
	}

	span.Debug("to update follower volume cache")
	if err := task.Run(ctx, tasks...); err != nil {
		span.Errorf("notify follower to update cache err[%+v]", err)
		c.RespondError(err)
		return
	}
	c.Respond()
}
