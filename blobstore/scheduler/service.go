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
	"fmt"
	"math/rand"
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

var errIllegalTaskType = rpc.NewError(http.StatusBadRequest, "illegal_type", errcode.ErrIllegalTaskType)

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

	shardRepairMgr  ITaskRunner
	blobDeleteMgr   ITaskRunner
	clusterTopology IClusterTopology
	volumeUpdater   client.IVolumeUpdater
	kafkaMonitors   []*base.KafkaTopicMonitor

	clusterMgrCli client.ClusterMgrAPI
}

func (svr *Service) mgrByType(typ proto.TaskType) (Migrator, error) {
	switch typ {
	case proto.TaskTypeDiskRepair:
		return svr.diskRepairMgr, nil
	case proto.TaskTypeBalance:
		return svr.balanceMgr, nil
	case proto.TaskTypeDiskDrop:
		return svr.diskDropMgr, nil
	case proto.TaskTypeManualMigrate:
		return svr.manualMigMgr, nil
	}
	return nil, errIllegalTaskType
}

func (svr *Service) diskMgrByType(typ proto.TaskType) (IDisKMigrator, error) {
	switch typ {
	case proto.TaskTypeDiskDrop:
		return svr.diskDropMgr, nil
	case proto.TaskTypeDiskRepair:
		return svr.diskRepairMgr, nil
	}
	return nil, errIllegalTaskType
}

// HTTPTaskAcquire acquire task
func (svr *Service) HTTPTaskAcquire(c *rpc.Context) {
	args := new(api.AcquireArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	// acquire task ordered: returns disk repair task first and other random
	ctx := c.Request.Context()
	migrators := []Migrator{svr.diskRepairMgr, svr.manualMigMgr, svr.diskDropMgr, svr.balanceMgr}
	shuffledMigrators := migrators[1:]
	rand.Shuffle(len(shuffledMigrators), func(i, j int) {
		shuffledMigrators[i], shuffledMigrators[j] = shuffledMigrators[j], shuffledMigrators[i]
	})
	for _, acquire := range migrators {
		if migrateTask, err := acquire.AcquireTask(ctx, args.IDC); err == nil {
			c.RespondJSON(migrateTask)
			return
		}
	}
	c.RespondError(errcode.ErrNothingTodo)
}

// HTTPTaskReclaim reclaim task
func (svr *Service) HTTPTaskReclaim(c *rpc.Context) {
	args := new(api.OperateTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if !client.ValidMigrateTask(args.TaskType, args.TaskID) {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}
	ctx := c.Request.Context()
	reclaimer, err := svr.mgrByType(args.TaskType)
	if err != nil {
		c.RespondError(err)
		return
	}

	newDst, err := base.AllocVunitSafe(ctx, svr.clusterMgrCli, args.Dest.Vuid, args.Src)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondError(reclaimer.ReclaimTask(ctx, args.IDC, args.TaskID, args.Src, args.Dest, newDst))
}

// HTTPTaskCancel cancel task
func (svr *Service) HTTPTaskCancel(c *rpc.Context) {
	args := new(api.OperateTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if !client.ValidMigrateTask(args.TaskType, args.TaskID) {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}
	ctx := c.Request.Context()
	canceler, err := svr.mgrByType(args.TaskType)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondError(canceler.CancelTask(ctx, args))
}

// HTTPTaskComplete complete task
func (svr *Service) HTTPTaskComplete(c *rpc.Context) {
	args := new(api.OperateTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if !client.ValidMigrateTask(args.TaskType, args.TaskID) {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}
	ctx := c.Request.Context()
	completer, err := svr.mgrByType(args.TaskType)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondError(completer.CompleteTask(ctx, args))
}

// HTTPInspectAcquire acquire inspect task
func (svr *Service) HTTPInspectAcquire(c *rpc.Context) {
	ctx := c.Request.Context()

	task, _ := svr.inspectMgr.AcquireInspect(ctx)
	if task != nil {
		c.RespondJSON(task)
		return
	}
	c.RespondError(errcode.ErrNothingTodo)
}

// HTTPInspectComplete complete inspect task
func (svr *Service) HTTPInspectComplete(c *rpc.Context) {
	args := new(proto.VolumeInspectRet)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	svr.inspectMgr.CompleteInspect(ctx, args)
	c.Respond()
}

// HTTPTaskRenewal renewal task
func (svr *Service) HTTPTaskRenewal(c *rpc.Context) {
	args := new(api.TaskRenewalArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	typeErrors := make(map[proto.TaskType]map[string]string)
	for typ, ids := range args.IDs {
		renewaler, err := svr.mgrByType(typ)
		if err != nil {
			c.RespondError(err)
			return
		}

		errors := make(map[string]string)
		for _, id := range ids {
			if !client.ValidMigrateTask(typ, id) {
				errors[id] = errcode.ErrIllegalArguments.Error()
				continue
			}
			if err := renewaler.RenewalTask(ctx, args.IDC, id); err != nil {
				errors[id] = err.Error()
			}
		}

		if len(errors) > 0 {
			typeErrors[typ] = errors
		}
	}
	c.RespondJSON(api.TaskRenewalRet{Errors: typeErrors})
}

// HTTPTaskReport reports task stats
func (svr *Service) HTTPTaskReport(c *rpc.Context) {
	args := new(api.TaskReportArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if !client.ValidMigrateTask(args.TaskType, args.TaskID) {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}
	reporter, err := svr.mgrByType(args.TaskType)
	if err != nil {
		c.RespondError(err)
		return
	}
	reporter.ReportWorkerTaskStats(args)
	c.Respond()
}

// HTTPMigrateTaskDetail returns migrate task detail.
func (svr *Service) HTTPMigrateTaskDetail(c *rpc.Context) {
	args := new(api.MigrateTaskDetailArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if args.ID == "" || !client.ValidMigrateTask(args.Type, args.ID) {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	querier, err := svr.mgrByType(args.Type)
	if err != nil {
		c.RespondError(err)
		return
	}
	detail, err := querier.QueryTask(c.Request.Context(), args.ID)
	if err != nil {
		c.RespondError(rpc.NewError(http.StatusNotFound, "NotFound", err))
		return
	}
	c.RespondJSON(detail)
}

// HTTPDiskMigratingStats returns disk migrating stats
func (svr *Service) HTTPDiskMigratingStats(c *rpc.Context) {
	args := new(api.DiskMigratingStatsArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	querier, err := svr.diskMgrByType(args.TaskType)
	if err != nil {
		c.RespondError(err)
		return
	}
	stats, err := querier.DiskProgress(c.Request.Context(), args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(stats)
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
	repairDisks, totalTasksCnt, repairedTasksCnt := svr.diskRepairMgr.Progress(ctx)
	taskStats.DiskRepair = &api.DiskRepairTasksStat{
		Enable:           svr.diskRepairMgr.Enabled(),
		RepairingDisks:   repairDisks,
		TotalTasksCnt:    totalTasksCnt,
		RepairedTasksCnt: repairedTasksCnt,
		MigrateTasksStat: svr.diskRepairMgr.Stats(),
	}

	// stats drop tasks
	dropDisks, totalTasksCnt, droppedTasksCnt := svr.diskDropMgr.Progress(ctx)
	taskStats.DiskDrop = &api.DiskDropTasksStat{
		Enable:           svr.diskDropMgr.Enabled(),
		DroppingDisks:    dropDisks,
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
	_, err := svr.clusterTopology.UpdateVolume(args.Vid)
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
