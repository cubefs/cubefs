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

package blobnode

import (
	"context"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	base "github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// WorkerConfigMeter worker controller meter.
type WorkerConfigMeter struct {
	// max task run count of disk repair & balance & disk drop
	MaxTaskRunnerCnt int `json:"max_task_runner_cnt"`
	// tasklet concurrency of single repair task
	RepairConcurrency int `json:"repair_concurrency"`
	// tasklet concurrency of single balance task
	BalanceConcurrency int `json:"balance_concurrency"`
	// tasklet concurrency of single disk drop task
	DiskDropConcurrency int `json:"disk_drop_concurrency"`
	// tasklet concurrency of single manual migrate task
	ManualMigrateConcurrency int `json:"manual_migrate_concurrency"`
	// shard repair concurrency
	ShardRepairConcurrency int `json:"shard_repair_concurrency"`
	// volume inspect concurrency
	InspectConcurrency int `json:"inspect_concurrency"`

	// batch download concurrency of single tasklet
	DownloadShardConcurrency int `json:"download_shard_concurrency"`
}

func (meter *WorkerConfigMeter) concurrencyByType(taskType proto.TaskType) int {
	switch taskType {
	case proto.TaskTypeDiskRepair:
		return meter.RepairConcurrency
	case proto.TaskTypeBalance:
		return meter.BalanceConcurrency
	case proto.TaskTypeDiskDrop:
		return meter.DiskDropConcurrency
	case proto.TaskTypeManualMigrate:
		return meter.ManualMigrateConcurrency
	}
	return 0
}

// WorkerConfig worker service config
type WorkerConfig struct {
	WorkerConfigMeter

	// buffer pool use for migrate and repair shard repair
	BufPoolConf base.BufConfig `json:"buf_pool_conf"`

	// acquire task period
	AcquireIntervalMs int `json:"acquire_interval_ms"`

	// scheduler client config
	Scheduler scheduler.Config `json:"scheduler"`
	// blbonode client config
	BlobNode bnapi.Config `json:"blobnode"`

	DroppedBidRecord *recordlog.Config `json:"dropped_bid_record"`
}

// WorkerService worker worker_service
type WorkerService struct {
	closer.Closer
	WorkerConfig

	taskRunnerMgr  *TaskRunnerMgr
	inspectTaskMgr *InspectTaskMgr

	shardRepairLimit limit.Limiter
	shardRepairer    *ShardRepairer

	schedulerCli scheduler.IScheduler
	blobNodeCli  client.IBlobNode
}

func (cfg *WorkerConfig) checkAndFix() {
	fixConfigItemInt(&cfg.AcquireIntervalMs, 500)
	fixConfigItemInt(&cfg.MaxTaskRunnerCnt, 1)
	fixConfigItemInt(&cfg.RepairConcurrency, 1)
	fixConfigItemInt(&cfg.BalanceConcurrency, 1)
	fixConfigItemInt(&cfg.DiskDropConcurrency, 1)
	fixConfigItemInt(&cfg.ManualMigrateConcurrency, 10)
	fixConfigItemInt(&cfg.ShardRepairConcurrency, 1)
	fixConfigItemInt(&cfg.InspectConcurrency, 1)
	fixConfigItemInt(&cfg.DownloadShardConcurrency, 10)
	fixConfigItemInt64(&cfg.Scheduler.ClientTimeoutMs, 1000)
	fixConfigItemInt64(&cfg.Scheduler.HostSyncIntervalMs, 1000)
	fixConfigItemInt64(&cfg.BlobNode.ClientTimeoutMs, 1000)
}

func fixConfigItemInt(actual *int, defaultVal int) {
	if *actual <= 0 {
		*actual = defaultVal
	}
}

func fixConfigItemInt64(actual *int64, defaultVal int64) {
	if *actual <= 0 {
		*actual = defaultVal
	}
}

// NewWorkerService returns rpc worker_service
func NewWorkerService(cfg *WorkerConfig, service cmapi.APIService, clusterID proto.ClusterID, idc string) (*WorkerService, error) {
	cfg.checkAndFix()

	base.TaskBufPool = base.NewBufPool(&cfg.BufPoolConf)

	schedulerCli := scheduler.New(&cfg.Scheduler, service, clusterID)
	blobNodeCli := client.NewBlobNodeClient(&cfg.BlobNode)

	renewalConfig := cfg.Scheduler
	renewalConfig.ClientTimeoutMs = 1000 * proto.RenewalTimeoutS
	renewalCli := scheduler.New(&renewalConfig, service, clusterID)
	taskRunnerMgr := NewTaskRunnerMgr(idc, cfg.WorkerConfigMeter, NewMigrateWorker, renewalCli, schedulerCli)
	inspectTaskMgr := NewInspectTaskMgr(cfg.InspectConcurrency, blobNodeCli, schedulerCli)

	shardRepairLimit := count.New(cfg.ShardRepairConcurrency)
	shardRepairer := NewShardRepairer(blobNodeCli)

	// init dropped bid record
	bidRecord := base.DroppedBidRecorderInst()
	err := bidRecord.Init(cfg.DroppedBidRecord, clusterID)
	if err != nil {
		return nil, err
	}

	svr := &WorkerService{
		Closer:       closer.New(),
		WorkerConfig: *cfg,

		schedulerCli:   schedulerCli,
		blobNodeCli:    blobNodeCli,
		taskRunnerMgr:  taskRunnerMgr,
		inspectTaskMgr: inspectTaskMgr,

		shardRepairLimit: shardRepairLimit,
		shardRepairer:    shardRepairer,
	}

	go svr.Run()
	return svr, nil
}

// ShardRepair repair shard
func (s *WorkerService) ShardRepair(c *rpc.Context) {
	args := new(proto.ShardRepairTask)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span := trace.SpanFromContextSafe(c.Request.Context())
	ctx := trace.ContextWithSpan(c.Request.Context(), span)

	if !args.IsValid() {
		span.Errorf("shard repair task is illegal: task[%+v]", args)
		c.RespondError(errcode.ErrIllegalTask)
		return
	}

	err := s.shardRepairLimit.Acquire()
	if err != nil {
		span.Errorf("the shard repair request is too much: err[%+v]", err)
		c.RespondError(errcode.ErrRequestLimited)
		return
	}
	defer s.shardRepairLimit.Release()

	err = s.shardRepairer.RepairShard(ctx, args)
	c.RespondError(err)
}

// ShardPartialRepair merging traffic from the same data center
func (s *WorkerService) ShardPartialRepair(c *rpc.Context) {
	args := bnapi.ShardPartialRepairArgs{}
	if err := c.ParseArgs(&args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if !args.IsValid() {
		span.Errorf("get partial shard param is illegal: args[%+v]", args)
		c.RespondError(errcode.ErrInvalidParam)
		return
	}
	span.Debugf("accept ShardPartialRepair request, args: %v", args)

	ret, err := s.shardRepairer.ShardPartialRepair(ctx, args)
	if err != nil {
		span.Errorf("shard partial repair failed, err: %v", err)
		c.RespondError(err)
		return
	}

	c.RespondJSON(ret)
}

// WorkerStats returns worker_service stats
func (s *WorkerService) WorkerStats(c *rpc.Context) {
	c.RespondJSON(s.taskRunnerMgr.TaskStats())
}

// Run runs backend task
func (s *WorkerService) Run() {
	// task lease
	s.taskRunnerMgr.RenewalTaskLoop(s.Done())
	s.loopAcquireTask()
}

func (s *WorkerService) loopAcquireTask() {
	ticker := time.NewTicker(time.Duration(s.AcquireIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.tryAcquireTask()
		case <-s.Done():
			return
		}
	}
}

func (s *WorkerService) tryAcquireTask() {
	if s.hasTaskRunnerResource() {
		s.acquireTask()
	}

	if s.hasInspectTaskResource() {
		s.acquireInspectTask()
	}
}

func (s *WorkerService) hasTaskRunnerResource() bool {
	running := s.taskRunnerMgr.RunningTaskCnt()
	all := 0
	for _, cnt := range running {
		all += cnt
	}
	log.Infof("task running %d / %d, %+v", all, s.MaxTaskRunnerCnt, running)
	return all < s.MaxTaskRunnerCnt
}

func (s *WorkerService) hasInspectTaskResource() bool {
	inspectCnt := s.inspectTaskMgr.RunningTaskSize()
	log.Infof("inspect running task %d / %d", inspectCnt, s.InspectConcurrency)
	return inspectCnt < s.InspectConcurrency
}

// acquire:disk repair & balance & disk drop task
func (s *WorkerService) acquireTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "acquireTask")
	task, err := s.schedulerCli.AcquireTask(ctx, &scheduler.AcquireArgs{IDC: s.taskRunnerMgr.idc})
	if err != nil {
		code := rpc.DetectStatusCode(err)
		if code != errcode.CodeNotingTodo {
			span.Infof("acquire task failed: code[%d], err[%v]", code, err)
		}
		return
	}

	if !task.IsValid() {
		span.Errorf("task is illegal: task type[%s], task[%+v]", task.TaskType, task)
		return
	}
	if err = s.taskRunnerMgr.AddTask(ctx, MigrateTaskEx{
		taskInfo:                 task,
		downloadShardConcurrency: s.DownloadShardConcurrency,
		blobNodeCli:              s.blobNodeCli,
	}); err != nil {
		span.Errorf("add task failed: taskID[%s], err[%v]", task.TaskID, err)
		return
	}
	span.Infof("acquire task success: task_type[%s], taskID[%s]", task.TaskType, task.TaskID)
}

// acquire inspect task
func (s *WorkerService) acquireInspectTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "acquireInspectTask")

	t, err := s.schedulerCli.AcquireInspectTask(ctx)
	if err != nil {
		code := rpc.DetectStatusCode(err)
		if code != errcode.CodeNotingTodo {
			span.Errorf("acquire inspect task failed: code[%d], err[%v]", code, err)
		}
		return
	}

	if !t.IsValid() {
		span.Errorf("inspect task is illegal: task[%+v]", t)
		return
	}

	err = s.inspectTaskMgr.AddTask(ctx, t)
	if err != nil {
		span.Errorf("add inspect task failed: taskID[%s], err[%v]", t.TaskID, err)
		return
	}

	span.Infof("acquire inspect task success: taskID[%s] task[%+v]", t.TaskID, t)
}
