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
	"fmt"
	"sync"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	schedulerapi "github.com/cubefs/cubefs/blobstore/api/scheduler"
	base "github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// ServiceRegisterConfig worker_service register config
type ServiceRegisterConfig struct {
	Idc  string `json:"idc"`
	Host string `json:"host"`
}

// WorkerConfig worker_service config
type WorkerConfig struct {
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

	// small buffer pool use for shard repair
	SmallBufPool base.BufPoolConfig `json:"small_buf_pool"`
	// bid buffer pool use for disk repair & balance & disk drop task
	BigBufPool base.BufPoolConfig `json:"big_buf_pool"`

	// acquire task period
	AcquireIntervalMs int `json:"acquire_interval_ms"`

	// scheduler client config
	Scheduler schedulerapi.Config `json:"scheduler"`
	// blbonode client config
	BlobNode bnapi.Config `json:"blobnode"`

	DroppedBidRecord *recordlog.Config `json:"dropped_bid_record"`
}

// WorkerService worker worker_service
type WorkerService struct {
	taskRunnerMgr  *TaskRunnerMgr
	inspectTaskMgr *InspectTaskMgr
	taskRenter     *TaskRenter

	shardRepairLimit limit.Limiter
	shardRepairer    *ShardRepairer

	closeCh   chan struct{}
	acquireCh chan struct{}
	closeOnce *sync.Once

	schedulerCli client.IScheduler
	blobNodeCli  client.IBlobNode
	WorkerConfig
}

func (cfg *WorkerConfig) checkAndFix() (err error) {
	fixConfigItemInt(&cfg.AcquireIntervalMs, 500)
	fixConfigItemInt(&cfg.MaxTaskRunnerCnt, 1)
	fixConfigItemInt(&cfg.RepairConcurrency, 1)
	fixConfigItemInt(&cfg.BalanceConcurrency, 1)
	fixConfigItemInt(&cfg.DiskDropConcurrency, 1)
	fixConfigItemInt(&cfg.ManualMigrateConcurrency, 10)
	fixConfigItemInt(&cfg.ShardRepairConcurrency, 1)
	fixConfigItemInt(&cfg.InspectConcurrency, 1)
	fixConfigItemInt(&cfg.DownloadShardConcurrency, 10)
	fixConfigItemInt(&cfg.SmallBufPool.PoolSize, 5)
	fixConfigItemInt(&cfg.SmallBufPool.BufSizeByte, 1048576)
	fixConfigItemInt(&cfg.BigBufPool.PoolSize, 5)
	fixConfigItemInt(&cfg.BigBufPool.BufSizeByte, 16777216)

	fixConfigItemInt64(&cfg.Scheduler.ClientTimeoutMs, 1000)
	fixConfigItemInt64(&cfg.Scheduler.HostSyncIntervalMs, 1000)
	fixConfigItemInt64(&cfg.BlobNode.ClientTimeoutMs, 1000)
	return nil
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
func NewWorkerService(cfg *WorkerConfig, clusterMgrCli cmapi.APIService, clusterID proto.ClusterID, idc string) (*WorkerService, error) {
	if err := cfg.checkAndFix(); err != nil {
		return nil, fmt.Errorf("check config: err[%w]", err)
	}

	base.BigBufPool = base.NewByteBufferPool(cfg.BigBufPool.BufSizeByte, cfg.BigBufPool.PoolSize)
	base.SmallBufPool = base.NewByteBufferPool(cfg.SmallBufPool.BufSizeByte, cfg.SmallBufPool.PoolSize)

	schedulerCli := client.NewSchedulerClient(&cfg.Scheduler, clusterMgrCli, clusterID)

	blobNodeCli := client.NewBlobNodeClient(&cfg.BlobNode)
	taskRunnerMgr := NewTaskRunnerMgr(
		cfg.DownloadShardConcurrency,
		cfg.RepairConcurrency,
		cfg.BalanceConcurrency,
		cfg.DiskDropConcurrency,
		cfg.ManualMigrateConcurrency,
		schedulerCli,
		&TaskWorkerCreator{})

	inspectTaskMgr := NewInspectTaskMgr(cfg.InspectConcurrency, blobNodeCli, schedulerCli)

	renewalCli := newRenewalCli(cfg.Scheduler, clusterMgrCli, clusterID)
	taskRenter := NewTaskRenter(idc, renewalCli, taskRunnerMgr)

	shardRepairLimit := count.New(cfg.ShardRepairConcurrency)
	shardRepairer := NewShardRepairer(blobNodeCli, base.SmallBufPool)

	// init dropped bid record
	bidRecord := base.DroppedBidRecorderInst()
	err := bidRecord.Init(cfg.DroppedBidRecord, clusterID)
	if err != nil {
		return nil, err
	}

	svr := &WorkerService{
		schedulerCli:   schedulerCli,
		blobNodeCli:    blobNodeCli,
		taskRunnerMgr:  taskRunnerMgr,
		inspectTaskMgr: inspectTaskMgr,

		shardRepairLimit: shardRepairLimit,
		shardRepairer:    shardRepairer,

		taskRenter:   taskRenter,
		acquireCh:    make(chan struct{}, 1),
		closeCh:      make(chan struct{}),
		closeOnce:    &sync.Once{},
		WorkerConfig: *cfg,
	}

	go svr.Run()

	return svr, nil
}

// ShardRepair repair shard
func (s *WorkerService) ShardRepair(c *rpc.Context) {
	args := new(bnapi.ShardRepairArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span := trace.SpanFromContextSafe(c.Request.Context())
	ctx := trace.ContextWithSpan(c.Request.Context(), span)

	err := s.shardRepairLimit.Acquire()
	if err != nil {
		span.Errorf("the shard repair request is too much: err[%+v]", err)
		c.RespondError(errcode.ErrRequestLimited)
		return
	}
	defer s.shardRepairLimit.Release()

	err = s.shardRepairer.RepairShard(ctx, args.Task)
	c.RespondError(err)
}

// WorkerStats returns worker_service stats
func (s *WorkerService) WorkerStats(c *rpc.Context) {
	cancelCount, reclaimCount := base.WorkerStatsInst().Stats()
	ret := bnapi.WorkerStats{
		CancelCount:  fmt.Sprint(cancelCount),
		ReclaimCount: fmt.Sprint(reclaimCount),
	}
	c.RespondJSON(ret)
}

func newRenewalCli(cfg schedulerapi.Config, service cmapi.APIService, clusterID proto.ClusterID) client.IScheduler {
	// The timeout period must be strictly controlled
	cfg.ClientTimeoutMs = proto.RenewalTimeoutS * 1000
	return client.NewSchedulerClient(&cfg, service, clusterID)
}

// Run runs backend task
func (s *WorkerService) Run() {
	// task lease
	go s.taskRenter.RenewalTaskLoop()

	s.loopAcquireTask()
}

func (s *WorkerService) loopAcquireTask() {
	go func() {
		ticker := time.NewTicker(time.Duration(s.AcquireIntervalMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.notifyAcquire()
			case <-s.closeCh:
				return
			}
		}
	}()

	for {
		select {
		case <-s.acquireCh:
			s.tryAcquireTask()
		case <-s.closeCh:
			return
		}
	}
}

func (s *WorkerService) notifyAcquire() {
	select {
	case s.acquireCh <- struct{}{}:
	default:
	}
}

// Close close worker_service
func (s *WorkerService) Close() {
	s.closeOnce.Do(func() {
		close(s.closeCh)
	})
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
	repair, balance, drop, manualMig := s.taskRunnerMgr.RunningTaskCnt()
	log.Infof("task count:repair %d balance %d drop %d manualMig %d max %d",
		repair, balance, drop, manualMig, s.MaxTaskRunnerCnt)
	return (repair + balance + drop + manualMig) < s.MaxTaskRunnerCnt
}

func (s *WorkerService) hasInspectTaskResource() bool {
	inspectCnt := s.inspectTaskMgr.RunningTaskSize()
	log.Infof("inspect task count:inspectCnt %d max %d", inspectCnt, s.InspectConcurrency)
	return inspectCnt < s.InspectConcurrency
}

// acquire:disk repair & balance & disk drop task
func (s *WorkerService) acquireTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "acquireTask")

	t, err := s.schedulerCli.AcquireTask(ctx, &schedulerapi.AcquireArgs{IDC: s.taskRenter.idc})
	if err != nil {
		code := rpc.DetectStatusCode(err)
		if code != errcode.CodeNotingTodo {
			span.Errorf("acquire task failed: code[%d], err[%v]", code, err)
		}
		return
	}

	if !t.IsValid() {
		span.Errorf("task is illegal: task type[%s], disk drop[%+v], balance[%+v], repair[%+v], manual[%+v]",
			t.TaskType, t.DiskDrop, t.Balance, t.Repair, t.ManualMigrate)
		return
	}

	var taskID string
	switch t.TaskType {
	case proto.RepairTaskType:
		taskID = t.Repair.TaskID
		err = s.taskRunnerMgr.AddRepairTask(ctx, VolRepairTaskEx{
			taskInfo:                 t.Repair,
			downloadShardConcurrency: s.DownloadShardConcurrency,
			blobNodeCli:              s.blobNodeCli,
		})

	case proto.BalanceTaskType:
		taskID = t.Balance.TaskID
		err = s.taskRunnerMgr.AddBalanceTask(ctx, MigrateTaskEx{
			taskInfo:                 t.Balance,
			taskType:                 proto.BalanceTaskType,
			blobNodeCli:              s.blobNodeCli,
			downloadShardConcurrency: s.DownloadShardConcurrency,
		})

	case proto.DiskDropTaskType:
		taskID = t.DiskDrop.TaskID
		err = s.taskRunnerMgr.AddDiskDropTask(ctx, MigrateTaskEx{
			taskInfo:                 t.DiskDrop,
			taskType:                 proto.DiskDropTaskType,
			blobNodeCli:              s.blobNodeCli,
			downloadShardConcurrency: s.DownloadShardConcurrency,
		})
	case proto.ManualMigrateType:
		taskID = t.ManualMigrate.TaskID
		err = s.taskRunnerMgr.AddManualMigrateTask(ctx, MigrateTaskEx{
			taskInfo:                 t.ManualMigrate,
			taskType:                 proto.ManualMigrateType,
			blobNodeCli:              s.blobNodeCli,
			downloadShardConcurrency: s.DownloadShardConcurrency,
		})
	default:
		span.Fatalf("can not support task: type[%+v]", t.TaskType)
	}

	if err != nil {
		span.Errorf("add task failed: taskID[%s], err[%v]", taskID, err)
		return
	}
	span.Infof("acquire task success: task_type[%s], taskID[%s]", t.TaskType, taskID)
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
		span.Errorf("inspect task is illegal: task[%+v]", t.Task)
		return
	}

	err = s.inspectTaskMgr.AddTask(ctx, t.Task)
	if err != nil {
		span.Errorf("add inspect task failed: taskID[%s], err[%v]", t.Task.TaskId, err)
		return
	}

	span.Infof("acquire inspect task success: taskID[%s]", t.Task.TaskId)
}
