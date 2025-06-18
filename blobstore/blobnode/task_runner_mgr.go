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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

var (
	// ErrBidMissing bid is missing
	ErrBidMissing = errors.New("bid is missing")
	// ErrBidNotMatch bid not match
	ErrBidNotMatch = errors.New("bid not match")

	errAddRunningTaskAgain = errors.New("running task add again")
)

// WorkerGenerator generates task worker.
type WorkerGenerator = func(task MigrateTaskEx) ITaskWorker

type taskCounter struct {
	cancel  counter.Counter
	reclaim counter.Counter
}

// TaskRunnerMgr task runner manager
type TaskRunnerMgr struct {
	mu      sync.Mutex
	typeMgr map[proto.TaskType]mapTaskRunner

	idc          string
	meter        WorkerConfigMeter
	genWorker    WorkerGenerator
	renewalCli   scheduler.IMigrator // TODO: must be timeout in proto.RenewalTimeoutS
	schedulerCli scheduler.IMigrator
	taskCounter  taskCounter
}

// NewTaskRunnerMgr returns task runner manager
func NewTaskRunnerMgr(idc string, meter WorkerConfigMeter, genWorker WorkerGenerator,
	renewalCli, schedulerCli scheduler.IMigrator,
) *TaskRunnerMgr {
	return &TaskRunnerMgr{
		typeMgr: map[proto.TaskType]mapTaskRunner{
			proto.TaskTypeBalance:         make(mapTaskRunner),
			proto.TaskTypeDiskDrop:        make(mapTaskRunner),
			proto.TaskTypeDiskRepair:      make(mapTaskRunner),
			proto.TaskTypeManualMigrate:   make(mapTaskRunner),
			proto.TaskTypeShardDiskRepair: make(mapTaskRunner),
		},

		idc:          idc,
		meter:        meter,
		genWorker:    genWorker,
		renewalCli:   renewalCli,
		schedulerCli: schedulerCli,
	}
}

// RenewalTaskLoop renewal task.
func (tm *TaskRunnerMgr) RenewalTaskLoop(stopCh <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(time.Duration(proto.TaskRenewalPeriodS) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tm.renewalTask()
			case <-stopCh:
				return
			}
		}
	}()
}

func (tm *TaskRunnerMgr) renewalTask() {
	aliveTasks := tm.GetAliveTasks()
	if len(aliveTasks) == 0 {
		return
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "renewalTask")
	ret, err := tm.renewalCli.RenewalTask(ctx, &scheduler.TaskRenewalArgs{IDC: tm.idc, IDs: aliveTasks})
	if err != nil {
		span.Errorf("renewal task failed and stop all runner: err[%+v]", err)
		tm.StopAllAliveRunner()
		return
	}
	if len(ret.Errors) == 0 {
		return
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()
	for typ, errs := range ret.Errors {
		mgr, ok := tm.typeMgr[typ]
		if !ok {
			continue
		}
		for taskID, errMsg := range errs {
			span.Warnf("renewal fail so stop runner: type[%s], taskID[%s], error[%s]", typ, taskID, errMsg)
			if err := mgr.stopTask(taskID); err != nil {
				span.Errorf("stop runner failed: type[%s], taskID[%s], err[%+v]", typ, taskID, err)
			}
		}
	}
}

// AddTask add blobNode migrate task.
func (tm *TaskRunnerMgr) AddTask(ctx context.Context, task MigrateTaskEx) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	t := task.taskInfo
	mgr, ok := tm.typeMgr[t.TaskType]
	if !ok {
		return fmt.Errorf("invalid task type: %s", t.TaskType)
	}

	if err := base.ValidateCodeMode(t.CodeMode); err != nil {
		return err
	}

	w := tm.genWorker(task)
	concurrency := tm.meter.concurrencyByType(t.TaskType)
	runner := NewTaskRunner(ctx, t.TaskID, w, t.SourceIDC, concurrency, &tm.taskCounter, tm.schedulerCli)
	if err := mgr.addTask(t.TaskID, runner); err != nil {
		return err
	}

	go runner.Run()
	return nil
}

// AddShardTask add shardNode migrate task.
func (tm *TaskRunnerMgr) AddShardTask(ctx context.Context, worker *ShardWorker) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	mgr, ok := tm.typeMgr[worker.task.TaskType]
	if !ok {
		return fmt.Errorf("invalid task type: %s", worker.task.TaskType)
	}
	runner := NewShardNodeTaskRunner(ctx, worker.task.TaskID, worker, worker.task.SourceIDC, &tm.taskCounter, tm.schedulerCli)
	if err := mgr.addTask(worker.task.TaskID, runner); err != nil {
		return err
	}

	go runner.Run()
	return nil
}

// GetAliveTasks returns all alive migrate task.
func (tm *TaskRunnerMgr) GetAliveTasks() map[proto.TaskType][]string {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	all := make(map[proto.TaskType][]string)
	for typ, mgr := range tm.typeMgr {
		if alives := mgr.getAliveTasks(); len(alives) > 0 {
			all[typ] = alives
		}
	}
	return all
}

// StopAllAliveRunner stops all alive runner
func (tm *TaskRunnerMgr) StopAllAliveRunner() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, mgr := range tm.typeMgr {
		for _, r := range mgr {
			if r.Alive() {
				r.Stop()
			}
		}
	}
}

// RunningTaskCnt return running task count
func (tm *TaskRunnerMgr) RunningTaskCnt() map[proto.TaskType]int {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	running := make(map[proto.TaskType]int)
	for typ, mgr := range tm.typeMgr {
		tm.typeMgr[typ] = mgr.eliminateStopped()
		running[typ] = len(tm.typeMgr[typ])
	}
	return running
}

// TaskStats task counter result.
func (tm *TaskRunnerMgr) TaskStats() blobnode.WorkerStats {
	return blobnode.WorkerStats{
		CancelCount:  fmt.Sprint(tm.taskCounter.cancel.Show()),
		ReclaimCount: fmt.Sprint(tm.taskCounter.reclaim.Show()),
	}
}

type mapTaskRunner map[string]Runner

func (m mapTaskRunner) eliminateStopped() mapTaskRunner {
	newTasks := make(mapTaskRunner, len(m))
	for taskID, task := range m {
		if task.Stopped() {
			log.Infof("remove stopped task: taskID[%s], state[%d]", task.TaskID(), task.State().get())
			continue
		}
		log.Debugf("remain task: taskID[%s], state[%d]", task.TaskID(), task.State().get())
		newTasks[taskID] = task
	}
	return newTasks
}

func (m mapTaskRunner) addTask(taskID string, runner Runner) error {
	if r, ok := m[taskID]; ok {
		if !r.Stopped() {
			log.Warnf("task is running shouldn't add again: taskID[%s]", taskID)
			return errAddRunningTaskAgain
		}
	}
	m[taskID] = runner
	return nil
}

func (m mapTaskRunner) stopTask(taskID string) error {
	if r, ok := m[taskID]; ok {
		r.Stop()
		return nil
	}
	return fmt.Errorf("no such task: %s", taskID)
}

func (m mapTaskRunner) getAliveTasks() []string {
	alive := make([]string, 0, 16)
	for _, r := range m {
		if r.Alive() {
			alive = append(alive, r.TaskID())
		}
	}
	return alive
}

// GenMigrateBids generates migrate blob ids
func GenMigrateBids(ctx context.Context, blobnodeCli client.IBlobNode, srcReplicas Vunits,
	dst proto.VunitLocation, mode codemode.CodeMode, badIdxs []uint8,
) (migBids, benchmarkBids []*ShardInfoSimple, bidInfos map[proto.Vuid]*ReplicaBidsRet, wErr *WorkError) {
	span := trace.SpanFromContextSafe(ctx)

	benchmarkBids, bidInfos, err := GetBenchmarkBids(ctx, blobnodeCli, srcReplicas, mode, badIdxs)
	if err != nil {
		span.Errorf("get benchmark bids failed: err[%v]", err)
		return nil, nil, nil, SrcError(err)
	}
	span.Infof("get benchmark success: len[%d]", len(benchmarkBids))

	// get destination bids and check the meta info,if the bid is good,
	// which means we don’task need to migrate the corresponding bid
	destBids, err := GetSingleVunitNormalBids(ctx, blobnodeCli, dst)
	if err != nil {
		span.Errorf("get single vunit normal bids failed: dst[%+v], err[%+v]", dst, err)
		return nil, nil, nil, DstError(err)
	}
	span.Infof("GetSingleVunitNormalBids success: destBids len[%d], idx[%d]", len(destBids), dst.Vuid.Index())

	existInDest := make(map[proto.BlobID]int64, len(destBids))
	for _, bid := range destBids {
		existInDest[bid.Bid] = bid.Size
	}

	for _, bid := range benchmarkBids {
		if size, ok := existInDest[bid.Bid]; ok && size == bid.Size {
			span.Debugf("benchmarkBids bid exist in dest: bid[%d]", bid.Bid)
			continue
		}
		span.Debugf("benchmarkBids append: bid[%d], size[%d]", bid.Bid, bid.Size)
		migBids = append(migBids, bid)
	}

	span.Infof("benchmarkBids: len[%d]", len(migBids))
	return migBids, benchmarkBids, bidInfos, nil
}

// MigrateBids migrate the bids data to destination
func MigrateBids(ctx context.Context, shardRecover *ShardRecover, badIdx uint8, destLocation proto.VunitLocation,
	direct bool, bids []*ShardInfoSimple, blobnodeCli client.IBlobNode,
) *WorkError {
	span := trace.SpanFromContextSafe(ctx)

	// step1 recover shards
	span.Infof("recover shard: len bids[%d]", len(bids))

	err := shardRecover.RecoverShards(ctx, []uint8{badIdx}, direct)
	if err != nil {
		return SrcError(err)
	}

	// put shards to dest
	span.Infof("put data to destination: dest[%+v]", destLocation)
	destIdx := destLocation.Vuid.Index()
	for _, bid := range bids {
		data, err := shardRecover.GetShard(destIdx, bid.Bid)
		if err != nil {
			return OtherError(err)
		}
		err = retry.Timed(3, 1000).On(func() error {
			return blobnodeCli.PutShard(ctx, destLocation, bid.Bid, bid.Size, bytes.NewReader(data), shardRecover.ioType)
		})
		if err != nil {
			return DstError(err)
		}
	}

	return nil
}

// CheckVunit checks volume unit info
func CheckVunit(ctx context.Context, expectBids []*ShardInfoSimple, dest proto.VunitLocation, blobnodeCli client.IBlobNode) *WorkError {
	span := trace.SpanFromContextSafe(ctx)

	// check dst shards
	destBids, err := GetSingleVunitNormalBids(ctx, blobnodeCli, dest)
	if err != nil {
		return DstError(err)
	}

	destBidsMap := make(map[proto.BlobID]*ShardInfoSimple, len(destBids))
	for _, bid := range destBids {
		info := ShardInfoSimple{Bid: bid.Bid, Size: bid.Size}
		destBidsMap[bid.Bid] = &info
	}

	for _, bid := range expectBids {
		info, ok := destBidsMap[bid.Bid]
		if !ok {
			span.Errorf("repair check destination failed: dest[%+v], bid[%d], err[%+v]", dest, bid.Bid, ErrBidMissing)
			return SrcError(ErrBidMissing)
		}
		if info.Size != bid.Size {
			span.Errorf("repair check failed: dest[%+v], bid[%d], size[%d], err[%+v]", dest, bid.Bid, bid.Size, ErrBidNotMatch)
			return SrcError(ErrBidNotMatch)
		}
	}
	return nil
}
