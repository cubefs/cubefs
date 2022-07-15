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

	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// VolRepairTaskEx  volume repair task execution
type VolRepairTaskEx struct {
	taskInfo                 *proto.MigrateTask
	downloadShardConcurrency int
	blobNodeCli              IVunitAccess
}

// RepairWorker repair worker
type RepairWorker struct {
	t                        *proto.MigrateTask
	blobNodeCli              IVunitAccess
	benchmarkBids            []*ShardInfoSimple
	downloadShardConcurrency int
}

// NewRepairWorker returns repair worker
func NewRepairWorker(task VolRepairTaskEx) ITaskWorker {
	return &RepairWorker{
		t:                        task.taskInfo,
		blobNodeCli:              task.blobNodeCli,
		downloadShardConcurrency: task.downloadShardConcurrency,
	}
}

// GenTasklets generate tasklets
func (w *RepairWorker) GenTasklets(ctx context.Context) ([]Tasklet, *WorkError) {
	if workutils.BigBufPool == nil {
		panic("BigBufPool should init before")
	}

	migBids, benchmarkBids, err := GenMigrateBids(ctx,
		w.blobNodeCli,
		w.t.Sources,
		w.t.Destination,
		w.t.CodeMode,
		[]uint8{w.t.SourceVuid.Index()})
	if err != nil {
		return nil, err
	}

	w.benchmarkBids = benchmarkBids

	tasklets := BidsSplit(ctx, migBids, workutils.BigBufPool.GetBufSize())
	return tasklets, nil
}

// ExecTasklet execute repair tasklet
func (w *RepairWorker) ExecTasklet(ctx context.Context, tasklet Tasklet) *WorkError {
	replicas := w.t.Sources
	mode := w.t.CodeMode
	shardRecover := NewShardRecover(replicas, mode, tasklet.bids, workutils.BigBufPool, w.blobNodeCli, w.downloadShardConcurrency)
	defer shardRecover.ReleaseBuf()

	return MigrateBids(ctx, shardRecover, w.t.SourceVuid.Index(), w.t.Destination, false, tasklet.bids, w.blobNodeCli)
}

// Check check repair task
func (w *RepairWorker) Check(ctx context.Context) *WorkError {
	return CheckVunit(ctx, w.benchmarkBids, w.t.Destination, w.blobNodeCli)
}

// CancelArgs returns cancel args
func (w *RepairWorker) CancelArgs() (taskID string, taskType proto.TaskType, src []proto.VunitLocation, dest proto.VunitLocation) {
	return w.t.TaskID, w.TaskType(), w.t.Sources, w.t.Destination
}

// CompleteArgs returns complete args
func (w *RepairWorker) CompleteArgs() (taskID string, taskType proto.TaskType, src []proto.VunitLocation, dest proto.VunitLocation) {
	return w.t.TaskID, w.TaskType(), w.t.Sources, w.t.Destination
}

// ReclaimArgs returns reclaim args
func (w *RepairWorker) ReclaimArgs() (taskID string, taskType proto.TaskType, src []proto.VunitLocation, dest proto.VunitLocation) {
	return w.t.TaskID, w.TaskType(), w.t.Sources, w.t.Destination
}

// TaskType returns task type
func (w *RepairWorker) TaskType() (taskType proto.TaskType) {
	return proto.TaskTypeDiskRepair
}

// GetBenchmarkBids returns benchmark bids
func (w *RepairWorker) GetBenchmarkBids() []*ShardInfoSimple {
	return w.benchmarkBids
}
