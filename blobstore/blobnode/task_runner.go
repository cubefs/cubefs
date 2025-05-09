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
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

// task runner status
const (
	TaskInit uint8 = iota + 1
	TaskRunning
	TaskStopping
	TaskSuccess
	TaskStopped
)

var errKilled = errors.New("task killed")

// TaskState task state
type TaskState struct {
	sync.Mutex
	state uint8
}

func (ts *TaskState) set(st uint8) {
	ts.Lock()
	defer ts.Unlock()
	if ts.state >= TaskSuccess {
		return
	}
	ts.state = st
}

func (ts *TaskState) stopped() bool {
	ts.Lock()
	defer ts.Unlock()
	return ts.state >= TaskSuccess
}

func (ts *TaskState) alive() bool {
	ts.Lock()
	defer ts.Unlock()
	return ts.state <= TaskRunning
}

// WokeErrorType worker error type
type WokeErrorType uint8

const (
	// StatusInterrupt interrupt error status
	StatusInterrupt = 596
)

// task runner error type
const (
	DstErr WokeErrorType = iota + 1
	SrcErr
	OtherErr
)

// WorkError with error type and error
type WorkError struct {
	errType WokeErrorType
	err     error
}

// String return error message with error type and error detail message
func (e *WorkError) String() string {
	return fmt.Sprintf("type %v, %v", e.errType, e.err)
}

// Error returns error info
func (e *WorkError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}

// DstError returns destination error type
func DstError(err error) *WorkError {
	return genWorkError(err, DstErr)
}

// SrcError returns source error type
func SrcError(err error) *WorkError {
	return genWorkError(err, SrcErr)
}

// OtherError returns other error type
func OtherError(err error) *WorkError {
	return genWorkError(err, OtherErr)
}

// ShouldReclaim returns true if the task should reclaim
func ShouldReclaim(e *WorkError) bool {
	if e.errType != DstErr {
		return false
	}

	errCode := rpc.DetectStatusCode(e.err)
	if errCode == StatusInterrupt {
		return false
	}

	if errCode == errcode.CodeOverload {
		return false
	}

	return true
}

func genWorkError(err error, errType WokeErrorType) *WorkError {
	if err == nil {
		return nil
	}
	return &WorkError{errType: errType, err: err}
}

type Runner interface {
	Run()
	Stop()
	Stopped() bool
	Alive() bool
	TaskID() string
	State() *TaskState
}

// ITaskWorker define interface used for task execution
type ITaskWorker interface {
	// split tasklets accord by volume benchmark bids
	GenTasklets(ctx context.Context) ([]Tasklet, *WorkError)
	// define tasklet execution operator ,eg:disk repair & migrate
	ExecTasklet(ctx context.Context, t Tasklet) *WorkError
	// check whether the task is executed successfully when volume task finish
	Check(ctx context.Context) *WorkError
	OperateArgs(reason string) *scheduler.TaskArgs
	TaskType() (taskType proto.TaskType)
	GetBenchmarkBids() []*ShardInfoSimple
}

// Tasklet is the smallest unit of task exe
type Tasklet struct {
	bids []*ShardInfoSimple
}

// DataSizeByte returns total bids size
func (t *Tasklet) DataSizeByte() uint64 {
	var dataSize uint64
	for _, info := range t.bids {
		dataSize += uint64(info.Size)
	}
	return dataSize
}

// TaskRunner used to manage task
type TaskRunner struct {
	taskID string
	w      ITaskWorker
	idc    string

	taskletRunConcurrency int
	state                 TaskState

	ctx    context.Context
	cancel context.CancelFunc
	span   trace.Span

	stopMu     sync.Mutex
	stopReason *WorkError

	schedulerCli scheduler.IMigrator
	stats        proto.TaskProgress // task progress statics
	taskCounter  *taskCounter
}

// NewTaskRunner return task runner
func NewTaskRunner(ctx context.Context, taskID string, w ITaskWorker, idc string,
	taskletRunConcurrency int, taskCounter *taskCounter, schedulerCli scheduler.IMigrator) Runner {
	span, ctx := trace.StartSpanFromContext(ctx, "taskRunner")
	ctx, cancel := context.WithCancel(ctx)

	task := TaskRunner{
		taskID:                taskID,
		w:                     w,
		idc:                   idc,
		taskletRunConcurrency: taskletRunConcurrency,
		ctx:                   ctx,
		cancel:                cancel,
		span:                  span,
		schedulerCli:          schedulerCli,
		stats:                 proto.NewTaskProgress(),
		taskCounter:           taskCounter,
	}
	task.state.set(TaskInit)
	return &task
}

// Run runs task
func (r *TaskRunner) Run() {
	span := r.span
	span.Infof("start run task: taskID[%s]", r.taskID)

	r.state.set(TaskRunning)

	tasklets, err := r.w.GenTasklets(r.ctx)
	if err != nil {
		span.Errorf("generate tasklets failed: taskID[%s], code[%d],err[%+v]", r.taskID, rpc.DetectStatusCode(err), err)
		r.cancelOrReclaim(err)
		return
	}

	totalDataSize, totalShardCnt := totalDataSizeAndShardCnt(r.w.GetBenchmarkBids())
	remainDataSize, remainShardCnt := totalDataSizeAndShardCntByTasklets(tasklets)
	migratedDataSize := totalDataSize - remainDataSize
	migratedShardCnt := totalShardCnt - remainShardCnt
	r.stats.Total(totalDataSize, totalShardCnt)
	r.stats.Do(migratedDataSize, migratedShardCnt)
	r.statsAndReportTask(0, 0)

	// all tasks are put into the task pool at one time to be executed
	span.Infof("start exec task: taskID[%s], tasklets len[%d]", r.taskID, len(tasklets))
	taskletsPool := taskpool.New(r.taskletRunConcurrency, len(tasklets))
	wg := sync.WaitGroup{}
	for i, t := range tasklets {
		tasklet := t
		_, ctx := trace.StartSpanFromContextWithTraceID(r.ctx, "execTaskletWrap", fmt.Sprintf("%s-%d", span.TraceID(), i))
		wg.Add(1)

		taskletsPool.Run(func() {
			r.execTaskletWrap(ctx, tasklet)
			wg.Done()
		})
	}
	wg.Wait()
	taskletsPool.Close()
	r.cancel()
	span.Infof("all tasklets has finished: taskID[%s]", r.taskID)

	r.stopMu.Lock()
	stopReason := r.stopReason
	r.stopMu.Unlock()
	if stopReason != nil {
		r.cancelOrReclaim(stopReason)
		return
	}

	// so far all tasklets are completed
	// check whether the task is executed correctly
	span.Infof("check task: taskID[%s]", r.taskID)
	err = r.w.Check(r.ctx)
	if err != nil {
		r.cancelOrReclaim(err)
		return
	}

	// task completed，send complete request to scheduler
	r.completeTask()
	span.Infof("task Runner finish: taskID[%s]", r.taskID)
}

func (r *TaskRunner) execTaskletWrap(ctx context.Context, t Tasklet) {
	select {
	case <-r.ctx.Done():
		r.span.Infof("tasklet canceled: taskID[%s]", r.taskID)
	default:
		retErr := r.w.ExecTasklet(ctx, t)
		if retErr != nil {
			r.stopWithFail(retErr)
			return
		}

		r.statsAndReportTask(t.DataSizeByte(), uint64(len(t.bids)))
	}
}

func (r *TaskRunner) TaskID() string {
	return r.taskID
}

func (r *TaskRunner) State() *TaskState {
	return &r.state
}

// Stop stops task
func (r *TaskRunner) Stop() {
	r.state.set(TaskStopping)
	r.stopWithFail(OtherError(errKilled))
}

func (r *TaskRunner) stopWithFail(fail *WorkError) {
	r.span.Infof("stop task: taskID[%s], err_type[%d], err[%+v]", r.taskID, fail.errType, fail.err)
	r.stopMu.Lock()
	if r.stopReason == nil {
		r.stopReason = fail
	}
	r.stopMu.Unlock()
	r.cancel()
}

func (r *TaskRunner) newCtx() context.Context {
	return trace.ContextWithSpan(context.Background(), r.span)
}

func (r *TaskRunner) cancelOrReclaim(retErr *WorkError) {
	span := r.span
	defer r.state.set(TaskStopped)

	if ShouldReclaim(retErr) {
		args := r.w.OperateArgs(retErr.Error())
		span.Infof("reclaim task: taskID[%s], err[%s]", r.taskID, retErr.String())
		if err := r.schedulerCli.ReclaimTask(r.newCtx(), args); err != nil {
			span.Errorf("reclaim task failed: taskID[%s], args[%+v], code[%d], err[%+v]",
				r.taskID, args, rpc.DetectStatusCode(err), err)
		}
		r.taskCounter.reclaim.Add()
		return
	}

	span.Infof("cancel task: taskID[%s], err[%+v]", r.taskID, retErr)

	args := r.w.OperateArgs(retErr.Error())
	if err := r.schedulerCli.CancelTask(r.newCtx(), args); err != nil {
		span.Errorf("cancel failed: taskID[%s], args[%+v], code[%d], err[%+v]",
			r.taskID, args, rpc.DetectStatusCode(err), err)
	}
	r.taskCounter.cancel.Add()
}

func (r *TaskRunner) completeTask() {
	defer r.state.set(TaskSuccess)

	r.span.Infof("complete task: taskID[%s]", r.taskID)
	args := r.w.OperateArgs("")
	if err := r.schedulerCli.CompleteTask(r.newCtx(), args); err != nil {
		r.span.Errorf("complete failed: taskID[%s], args[%+v], code[%d], err[%+v]",
			r.taskID, args, rpc.DetectStatusCode(err), err)
	}
}

func (r *TaskRunner) statsAndReportTask(increaseDataSize, increaseShardCnt uint64) {
	r.stats.Do(increaseDataSize, increaseShardCnt)

	reportArgs := scheduler.TaskReportArgs{
		TaskID:               r.taskID,
		TaskType:             r.w.TaskType(),
		TaskStats:            r.stats.Done(),
		IncreaseDataSizeByte: int(increaseDataSize),
		IncreaseShardCnt:     int(increaseShardCnt),
	}
	data, _ := json.Marshal(reportArgs)

	err := r.schedulerCli.ReportTask(r.newCtx(), &scheduler.TaskArgs{
		TaskType:   reportArgs.TaskType,
		ModuleType: proto.TypeBlobNode,
		Data:       data})
	if err != nil {
		r.span.Errorf("report task failed: taskID[%s], code[%d], err[%+v]", r.taskID, rpc.DetectStatusCode(err), err)
	}
}

// Stopped returns true if task is stopped
func (r *TaskRunner) Stopped() bool {
	return r.state.stopped()
}

// Alive returns true if task is alive
func (r *TaskRunner) Alive() bool {
	return r.state.alive()
}

func totalDataSizeAndShardCntByTasklets(tasklets []Tasklet) (dataSize, shardCnt uint64) {
	var bidsCnt, idx int
	for _, tasklet := range tasklets {
		bidsCnt += len(tasklet.bids)
	}
	bids := make([]*ShardInfoSimple, bidsCnt)
	for _, tasklet := range tasklets {
		for _, bid := range tasklet.bids {
			bids[idx] = bid
			idx++
		}
	}
	return totalDataSizeAndShardCnt(bids)
}

func totalDataSizeAndShardCnt(bids []*ShardInfoSimple) (dataSize, shardCnt uint64) {
	for _, info := range bids {
		dataSize += uint64(info.Size)
	}
	return dataSize, uint64(len(bids))
}
