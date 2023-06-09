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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var errAddRunningTaskAgain = errors.New("running task add again")

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
	renewalCli, schedulerCli scheduler.IMigrator) *TaskRunnerMgr {
	return &TaskRunnerMgr{
		typeMgr: map[proto.TaskType]mapTaskRunner{
			proto.TaskTypeBalance:       make(mapTaskRunner),
			proto.TaskTypeDiskDrop:      make(mapTaskRunner),
			proto.TaskTypeDiskRepair:    make(mapTaskRunner),
			proto.TaskTypeManualMigrate: make(mapTaskRunner),
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

// AddTask add migrate task.
func (tm *TaskRunnerMgr) AddTask(ctx context.Context, task MigrateTaskEx) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	t := task.taskInfo
	mgr, ok := tm.typeMgr[t.TaskType]
	if !ok {
		return fmt.Errorf("invalid task type: %s", t.TaskType)
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
		mgr.removeStoppedRunner()
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

type mapTaskRunner map[string]*TaskRunner

func (m mapTaskRunner) removeStoppedRunner() {
	for taskID, task := range m {
		if task.Stopped() {
			delete(m, taskID)
			log.Infof("remove stopped task: taskID[%s], state[%d]", task.taskID, task.state.state)
			continue
		}
		log.Debugf("remain task: taskID[%s], state[%d]", task.taskID, task.state.state)
	}
	return
}

func (m mapTaskRunner) addTask(taskID string, runner *TaskRunner) error {
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
			alive = append(alive, r.taskID)
		}
	}
	return alive
}
