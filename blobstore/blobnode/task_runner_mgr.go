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
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var errAddRunningTaskAgain = errors.New("running task add again")

// WorkerGenerator generates task worker.
type WorkerGenerator = func(task MigrateTaskEx) ITaskWorker

// TaskRunnerMgr task runner manager
type TaskRunnerMgr struct {
	repair        map[string]*TaskRunner
	balance       map[string]*TaskRunner
	diskDrop      map[string]*TaskRunner
	manualMigrate map[string]*TaskRunner

	mu           sync.Mutex
	idc          string
	meter        WorkerConfigMeter
	schedulerCli TaskSchedulerCli
	genWorker    WorkerGenerator
}

// NewTaskRunnerMgr returns task runner manager
func NewTaskRunnerMgr(idc string, meter WorkerConfigMeter, schedulerCli TaskSchedulerCli, genWorker WorkerGenerator) *TaskRunnerMgr {
	return &TaskRunnerMgr{
		repair:        make(map[string]*TaskRunner),
		balance:       make(map[string]*TaskRunner),
		diskDrop:      make(map[string]*TaskRunner),
		manualMigrate: make(map[string]*TaskRunner),

		idc:          idc,
		meter:        meter,
		schedulerCli: schedulerCli,
		genWorker:    genWorker,
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
	ret, err := tm.schedulerCli.RenewalTask(ctx, &scheduler.TaskRenewalArgs{IDC: tm.idc, IDs: aliveTasks})
	if err != nil {
		span.Errorf("renewal task failed and stop all runner: err[%+v]", err)
		tm.StopAllAliveRunner()
		return
	}

	for typ, errs := range ret.Errors {
		for taskID, errMsg := range errs {
			span.Warnf("renewal fail so stop runner: type[%s], taskID[%s], error[%s]", typ, taskID, errMsg)
			if err := tm.StopTaskRunner(taskID, typ); err != nil {
				span.Errorf("stop runner failed: type[%s], taskID[%s], err[%+v]", typ, taskID, err)
			}
		}
	}
}

// AddTask add migrate task.
func (tm *TaskRunnerMgr) AddTask(ctx context.Context, task MigrateTaskEx) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	var concurrency int
	var mgrType map[string]*TaskRunner

	switch task.taskInfo.TaskType {
	case proto.TaskTypeDiskRepair:
		concurrency = tm.meter.RepairConcurrency
		mgrType = tm.repair
	case proto.TaskTypeBalance:
		concurrency = tm.meter.BalanceConcurrency
		mgrType = tm.balance
	case proto.TaskTypeDiskDrop:
		concurrency = tm.meter.DiskDropConcurrency
		mgrType = tm.diskDrop
	case proto.TaskTypeManualMigrate:
		concurrency = tm.meter.ManualMigrateConcurrency
		mgrType = tm.manualMigrate
	}

	w := tm.genWorker(task)
	runner := NewTaskRunner(ctx, task.taskInfo.TaskID, w, task.taskInfo.SourceIDC, concurrency, tm.schedulerCli)
	err := addRunner(mgrType, task.taskInfo.TaskID, runner)
	if err != nil {
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
	if tasks := getAliveTask(tm.repair); len(tasks) > 0 {
		all[proto.TaskTypeDiskRepair] = tasks
	}
	if tasks := getAliveTask(tm.balance); len(tasks) > 0 {
		all[proto.TaskTypeBalance] = tasks
	}
	if tasks := getAliveTask(tm.diskDrop); len(tasks) > 0 {
		all[proto.TaskTypeDiskDrop] = tasks
	}
	if tasks := getAliveTask(tm.manualMigrate); len(tasks) > 0 {
		all[proto.TaskTypeManualMigrate] = tasks
	}

	return all
}

// StopTaskRunner stops task runner
func (tm *TaskRunnerMgr) StopTaskRunner(taskID string, taskType proto.TaskType) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	switch taskType {
	case proto.TaskTypeDiskRepair:
		return stopRunner(tm.repair, taskID)
	case proto.TaskTypeBalance:
		return stopRunner(tm.balance, taskID)
	case proto.TaskTypeDiskDrop:
		return stopRunner(tm.diskDrop, taskID)
	case proto.TaskTypeManualMigrate:
		return stopRunner(tm.manualMigrate, taskID)
	default:
		log.Panicf("unknown task type %s", taskType)
	}
	return nil
}

// StopAllAliveRunner stops all alive runner
func (tm *TaskRunnerMgr) StopAllAliveRunner() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, runners := range []map[string]*TaskRunner{
		tm.repair, tm.balance, tm.diskDrop, tm.manualMigrate,
	} {
		for _, r := range runners {
			if r.Alive() {
				r.Stop()
			}
		}
	}
}

// RunningTaskCnt return running task count
func (tm *TaskRunnerMgr) RunningTaskCnt() (repair, balance, drop, manualMigrate int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.removeStoppedRunner()
	return len(tm.repair), len(tm.balance), len(tm.diskDrop), len(tm.manualMigrate)
}

func (tm *TaskRunnerMgr) removeStoppedRunner() {
	tm.repair = removeStoppedRunner(tm.repair)
	tm.balance = removeStoppedRunner(tm.balance)
	tm.diskDrop = removeStoppedRunner(tm.diskDrop)
	tm.manualMigrate = removeStoppedRunner(tm.manualMigrate)
}

func removeStoppedRunner(tasks map[string]*TaskRunner) map[string]*TaskRunner {
	newTasks := make(map[string]*TaskRunner)
	for taskID, task := range tasks {
		if task.Stopped() {
			log.Infof("remove stopped task: taskID[%s], state[%d]", task.taskID, task.state.state)
			continue
		}
		log.Debugf("remain task: taskID[%s], state[%d]", task.taskID, task.state.state)
		newTasks[taskID] = task
	}
	return newTasks
}

func addRunner(m map[string]*TaskRunner, taskID string, r *TaskRunner) error {
	if r, ok := m[taskID]; ok {
		if !r.Stopped() {
			log.Warnf("task is running shouldn't add again: taskID[%s]", taskID)
			return errAddRunningTaskAgain
		}
	}
	m[taskID] = r
	return nil
}

func stopRunner(m map[string]*TaskRunner, taskID string) error {
	if r, ok := m[taskID]; ok {
		r.Stop()
		return nil
	}
	return errors.New("no such task")
}

func getAliveTask(m map[string]*TaskRunner) []string {
	alive := make([]string, 0, 16)
	for _, r := range m {
		if r.Alive() {
			alive = append(alive, r.taskID)
		}
	}
	return alive
}
