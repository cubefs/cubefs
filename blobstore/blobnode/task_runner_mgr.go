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

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var errAddRunningTaskAgain = errors.New("running task add again")

// TaskRunnerMgr task runner manager
type TaskRunnerMgr struct {
	repair                      map[string]*TaskRunner
	repairTaskletRunConcurrency int

	balance                      map[string]*TaskRunner
	balanceTaskletRunConcurrency int

	diskDrop                      map[string]*TaskRunner
	diskDropTaskletRunConcurrency int

	manualMigrate                      map[string]*TaskRunner
	manualMigrateTaskletRunConcurrency int

	schedulerCli TaskSchedulerCli
	wf           IWorkerFactory
	mu           sync.Mutex

	shardGetConcurrency int
}

// IWorkerFactory just for UT
type IWorkerFactory interface {
	NewRepairWorker(task VolRepairTaskEx) ITaskWorker
	NewMigrateWorker(task MigrateTaskEx) ITaskWorker
}

// TaskWorkerCreator task worker creator
type TaskWorkerCreator struct{}

// NewRepairWorker returns repair worker
func (wf *TaskWorkerCreator) NewRepairWorker(task VolRepairTaskEx) ITaskWorker {
	return NewRepairWorker(task)
}

// NewMigrateWorker returns migrate worker
func (wf *TaskWorkerCreator) NewMigrateWorker(task MigrateTaskEx) ITaskWorker {
	return NewMigrateWorker(task)
}

// NewTaskRunnerMgr returns task runner manager
func NewTaskRunnerMgr(
	shardGetConcurrency,
	repairTaskletRunConcurrency,
	balanceTaskletRunConcurrency,
	diskDropTaskletRunConcurrency,
	manualMigrateTaskletRunConcurrency int,
	schedulerCli TaskSchedulerCli,
	wf IWorkerFactory,
) *TaskRunnerMgr {
	tm := &TaskRunnerMgr{
		repair:                      make(map[string]*TaskRunner),
		repairTaskletRunConcurrency: repairTaskletRunConcurrency,

		balance:                      make(map[string]*TaskRunner),
		balanceTaskletRunConcurrency: balanceTaskletRunConcurrency,

		diskDrop:                      make(map[string]*TaskRunner),
		diskDropTaskletRunConcurrency: diskDropTaskletRunConcurrency,

		manualMigrate:                      make(map[string]*TaskRunner),
		manualMigrateTaskletRunConcurrency: manualMigrateTaskletRunConcurrency,

		wf:           wf,
		schedulerCli: schedulerCli,

		shardGetConcurrency: shardGetConcurrency,
	}
	return tm
}

// AddRepairTask adds repair task
func (tm *TaskRunnerMgr) AddRepairTask(ctx context.Context, task VolRepairTaskEx) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	w := tm.wf.NewRepairWorker(task)
	runner := NewTaskRunner(
		ctx,
		task.taskInfo.TaskID,
		w, task.taskInfo.BrokenDiskIDC,
		tm.repairTaskletRunConcurrency,
		tm.schedulerCli)
	err := addRunner(tm.repair, task.taskInfo.TaskID, runner)
	if err != nil {
		return err
	}

	go runner.Run()
	return nil
}

// AddBalanceTask adds balance task
func (tm *TaskRunnerMgr) AddBalanceTask(ctx context.Context, task MigrateTaskEx) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	w := tm.wf.NewMigrateWorker(task)
	runner := NewTaskRunner(
		ctx,
		task.taskInfo.TaskID,
		w, task.taskInfo.SourceIdc,
		tm.balanceTaskletRunConcurrency,
		tm.schedulerCli)
	err := addRunner(tm.balance, task.taskInfo.TaskID, runner)
	if err != nil {
		return err
	}

	go runner.Run()
	return nil
}

// AddDiskDropTask adds disk drop task
func (tm *TaskRunnerMgr) AddDiskDropTask(ctx context.Context, task MigrateTaskEx) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	w := tm.wf.NewMigrateWorker(task)
	runner := NewTaskRunner(
		ctx,
		task.taskInfo.TaskID,
		w, task.taskInfo.SourceIdc,
		tm.diskDropTaskletRunConcurrency,
		tm.schedulerCli)
	err := addRunner(tm.diskDrop, task.taskInfo.TaskID, runner)
	if err != nil {
		return err
	}

	go runner.Run()
	return nil
}

// AddManualMigrateTask adds manual migrate task
func (tm *TaskRunnerMgr) AddManualMigrateTask(ctx context.Context, task MigrateTaskEx) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	w := tm.wf.NewMigrateWorker(task)
	runner := NewTaskRunner(
		ctx,
		task.taskInfo.TaskID,
		w, task.taskInfo.SourceIdc,
		tm.manualMigrateTaskletRunConcurrency,
		tm.schedulerCli)
	err := addRunner(tm.manualMigrate, task.taskInfo.TaskID, runner)
	if err != nil {
		return err
	}

	go runner.Run()
	return nil
}

// GetRepairAliveTask returns repair alive task runner
func (tm *TaskRunnerMgr) GetRepairAliveTask() []*TaskRunner {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return getAliveTask(tm.repair)
}

// GetBalanceAliveTask returns balance alive task runner
func (tm *TaskRunnerMgr) GetBalanceAliveTask() []*TaskRunner {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return getAliveTask(tm.balance)
}

// GetDiskDropAliveTask returns disk drop alive task runner
func (tm *TaskRunnerMgr) GetDiskDropAliveTask() []*TaskRunner {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return getAliveTask(tm.diskDrop)
}

// GetManualMigrateAliveTask returns manual migrate alive task runner
func (tm *TaskRunnerMgr) GetManualMigrateAliveTask() []*TaskRunner {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return getAliveTask(tm.manualMigrate)
}

// StopTaskRunner stops task runner
func (tm *TaskRunnerMgr) StopTaskRunner(taskID, taskType string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	switch taskType {
	case proto.RepairTaskType:
		return stopRunner(tm.repair, taskID)
	case proto.BalanceTaskType:
		return stopRunner(tm.balance, taskID)
	case proto.DiskDropTaskType:
		return stopRunner(tm.diskDrop, taskID)
	case proto.ManualMigrateType:
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

	runners := getAliveTask(tm.repair)
	runners = append(runners, getAliveTask(tm.balance)...)
	runners = append(runners, getAliveTask(tm.diskDrop)...)
	runners = append(runners, getAliveTask(tm.manualMigrate)...)
	for _, r := range runners {
		r.Stop()
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

func getAliveTask(m map[string]*TaskRunner) []*TaskRunner {
	alive := make([]*TaskRunner, 0)
	for _, r := range m {
		if r.Alive() {
			alive = append(alive, r)
		}
	}
	return alive
}
