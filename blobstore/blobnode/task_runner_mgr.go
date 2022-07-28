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
	wf                                 IWorkerFactory
	schedulerCli                       TaskSchedulerCli
	mu                                 sync.Mutex

	shardGetConcurrency int
}

type IWorkerFactory interface {
	NewMigrateWorker(task MigrateTaskEx) ITaskWorker
}

// TaskWorkerCreator task worker creator
type TaskWorkerCreator struct{}

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
		wf:                                 wf,
		schedulerCli:                       schedulerCli,

		shardGetConcurrency: shardGetConcurrency,
	}
	return tm
}

func (tm *TaskRunnerMgr) AddTask(ctx context.Context, task MigrateTaskEx) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	w := tm.wf.NewMigrateWorker(task)

	var taskletRunConcurrency int
	var mgrType map[string]*TaskRunner

	switch task.taskInfo.TaskType {
	case proto.TaskTypeDiskRepair:
		taskletRunConcurrency = tm.repairTaskletRunConcurrency
		mgrType = tm.repair
	case proto.TaskTypeBalance:
		taskletRunConcurrency = tm.balanceTaskletRunConcurrency
		mgrType = tm.balance
	case proto.TaskTypeDiskDrop:
		taskletRunConcurrency = tm.diskDropTaskletRunConcurrency
		mgrType = tm.diskDrop
	case proto.TaskTypeManualMigrate:
		taskletRunConcurrency = tm.manualMigrateTaskletRunConcurrency
		mgrType = tm.manualMigrate
	}
	runner := NewTaskRunner(
		ctx,
		task.taskInfo.TaskID,
		w, task.taskInfo.SourceIDC,
		taskletRunConcurrency,
		tm.schedulerCli)
	err := addRunner(mgrType, task.taskInfo.TaskID, runner)
	if err != nil {
		return err
	}

	go runner.Run()
	return nil
}

// GetRepairAliveTask returns repair alive task runner
func (tm *TaskRunnerMgr) GetAliveTask(taskType proto.TaskType) []*TaskRunner {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	var mgrType map[string]*TaskRunner
	switch taskType {
	case proto.TaskTypeDiskRepair:
		mgrType = tm.repair
	case proto.TaskTypeBalance:
		mgrType = tm.balance
	case proto.TaskTypeDiskDrop:
		mgrType = tm.diskDrop
	case proto.TaskTypeManualMigrate:
		mgrType = tm.manualMigrate
	}
	return getAliveTask(mgrType)
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
