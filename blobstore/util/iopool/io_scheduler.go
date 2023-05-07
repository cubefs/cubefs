// Copyright 2023 The CubeFS Authors.
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

package iopool

import (
	"sort"
	"sync"
	"sync/atomic"
)

type IoScheduler interface {
	Schedule(task *IoTask)
}

type FileController struct {
	routine      *GoroutineWrap
	taskCount    uint32
	pendingTasks []*IoTask
	tasks        []*IoTask
	taskLock     *sync.Mutex
	routineLock  *sync.Mutex
}

func NewFileController() *FileController {
	return &FileController{
		routine:      nil,
		pendingTasks: make([]*IoTask, 0),
		tasks:        make([]*IoTask, 0),
		taskLock:     &sync.Mutex{},
		routineLock:  &sync.Mutex{},
	}
}

func (cb *FileController) addTaskCount(count uint32) uint32 {
	return atomic.AddUint32(&cb.taskCount, count)
}

func (cb *FileController) subtractTaskCount(count int32) uint32 {
	return atomic.AddUint32(&cb.taskCount, ^uint32(count-1))
}

func (cb *FileController) addTask(task *IoTask) {
	cb.taskLock.Lock()
	defer cb.taskLock.Unlock()
	cb.pendingTasks = append(cb.pendingTasks, task)
}

func (cb *FileController) swapTasks() {
	cb.taskLock.Lock()
	defer cb.taskLock.Unlock()
	cb.pendingTasks, cb.tasks = cb.tasks, cb.pendingTasks
}

func (cb *FileController) exec(pool *GoroutinePool) {
	for {
		cb.swapTasks()
		if len(cb.tasks) == 0 {
			continue
		}
		sort.SliceStable(cb.tasks, func(i, j int) bool {
			return cb.tasks[i].offset < cb.tasks[j].offset
		})
		isSync := false
		for _, task := range cb.tasks {
			task.Exec()
			isSync = isSync || task.IsSync()
			if !task.IsSync() {
				task.Complete()
			}
		}
		count := len(cb.tasks)
		// sync file
		if isSync {
			cb.tasks[0].Sync()
		}
		// complete sync tasks
		for _, task := range cb.tasks {
			if task.IsSync() {
				task.Complete()
			}
		}
		// clear tasks queue
		cb.tasks = cb.tasks[:0]
		// release goroutine if needed
		newCount := cb.subtractTaskCount(int32(count))
		if newCount == 0 {
			cb.routineLock.Lock()
			defer cb.routineLock.Unlock()
			newCount = atomic.LoadUint32(&cb.taskCount)
			if newCount == 0 {
				var routine *GoroutineWrap
				cb.routine, routine = routine, cb.routine
				pool.ReleaseOne(routine)
				return
			}
		}
	}

}

func (cb *FileController) Submit(task *IoTask, pool *GoroutinePool) {
	cb.addTaskCount(1)
	cb.addTask(task)
	cb.routineLock.Lock()
	defer cb.routineLock.Unlock()
	if cb.routine == nil {
		cb.routine = pool.RequireOne()
		cb.routine.Submit(func() {
			cb.exec(pool)
		})
	}
}

type SimpleIoScheduler struct {
	pool            *GoroutinePool
	controllerTable map[uint64]*FileController
	tabelLock       *sync.RWMutex
}

func (scheduler *SimpleIoScheduler) getControllerLockless(id uint64) *FileController {
	ctrl, exist := scheduler.controllerTable[id]
	if exist {
		return ctrl
	}
	return nil
}

func (scheduler *SimpleIoScheduler) getController(id uint64) *FileController {
	scheduler.tabelLock.RLock()
	defer scheduler.tabelLock.RUnlock()
	return scheduler.getControllerLockless(id)
}

func (scheduler *SimpleIoScheduler) ensureController(id uint64) *FileController {
	ctrl := scheduler.getController(id)
	if ctrl == nil {
		scheduler.tabelLock.Lock()
		defer scheduler.tabelLock.Unlock()
		ctrl = scheduler.getControllerLockless(id)
		if ctrl == nil {
			ctrl = NewFileController()
			scheduler.controllerTable[id] = ctrl
		}
	}
	return ctrl
}

func (scheduler *SimpleIoScheduler) Schedule(task *IoTask) {
	ctrl := scheduler.ensureController(task.handleID)
	ctrl.Submit(task, scheduler.pool)
}

func NewSimpleIoScheduler(pool *GoroutinePool) *SimpleIoScheduler {
	return &SimpleIoScheduler{
		pool:            pool,
		controllerTable: make(map[uint64]*FileController),
		tabelLock:       &sync.RWMutex{},
	}
}

type ShardedIoScheduler struct {
	subScheduler []IoScheduler
}

func (scheduler *ShardedIoScheduler) Schedule(task *IoTask) {
	index := uint(task.GetHandleID() % uint64(len(scheduler.subScheduler)))
	scheduler.subScheduler[index].Schedule(task)
}

func NewShardedIoScheduler(count uint32, pool *GoroutinePool) *ShardedIoScheduler {
	if count == 0 {
		count = 64
	}
	schedulers := make([]IoScheduler, 0, count)
	for i := uint32(0); i < count; i++ {
		schedulers = append(schedulers, NewSimpleIoScheduler(pool))
	}
	return &ShardedIoScheduler{
		subScheduler: schedulers,
	}
}
