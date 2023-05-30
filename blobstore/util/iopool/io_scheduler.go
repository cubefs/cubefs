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

	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

type IoScheduler interface {
	Schedule(task *IoTask)
}

type FileController struct {
	taskCount    uint32
	pendingTasks []*IoTask
	tasks        []*IoTask
	taskLock     *sync.Mutex
}

func NewFileController() *FileController {
	return &FileController{
		pendingTasks: make([]*IoTask, 0),
		tasks:        make([]*IoTask, 0),
		taskLock:     &sync.Mutex{},
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

func (cb *FileController) runLoop() {
	for {
		cb.swapTasks()
		if len(cb.tasks) != 0 {
			sort.SliceStable(cb.tasks, func(i, j int) bool {
				return cb.tasks[i].offset < cb.tasks[j].offset
			})
			isSync := false
			for _, task := range cb.tasks {
				task.Exec()
				isSync = isSync || task.IsSync()
			}
			// sync file
			// these tasks have the same handle, so we only need to sync tasks[0]
			if isSync {
				cb.tasks[0].Sync()
			}
			// complete tasks
			for _, task := range cb.tasks {
				task.Complete()
			}
		}
		count := len(cb.tasks)
		// clear tasks queue
		cb.tasks = cb.tasks[:0]
		// release goroutine if needed
		newCount := cb.subtractTaskCount(int32(count))
		if newCount == 0 {
			return
		}
	}
}

func (cb *FileController) Submit(task *IoTask, pool taskpool.TaskPool) {
	count := cb.addTaskCount(1)
	cb.addTask(task)
	if count == 1 {
		// if count is 1, we need to submit a task
		pool.Run(func() {
			cb.runLoop()
		})
	}
}

type SimpleIoScheduler struct {
	pool            taskpool.TaskPool
	controllerTable map[uint64]*FileController
	tableLock       *sync.RWMutex
}

func (s *SimpleIoScheduler) getControllerLockless(id uint64) *FileController {
	return s.controllerTable[id]
}

func (s *SimpleIoScheduler) getController(id uint64) *FileController {
	s.tableLock.RLock()
	defer s.tableLock.RUnlock()
	return s.getControllerLockless(id)
}

func (s *SimpleIoScheduler) ensureController(id uint64) *FileController {
	ctrl := s.getController(id)
	if ctrl == nil {
		s.tableLock.Lock()
		defer s.tableLock.Unlock()
		ctrl = s.getControllerLockless(id)
		if ctrl == nil {
			ctrl = NewFileController()
			s.controllerTable[id] = ctrl
		}
	}
	return ctrl
}

func (s *SimpleIoScheduler) Schedule(task *IoTask) {
	ctrl := s.ensureController(task.handleID)
	ctrl.Submit(task, s.pool)
}

func NewSimpleIoScheduler(pool taskpool.TaskPool) *SimpleIoScheduler {
	return &SimpleIoScheduler{
		pool:            pool,
		controllerTable: make(map[uint64]*FileController),
		tableLock:       &sync.RWMutex{},
	}
}

type ShardedIoScheduler struct {
	subScheduler []IoScheduler
}

func (s *ShardedIoScheduler) Schedule(task *IoTask) {
	index := uint(task.GetHandleID() % uint64(len(s.subScheduler)))
	s.subScheduler[index].Schedule(task)
}

const defaultSharedIoSchedulerCount = 64

func NewShardedIoScheduler(count uint32, pool taskpool.TaskPool) *ShardedIoScheduler {
	if count == 0 {
		count = defaultSharedIoSchedulerCount
	}
	schedulers := make([]IoScheduler, 0, count)
	for i := uint32(0); i < count; i++ {
		schedulers = append(schedulers, NewSimpleIoScheduler(pool))
	}
	return &ShardedIoScheduler{
		subScheduler: schedulers,
	}
}
