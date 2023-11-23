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

package taskpool

import (
	"sync"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

type IoPool interface {
	Submit(taskId uint64, taskFn func())
	Close()
}

type taskInfo struct {
	fn   func()
	done chan struct{}
}

type ioPoolSimple struct {
	queue []chan *taskInfo
	wg    sync.WaitGroup
}

func NewWritePool(threadCnt, queueDepth int) IoPool {
	// The number of chan queues, $chanCnt is one-to-one with $threadCnt
	return newCommonIoPool(threadCnt, threadCnt, queueDepth)
}

func NewReadPool(threadCnt, queueDepth int) IoPool {
	// Multiple $threadCnt share a same chan queue
	return newCommonIoPool(1, threadCnt, queueDepth)
}

// $chanCnt: The number of chan queues
// $threadCnt: The number of read/write work goroutine, it must be greater than $chanCnt
// $queueDepth: The number of elements in the queue
func newCommonIoPool(chanCnt, threadCnt, queueDepth int) *ioPoolSimple {
	pool := &ioPoolSimple{
		queue: make([]chan *taskInfo, chanCnt),
	}
	for i := range pool.queue {
		pool.queue[i] = make(chan *taskInfo, queueDepth)
	}

	for j := 0; j < threadCnt; j++ {
		pool.wg.Add(1)
		idx := j % chanCnt
		// do work
		go func() {
			defer pool.wg.Done()
			for task := range pool.queue[idx] {
				task.fn()
				task.done <- struct{}{}
			}
			log.Debug("close io pool")
		}()
	}
	return pool
}

func (p *ioPoolSimple) Submit(taskId uint64, taskFn func()) {
	idx, task := p.generateTask(taskId, taskFn)
	p.queue[idx] <- task

	<-task.done
}

func (p *ioPoolSimple) generateTask(taskId uint64, taskFn func()) (idx uint64, task *taskInfo) {
	idx = taskId % uint64(len(p.queue))

	task = &taskInfo{
		fn:   taskFn,
		done: make(chan struct{}, 1),
	}

	return idx, task
}

func (p *ioPoolSimple) Close() {
	for i := range p.queue {
		close(p.queue[i])
	}
	p.wg.Wait() // wait all io task done
	log.Info("close all io pool, exit")
}
