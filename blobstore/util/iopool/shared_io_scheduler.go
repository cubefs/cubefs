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
	"context"

	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	"github.com/cubefs/cubefs/util/chanutil"
)

type SharedIoScheduler struct {
	queue chanutil.Queue
	pool  taskpool.TaskPool
}

func NewSharedIoScheduler(workerCount int, queueDepth int) *SharedIoScheduler {
	// we never use the queue of taskpool
	// so the poolSize of taskpool.New should be 0
	scheduler := &SharedIoScheduler{
		queue: chanutil.NewQueue(queueDepth),
		pool:  taskpool.New(workerCount, 0),
	}
	scheduler.startWorkers(workerCount)
	return scheduler
}

func (s *SharedIoScheduler) startWorkers(workerCount int) {
	for i := 0; i < workerCount; i++ {
		s.pool.Run(func() {
			for {
				item, ok := s.queue.Deque()
				if !ok {
					return
				}
				task := item.(*IoTask)
				task.Exec()
				if task.IsSync() {
					task.Sync()
				}
				task.Complete()
			}
		})
	}
}

func (s *SharedIoScheduler) Submit(task *IoTask) {
	s.queue.Enque(task)
}

func (s *SharedIoScheduler) TrySubmit(task *IoTask) bool {
	return s.queue.TryEnque(task)
}

func (s *SharedIoScheduler) SubmitWithContext(task *IoTask, ctx context.Context) bool {
	return s.queue.EnqueWithContext(task, ctx)
}

func (s *SharedIoScheduler) Close() {
	s.queue.Close()
	s.pool.Close()
}
