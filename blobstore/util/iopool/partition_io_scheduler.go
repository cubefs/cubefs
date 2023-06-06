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
	"math"
	"sort"

	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	"github.com/cubefs/cubefs/util/chanutil"
)

type PartitionIoScheduler struct {
	queues []chanutil.Queue
	pool   taskpool.TaskPool
}

func (s *PartitionIoScheduler) workLoop(index int, queueDepth int) {
	localQueue := s.queues[index]
	for {
		items := localQueue.DequeBatch(queueDepth)
		// if len(items) is 0 it means the channel has been closed.
		if len(items) == 0 {
			return
		}
		sort.SliceStable(items, func(l, r int) bool {
			lhs := items[l].(*IoTask)
			rhs := items[r].(*IoTask)
			return lhs.GetHandleID() < rhs.GetHandleID() || (lhs.GetHandleID() == rhs.GetHandleID() && lhs.GetOffset() < rhs.GetOffset())
		})
		syncMap := make(map[int]FileHandle)
		for _, item := range items {
			task := item.(*IoTask)
			task.Exec()
			if task.IsSync() {
				syncMap[int(task.handle.Fd())] = task.handle
			}
		}
		for _, handle := range syncMap {
			handle.Sync()
		}
		for _, item := range items {
			task := item.(*IoTask)
			task.Complete()
		}
	}
}

func (s *PartitionIoScheduler) startWorkers(workerCount int, queueDepth int) {
	for i := 0; i < workerCount; i++ {
		index := i
		s.pool.Run(func() {
			s.workLoop(index, queueDepth)
		})
	}
}

func NewPartitionIoScheduler(workerCount int, queueDepth int) *PartitionIoScheduler {
	queues := make([]chanutil.Queue, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		queue := chanutil.NewQueue(queueDepth)
		queues = append(queues, queue)
	}
	pool := taskpool.New(workerCount, 0)
	scheduler := &PartitionIoScheduler{
		queues: queues,
		pool:   pool,
	}
	scheduler.startWorkers(workerCount, queueDepth)
	return scheduler
}

func (s *PartitionIoScheduler) Submit(task *IoTask) {
	index := int(task.GetHandleID() % uint64(len(s.queues)) % math.MaxInt)
	s.queues[index].Enque(task)
}

func (s *PartitionIoScheduler) Close() {
	count := len(s.queues)
	for i := 0; i < count; i++ {
		s.queues[i].Close()
	}
	s.pool.Close()
}
