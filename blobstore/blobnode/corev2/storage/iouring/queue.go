// Copyright 2024 The CubeFS Authors.
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

package iouring

import (
	"math"
	"sync"
)

func newQueue() *queue {
	return &queue{
		queues: [2]struct {
			written bool
			lms     []request
		}{{lms: make([]request, 0, 1024), written: true}, {lms: make([]request, 0, 1024), written: true}},
	}
}

type queue struct {
	currentQueueIdx int
	queues          [2]struct {
		written bool
		lms     []request
	}
	lock sync.Mutex
}

func (q *queue) add(lm request) {
	q.lock.Lock()

	q.queues[q.currentQueueIdx].lms = append(q.queues[q.currentQueueIdx].lms, lm)

	q.lock.Unlock()
	return
}

func (q *queue) addOld(lm request) (queueIdx int) {
	q.lock.Lock()

	q.queues[q.currentQueueIdx].lms = append(q.queues[q.currentQueueIdx].lms, lm)

	lastQueueIdx := (q.currentQueueIdx + 1) % 2
	queueIdx = math.MaxInt64
	// last queue has been written, then start new write operation if first add
	if q.queues[lastQueueIdx].written {
		queueIdx = len(q.queues[q.currentQueueIdx].lms)
	}

	q.lock.Unlock()
	return
}

func (q *queue) drain() ([]request, bool) {
	q.lock.Lock()

	lastQueueIdx := (q.currentQueueIdx + 1) % 2

	// no queue items or another write batch is executing currently
	if len(q.queues[q.currentQueueIdx].lms) == 0 || !q.queues[lastQueueIdx].written {
		q.lock.Unlock()
		return nil, false
	}

	lms := q.queues[q.currentQueueIdx].lms
	q.queues[q.currentQueueIdx].lms = nil
	q.queues[q.currentQueueIdx].written = false
	q.currentQueueIdx = (q.currentQueueIdx + 1) % 2

	q.lock.Unlock()
	return lms, true
}

func (q *queue) recycle(processed []request) {
	q.lock.Lock()
	lastQueueIdx := (q.currentQueueIdx + 1) % 2
	q.queues[lastQueueIdx].written = true
	q.queues[lastQueueIdx].lms = processed[:0]
	q.lock.Unlock()
}
