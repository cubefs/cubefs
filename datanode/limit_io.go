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

package datanode

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

const minusOne = ^uint32(0)

type ioLimiter struct {
	limit int
	flow  *rate.Limiter
	io    atomic.Value
}

type LimiterStatus struct {
	FlowLimit int
	FlowUsed  int

	IOConcurrency int
	IOQueue       int
	IORunning     int
	IOWaiting     int
}

// flow rate limiter's burst is double limit.
// max queue size of io is 8-times io concurrency.
func newIOLimiter(flowLimit, ioConcurrency int) *ioLimiter {
	flow := rate.NewLimiter(rate.Inf, 0)
	if flowLimit > 0 {
		flow = rate.NewLimiter(rate.Limit(flowLimit), 2*flowLimit)
	}
	l := &ioLimiter{limit: flowLimit, flow: flow}
	l.io.Store(newIOQueue(ioConcurrency))
	return l
}

func (l *ioLimiter) getIO() *ioQueue {
	return l.io.Load().(*ioQueue)
}

func (l *ioLimiter) ResetFlow(flowLimit int) {
	l.limit = flowLimit
	if flowLimit <= 0 {
		l.flow.SetLimit(rate.Inf)
		l.flow.SetBurst(0)
	} else {
		l.flow.SetLimit(rate.Limit(flowLimit))
		l.flow.SetBurst(2 * flowLimit)
	}
}

func (l *ioLimiter) ResetIO(ioConcurrency int) {
	q := l.io.Swap(newIOQueue(ioConcurrency)).(*ioQueue)
	q.Close()
}

func (l *ioLimiter) Run(size int, taskFn func()) {
	if size > 0 {
		if err := l.flow.WaitN(context.Background(), size); err != nil {
			log.LogWarnf("action[limitio] run wait flow with %d %s", size, err.Error())
		}
	}
	l.getIO().Run(taskFn)
}

func (l *ioLimiter) TryRun(size int, taskFn func()) bool {
	if ok := l.getIO().TryRun(taskFn); !ok {
		return false
	}
	if size > 0 {
		if err := l.flow.WaitN(context.Background(), size); err != nil {
			log.LogWarnf("action[limitio] tryrun wait flow with %d %s", size, err.Error())
			return false
		}
	}
	return true
}

func (l *ioLimiter) Status() (st LimiterStatus) {
	st = l.getIO().Status()

	limit := l.limit
	st.FlowLimit = limit
	if limit > 0 {
		now := time.Now()
		reserve := l.flow.ReserveN(now, l.flow.Burst())
		duration := reserve.DelayFrom(now)
		reserve.Cancel()

		if ms := duration.Microseconds(); ms > 0 {
			st.FlowUsed = int(math.Ceil(float64(limit) * (float64(ms) / 1e6)))
		}
	}
	return
}

func (l *ioLimiter) Close() {
	q := l.io.Swap(newIOQueue(0)).(*ioQueue)
	q.Close()
}

type task struct {
	fn   func()
	done chan struct{}
}

type ioQueue struct {
	wg          sync.WaitGroup
	once        sync.Once
	running     uint32
	concurrency int
	stopCh      chan struct{}
	queue       chan *task
}

func newIOQueue(concurrency int) *ioQueue {
	q := &ioQueue{concurrency: concurrency}
	if q.concurrency <= 0 {
		return q
	}

	q.stopCh = make(chan struct{})
	q.queue = make(chan *task, 8*concurrency)
	q.wg.Add(concurrency)
	for ii := 0; ii < concurrency; ii++ {
		go func() {
			defer q.wg.Done()
			for {
				select {
				case <-q.stopCh:
					return
				case task := <-q.queue:
					atomic.AddUint32(&q.running, 1)
					task.fn()
					atomic.AddUint32(&q.running, minusOne)
					close(task.done)
				}
			}
		}()
	}
	return q
}

func (q *ioQueue) Run(taskFn func()) {
	if q.concurrency <= 0 {
		taskFn()
		return
	}

	select {
	case <-q.stopCh:
		taskFn()
		return
	default:
	}

	task := &task{fn: taskFn, done: make(chan struct{})}
	select {
	case <-q.stopCh:
		taskFn()
	case q.queue <- task:
		<-task.done
	}
}

func (q *ioQueue) TryRun(taskFn func()) bool {
	if q.concurrency <= 0 {
		taskFn()
		return true
	}

	select {
	case <-q.stopCh:
		taskFn()
		return true
	default:
	}

	task := &task{fn: taskFn, done: make(chan struct{})}
	select {
	case <-q.stopCh:
		taskFn()
		return true
	case q.queue <- task:
		<-task.done
		return true
	default:
		return false
	}
}

func (q *ioQueue) Status() (st LimiterStatus) {
	st.IOConcurrency = q.concurrency
	st.IOQueue = cap(q.queue)
	st.IORunning = int(atomic.LoadUint32(&q.running))
	st.IOWaiting = len(q.queue)
	return
}

func (q *ioQueue) Close() {
	q.once.Do(func() {
		if q.concurrency > 0 {
			close(q.stopCh)
		}
	})
	q.wg.Wait()

	// wait one minute if no task in the queue
	// to protect task been blocked.
	go func() {
		waitTimer := time.NewTimer(time.Minute)
		defer waitTimer.Stop()
		for {
			select {
			case task := <-q.queue:
				task.fn()
				close(task.done)
				waitTimer.Reset(time.Minute)
			case <-waitTimer.C:
				return
			}
		}
	}()
}
