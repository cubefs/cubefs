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

package util

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
	"golang.org/x/time/rate"
)

const (
	minusOne           = ^uint32(0)
	defaultQueueFactor = 8
)

type IoLimiter struct {
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
	Factor        int
}

var (
	IOLimitTicket      = 60 * 1000 // 1 min
	IOLimitTicketInner = time.Millisecond * 100
	LimitedIoError     = errors.New("limited io error")
	LimitedFlowError   = errors.New("flow limited")
	LimitedRunError    = errors.New("run limited")
)

// flow rate limiter's burst is double limit.
// max queue size of io is 8-times io concurrency.
func NewIOLimiter(flowLimit, ioConcurrency int) *IoLimiter {
	return NewIOLimiterEx(flowLimit, ioConcurrency, 0, 0)
}

func NewIOLimiterEx(flowLimit, ioConcurrency, factor, hangMaxSecond int) *IoLimiter {
	flow := rate.NewLimiter(rate.Inf, 0)
	if flowLimit > 0 {
		flow = rate.NewLimiter(rate.Limit(flowLimit), flowLimit/2)
	}
	l := &IoLimiter{limit: flowLimit, flow: flow}
	l.io.Store(newIOQueue(ioConcurrency, factor, hangMaxSecond))
	return l
}

func (l *IoLimiter) getIO() *ioQueue {
	return l.io.Load().(*ioQueue)
}

func (l *IoLimiter) ResetFlow(flowLimit int) {
	l.limit = flowLimit
	if flowLimit <= 0 {
		l.flow.SetLimit(rate.Inf)
		l.flow.SetBurst(0)
	} else {
		l.flow.SetLimit(rate.Limit(flowLimit))
		l.flow.SetBurst(flowLimit / 2)
	}
}

func (l *IoLimiter) ResetIO(ioConcurrency, factor int) {
	q := l.io.Swap(newIOQueueEx(ioConcurrency, factor)).(*ioQueue)
	q.Close()
}

func (l *IoLimiter) ResetIOEx(ioConcurrency, factor, hangMaxMillSecond int) {
	q := l.io.Swap(newIOQueue(ioConcurrency, factor, hangMaxMillSecond)).(*ioQueue)
	q.Close()
}

func (l *IoLimiter) Run(size int, allowHang bool, taskFn func()) (err error) {
	if size > 0 && l.limit > 0 {
		if err := l.flow.WaitN(context.Background(), size); err != nil {
			log.LogWarnf("action[limitio] run wait flow with %d %s", size, err.Error())
		}
	}
	return l.getIO().Run(taskFn, allowHang)
}

func (l *IoLimiter) RunNoWait(size int, allowHang bool, taskFn func()) (err error) {
	if size > 0 && l.limit > 0 {
		if !l.flow.AllowN(time.Now(), size) {
			return fmt.Errorf("flow limited")
		}
	}
	return l.getIO().Run(taskFn, allowHang)
}

func (l *IoLimiter) TryRun(size int, taskFn func()) bool {
	if ok := l.getIO().TryRun(taskFn, false); !ok {
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

func (l *IoLimiter) TryRunAsync(size int, taskFn func()) error {
	if size > 0 && l.limit > 0 {
		if err := l.flow.WaitN(context.Background(), size); err != nil {
			return err
		}
	}
	if ok := l.getIO().TryRun(taskFn, true); !ok {
		return LimitedRunError
	}
	return nil
}

func (l *IoLimiter) Status(ignoreUsed bool) (st LimiterStatus) {
	st = l.getIO().Status()

	limit := l.limit
	st.FlowLimit = limit
	if limit > 0 && !ignoreUsed {
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

func (l *IoLimiter) Close() {
	q := l.io.Swap(newIOQueue(0, 0, 0)).(*ioQueue)
	q.Close()
}

type task struct {
	fn   func()
	done chan struct{}
	tm   time.Time
	err  error
}

type ioQueue struct {
	wg                sync.WaitGroup
	once              sync.Once
	running           uint32
	concurrency       int
	stopCh            chan struct{}
	queue             chan *task
	midQueue          chan *task
	factor            int
	hangMaxMillSecond int
}

func newIOQueueEx(concurrency, factor int) *ioQueue {
	return newIOQueue(concurrency, factor, 0)
}

func newIOQueue(concurrency, factor, hangMaxMillSecond int) *ioQueue {
	q := &ioQueue{concurrency: concurrency}
	if q.concurrency <= 0 {
		return q
	}

	if factor <= 0 {
		factor = defaultQueueFactor
	}
	q.hangMaxMillSecond = hangMaxMillSecond
	if hangMaxMillSecond <= 0 {
		q.hangMaxMillSecond = IOLimitTicket
	}
	q.factor = factor
	q.midQueue = make(chan *task, 100)
	q.stopCh = make(chan struct{})
	q.queue = make(chan *task, factor*concurrency)
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

	go q.innerRun()

	return q
}

func (q *ioQueue) innerRun() {
	tickerInner := time.NewTicker(IOLimitTicketInner)
	defer tickerInner.Stop()

	for {
		select {
		case <-q.stopCh:
			return
		case task := <-q.midQueue:
			if timeutil.GetCurrentTime().After(task.tm.Add(time.Duration(q.hangMaxMillSecond) * time.Millisecond)) {
				task.err = LimitedIoError
				close(task.done)
				continue
			}
			stop := false
			for !stop {
				select {
				case <-q.stopCh:
					return
				case q.queue <- task:
					stop = true
				case <-tickerInner.C:
					if timeutil.GetCurrentTime().After(task.tm.Add(time.Duration(q.hangMaxMillSecond) * time.Millisecond)) {
						task.err = LimitedIoError
						close(task.done)
						stop = true
					}
				}
			}
		}
	}
}

func (q *ioQueue) Run(taskFn func(), allowHang bool) (err error) {
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
	ch := q.queue
	if !allowHang {
		ch = q.midQueue
	}
	task := &task{fn: taskFn, done: make(chan struct{}), tm: timeutil.GetCurrentTime()}
	select {
	case <-q.stopCh:
		taskFn()
	case ch <- task:
		<-task.done
		return task.err
	}
	return
}

func (q *ioQueue) TryRun(taskFn func(), async bool) bool {
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
		if !async {
			<-task.done
		}
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
	st.Factor = q.factor
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
