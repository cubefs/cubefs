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

package base

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	opDequeue = "dequeue"
	opOnDisk  = "disk"
)

type IoPoolTaskArgs struct {
	BucketId uint64
	Tm       time.Time
	Ctx      context.Context
	TaskFn   func() error
}

type IoPool interface {
	Submit(IoPoolTaskArgs) error
	Close()
}

type IoPoolMetricConf struct {
	ClusterID uint32
	DiskID    uint32
	Host      string
	poolType  string
}

type taskInfo struct {
	fn   func() error
	done chan error
	tm   time.Time
	ctx  context.Context
}

type ioPoolSimple struct {
	queue  []chan *taskInfo
	wg     sync.WaitGroup
	closed chan struct{}

	conf   IoPoolMetricConf
	metric *prometheus.SummaryVec
}

func (p *ioPoolSimple) Submit(args IoPoolTaskArgs) error {
	if p.notLimit() {
		return args.TaskFn()
	}

	idx, task := p.generateTask(args)

	// if ctx has been cancelled, try to avoid enqueuing as much as possible;
	// even if it has enqueued, doWork/task.fn will judge ctx again
	select {
	// don't enqueue
	case <-task.ctx.Done():
		return task.ctx.Err()
	// closing, try to complete the task
	case <-p.closed:
		return args.TaskFn()
	// 1.normal enqueue -> do work; 2.when closing, tasks that are already in the queue will be executed
	case p.queue[idx] <- task:
		return <-task.done
	}
}

// Close the pool, Submit task maybe concurrent unsafe
func (p *ioPoolSimple) Close() {
	close(p.closed)
	p.wg.Wait() // wait all io task done

	for _, val := range p.queue {
		go func(queue chan *taskInfo) {
			waitTimer := time.NewTimer(time.Minute)
			defer waitTimer.Stop()
			for {
				select {
				case task := <-queue:
					p.doWork(task)
					waitTimer.Reset(time.Minute)
				case <-waitTimer.C:
					return
				}
			}
		}(val)
	}

	log.Info("close all io pool, exit")
}

func (p *ioPoolSimple) backgroundExecute(chanCnt, threadCnt int) {
	for j := 0; j < threadCnt; j++ {
		p.wg.Add(1)
		idx := j % chanCnt
		// do work
		go func() {
			defer func() {
				log.Debug("close io pool")
				p.wg.Done()
			}()

			for {
				select {
				case <-p.closed:
					return
				case task := <-p.queue[idx]:
					p.doWork(task)
				}
			}
		}()
	}
}

func (p *ioPoolSimple) doWork(task *taskInfo) {
	start := time.Now()
	p.reportMetric(opDequeue, task.tm) // from enqueue to dequeue
	select {
	// don't exec func
	case <-task.ctx.Done():
		task.done <- task.ctx.Err()
	default:
		err := task.fn()
		task.done <- err
	}
	p.reportMetric(opOnDisk, start) // from dequeue to op done
}

func (p *ioPoolSimple) reportMetric(opStage string, tm time.Time) {
	costMs := time.Since(tm).Milliseconds()
	p.metric.WithLabelValues(p.conf.poolType, opStage).Observe(float64(costMs))
}

func (p *ioPoolSimple) generateTask(args IoPoolTaskArgs) (idx uint64, task *taskInfo) {
	idx = args.BucketId % uint64(len(p.queue))

	task = &taskInfo{
		fn:   args.TaskFn,
		done: make(chan error, 1),
		tm:   args.Tm,
		ctx:  args.Ctx,
	}
	if args.Ctx == nil { // fix nil
		task.ctx = context.Background()
	}

	return idx, task
}

func (p *ioPoolSimple) notLimit() bool {
	// dont limit: queue is empty
	return p.queue == nil
}

func NewIOPool(threadCnt, queueDepth int, tp string, conf IoPoolMetricConf) IoPool {
	conf.poolType = tp
	return newCommonIoPool(threadCnt, queueDepth, conf)
}

// $chanCnt: The number of chan queues
// $threadCnt: The number of read/write/xxx work goroutine, it must be greater than $chanCnt
// $queueDepth: The number of elements in the queue
func newCommonIoPool(threadCnt, queueDepth int, conf IoPoolMetricConf) *ioPoolSimple {
	if threadCnt <= 0 || queueDepth <= 0 {
		// empty io pool, dont limit
		return &ioPoolSimple{closed: make(chan struct{})}
	}

	iopoolMetric := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  "blobstore",
			Subsystem:  "blobnode",
			Name:       "iopool",
			Help:       "iopool latency",
			Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001},
			ConstLabels: map[string]string{
				"cluster_id": fmt.Sprintf("%d", conf.ClusterID),
				"host":       conf.Host,
				"disk_id":    fmt.Sprintf("%d", conf.DiskID),
			},
		},
		[]string{"pool_type", "op_stage"},
	)
	if err := prometheus.Register(iopoolMetric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			iopoolMetric = are.ExistingCollector.(*prometheus.SummaryVec)
		} else {
			panic(err)
		}
	}

	// $chanCnt must less or equal  $threadCnt
	chanCnt := threadCnt
	pool := &ioPoolSimple{
		queue:  make([]chan *taskInfo, chanCnt),
		closed: make(chan struct{}),
		conf:   conf,
		metric: iopoolMetric,
	}
	for i := range pool.queue {
		pool.queue[i] = make(chan *taskInfo, queueDepth)
	}

	pool.backgroundExecute(chanCnt, threadCnt)
	return pool
}
