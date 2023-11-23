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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type iopoolType int

const (
	iopoolTypeRead iopoolType = iota + 1
	iopoolTypeWrite

	// defaultTimeout = time.Second * 5 // from defaultTimeoutBlobnode
	sampleNum = 1 * 1024 * 1024
)

type IoPoolTaskArgs struct {
	BucketId uint64
	Tm       time.Time
	TaskFn   func()
}

type IoPool interface {
	Submit(IoPoolTaskArgs)
	Close()
}

type IoPoolMetricConf struct {
	ClusterID proto.ClusterID
	IDC       string
	Rack      string
	Host      string
	DiskID    proto.DiskID
	poolType  iopoolType
}

type taskInfo struct {
	fn   func()
	done chan struct{}
	tm   time.Time
}

type ioPoolSimple struct {
	queue  []chan *taskInfo
	wg     sync.WaitGroup
	closed chan struct{}

	conf    IoPoolMetricConf
	metric  *prometheus.GaugeVec
	samples []time.Duration
	count   int64
}

func NewWritePool(threadCnt, queueDepth int, conf IoPoolMetricConf) IoPool {
	// The number of chan queues, $chanCnt is one-to-one with $threadCnt
	conf.poolType = iopoolTypeWrite
	return newCommonIoPool(threadCnt, threadCnt, queueDepth, conf)
}

func NewReadPool(threadCnt, queueDepth int, conf IoPoolMetricConf) IoPool {
	// Multiple $threadCnt share a same chan queue
	conf.poolType = iopoolTypeRead
	return newCommonIoPool(1, threadCnt, queueDepth, conf)
}

// $chanCnt: The number of chan queues
// $threadCnt: The number of read/write work goroutine, it must be greater than $chanCnt
// $queueDepth: The number of elements in the queue
func newCommonIoPool(chanCnt, threadCnt, queueDepth int, conf IoPoolMetricConf) *ioPoolSimple {
	iopoolMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "iopool",
			Help:      "blobnode iopool timeout",
			ConstLabels: map[string]string{
				"cluster_id": fmt.Sprintf("%d", conf.ClusterID),
			},
		},
		[]string{"idc", "rack", "host", "disk_id", "pool_type"},
	)
	if err := prometheus.Register(iopoolMetric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			iopoolMetric = are.ExistingCollector.(*prometheus.GaugeVec)
		} else {
			panic(err)
		}
	}

	pool := &ioPoolSimple{
		queue:   make([]chan *taskInfo, chanCnt),
		closed:  make(chan struct{}),
		conf:    conf,
		metric:  iopoolMetric,
		samples: make([]time.Duration, 0, sampleNum),
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
				//if time.Since(task.tm) > defaultTimeout {
				//	pool.reportTimeoutTask()
				//}
				pool.addToSamples(time.Since(task.tm))
				task.fn()
				task.done <- struct{}{}
			}
			log.Debug("close io pool")
		}()
	}
	go pool.reportLatency()
	return pool
}

func (p *ioPoolSimple) addToSamples(d time.Duration) {
	current := atomic.AddInt64(&p.count, 1)
	if current <= sampleNum {
		p.samples = append(p.samples, d)
		return
	}

	// todo: rotate array

	i := rand.Int63n(current) // Uint64(current)
	if i < sampleNum {
		p.samples[i] = d
	}
}

func (p *ioPoolSimple) Submit(args IoPoolTaskArgs) {
	select {
	case <-p.closed:
		return
	default:
	}

	idx, task := p.generateTask(args)
	p.queue[idx] <- task
	<-task.done
}

func (p *ioPoolSimple) generateTask(args IoPoolTaskArgs) (idx uint64, task *taskInfo) {
	idx = args.BucketId % uint64(len(p.queue))

	task = &taskInfo{
		fn:   args.TaskFn,
		done: make(chan struct{}, 1),
		tm:   args.Tm,
	}

	return idx, task
}

func (p *ioPoolSimple) reportLatency() {
	tk := time.NewTicker(time.Second * 15)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
		case <-p.closed:
		}

		latency := p.getLatency()
		p.metric.WithLabelValues(p.conf.IDC,
			p.conf.Rack,
			p.conf.Host,
			p.conf.DiskID.ToString(),
			p.conf.poolType.String()).Set(float64(latency))
	}
}

func (p *ioPoolSimple) getLatency() int64 {
	if len(p.samples) == 0 {
		return 0
	}

	tmp := make([]time.Duration, len(p.samples))
	copy(tmp, p.samples)

	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i] < tmp[j]
	})

	rank := float64(99/100) * float64(len(tmp)-1)
	lower := math.Floor(rank)
	return int64(tmp[int(lower)])
}

// Close the pool, Submit task maybe concurrent unsafe
func (p *ioPoolSimple) Close() {
	close(p.closed)
	for i := range p.queue {
		close(p.queue[i])
	}

	p.wg.Wait() // wait all io task done
	log.Info("close all io pool, exit")
}

func (tp iopoolType) String() string {
	return strconv.Itoa(int(tp))
}
