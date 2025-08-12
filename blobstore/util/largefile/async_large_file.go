// Copyright 2025 The CubeFS Authors.
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

package largefile

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	opWrite op = iota
	opReadAt
)

type (
	op uint8

	AsyncConfig struct {
		// WorkerPoolNum is the number of goroutines for async write
		WorkerPoolNum int `json:"worker_pool_num"`

		lf LargeFile
	}
	logEntry struct {
		raw   []byte
		op    op
		off   int64
		retCh chan opRet
	}
	opRet struct {
		n   int
		err error
	}
)

var (
	ErrClosed = errors.New("large file object has been closed")

	retChPool = &sync.Pool{New: func() any {
		return make(chan opRet, 1)
	}}
)

func newAsyncLargeFile(cfg AsyncConfig) *asyncLargeFile {
	defaulter.LessOrEqual(&cfg.WorkerPoolNum, 1)

	a := &asyncLargeFile{
		lf:     cfg.lf,
		closer: closer.New(),
		cfg:    cfg,
	}
	for i := 0; i < cfg.WorkerPoolNum; i++ {
		a.logQueues = append(a.logQueues, newLogQueue())
	}
	a.startWorkerPool(cfg.WorkerPoolNum)

	return a
}

type asyncLargeFile struct {
	roundRobinCount uint32
	logQueues       []*logQueue

	lf     LargeFile
	closer closer.Closer
	cfg    AsyncConfig
}

func (a *asyncLargeFile) Write(b []byte) (int, error) {
	if a.closer.IsClosed() {
		return 0, ErrClosed
	}

	idx := atomic.AddUint32(&a.roundRobinCount, 1) % uint32(len(a.logQueues))
	le := logEntry{raw: b, op: opWrite, retCh: retChPool.Get().(chan opRet)}
	a.logQueues[idx].add(le)

	ret := <-le.retCh
	retChPool.Put(le.retCh)
	return ret.n, ret.err
}

func (a *asyncLargeFile) ReadAt(b []byte, off int64) (n int, err error) {
	if a.closer.IsClosed() {
		return 0, ErrClosed
	}

	idx := atomic.AddUint32(&a.roundRobinCount, 1) % uint32(len(a.logQueues))
	le := logEntry{raw: b, op: opReadAt, off: off, retCh: retChPool.Get().(chan opRet)}
	a.logQueues[idx].add(le)

	ret := <-le.retCh
	retChPool.Put(le.retCh)
	return ret.n, ret.err
}

func (a *asyncLargeFile) FsizeOf() (fsize int64, err error) {
	return a.lf.FsizeOf()
}

func (a *asyncLargeFile) Rotate() error {
	return a.lf.Rotate()
}

func (a *asyncLargeFile) Close() error {
	a.closer.Close()
	return a.lf.Close()
}

func (a *asyncLargeFile) startWorkerPool(num int) {
	for i := 0; i < num; i++ {
		lq := a.logQueues[i]

		go func() {
			for {
				logs, waitCh, ok := lq.drain()
				if !ok {
					select {
					case <-waitCh:
						continue
					case <-a.closer.Done():
						return
					}
				}

				for _, l := range logs {
					switch l.op {
					case opWrite:
						n, err := a.lf.Write(l.raw)
						l.retCh <- opRet{
							n:   n,
							err: err,
						}
					case opReadAt:
						n, err := a.lf.ReadAt(l.raw, l.off)
						l.retCh <- opRet{
							n:   n,
							err: err,
						}
					default:
						panic(fmt.Sprintf("invalid log op: %d", l.op))
					}
				}

				lq.recycle(logs)
			}
		}()
	}
}

func newLogQueue() *logQueue {
	return &logQueue{
		queues: [2]struct {
			written bool
			logs    []logEntry
			waitCh  chan struct{}
		}{
			{logs: make([]logEntry, 0, 256), written: true, waitCh: make(chan struct{}, 1)},
			{logs: make([]logEntry, 0, 256), written: true, waitCh: make(chan struct{}, 1)},
		},
	}
}

type logQueue struct {
	currentQueueIdx int
	queues          [2]struct {
		written bool
		logs    []logEntry
		waitCh  chan struct{}
	}
	sync.Mutex
}

func (q *logQueue) add(lm logEntry) (queueIdx int) {
	q.Lock()
	q.queues[q.currentQueueIdx].logs = append(q.queues[q.currentQueueIdx].logs, lm)
	currentQueueIdx := q.currentQueueIdx
	lastQueueIdx := (currentQueueIdx + 1) % 2

	queueIdx = math.MaxInt
	// last queue has been written, then notify new write operation coming when add logEntry firstly
	if q.queues[lastQueueIdx].written {
		queueIdx = len(q.queues[currentQueueIdx].logs)
	}
	q.Unlock()

	if queueIdx == 1 {
		select {
		case q.queues[currentQueueIdx].waitCh <- struct{}{}:
		default:
		}
	}

	return
}

func (q *logQueue) drain() ([]logEntry, <-chan struct{}, bool) {
	q.Lock()

	if len(q.queues[q.currentQueueIdx].logs) == 0 {
		ch := q.queues[q.currentQueueIdx].waitCh
		q.Unlock()
		return nil, ch, false
	}

	drainQueueIdx := q.currentQueueIdx
	lms := q.queues[q.currentQueueIdx].logs
	q.queues[q.currentQueueIdx].logs = nil
	q.queues[q.currentQueueIdx].written = false
	q.currentQueueIdx = (q.currentQueueIdx + 1) % 2

	q.Unlock()

	// consume waitCh when drain logs success
	select {
	case <-q.queues[drainQueueIdx].waitCh:
	default:
	}

	return lms, nil, true
}

func (q *logQueue) recycle(processed []logEntry) {
	q.Lock()
	lastQueueIdx := (q.currentQueueIdx + 1) % 2
	q.queues[lastQueueIdx].written = true
	q.queues[lastQueueIdx].logs = processed[:0]
	q.Unlock()
}
