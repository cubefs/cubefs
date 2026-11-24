// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package largefile

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// LossyAsyncLog is an asynchronous large file log that supports dropping
// lines when the queue is full (opt-in "lossy" mode, see DropWhenFull): a
// bounded queue plus a single consumer goroutine on top of a synchronous
// LogCloser, so that Log returns without waiting for the disk.
//
// The underlying writer keeps its synchronous contract ("returned means
// written"); LossyAsyncLog intentionally relaxes that contract for its callers.
// When the queue is full, the behavior is decided by LossyAsyncLogOptions.DropWhenFull:
//
//	false (default): Log blocks until the queue has room or the log is
//	                 closed (backpressure), nothing is dropped; callers
//	                 may be dragged by a hung disk, use with care;
//	true:            the line is dropped and Log returns immediately,
//	                 the caller never waits for the disk.
//
// If dropping is not acceptable, do not use LossyAsyncLog: rely on the plain
// synchronous large file log (OpenLargeFileLog) instead, whose Log only
// returns after the line is written.
//
// The queue channel is never closed, so Log never sends on a closed
// channel. Close broadcasts via util/closer, the consumer drains the
// queue and closes the underlying writer, and Close waits at most
// LossyAsyncLogOptions.CloseTimeout for that to happen.
type LossyAsyncLog struct {
	queue        chan BufferOwned
	dropWhenFull bool
	closer       closer.Closer
	exited       chan struct{}
	closeTimeout time.Duration
	closeErr     error

	// errCount counts write failures seen by the consumer goroutine
	errCount uint64
	// dropCount counts lines dropped for a full queue or a closed log;
	// producers enqueue concurrently, hence the atomic access
	dropCount uint64

	underlying LogCloser
}

const (
	// LossyAsyncDefaultQueueSize is the queue capacity used when
	// LossyAsyncLogOptions.QueueSize <= 0.
	LossyAsyncDefaultQueueSize = 4096
	// LossyAsyncMaxQueueSize is the upper bound of LossyAsyncLogOptions.QueueSize.
	// Larger values are clamped instead of panicking: this is a side
	// channel and must not prevent the process from starting on a bad
	// config.
	LossyAsyncMaxQueueSize = 1 << 20
	// LossyAsyncDefaultCloseTimeout is used when
	// LossyAsyncLogOptions.CloseTimeout <= 0.
	LossyAsyncDefaultCloseTimeout = 5 * time.Second
)

const (
	// lossyBufInitCap is the initial capacity of pooled line buffers;
	// buffers grow on demand to the workload's line size.
	lossyBufInitCap = 512
	// lossyMaxBufCap is the largest buffer capacity recycled into the
	// pool, so an occasional huge line does not pin memory forever.
	// 1MB: large enough for audit lines carrying a full resp_body, while
	// the worst-case pinned memory stays GOMAXPROCS * 1MB.
	lossyMaxBufCap = 1 << 20
	// lossyAsyncErrLogInterval is the rate limit of write-failure logs:
	// the first failure and every Nth failure are logged.
	lossyAsyncErrLogInterval = 100
	// lossyAsyncDropLogInterval is the rate limit of dropped-line logs:
	// the first drop and every Nth drop are logged.
	lossyAsyncDropLogInterval = 1000
)

// pooledLine is the pooled BufferOwned implementation: Log hands one out,
// Release puts it back with its capacity retained, so the steady state Log
// path runs without allocations. The slice lives in a struct field, so a
// growing append updates the field inside the pooled object and the pool
// always sees the latest header, no *[]byte needed.
type pooledLine struct {
	buf []byte
}

func (p *pooledLine) Buffer() []byte { return p.buf }

// Release returns the line to the pool. Oversized buffers are dropped and
// left to the GC, so an occasional huge line does not pin memory forever.
func (p *pooledLine) Release() {
	if cap(p.buf) <= lossyMaxBufCap {
		pooledLinePool.Put(p)
	}
}

var pooledLinePool = &sync.Pool{New: func() any {
	return &pooledLine{buf: make([]byte, 0, lossyBufInitCap)}
}}

// LossyAsyncLogOptions of NewLossyAsyncLog and OpenLossyAsyncLog.
type LossyAsyncLogOptions struct {
	// QueueSize is the max number of buffered lines.
	// <= 0 means LossyAsyncDefaultQueueSize, > LossyAsyncMaxQueueSize is clamped
	// to LossyAsyncMaxQueueSize.
	QueueSize int
	// DropWhenFull changes the queue-full behavior from blocking the
	// caller (backpressure, nothing is dropped) to dropping the line.
	// Default false: do not drop, compatible with the historical writer.
	DropWhenFull bool
	// CloseTimeout bounds how long Close waits for the consumer to drain
	// the queue and close the underlying writer.
	// <= 0 means LossyAsyncDefaultCloseTimeout.
	CloseTimeout time.Duration
}

// OpenLossyAsyncLog opens a large file log (see OpenLargeFileLog) and wraps it
// with a droppable asynchronous queue, see LossyAsyncLog. The queue brings its
// own async mechanism (bounded queue plus a single consumer goroutine), so the
// underlying file is opened without the async worker pool: on this
// single-writer path a second queue would only add one more hop per line.
func OpenLossyAsyncLog(cfg Config, rotateNew bool, opts LossyAsyncLogOptions) (OwnedLogCloser, error) {
	lf, err := openLargeFileLog(cfg, rotateNew, false)
	if err != nil {
		return nil, err
	}
	return NewLossyAsyncLog(lf, opts), nil
}

// OpenLog opens a line-oriented large file log with an optional async queue,
// driven by opts.QueueSize:
//   - == 0 opens the underlying synchronous log (see OpenLargeFileLog),
//   - != 0 wraps it in a lossy async queue (see OpenLossyAsyncLog, where
//     QueueSize < 0 means LossyAsyncDefaultQueueSize).
func OpenLog(cfg Config, rotateNew bool, opts LossyAsyncLogOptions) (OwnedLogCloser, error) {
	if opts.QueueSize == 0 {
		return OpenLargeFileLog(cfg, rotateNew)
	}
	return OpenLossyAsyncLog(cfg, rotateNew, opts)
}

// NewLossyAsyncLog wraps a synchronous LogCloser with a bounded queue and a
// single consumer goroutine. Lines already queued are written in order.
//
// The underlying LogCloser must not retain the bytes passed to Log after
// Log returns: the buffer is recycled and reused by later Log calls.
func NewLossyAsyncLog(underlying LogCloser, opts LossyAsyncLogOptions) *LossyAsyncLog {
	l := &LossyAsyncLog{
		queue:        make(chan BufferOwned, normalizeLossyAsyncQueueSize(opts.QueueSize)),
		dropWhenFull: opts.DropWhenFull,
		closer:       closer.New(),
		exited:       make(chan struct{}),
		closeTimeout: normalizeLossyAsyncCloseTimeout(opts.CloseTimeout),
		underlying:   underlying,
	}
	go l.run()
	return l
}

// Log enqueues a copy of b and returns. It waits for queue room unless
// DropWhenFull is set, in which case a full queue drops the line; a closed log
// drops it as well.
func (l *LossyAsyncLog) Log(b []byte) error {
	if l.closer.IsClosed() {
		return nil
	}
	// b may be reused by the caller after return: copy into a pooled line
	// and hand the owned object over, Release recycles it after the line is
	// written or dropped.
	p := pooledLinePool.Get().(*pooledLine)
	p.buf = append(p.buf[:0], b...)
	return l.LogOwned(p)
}

// LogOwned enqueues the line without copying it. The caller hands the buffer
// over and must not touch it after the call.
func (l *LossyAsyncLog) LogOwned(line BufferOwned) error {
	if l.closer.IsClosed() {
		line.Release()
		return nil
	}
	l.enqueue(line)
	return nil
}

// enqueue applies the queue-full policy shared by Log and LogOwned, giving the
// buffer back to its owner when the line cannot be queued.
func (l *LossyAsyncLog) enqueue(line BufferOwned) {
	if l.dropWhenFull {
		select {
		case l.queue <- line:
		default:
			line.Release()
			n := atomic.AddUint64(&l.dropCount, 1)
			if n%lossyAsyncDropLogInterval == 1 {
				log.Warnf("lossy async log: dropped lines: %d", n)
			}
		}
		return
	}
	// backpressure: wait for room, or give up when closed
	select {
	case l.queue <- line:
	case <-l.closer.Done():
		line.Release()
	}
}

// Close waits at most LossyAsyncLogOptions.CloseTimeout for queued lines to be
// written; the consumer drains the queue and closes the underlying writer. On
// timeout it does not join a stuck write, so pending release callbacks may
// still run after Close returns.
func (l *LossyAsyncLog) Close() error {
	l.closer.Close()
	t := time.NewTimer(l.closeTimeout)
	defer t.Stop()
	select {
	case <-l.exited:
		return l.closeErr
	case <-t.C:
		return nil
	}
}

// writeLine passes one queued line to the underlying writer and recycles it:
// the underlying Log must not retain the bytes after it returns, so the line
// is released right after the write.
//
// The failure is counted here and logged at a limited rate.
// Atomic is defensive: single writer, so the cost is negligible.
func (l *LossyAsyncLog) writeLine(line BufferOwned) {
	if err := l.underlying.Log(line.Buffer()); err != nil {
		n := atomic.AddUint64(&l.errCount, 1)
		if n%lossyAsyncErrLogInterval == 1 {
			log.Errorf("lossy async log: write failed, total failed lines: %d, err: %v", n, err)
		}
	}
	line.Release()
}

func (l *LossyAsyncLog) run() {
	defer close(l.exited)
	for {
		select {
		case line := <-l.queue:
			l.writeLine(line)
		case <-l.closer.Done():
			// drain the queue before closing: when both channels are
			// ready the select picks randomly, not draining would lose
			// lines that were already queued.
			for {
				select {
				case line := <-l.queue:
					l.writeLine(line)
				default:
					l.closeErr = l.underlying.Close()
					return
				}
			}
		}
	}
}

func normalizeLossyAsyncQueueSize(size int) int {
	if size <= 0 {
		return LossyAsyncDefaultQueueSize
	}
	if size > LossyAsyncMaxQueueSize {
		log.Warnf("largefile: async queue size %d exceeds the max %d, clamped", size, LossyAsyncMaxQueueSize)
		return LossyAsyncMaxQueueSize
	}
	return size
}

func normalizeLossyAsyncCloseTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return LossyAsyncDefaultCloseTimeout
	}
	return d
}
