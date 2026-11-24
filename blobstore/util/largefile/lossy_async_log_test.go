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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package largefile

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/stretchr/testify/require"
)

type lossyTestWriter struct {
	block  chan struct{}
	logged chan []byte
	closed chan struct{}
	err    error
}

func newLossyTestWriter(blocked bool) *lossyTestWriter {
	w := &lossyTestWriter{
		block:  make(chan struct{}),
		logged: make(chan []byte, 1024),
		closed: make(chan struct{}),
	}
	if !blocked {
		close(w.block)
	}
	return w
}

func (w *lossyTestWriter) Log(b []byte) error {
	<-w.block
	// the writer contract allows b to be recycled after Log returns, so
	// keep a copy of it for the test assertions
	w.logged <- append([]byte(nil), b...)
	return w.err
}

func (w *lossyTestWriter) Close() error {
	close(w.closed)
	return nil
}

func returnIn(t *testing.T, d time.Duration, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("not returned in %s", d)
	}
}

func TestLossyAsyncLogDropWhenFull(t *testing.T) {
	under := newLossyTestWriter(true)
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, DropWhenFull: true, CloseTimeout: time.Second})

	// one line in consumer and the rest in queue, then drop when queue is full
	returnIn(t, time.Second, func() {
		for i := 0; i < 100; i++ {
			require.NoError(t, l.Log([]byte{byte(i)}))
		}
	})
	close(under.block)
	require.NoError(t, l.Close())
	<-under.closed
	require.LessOrEqual(t, len(under.logged), 5)
	require.Equal(t, []byte{0}, <-under.logged)
}

func TestLossyAsyncLogError(t *testing.T) {
	under := newLossyTestWriter(false)
	under.err = errors.New("mock error")
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, CloseTimeout: time.Second})

	for i := 0; i < 3; i++ {
		require.NoError(t, l.Log([]byte{byte(i)}))
		require.Equal(t, []byte{byte(i)}, <-under.logged)
	}
	require.NoError(t, l.Close())
}

// TestLossyAsyncLogErrorRateLimited verifies write failures are counted and
// logged at a limited rate: the first failure and every 100th failure go to
// the running log, the rest are counted silently.
func TestLossyAsyncLogErrorRateLimited(t *testing.T) {
	under := newLossyTestWriter(false)
	under.err = errors.New("mock error")
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 256, CloseTimeout: time.Second})

	errBuf := &bytes.Buffer{}
	log.SetOutput(errBuf)
	defer log.SetOutput(os.Stderr)

	const lines = 250
	for i := 0; i < lines; i++ {
		require.NoError(t, l.Log([]byte{byte(i)}))
	}
	require.NoError(t, l.Close())
	<-under.closed

	require.Equal(t, uint64(lines), atomic.LoadUint64(&l.errCount))
	// logged at the 1st failure and every lossyAsyncErrLogInterval-th
	// failure after it: 1st, 101st and 201st for 250 failed lines
	require.Equal(t, 3, strings.Count(errBuf.String(), "[ERROR]"))
	require.Contains(t, errBuf.String(), "total failed lines: 1,")
	require.Contains(t, errBuf.String(), "total failed lines: 101,")
	require.Contains(t, errBuf.String(), "total failed lines: 201,")
}

func TestLossyAsyncLogDropRateLimited(t *testing.T) {
	under := newLossyTestWriter(true)
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 1, DropWhenFull: true, CloseTimeout: time.Minute})

	errBuf := &bytes.Buffer{}
	log.SetOutput(errBuf)
	defer log.SetOutput(os.Stderr)

	// 1 line in the consumer, 1 in the queue, the rest dropped
	const lines = 2501
	for i := 0; i < lines; i++ {
		require.NoError(t, l.Log([]byte{byte(i)}))
	}
	close(under.block)
	require.NoError(t, l.Close())
	<-under.closed

	const dropped = lines - 2
	require.Equal(t, uint64(dropped), atomic.LoadUint64(&l.dropCount))
	// logged at the 1st drop and every lossyAsyncDropLogInterval-th drop
	// after it: 1st, 1001st and 2001st for 2499 dropped lines
	require.Equal(t, 3, strings.Count(errBuf.String(), "[WARN]"))
	require.Contains(t, errBuf.String(), "dropped lines: 1")
	require.Contains(t, errBuf.String(), "dropped lines: 1001")
	require.Contains(t, errBuf.String(), "dropped lines: 2001")
}

func TestLossyAsyncLogCopy(t *testing.T) {
	under := newLossyTestWriter(true)
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, CloseTimeout: time.Second})

	src := []byte("audit-line")
	require.NoError(t, l.Log(src))
	copy(src, "xxxxxxxxxx")
	close(under.block)
	require.Equal(t, []byte("audit-line"), <-under.logged)
	require.NoError(t, l.Close())
}

func TestLossyAsyncLogClose(t *testing.T) {
	{
		// empty queue: close immediately
		under := newLossyTestWriter(false)
		l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, CloseTimeout: time.Minute})
		returnIn(t, time.Second, func() { require.NoError(t, l.Close()) })
		<-under.closed
		// dropped after closed
		require.NoError(t, l.Log([]byte("x")))
		require.Equal(t, 0, len(under.logged))
	}
	{
		// flush queued lines before closing file
		under := newLossyTestWriter(true)
		l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, CloseTimeout: time.Minute})
		for i := 0; i < 4; i++ {
			require.NoError(t, l.Log([]byte{byte(i)}))
		}
		time.AfterFunc(100*time.Millisecond, func() { close(under.block) })
		returnIn(t, time.Second, func() { require.NoError(t, l.Close()) })
		<-under.closed
		require.Equal(t, 4, len(under.logged))
	}
	{
		// return at timeout without waiting for blocked writing
		under := newLossyTestWriter(true)
		l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, CloseTimeout: 200 * time.Millisecond})
		for i := 0; i < 3; i++ {
			require.NoError(t, l.Log([]byte{byte(i)}))
		}
		start := time.Now()
		require.NoError(t, l.Close())
		require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
		select {
		case <-under.closed:
			t.Fatal("closed while writing")
		default:
		}
		// consumer flushes the rest and closes file after disk recovered
		close(under.block)
		<-under.closed
		<-l.exited
		require.Equal(t, 3, len(under.logged))
	}
}

func TestLossyAsyncLogConcurrent(t *testing.T) {
	under := newLossyTestWriter(false)
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 16, CloseTimeout: time.Second})
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-under.logged:
			case <-stop:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				require.NoError(t, l.Log([]byte("line")))
			}
		}()
	}
	time.Sleep(time.Millisecond)
	returnIn(t, 2*time.Second, func() { l.Close() })
	wg.Wait()
	select {
	case <-under.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("file not closed")
	}
	close(stop)
}

func TestLossyAsyncLogBlockWhenFull(t *testing.T) {
	under := newLossyTestWriter(true)
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 2, CloseTimeout: time.Minute})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			require.NoError(t, l.Log([]byte{byte(i)}))
		}
	}()

	// queue(2) plus one line in the consumer, further Log calls block (default: nothing is dropped)
	select {
	case <-done:
		t.Fatal("Log returned while queue full")
	case <-time.After(100 * time.Millisecond):
	}

	// disk recovered: everything written, nothing dropped, in order
	close(under.block)
	<-done
	require.NoError(t, l.Close())
	<-under.closed
	require.Equal(t, 10, len(under.logged))
	for i := 0; i < 10; i++ {
		require.Equal(t, []byte{byte(i)}, <-under.logged)
	}
}

func TestLossyAsyncLogBlockWhenFullReleasedByClose(t *testing.T) {
	under := newLossyTestWriter(true) // consumer stuck in underlying.Log
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 1, CloseTimeout: 200 * time.Millisecond})

	// one line stuck in the consumer, one in the queue
	require.NoError(t, l.Log([]byte{0}))
	require.NoError(t, l.Log([]byte{1}))

	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		require.NoError(t, l.Log([]byte{2})) // blocks: queue full
	}()

	select {
	case <-blocked:
		t.Fatal("Log returned while queue full")
	case <-time.After(100 * time.Millisecond):
	}

	// Close returns at timeout and releases the blocked Log
	require.NoError(t, l.Close())
	<-blocked

	// consumer recovers, drains the queue and closes the file
	close(under.block)
	<-under.closed
	<-l.exited
}

func TestNormalizeLossyAsyncQueueSize(t *testing.T) {
	require.Equal(t, LossyAsyncDefaultQueueSize, normalizeLossyAsyncQueueSize(0))
	require.Equal(t, LossyAsyncDefaultQueueSize, normalizeLossyAsyncQueueSize(-1))
	require.Equal(t, 100, normalizeLossyAsyncQueueSize(100))
	require.Equal(t, LossyAsyncMaxQueueSize, normalizeLossyAsyncQueueSize(LossyAsyncMaxQueueSize))
	require.Equal(t, LossyAsyncMaxQueueSize, normalizeLossyAsyncQueueSize(LossyAsyncMaxQueueSize+1))
}

// TestOpenLog verifies the config-driven dispatch: QueueSize == 0 opens the
// synchronous writer, any other size wraps it in the async queue.
func TestOpenLog(t *testing.T) {
	cfg := Config{Path: t.TempDir(), FileChunkSizeBits: 12, Backup: 2, Suffix: ".log"}

	syncLog, err := OpenLog(cfg, true, LossyAsyncLogOptions{})
	require.NoError(t, err)
	_, async := syncLog.(*LossyAsyncLog)
	require.False(t, async)
	require.NoError(t, syncLog.Close())

	asyncLog, err := OpenLog(cfg, true, LossyAsyncLogOptions{QueueSize: -1})
	require.NoError(t, err)
	_, async = asyncLog.(*LossyAsyncLog)
	require.True(t, async)
	require.NoError(t, asyncLog.Close())
}

// TestLossyAsyncLogPooledBufferNoReuse verifies the buffer recycling: content of
// a queued line must not change when the pooled buffer is recycled and
// reused by later Log calls, in drop mode.
func TestLossyAsyncLogPooledBufferNoReuse(t *testing.T) {
	under := newLossyTestWriter(true) // consumer blocked: lines stay queued
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 8, DropWhenFull: true, CloseTimeout: time.Minute})

	line := []byte("queued-line-0")
	require.NoError(t, l.Log(line))
	// force the same pooled buffer to be reused and rewritten many times
	for i := 0; i < 100; i++ {
		copy(line, []byte("overwritten!!!"))
		require.NoError(t, l.Log(line))
	}

	close(under.block)
	require.NoError(t, l.Close())
	<-under.closed
	require.Equal(t, []byte("queued-line-0"), <-under.logged)
}

// TestLossyAsyncLogOversizedBufferNotPooled verifies that a line larger than
// lossyMaxBufCap is still written correctly and does not get recycled.
func TestLossyAsyncLogOversizedBufferNotPooled(t *testing.T) {
	under := newLossyTestWriter(false)
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, CloseTimeout: time.Second})

	big := bytes.Repeat([]byte("x"), lossyMaxBufCap+1)
	require.NoError(t, l.Log(big))
	require.Equal(t, big, <-under.logged)
	require.NoError(t, l.Close())
}

// TestLossyAsyncLogOwnedNoCopy verifies LogOwned does not copy: a mutation made
// before the consumer runs is visible to the writer.
func TestLossyAsyncLogOwnedNoCopy(t *testing.T) {
	under := newLossyTestWriter(true) // blocked until under.block is closed
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, CloseTimeout: time.Second})

	var released int32
	line := &countedLine{buf: []byte("line-0"), released: &released}
	require.NoError(t, l.LogOwned(line))

	line.buf[0] = 'X' // breaks the contract on purpose: a copier would write "line-0"

	close(under.block)
	select {
	case got := <-under.logged:
		require.Equal(t, []byte("Xine-0"), got)
	case <-time.After(time.Second):
		t.Fatal("line was not written")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&released))
	require.NoError(t, l.Close())
}

// TestLossyAsyncLogOwnedReleaseExactlyOnce verifies release runs once per line
// on the written, dropped and closed paths.
func TestLossyAsyncLogOwnedReleaseExactlyOnce(t *testing.T) {
	under := newLossyTestWriter(false)
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 4, DropWhenFull: true, CloseTimeout: 2 * time.Second})

	const lines = 200
	var released int32
	for i := 0; i < lines; i++ {
		require.NoError(t, l.LogOwned(&countedLine{buf: []byte("x\n"), released: &released}))
	}
	require.NoError(t, l.Close())
	require.Eventually(t, func() bool { return atomic.LoadInt32(&released) == lines },
		2*time.Second, time.Millisecond)
	require.Equal(t, int32(lines), atomic.LoadInt32(&released))

	// a line offered after Close is handed straight back, never written
	require.NoError(t, l.LogOwned(&countedLine{buf: []byte("y\n"), released: &released}))
	require.Equal(t, int32(lines+1), atomic.LoadInt32(&released))
}

func BenchmarkLossyAsyncLog(b *testing.B) {
	under := newLossyTestWriter(false)
	l := NewLossyAsyncLog(under, LossyAsyncLogOptions{QueueSize: 1024, DropWhenFull: true})
	defer l.Close()
	// drain the logged channel in the background so the consumer never stalls
	go func() {
		for range under.logged {
		}
	}()
	line := bytes.Repeat([]byte("a"), 256)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Log(line)
		}
	})
}
