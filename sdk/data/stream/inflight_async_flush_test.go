package stream

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"golang.org/x/time/rate"
	"github.com/stretchr/testify/require"
)

var initPacketBufPoolOnce sync.Once

func ensurePacketBufferPool() {
	initPacketBufPoolOnce.Do(func() {
		proto.InitBufferPool(64 << 20)
	})
}

func TestExtentHandlerWaitForFlushBlockedUntilInflightZero(t *testing.T) {
	eh := &ExtentHandler{
		empty: make(chan struct{}, 1),
		stop:  make(chan struct{}),
	}
	atomic.StoreInt32(&eh.inflight, 1)

	done := make(chan error, 1)
	go func() {
		done <- eh.waitForFlush()
	}()

	select {
	case <-done:
		t.Fatalf("waitForFlush should block when inflight > 0")
	case <-time.After(50 * time.Millisecond):
	}

	atomic.StoreInt32(&eh.inflight, 0)
	eh.empty <- struct{}{}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("waitForFlush should return after inflight reaches zero")
	}
}

func TestIssueFlushRequestBlockedByInflightAndResumed(t *testing.T) {
	s := &Streamer{
		client:              &ExtentClient{enableAsyncFlush: true},
		inode:               1001,
		isOpen:              true,
		request:             make(chan interface{}, 1),
		dirtylist:           NewDirtyExtentList(),
		asyncFlushCh:        make(chan *AsyncFlushRequest, 16),
		asyncFlushDone:      make(chan struct{}),
		asyncFlushSemaphore: make(chan struct{}, 4),
	}
	defer close(s.asyncFlushDone)

	eh := &ExtentHandler{
		stream:       s,
		id:           1,
		storeMode:    proto.NormalExtentType,
		empty:        make(chan struct{}, 1),
		stop:         make(chan struct{}),
		doneSender:   make(chan struct{}, 1),
		doneReceiver: make(chan struct{}, 1),
		doneWriteData: make(chan struct{}),
		dp:           &wrapper.DataPartition{},
	}
	atomic.StoreInt32(&eh.inflight, 1)
	s.dirtylist.Put(eh)

	go s.asyncFlushManager()
	go func() {
		req := <-s.request
		s.handleRequest(req)
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.IssueFlushRequest()
	}()

	select {
	case err := <-errCh:
		t.Fatalf("IssueFlushRequest should block while inflight > 0, got err(%v)", err)
	case <-time.After(80 * time.Millisecond):
	}

	atomic.StoreInt32(&eh.inflight, 0)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatalf("IssueFlushRequest should resume once inflight reaches zero")
	}
}

func TestExtentClientReadBlockedByFlushInflightAndResumed(t *testing.T) {
	client := &ExtentClient{
		enableAsyncFlush: true,
		streamers:        make(map[uint64]*Streamer),
		LimitManager:     manager.NewLimitManager(nil),
		readLimiter:      rate.NewLimiter(rate.Inf, 128),
	}

	s := &Streamer{
		client:              client,
		inode:               2002,
		isOpen:              true,
		request:             make(chan interface{}, 1),
		dirtylist:           NewDirtyExtentList(),
		extents:             NewExtentCache(2002),
		asyncFlushCh:        make(chan *AsyncFlushRequest, 16),
		asyncFlushDone:      make(chan struct{}),
		asyncFlushSemaphore: make(chan struct{}, 4),
	}
	// Avoid fetching extents in Read once.Do closure.
	s.extents.gen = 1
	client.streamers[s.inode] = s
	defer close(s.asyncFlushDone)

	eh := &ExtentHandler{
		stream:        s,
		id:            1,
		storeMode:     proto.NormalExtentType,
		empty:         make(chan struct{}, 1),
		stop:          make(chan struct{}),
		doneSender:    make(chan struct{}, 1),
		doneReceiver:  make(chan struct{}, 1),
		doneWriteData: make(chan struct{}),
		dp:            &wrapper.DataPartition{},
	}
	atomic.StoreInt32(&eh.inflight, 1)
	s.dirtylist.Put(eh)

	go s.asyncFlushManager()
	go func() {
		req := <-s.request
		s.handleRequest(req)
	}()

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 4<<10)
		_, err := client.Read(s.inode, buf, 0, len(buf), proto.StorageClass_Replica_HDD, false)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		t.Fatalf("Read should block while flush inflight > 0, got err(%v)", err)
	case <-time.After(80 * time.Millisecond):
	}

	atomic.StoreInt32(&eh.inflight, 0)

	select {
	case err := <-readDone:
		// Extents are empty in this test setup, so nil or EOF is expected.
		if err != nil && err != io.EOF {
			t.Fatalf("unexpected read error after unblocking flush: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Read should resume once flush inflight reaches zero")
	}
}

func TestAsyncFlushSequencingOldestInflightBlocksNewer(t *testing.T) {
	s := &Streamer{
		client:              &ExtentClient{enableAsyncFlush: true},
		inode:               3003,
		isOpen:              true,
		request:             make(chan interface{}, 1),
		dirtylist:           NewDirtyExtentList(),
		asyncFlushCh:        make(chan *AsyncFlushRequest, 16),
		asyncFlushDone:      make(chan struct{}),
		asyncFlushSemaphore: make(chan struct{}, 4),
	}
	defer close(s.asyncFlushDone)

	oldest := &ExtentHandler{
		stream:        s,
		id:            1,
		storeMode:     proto.NormalExtentType,
		empty:         make(chan struct{}, 1),
		stop:          make(chan struct{}),
		doneSender:    make(chan struct{}, 1),
		doneReceiver:  make(chan struct{}, 1),
		doneWriteData: make(chan struct{}),
		dp:            &wrapper.DataPartition{},
	}
	newer := &ExtentHandler{
		stream:        s,
		id:            2,
		storeMode:     proto.NormalExtentType,
		empty:         make(chan struct{}, 1),
		stop:          make(chan struct{}),
		doneSender:    make(chan struct{}, 1),
		doneReceiver:  make(chan struct{}, 1),
		doneWriteData: make(chan struct{}),
		dp:            &wrapper.DataPartition{},
	}
	atomic.StoreInt32(&oldest.inflight, 1) // keep the oldest pending request blocked
	atomic.StoreInt32(&newer.inflight, 0)  // newer one is otherwise ready
	s.dirtylist.Put(oldest)
	s.dirtylist.Put(newer)

	go s.asyncFlushManager()
	go func() {
		req := <-s.request
		s.handleRequest(req)
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.IssueFlushRequest()
	}()

	// Wait until newer request observes at least one requeue due to sequence ordering.
	deadline := time.Now().Add(500 * time.Millisecond)
	requeued := false
	for time.Now().Before(deadline) {
		if req := s.getActiveHandlerFlush(newer.id); req != nil {
			if atomic.LoadUint64(&req.requeueCount) > 0 {
				requeued = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !requeued {
		t.Fatalf("expected newer async flush request to be requeued while oldest is blocked")
	}
	// Guard against future regressions that make requeue spin too aggressively.
	newerReq := s.getActiveHandlerFlush(newer.id)
	if newerReq == nil {
		t.Fatalf("expected newer async flush request to stay pending while oldest is blocked")
	}
	startRequeue := atomic.LoadUint64(&newerReq.requeueCount)
	time.Sleep(20 * time.Millisecond)
	endRequeue := atomic.LoadUint64(&newerReq.requeueCount)
	if endRequeue <= startRequeue {
		t.Fatalf("expected requeueCount to keep growing while oldest is blocked, start=%d end=%d", startRequeue, endRequeue)
	}
	// Keep a broad upper bound to catch accidental tight-loop regressions.
	if endRequeue-startRequeue > 100000 {
		t.Fatalf("requeueCount grows too fast, possible hot-spin regression: start=%d end=%d", startRequeue, endRequeue)
	}

	select {
	case err := <-errCh:
		t.Fatalf("IssueFlushRequest should still block while oldest inflight > 0, got err(%v)", err)
	case <-time.After(80 * time.Millisecond):
	}

	atomic.StoreInt32(&oldest.inflight, 0)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatalf("IssueFlushRequest should resume after oldest inflight reaches zero")
	}
}

func TestExtentClientReadBlockedByStalledReplyAndResumed(t *testing.T) {
	ensurePacketBufferPool()

	client := &ExtentClient{
		enableAsyncFlush: true,
		streamers:        make(map[uint64]*Streamer),
		LimitManager:     manager.NewLimitManager(nil),
		readLimiter:      rate.NewLimiter(rate.Inf, 128),
	}

	s := &Streamer{
		client:              client,
		inode:               4004,
		isOpen:              true,
		request:             make(chan interface{}, 1),
		dirtylist:           NewDirtyExtentList(),
		extents:             NewExtentCache(4004),
		asyncFlushCh:        make(chan *AsyncFlushRequest, 16),
		asyncFlushDone:      make(chan struct{}),
		asyncFlushSemaphore: make(chan struct{}, 4),
	}
	// Avoid fetching extents in Read once.Do closure.
	s.extents.gen = 1
	client.streamers[s.inode] = s
	defer close(s.asyncFlushDone)

	eh := &ExtentHandler{
		stream:        s,
		id:            1,
		storeMode:     proto.NormalExtentType,
		empty:         make(chan struct{}, 1),
		stop:          make(chan struct{}),
		doneSender:    make(chan struct{}, 1),
		doneReceiver:  make(chan struct{}, 1),
		doneWriteData: make(chan struct{}),
		reply:         make(chan *Packet, 1),
		dp:            &wrapper.DataPartition{},
	}
	s.dirtylist.Put(eh)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	serverConnCh := make(chan *net.TCPConn, 1)
	go func() {
		conn, e := ln.Accept()
		if e != nil {
			return
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			serverConnCh <- tcpConn
			return
		}
		_ = conn.Close()
	}()

	clientConn, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	require.NoError(t, err)
	eh.conn = clientConn
	serverConn := <-serverConnCh
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	// Simulate an in-flight packet whose reply read stalls on conn.
	pkt := &Packet{}
	pkt.ReqID = 1
	pkt.Data = make([]byte, 0)
	pkt.errCount = MaxPacketErrorCount
	atomic.StoreInt32(&eh.inflight, 1)
	epoch := eh.handoffIOOwner()
	go eh.receiver(epoch)
	eh.reply <- pkt

	go s.asyncFlushManager()
	go func() {
		req := <-s.request
		s.handleRequest(req)
	}()

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 4<<10)
		_, err := client.Read(s.inode, buf, 0, len(buf), proto.StorageClass_Replica_HDD, false)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		t.Fatalf("Read should block while reply processing is stalled, got err(%v)", err)
	case <-time.After(80 * time.Millisecond):
	}

	// Unblock the stalled read path; processReply defer should dec inflight and wake flush.
	_ = serverConn.Close()

	select {
	case err := <-readDone:
		// We only assert liveness here: once stalled reply is released, read should return
		// (either success/EOF or flush-related error from the injected failure path).
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatalf("Read should resume after stalled reply path is released")
	}
}

func TestExtentHandlerCleanupOwnerHandoffDrainsPendingWrites(t *testing.T) {
	ensurePacketBufferPool()

	eh := &ExtentHandler{
		inode:         5005,
		storeMode:     proto.NormalExtentType,
		writeDataChan: make(chan *WriteDataRequest, 4),
		doneWriteData: make(chan struct{}),
		doneSender:    make(chan struct{}, 1),
		doneReceiver:  make(chan struct{}, 1),
		stop:          make(chan struct{}),
	}

	req := getWriteDataRequest()
	req.data = []byte("abc")
	req.offset = 0
	req.size = 3
	req.blksize = 4
	eh.writeDataWg.Add(1)
	atomic.AddInt64(&eh.pendingWrites, 1)
	eh.writeDataChan <- req

	done := make(chan struct{})
	go func() {
		_ = eh.cleanup()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("cleanup should complete after owner handoff and draining pending writes")
	}

	require.EqualValues(t, 0, atomic.LoadInt64(&eh.pendingWrites))
	waitDone := make(chan struct{})
	go func() {
		eh.writeDataWg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("writeDataWg should be fully released after cleanup drains pending writes")
	}
}

func TestExtentHandlerCleanupDoesNotBlockWithoutIOWorkers(t *testing.T) {
	eh := &ExtentHandler{
		storeMode:      proto.NormalExtentType,
		writeDataChan:  make(chan *WriteDataRequest, 1),
		doneWriteData:  make(chan struct{}),
		doneSender:     make(chan struct{}),
		doneReceiver:   make(chan struct{}),
		stop:           make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		_ = eh.cleanup()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("cleanup should not block when io workers have already exited")
	}

	// cleanup is idempotent via Once and should return immediately.
	require.NoError(t, eh.cleanup())
}

func TestExtentHandlerFlushReturnsErrorForCleanedUnresolvedState(t *testing.T) {
	eh := &ExtentHandler{
		inode:       6006,
		storeMode:   proto.NormalExtentType,
		empty:       make(chan struct{}, 1),
		stop:        make(chan struct{}),
		doneSender:  make(chan struct{}),
		doneReceiver: make(chan struct{}),
	}
	eh.size = 128
	// Simulate cleaned state with unresolved write metadata.
	close(eh.stop)

	err := eh.flush()
	require.Error(t, err)
	require.Contains(t, err.Error(), "cleaned before flush completion")
}

func TestExtentHandlerFlushWaitsForPendingWriteData(t *testing.T) {
	eh := &ExtentHandler{
		inode:         7007,
		storeMode:     proto.NormalExtentType,
		empty:         make(chan struct{}, 1),
		stop:          make(chan struct{}),
		doneSender:    make(chan struct{}),
		doneReceiver:  make(chan struct{}),
		writeDataChan: make(chan *WriteDataRequest, 1),
		doneWriteData: make(chan struct{}),
	}
	eh.writeDataWg.Add(1)
	atomic.AddInt64(&eh.pendingWrites, 1)

	go func() {
		time.Sleep(60 * time.Millisecond)
		atomic.AddInt64(&eh.pendingWrites, -1)
		eh.writeDataWg.Done()
	}()

	start := time.Now()
	err := eh.flush()
	require.NoError(t, err)
	if time.Since(start) < 50*time.Millisecond {
		t.Fatalf("flush should wait for pending writeData drain")
	}
}
