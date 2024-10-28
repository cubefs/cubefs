// Copyright 2022 The CubeFS Authors.
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

package qos

import (
	"context"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

type Qos interface {
	ReaderAt(context.Context, bnapi.IOType, io.ReaderAt) io.ReaderAt
	WriterAt(context.Context, bnapi.IOType, io.WriterAt) io.WriterAt
	Writer(context.Context, bnapi.IOType, io.Writer) io.Writer
	Reader(context.Context, bnapi.IOType, io.Reader) io.Reader
	Allow(rwType IOTypeRW) bool
	Release(rwType IOTypeRW)
	ResetQosLimit(Config)
	GetConfig() Config
	Close()
}

type (
	IOTypeRW  int
	LimitType int
)

const (
	IOTypeRead = IOTypeRW(iota)
	IOTypeWrite
	IOTypeMax

	percent = 100
)

const (
	LimitTypeWrite = LimitType(iota) // bnapi.NormalIO
	LimitTypeBack                    // bnapi.BackgroundIO
	LimitTypeRead
	LimitTypeMax
)

type IoQueueQos struct {
	maxWaitCnt   []int32         // const, max total wait IO count, per disk. different between read and write
	ioCnt        []int32         // current total IO count, per disk.
	bpsLimiters  []*rate.Limiter // limit bandwidth
	readDiscard  *IoQosDiscard
	writeDiscard []*IoQosDiscard // discard some low level IO
	conf         Config
	closer.Closer
}

func NewIoQueueQos(conf Config) (Qos, error) {
	qos := &IoQueueQos{
		ioCnt:        make([]int32, IOTypeMax), // idx 0:read, 1:write
		maxWaitCnt:   []int32{conf.ReadQueueDepth, conf.WriteQueueDepth * conf.WriteChanQueCnt},
		readDiscard:  newIoQosDiscard(conf.ReadQueueDepth, conf.ReadDiscard),
		writeDiscard: newWriteIoQosLimit(conf.WriteQueueDepth, conf.WriteChanQueCnt, conf.WriteDiscard),
		conf:         conf,
		Closer:       closer.New(),
	}
	qos.initBpsLimiters()

	return qos, nil
}

func (qos *IoQueueQos) ReaderAt(ctx context.Context, ioType bnapi.IOType, reader io.ReaderAt) io.ReaderAt {
	r := reader
	if ios := qos.getIostat(ioType); ios != nil {
		r = ios.ReaderAt(reader)
	}

	// if lmt is null, dont limit io rate
	idx := LimitTypeRead
	if ioType == bnapi.BackgroundIO {
		idx = LimitTypeBack
	}
	if lmt := qos.getBpsLimiter(idx); lmt != nil {
		r = &rateLimiter{
			ctx:        ctx,
			readerAt:   r,
			bpsLimiter: lmt,
		}
	}
	return r
}

func (qos *IoQueueQos) WriterAt(ctx context.Context, ioType bnapi.IOType, writer io.WriterAt) io.WriterAt {
	w := writer
	if ios := qos.getIostat(ioType); ios != nil {
		w = ios.WriterAt(writer)
	}

	if lmt := qos.getBpsLimiter(LimitType(ioType)); lmt != nil {
		w = &rateLimiter{
			ctx:        ctx,
			writerAt:   w,
			bpsLimiter: lmt,
		}
	}
	return w
}

func (qos *IoQueueQos) Writer(ctx context.Context, ioType bnapi.IOType, writer io.Writer) io.Writer {
	w := writer
	if ios := qos.getIostat(ioType); ios != nil {
		w = ios.Writer(writer)
	}

	if lmt := qos.getBpsLimiter(LimitType(ioType)); lmt != nil {
		w = &rateLimiter{
			ctx:        ctx,
			writer:     w,
			bpsLimiter: lmt,
		}
	}
	return w
}

func (qos *IoQueueQos) Reader(ctx context.Context, ioType bnapi.IOType, reader io.Reader) io.Reader {
	r := reader
	if ios := qos.getIostat(ioType); ios != nil {
		r = ios.Reader(reader)
	}

	idx := LimitTypeRead
	if ioType == bnapi.BackgroundIO {
		idx = LimitTypeBack
	}
	if lmt := qos.getBpsLimiter(idx); lmt != nil {
		r = &rateLimiter{
			ctx:        ctx,
			reader:     r,
			bpsLimiter: lmt,
		}
	}
	return r
}

// Allow whether beyond max wait num
func (qos *IoQueueQos) Allow(rwType IOTypeRW) bool {
	if atomic.AddInt32(&qos.ioCnt[rwType], 1) > qos.maxWaitCnt[rwType] {
		atomic.AddInt32(&qos.ioCnt[rwType], -1)
		return false
	}
	return true
}

func (qos *IoQueueQos) Release(rwType IOTypeRW) {
	atomic.AddInt32(&qos.ioCnt[rwType], -1)
}

// TryAcquireIO is just simply counts, and determines if you can operate IO. return true means you can operate IO.
// 1.If the total IO is less than the queue depth, high-priority IO can be added to the queue; otherwise, they are discarded all
// 2.If the low-priority IO less than half of the queue depth, it can be added to the queue; otherwise, they are discarded some
func (qos *IoQueueQos) TryAcquireIO(ctx context.Context, chunkId uint64, rwType IOTypeRW) bool {
	// judge whether the number exceeds the maximum(queue depth)
	if !qos.Allow(rwType) {
		return false
	}

	ret := false
	switch rwType {
	case IOTypeWrite:
		idx := chunkId % uint64(len(qos.writeDiscard))
		ret = qos.writeDiscard[idx].tryAcquire(ctx)
	case IOTypeRead:
		ret = qos.readDiscard.tryAcquire(ctx)
	default:
		// do nothing
	}

	if !ret {
		qos.Release(rwType)
	}
	return ret
}

func (qos *IoQueueQos) ReleaseIO(chunkId uint64, rwType IOTypeRW) {
	switch rwType {
	case IOTypeWrite:
		idx := chunkId % uint64(len(qos.writeDiscard))
		qos.writeDiscard[idx].release()
	case IOTypeRead:
		qos.readDiscard.release()
	default:
		// do nothing
	}
	qos.Release(rwType)
}

func (qos *IoQueueQos) Close() {
	qos.readDiscard.Close()
	for _, w := range qos.writeDiscard {
		w.Close()
	}
	qos.Closer.Close()
}

func (qos *IoQueueQos) ResetQosLimit(conf Config) {
	qos.resetConfLimiter(qos.bpsLimiters[LimitTypeRead], conf.ReadMBPS, &qos.conf.ReadMBPS)
	qos.resetConfLimiter(qos.bpsLimiters[LimitTypeWrite], conf.WriteMBPS, &qos.conf.WriteMBPS)
	qos.resetConfLimiter(qos.bpsLimiters[LimitTypeBack], conf.BackgroundMBPS, &qos.conf.BackgroundMBPS)
	qos.resetReadDiscard(conf.ReadDiscard, &qos.conf.ReadDiscard)
	qos.resetWriteDiscard(conf.WriteDiscard, &qos.conf.WriteDiscard)
}

func (qos *IoQueueQos) GetConfig() Config {
	return qos.conf
}

func (qos *IoQueueQos) GetBpsLimiter() []*rate.Limiter {
	return qos.bpsLimiters
}

func (qos *IoQueueQos) resetConfLimiter(limiter *rate.Limiter, expect int64, confVal *int64) {
	if expect > 0 {
		qosVal := expect * humanize.MiByte
		resetLimiter(limiter, int(qosVal))
		atomic.StoreInt64(confVal, expect)
	}
}

func (qos *IoQueueQos) resetReadDiscard(expect int32, confVal *int32) {
	if expect > 0 {
		atomic.StoreInt32(&qos.readDiscard.discardRatio, expect)
		atomic.StoreInt32(confVal, expect)
	}
}

func (qos *IoQueueQos) resetWriteDiscard(expect int32, confVal *int32) {
	if expect > 0 {
		for _, d := range qos.writeDiscard {
			atomic.StoreInt32(&d.discardRatio, expect)
			atomic.StoreInt32(confVal, expect)
		}
	}
}

func (qos *IoQueueQos) initBpsLimiters() {
	qos.bpsLimiters = make([]*rate.Limiter, LimitTypeMax)
	qos.bpsLimiters[LimitTypeRead] = qos.initBpsLimiter(qos.conf.ReadMBPS * humanize.MiByte)
	qos.bpsLimiters[LimitTypeWrite] = qos.initBpsLimiter(qos.conf.WriteMBPS * humanize.MiByte)
	qos.bpsLimiters[LimitTypeBack] = qos.initBpsLimiter(qos.conf.BackgroundMBPS * humanize.MiByte)
}

func (qos *IoQueueQos) initBpsLimiter(bps int64) *rate.Limiter {
	if bps > 0 {
		return rate.NewLimiter(rate.Limit(bps), 2*int(bps))
	}
	return nil
}

func (qos *IoQueueQos) getIostat(iot bnapi.IOType) (ios iostat.StatMgrAPI) {
	if qos.conf.StatGetter != nil {
		ios = qos.conf.StatGetter.GetStatMgr(iot)
	}
	return ios
}

func (qos *IoQueueQos) getBpsLimiter(tp LimitType) (l *rate.Limiter) {
	return qos.bpsLimiters[tp]
}

func resetLimiter(limiter *rate.Limiter, capacity int) {
	limiter.SetLimit(rate.Limit(capacity))
	limiter.SetBurst(2 * capacity)
}

type IoQosDiscard struct {
	queueDepth   int32 // const, queue depth
	currentCnt   int32 // current IO cnt in per queue, may be $currentCnt: [0, $queueDepth]
	discardRatio int32
	rand         *rand.Rand
	closer.Closer
}

func newIoQosDiscard(depth int32, discard int32) *IoQosDiscard {
	qos := &IoQosDiscard{
		queueDepth:   depth,
		discardRatio: discard,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		Closer:       closer.New(),
	}
	return qos
}

// $depth: equal $queueDepth of io pool. The number of elements in the queue
// $cnt: The number of chan queues, equal $chanCnt of write io pool
func newWriteIoQosLimit(depth int32, cnt int32, discard int32) []*IoQosDiscard {
	w := make([]*IoQosDiscard, cnt)
	for i := range w {
		w[i] = newIoQosDiscard(depth, discard)
	}
	return w
}

func (q *IoQosDiscard) tryAcquire(ctx context.Context) bool {
	ioType := bnapi.GetIoType(ctx)
	// high-level: more than full capacity,discard; else okay
	if ioType.IsHighLevel() {
		if !q.isMoreThanFull() {
			q.incCount()
			return true
		}
		return false
	}
	// more than full capacity, discard; not more than half, okay; otherwise, discard half IO
	return q.allowLowLevel()
}

func (q *IoQosDiscard) incCount() {
	atomic.AddInt32(&q.currentCnt, 1)
}

func (q *IoQosDiscard) decCount() {
	atomic.AddInt32(&q.currentCnt, -1)
}

func (q *IoQosDiscard) isMoreThanFull() bool {
	return atomic.LoadInt32(&q.currentCnt) >= q.queueDepth
}

func (q *IoQosDiscard) isMoreThanHalf() bool {
	return atomic.LoadInt32(&q.currentCnt) >= (q.queueDepth / 2)
}

func (q *IoQosDiscard) allowLowLevel() bool {
	// more than full capacity, discard
	if q.isMoreThanFull() {
		return false
	}

	// not exceeding half capacity, acceptable
	if !q.isMoreThanHalf() {
		q.incCount()
		return true
	}

	// If your score(rand.Intn) is lower than the threshold, return false and discard this IO
	if int32(q.rand.Intn(percent)) < atomic.LoadInt32(&q.discardRatio) {
		return false
	}

	q.incCount()
	return true
}

func (q *IoQosDiscard) release() {
	q.decCount()
}
