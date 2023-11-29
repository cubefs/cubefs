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
	Allow() bool
	Release()
	ResetQosLimit(Config)
	Close()
}

type IOTypeRW int

const (
	ReadType IOTypeRW = iota
	WriteType

	percent          = 100
	ratioQueueLow    = 1
	ratioQueueMiddle = 3
	ratioDiscardLow  = 30
	ratioDiscardMid  = 40
	ratioDiscardHigh = 70
)

type IoQosDiscard struct {
	queueDepth   int32 // const, queue depth
	currentCnt   int32 // current IO cnt in per queue, may be $queueDepth < $currentCnt < $maxWaitCnt
	discardRatio int32
	rand         *rand.Rand
	closer.Closer
}

func newIoQosDiscard(depth int) *IoQosDiscard {
	qos := &IoQosDiscard{
		queueDepth:   int32(depth),
		discardRatio: ratioDiscardLow,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		Closer:       closer.New(),
	}
	go qos.adjustDiscardRatio() // dynamic adjust discard ratio
	return qos
}

// $depth: equal $queueDepth of io pool. The number of elements in the queue
// $cnt: The number of chan queues, equal $chanCnt of write io pool
func newWriteIoQosLimit(depth int, cnt int) []*IoQosDiscard {
	w := make([]*IoQosDiscard, cnt)
	for i := range w {
		w[i] = newIoQosDiscard(depth)
	}
	return w
}

func (q *IoQosDiscard) tryAcquire(ctx context.Context) bool {
	ioType := bnapi.GetIoType(ctx)
	if ioType.IsHighLevel() {
		q.incCount()
		return true
	}
	return q.allowLowLevel()
}

func (q *IoQosDiscard) incCount() {
	atomic.AddInt32(&q.currentCnt, 1)
}

func (q *IoQosDiscard) decCount() {
	atomic.AddInt32(&q.currentCnt, -1)
}

func (q *IoQosDiscard) isNotFullLoad() bool {
	return atomic.LoadInt32(&q.currentCnt) < q.queueDepth
}

func (q *IoQosDiscard) allowLowLevel() bool {
	if q.isNotFullLoad() {
		q.incCount()
		return true
	}
	// If your score(rand.Intn) is lower than the threshold, return false and discard this IO
	if q.rand.Intn(percent) < q.getDiscardRatio() {
		return false
	}

	q.incCount()
	return true
}

func (q *IoQosDiscard) release() {
	q.decCount()
}

func (q *IoQosDiscard) getDiscardRatio() int {
	return int(atomic.LoadInt32(&q.discardRatio))
}

// dynamic adjust discard ratio
func (q *IoQosDiscard) adjustDiscardRatio() {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
		case <-q.Closer.Done():
			return
		}

		currentCnt := atomic.LoadInt32(&q.currentCnt)
		if currentCnt < q.queueDepth {
			continue
		}

		// adjust discard ratio, when $queueDepth <= $currentCnt <= $maxWaitCnt
		ratio := (currentCnt - q.queueDepth) / q.queueDepth
		switch {
		case ratio < ratioQueueLow:
			atomic.StoreInt32(&q.discardRatio, ratioDiscardLow)
		case ratio < ratioQueueMiddle:
			atomic.StoreInt32(&q.discardRatio, ratioDiscardMid)
		default:
			atomic.StoreInt32(&q.discardRatio, ratioDiscardHigh)
		}
	}
}

type IoQueueQos struct {
	maxWaitCnt   int32           // const, max total wait IO count, per disk
	ioCnt        int32           // current total IO count, per disk
	bpsLimiters  []*rate.Limiter // limit bandwidth
	readDiscard  *IoQosDiscard   // discard some low level IO
	writeDiscard []*IoQosDiscard
	conf         Config
	closer.Closer
}

func NewIoQueueQos(conf Config) (Qos, error) {
	qos := &IoQueueQos{
		maxWaitCnt:   int32(conf.MaxWaitCount),
		readDiscard:  newIoQosDiscard(conf.ReadQueueDepth),
		writeDiscard: newWriteIoQosLimit(conf.WriteQueueDepth, conf.WriteChanQueCnt),
		conf:         conf,
		Closer:       closer.New(),
	}
	qos.initBpsLimiters()

	return qos, nil
}

func (qos *IoQueueQos) initBpsLimiters() {
	qos.bpsLimiters = make([]*rate.Limiter, bnapi.IOTypeMax)
	qos.conf.BackgroundMBPS *= humanize.MiByte
	qos.conf.NormalMBPS *= humanize.MiByte
	qos.initBpsLimiter(bnapi.NormalIO, qos.conf.NormalMBPS)
	qos.initBpsLimiter(bnapi.BackgroundIO, qos.conf.BackgroundMBPS)
}

func (qos *IoQueueQos) initBpsLimiter(idx bnapi.IOType, bps int64) {
	if bps > 0 {
		qos.bpsLimiters[idx] = rate.NewLimiter(rate.Limit(bps), 2*int(bps))
	}
}

func (qos *IoQueueQos) getIostat(iot bnapi.IOType) (ios iostat.StatMgrAPI) {
	if qos.conf.StatGetter != nil {
		ios = qos.conf.StatGetter.GetStatMgr(iot)
	}
	return ios
}

func (qos *IoQueueQos) getBpsLimiter(iot bnapi.IOType) (l *rate.Limiter) {
	return qos.bpsLimiters[iot]
}

func (qos *IoQueueQos) ReaderAt(ctx context.Context, ioType bnapi.IOType, reader io.ReaderAt) io.ReaderAt {
	r := reader
	if ios := qos.getIostat(ioType); ios != nil {
		r = ios.ReaderAt(reader)
	}

	if lmt := qos.getBpsLimiter(ioType); lmt != nil { // if lmt is null, dont limit io rate
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

	if lmt := qos.getBpsLimiter(ioType); lmt != nil {
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

	if lmt := qos.getBpsLimiter(ioType); lmt != nil {
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

	if lmt := qos.getBpsLimiter(ioType); lmt != nil {
		r = &rateLimiter{
			ctx:        ctx,
			reader:     r,
			bpsLimiter: lmt,
		}
	}
	return r
}

// Allow whether beyond max wait num
func (qos *IoQueueQos) Allow() bool {
	if atomic.AddInt32(&qos.ioCnt, 1) > qos.maxWaitCnt {
		atomic.AddInt32(&qos.ioCnt, -1)
		return false
	}
	return true
}

func (qos *IoQueueQos) Release() {
	atomic.AddInt32(&qos.ioCnt, -1)
}

func (qos *IoQueueQos) TryAcquireIO(ctx context.Context, chunkId uint64, rwType IOTypeRW) bool {
	if !qos.Allow() {
		return false
	}

	ret := false
	switch rwType {
	case WriteType:
		idx := chunkId % uint64(len(qos.writeDiscard))
		ret = qos.writeDiscard[idx].tryAcquire(ctx)
	case ReadType:
		ret = qos.readDiscard.tryAcquire(ctx)
	default:
		// do nothing
	}
	return ret
}

func (qos *IoQueueQos) ReleaseIO(chunkId uint64, rwType IOTypeRW) {
	switch rwType {
	case WriteType:
		idx := chunkId % uint64(len(qos.writeDiscard))
		qos.writeDiscard[idx].release()
	case ReadType:
		qos.readDiscard.release()
	default:
		// do nothing
	}
	qos.Release()
}

func (qos *IoQueueQos) Close() {
	qos.readDiscard.Close()
	for _, w := range qos.writeDiscard {
		w.Close()
	}
	qos.Closer.Close()
}

func (qos *IoQueueQos) ResetQosLimit(conf Config) {
	if conf.NormalMBPS > 0 {
		qos.conf.NormalMBPS = conf.NormalMBPS * humanize.MiByte
		qos.resetLimit(bnapi.NormalIO, qos.conf.NormalMBPS)
	}

	if conf.BackgroundMBPS > 0 {
		qos.conf.BackgroundMBPS = conf.BackgroundMBPS * humanize.MiByte
		qos.resetLimit(bnapi.BackgroundIO, qos.conf.BackgroundMBPS)
	}
}

func (qos *IoQueueQos) resetLimit(idx bnapi.IOType, bps int64) {
	qos.bpsLimiters[idx].SetLimit(rate.Limit(bps))
	qos.bpsLimiters[idx].SetBurst(2 * int(bps))
}

func (qos *IoQueueQos) GetConf() Config {
	return qos.conf
}
