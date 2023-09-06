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

	writePoolCnt     = 2
	reserve          = 2
	percent          = 100
	ratioQueueLow    = 50
	ratioQueueMiddle = 75
	ratioDiscardLow  = 30
	ratioDiscardMid  = 50
	ratioDiscardHigh = 80
)

type IoQosInternal struct {
	maxQueueDepth int32 // max queue depth
	queueLen      int32 // current queue length
	discardRatio  int32
	rand          *rand.Rand
	closer.Closer
}

func newIoQosLimit(length int) *IoQosInternal {
	qos := &IoQosInternal{
		maxQueueDepth: int32(length),
		discardRatio:  ratioDiscardLow,
		rand:          rand.New(rand.NewSource(time.Now().UnixNano())),
		Closer:        closer.New(),
	}
	go qos.adjustDiscardRatio()
	return qos
}

func (q *IoQosInternal) tryAcquire(ctx context.Context) bool {
	ioType := bnapi.GetIoType(ctx)
	if ioType.IsHighLevel() {
		q.incCount()
		return true
	}
	return q.allowLowLevel()
}

func (q *IoQosInternal) incCount() {
	atomic.AddInt32(&q.queueLen, 1)
}

func (q *IoQosInternal) decCount() {
	atomic.AddInt32(&q.queueLen, -1)
}

func (q *IoQosInternal) isNotFullLoad() bool {
	return atomic.LoadInt32(&q.queueLen) < q.maxQueueDepth
}

func (q *IoQosInternal) allowLowLevel() bool {
	if q.isNotFullLoad() {
		q.incCount()
		return true
	}

	if q.rand.Intn(percent) < q.getDiscardRatio() {
		return false
	}
	q.incCount()
	return true
}

func (q *IoQosInternal) release() {
	q.decCount()
}

func (q *IoQosInternal) getDiscardRatio() int {
	return int(atomic.LoadInt32(&q.discardRatio))
}

func (q *IoQosInternal) adjustDiscardRatio() {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
		case <-q.Closer.Done():
			return
		}

		ratio := atomic.LoadInt32(&q.queueLen) * percent / q.maxQueueDepth / reserve
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

func newWriteIoQosLimit(length int) [writePoolCnt]*IoQosInternal {
	w := [writePoolCnt]*IoQosInternal{
		newIoQosLimit(length),
		newIoQosLimit(length),
	}
	return w
}

type IoQueueQos struct {
	bpsLimiters []*rate.Limiter
	maxWaitCnt  chan struct{}
	writeLimit  [writePoolCnt]*IoQosInternal
	readLimit   *IoQosInternal
	conf        Config
	closer.Closer
}

func NewIoQueueQos(conf Config) (Qos, error) {
	maxWaitCnt := make(chan struct{}, conf.MaxWaitCount)
	qos := &IoQueueQos{
		maxWaitCnt: maxWaitCnt,
		readLimit:  newIoQosLimit(conf.ReadQueueLen),
		writeLimit: newWriteIoQosLimit(conf.WriteQueueLen),
		conf:       conf,
		Closer:     closer.New(),
	}
	qos.initBpsLimiters()

	return qos, nil
}

func (qos *IoQueueQos) initBpsLimiters() {
	qos.bpsLimiters = make([]*rate.Limiter, bnapi.IOTypeMax)
	qos.conf.BackgroundMBPS *= humanize.MiByte
	qos.conf.DiskBandwidthMBPS *= humanize.MiByte
	qos.initBpsLimiter(bnapi.NormalIO, qos.conf.DiskBandwidthMBPS)
	qos.initBpsLimiter(bnapi.BackgroundIO, qos.conf.BackgroundMBPS)
}

func (qos *IoQueueQos) initBpsLimiter(idx bnapi.IOType, bps int64) {
	if bps != 0 {
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
	if iot.IsValid() {
		l = qos.bpsLimiters[iot]
	}
	return l
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

// whether beyond max wait num
func (qos *IoQueueQos) Allow() bool {
	select {
	case qos.maxWaitCnt <- struct{}{}:
		return true
	default:
		return false
	}
}

func (qos *IoQueueQos) Release() {
	<-qos.maxWaitCnt
}

func (qos *IoQueueQos) TryAcquireIO(ctx context.Context, chunkId uint64, rwType IOTypeRW) bool {
	if !qos.Allow() {
		return false
	}

	ret := false
	switch rwType {
	case WriteType:
		idx := chunkId % uint64(len(qos.writeLimit))
		ret = qos.writeLimit[idx].tryAcquire(ctx)
	case ReadType:
		ret = qos.readLimit.tryAcquire(ctx)
	}
	return ret
}

func (qos *IoQueueQos) ReleaseIO(chunkId uint64, rwType IOTypeRW) {
	switch rwType {
	case WriteType:
		idx := chunkId % uint64(len(qos.writeLimit))
		qos.writeLimit[idx].release()
	case ReadType:
		qos.readLimit.release()
	}
	qos.Release()
}

func (qos *IoQueueQos) Close() {
	qos.readLimit.Close()
	for _, w := range qos.writeLimit {
		w.Close()
	}
	qos.Closer.Close()
}

func (qos *IoQueueQos) ResetQosLimit(conf Config) {
	qos.conf.DiskBandwidthMBPS = conf.DiskBandwidthMBPS * humanize.MiByte
	qos.conf.BackgroundMBPS = conf.BackgroundMBPS * humanize.MiByte
	qos.resetLimit(bnapi.NormalIO, qos.conf.DiskBandwidthMBPS)
	qos.resetLimit(bnapi.BackgroundIO, qos.conf.BackgroundMBPS)
}

func (qos *IoQueueQos) resetLimit(idx bnapi.IOType, bps int64) {
	qos.bpsLimiters[idx].SetLimit(rate.Limit(bps))
	qos.bpsLimiters[idx].SetBurst(2 * int(bps))
}
