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

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	prio "github.com/cubefs/cubefs/blobstore/blobnode/base/priority"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

type IOQos struct {
	LevelMgr LevelGetter     // Identify: a level qos controller
	StatMgr  flow.StatGetter // Identify: a io flow
}

type Qos interface {
	ReaderAt(context.Context, bnapi.IOType, io.ReaderAt) io.ReaderAt
	WriterAt(context.Context, bnapi.IOType, io.WriterAt) io.WriterAt
	Writer(context.Context, bnapi.IOType, io.Writer) io.Writer
	Reader(context.Context, bnapi.IOType, io.Reader) io.Reader
	GetLevelMgr() LevelGetter
	Close()
}

type IOTypeRW int

const (
	ReadType IOTypeRW = iota
	WriteType

	writePoolCnt = 2
	reserve      = 2
	percent      = 100
)

type IoQosInternal struct {
	maxWaitCnt   chan struct{}
	maxQueueLen  int32
	queueLen     int32
	discardRatio int32
	rand         *rand.Rand
	closer.Closer
}

func newIoQosLimit(length int) *IoQosInternal {
	qos := &IoQosInternal{
		maxWaitCnt:   make(chan struct{}, reserve*length),
		maxQueueLen:  int32(length),
		discardRatio: 10,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		Closer:       closer.New(),
	}
	go qos.adjustDiscardRatio()
	return qos
}

func (q *IoQosInternal) tryAcquire(ctx context.Context) bool {
	select {
	case q.maxWaitCnt <- struct{}{}:
		ioType := bnapi.GetIoType(ctx)
		if ioType.IsHighLevel() {
			q.incCount()
			return true
		}
		return q.allowLowLevel()

	default:
		return false
	}
}

func (q *IoQosInternal) incCount() {
	atomic.AddInt32(&q.queueLen, 1)
}

func (q *IoQosInternal) decCount() {
	atomic.AddInt32(&q.queueLen, -1)
}

func (q *IoQosInternal) isNotFullLoad() bool {
	return atomic.LoadInt32(&q.queueLen) < q.maxQueueLen
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
	<-q.maxWaitCnt
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

		ratio := atomic.LoadInt32(&q.queueLen) * percent / q.maxQueueLen / reserve
		switch {
		case ratio < 50:
			atomic.StoreInt32(&q.discardRatio, 30)
		case ratio < 75:
			atomic.StoreInt32(&q.discardRatio, 50)
		default:
			atomic.StoreInt32(&q.discardRatio, 80)
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
	LevelMgr   LevelGetter     // Identify: a level qos controller
	StatMgr    flow.StatGetter // Identify: a io flow
	writeLimit [writePoolCnt]*IoQosInternal
	readLimit  *IoQosInternal
}

func (qos *IoQueueQos) getiostat(iot bnapi.IOType) (ios iostat.StatMgrAPI) {
	if qos.StatMgr != nil {
		ios = qos.StatMgr.GetStatMgr(iot)
	}
	return ios
}

func (qos *IoQueueQos) ReaderAt(ctx context.Context, ioType bnapi.IOType, reader io.ReaderAt) io.ReaderAt {
	r := reader

	if ios := qos.getiostat(ioType); ios != nil {
		r = ios.ReaderAt(reader)
	}

	priority := prio.GetPriority(ioType)
	if level := qos.LevelMgr.GetLevel(priority); level != nil {
		r = level.ReaderAt(ctx, r)
	}

	return r
}

func (qos *IoQueueQos) WriterAt(ctx context.Context, ioType bnapi.IOType, writer io.WriterAt) io.WriterAt {
	w := writer

	if ios := qos.getiostat(ioType); ios != nil {
		w = ios.WriterAt(writer)
	}

	priority := prio.GetPriority(ioType)
	if level := qos.LevelMgr.GetLevel(priority); level != nil {
		w = level.WriterAt(ctx, w)
	}

	return w
}

func (qos *IoQueueQos) Writer(ctx context.Context, ioType bnapi.IOType, writer io.Writer) io.Writer {
	w := writer

	if ios := qos.getiostat(ioType); ios != nil {
		w = ios.Writer(writer)
	}

	priority := prio.GetPriority(ioType)
	if level := qos.LevelMgr.GetLevel(priority); level != nil {
		w = level.Writer(ctx, w)
	}

	return w
}

func (qos *IoQueueQos) Reader(ctx context.Context, ioType bnapi.IOType, reader io.Reader) io.Reader {
	r := reader

	if ios := qos.getiostat(ioType); ios != nil {
		r = ios.Reader(reader)
	}

	priority := prio.GetPriority(ioType)
	if level := qos.LevelMgr.GetLevel(priority); level != nil {
		r = level.Reader(ctx, r)
	}

	return r
}

func (qos *IoQueueQos) GetLevelMgr() LevelGetter {
	return qos.LevelMgr
}

func (qos *IoQueueQos) TryAcquire(ctx context.Context, chunkId uint64, rwType IOTypeRW) bool {
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

func (qos *IoQueueQos) Release(chunkId uint64, rwType IOTypeRW) {
	switch rwType {
	case WriteType:
		idx := chunkId % uint64(len(qos.writeLimit))
		qos.writeLimit[idx].release()
	case ReadType:
		qos.readLimit.release()
	}
}

func NewIoQueueQos(conf Config) (Qos, error) {
	levelMgr, err := NewLevelQosMgr(conf, conf.DiskViewer)
	if err != nil {
		return nil, err
	}

	qos := &IoQueueQos{
		LevelMgr:   levelMgr,
		StatMgr:    conf.StatGetter,
		readLimit:  newIoQosLimit(conf.ReadQueueLen),
		writeLimit: newWriteIoQosLimit(conf.WriteQueueLen),
	}

	return qos, nil
}

func (qos *IoQueueQos) Close() {
	qos.readLimit.Close()
	for _, w := range qos.writeLimit {
		w.Close()
	}
}
