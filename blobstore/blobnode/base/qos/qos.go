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
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
)

const limitConcurrencyKey = 0xffffffff

type QosAPI interface {
	ReaderAt(context.Context, io.ReaderAt) io.ReaderAt
	WriterAt(context.Context, io.WriterAt) io.WriterAt
	Writer(context.Context, io.Writer) io.Writer
	Reader(context.Context, io.Reader) io.Reader
	Acquire() error
	Release()
	AcquireBid(uint64) error
	ReleaseBid(uint64)
	ResetDiskLimit(conf CommonDiskConfig)
	ResetLevelLimit(conf LevelFlowConfig)
}

type QosMgr struct {
	qos    map[bnapi.IOType]*queueQos
	conf   Config
	closed closer.Closer
}

// NewQosMgr create a new qos manager for this disk
func NewQosMgr(conf Config) (*QosMgr, error) {
	if err := FixQosConfigOnInit(&conf); err != nil {
		return nil, err
	}
	closed := closer.New()

	mgr := &QosMgr{
		qos: map[bnapi.IOType]*queueQos{
			bnapi.ReadIO:       newQueueQos(bnapi.ReadIO, conf, closed),
			bnapi.WriteIO:      newQueueQos(bnapi.WriteIO, conf, closed),
			bnapi.DeleteIO:     newQueueQos(bnapi.DeleteIO, conf, closed),
			bnapi.BackgroundIO: newQueueQos(bnapi.BackgroundIO, conf, closed),
		},
		conf:   conf,
		closed: closed,
	}

	return mgr, nil
}

// Close this disk close destroy
func (mgr *QosMgr) Close() {
	mgr.closed.Close()
}

// GetQueueQos returns the queue qos for the given io type(which is the io type of the context).
func (mgr *QosMgr) GetQueueQos(ctx context.Context) (QosAPI, bool) {
	ret, ok := mgr.qos[bnapi.GetIoType(ctx)]
	return ret, ok
}

// GetConfig returns the config of the qos manager.
func (mgr *QosMgr) GetConfig() Config {
	return mgr.conf
}

// queueQos limit disk bandwidth rate, iops rate and iops total rate, dynamic adjust rate
type queueQos struct {
	limitBps         *rate.Limiter
	limitBid         limit.Limiter
	limitConcurrency limit.ResettableLimiter

	ioStat      iostat.StatMgrAPI
	diskStat    iostat.IOViewer
	currentBps  uint64
	currentIOps uint64

	conf   *perIOQosConfig
	closed closer.Closer
}

func newQueueQos(ioType bnapi.IOType, conf Config, closed closer.Closer) *queueQos {
	perIOConf := &perIOQosConfig{
		LevelFlowConfig: conf.FlowConf.Level[ioType.String()],
	}
	perIOConf.CommonDiskConfig = conf.FlowConf.CommonDiskConfig

	q := &queueQos{
		limitBps:         initLimiter(perIOConf.MBPS),
		limitBid:         keycount.New(perIOConf.BidConcurrency),
		limitConcurrency: keycount.New(int(perIOConf.Concurrency)),
		ioStat:           conf.StatGetter.GetStatMgr(ioType),
		diskStat:         conf.DiskViewer,
		conf:             perIOConf,
		closed:           closed,
	}

	go q.loopUpdateCurrentStat(conf.FlowConf.UpdateIntervalMs)

	return q
}

// ReaderAt implements the io.ReaderAt interface. with io stat, qos limiter and disk stat
func (q *queueQos) ReaderAt(ctx context.Context, reader io.ReaderAt) io.ReaderAt {
	r := q.ioStat.ReaderAt(reader)
	return &rateLimiter{ctx: ctx, readerAt: r, ctrl: q}
}

// Reader implements the io.Reader interface. with io stat, qos limiter and disk stat
func (q *queueQos) Reader(ctx context.Context, reader io.Reader) io.Reader {
	r := q.ioStat.Reader(reader)
	return &rateLimiter{ctx: ctx, reader: r, ctrl: q}
}

// WriterAt implements the io.WriterAt interface. with io stat, qos limiter and disk stat
func (q *queueQos) WriterAt(ctx context.Context, writer io.WriterAt) io.WriterAt {
	w := q.ioStat.WriterAt(writer)
	return &rateLimiter{ctx: ctx, writerAt: w, ctrl: q}
}

// Writer implements the io.Writer interface. with io stat, qos limiter and disk stat
func (q *queueQos) Writer(ctx context.Context, writer io.Writer) io.Writer {
	w := q.ioStat.Writer(writer)
	return &rateLimiter{ctx: ctx, writer: w, ctrl: q}
}

// Acquire : limit per io type concurrency on the disk
func (q *queueQos) Acquire() (err error) {
	if err = q.limitConcurrency.Acquire(limitConcurrencyKey); err != nil {
		return errcode.ErrOverload
	}
	return nil
}

// Release : release concurrency limiter
func (q *queueQos) Release() {
	q.limitConcurrency.Release(limitConcurrencyKey)
}

// AcquireBid : acquire concurrency limiter key bid, only for delete/get
func (q *queueQos) AcquireBid(bid uint64) (err error) {
	if err = q.limitBid.Acquire(bid); err != nil {
		return errcode.ErrOverload
	}
	return nil
}

// ReleaseBid : release concurrency limiter key bid, only for delete/get
func (q *queueQos) ReleaseBid(bid uint64) {
	q.limitBid.Release(bid)
}

// ResetDiskLimit : reset qos disk limit config
func (q *queueQos) ResetDiskLimit(conf CommonDiskConfig) {
	q.conf.resetDisk(conf)
}

// ResetLevelLimit : reset qos level limit config
func (q *queueQos) ResetLevelLimit(conf LevelFlowConfig) {
	q.conf.resetLevel(conf)
	if conf.Concurrency > 0 {
		q.limitConcurrency.Reset(int(q.conf.Concurrency))
	}
	if conf.MBPS > 0 {
		resetLimiter(q.limitBps, int(q.conf.MBPS*humanize.MiByte))
	}
}

// GetQosConf : get qos per io type config
func (q *queueQos) GetQosConf() *perIOQosConfig {
	return q.conf
}

func (q *queueQos) ReserveN(t time.Time, n int) *rate.Reservation {
	return q.limitBps.ReserveN(t, n)
}

func (q *queueQos) UpdateQosBpsLimiter() {
	if q.diskStat == nil {
		return
	}
}

func (q *queueQos) UpdateQosConcurrency() {
	if q.diskStat == nil {
		return
	}
}

func (q *queueQos) loopUpdateCurrentStat(intervalMs int64) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(intervalMs))
	defer ticker.Stop()

	updateFn := func() {
		if q.diskStat == nil {
			return
		}

		rStat, wStat := q.diskStat.ReadStat(), q.diskStat.WriteStat()
		atomic.StoreUint64(&q.currentBps, rStat.Bps+wStat.Bps)
		atomic.StoreUint64(&q.currentIOps, rStat.Iops+wStat.Iops)
	}

	for {
		select {
		case <-q.closed.Done():
			return
		case <-ticker.C:
			updateFn()
		}
	}
}

func initLimiter(bps int64) *rate.Limiter {
	if bps > 0 {
		bps *= humanize.MiByte
		return rate.NewLimiter(rate.Limit(bps), 2*int(bps))
	}
	return nil
}

func resetLimiter(limiter *rate.Limiter, capacity int) {
	limiter.SetLimit(rate.Limit(capacity))
	limiter.SetBurst(2 * capacity)
}
