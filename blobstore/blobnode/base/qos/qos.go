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

const (
	limitConcurrencyKey = 0xffffffff
	emaMultiple         = 1000
)

// QosAPI defines the interface for QoS control operations
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

// QosMgr manages QoS for different IO types
type QosMgr struct {
	qos    map[bnapi.IOType]*queueQos
	conf   Config
	closed closer.Closer
}

// NewQosMgr creates a QoS manager with specified configuration(per disk)
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

// Close gracefully shuts down the QoS manager
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
	limitBps         *rate.Limiter           // Bandwidth rate limiter
	limitBid         limit.ResettableLimiter // bid-based concurrency limiter
	limitConcurrency limit.ResettableLimiter // Global concurrency limiter

	ioStat      iostat.StatMgrAPI // IO statistics collector
	diskStat    iostat.IOViewer   // Disk IO statistics viewer
	diskBps     uint64            // Current disk bandwidth usage
	diskIOps    uint64            // Current disk IOps usage
	concurrence int64

	conf   *perIOQosConfig // QoS configuration per IO type
	closed closer.Closer   // Resource cleanup handler
}

// newQueueQos creates a new QoS controller for specified IO type
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
		concurrence:      perIOConf.Concurrency,
		conf:             perIOConf,
		closed:           closed,
	}

	go q.loopUpdateCurrentStat(conf.FlowConf.UpdateIntervalMs)

	return q
}

// ReaderAt wraps io.ReaderAt with QoS control
func (q *queueQos) ReaderAt(ctx context.Context, reader io.ReaderAt) io.ReaderAt {
	r := q.ioStat.ReaderAt(reader)
	return &rateLimiter{ctx: ctx, readerAt: r, ctrl: q}
}

// Reader wraps io.Reader with QoS control
func (q *queueQos) Reader(ctx context.Context, reader io.Reader) io.Reader {
	r := q.ioStat.Reader(reader)
	return &rateLimiter{ctx: ctx, reader: r, ctrl: q}
}

// WriterAt wraps io.WriterAt with QoS control
func (q *queueQos) WriterAt(ctx context.Context, writer io.WriterAt) io.WriterAt {
	w := q.ioStat.WriterAt(writer)
	return &rateLimiter{ctx: ctx, writerAt: w, ctrl: q}
}

// Writer wraps io.Writer with QoS control. with io stat, qos limiter and disk stat
func (q *queueQos) Writer(ctx context.Context, writer io.Writer) io.Writer {
	w := q.ioStat.Writer(writer)
	return &rateLimiter{ctx: ctx, writer: w, ctrl: q}
}

// Acquire attempts to acquire a concurrency slot
func (q *queueQos) Acquire() (err error) {
	if err = q.limitConcurrency.Acquire(limitConcurrencyKey); err != nil {
		return errcode.ErrOverload
	}
	return nil
}

// Release frees a concurrency slot
func (q *queueQos) Release() {
	q.limitConcurrency.Release(limitConcurrencyKey)
}

// AcquireBid attempts to acquire a bid-specific concurrency slot
func (q *queueQos) AcquireBid(bid uint64) (err error) {
	if err = q.limitBid.Acquire(bid); err != nil {
		return errcode.ErrOverload
	}
	return nil
}

// ReleaseBid frees a bid-specific concurrency slot
func (q *queueQos) ReleaseBid(bid uint64) {
	q.limitBid.Release(bid)
}

// ResetDiskLimit updates QoS common disk limits based on new configuration
func (q *queueQos) ResetDiskLimit(conf CommonDiskConfig) {
	q.conf.resetDisk(conf)
}

// ResetLevelLimit : updates Qos level flow  limits configuration
func (q *queueQos) ResetLevelLimit(conf LevelFlowConfig) {
	q.conf.resetLevel(conf)
	if conf.BidConcurrency > 0 {
		q.limitBid.Reset(conf.BidConcurrency)
	}
	if conf.Concurrency > 0 {
		q.limitConcurrency.Reset(int(conf.Concurrency))
	}
	if conf.MBPS > 0 {
		resetLimiter(q.limitBps, int(conf.MBPS*humanize.MiByte))
	}
}

// GetQosConf : get qos per io type config
func (q *queueQos) GetQosConf() *perIOQosConfig {
	return q.conf
}

// ReserveN reserves n tokens from bandwidth limiter
func (q *queueQos) ReserveN(t time.Time, n int) *rate.Reservation {
	return q.limitBps.ReserveN(t, n)
}

// UpdateQosBpsLimiter dynamically adjusts bandwidth limits based on disk usage
func (q *queueQos) UpdateQosBpsLimiter() {
	target, diskBps := 0, q.diskBps
	lastBps := int64(q.limitBps.Limit())
	diskConfBps := uint64(q.conf.DiskBandwidthMB * humanize.MiByte)
	ioConfBps := q.conf.MBPS * humanize.MiByte
	idleBps := int64(float64(ioConfBps) * q.conf.IdleFactor)

	switch {
	case diskBps >= diskConfBps && lastBps >= ioConfBps:
		// reduce limit when disk is busy
		target = int(float64(ioConfBps) * q.conf.Factor)
	case diskBps < uint64(idleBps) && lastBps < idleBps:
		// increase limit when disk is idle
		target = int(idleBps)
	case diskBps < diskConfBps && lastBps < ioConfBps:
		// reset to original limit when load normalizes
		target = int(ioConfBps)
	default:
		return
	}

	resetLimiter(q.limitBps, target)
}

// UpdateQosConcurrency dynamically adjusts concurrency limits using EMA
func (q *queueQos) UpdateQosConcurrency() {
	// The value of iops is very small, so the result of ema needs to be increased by 1000 multiple to be accurate
	currIops := q.diskIOps / emaMultiple
	target, original, lastCon := int64(0), q.conf.Concurrency, q.concurrence
	diskIopsUsage := float64(currIops) / float64(q.conf.DiskIops)
	idle := int64(float64(original) * q.conf.IdleFactor)

	// Adjust concurrency based on disk utilization
	switch {
	case diskIopsUsage >= 1.0 && lastCon >= original:
		// Reduce concurrency when disk is saturated
		target = int64(float64(original) * q.conf.Factor)
	case diskIopsUsage < q.conf.DiskIdleFactor && lastCon < idle:
		// Increase concurrency when disk is idle
		target = idle
	case diskIopsUsage < 1 && lastCon < original:
		// Reset to original limit when load normalizes
		target = original
	default:
		return
	}

	q.concurrence = target
	q.limitConcurrency.Reset(int(target))
}

// loopUpdateCurrentStat periodically updates IO statistics and adjusts QoS limits
func (q *queueQos) loopUpdateCurrentStat(intervalMs int64) {
	if q.diskStat == nil {
		return
	}

	ticker := time.NewTicker(time.Millisecond * time.Duration(intervalMs))
	defer ticker.Stop()

	updateFn := func() {
		rStat, wStat := q.diskStat.ReadStat(), q.diskStat.WriteStat()

		q.diskBps = ema(rStat.Bps+wStat.Bps, q.diskBps)
		q.diskIOps = ema((rStat.Iops+wStat.Iops)*emaMultiple, q.diskIOps)
	}

	for {
		select {
		case <-q.closed.Done():
			return
		case <-ticker.C:
			updateFn()
			q.UpdateQosBpsLimiter()
			q.UpdateQosConcurrency()
		}
	}
}

// initLimiter creates a new rate limiter with specified bandwidth
func initLimiter(bps int64) *rate.Limiter {
	if bps > 0 {
		bps *= humanize.MiByte
		return rate.NewLimiter(rate.Limit(bps), 2*int(bps))
	}
	return nil
}

// resetLimiter updates rate limiter with new capacity
func resetLimiter(limiter *rate.Limiter, capacity int) {
	limiter.SetLimit(rate.Limit(capacity))
	limiter.SetBurst(2 * capacity)
}

// EMA (Exponential Moving Average) calculation:
// EMA = α * Current Value + (1-α) * Previous EMA Value
// - α is tThe smoothing factor α (0 to 1):
// - Higher α: more weight on recent data
// - Lower α: smoother output, less sensitive to fluctuations
func ema(curVal, lastVal uint64) uint64 {
	if lastVal == 0 {
		lastVal = curVal
	}
	return (curVal*2 + lastVal*8) / 10
}
