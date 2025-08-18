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
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/common/trace"
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
}

// QosMgr manages QoS for different IO types
type QosMgr struct {
	qos    map[bnapi.IOType]*queueQos
	conf   Config
	lck    sync.Mutex
	closed closer.Closer
}

// NewQosMgr creates a QoS manager with specified configuration(per disk)
func NewQosMgr(conf Config) (*QosMgr, error) {
	if err := FixQosConfigOnInit(&conf); err != nil {
		return nil, err
	}
	closed := closer.New()

	mgr := &QosMgr{
		conf:   conf,
		closed: closed,
	}
	mgr.qos = map[bnapi.IOType]*queueQos{
		bnapi.ReadIO:       newQueueQos(bnapi.ReadIO, conf, closed, mgr),
		bnapi.WriteIO:      newQueueQos(bnapi.WriteIO, conf, closed, mgr),
		bnapi.DeleteIO:     newQueueQos(bnapi.DeleteIO, conf, closed, mgr),
		bnapi.BackgroundIO: newQueueQos(bnapi.BackgroundIO, conf, closed, mgr),
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
func (mgr *QosMgr) GetConfig() FlowConfig {
	allConf := FlowConfig{
		CommonDiskConfig: mgr.getDiskConfig(),
		Level:            make(LevelConfigMap),
	}

	for ioType, q := range mgr.qos {
		allConf.Level[ioType.String()] = q.getLevelConf()
	}

	return allConf
}

// ResetDiskConfig updates QosMgr config and level common disk limits based on new configuration
func (mgr *QosMgr) ResetDiskConfig(diskConf CommonDiskConfig) {
	mgr.setDiskConfig(diskConf)
}

// ResetLevelConfig updates QosMgr config and level flow limits configuration
func (mgr *QosMgr) ResetLevelConfig(level bnapi.IOType, levelConf LevelFlowConfig) bool {
	levelQos, exist := mgr.qos[level]
	if !exist {
		return false
	}

	levelQos.resetLevelLimit(levelConf)
	return true
}

func (mgr *QosMgr) setDiskConfig(diskConf CommonDiskConfig) {
	mgr.lck.Lock()
	defer mgr.lck.Unlock()
	mgr.conf.CommonDiskConfig.resetDisk(diskConf)
}

func (mgr *QosMgr) getDiskConfig() CommonDiskConfig {
	mgr.lck.Lock()
	defer mgr.lck.Unlock()
	return mgr.conf.CommonDiskConfig
}

type diskConfigGetter interface {
	getDiskConfig() CommonDiskConfig
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
	concurrence int64             // current level qos concurrency

	getter diskConfigGetter
	conf   *perIOQosConfig // QoS configuration per IO type
	closed closer.Closer   // Resource cleanup handler
}

// newQueueQos creates a new QoS controller for specified IO type
func newQueueQos(ioType bnapi.IOType, conf Config, closed closer.Closer, getter diskConfigGetter) *queueQos {
	levelConf := &perIOQosConfig{
		IOType:          ioType,
		LevelFlowConfig: conf.Level[ioType.String()],
	}

	q := &queueQos{
		limitBps:         initLimiter(levelConf.MBPS),
		limitBid:         keycount.New(int(levelConf.BidConcurrency)),
		limitConcurrency: keycount.New(int(levelConf.Concurrency)),
		ioStat:           conf.StatGetter.GetStatMgr(ioType),
		diskStat:         conf.DiskViewer,
		concurrence:      levelConf.Concurrency,
		conf:             levelConf,
		closed:           closed,
		getter:           getter,
	}

	go q.loopUpdateCurrentStat(conf.UpdateIntervalMs)

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

// ReserveN reserves n tokens from bandwidth limiter
func (q *queueQos) ReserveN(t time.Time, n int) *rate.Reservation {
	return q.limitBps.ReserveN(t, n)
}

// UpdateQosBpsLimiter dynamically adjusts bandwidth limits based on disk usage
func (q *queueQos) UpdateQosBpsLimiter(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	target, currentBps := 0, q.diskBps
	lastBps := int64(q.limitBps.Limit())
	diskConf := q.getter.getDiskConfig()
	levelConf := q.getLevelConf()
	diskConfBps := uint64(diskConf.DiskBandwidthMB * humanize.MiByte)
	levelConfBps := levelConf.MBPS * humanize.MiByte
	diskIdleBps := uint64(float64(diskConfBps) * diskConf.DiskIdleFactor)
	levelIdleBps := int64(float64(levelConfBps) * levelConf.IdleFactor)

	switch {
	case currentBps >= diskConfBps && lastBps >= levelConfBps:
		// reduce limit to level*busy when disk is busy
		target = int(float64(levelConfBps) * levelConf.BusyFactor)
	case currentBps < diskIdleBps && lastBps < levelIdleBps:
		// increase limit to level*idle when disk is idle
		target = int(levelIdleBps)
	case currentBps < diskConfBps && lastBps < levelConfBps:
		// reset to original limit when load normalizes
		target = int(levelConfBps)
	default:
		return
	}

	resetLimiter(q.limitBps, target)
	span.Infof("qos dynamical update Bps: (%s) %d -> %d", q.conf.IOType.String(), lastBps, target)
}

// UpdateQosConcurrency dynamically adjusts concurrency limits using EMA
func (q *queueQos) UpdateQosConcurrency(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	// The value of iops is very small, so the result of ema needs to be increased by 1000 multiple to be accurate
	currIops := q.diskIOps / emaMultiple
	diskConf := q.getter.getDiskConfig()
	levelConf := q.getLevelConf()
	target, original, lastCon := int64(0), levelConf.Concurrency, q.concurrence
	diskIopsUsage := float64(currIops) / float64(diskConf.DiskIops)
	idle := int64(float64(original) * levelConf.IdleFactor)

	// Adjust concurrency based on disk utilization
	switch {
	case diskIopsUsage >= 1.0 && lastCon >= original:
		// Reduce concurrency when disk is saturated
		target = int64(float64(original) * levelConf.BusyFactor)
	case diskIopsUsage < diskConf.DiskIdleFactor && lastCon < idle:
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
	span.Infof("qos dynamical update concurrence: (%s) %d -> %d", q.conf.IOType.String(), lastCon, target)
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
		_, ctx := trace.StartSpanFromContext(context.Background(), "Qos")
		select {
		case <-q.closed.Done():
			return
		case <-ticker.C:
			updateFn()
			q.UpdateQosBpsLimiter(ctx)
			q.UpdateQosConcurrency(ctx)
		}
	}
}

func (q *queueQos) getLevelConf() LevelFlowConfig {
	q.conf.lck.Lock()
	defer q.conf.lck.Unlock()
	return q.conf.LevelFlowConfig
}

func (q *queueQos) resetLevelLimit(conf LevelFlowConfig) {
	q.conf.resetLevel(conf)
	if conf.BidConcurrency > 0 {
		q.limitBid.Reset(int(conf.BidConcurrency))
	}
	if conf.Concurrency > 0 {
		q.limitConcurrency.Reset(int(conf.Concurrency))
	}
	if conf.MBPS > 0 {
		resetLimiter(q.limitBps, int(conf.MBPS*humanize.MiByte))
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
