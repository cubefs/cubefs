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
	"errors"
	"io"
	"time"

	"github.com/dustin/go-humanize"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/blobstore/blobnode/base/priority"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

var ErrWrongConfig = errors.New("qos: wrong config item")

// qos controller. controlling two dimensions:
// - iopsLimiter
// - bpsLimiter
type LevelQos interface {
	Writer(ctx context.Context, underlying io.Writer) io.Writer
	WriterAt(ctx context.Context, underlying io.WriterAt) io.WriterAt
	Reader(ctx context.Context, underlying io.Reader) io.Reader
	ReaderAt(ctx context.Context, underlying io.ReaderAt) io.ReaderAt
	ChangeLevelQos(level string, conf Config)
}

type LevelGetter interface {
	GetLevel(priority.Priority) LevelQos
}

// Manage multiple level qos controllers
// Note: controllers that are not configured, nil
type LevelManager struct {
	levels []LevelQos
}

// Implementation of LevelQos interface
// Note: iopsLimiter and bpsLimiter can be left unconfigured.
// levelQos Control adaptive threshold
type levelQos struct {
	bpsLimiter  *rate.Limiter
	iopsLimiter *rate.Limiter
	diskStat    iostat.IOViewer
	threshold   *Threshold
	closer.Closer
}

func (l *levelQos) Writer(ctx context.Context, underlying io.Writer) io.Writer {
	return &rateLimiter{
		ctx:      ctx,
		writer:   underlying,
		levelQos: l,
	}
}

func (l *levelQos) WriterAt(ctx context.Context, underlying io.WriterAt) io.WriterAt {
	return &rateLimiter{
		ctx:      ctx,
		writerAt: underlying,
		levelQos: l,
	}
}

func (l *levelQos) Reader(ctx context.Context, underlying io.Reader) io.Reader {
	return &rateLimiter{
		ctx:      ctx,
		reader:   underlying,
		levelQos: l,
	}
}

func (l *levelQos) ReaderAt(ctx context.Context, underlying io.ReaderAt) io.ReaderAt {
	return &rateLimiter{
		ctx:      ctx,
		readerAt: underlying,
		levelQos: l,
	}
}

func (l *levelQos) updateCapacity() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	updatefn := func() {
		if l.diskStat == nil || (l.iopsLimiter == nil && l.bpsLimiter == nil) {
			return
		}

		rstat := l.diskStat.ReadStat()
		wstat := l.diskStat.WriteStat()

		bps := rstat.Bps + wstat.Bps
		iops := rstat.Iops + wstat.Iops

		if l.iopsLimiter != nil {
			deepIops := int(float64(l.threshold.Iops) * l.threshold.Factor)
			// iops is not in deep limit but disk real load is over threshold, will reset to deep limit threshold
			if l.iopsLimiter.Burst() == 2*int(l.threshold.Iops) && iops > uint64(l.threshold.DiskIOPS) {
				resetLimiter(l.iopsLimiter, deepIops)
			}
			// iops is in deep limit but disk real load is below threshold, will reset to original limit threshold
			if l.iopsLimiter.Burst() != 2*int(l.threshold.Iops) && iops < uint64(l.threshold.DiskIOPS) {
				resetLimiter(l.iopsLimiter, int(l.threshold.Iops))
			}
		}

		if l.bpsLimiter != nil {
			deepBps := int(float64(l.threshold.Bandwidth) * l.threshold.Factor)
			// bps is not in deep limit but disk real load is over threshold, will reset to deep limit threshold
			if l.bpsLimiter.Burst() == 2*int(l.threshold.Bandwidth) && bps > uint64(l.threshold.DiskBandwidth) {
				resetLimiter(l.bpsLimiter, deepBps)
			}
			// bps is in deep limit but disk real load is below threshold, will reset to original limit threshold
			if l.bpsLimiter.Burst() != 2*int(l.threshold.Bandwidth) && bps < uint64(l.threshold.DiskBandwidth) {
				resetLimiter(l.bpsLimiter, int(l.threshold.Bandwidth))
			}
		}
	}

	for {
		select {
		case <-l.Done():
			return
		case <-ticker.C:
			updatefn()
		}
	}
}

func resetLimiter(limiter *rate.Limiter, capacity int) {
	limiter.SetLimit(rate.Limit(capacity))
	limiter.SetBurst(2 * capacity)
}

// get the controller of the specified priority
func (mgr *LevelManager) GetLevel(pri priority.Priority) LevelQos {
	if mgr.levels == nil {
		return nil
	}
	// if qos is not configured for the corresponding level,
	// nil is returned
	return mgr.levels[pri]
}

func NewLevelQos(threshold *Threshold, diskStat iostat.IOViewer) *levelQos {
	qos := &levelQos{
		diskStat:  diskStat,
		threshold: threshold,
		Closer:    closer.New(),
	}
	qos.iopsLimiter = rate.NewLimiter(rate.Limit(threshold.Iops), 2*int(threshold.Iops))
	qos.bpsLimiter = rate.NewLimiter(rate.Limit(threshold.Bandwidth), 2*int(threshold.Bandwidth))
	go qos.updateCapacity()
	return qos
}

func NewLevelQosMgr(conf Config, diskStat iostat.IOViewer) (*LevelManager, error) {
	if err := initConfig(&conf); err != nil {
		return nil, err
	}
	priLevels := priority.GetLevels()
	mgr := &LevelManager{
		levels: make([]LevelQos, len(priLevels)),
	}
	for prio, name := range priLevels {
		para, exist := conf.LevelConfigs[name]
		if !exist {
			continue
		}
		threshold := &Threshold{
			ParaConfig: ParaConfig{
				Iops:      para.Iops,
				Bandwidth: para.Bandwidth,
				Factor:    para.Factor,
			},
			DiskIOPS:      conf.DiskIOPS,
			DiskBandwidth: conf.DiskBandwidthMBPS,
		}
		threshold.DiskBandwidth = threshold.DiskBandwidth * humanize.MiByte
		threshold.Bandwidth = threshold.Bandwidth * humanize.MiByte
		levelController := NewLevelQos(threshold, diskStat)
		mgr.levels[prio] = levelController
	}
	return mgr, nil
}

func (l *levelQos) ChangeLevelQos(level string, conf Config) {
	l.threshold.reset(level, conf)
	resetLimiter(l.iopsLimiter, int(l.threshold.Iops))
	resetLimiter(l.bpsLimiter, int(l.threshold.Bandwidth))
}
