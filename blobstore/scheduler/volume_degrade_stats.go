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

package scheduler

import (
	"context"
	"sort"
	"strings"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

type degradeCodeModeCounter struct {
	codeMode       codemode.CodeMode
	degradeCounter map[int]uint32
}

type degradeBatchCounter struct {
	batchIndex      uint32
	codeModeCounter map[codemode.CodeMode]*degradeCodeModeCounter
}

func newBatchCounter(batchIndex uint32) *degradeBatchCounter {
	return &degradeBatchCounter{
		batchIndex:      batchIndex,
		codeModeCounter: make(map[codemode.CodeMode]*degradeCodeModeCounter),
	}
}

type degradeStatsCounter struct {
	batchSize   uint32
	blobCounter map[uint32]*degradeBatchCounter
	volCounter  map[uint32]*degradeBatchCounter
}

func newDegradeStatsCounter(batchSize uint32) *degradeStatsCounter {
	return &degradeStatsCounter{
		batchSize:   batchSize,
		blobCounter: make(map[uint32]*degradeBatchCounter),
		volCounter:  make(map[uint32]*degradeBatchCounter),
	}
}

func degradeStatsToRecord(stats map[uint32]*degradeBatchCounter) []proto.VolumeDegradeBatch {
	record := make([]proto.VolumeDegradeBatch, 0, len(stats))

	batchIndices := make([]uint32, 0, len(stats))
	for idx := range stats {
		batchIndices = append(batchIndices, idx)
	}
	sort.Slice(batchIndices, func(i, j int) bool {
		return batchIndices[i] < batchIndices[j]
	})

	for _, batchIndex := range batchIndices {
		batchCounter := stats[batchIndex]
		batchProto := proto.VolumeDegradeBatch{
			BatchIndex:    batchIndex,
			CodeModeStats: make([]proto.VolumeDegradeCodeMode, 0, len(batchCounter.codeModeCounter)),
		}

		codeModeNames := make([]string, 0, len(batchCounter.codeModeCounter))
		codeModeMap := make(map[string]codemode.CodeMode)
		for cm := range batchCounter.codeModeCounter {
			name := cm.String()
			codeModeNames = append(codeModeNames, name)
			codeModeMap[name] = cm
		}
		sort.Strings(codeModeNames)

		for _, name := range codeModeNames {
			cm := codeModeMap[name]
			codeModeCounter := batchCounter.codeModeCounter[cm]

			degradeStats := make([]proto.VolumeDegradeLevel, 0, len(codeModeCounter.degradeCounter))
			levels := make([]int, 0, len(codeModeCounter.degradeCounter))
			for level := range codeModeCounter.degradeCounter {
				levels = append(levels, level)
			}
			sort.Ints(levels)

			for _, level := range levels {
				degradeStats = append(degradeStats, proto.VolumeDegradeLevel{
					Level: level,
					Count: codeModeCounter.degradeCounter[level],
				})
			}

			batchProto.CodeModeStats = append(batchProto.CodeModeStats, proto.VolumeDegradeCodeMode{
				Mode:         cm,
				DegradeStats: degradeStats,
			})
		}

		record = append(record, batchProto)
	}

	return record
}

func degradeStatsFromRecord(record []proto.VolumeDegradeBatch) map[uint32]*degradeBatchCounter {
	counter := make(map[uint32]*degradeBatchCounter)

	for _, batchProto := range record {
		batchIndex := batchProto.BatchIndex
		batchCounter, exists := counter[batchIndex]
		if !exists {
			batchCounter = &degradeBatchCounter{
				batchIndex:      batchIndex,
				codeModeCounter: make(map[codemode.CodeMode]*degradeCodeModeCounter),
			}
			counter[batchIndex] = batchCounter
		}

		for _, codeModeProto := range batchProto.CodeModeStats {
			codeMode := codeModeProto.Mode

			codeModeCounter, exists := batchCounter.codeModeCounter[codeMode]
			if !exists {
				codeModeCounter = &degradeCodeModeCounter{
					codeMode:       codeMode,
					degradeCounter: make(map[int]uint32),
				}
				batchCounter.codeModeCounter[codeMode] = codeModeCounter
			}

			for _, levelStat := range codeModeProto.DegradeStats {
				codeModeCounter.degradeCounter[levelStat.Level] += levelStat.Count
			}
		}
	}

	return counter
}

func (b *degradeStatsCounter) fromRecord(record *proto.VolumeDegradeStats) {
	b.batchSize = record.BatchSize
	b.blobCounter = degradeStatsFromRecord(record.BlobStats)
	b.volCounter = degradeStatsFromRecord(record.VolStats)
}

func (b *degradeStatsCounter) toRecord() *proto.VolumeDegradeStats {
	record := &proto.VolumeDegradeStats{
		BatchSize: b.batchSize,
	}
	record.BlobStats = degradeStatsToRecord(b.blobCounter)
	record.VolStats = degradeStatsToRecord(b.volCounter)
	return record
}

type VolumeDegradeStatsCfg struct {
	Enable bool   `json:"enable"`
	Batch  uint32 `json:"batch"`
}

type VolumeDegradeStatsMgr struct {
	clusterID proto.ClusterID

	stats         *degradeStatsCounter
	curBlobStats  *degradeBatchCounter
	curVolStats   *degradeBatchCounter
	curBatchIndex uint32
	firstUpdate   bool

	clusterMgrCli client.ClusterMgrAPI
	cfg           *VolumeDegradeStatsCfg
}

func newVolumeDegradeStatsMgr(
	clusterID proto.ClusterID,
	clusterMgrCli client.ClusterMgrAPI,
	cfg *VolumeDegradeStatsCfg,
) *VolumeDegradeStatsMgr {
	return &VolumeDegradeStatsMgr{
		clusterID:     clusterID,
		stats:         newDegradeStatsCounter(cfg.Batch),
		firstUpdate:   true,
		clusterMgrCli: clusterMgrCli,
		cfg:           cfg,
	}
}

// When an error is encountered, only log it to avoid affecting
// the original volume inspect logic.
func (v *VolumeDegradeStatsMgr) updateDegradeInfo(
	ctx context.Context,
	vid uint32,
	codeMode codemode.CodeMode,
	bidsMissed map[proto.BlobID][]uint8,
) {
	span := trace.SpanFromContextSafe(ctx)

	if !v.cfg.Enable {
		span.Debugf("volume degrade stats don't enabled, just return")
		return
	}

	batchIndex := v.getBatchIndex(vid)

	// Lazy initialization curStats
	if v.firstUpdate {
		v.firstUpdate = false
		v.curBatchIndex = batchIndex
		v.curBlobStats = newBatchCounter(batchIndex)
		v.curVolStats = newBatchCounter(batchIndex)
	}

	// Determine whether to switch to a new batch
	if batchIndex != v.curBatchIndex {
		span.Infof("batch index changed from %d to %d", v.curBatchIndex, batchIndex)

		// 1. update stats
		v.stats.blobCounter[v.curBatchIndex] = v.curBlobStats
		v.stats.volCounter[v.curBatchIndex] = v.curVolStats

		// 2. create a new batch
		v.curBatchIndex = batchIndex
		v.curBlobStats = newBatchCounter(batchIndex)
		v.curVolStats = newBatchCounter(batchIndex)

		// 3. set to kv
		if err := v.setStatsToKV(ctx); err != nil {
			return
		}

		// 4. update prometheus
		v.updateMetric()
	}

	// Update curBatchStats
	maxMissingShard := 0
	getOrCreateCounter := func(dbc *degradeBatchCounter) *degradeCodeModeCounter {
		if counter, exists := dbc.codeModeCounter[codeMode]; exists {
			return counter
		}

		counter := &degradeCodeModeCounter{
			codeMode:       codeMode,
			degradeCounter: make(map[int]uint32),
		}
		dbc.codeModeCounter[codeMode] = counter
		return counter
	}

	blobCounter := getOrCreateCounter(v.curBlobStats)
	for _, shardMissing := range bidsMissed {
		degradeLevel := len(shardMissing)
		if maxMissingShard < degradeLevel {
			maxMissingShard = degradeLevel
		}
		blobCounter.degradeCounter[degradeLevel]++
	}

	volCounter := getOrCreateCounter(v.curVolStats)
	volCounter.degradeCounter[maxMissingShard]++

	span.Debugf("succeed to update degrade info: vid[%d], codemode[%s], bids[%d]",
		vid, codeMode.String(), len(bidsMissed))
}

func (v *VolumeDegradeStatsMgr) getBatchIndex(vid uint32) uint32 {
	return vid / uint32(v.cfg.Batch)
}

func (v *VolumeDegradeStatsMgr) initialize() error {
	span, ctx := trace.StartSpanFromContext(context.Background(), "VolumeDegrade.Initialize")
	defer span.Finish()

	if !v.cfg.Enable {
		span.Infof("volume degrade stats don't enabled, just return")
		return nil
	}

	if err := v.getStatsFromKV(ctx); err != nil {
		if !strings.Contains(err.Error(), errcode.ErrNotFound.Error()) {
			span.Errorf("failed to get volume degrade batch size from kv, err[%+v]", err)
			return err
		}
	}
	span.Infof("succeed to get all exist stats from kv")

	kvBatchSize := v.stats.batchSize
	if kvBatchSize != v.cfg.Batch {
		span.Infof("degrade batch changed from %d to %d, will delete all exist stats", kvBatchSize, v.cfg.Batch)
		if err := v.deleteStatsFromKV(ctx); err != nil {
			span.Errorf("failed to delete all volume degrade batch stats from kv, err[%+v]", err)
			return err
		}
		span.Infof("succeed to delete all exist stats from clustermgr")
		v.stats = newDegradeStatsCounter(v.cfg.Batch)
	}

	span.Infof("succeed to initialized: batchSize[%d], stats[%+v]", v.cfg.Batch, v.stats)
	return nil
}

func (v *VolumeDegradeStatsMgr) setStatsToKV(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	record := v.stats.toRecord()
	if err := v.clusterMgrCli.SetVolumeDegradeStats(ctx, record); err != nil {
		span.Errorf("failed to set volume degrade stats[%+v] to kv, err[%+v]", record, err)
		return err
	}
	span.Debugf("succeed to set volume degrade stats[%+v] to kv", record)
	return nil
}

func (v *VolumeDegradeStatsMgr) getStatsFromKV(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	record, err := v.clusterMgrCli.GetVolumeDegradeStats(ctx)
	if err != nil {
		span.Errorf("failed to get volume degrade stats from kv, err[%+v]", err)
		return err
	}
	span.Debugf("succeed to get volume degrade stats[%+v] from kv", record)
	v.stats.fromRecord(record)
	return nil
}

func (v *VolumeDegradeStatsMgr) deleteStatsFromKV(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	if err := v.clusterMgrCli.DeleteVolumeDegradeStats(ctx); err != nil {
		span.Errorf("failed to delete volume degrade stats from kv, err[%+v]", err)
		return err
	}
	span.Debugf("succeed to delete volume degrade stats from kv")
	return nil
}

func aggregateDegradeStats(batches map[uint32]*degradeBatchCounter) map[codemode.CodeMode]map[int]float64 {
	aggregated := make(map[codemode.CodeMode]map[int]float64)

	for _, batch := range batches {
		if batch == nil {
			continue
		}
		for cm, counter := range batch.codeModeCounter {
			if counter == nil {
				continue
			}
			if _, ok := aggregated[cm]; !ok {
				aggregated[cm] = make(map[int]float64)
			}
			for degradeLevel, count := range counter.degradeCounter {
				aggregated[cm][degradeLevel] += float64(count)
			}
		}
	}

	return aggregated
}

func (v *VolumeDegradeStatsMgr) updateMetric() {
	clusterID := v.clusterID.ToString()
	blobAggregated := aggregateDegradeStats(v.stats.blobCounter)
	volAggregated := aggregateDegradeStats(v.stats.volCounter)
	reportDegradeStats(clusterID, _metricDegradeStatsItemBlob, blobAggregated)
	reportDegradeStats(clusterID, _metricDegradeStatsItemVolume, volAggregated)
}
