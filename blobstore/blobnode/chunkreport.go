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

package blobnode

import (
	"context"
	"fmt"
	"time"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const chunkReportBatchOnce = 1 << 16 // report body < 4M

type chunkReportRecord struct {
	used uint64
	at   time.Time
}

// report chunk info to clusterMgr
func (s *Service) loopReportChunkInfoToClusterMgr() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "ChunkReport")
	span.Infof("loop report chunk to cluster mgr")

	ticker := time.NewTicker(time.Duration(s.Conf.ChunkReportIntervalSec) * time.Second)
	for {
		select {
		case <-s.closeCh:
			span.Warnf("loop report chunk done.")
			return
		case <-ticker.C:
			s.reportChunkInfoToClusterMgr()
		}
	}
}

func (s *Service) reportChunkInfoToClusterMgr() {
	if s.chunkReportRecords == nil {
		s.chunkReportRecords = make(map[proto.Vuid]chunkReportRecord)
	}

	rule := chunkReportRule{
		now:     time.Now(),
		timeMax: time.Duration(s.Conf.ChunkReportMaxTimeSec) * time.Second,
		sizePct: s.Conf.ChunkReportSizeRatio,
		highPct: s.Conf.ChunkReportHighUsed,
	}

	traceID := fmt.Sprintf("ChunkReport-%d-%d", s.Conf.NodeID, rule.now.Unix())
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", traceID)

	chunks := s.copyChunkStorages(ctx)

	chosen := make(map[proto.Vuid]core.ChunkAPI)
	cis := make([]cmapi.ChunkInfo, 0, 64)
	for _, cs := range chunks {
		if cs.IsClosed() || !cs.Disk().IsWritable() || !cs.IsDirty() {
			continue
		}
		info := cs.ChunkInfo(ctx)
		if info.Vuid == 0 {
			continue
		}
		var record *chunkReportRecord
		if rec, ok := s.chunkReportRecords[info.Vuid]; ok {
			record = &rec
		}
		if !rule.hit(info.Used, info.Total, record) {
			if record == nil {
				s.chunkReportRecords[info.Vuid] = chunkReportRecord{at: rule.now}
			}
			continue
		}
		cis = append(cis, info)
		chosen[info.Vuid] = cs
		cs.SetDirty(false)
		if len(cis) >= chunkReportBatchOnce {
			break
		}
	}
	if len(cis) == 0 {
		span.Debugf("do not need to report")
		return
	}

	if err := s.ClusterMgrClient.ReportChunk(ctx, &cmapi.ReportChunkArgs{ChunkInfos: cis}); err != nil {
		for _, cs := range chosen {
			cs.SetDirty(true)
		}
		span.Errorf("report chunks info to clusterMgr failed: %v", err)
		return
	}
	for _, info := range cis {
		s.chunkReportRecords[info.Vuid] = chunkReportRecord{used: info.Used, at: rule.now}
	}
	span.Infof("report chunks to clusterMgr success, count:%d", len(cis))
}

type chunkReportRule struct {
	now     time.Time
	timeMax time.Duration
	sizePct int
	highPct int
}

func (rule chunkReportRule) hit(used, total uint64, record *chunkReportRecord) bool {
	if total == 0 {
		return false
	}
	var last uint64
	if record != nil {
		last = record.used
	}
	delta := used - last
	if used < last {
		delta = last - used
	}
	if delta == 0 {
		return false
	}

	// High used: Used reaches highPct and is still growing.
	if rule.highPct > 0 && used > last && used*100 >= uint64(rule.highPct)*total {
		return true
	}
	// Size: Used changed by at least sizePct of chunk total.
	if rule.sizePct > 0 && delta*100 >= uint64(rule.sizePct)*total {
		return true
	}
	// Time: timeMax has passed since the last report and Used has changed.
	return record != nil && rule.timeMax > 0 && rule.now.Sub(record.at) >= rule.timeMax
}
