// Copyright 2026 The CubeFS Authors.
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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// metrics for local inspect

var (
	// dataInspectBadVec counts bad shard occurrences found by background inspect (not deduplicated).
	dataInspectBadVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "data_inspect_bad",
			Help:      "Number of bad shard occurrences found by background data inspect (not deduplicated by bid)",
		},
		[]string{"cluster_id", "disk_id"},
	)

	// dataInspectBadShardByDiskVec gauges the current bad-bid count per disk (sum of all chunks'
	// BadBids). Only disks with actual bad bids (>0) export series.
	dataInspectBadShardByDiskVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "data_inspect_bad_shard_by_disk",
			Help:      "Current number of tracked bad bids on the disk, reported when greater than 0",
		},
		[]string{"cluster_id", "disk_id"},
	)

	// dataInspectBadShardByChunkVec gauges unresolved bad bids per chunk (BadBids length).
	// Only chunks with bad bids (>0) export series to keep the metric count small.
	dataInspectBadShardByChunkVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "data_inspect_bad_shard_by_chunk",
			Help:      "Current number of tracked bad bids per chunk, reported when greater than 0",
		},
		[]string{"cluster_id", "disk_id", "vuid"},
	)
)

func init() {
	prometheus.MustRegister(
		dataInspectBadVec,
		dataInspectBadShardByDiskVec,
		dataInspectBadShardByChunkVec,
	)
}

func dataInspectDiskLabelValues(info clustermgr.BlobNodeDiskInfo) []string {
	return []string{info.ClusterID.ToString(), info.DiskID.ToString()}
}

func dataInspectChunkLabelValues(info clustermgr.BlobNodeDiskInfo, vuid proto.Vuid) []string {
	return []string{info.ClusterID.ToString(), info.DiskID.ToString(), vuid.ToString()}
}

// setDiskBadShardMetric publishes the per-disk bad-bid gauge
func setDiskBadShardMetric(info clustermgr.BlobNodeDiskInfo, badCnt int64) {
	labels := dataInspectDiskLabelValues(info)
	if badCnt <= 0 {
		dataInspectBadShardByDiskVec.DeleteLabelValues(labels...)
		return
	}
	dataInspectBadShardByDiskVec.WithLabelValues(labels...).Set(float64(badCnt))
}

// setChunkBadShardMetric publishes the per-chunk bad-bid gauge
func setChunkBadShardMetric(info clustermgr.BlobNodeDiskInfo, vuid proto.Vuid, badCnt int64) {
	labels := dataInspectChunkLabelValues(info, vuid)
	if badCnt <= 0 {
		dataInspectBadShardByChunkVec.DeleteLabelValues(labels...)
		return
	}
	dataInspectBadShardByChunkVec.WithLabelValues(labels...).Set(float64(badCnt))
}

// onChunkReleased drops inspect metrics for a released vuid.
//
// The per-chunk inspect state (Cursor/CycleScanned/BadBids) is intentionally
// NOT deleted here: inspect state is bind to vuid, and vuid can change its
// target chunk instance.
// The metas are small and not many, so keep them until the blobnode next
// restart, doing the garbage-collected task through gcOrphanInspectState.
func (mgr *DataInspectMgr) onChunkReleased(ds core.DiskAPI, vuid proto.Vuid) {
	info := ds.DiskInfo()
	dataInspectBadShardByChunkVec.DeleteLabelValues(dataInspectChunkLabelValues(info, vuid)...)
}
