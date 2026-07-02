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
	// BadBids). Updated at the end of each round. Reset to zero via /inspect/cleanmetric.
	dataInspectBadShardByDiskVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "data_inspect_bad_shard_by_disk",
			Help:      "Current number of tracked bad bids on the disk as aggregated from all chunks",
		},
		[]string{"cluster_id", "disk_id"},
	)

	// dataInspectBadShardByChunkVec gauges unresolved bad bids per chunk (BadBids length).
	dataInspectBadShardByChunkVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "data_inspect_bad_shard_by_chunk",
			Help:      "Current number of tracked bad bids per chunk",
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
