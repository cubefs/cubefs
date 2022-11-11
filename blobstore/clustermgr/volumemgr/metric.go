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

package volumemgr

import (
	"strconv"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	VolStatusMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "vol_status_vol_count",
			Help:      "vol status volume count",
		},
		[]string{"region", "cluster", "status", "is_leader"},
	)
	VolRetainMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "vol_retain_error",
			Help:      "retain volume token error",
		},
		[]string{"region", "cluster", "is_leader"},
	)
	VolAllocMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "vol_alloc_over_disk_load",
			Help:      "alloc volume over disk load",
		},
		[]string{"region", "cluster", "is_leader"},
	)
)

func init() {
	prometheus.MustRegister(VolStatusMetric)
	prometheus.MustRegister(VolRetainMetric)
	prometheus.MustRegister(VolAllocMetric)
}

func (v *VolumeMgr) reportVolStatusInfo(stat clustermgr.VolumeStatInfo, region string, clusterID proto.ClusterID) {
	VolStatusMetric.Reset()
	isLeader := strconv.FormatBool(v.raftServer.IsLeader())

	VolStatusMetric.WithLabelValues(region, clusterID.ToString(), "lock", isLeader).Set(float64(stat.LockVolume))
	VolStatusMetric.WithLabelValues(region, clusterID.ToString(), "idle", isLeader).Set(float64(stat.IdleVolume))
	VolStatusMetric.WithLabelValues(region, clusterID.ToString(), "total", isLeader).Set(float64(stat.TotalVolume))
	VolStatusMetric.WithLabelValues(region, clusterID.ToString(), "active", isLeader).Set(float64(stat.ActiveVolume))
	VolStatusMetric.WithLabelValues(region, clusterID.ToString(), "allocatable", isLeader).Set(float64(stat.AllocatableVolume))
	VolStatusMetric.WithLabelValues(region, clusterID.ToString(), "unlocking", isLeader).Set(float64(stat.UnlockingVolume))
}

func (v *VolumeMgr) reportVolRetainError(num float64) {
	VolRetainMetric.Reset()
	isLeader := strconv.FormatBool(v.raftServer.IsLeader())
	VolRetainMetric.WithLabelValues(v.Region, v.ClusterID.ToString(), isLeader).Set(num)
}

func (v *VolumeMgr) reportVolAllocOverDiskLoad(num float64) {
	VolAllocMetric.Reset()
	isLeader := strconv.FormatBool(v.raftServer.IsLeader())
	VolAllocMetric.WithLabelValues(v.Region, v.ClusterID.ToString(), isLeader).Set(num)
}
