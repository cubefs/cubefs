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

package clustermgr

import (
	"context"
	"strconv"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	diskHeartbeatChangeMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "disk_heartbeat_change",
			Help:      "cluster disk heartbeat change",
		},
		[]string{"region", "cluster"},
	)
	raftStatMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "raft_stat",
			Help:      "cluster raft stat info",
		},
		[]string{"region", "cluster", "is_leader", "item"},
	)
	VolInconsistencyMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "vol_inconsistent",
			Help:      "volume status or vuid inconsistent",
		},
		[]string{"region", "cluster", "is_leader", "item"},
	)
)

func init() {
	prometheus.MustRegister(raftStatMetric)
	prometheus.MustRegister(diskHeartbeatChangeMetric)
	prometheus.MustRegister(VolInconsistencyMetric)
}

func (s *Service) report(ctx context.Context) {
	isLeader := strconv.FormatBool(s.raftNode.IsLeader())
	stat := s.raftNode.Status()
	raftStatMetric.Reset()
	raftStatMetric.WithLabelValues(s.Region, s.ClusterID.ToString(), isLeader, "term").Set(float64(stat.Term))
	raftStatMetric.WithLabelValues(s.Region, s.ClusterID.ToString(), isLeader, "applied_index").Set(float64(stat.Applied))
	raftStatMetric.WithLabelValues(s.Region, s.ClusterID.ToString(), isLeader, "committed_index").Set(float64(stat.Commit))
	raftStatMetric.WithLabelValues(s.Region, s.ClusterID.ToString(), isLeader, "peers").Set(float64(len(stat.Peers)))
}

func (s *Service) reportHeartbeatChange(num float64) {
	diskHeartbeatChangeMetric.Reset()
	diskHeartbeatChangeMetric.WithLabelValues(s.Region, s.ClusterID.ToString()).Set(num)
}

func (s *Service) reportInConsistentVols(vids []proto.Vid) {
	VolInconsistencyMetric.Reset()
	isLeader := strconv.FormatBool(s.raftNode.IsLeader())
	for _, vid := range vids {
		VolInconsistencyMetric.WithLabelValues(s.Region, s.ClusterID.ToString(), isLeader, "vid").Set(float64(vid))
	}
}
