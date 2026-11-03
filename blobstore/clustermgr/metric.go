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
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/kvstat"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
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
	volInconsistencyMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "vol_inconsistent",
			Help:      "volume status or vuid inconsistent",
		},
		[]string{"region", "cluster", "is_leader", "item"},
	)
	shardNodeDiskHeartbeatChangeMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "shardnode_disk_heartbeat_change",
			Help:      "shardnode disk heartbeat change",
		},
		[]string{"region", "cluster"},
	)
	brokenChunkNumInVolumeMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "broken_chunk_num_in_volume",
			Help:      "broken chunk num in volume",
		},
		[]string{"region", "cluster", "vid"},
	)
	brokenShardUnitNumInShardMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "broken_shard_unit_num_in_shard",
			Help:      "broken shard unit num in shard",
		},
		[]string{"region", "cluster", "shard_id"},
	)

	dashboardMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "dashboard",
			Help:      "cluster dashboard counts; value label carries detail",
		},
		[]string{"region", "cluster", "item", "value"},
	)

	metaStatMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "meta_stat",
			Help:      "clustermgr meta engine stat",
		},
		[]string{"region", "cluster", "db_name", "item"},
	)
	metaCFStatMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "meta_cf_stat",
			Help:      "clustermgr meta column family stat",
		},
		[]string{"region", "cluster", "db_name", "cf", "item"},
	)
)

func init() {
	prometheus.MustRegister(raftStatMetric)
	prometheus.MustRegister(diskHeartbeatChangeMetric)
	prometheus.MustRegister(volInconsistencyMetric)
	prometheus.MustRegister(brokenChunkNumInVolumeMetric)
	prometheus.MustRegister(brokenShardUnitNumInShardMetric)
	prometheus.MustRegister(dashboardMetric)
	prometheus.MustRegister(metaStatMetric)
	prometheus.MustRegister(metaCFStatMetric)
}

func (s *Service) report(_ context.Context) {
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
	volInconsistencyMetric.Reset()
	isLeader := strconv.FormatBool(s.raftNode.IsLeader())
	for _, vid := range vids {
		volInconsistencyMetric.WithLabelValues(s.Region, s.ClusterID.ToString(), isLeader, "vid").Set(float64(vid))
	}
}

func (s *Service) reportShardNodeHeartbeatChange(num float64) {
	shardNodeDiskHeartbeatChangeMetric.Reset()
	shardNodeDiskHeartbeatChangeMetric.WithLabelValues(s.Region, s.ClusterID.ToString()).Set(num)
}

func (s *Service) reportBrokenChunkNumInVolume(vids map[proto.Vid]int) {
	brokenChunkNumInVolumeMetric.Reset()
	for vid, num := range vids {
		brokenChunkNumInVolumeMetric.WithLabelValues(s.Region, s.ClusterID.ToString(), vid.ToString()).Set(float64(num))
	}
}

func (s *Service) reportDashboard(d clustermgr.ClusterDashboard) {
	region := s.Region
	cluster := s.ClusterID.ToString()

	set := func(item string, val int) {
		dashboardMetric.WithLabelValues(region, cluster, item, "").Set(float64(val))
	}
	setValue := func(item, value string, val int) {
		dashboardMetric.WithLabelValues(region, cluster, item, value).Set(float64(val))
	}

	dashboardMetric.Reset()

	set("disk_broken", d.Disk.SumIDC(proto.DiskStatusBroken.String()))
	set("disk_repairing", d.Disk.SumIDC(proto.DiskStatusRepairing.String()))

	set("service_offline", len(d.Service.OfflineNodes))
	now := time.Now().Unix()
	for _, n := range d.Service.OfflineNodes {
		elapsed := now - n.ExpireAt
		offlineHours := int((elapsed + int64(time.Hour/time.Second) - 1) / int64(time.Hour/time.Second))
		setValue("service_offline", n.Name+"-"+n.Idc+"-"+n.Host, offlineHours)
	}
	set("service_expired_disk", d.Service.ExpiredDisks)
	for host, diskIDs := range d.Service.ExpiredByNode {
		if len(diskIDs) > 1 {
			setValue("service_expired_disk", host, len(diskIDs))
		} else if len(diskIDs) == 1 {
			setValue("service_expired_disk", host+"-"+diskIDs[0].ToString(), 1)
		}
	}

	set("volume_active", d.Volume.Status.ActiveTotal)
	set("volume_idle", d.Volume.Status.IdleTotal)

	// global TopDiskLoad entry (CodeMode == "") holds cross-codemode disk load ranking.
	for _, tl := range d.Volume.TopDiskLoad {
		if tl.CodeMode == "" {
			if len(tl.TopN) > 0 {
				set("volume_diskload_top1", tl.TopN[0].Load)
			}
			if len(tl.TopN) >= 10 {
				set("volume_diskload_top10", tl.TopN[9].Load)
			}
			break
		}
	}

	set("safety_degraded", d.VolumeSafety.DegradedVolumes)
	set("safety_at_risk", d.VolumeSafety.AtRiskVolumes)
	set("safety_data_loss", d.VolumeSafety.DataLossVolumes)
}

func (s *Service) reportBrokenUnitNumInShard(shardIDs map[proto.ShardID]int) {
	brokenShardUnitNumInShardMetric.Reset()
	for shardID, num := range shardIDs {
		brokenShardUnitNumInShardMetric.WithLabelValues(s.Region, s.ClusterID.ToString(), shardID.ToString()).Set(float64(num))
	}
}

func (s *Service) reportMetaStat(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	metaStatMetric.Reset()
	metaCFStatMetric.Reset()

	dbNames := make([]string, 0, len(s.dbs)+1)
	for name := range s.dbs {
		dbNames = append(dbNames, name)
	}
	dbNames = append(dbNames, "raft")
	for _, dbName := range dbNames {
		db, err := s.resolveDBForMetric(dbName)
		if err != nil {
			span.Debugf("resolve db %s failed: %v", dbName, err)
			metaStatMetric.With(prometheus.Labels{
				"region":  s.Region,
				"cluster": s.ClusterID.ToString(),
				"db_name": dbName,
				"item":    "stats_success",
			}).Set(0)
			continue
		}

		stats, err := db.Stats()
		if err != nil {
			span.Debugf("db %s stats failed: %v", dbName, err)
			metaStatMetric.With(prometheus.Labels{
				"region":  s.Region,
				"cluster": s.ClusterID.ToString(),
				"db_name": dbName,
				"item":    "stats_success",
			}).Set(0)
			continue
		}

		region := s.Region
		cluster := s.ClusterID.ToString()

		stats.RangeTagValue(func(tag string, value uint64) {
			metaStatMetric.WithLabelValues(region, cluster, dbName, tag).Set(float64(value))
		})
		metaStatMetric.WithLabelValues(region, cluster, dbName, "stats_success").Set(1)

		// per-CF metrics (only for multi-CF databases)
		stats.RangeCFs(func(name string, cf *kvstat.CFStats) {
			cf.RangeTagValue(func(tag string, value uint64) {
				metaCFStatMetric.WithLabelValues(region, cluster, dbName, name, tag).Set(float64(value))
			})
		})
	}
}

// statsProvider is the minimal read capability required by metrics and debug interfaces.
type statsProvider interface {
	Stats() (kvstat.Stats, error)
}

func (s *Service) resolveDBForMetric(dbName string) (statsProvider, error) {
	switch dbName {
	case "raft":
		if s.raftDB == nil {
			return nil, apierrors.ErrIllegalArguments
		}
		return s.raftDB, nil
	default:
		if db, ok := s.dbs[dbName]; ok {
			return db, nil
		}
	}
	return nil, apierrors.ErrIllegalArguments
}
