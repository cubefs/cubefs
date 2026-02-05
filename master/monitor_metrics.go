// Copyright 2018 The CubeFS Authors.
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

package master

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/remotecache/flashgroupmanager"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// metrics
const (
	StatPeriod                             = time.Minute * time.Duration(1)
	MetricDataNodesUsedGB                  = "dataNodes_used_GB"
	MetricDataNodesTotalGB                 = "dataNodes_total_GB"
	MetricDataNodesStat                    = "dataNodes_stats"
	MetricDataNodesIncreasedGB             = "dataNodes_increased_GB"
	MetricMetaNodesUsedGB                  = "metaNodes_used_GB"
	MetricMetaNodesTotalGB                 = "metaNodes_total_GB"
	MetricMetaNodesIncreasedGB             = "metaNodes_increased_GB"
	MetricDataNodesCount                   = "dataNodes_count"
	MetricMetaNodesCount                   = "metaNodes_count"
	MetricNodeStat                         = "node_stat"
	MetricPartitionCreateMetrics           = "partition_create_alarm"
	MetricVolCount                         = "vol_count"
	MetricVolTotalGB                       = "vol_total_GB"
	MetricVolUsedGB                        = "vol_used_GB"
	MetricVolUsageGB                       = "vol_usage_ratio"
	MetricVolStats                         = "vol_stats"
	MetricVolMetaCount                     = "vol_meta_count"
	MetricBadMpCount                       = "bad_mp_count"
	MetricBadDpCount                       = "bad_dp_count"
	MetricDiskError                        = "disk_error"
	MetricFlashNodesDiskError              = "flashNodes_disk_error"
	MetricDiskLost                         = "disk_lost"
	MetricDpUnableDecommissionCount        = "dp_unable_decommission_count"
	MetricDpNoSamePeer                     = "dp_no_same_peer"
	MetricMpNoSamePeer                     = "mp_no_same_peer"
	MetricBadDiskDecommissionTimeOverLimit = "bad_disk_decommission_time_over_limit"
	MetricDataNodesInactive                = "dataNodes_inactive"
	MetricInactiveDataNodeInfo             = "inactive_dataNodes_info"
	MetricMetaNodesInactive                = "metaNodes_inactive"
	MetricDataNodesNotWritable             = "dataNodes_not_writable"
	MetricDataNodesAllocable               = "dataNodes_allocable"
	MetricMetaNodesNotWritable             = "metaNodes_not_writable"
	MetricInactiveMetaNodeInfo             = "inactive_metaNodes_info"
	MetricMetaInconsistent                 = "mp_inconsistent"
	MetricMasterNoLeader                   = "master_no_leader"
	MetricMasterNoCache                    = "master_no_cache"
	MetricMasterSnapshot                   = "master_snapshot"
	MetricMastersInactive                  = "masters_inactive"
	MetricInactiveMasterInfo               = "inactive_masters_info"

	MetricMissingDp             = "missing_dp"
	MetricDpNoLeader            = "dp_no_leader"
	MetricMissingMp             = "missing_mp"
	MetricMpNoLeader            = "mp_no_leader"
	MetricReplicaMissingDPCount = "replica_missing_dp_count"
	MetricDpMissingLeaderCount  = "dp_missing_Leader_count"
	MetricMpMissingLeaderCount  = "mp_missing_Leader_count"
	MetricMpMissingReplicaCount = "mp_missing_Replica_count"
	MetricMpFailedRecoveryCount = "mp_failed_recovery_count"

	MetricLcNodesCount      = "lc_nodes_count"
	MetricLcVolStatus       = "lc_vol_status"
	MetricLcVolScanned      = "lc_vol_scanned"
	MetricLcVolExpired      = "lc_vol_expired"
	MetricLcVolMigrateBytes = "lc_vol_migrate_bytes"
	MetricLcVolError        = "lc_vol_error"

	MetricDiskDecommissionSuccess = "disk_decommission_success"
	MetricMetaNotRocksdbWritable  = "meta_rocksdb_not_writable"

	MetricSSDNodeSetUnbalancedDPs = "ssd_nodeset_unbalanced_dp_count"
	MetricSSDRackConflictDPs      = "ssd_rack_conflict_dp_count"
	MetricHDDNodeSetUnbalancedDPs = "hdd_nodeset_unbalanced_dp_count"
	MetricHDDRackConflictDPs      = "hdd_rack_conflict_dp_count"
)

const (
	txLabel = "tx"
)

var WarnMetrics *warningMetrics

type monitorMetrics struct {
	cluster                          *Cluster
	dataNodesCount                   *exporter.Gauge
	metaNodesCount                   *exporter.Gauge
	volCount                         *exporter.Gauge
	dataNodesTotal                   *exporter.Gauge
	dataNodesUsed                    *exporter.Gauge
	dataNodeStat                     *exporter.GaugeVec
	dataNodeIncreased                *exporter.Gauge
	metaNodesTotal                   *exporter.Gauge
	metaNodesUsed                    *exporter.Gauge
	metaNodesIncreased               *exporter.Gauge
	volTotalSpace                    *exporter.GaugeVec
	volUsedSpace                     *exporter.GaugeVec
	volUsage                         *exporter.GaugeVec
	volMetaCount                     *exporter.GaugeVec
	volStats                         *exporter.GaugeVec
	badMpCount                       *exporter.Gauge
	badDpCount                       *exporter.Gauge
	diskError                        *exporter.GaugeVec
	flashNodesDiskError              *exporter.GaugeVec
	diskLost                         *exporter.GaugeVec
	dpUnableDecommissionCount        *exporter.Gauge
	dpNoSamePeer                     *exporter.GaugeVec
	mpNoSamePeer                     *exporter.GaugeVec
	badDiskDecommissionTimeOverLimit *exporter.GaugeVec
	dataNodesNotWritable             *exporter.Gauge    // TODO: remove in the future
	dataNodesAllocable               *exporter.Gauge    // TODO: remove in the future
	metaNodesNotWritable             *exporter.Gauge    // TODO: remove in the future
	dataNodesInactive                *exporter.Gauge    // TODO: remove in the future
	InactiveDataNodeInfo             *exporter.GaugeVec // TODO: remove in the future
	metaNodesInactive                *exporter.Gauge    // TODO: remove in the future
	InactiveMetaNodeInfo             *exporter.GaugeVec // TODO: remove in the future
	mastersInactive                  *exporter.Gauge
	InactiveMasterInfo               *exporter.GaugeVec
	ReplicaMissingDPCount            *exporter.GaugeVec
	DpMissingLeaderCount             *exporter.GaugeVec
	MpMissingLeaderCount             *exporter.Gauge
	MpMissingReplicaCount            *exporter.Gauge
	MpFailedRecoveryCount            *exporter.Gauge
	metaEqualCheckFail               *exporter.GaugeVec
	masterNoLeader                   *exporter.Gauge
	masterNoCache                    *exporter.GaugeVec
	masterSnapshot                   *exporter.Gauge

	nodeStat        *exporter.GaugeVec
	partitionCreate *exporter.GaugeVec

	volNames           map[string]struct{}
	badDisks           map[string]string
	flashNodesBadDisks map[string]string
	inconsistentMps    map[string]string
	replicaCntMap      map[uint64]struct{}

	lcNodesCount      *exporter.Gauge
	lcId              map[string]struct{}
	lcVolStatus       *exporter.GaugeVec
	lcVolScanned      *exporter.GaugeVec
	lcVolExpired      *exporter.GaugeVec
	lcVolMigrateBytes *exporter.GaugeVec
	lcVolError        *exporter.GaugeVec

	diskDecommissionSuccess *exporter.GaugeVec

	metaNodesNotRocksdbWritable *exporter.Gauge

	ssdNodeSetUnbalancedDPs *exporter.Gauge
	ssdRackConflictDPs      *exporter.Gauge
	hddNodeSetUnbalancedDPs *exporter.Gauge
	hddRackConflictDPs      *exporter.Gauge

	lastCheckPartitionCreateTime time.Time
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{
		cluster:            c,
		volNames:           make(map[string]struct{}),
		badDisks:           make(map[string]string),
		flashNodesBadDisks: make(map[string]string),
		inconsistentMps:    make(map[string]string),
		replicaCntMap:      make(map[uint64]struct{}),
		lcId:               make(map[string]struct{}),
	}
}

type voidType struct{}

var voidVal voidType

type addrSet struct {
	addrs        map[string]voidType // empty value of map does not occupy memory
	replicaNum   string
	replicaAlive string
}

type NoLeaderPartInfo struct {
	ReportTime int64
	Replicas   uint8
}

type warningMetrics struct {
	cluster               *Cluster
	missingDp             *exporter.GaugeVec
	dpNoLeader            *exporter.GaugeVec
	missingMp             *exporter.GaugeVec
	mpNoLeader            *exporter.GaugeVec
	dpMutex               sync.Mutex
	mpMutex               sync.Mutex
	dpNoLeaderInfo        map[uint64]NoLeaderPartInfo
	mpNoLeaderInfo        map[uint64]NoLeaderPartInfo
	dpMissingReplicaMutex sync.Mutex
	mpMissingReplicaMutex sync.Mutex
	dpMissingReplicaInfo  map[string]addrSet
	mpMissingReplicaInfo  map[string]addrSet
}

func newWarningMetrics(c *Cluster) *warningMetrics {
	return &warningMetrics{
		cluster:              c,
		missingDp:            exporter.NewGaugeVec(MetricMissingDp, "", []string{"clusterName", "partitionID", "addr", "ReplicaAlive", "ReplicaNum"}),
		dpNoLeader:           exporter.NewGaugeVec(MetricDpNoLeader, "", []string{"clusterName", "partitionID", "ReplicaNum"}),
		missingMp:            exporter.NewGaugeVec(MetricMissingMp, "", []string{"clusterName", "partitionID", "addr"}),
		mpNoLeader:           exporter.NewGaugeVec(MetricMpNoLeader, "", []string{"clusterName", "partitionID", "ReplicaNum"}),
		dpNoLeaderInfo:       make(map[uint64]NoLeaderPartInfo),
		mpNoLeaderInfo:       make(map[uint64]NoLeaderPartInfo),
		dpMissingReplicaInfo: make(map[string]addrSet),
		mpMissingReplicaInfo: make(map[string]addrSet),
	}
}

func (m *warningMetrics) reset() {
	log.LogInfo("action[warningMetrics] reset all")
	m.dpMutex.Lock()
	for dp, noLeaderInfo := range m.dpNoLeaderInfo {
		if m.dpNoLeader != nil {
			m.dpNoLeader.DeleteLabelValues(m.cluster.Name, strconv.FormatUint(dp, 10), strconv.FormatUint(uint64(noLeaderInfo.Replicas), 10))
		}
		delete(m.dpNoLeaderInfo, dp)
	}
	m.dpMutex.Unlock()

	m.mpMutex.Lock()
	for mp, noLeaderInfo := range m.mpNoLeaderInfo {
		if m.mpNoLeader != nil {
			m.mpNoLeader.DeleteLabelValues(m.cluster.Name, strconv.FormatUint(mp, 10), strconv.FormatUint(uint64(noLeaderInfo.Replicas), 10))
		}
		delete(m.mpNoLeaderInfo, mp)
	}
	m.mpMutex.Unlock()

	m.dpMissingReplicaMutex.Lock()
	for id, dpAddrSet := range m.dpMissingReplicaInfo {
		for addr := range dpAddrSet.addrs {
			if m.missingDp != nil {
				m.missingDp.DeleteLabelValues(m.cluster.Name, id, addr, dpAddrSet.replicaAlive, dpAddrSet.replicaNum)
			}
		}
		delete(m.dpMissingReplicaInfo, id)
	}
	m.dpMissingReplicaMutex.Unlock()

	m.mpMissingReplicaMutex.Lock()
	for id, mpAddrSet := range m.mpMissingReplicaInfo {
		for addr := range mpAddrSet.addrs {
			if m.missingMp != nil {
				m.missingMp.DeleteLabelValues(m.cluster.Name, id, addr)
			}
		}
		delete(m.mpMissingReplicaInfo, id)
	}
	m.mpMissingReplicaMutex.Unlock()
}

// The caller is responsible for lock
func (m *warningMetrics) deleteMissingDp(missingDpAddrSet addrSet, clusterName, dpId, addr string) {
	if len(missingDpAddrSet.addrs) == 0 {
		return
	}

	if _, ok := missingDpAddrSet.addrs[addr]; !ok {
		return
	}
	m.dpMissingReplicaMutex.Lock()
	replicaAlive := m.dpMissingReplicaInfo[dpId].replicaAlive
	replicaNum := m.dpMissingReplicaInfo[dpId].replicaNum

	delete(missingDpAddrSet.addrs, addr)
	if len(missingDpAddrSet.addrs) == 0 {
		delete(m.dpMissingReplicaInfo, dpId)
	}
	m.dpMissingReplicaMutex.Unlock()

	if m.missingDp != nil {
		m.missingDp.DeleteLabelValues(clusterName, dpId, addr, replicaAlive, replicaNum)
	}
	log.LogDebugf("action[deleteMissingDp] delete: dpId(%v), addr(%v)", dpId, addr)
}

// leader only, TODO: remove
func (m *warningMetrics) WarnMissingDp(clusterName, addr string, partitionID uint64, report bool) {
	if clusterName != m.cluster.Name {
		return
	}

	m.dpMissingReplicaMutex.Lock()
	id := strconv.FormatUint(partitionID, 10)
	if !report {
		m.dpMissingReplicaMutex.Unlock()
		m.deleteMissingDp(m.dpMissingReplicaInfo[id], clusterName, id, addr)
		return
	}
	defer m.dpMissingReplicaMutex.Unlock()

	m.dpMissingReplicaMutex.Lock()
	defer m.dpMissingReplicaMutex.Unlock()

	m.dpMissingReplicaMutex.Lock()
	defer m.dpMissingReplicaMutex.Unlock()

	m.dpMissingReplicaMutex.Lock()
	defer m.dpMissingReplicaMutex.Unlock()

	if _, ok := m.dpMissingReplicaInfo[id]; !ok {
		m.dpMissingReplicaInfo[id] = addrSet{addrs: make(map[string]voidType)}
	}
	m.dpMissingReplicaInfo[id].addrs[addr] = voidVal
}

// leader only
func (m *warningMetrics) CleanObsoleteDpMissing(clusterName string, dp *DataPartition) {
	if clusterName != m.cluster.Name {
		return
	}
	id := strconv.FormatUint(dp.PartitionID, 10)

	m.dpMissingReplicaMutex.Lock()
	missingRepAddrs, ok := m.dpMissingReplicaInfo[id]
	m.dpMissingReplicaMutex.Unlock()
	if !ok {
		return
	}

	for addr := range missingRepAddrs.addrs {
		_, hasReplica := dp.hasReplica(addr)
		hasHost := dp.hasHost(addr)

		if !hasReplica && !hasHost {
			log.LogDebugf("action[warningMetrics] delete obsolete dp missing record: dpId(%v), addr(%v)", id, addr)
			m.deleteMissingDp(missingRepAddrs, clusterName, id, addr)
		}
	}
}

// leader only
func (m *warningMetrics) WarnDpNoLeader(clusterName string, partitionID uint64, replicas uint8, report bool) {
	if clusterName != m.cluster.Name {
		return
	}

	m.dpMutex.Lock()
	defer m.dpMutex.Unlock()
	info, ok := m.dpNoLeaderInfo[partitionID]
	if !report {
		if ok {
			delete(m.dpNoLeaderInfo, partitionID)
			if m.dpNoLeader != nil {
				m.dpNoLeader.DeleteLabelValues(clusterName, strconv.FormatUint(partitionID, 10), strconv.FormatUint(uint64(replicas), 10))
			}
		}
		return
	}

	now := time.Now().Unix()
	if !ok {
		m.dpNoLeaderInfo[partitionID] = NoLeaderPartInfo{ReportTime: now, Replicas: replicas}
		return
	}
	if now-info.ReportTime > m.cluster.cfg.DpNoLeaderReportIntervalSec {
		// if m.dpNoLeader != nil {
		// 	// m.dpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10), strconv.FormatUint(uint64(replicas), 10))
		// }
		m.dpNoLeaderInfo[partitionID] = NoLeaderPartInfo{ReportTime: now, Replicas: replicas}
	}
}

// The caller is responsible for lock
func (m *warningMetrics) deleteMissingMp(missingMpAddrSet addrSet, clusterName, mpId, addr string) {
	m.mpMissingReplicaMutex.Lock()
	defer m.mpMissingReplicaMutex.Unlock()
	if len(missingMpAddrSet.addrs) == 0 {
		return
	}

	if _, ok := missingMpAddrSet.addrs[addr]; !ok {
		return
	}

	delete(missingMpAddrSet.addrs, addr)
	if len(missingMpAddrSet.addrs) == 0 {
		delete(m.mpMissingReplicaInfo, mpId)
	}

	if m.missingMp != nil {
		m.missingMp.DeleteLabelValues(clusterName, mpId, addr)
	}
	log.LogDebugf("action[deleteMissingMp] delete: mpId(%v), addr(%v)", mpId, addr)
}

// leader only
func (m *warningMetrics) WarnMissingMp(clusterName, addr string, partitionID uint64, report bool) {
	m.mpMissingReplicaMutex.Lock()

	if clusterName != m.cluster.Name {
		m.mpMissingReplicaMutex.Unlock()
		return
	}

	id := strconv.FormatUint(partitionID, 10)
	if !report {
		m.mpMissingReplicaMutex.Unlock()
		m.deleteMissingMp(m.mpMissingReplicaInfo[id], clusterName, id, addr)
		return
	}

	// if m.missingMp != nil {
	// 	// m.missingMp.SetWithLabelValues(1, clusterName, id, addr)
	// }
	if _, ok := m.mpMissingReplicaInfo[id]; !ok {
		m.mpMissingReplicaInfo[id] = addrSet{addrs: make(map[string]voidType)}
		// m.mpMissingReplicaInfo[id] = make(addrSet)
	}
	m.mpMissingReplicaInfo[id].addrs[addr] = voidVal
	m.mpMissingReplicaMutex.Unlock()
}

// leader only
func (m *warningMetrics) CleanObsoleteMpMissing(clusterName string, mp *MetaPartition) {
	if clusterName != m.cluster.Name {
		return
	}
	id := strconv.FormatUint(mp.PartitionID, 10)

	m.mpMissingReplicaMutex.Lock()
	missingRepAddrs, ok := m.mpMissingReplicaInfo[id]
	if !ok {
		m.mpMissingReplicaMutex.Unlock()
		return
	}
	m.mpMissingReplicaMutex.Unlock()
	for addr := range missingRepAddrs.addrs {
		if _, err := mp.getMetaReplica(addr); err != nil {
			log.LogDebugf("action[warningMetrics] delete obsolete Mp missing record: dpId(%v), addr(%v)", id, addr)
			m.deleteMissingMp(missingRepAddrs, clusterName, id, addr)
		}
	}
}

// leader only
func (m *warningMetrics) WarnMpNoLeader(clusterName string, partitionID uint64, replicas uint8, report bool) {
	if clusterName != m.cluster.Name {
		return
	}
	m.mpMutex.Lock()
	defer m.mpMutex.Unlock()
	info, ok := m.mpNoLeaderInfo[partitionID]
	if !report {
		if ok {
			delete(m.mpNoLeaderInfo, partitionID)
			if m.mpNoLeader != nil {
				m.mpNoLeader.DeleteLabelValues(clusterName, strconv.FormatUint(partitionID, 10), strconv.FormatUint(uint64(replicas), 10))
			}
		}
		return
	}

	now := time.Now().Unix()

	if !ok {
		m.mpNoLeaderInfo[partitionID] = NoLeaderPartInfo{ReportTime: now, Replicas: replicas}
		return
	}

	if now-info.ReportTime > m.cluster.cfg.MpNoLeaderReportIntervalSec {
		// if m.mpNoLeader != nil {
		// 	// m.mpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10), strconv.FormatUint(uint64(replicas), 10))
		// }
		m.mpNoLeaderInfo[partitionID] = NoLeaderPartInfo{ReportTime: now, Replicas: replicas}
	}
}

func (mm *monitorMetrics) start() {
	mm.dataNodesTotal = exporter.NewGauge(MetricDataNodesTotalGB)
	mm.dataNodesUsed = exporter.NewGauge(MetricDataNodesUsedGB)
	mm.dataNodeStat = exporter.NewGaugeVec(MetricDataNodesStat, "", []string{"media", "type"})
	mm.dataNodeIncreased = exporter.NewGauge(MetricDataNodesIncreasedGB)
	mm.metaNodesTotal = exporter.NewGauge(MetricMetaNodesTotalGB)
	mm.metaNodesUsed = exporter.NewGauge(MetricMetaNodesUsedGB)
	mm.metaNodesIncreased = exporter.NewGauge(MetricMetaNodesIncreasedGB)
	mm.dataNodesCount = exporter.NewGauge(MetricDataNodesCount)
	mm.metaNodesCount = exporter.NewGauge(MetricMetaNodesCount)
	mm.volCount = exporter.NewGauge(MetricVolCount)
	mm.volTotalSpace = exporter.NewGaugeVec(MetricVolTotalGB, "", []string{"volName"})
	mm.volUsedSpace = exporter.NewGaugeVec(MetricVolUsedGB, "", []string{"volName"})
	mm.volUsage = exporter.NewGaugeVec(MetricVolUsageGB, "", []string{"volName"})
	mm.volStats = exporter.NewGaugeVec(MetricVolStats, "", []string{"volName", "type", "media"})
	mm.volMetaCount = exporter.NewGaugeVec(MetricVolMetaCount, "", []string{"volName", "type"})
	mm.badMpCount = exporter.NewGauge(MetricBadMpCount)
	mm.badDpCount = exporter.NewGauge(MetricBadDpCount)
	mm.diskError = exporter.NewGaugeVec(MetricDiskError, "", []string{"addr", "path"})
	mm.flashNodesDiskError = exporter.NewGaugeVec(MetricFlashNodesDiskError, "", []string{"addr", "path"})
	mm.diskLost = exporter.NewGaugeVec(MetricDiskLost, "", []string{"addr", "path"})
	mm.dpUnableDecommissionCount = exporter.NewGauge(MetricDpUnableDecommissionCount)
	mm.dpNoSamePeer = exporter.NewGaugeVec(MetricDpNoSamePeer, "", []string{"dpId"})
	mm.mpNoSamePeer = exporter.NewGaugeVec(MetricMpNoSamePeer, "", []string{"mpId"})
	mm.badDiskDecommissionTimeOverLimit = exporter.NewGaugeVec(MetricBadDiskDecommissionTimeOverLimit, "", []string{"addr", "path", "firstReportTime"})
	mm.nodeStat = exporter.NewGaugeVec(MetricNodeStat, "", []string{"type", "addr", "stat", "zone", "set", "media", "writable", "alloc", "rack", "pool"})
	mm.partitionCreate = exporter.NewGaugeVec(MetricPartitionCreateMetrics, "", []string{"type", "racklevel", "media"})
	mm.dataNodesInactive = exporter.NewGauge(MetricDataNodesInactive)
	mm.InactiveDataNodeInfo = exporter.NewGaugeVec(MetricInactiveDataNodeInfo, "", []string{"clusterName", "addr"})
	mm.metaNodesInactive = exporter.NewGauge(MetricMetaNodesInactive)
	mm.dataNodesNotWritable = exporter.NewGauge(MetricDataNodesNotWritable)
	mm.dataNodesAllocable = exporter.NewGauge(MetricDataNodesAllocable)
	mm.metaNodesNotWritable = exporter.NewGauge(MetricMetaNodesNotWritable)
	mm.InactiveMetaNodeInfo = exporter.NewGaugeVec(MetricInactiveMetaNodeInfo, "", []string{"clusterName", "addr"})
	mm.ReplicaMissingDPCount = exporter.NewGaugeVec(MetricReplicaMissingDPCount, "", []string{"replicaNum", "media"})
	mm.DpMissingLeaderCount = exporter.NewGaugeVec(MetricDpMissingLeaderCount, "", []string{"replicaNum", "media"})
	mm.MpMissingLeaderCount = exporter.NewGauge(MetricMpMissingLeaderCount)
	mm.MpMissingReplicaCount = exporter.NewGauge(MetricMpMissingReplicaCount)
	mm.MpFailedRecoveryCount = exporter.NewGauge(MetricMpFailedRecoveryCount)
	mm.metaEqualCheckFail = exporter.NewGaugeVec(MetricMetaInconsistent, "", []string{"volume", "mpId"})

	mm.masterSnapshot = exporter.NewGauge(MetricMasterSnapshot)
	mm.masterNoLeader = exporter.NewGauge(MetricMasterNoLeader)
	mm.masterNoCache = exporter.NewGaugeVec(MetricMasterNoCache, "", []string{"volName"})
	mm.mastersInactive = exporter.NewGauge(MetricMastersInactive)
	mm.InactiveMasterInfo = exporter.NewGaugeVec(MetricInactiveMasterInfo, "", []string{"clusterName", "addr"})

	mm.lcNodesCount = exporter.NewGauge(MetricLcNodesCount)
	mm.lcVolStatus = exporter.NewGaugeVec(MetricLcVolStatus, "", []string{"id"})
	mm.lcVolScanned = exporter.NewGaugeVec(MetricLcVolScanned, "", []string{"id", "type"})
	mm.lcVolExpired = exporter.NewGaugeVec(MetricLcVolExpired, "", []string{"id", "type"})
	mm.lcVolMigrateBytes = exporter.NewGaugeVec(MetricLcVolMigrateBytes, "", []string{"id", "type"})
	mm.lcVolError = exporter.NewGaugeVec(MetricLcVolError, "", []string{"id", "type"})

	mm.diskDecommissionSuccess = exporter.NewGaugeVec(MetricDiskDecommissionSuccess, "", []string{"addr", "path"})
	mm.metaNodesNotRocksdbWritable = exporter.NewGauge(MetricMetaNotRocksdbWritable)

	mm.ssdNodeSetUnbalancedDPs = exporter.NewGauge(MetricSSDNodeSetUnbalancedDPs)
	mm.ssdRackConflictDPs = exporter.NewGauge(MetricSSDRackConflictDPs)
	mm.hddNodeSetUnbalancedDPs = exporter.NewGauge(MetricHDDNodeSetUnbalancedDPs)
	mm.hddRackConflictDPs = exporter.NewGauge(MetricHDDRackConflictDPs)

	go mm.statMetrics()
}

func (mm *monitorMetrics) statMetrics() {
	ticker := time.NewTicker(StatPeriod)
	defer func() {
		ticker.Stop()
		if err := recover(); err != nil {
			ticker.Stop()
			log.LogErrorf("statMetrics panic,msg:%v", err)
		}
	}()

	for range ticker.C {
		partition := mm.cluster.partition
		if partition != nil && partition.IsRaftLeader() {
			mm.resetFollowerMetrics()
			mm.doStat()
		} else {
			mm.resetAllLeaderMetrics()
			mm.doFollowerStat()
		}
	}
}

func (mm *monitorMetrics) doFollowerStat() {
	if mm.cluster.leaderInfo.addr == "" {
		mm.masterNoLeader.Set(1)
	} else {
		mm.masterNoLeader.Set(0)
	}
	if mm.cluster.fsm.onSnapshot {
		mm.masterSnapshot.Set(1)
	} else {
		mm.masterSnapshot.Set(0)
	}
	mm.setVolNoCacheMetrics()
}

func (mm *monitorMetrics) doStat() {
	dataNodeCount := mm.cluster.dataNodeCount()
	mm.dataNodesCount.Set(float64(dataNodeCount))
	metaNodeCount := mm.cluster.metaNodeCount()
	mm.metaNodesCount.Set(float64(metaNodeCount))
	lcNodeCount := mm.cluster.lcNodeCount()
	mm.lcNodesCount.Set(float64(lcNodeCount))
	volCount := len(mm.cluster.vols)
	mm.volCount.Set(float64(volCount))
	for m, s := range mm.cluster.dataStatsByMedia {
		mm.dataNodeStat.SetWithLabelValues(float64(s.UsedGB), m, "used")
		mm.dataNodeStat.SetWithLabelValues(float64(s.TotalGB), m, "total")
	}
	mm.dataNodesTotal.Set(float64(mm.cluster.dataNodeStatInfo.TotalGB))
	mm.dataNodesUsed.Set(float64(mm.cluster.dataNodeStatInfo.UsedGB))
	mm.dataNodeIncreased.Set(float64(mm.cluster.dataNodeStatInfo.IncreasedGB))
	mm.metaNodesTotal.Set(float64(mm.cluster.metaNodeStatInfo.TotalGB))
	mm.metaNodesUsed.Set(float64(mm.cluster.metaNodeStatInfo.UsedGB))
	mm.metaNodesIncreased.Set(float64(mm.cluster.metaNodeStatInfo.IncreasedGB))
	mm.setVolMetrics()
	mm.setBadPartitionMetrics()
	mm.checkPartitionCreateMetrics()
	mm.setDiskErrorMetric()
	mm.setDiskLostMetric()
	mm.setFlashNodesDiskErrorMetric()
	mm.setDpUnableDecommissionMetric()
	mm.setDpNoSamePeerMetric()
	mm.setMpNoSamePeerMetric()
	mm.setBadDiskDecommissionTimeOverLimit()
	mm.setNotWritableDataNodesCount()
	mm.setNotWritableMetaNodesCount()
	mm.setMpInconsistentErrorMetric()
	mm.setMpAndDpMetrics()
	mm.setLcMetrics()
	mm.setDiskDecommissionedMetric()
	mm.updateDataNodesStat()
	mm.updateMetaNodesStat()
	mm.updateMastersStat()
	mm.setNotRocksdbWritableMetaNodesCount()
	mm.setDistributionOptimizationMetrics()
}

func (mm *monitorMetrics) checkPartitionCreateMetrics() {
	if time.Since(mm.lastCheckPartitionCreateTime) < time.Duration(mm.cluster.cfg.checkPartitionCreateInterval)*time.Minute {
		return
	}
	mm.lastCheckPartitionCreateTime = time.Now()

	vols := mm.cluster.copyVols()

	testVols := make([]*Vol, 0)
	for _, vol := range vols {
		same := false

		for _, v1 := range testVols {
			if v1.sameCreateMode(vol) {
				same = true
				break
			}
		}

		if !same {
			testVols = append(testVols, vol)
		}
	}

	rackAwareLevel := []proto.RackAwareLevel{proto.RackAwareStrong, proto.RackAwareNone}
	failStats := make(map[string]int)
	allStats := make(map[string]bool)

	// try create partitions
	for _, vol := range testVols {

		log.LogInfof("checkPartitionCreateMetrics: vol %v", vol.createModeString())

		for _, rackLevel := range rackAwareLevel {
			if vol.DefaultStoreMode == proto.StoreModeRocksDb {
				key := fmt.Sprintf("metaRocksdb_%v_default", rackLevel)
				allStats[key] = true
				if !mm.checkHostSelection(TypeRocksdbPartition, vol, 0, rackLevel) {
					failStats[key]++
				}
			} else {
				key := fmt.Sprintf("metaMem_%v_default", rackLevel)
				allStats[key] = true

				if !mm.checkHostSelection(TypeMetaPartition, vol, 0, rackLevel) {
					failStats[key]++
				}
			}
		}

		for _, media := range vol.allowedStorageClass {
			if !proto.IsStorageClassReplica(media) {
				continue
			}
			for _, rackLevel := range rackAwareLevel {
				key := fmt.Sprintf("data_%v_%v", rackLevel, proto.MediaTypeString(media))
				allStats[key] = true
				if !mm.checkHostSelection(TypeDataPartition, vol, media, rackLevel) {
					failStats[key]++
				}
			}
		}
	}

	for key := range allStats {
		parts := strings.Split(key, "_")
		if len(parts) != 3 {
			log.LogErrorf("checkPartitionCreateMetrics: failStats key %v is invalid", key)
			continue
		}

		count := failStats[key]
		mm.partitionCreate.SetWithLabelValues(float64(count), parts...)
	}

	log.LogInfof("checkPartitionCreateMetrics finished: testVols %v, failStats %v", len(testVols), failStats)
}

func (mm *monitorMetrics) checkHostSelection(nodeType uint32, vol *Vol, mediaType uint32, rackLevel proto.RackAwareLevel) bool {
	_, _, err := mm.cluster.getHostFromNormalZoneForCreate(
		nodeType, int(vol.dpReplicaNum), vol.zoneName, proto.UnSpecifiedPoolId, rackLevel, vol)

	partitionType := "metaMem"
	if nodeType == TypeRocksdbPartition {
		partitionType = "metaRocksdb"
	} else if nodeType == TypeDataPartition {
		partitionType = "data"
	}

	mediaStr := ""
	if mediaType != 0 {
		mediaStr = fmt.Sprintf(", media: %v", proto.MediaTypeString(mediaType))
	}

	if err != nil {
		log.LogWarnf("checkPartitionCreateMetrics: getHostFromNormalZoneForCreate failed: vol %v, mode %v, type %s%s, rackLevel %v, err %v",
			vol.Name, vol.createModeString(), partitionType, mediaStr, rackLevel, err)
		return false
	}
	log.LogInfof("checkPartitionCreateMetrics: getHostFromNormalZoneForCreate success: vol %v, mode %v, type %s%s, rackLevel %v",
		vol.Name, vol.createModeString(), partitionType, mediaStr, rackLevel)
	return true
}

func (mm *monitorMetrics) setMpAndDpMetrics() {
	start := time.Now()
	defer func() {
		log.LogInfof("setMpAndDpMetrics: total cost %d ms", time.Since(start).Milliseconds())
	}()

	type dpInfo struct {
		ReplicNum string
		Cnt       int
		Media     string
	}

	dpMissingLeaderMap := make(map[string]*dpInfo)
	dpMissingReplicaMap := make(map[string]*dpInfo)

	addMap := func(dpMap map[string]*dpInfo, repNum uint8, media uint32) {
		key := fmt.Sprintf("%s_%v", proto.MediaTypeString(media), repNum)
		info := dpMap[key]
		if info != nil {
			info.Cnt++
			return
		}

		info = &dpInfo{
			ReplicNum: strconv.Itoa(int(repNum)),
			Cnt:       1,
			Media:     proto.MediaTypeString(media),
		}

		dpMap[key] = info
	}

	mpMissingLeaderCount := 0
	mpMissingReplicaCount := 0
	mpFailedRecoveryCount := 0

	vols := mm.cluster.copyVols()
	for _, vol := range vols {
		if vol.IsDeleted() || vol.isInitializingOrInitFailed() {
			continue
		}

		replicaNum := vol.dpReplicaNum
		mm.replicaCntMap[uint64(replicaNum)] = struct{}{}

		dps := vol.dataPartitions.clonePartitions()
		for _, dp := range dps {
			if dp.IsDiscard {
				continue
			}

			if replicaNum > uint8(len(dp.liveReplicas(mm.cluster.getDataPartitionTimeoutSec()))) {
				addMap(dpMissingReplicaMap, replicaNum, dp.MediaType)
			}
			if proto.IsNormalDp(dp.PartitionType) && dp.getLeaderAddr() == "" && time.Now().Unix()-dp.LeaderReportTime > mm.cluster.cfg.DpNoLeaderReportIntervalSec {
				reportTime := time.Unix(dp.LeaderReportTime, 0)
				msg := fmt.Sprintf("dp(%v) lost leader, leader last report time(%v), since report time(%v)", dp.PartitionID, reportTime, time.Since(reportTime))
				auditlog.LogMasterOp("setMpAndDpMetrics", msg, nil)
				addMap(dpMissingLeaderMap, replicaNum, dp.MediaType)
			}
		}
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			if !mp.isLeaderExist() && time.Now().Unix()-mp.LeaderReportTime > mm.cluster.cfg.MpNoLeaderReportIntervalSec {
				mpMissingLeaderCount++
			}
			if len(mp.getActiveAddrs(mm.cluster.getMetaPartitionTimeoutSec())) < int(mp.ReplicaNum) {
				mpMissingReplicaCount++
			}
			if mp.RecoverState == proto.RecoverStateFailed {
				mpFailedRecoveryCount++
			}
		}
		vol.mpsLock.RUnlock()
	}

	mm.DpMissingLeaderCount.Reset()
	mm.ReplicaMissingDPCount.Reset()

	for _, dp := range dpMissingLeaderMap {
		mm.DpMissingLeaderCount.SetWithLabelValues(float64(dp.Cnt), dp.ReplicNum, dp.Media)
	}
	for _, dp := range dpMissingReplicaMap {
		mm.ReplicaMissingDPCount.SetWithLabelValues(float64(dp.Cnt), dp.ReplicNum, dp.Media)
	}

	mm.MpMissingLeaderCount.Set(float64(mpMissingLeaderCount))
	mm.MpMissingReplicaCount.Set(float64(mpMissingReplicaCount))
	mm.MpFailedRecoveryCount.Set(float64(mpFailedRecoveryCount))
}

func (mm *monitorMetrics) setVolNoCacheMetrics() {
	deleteVolNames := make(map[string]struct{})
	ObsoleteVVolNames := make(map[string]struct{})

	mm.cluster.followerReadManager.rwMutex.RLock()
	for volName, stat := range mm.cluster.followerReadManager.status {
		if mm.cluster.followerReadManager.isVolRecordObsolete(volName) {
			deleteVolNames[volName] = struct{}{}
			ObsoleteVVolNames[volName] = struct{}{}
			log.LogDebugf("setVolNoCacheMetrics: to deleteVolNames volName %v for vol becomes obsolete", volName)
			continue
		}

		if stat {
			deleteVolNames[volName] = struct{}{}
			log.LogDebugf("setVolNoCacheMetrics: to deleteVolNames volName %v for status becomes ok", volName)
			continue
		}
		log.LogWarnf("setVolNoCacheMetrics volName %v", volName)
		mm.masterNoCache.SetWithLabelValues(1, volName)
	}
	mm.cluster.followerReadManager.rwMutex.RUnlock()

	for volName := range deleteVolNames {
		mm.masterNoCache.DeleteLabelValues(volName)
	}

	mm.cluster.followerReadManager.DelObsoleteVolRecord(ObsoleteVVolNames)
}

func (mm *monitorMetrics) setVolMetrics() {
	deleteVolNames := make(map[string]struct{})
	for k, v := range mm.volNames {
		deleteVolNames[k] = v
		delete(mm.volNames, k)
	}

	mm.cluster.volStatInfo.Range(func(key, value interface{}) bool {
		volStatInfo, ok := value.(*volStatInfo)
		if !ok {
			return true
		}
		volName, ok := key.(string)
		if !ok {
			return true
		}
		mm.volNames[volName] = struct{}{}
		delete(deleteVolNames, volName)

		mm.volTotalSpace.SetWithLabelValues(float64(volStatInfo.TotalSize), volName)
		mm.volUsedSpace.SetWithLabelValues(float64(volStatInfo.UsedSize), volName)
		usedRatio, e := strconv.ParseFloat(volStatInfo.UsedRatio, 64)
		if e == nil {
			mm.volUsage.SetWithLabelValues(usedRatio, volName)
		}
		if usedRatio > volWarnUsedRatio {
			WarnBySpecialKey("vol size used too high", fmt.Sprintf("vol: %v(total: %v, used: %v) has used(%v) to be full", volName, volStatInfo.TotalSize, volStatInfo.UsedRatio, volStatInfo.UsedSize))
		}

		return true
	})

	for volName, vol := range mm.cluster.allVols() {
		inodeCount := uint64(0)
		dentryCount := uint64(0)
		mpCount := uint64(0)
		freeListLen := uint64(0)
		txCnt := uint64(0)

		for _, mpv := range vol.getMetaPartitionsView() {
			inodeCount += mpv.InodeCount
			dentryCount += mpv.DentryCount
			mpCount += 1
			freeListLen += mpv.FreeListLen
			txCnt += mpv.TxCnt
		}

		for _, s := range vol.StatByStorageClass {
			used := float64(s.UsedSizeBytes / util.GB)
			quota := s.QuotaGB
			ratio := float64(0)
			if quota == 0 {
				quota = vol.Capacity
			}
			if quota > 0 {
				ratio = math.Round(used/float64(quota)*1000) / 1000
			}

			mm.volStats.SetWithLabelValues(used, volName, "used", proto.StorageClassString(s.StorageClass))
			mm.volStats.SetWithLabelValues(float64(quota), volName, "total", proto.StorageClassString(s.StorageClass))
			mm.volStats.SetWithLabelValues(ratio, volName, "ratio", proto.StorageClassString(s.StorageClass))
		}

		for _, s := range vol.StatByPool {
			used := float64(s.UsedSizeBytes / util.GB)
			quota := s.QuotaGB
			ratio := float64(0)
			if quota == 0 {
				quota = vol.Capacity
			}
			if quota > 0 {
				ratio = math.Round(used/float64(quota)*1000) / 1000
			}

			poodIdString := mm.cluster.getPoolNameById(s.PoolId)
			mm.volStats.SetWithLabelValues(used, volName, "pool_used", poodIdString)
			mm.volStats.SetWithLabelValues(float64(quota), volName, "pool_total", poodIdString)
			mm.volStats.SetWithLabelValues(ratio, volName, "pool_ratio", poodIdString)
		}

		for _, s := range vol.StatByDpPool {
			used := float64(s.UsedSizeBytes / util.GB)
			mm.volStats.SetWithLabelValues(used, volName, "dp_pool_used", mm.cluster.getPoolNameById(s.PoolId))
		}

		for _, s := range vol.StatByDpMediaType {
			used := float64(s.UsedSizeBytes / util.GB)
			mm.volStats.SetWithLabelValues(used, volName, "dp_media_used", proto.StorageClassString(s.StorageClass))
		}

		mm.volMetaCount.SetWithLabelValues(float64(inodeCount), volName, "inode")
		mm.volMetaCount.SetWithLabelValues(float64(dentryCount), volName, "dentry")
		mm.volMetaCount.SetWithLabelValues(float64(mpCount), volName, "mp")
		mm.volMetaCount.SetWithLabelValues(float64(vol.getDataPartitionsCount()), volName, "dp")
		mm.volMetaCount.SetWithLabelValues(float64(freeListLen), volName, "freeList")
		mm.volMetaCount.SetWithLabelValues(float64(txCnt), volName, txLabel)
	}

	for volName := range deleteVolNames {
		mm.deleteVolMetric(volName)
	}
}

func (mm *monitorMetrics) setBadPartitionMetrics() {
	badMpCount := uint64(0)
	mm.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMpCount += uint64(len(value.([]uint64)))
		return true
	})
	mm.badMpCount.SetWithLabels(float64(badMpCount), map[string]string{"type": "bad_mp"})

	badDpCount := uint64(0)
	mm.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDpCount += uint64(len(value.([]uint64)))
		return true
	})
	mm.badDpCount.SetWithLabels(float64(badDpCount), map[string]string{"type": "bad_dp"})
}

func (mm *monitorMetrics) deleteVolMetric(volName string) {
	mm.volTotalSpace.DeleteLabelValues(volName)
	mm.volUsedSpace.DeleteLabelValues(volName)
	mm.volUsage.DeleteLabelValues(volName)
	mm.volMetaCount.DeleteLabelValues(volName, "inode")
	mm.volMetaCount.DeleteLabelValues(volName, "dentry")
	mm.volMetaCount.DeleteLabelValues(volName, "mp")
	mm.volMetaCount.DeleteLabelValues(volName, "dp")
	mm.volMetaCount.DeleteLabelValues(volName, "freeList")
	mm.volMetaCount.DeleteLabelValues(volName, txLabel)
	mm.volStats.Reset()
}

func (mm *monitorMetrics) setMpInconsistentErrorMetric() {
	deleteMps := make(map[string]string)
	for k, v := range mm.inconsistentMps {
		deleteMps[k] = v
		delete(mm.inconsistentMps, k)
	}

	vols := mm.cluster.copyVols()
	for _, vol := range vols {
		if (vol.Status == proto.VolStatusMarkDelete && !vol.Forbidden) || (vol.Status == proto.VolStatusMarkDelete && vol.Forbidden && time.Until(vol.DeleteExecTime) <= 0) ||
			vol.isInitializingOrInitFailed() {
			continue
		}
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			if mp.IsRecover.Load() || mp.EqualCheckPass {
				continue
			}
			idStr := strconv.FormatUint(mp.PartitionID, 10)
			mm.metaEqualCheckFail.SetWithLabelValues(1, vol.Name, idStr)
			mm.inconsistentMps[idStr] = vol.Name
			log.LogWarnf("setMpInconsistentErrorMetric.mp %v SetWithLabelValues id %v vol %v", mp.PartitionID, idStr, vol.Name)
			delete(deleteMps, idStr)
		}
		vol.mpsLock.RUnlock()
	}

	for k, v := range deleteMps {
		mm.metaEqualCheckFail.DeleteLabelValues(v, k)
	}
}

func (mm *monitorMetrics) setDiskErrorMetric() {
	// key: addr_diskpath, val: addr
	deleteBadDisks := make(map[string]string)
	for k, v := range mm.badDisks {
		deleteBadDisks[k] = v
		delete(mm.badDisks, k)
		mm.diskError.DeleteLabelValues(v, k)
	}

	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		for _, badDisk := range dataNode.BadDisks {
			key := fmt.Sprintf("%s_%s", dataNode.Addr, badDisk)
			mm.diskError.SetWithLabelValues(1, dataNode.Addr, key)
			mm.badDisks[key] = dataNode.Addr
		}
		return true
	})
}

func (mm *monitorMetrics) setFlashNodesDiskErrorMetric() {
	// key: addr_diskpath, val: addr
	deleteBadDisks := make(map[string]string)
	for k, v := range mm.flashNodesBadDisks {
		deleteBadDisks[k] = v
		delete(mm.flashNodesBadDisks, k)
		mm.flashNodesDiskError.DeleteLabelValues(v, k)
	}

	mm.cluster.flashNodeTopo.Range(func(key, value interface{}) bool {
		if value == nil {
			return true
		}
		topo, ok := value.(*flashgroupmanager.FlashNodeTopology)
		if !ok {
			return true
		}
		infos := topo.BadDiskInfos()
		for _, info := range infos {
			key := fmt.Sprintf("%s_%s", info.Addr, info.DiskPath)
			mm.flashNodesDiskError.SetWithLabelValues(1, info.Addr, key)
			mm.flashNodesBadDisks[key] = info.Addr
		}
		return true
	})
}

func (mm *monitorMetrics) setDiskLostMetric() {
	mm.diskLost.Reset()

	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		for _, lostDisk := range dataNode.LostDisks {
			key := fmt.Sprintf("%s_%s", dataNode.Addr, lostDisk)
			mm.diskLost.SetWithLabelValues(1, dataNode.Addr, key)
		}
		return true
	})
}

func (mm *monitorMetrics) setDpUnableDecommissionMetric() {
	dpUnableDecommissionCount := 0
	vols := mm.cluster.allVols()
	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if dp.GetDecommissionStatus() == DecommissionFail &&
				strings.Contains(dp.DecommissionErrorMessage, proto.ErrAllReplicaUnavailable.Error()) && !dp.IsDiscard {
				dpUnableDecommissionCount++
			}
		}
	}
	mm.dpUnableDecommissionCount.Set(float64(dpUnableDecommissionCount))
}

func (mm *monitorMetrics) setDpNoSamePeerMetric() {
	mm.dpNoSamePeer.Reset()

	mm.cluster.NoSamePeerDps.Range(func(key, value interface{}) bool {
		dpId := key.(uint64)
		idStr := strconv.FormatUint(dpId, 10)
		mm.dpNoSamePeer.SetWithLabelValues(1, idStr)
		return true
	})
}

func (mm *monitorMetrics) setMpNoSamePeerMetric() {
	mm.mpNoSamePeer.Reset()

	mm.cluster.NoSamePeerMps.Range(func(key, value interface{}) bool {
		mpId := key.(uint64)
		idStr := strconv.FormatUint(mpId, 10)
		mm.mpNoSamePeer.SetWithLabelValues(1, idStr)
		return true
	})
}

func (mm *monitorMetrics) setBadDiskDecommissionTimeOverLimit() {
	mm.badDiskDecommissionTimeOverLimit.Reset()

	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		for _, badDiskStat := range dataNode.BadDiskStats {
			_, isSuccess := dataNode.DecommissionSuccessDisks.Load(badDiskStat.DiskPath)
			if !badDiskStat.FirstReportTime.IsZero() && time.Since(badDiskStat.FirstReportTime) > 24*time.Hour && !isSuccess {
				mm.badDiskDecommissionTimeOverLimit.SetWithLabelValues(time.Since(badDiskStat.FirstReportTime).Hours(), dataNode.Addr, badDiskStat.DiskPath,
					badDiskStat.FirstReportTime.Format("2006-01-02 15:04:05"))
			}
		}
		return true
	})
}

func (mm *monitorMetrics) setDiskDecommissionedMetric() {
	mm.diskDecommissionSuccess.Reset()

	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		if dataNode.GetDecommissionStatus() != DecommissionInitial &&
			dataNode.GetDecommissionStatus() != DecommissionFail {
			return true
		}
		successDisks := dataNode.getDecommissionSuccessDisks()
		successDiskMap := make(map[string]struct{})
		for _, disk := range successDisks {
			successDiskMap[disk] = struct{}{}
		}
		for _, badDisk := range dataNode.BadDisks {
			if _, exists := successDiskMap[badDisk]; exists {
				key := fmt.Sprintf("%s_%s", dataNode.Addr, badDisk)
				mm.diskDecommissionSuccess.SetWithLabelValues(1, dataNode.Addr, key)
			}
		}
		return true
	})
}

func (mm *monitorMetrics) updateMetaNodesStat() {
	var inactiveMetaNodesCount int64

	mm.cluster.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode, ok := node.(*MetaNode)
		if !ok {
			return true
		}
		if !metaNode.IsActive {
			inactiveMetaNodesCount++
			mm.InactiveMetaNodeInfo.SetWithLabelValues(1, mm.cluster.Name, metaNode.Addr)
		} else {
			mm.InactiveMetaNodeInfo.DeleteLabelValues(mm.cluster.Name, metaNode.Addr)
		}

		zone := metaNode.ZoneName
		setId := strconv.Itoa(int(metaNode.NodeSetID))
		media := "default"
		mAddr := metaNode.Addr
		rack := metaNode.Rack

		writable := "false"
		if metaNode.IsWriteAble() {
			writable = "true"
		}
		alloc := "false"
		if metaNode.PartitionCntLimited() && metaNode.IsWriteAble() {
			alloc = "true"
		}

		mm.nodeStat.Delete(map[string]string{"addr": mAddr})

		mm.nodeStat.SetWithLabelValues(metaNode.Ratio, MetricRoleMetaNode, mAddr, "usageRatio", zone, setId, media, writable, alloc, rack, "")
		mm.nodeStat.SetWithLabelValues(float64(metaNode.Total), MetricRoleMetaNode, mAddr, "memTotal", zone, setId, media, writable, alloc, rack, "")
		mm.nodeStat.SetWithLabelValues(float64(metaNode.Used), MetricRoleMetaNode, mAddr, "memUsed", zone, setId, media, writable, alloc, rack, "")
		mm.nodeStat.SetWithLabelValues(float64(metaNode.MetaPartitionCount), MetricRoleMetaNode, mAddr, "mpCount", zone, setId, media, writable, alloc, rack, "")
		mm.nodeStat.SetWithLabelValues(float64(metaNode.Threshold), MetricRoleMetaNode, mAddr, "threshold", zone, setId, media, writable, alloc, rack, "")
		mm.nodeStat.SetBoolWithLabelValues(metaNode.IsWriteAble(), MetricRoleMetaNode, mAddr, "writable", zone, setId, media, writable, alloc, rack, "")
		mm.nodeStat.SetBoolWithLabelValues(metaNode.IsRocksdbWriteAble(), MetricRoleMetaNode, metaNode.Addr, "rocksdbWritable", zone, setId, media, writable, alloc, rack, "")
		mm.nodeStat.SetBoolWithLabelValues(metaNode.IsActive, MetricRoleMetaNode, mAddr, "active", zone, setId, media, writable, alloc, rack, "")
		mm.nodeStat.SetBoolWithLabelValues(metaNode.PartitionCntLimited() && metaNode.IsWriteAble(), MetricRoleMetaNode, mAddr, "alloc", zone, setId, media, writable, alloc, rack, "")

		return true
	})

	mm.metaNodesInactive.Set(float64(inactiveMetaNodesCount))
}

func (mm *monitorMetrics) updateDataNodesStat() {
	var inactiveDataNodesCount uint64

	log.LogInfof("action[updateDataNodesStat] dataNodes count[%v]", mm.cluster.dataNodeCount())

	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		if !dataNode.isActive {
			inactiveDataNodesCount++
			mm.InactiveDataNodeInfo.SetWithLabelValues(1, mm.cluster.Name, dataNode.Addr)
		} else {
			mm.InactiveDataNodeInfo.DeleteLabelValues(mm.cluster.Name, dataNode.Addr)
		}

		zone := dataNode.ZoneName
		setId := strconv.Itoa(int(dataNode.NodeSetID))
		media := proto.MediaTypeString(dataNode.MediaType)
		dAddr := dataNode.Addr
		rack := dataNode.Rack
		poolId := mm.cluster.getPoolNameById(dataNode.PoolId)

		writable := "false"
		if dataNode.IsWriteAble() {
			writable = "true"
		}
		alloc := "false"
		if dataNode.canAllocDp() {
			alloc = "true"
		}

		mm.nodeStat.Delete(map[string]string{"addr": dAddr})

		mm.nodeStat.SetWithLabelValues(float64(dataNode.DataPartitionCount), MetricRoleDataNode, dAddr, "dpCount", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetWithLabelValues(float64(dataNode.Total), MetricRoleDataNode, dAddr, "diskTotal", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetWithLabelValues(float64(dataNode.Used), MetricRoleDataNode, dAddr, "diskUsed", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetWithLabelValues(float64(dataNode.AvailableSpace), MetricRoleDataNode, dAddr, "diskAvail", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetWithLabelValues(dataNode.UsageRatio, MetricRoleDataNode, dAddr, "usageRatio", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetWithLabelValues(float64(len(dataNode.BadDisks)), MetricRoleDataNode, dAddr, "badDiskCount", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetWithLabelValues(float64(len(dataNode.LostDisks)), MetricRoleDataNode, dAddr, "lostDiskCount", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetBoolWithLabelValues(dataNode.isActive, MetricRoleDataNode, dAddr, "active", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetBoolWithLabelValues(dataNode.IsWriteAble(), MetricRoleDataNode, dAddr, "writable", zone, setId, media, writable, alloc, rack, poolId)
		mm.nodeStat.SetBoolWithLabelValues(dataNode.canAllocDp(), MetricRoleDataNode, dAddr, "canAlloc", zone, setId, media, writable, alloc, rack, poolId)

		return true
	})
	mm.dataNodesInactive.Set(float64(inactiveDataNodesCount))
}

func (mm *monitorMetrics) updateMastersStat() {
	mm.InactiveMasterInfo.Reset()

	InactiveNodeIds := mm.cluster.server.raftStore.RaftServer().GetUnreachable(1)
	mm.mastersInactive.Set(float64(len(InactiveNodeIds)))

	masterNodes := mm.cluster.allMasterNodes()
	masterNodeMap := make(map[uint64]proto.NodeView, len(masterNodes))
	for _, node := range masterNodes {
		masterNodeMap[node.ID] = node
	}

	for _, id := range InactiveNodeIds {
		if node, exists := masterNodeMap[id]; exists {
			mm.InactiveMasterInfo.SetWithLabelValues(1, mm.cluster.Name, node.Addr)
		}
	}
}

func (mm *monitorMetrics) setNotWritableMetaNodesCount() {
	var notWritabelMetaNodesCount int64
	mm.cluster.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode, ok := node.(*MetaNode)
		if !ok {
			return true
		}
		if !metaNode.IsWriteAble() {
			notWritabelMetaNodesCount++
		}
		return true
	})
	mm.metaNodesNotWritable.Set(float64(notWritabelMetaNodesCount))
}

func (mm *monitorMetrics) setNotWritableDataNodesCount() {
	var notWritabelDataNodesCount int64
	var allocableCnt int64
	meidaMap := map[string]map[string]int{}

	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}

		media := proto.MediaTypeString(dataNode.MediaType)
		mmap := meidaMap[media]
		if len(mmap) == 0 {
			mmap = make(map[string]int)
			meidaMap[media] = mmap
		}
		mmap["totalCnt"]++

		if dataNode.canAllocDp() {
			allocableCnt++
			mmap["allocCnt"]++
		}

		if !dataNode.IsWriteAble() {
			notWritabelDataNodesCount++
			mmap["notWritable"]++
		}
		return true
	})
	mm.dataNodesNotWritable.Set(float64(notWritabelDataNodesCount))
	mm.dataNodesAllocable.Set(float64(allocableCnt))

	for media, m := range meidaMap {
		if len(m) == 0 {
			continue
		}
		for t, c := range m {
			mm.dataNodeStat.SetWithLabelValues(float64(c), media, t)
		}
	}
}

func (mm *monitorMetrics) deleteS3LcVolMetric(id string) {
	mm.lcVolStatus.Reset()
	mm.lcVolScanned.Reset()
	mm.lcVolExpired.Reset()
	mm.lcVolMigrateBytes.Reset()
	mm.lcVolError.Reset()
}

func (mm *monitorMetrics) setLcMetrics() {
	lcTaskStatus := mm.cluster.lcMgr.lcRuleTaskStatus
	volumeScanStatistics := make(map[string]proto.LcNodeRuleTaskStatistics)
	lcTaskStatus.RLock()
	for id, r := range lcTaskStatus.Results {
		if r.Done {
			mm.lcVolStatus.SetWithLabelValues(0, id)
			volumeScanStatistics[id] = proto.LcNodeRuleTaskStatistics{}
		} else {
			mm.lcVolStatus.SetWithLabelValues(1, id)
			volumeScanStatistics[id] = r.LcNodeRuleTaskStatistics
		}
	}
	lcTaskStatus.RUnlock()
	for id, stat := range volumeScanStatistics {
		mm.lcId[id] = struct{}{}
		mm.lcVolScanned.SetWithLabelValues(float64(stat.TotalFileScannedNum), id, "file")
		mm.lcVolScanned.SetWithLabelValues(float64(stat.TotalDirScannedNum), id, "dir")

		mm.lcVolExpired.SetWithLabelValues(float64(stat.ExpiredDeleteNum), id, "delete")
		mm.lcVolExpired.SetWithLabelValues(float64(stat.ExpiredMToHddNum), id, "hdd")
		mm.lcVolExpired.SetWithLabelValues(float64(stat.ExpiredMNum), id, "expired")
		mm.lcVolExpired.SetWithLabelValues(float64(stat.ExpiredMToBlobstoreNum), id, "blobstore")
		mm.lcVolExpired.SetWithLabelValues(float64(stat.ExpiredSkipNum), id, "skip")

		mm.lcVolMigrateBytes.SetWithLabelValues(float64(stat.ExpiredMToHddBytes), id, "hdd")
		mm.lcVolMigrateBytes.SetWithLabelValues(float64(stat.ExpiredMBytes), id, "expiredBytes")
		mm.lcVolMigrateBytes.SetWithLabelValues(float64(stat.ExpiredMToBlobstoreBytes), id, "blobstore")

		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorDeleteNum), id, "delete")
		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorMToHddNum), id, "hdd")
		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorMToBlobstoreNum), id, "blobstore")
		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorReadDirNum), id, "readdir")
		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorMNum), id, "error")
	}
}

func (mm *monitorMetrics) clearLcMetrics() {
	for vol := range mm.lcId {
		mm.deleteS3LcVolMetric(vol)
		delete(mm.lcId, vol)
	}
}

func (mm *monitorMetrics) clearVolMetrics() {
	mm.cluster.volStatInfo.Range(func(key, value interface{}) bool {
		if volName, ok := key.(string); ok {
			mm.deleteVolMetric(volName)
		}
		return true
	})
}

func (mm *monitorMetrics) clearDiskErrMetrics() {
	for k, v := range mm.badDisks {
		mm.diskError.DeleteLabelValues(v, k)
	}
}

func (mm *monitorMetrics) clearFlashNodesDiskErrMetrics() {
	for k, v := range mm.flashNodesBadDisks {
		mm.flashNodesDiskError.DeleteLabelValues(v, k)
	}
}

func (mm *monitorMetrics) resetFollowerMetrics() {
	mm.masterNoCache.GaugeVec.Reset()
	mm.masterNoLeader.Set(0)
	mm.masterSnapshot.Set(0)
}

func (mm *monitorMetrics) resetAllLeaderMetrics() {
	mm.clearVolMetrics()
	mm.clearDiskErrMetrics()
	mm.clearFlashNodesDiskErrMetrics()
	mm.metaEqualCheckFail.Reset()
	mm.clearLcMetrics()

	mm.partitionCreate.Reset()
	mm.dataNodesCount.Set(0)
	mm.metaNodesCount.Set(0)
	mm.lcNodesCount.Set(0)
	mm.volCount.Set(0)
	mm.dataNodeStat.Reset()
	mm.dataNodesTotal.Set(0)
	mm.dataNodesUsed.Set(0)
	mm.dataNodeIncreased.Set(0)
	mm.metaNodesTotal.Set(0)
	mm.metaNodesUsed.Set(0)
	mm.metaNodesIncreased.Set(0)
	mm.nodeStat.Reset()
	// mm.diskError.Set(0)
	mm.diskLost.Reset()
	mm.dpUnableDecommissionCount.Set(0)
	mm.dpNoSamePeer.Reset()
	mm.mpNoSamePeer.Reset()
	mm.badDiskDecommissionTimeOverLimit.Reset()
	mm.diskDecommissionSuccess.Reset()
	mm.dataNodesInactive.Set(0)
	mm.metaNodesInactive.Set(0)
	mm.mastersInactive.Set(0)

	mm.InactiveMasterInfo.Reset()

	mm.dataNodesNotWritable.Set(0)
	mm.dataNodesAllocable.Set(0)
	mm.metaNodesNotWritable.Set(0)

	mm.MpMissingLeaderCount.Set(0)
	mm.MpMissingReplicaCount.Set(0)
	mm.MpFailedRecoveryCount.Set(0)
	mm.ReplicaMissingDPCount.Reset()
	mm.DpMissingLeaderCount.Reset()

	mm.ssdNodeSetUnbalancedDPs.Set(0)
	mm.ssdRackConflictDPs.Set(0)
	mm.hddNodeSetUnbalancedDPs.Set(0)
	mm.hddRackConflictDPs.Set(0)
}

func (mm *monitorMetrics) setNotRocksdbWritableMetaNodesCount() {
	var count int64
	mm.cluster.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode, ok := node.(*MetaNode)
		if !ok {
			return true
		}
		if !metaNode.IsRocksdbWriteAble() {
			count++
		}
		return true
	})
	mm.metaNodesNotRocksdbWritable.Set(float64(count))
}

func (mm *monitorMetrics) setDistributionOptimizationMetrics() {
	ssdNodeSetUnbalancedDPs := mm.cluster.SSDNodeSetUnbalancedDPs.Load()
	ssdRackConflictDPs := mm.cluster.SSDRackConflictDPs.Load()
	hddNodeSetUnbalancedDPs := mm.cluster.HDDNodeSetUnbalancedDPs.Load()
	hddRackConflictDPs := mm.cluster.HDDRackConflictDPs.Load()

	mm.ssdNodeSetUnbalancedDPs.Set(float64(ssdNodeSetUnbalancedDPs))
	mm.ssdRackConflictDPs.Set(float64(ssdRackConflictDPs))
	mm.hddNodeSetUnbalancedDPs.Set(float64(hddNodeSetUnbalancedDPs))
	mm.hddRackConflictDPs.Set(float64(hddRackConflictDPs))

	log.LogDebugf("action[setDistributionOptimizationMetrics] SSD: %d/%d, HDD: %d/%d",
		ssdNodeSetUnbalancedDPs, ssdRackConflictDPs,
		hddNodeSetUnbalancedDPs, hddRackConflictDPs)
	// mm.nodeStat.Reset()
}
