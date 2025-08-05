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
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// metrics
const (
	StatPeriod                 = time.Minute * time.Duration(1)
	MetricDataNodesUsedGB      = "dataNodes_used_GB"
	MetricDataNodesTotalGB     = "dataNodes_total_GB"
	MetricDataNodesIncreasedGB = "dataNodes_increased_GB"
	MetricMetaNodesUsedGB      = "metaNodes_used_GB"
	MetricMetaNodesTotalGB     = "metaNodes_total_GB"
	MetricMetaNodesIncreasedGB = "metaNodes_increased_GB"
	MetricDataNodesCount       = "dataNodes_count"
	MetricMetaNodesCount       = "metaNodes_count"
	MetricNodeStat             = "node_stat"
	MetricVolCount             = "vol_count"
	MetricVolTotalGB           = "vol_total_GB"
	MetricVolUsedGB            = "vol_used_GB"
	MetricVolUsageGB           = "vol_usage_ratio"
	MetricVolMetaCount         = "vol_meta_count"
	MetricBadMpCount           = "bad_mp_count"
	MetricBadDpCount           = "bad_dp_count"
	MetricDiskError            = "disk_error"
	MetricFlashNodesDiskError  = "flashNodes_disk_error"
	MetricDataNodesInactive    = "dataNodes_inactive"
	MetricInactiveDataNodeInfo = "inactive_dataNodes_info"
	MetricMetaNodesInactive    = "metaNodes_inactive"
	MetricDataNodesNotWritable = "dataNodes_not_writable"
	MetricDataNodesAllocable   = "dataNodes_allocable"
	MetricMetaNodesNotWritable = "metaNodes_not_writable"
	MetricInactiveMataNodeInfo = "inactive_mataNodes_info"
	MetricMetaInconsistent     = "mp_inconsistent"
	MetricMasterNoLeader       = "master_no_leader"
	MetricMasterNoCache        = "master_no_cache"
	MetricMasterSnapshot       = "master_snapshot"

	MetricMissingDp                = "missing_dp"
	MetricDpNoLeader               = "dp_no_leader"
	MetricMissingMp                = "missing_mp"
	MetricMpNoLeader               = "mp_no_leader"
	MetricReplicaMissingDPCount    = "replica_missing_dp_count"
	MetricDpMissingLeaderCount     = "dp_missing_Leader_count"
	MetricMpMissingLeaderCount     = "mp_missing_Leader_count"
	MetricMpMissingReplicaCount    = "mp_missing_Replica_count"
	MetricDataNodesetInactiveCount = "data_nodeset_inactive_count"
	MetricMetaNodesetInactiveCount = "meta_nodeset_inactive_count"

	MetricNodesetMetaTotalGB    = "nodeset_meta_total_GB"
	MetricNodesetMetaUsedGB     = "nodeset_meta_used_GB"
	MetricNodesetMetaUsageRadio = "nodeset_meta_usage_ratio"
	MetricNodesetDataTotalGB    = "nodeset_data_total_GB"
	MetricNodesetDataUsedGB     = "nodeset_data_used_GB"
	MetricNodesetDataUsageRadio = "nodeset_data_usage_ratio"
	MetricNodesetMpReplicaCount = "nodeset_mp_replica_count"
	MetricNodesetDpReplicaCount = "nodeset_dp_replica_count"

	MetricLcNodesCount      = "lc_nodes_count"
	MetricLcVolStatus       = "lc_vol_status"
	MetricLcVolScanned      = "lc_vol_scanned"
	MetricLcVolExpired      = "lc_vol_expired"
	MetricLcVolMigrateBytes = "lc_vol_migrate_bytes"
	MetricLcVolError        = "lc_vol_error"
)

const (
	txLabel = "tx"
)

var WarnMetrics *warningMetrics

type monitorMetrics struct {
	cluster                  *Cluster
	dataNodesCount           *exporter.Gauge
	metaNodesCount           *exporter.Gauge
	volCount                 *exporter.Gauge
	dataNodesTotal           *exporter.Gauge
	dataNodesUsed            *exporter.Gauge
	dataNodeIncreased        *exporter.Gauge
	metaNodesTotal           *exporter.Gauge
	metaNodesUsed            *exporter.Gauge
	metaNodesIncreased       *exporter.Gauge
	volTotalSpace            *exporter.GaugeVec
	volUsedSpace             *exporter.GaugeVec
	volUsage                 *exporter.GaugeVec
	volMetaCount             *exporter.GaugeVec
	badMpCount               *exporter.Gauge
	badDpCount               *exporter.Gauge
	diskError                *exporter.GaugeVec
	flashNodesDiskError      *exporter.GaugeVec
	dataNodesNotWritable     *exporter.Gauge
	dataNodesAllocable       *exporter.Gauge
	metaNodesNotWritable     *exporter.Gauge
	dataNodesInactive        *exporter.Gauge
	InactiveDataNodeInfo     *exporter.GaugeVec
	metaNodesInactive        *exporter.Gauge
	InactiveMataNodeInfo     *exporter.GaugeVec
	ReplicaMissingDPCount    *exporter.GaugeVec
	DpMissingLeaderCount     *exporter.GaugeVec
	MpMissingLeaderCount     *exporter.Gauge
	MpMissingReplicaCount    *exporter.Gauge
	dataNodesetInactiveCount *exporter.GaugeVec
	metaNodesetInactiveCount *exporter.GaugeVec
	metaEqualCheckFail       *exporter.GaugeVec
	masterNoLeader           *exporter.Gauge
	masterNoCache            *exporter.GaugeVec
	masterSnapshot           *exporter.Gauge
	nodesetMetaTotal         *exporter.GaugeVec
	nodesetMetaUsed          *exporter.GaugeVec
	nodesetMetaUsageRatio    *exporter.GaugeVec
	nodesetDataTotal         *exporter.GaugeVec
	nodesetDataUsed          *exporter.GaugeVec
	nodesetDataUsageRatio    *exporter.GaugeVec
	nodesetMpReplicaCount    *exporter.GaugeVec
	nodesetDpReplicaCount    *exporter.GaugeVec
	nodeStat                 *exporter.GaugeVec

	volNames                      map[string]struct{}
	badDisks                      map[string]string
	flashNodesBadDisks            map[string]string
	nodesetInactiveDataNodesCount map[uint64]int64
	nodesetInactiveMetaNodesCount map[uint64]int64
	inconsistentMps               map[string]string
	replicaCntMap                 map[uint64]struct{}
	nodesetIds                    map[uint64]string

	lcNodesCount      *exporter.Gauge
	lcId              map[string]struct{}
	lcVolStatus       *exporter.GaugeVec
	lcVolScanned      *exporter.GaugeVec
	lcVolExpired      *exporter.GaugeVec
	lcVolMigrateBytes *exporter.GaugeVec
	lcVolError        *exporter.GaugeVec
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{
		cluster:                       c,
		volNames:                      make(map[string]struct{}),
		badDisks:                      make(map[string]string),
		flashNodesBadDisks:            make(map[string]string),
		nodesetInactiveDataNodesCount: make(map[uint64]int64),
		nodesetInactiveMetaNodesCount: make(map[uint64]int64),
		inconsistentMps:               make(map[string]string),
		replicaCntMap:                 make(map[uint64]struct{}),
		lcId:                          make(map[string]struct{}),
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
	mm.volMetaCount = exporter.NewGaugeVec(MetricVolMetaCount, "", []string{"volName", "type"})
	mm.badMpCount = exporter.NewGauge(MetricBadMpCount)
	mm.badDpCount = exporter.NewGauge(MetricBadDpCount)
	mm.diskError = exporter.NewGaugeVec(MetricDiskError, "", []string{"addr", "path"})
	mm.flashNodesDiskError = exporter.NewGaugeVec(MetricFlashNodesDiskError, "", []string{"addr", "path"})
	mm.nodeStat = exporter.NewGaugeVec(MetricNodeStat, "", []string{"type", "addr", "stat"})
	mm.dataNodesInactive = exporter.NewGauge(MetricDataNodesInactive)
	mm.InactiveDataNodeInfo = exporter.NewGaugeVec(MetricInactiveDataNodeInfo, "", []string{"clusterName", "addr"})
	mm.metaNodesInactive = exporter.NewGauge(MetricMetaNodesInactive)
	mm.dataNodesNotWritable = exporter.NewGauge(MetricDataNodesNotWritable)
	mm.dataNodesAllocable = exporter.NewGauge(MetricDataNodesAllocable)
	mm.metaNodesNotWritable = exporter.NewGauge(MetricMetaNodesNotWritable)
	mm.InactiveMataNodeInfo = exporter.NewGaugeVec(MetricInactiveMataNodeInfo, "", []string{"clusterName", "addr"})
	mm.ReplicaMissingDPCount = exporter.NewGaugeVec(MetricReplicaMissingDPCount, "", []string{"replicaNum"})
	mm.DpMissingLeaderCount = exporter.NewGaugeVec(MetricDpMissingLeaderCount, "", []string{"replicaNum"})
	mm.MpMissingLeaderCount = exporter.NewGauge(MetricMpMissingLeaderCount)
	mm.MpMissingReplicaCount = exporter.NewGauge(MetricMpMissingReplicaCount)
	mm.dataNodesetInactiveCount = exporter.NewGaugeVec(MetricDataNodesetInactiveCount, "", []string{"nodeset"})
	mm.metaNodesetInactiveCount = exporter.NewGaugeVec(MetricMetaNodesetInactiveCount, "", []string{"nodeset"})
	mm.metaEqualCheckFail = exporter.NewGaugeVec(MetricMetaInconsistent, "", []string{"volume", "mpId"})

	mm.masterSnapshot = exporter.NewGauge(MetricMasterSnapshot)
	mm.masterNoLeader = exporter.NewGauge(MetricMasterNoLeader)
	mm.masterNoCache = exporter.NewGaugeVec(MetricMasterNoCache, "", []string{"volName"})

	mm.nodesetMetaTotal = exporter.NewGaugeVec(MetricNodesetMetaTotalGB, "", []string{"nodeset"})
	mm.nodesetMetaUsed = exporter.NewGaugeVec(MetricNodesetMetaUsedGB, "", []string{"nodeset"})
	mm.nodesetMetaUsageRatio = exporter.NewGaugeVec(MetricNodesetMetaUsageRadio, "", []string{"nodeset"})
	mm.nodesetDataTotal = exporter.NewGaugeVec(MetricNodesetDataTotalGB, "", []string{"nodeset"})
	mm.nodesetDataUsed = exporter.NewGaugeVec(MetricNodesetDataUsedGB, "", []string{"nodeset"})
	mm.nodesetDataUsageRatio = exporter.NewGaugeVec(MetricNodesetDataUsageRadio, "", []string{"nodeset"})
	mm.nodesetMpReplicaCount = exporter.NewGaugeVec(MetricNodesetMpReplicaCount, "", []string{"nodeset"})
	mm.nodesetDpReplicaCount = exporter.NewGaugeVec(MetricNodesetDpReplicaCount, "", []string{"nodeset"})

	mm.lcNodesCount = exporter.NewGauge(MetricLcNodesCount)
	mm.lcVolStatus = exporter.NewGaugeVec(MetricLcVolStatus, "", []string{"id"})
	mm.lcVolScanned = exporter.NewGaugeVec(MetricLcVolScanned, "", []string{"id", "type"})
	mm.lcVolExpired = exporter.NewGaugeVec(MetricLcVolExpired, "", []string{"id", "type"})
	mm.lcVolMigrateBytes = exporter.NewGaugeVec(MetricLcVolMigrateBytes, "", []string{"id", "type"})
	mm.lcVolError = exporter.NewGaugeVec(MetricLcVolError, "", []string{"id", "type"})
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
	mm.dataNodesTotal.Set(float64(mm.cluster.dataNodeStatInfo.TotalGB))
	mm.dataNodesUsed.Set(float64(mm.cluster.dataNodeStatInfo.UsedGB))
	mm.dataNodeIncreased.Set(float64(mm.cluster.dataNodeStatInfo.IncreasedGB))
	mm.metaNodesTotal.Set(float64(mm.cluster.metaNodeStatInfo.TotalGB))
	mm.metaNodesUsed.Set(float64(mm.cluster.metaNodeStatInfo.UsedGB))
	mm.metaNodesIncreased.Set(float64(mm.cluster.metaNodeStatInfo.IncreasedGB))
	mm.setVolMetrics()
	mm.setBadPartitionMetrics()
	mm.setDiskErrorMetric()
	mm.setFlashNodesDiskErrorMetric()
	mm.setNotWritableDataNodesCount()
	mm.setNotWritableMetaNodesCount()
	mm.setMpInconsistentErrorMetric()
	mm.setMpAndDpMetrics()
	mm.setNodesetMetrics()
	mm.setLcMetrics()
	mm.updateDataNodesStat()
	mm.updateMetaNodesStat()
}

func (mm *monitorMetrics) setMpAndDpMetrics() {
	start := time.Now()
	defer func() {
		log.LogInfof("setMpAndDpMetrics: total cost %d ms", time.Since(start).Milliseconds())
	}()

	dpMissingLeaderMap := make(map[uint64]int)
	dpMissingReplicaMap := make(map[uint64]int)

	for num := range mm.replicaCntMap {
		dpMissingLeaderMap[num] = 0
		dpMissingReplicaMap[num] = 0
	}

	mpMissingLeaderCount := 0
	mpMissingReplicaCount := 0

	vols := mm.cluster.copyVols()
	for _, vol := range vols {
		if (vol.Status == proto.VolStatusMarkDelete && !vol.Forbidden) || (vol.Status == proto.VolStatusMarkDelete && vol.Forbidden && vol.DeleteExecTime.Before(time.Now())) {
			continue
		}

		replicaNum := vol.dpReplicaNum
		mm.replicaCntMap[uint64(replicaNum)] = struct{}{}

		dps := vol.dataPartitions
		for _, dp := range dps.partitions {
			if dp.IsDiscard {
				continue
			}

			if replicaNum > uint8(len(dp.liveReplicas(defaultDataPartitionTimeOutSec))) {
				dpMissingReplicaMap[uint64(replicaNum)]++
			}
			if proto.IsNormalDp(dp.PartitionType) && dp.getLeaderAddr() == "" && time.Now().Unix()-dp.LeaderReportTime > mm.cluster.cfg.DpNoLeaderReportIntervalSec {
				dpMissingLeaderMap[uint64(replicaNum)]++
			}
		}
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			if !mp.isLeaderExist() && time.Now().Unix()-mp.LeaderReportTime > mm.cluster.cfg.MpNoLeaderReportIntervalSec {
				mpMissingLeaderCount++
			}
			if len(mp.getActiveAddrs()) < int(mp.ReplicaNum) {
				mpMissingReplicaCount++
			}
		}
		vol.mpsLock.RUnlock()
	}

	for num, cnt := range dpMissingLeaderMap {
		mm.DpMissingLeaderCount.SetWithLabelValues(float64(cnt), strconv.Itoa(int(num)))
	}
	for num, cnt := range dpMissingReplicaMap {
		mm.ReplicaMissingDPCount.SetWithLabelValues(float64(cnt), strconv.Itoa(int(num)))
	}
	mm.MpMissingLeaderCount.Set(float64(mpMissingLeaderCount))
	mm.MpMissingReplicaCount.Set(float64(mpMissingReplicaCount))
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
}

func (mm *monitorMetrics) setMpInconsistentErrorMetric() {
	deleteMps := make(map[string]string)
	for k, v := range mm.inconsistentMps {
		deleteMps[k] = v
		delete(mm.inconsistentMps, k)
	}

	vols := mm.cluster.copyVols()
	for _, vol := range vols {
		if (vol.Status == proto.VolStatusMarkDelete && !vol.Forbidden) || (vol.Status == proto.VolStatusMarkDelete && vol.Forbidden && time.Until(vol.DeleteExecTime) <= 0) {
			continue
		}
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			if mp.IsRecover || mp.EqualCheckPass {
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

	mm.cluster.flashNodeTopo.flashNodeMap.Range(func(addr, node interface{}) bool {
		flashNode, ok := node.(*FlashNode)
		if !ok {
			return true
		}
		for _, disk := range flashNode.DiskStat {
			if disk.Status == proto.Unavailable {
				key := fmt.Sprintf("%s_%s", flashNode.Addr, disk.DataPath)
				mm.flashNodesDiskError.SetWithLabelValues(1, flashNode.Addr, key)
				mm.flashNodesBadDisks[key] = flashNode.Addr
			}
		}
		return true
	})
}

func (mm *monitorMetrics) updateMetaNodesStat() {
	var inactiveMetaNodesCount int64

	deleteNodesetCount := make(map[uint64]int64)
	for k, v := range mm.nodesetInactiveMetaNodesCount {
		deleteNodesetCount[k] = v
		delete(mm.nodesetInactiveMetaNodesCount, k)
	}

	mm.cluster.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode, ok := node.(*MetaNode)
		if !ok {
			return true
		}
		if !metaNode.IsActive {
			inactiveMetaNodesCount++
			mm.InactiveMataNodeInfo.SetWithLabelValues(1, mm.cluster.Name, metaNode.Addr)
			mm.nodesetInactiveMetaNodesCount[metaNode.NodeSetID] = mm.nodesetInactiveMetaNodesCount[metaNode.NodeSetID] + 1
			delete(deleteNodesetCount, metaNode.NodeSetID)
		} else {
			mm.InactiveMataNodeInfo.DeleteLabelValues(mm.cluster.Name, metaNode.Addr)
		}
		mm.nodeStat.SetWithLabelValues(metaNode.Ratio, MetricRoleMetaNode, metaNode.Addr, "usageRatio")
		mm.nodeStat.SetWithLabelValues(float64(metaNode.Total), MetricRoleMetaNode, metaNode.Addr, "memTotal")
		mm.nodeStat.SetWithLabelValues(float64(metaNode.Used), MetricRoleMetaNode, metaNode.Addr, "memUsed")
		mm.nodeStat.SetWithLabelValues(float64(metaNode.MetaPartitionCount), MetricRoleMetaNode, metaNode.Addr, "mpCount")
		mm.nodeStat.SetWithLabelValues(float64(metaNode.Threshold), MetricRoleMetaNode, metaNode.Addr, "threshold")
		mm.nodeStat.SetBoolWithLabelValues(metaNode.IsWriteAble(), MetricRoleMetaNode, metaNode.Addr, "writable")
		mm.nodeStat.SetBoolWithLabelValues(metaNode.IsActive, MetricRoleMetaNode, metaNode.Addr, "active")

		return true
	})

	mm.metaNodesInactive.Set(float64(inactiveMetaNodesCount))
	for id, count := range mm.nodesetInactiveMetaNodesCount {
		mm.metaNodesetInactiveCount.SetWithLabelValues(float64(count), strconv.FormatUint(id, 10))
	}

	for k := range deleteNodesetCount {
		mm.metaNodesetInactiveCount.DeleteLabelValues(strconv.FormatUint(k, 10))
	}
}

func (mm *monitorMetrics) clearInactiveMetaNodesCountMetric() {
	for k := range mm.nodesetInactiveMetaNodesCount {
		mm.metaNodesetInactiveCount.DeleteLabelValues(strconv.FormatUint(k, 10))
	}
}

func (mm *monitorMetrics) updateDataNodesStat() {
	var inactiveDataNodesCount uint64
	deleteNodesetCount := make(map[uint64]int64)
	for k, v := range mm.nodesetInactiveDataNodesCount {
		log.LogErrorf("setInactiveDataNodesCountMetric, init deleteNodesetCount")
		deleteNodesetCount[k] = v
		delete(mm.nodesetInactiveDataNodesCount, k)
	}

	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		if !dataNode.isActive {
			inactiveDataNodesCount++
			mm.InactiveDataNodeInfo.SetWithLabelValues(1, mm.cluster.Name, dataNode.Addr)
			mm.nodesetInactiveDataNodesCount[dataNode.NodeSetID] = mm.nodesetInactiveDataNodesCount[dataNode.NodeSetID] + 1
			delete(deleteNodesetCount, dataNode.NodeSetID)
		} else {
			mm.InactiveDataNodeInfo.DeleteLabelValues(mm.cluster.Name, dataNode.Addr)
		}
		mm.nodeStat.SetWithLabelValues(float64(dataNode.DataPartitionCount), MetricRoleDataNode, dataNode.Addr, "dpCount")
		mm.nodeStat.SetWithLabelValues(float64(dataNode.Total), MetricRoleDataNode, dataNode.Addr, "diskTotal")
		mm.nodeStat.SetWithLabelValues(float64(dataNode.Used), MetricRoleDataNode, dataNode.Addr, "diskUsed")
		mm.nodeStat.SetWithLabelValues(float64(dataNode.AvailableSpace), MetricRoleDataNode, dataNode.Addr, "diskAvail")
		mm.nodeStat.SetWithLabelValues(dataNode.UsageRatio, MetricRoleDataNode, dataNode.Addr, "usageRatio")
		mm.nodeStat.SetWithLabelValues(float64(len(dataNode.BadDisks)), MetricRoleDataNode, dataNode.Addr, "badDiskCount")
		mm.nodeStat.SetBoolWithLabelValues(dataNode.isActive, MetricRoleDataNode, dataNode.Addr, "active")
		mm.nodeStat.SetBoolWithLabelValues(dataNode.IsWriteAble(), MetricRoleDataNode, dataNode.Addr, "writable")
		return true
	})
	mm.dataNodesInactive.Set(float64(inactiveDataNodesCount))
	for id, count := range mm.nodesetInactiveDataNodesCount {
		mm.dataNodesetInactiveCount.SetWithLabelValues(float64(count), strconv.FormatUint(id, 10))
	}

	for k := range deleteNodesetCount {
		mm.dataNodesetInactiveCount.DeleteLabelValues(strconv.FormatUint(k, 10))
	}
}

func (mm *monitorMetrics) clearInactiveDataNodesCountMetric() {
	for k := range mm.nodesetInactiveDataNodesCount {
		mm.dataNodesetInactiveCount.DeleteLabelValues(strconv.FormatUint(k, 10))
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
	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}

		if dataNode.canAllocDp() {
			allocableCnt++
		}

		if !dataNode.IsWriteAble() {
			notWritabelDataNodesCount++
		}
		return true
	})
	mm.dataNodesNotWritable.Set(float64(notWritabelDataNodesCount))
	mm.dataNodesAllocable.Set(float64(allocableCnt))
}

func (mm *monitorMetrics) clearInconsistentMps() {
	for k := range mm.inconsistentMps {
		mm.dataNodesetInactiveCount.DeleteLabelValues(k)
	}
}

func (mm *monitorMetrics) deleteS3LcVolMetric(id string) {
	mm.lcVolStatus.DeleteLabelValues(id)
	mm.lcVolScanned.DeleteLabelValues(id, "file")
	mm.lcVolScanned.DeleteLabelValues(id, "dir")
	mm.lcVolExpired.DeleteLabelValues(id, "delete")
	mm.lcVolExpired.DeleteLabelValues(id, "hdd")
	mm.lcVolExpired.DeleteLabelValues(id, "blobstore")
	mm.lcVolExpired.DeleteLabelValues(id, "skip")
	mm.lcVolMigrateBytes.DeleteLabelValues(id, "hdd")
	mm.lcVolMigrateBytes.DeleteLabelValues(id, "blobstore")
	mm.lcVolError.DeleteLabelValues(id, "delete")
	mm.lcVolError.DeleteLabelValues(id, "hdd")
	mm.lcVolError.DeleteLabelValues(id, "blobstore")
	mm.lcVolError.DeleteLabelValues(id, "readdir")
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
		mm.lcVolExpired.SetWithLabelValues(float64(stat.ExpiredMToBlobstoreNum), id, "blobstore")
		mm.lcVolExpired.SetWithLabelValues(float64(stat.ExpiredSkipNum), id, "skip")
		mm.lcVolMigrateBytes.SetWithLabelValues(float64(stat.ExpiredMToHddBytes), id, "hdd")
		mm.lcVolMigrateBytes.SetWithLabelValues(float64(stat.ExpiredMToBlobstoreBytes), id, "blobstore")
		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorDeleteNum), id, "delete")
		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorMToHddNum), id, "hdd")
		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorMToBlobstoreNum), id, "blobstore")
		mm.lcVolError.SetWithLabelValues(float64(stat.ErrorReadDirNum), id, "readdir")
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

func (mm *monitorMetrics) setNodesetMetrics() {
	deleteNodesetIds := make(map[uint64]string)
	for k, v := range mm.nodesetIds {
		deleteNodesetIds[k] = v
	}
	mm.nodesetIds = make(map[uint64]string)

	zones := mm.cluster.t.getAllZones()
	for _, zone := range zones {
		nodeSets := zone.getAllNodeSet()
		for _, nodeset := range nodeSets {
			var metaTotal, metaUsed, dataTotal, dataUsed uint64
			var mpReplicasCount, dpReplicasCount int
			nodeset.metaNodes.Range(func(key, value interface{}) bool {
				metaNode := value.(*MetaNode)
				metaTotal += metaNode.Total
				metaUsed += metaNode.Used
				mpReplicasCount += metaNode.MetaPartitionCount
				return true
			})
			nodeset.dataNodes.Range(func(ney, value interface{}) bool {
				dataNode := value.(*DataNode)
				dataTotal += dataNode.Total
				dataUsed += dataNode.Used
				dpReplicasCount += int(dataNode.DataPartitionCount)
				return true
			})

			nodesetId := strconv.FormatUint(nodeset.ID, 10)

			mm.nodesetIds[nodeset.ID] = nodesetId
			delete(deleteNodesetIds, nodeset.ID)

			mm.nodesetMetaTotal.SetWithLabelValues(float64(metaTotal)/util.GB, nodesetId)
			mm.nodesetMetaUsed.SetWithLabelValues(float64(metaUsed)/util.GB, nodesetId)
			mm.nodesetDataTotal.SetWithLabelValues(float64(dataTotal)/util.GB, nodesetId)
			mm.nodesetDataUsed.SetWithLabelValues(float64(dataUsed)/util.GB, nodesetId)

			if metaTotal == 0 {
				mm.nodesetMetaUsageRatio.SetWithLabelValues(0, nodesetId)
			} else {
				mm.nodesetMetaUsageRatio.SetWithLabelValues(float64(metaUsed)/float64(metaTotal), nodesetId)
			}
			if dataTotal == 0 {
				mm.nodesetDataUsageRatio.SetWithLabelValues(0, nodesetId)
			} else {
				mm.nodesetDataUsageRatio.SetWithLabelValues(float64(dataUsed)/float64(dataTotal), nodesetId)
			}

			mm.nodesetMpReplicaCount.SetWithLabelValues(float64(mpReplicasCount), nodesetId)
			mm.nodesetDpReplicaCount.SetWithLabelValues(float64(dpReplicasCount), nodesetId)
		}
	}

	for _, v := range deleteNodesetIds {
		mm.deleteNodesetMetric(v)
	}
}

func (mm *monitorMetrics) deleteNodesetMetric(nodesetId string) {
	mm.nodesetMetaTotal.DeleteLabelValues(nodesetId)
	mm.nodesetMetaUsed.DeleteLabelValues(nodesetId)
	mm.nodesetMetaUsageRatio.DeleteLabelValues(nodesetId)
	mm.nodesetDataTotal.DeleteLabelValues(nodesetId)
	mm.nodesetDataUsed.DeleteLabelValues(nodesetId)
	mm.nodesetDataUsageRatio.DeleteLabelValues(nodesetId)
	mm.nodesetMpReplicaCount.DeleteLabelValues(nodesetId)
	mm.nodesetDpReplicaCount.DeleteLabelValues(nodesetId)
}

func (mm *monitorMetrics) clearNodesetMetrics() {
	zones := mm.cluster.t.getAllZones()
	for _, zone := range zones {
		nodeSets := zone.getAllNodeSet()
		for _, nodeset := range nodeSets {
			mm.deleteNodesetMetric(strconv.FormatUint(nodeset.ID, 10))
		}
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
	mm.clearInactiveMetaNodesCountMetric()
	mm.clearInactiveDataNodesCountMetric()
	mm.clearInconsistentMps()
	mm.clearNodesetMetrics()
	mm.clearLcMetrics()

	mm.dataNodesCount.Set(0)
	mm.metaNodesCount.Set(0)
	mm.lcNodesCount.Set(0)
	mm.volCount.Set(0)
	mm.dataNodesTotal.Set(0)
	mm.dataNodesUsed.Set(0)
	mm.dataNodeIncreased.Set(0)
	mm.metaNodesTotal.Set(0)
	mm.metaNodesUsed.Set(0)
	mm.metaNodesIncreased.Set(0)
	// mm.diskError.Set(0)
	mm.dataNodesInactive.Set(0)
	mm.metaNodesInactive.Set(0)

	mm.dataNodesNotWritable.Set(0)
	mm.dataNodesAllocable.Set(0)
	mm.metaNodesNotWritable.Set(0)

	mm.MpMissingLeaderCount.Set(0)
	mm.MpMissingReplicaCount.Set(0)
	for num := range mm.replicaCntMap {
		mm.ReplicaMissingDPCount.DeleteLabelValues(strconv.FormatUint(num, 10))
		mm.DpMissingLeaderCount.DeleteLabelValues(strconv.FormatUint(num, 10))
	}
}
