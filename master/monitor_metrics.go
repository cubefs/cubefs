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

	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

//metrics
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
	MetricVolCount             = "vol_count"
	MetricVolTotalGB           = "vol_total_GB"
	MetricVolUsedGB            = "vol_used_GB"
	MetricVolUsageGB           = "vol_usage_ratio"
	MetricVolMetaCount         = "vol_meta_count"
	MetricBadMpCount           = "bad_mp_count"
	MetricBadDpCount           = "bad_dp_count"
	MetricDiskError            = "disk_error"
	MetricDataNodesInactive    = "dataNodes_inactive"
	MetricInactiveDataNodeInfo = "inactive_dataNodes_info"
	MetricMetaNodesInactive    = "metaNodes_inactive"
	MetricDataNodesNotWritable = "dataNodes_not_writable"
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
	MetricDataPartitionCount       = "dataPartition_count"
	MetricReplicaMissingDPCount    = "replica_missing_dp_count"
	MetricDpMissingLeaderCount     = "dp_missing_Leader_count"
	MetricMpMissingLeaderCount     = "mp_missing_Leader_count"
	MetricDataNodesetInactiveCount = "data_nodeset_inactive_count"
	MetricMetaNodesetInactiveCount = "meta_nodeset_inactive_count"
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
	dataNodesNotWritable     *exporter.Gauge
	metaNodesNotWritable     *exporter.Gauge
	dataNodesInactive        *exporter.Gauge
	InactiveDataNodeInfo     *exporter.GaugeVec
	metaNodesInactive        *exporter.Gauge
	InactiveMataNodeInfo     *exporter.GaugeVec
	dataPartitionCount       *exporter.Gauge
	ReplicaMissingDPCount    *exporter.Gauge
	DpMissingLeaderCount     *exporter.Gauge
	MpMissingLeaderCount     *exporter.Gauge
	dataNodesetInactiveCount *exporter.GaugeVec
	metaNodesetInactiveCount *exporter.GaugeVec
	metaEqualCheckFail       *exporter.GaugeVec
	masterNoLeader           *exporter.Gauge
	masterNoCache            *exporter.GaugeVec
	masterSnapshot           *exporter.Gauge

	volNames                      map[string]struct{}
	badDisks                      map[string]string
	nodesetInactiveDataNodesCount map[uint64]int64
	nodesetInactiveMetaNodesCount map[uint64]int64
	inconsistentMps               map[string]string
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{cluster: c,
		volNames:                      make(map[string]struct{}),
		badDisks:                      make(map[string]string),
		nodesetInactiveDataNodesCount: make(map[uint64]int64),
		nodesetInactiveMetaNodesCount: make(map[uint64]int64),
		inconsistentMps:               make(map[string]string),
	}
}

type warningMetrics struct {
	cluster               *Cluster
	missingDp             *exporter.GaugeVec
	dpNoLeader            *exporter.GaugeVec
	missingMp             *exporter.GaugeVec
	mpNoLeader            *exporter.GaugeVec
	dpMutex               sync.Mutex
	mpMutex               sync.Mutex
	dpNoLeaderInfo        map[uint64]int64
	mpNoLeaderInfo        map[uint64]int64
	dpMissingReplicaMutex sync.Mutex
	mpMissingReplicaMutex sync.Mutex
	dpMissingReplicaInfo  map[string]string
	mpMissingReplicaInfo  map[string]string
}

func newWarningMetrics(c *Cluster) *warningMetrics {
	return &warningMetrics{
		cluster:              c,
		missingDp:            exporter.NewGaugeVec(MetricMissingDp, "", []string{"clusterName", "partitionID", "addr"}),
		dpNoLeader:           exporter.NewGaugeVec(MetricDpNoLeader, "", []string{"clusterName", "partitionID"}),
		missingMp:            exporter.NewGaugeVec(MetricMissingMp, "", []string{"clusterName", "partitionID", "addr"}),
		mpNoLeader:           exporter.NewGaugeVec(MetricMpNoLeader, "", []string{"clusterName", "partitionID"}),
		dpNoLeaderInfo:       make(map[uint64]int64),
		mpNoLeaderInfo:       make(map[uint64]int64),
		dpMissingReplicaInfo: make(map[string]string),
		mpMissingReplicaInfo: make(map[string]string),
	}
}

func (m *warningMetrics) reset() {
	m.dpMutex.Lock()
	for dp := range m.dpNoLeaderInfo {
		m.dpNoLeader.DeleteLabelValues(m.cluster.Name, strconv.FormatUint(dp, 10))
		delete(m.dpNoLeaderInfo, dp)
	}
	m.dpMutex.Unlock()

	m.mpMutex.Lock()
	for mp := range m.mpNoLeaderInfo {
		m.mpNoLeader.DeleteLabelValues(m.cluster.Name, strconv.FormatUint(mp, 10))
		delete(m.mpNoLeaderInfo, mp)
	}
	m.mpMutex.Unlock()

	m.dpMissingReplicaMutex.Lock()
	for id, addr := range m.dpMissingReplicaInfo {
		m.missingDp.DeleteLabelValues(m.cluster.Name, id, addr)
		delete(m.dpMissingReplicaInfo, id)
	}
	m.dpMissingReplicaMutex.Unlock()

	m.mpMissingReplicaMutex.Lock()
	for id, addr := range m.mpMissingReplicaInfo {
		m.missingMp.DeleteLabelValues(m.cluster.Name, id, addr)
		delete(m.mpMissingReplicaInfo, id)
	}
	m.mpMissingReplicaMutex.Unlock()
}

//leader only
func (m *warningMetrics) WarnMissingDp(clusterName, addr string, partitionID uint64, report bool) {
	m.dpMissingReplicaMutex.Lock()
	defer m.dpMissingReplicaMutex.Unlock()
	if clusterName != m.cluster.Name {
		return
	}
	id := strconv.FormatUint(partitionID, 10)
	if !report {
		m.missingDp.DeleteLabelValues(clusterName, id, addr)
		delete(m.dpMissingReplicaInfo, id)
		return
	}

	m.missingDp.SetWithLabelValues(1, clusterName, id, addr)
	m.dpMissingReplicaInfo[id] = addr
}

//func (m *warningMetrics) WarnDpNoLeader(clusterName string, partitionID uint64) {
//	m.dpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
//}

//leader only
func (m *warningMetrics) WarnDpNoLeader(clusterName string, partitionID uint64, report bool) {
	if clusterName != m.cluster.Name {
		return
	}

	m.dpMutex.Lock()
	defer m.dpMutex.Unlock()
	t, ok := m.dpNoLeaderInfo[partitionID]
	if !report {
		if ok {
			delete(m.dpNoLeaderInfo, partitionID)
			m.dpNoLeader.DeleteLabelValues(clusterName, strconv.FormatUint(partitionID, 10))
		}
		return
	}

	now := time.Now().Unix()
	if !ok {
		m.dpNoLeaderInfo[partitionID] = now
		return
	}
	if now-t > m.cluster.cfg.NoLeaderReportInterval {
		m.dpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
		m.dpNoLeaderInfo[partitionID] = now
	}
}

//leader only
func (m *warningMetrics) WarnMissingMp(clusterName, addr string, partitionID uint64, report bool) {
	m.mpMissingReplicaMutex.Lock()
	defer m.mpMissingReplicaMutex.Unlock()
	if clusterName != m.cluster.Name {
		return
	}

	id := strconv.FormatUint(partitionID, 10)
	if !report {
		m.missingMp.DeleteLabelValues(clusterName, id, addr)
		delete(m.mpMissingReplicaInfo, id)
		return
	}
	m.missingMp.SetWithLabelValues(1, clusterName, id, addr)
	m.mpMissingReplicaInfo[id] = addr
}

//func (m *warningMetrics) WarnMpNoLeader(clusterName string, partitionID uint64) {
//	m.mpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
//}

//leader only
func (m *warningMetrics) WarnMpNoLeader(clusterName string, partitionID uint64, report bool) {
	if clusterName != m.cluster.Name {
		return
	}
	m.mpMutex.Lock()
	defer m.mpMutex.Unlock()
	t, ok := m.mpNoLeaderInfo[partitionID]
	if !report {
		if ok {
			delete(m.mpNoLeaderInfo, partitionID)
			m.mpNoLeader.DeleteLabelValues(clusterName, strconv.FormatUint(partitionID, 10))
		}
		return
	}

	now := time.Now().Unix()

	if !ok {
		m.mpNoLeaderInfo[partitionID] = now
		return
	}

	if now-t > m.cluster.cfg.NoLeaderReportInterval {
		m.mpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
		m.mpNoLeaderInfo[partitionID] = now
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
	mm.dataNodesInactive = exporter.NewGauge(MetricDataNodesInactive)
	mm.InactiveDataNodeInfo = exporter.NewGaugeVec(MetricInactiveDataNodeInfo, "", []string{"clusterName", "addr"})
	mm.metaNodesInactive = exporter.NewGauge(MetricMetaNodesInactive)
	mm.dataNodesNotWritable = exporter.NewGauge(MetricDataNodesNotWritable)
	mm.metaNodesNotWritable = exporter.NewGauge(MetricMetaNodesNotWritable)
	mm.InactiveMataNodeInfo = exporter.NewGaugeVec(MetricInactiveMataNodeInfo, "", []string{"clusterName", "addr"})
	mm.dataPartitionCount = exporter.NewGauge(MetricDataPartitionCount)
	mm.ReplicaMissingDPCount = exporter.NewGauge(MetricReplicaMissingDPCount)
	mm.DpMissingLeaderCount = exporter.NewGauge(MetricDpMissingLeaderCount)
	mm.MpMissingLeaderCount = exporter.NewGauge(MetricMpMissingLeaderCount)
	mm.dataNodesetInactiveCount = exporter.NewGaugeVec(MetricDataNodesetInactiveCount, "", []string{"nodeset"})
	mm.metaNodesetInactiveCount = exporter.NewGaugeVec(MetricMetaNodesetInactiveCount, "", []string{"nodeset"})
	mm.metaEqualCheckFail = exporter.NewGaugeVec(MetricMetaInconsistent, "", []string{"volume", "mpId"})

	mm.masterSnapshot = exporter.NewGauge(MetricMasterSnapshot)
	mm.masterNoLeader = exporter.NewGauge(MetricMasterNoLeader)
	mm.masterNoCache = exporter.NewGaugeVec(MetricMasterNoCache, "", []string{"volName"})
	go mm.statMetrics()
}

func (mm *monitorMetrics) statMetrics() {
	ticker := time.NewTicker(StatPeriod)
	defer func() {
		if err := recover(); err != nil {
			ticker.Stop()
			log.LogErrorf("statMetrics panic,msg:%v", err)
		}
	}()

	for {
		select {
		case <-ticker.C:
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
	mm.setNotWritableDataNodesCount()
	mm.setNotWritableMetaNodesCount()
	mm.setMpInconsistentErrorMetric()
	mm.setInactiveDataNodesCountMetric()
	mm.setInactiveMetaNodesCountMetric()
	mm.setMpAndDpMetrics()
}

func (mm *monitorMetrics) setMpAndDpMetrics() {
	dpCount := 0
	dpMissingReplicaDpCount := 0
	dpMissingLeaderCount := 0
	mpMissingLeaderCount := 0

	vols := mm.cluster.copyVols()
	for _, vol := range vols {
		if vol.Status == markDelete {
			continue
		}
		var dps *DataPartitionMap
		dps = vol.dataPartitions
		dpCount += len(dps.partitions)
		for _, dp := range dps.partitions {
			if dp.ReplicaNum > uint8(len(dp.liveReplicas(defaultDataPartitionTimeOutSec))) {
				dpMissingReplicaDpCount++
			}
			if dp.getLeaderAddr() == "" {
				dpMissingLeaderCount++
			}
		}
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			if !mp.isLeaderExist() {
				mpMissingLeaderCount++
			}
		}
		vol.mpsLock.RUnlock()
	}

	mm.dataPartitionCount.Set(float64(dpCount))
	mm.ReplicaMissingDPCount.Set(float64(dpMissingReplicaDpCount))
	mm.DpMissingLeaderCount.Set(float64(dpMissingLeaderCount))

	mm.MpMissingLeaderCount.Set(float64(mpMissingLeaderCount))
	return
}

func (mm *monitorMetrics) setVolNoCacheMetrics() {
	deleteVolNames := make(map[string]struct{})
	mm.cluster.followerReadManager.rwMutex.RLock()
	defer mm.cluster.followerReadManager.rwMutex.RUnlock()

	for volName, stat := range mm.cluster.followerReadManager.status {
		if stat == true {
			deleteVolNames[volName] = struct{}{}
			log.LogDebugf("setVolNoCacheMetrics deleteVolNames volName %v", volName)
			continue
		}
		log.LogWarnf("setVolNoCacheMetrics volName %v", volName)
		mm.masterNoCache.SetWithLabelValues(1, volName)
	}

	for volName := range deleteVolNames {
		mm.masterNoCache.DeleteLabelValues(volName)
	}
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
		if _, ok := deleteVolNames[volName]; ok {
			delete(deleteVolNames, volName)
		}

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
		for _, mpv := range vol.getMetaPartitionsView() {
			inodeCount += mpv.InodeCount
			dentryCount += mpv.DentryCount
			mpCount += 1
			freeListLen += mpv.FreeListLen
		}
		mm.volMetaCount.SetWithLabelValues(float64(inodeCount), volName, "inode")
		mm.volMetaCount.SetWithLabelValues(float64(dentryCount), volName, "dentry")
		mm.volMetaCount.SetWithLabelValues(float64(mpCount), volName, "mp")
		mm.volMetaCount.SetWithLabelValues(float64(vol.getDataPartitionsCount()), volName, "dp")
		mm.volMetaCount.SetWithLabelValues(float64(freeListLen), volName, "freeList")
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
}

func (mm *monitorMetrics) setMpInconsistentErrorMetric() {
	deleteMps := make(map[string]string)
	for k, v := range mm.inconsistentMps {
		deleteMps[k] = v
		delete(mm.inconsistentMps, k)
	}
	mm.cluster.volMutex.RLock()
	defer mm.cluster.volMutex.RUnlock()

	for _, vol := range mm.cluster.vols {
		if vol.Status == markDelete {
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
	}

	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		for _, badDisk := range dataNode.BadDisks {
			for _, partition := range dataNode.DataPartitionReports {
				if partition.DiskPath == badDisk {
					key := fmt.Sprintf("%s_%s", dataNode.Addr, badDisk)
					mm.diskError.SetWithLabelValues(1, dataNode.Addr, key)
					mm.badDisks[key] = dataNode.Addr
					delete(deleteBadDisks, key)
					break
				}
			}
		}

		return true
	})

	for k, v := range deleteBadDisks {
		mm.diskError.DeleteLabelValues(v, k)
	}
}

func (mm *monitorMetrics) setInactiveMetaNodesCountMetric() {
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

func (mm *monitorMetrics) setInactiveDataNodesCountMetric() {
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
		if !metaNode.isWritable() {
			notWritabelMetaNodesCount++
		}
		return true
	})
	mm.metaNodesNotWritable.Set(float64(notWritabelMetaNodesCount))
}

func (mm *monitorMetrics) setNotWritableDataNodesCount() {
	var notWritabelDataNodesCount int64
	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		if !dataNode.isWriteAble() {
			notWritabelDataNodesCount++
		}
		return true
	})
	mm.dataNodesNotWritable.Set(float64(notWritabelDataNodesCount))
}
func (mm *monitorMetrics) clearInconsistentMps() {
	for k := range mm.inconsistentMps {
		mm.dataNodesetInactiveCount.DeleteLabelValues(k)
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
func (mm *monitorMetrics) resetFollowerMetrics() {
	mm.masterNoCache.GaugeVec.Reset()
	mm.masterNoLeader.Set(0)
	mm.masterSnapshot.Set(0)
}

func (mm *monitorMetrics) resetAllLeaderMetrics() {
	mm.clearVolMetrics()
	mm.clearDiskErrMetrics()
	mm.clearInactiveMetaNodesCountMetric()
	mm.clearInactiveDataNodesCountMetric()
	mm.clearInconsistentMps()

	mm.dataNodesCount.Set(0)
	mm.metaNodesCount.Set(0)
	mm.volCount.Set(0)
	mm.dataNodesTotal.Set(0)
	mm.dataNodesUsed.Set(0)
	mm.dataNodeIncreased.Set(0)
	mm.metaNodesTotal.Set(0)
	mm.metaNodesUsed.Set(0)
	mm.metaNodesIncreased.Set(0)
	//mm.diskError.Set(0)
	mm.dataNodesInactive.Set(0)
	mm.metaNodesInactive.Set(0)
	mm.dataNodesNotWritable.Set(0)
	mm.metaNodesNotWritable.Set(0)
	mm.dataPartitionCount.Set(0)
	mm.ReplicaMissingDPCount.Set(0)
	mm.MpMissingLeaderCount.Set(0)
	mm.DpMissingLeaderCount.Set(0)
}
