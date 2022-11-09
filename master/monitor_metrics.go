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

	MetricMissingDp  = "missing_dp"
	MetricDpNoLeader = "dp_no_leader"
	MetricMissingMp  = "missing_mp"
	MetricMpNoLeader = "mp_no_leader"
	MetricDataPartitionCount       = "dataPartition_count"
	MetricReplicaMissingDPCount    = "replica_missing_dp_count"
	MetricDataNodesetInactiveCount = "data_nodeset_inactive_count"
	MetricMetaNodesetInactiveCount = "meta_nodeset_inactive_count"
)

var WarnMetrics *warningMetrics

type monitorMetrics struct {
	cluster              *Cluster
	dataNodesCount       *exporter.Gauge
	metaNodesCount       *exporter.Gauge
	volCount             *exporter.Gauge
	dataNodesTotal       *exporter.Gauge
	dataNodesUsed        *exporter.Gauge
	dataNodeIncreased    *exporter.Gauge
	metaNodesTotal       *exporter.Gauge
	metaNodesUsed        *exporter.Gauge
	metaNodesIncreased   *exporter.Gauge
	volTotalSpace        *exporter.GaugeVec
	volUsedSpace         *exporter.GaugeVec
	volUsage             *exporter.GaugeVec
	volMetaCount         *exporter.GaugeVec
	badMpCount           *exporter.Gauge
	badDpCount           *exporter.Gauge
	diskError            *exporter.GaugeVec
	dataNodesNotWritable *exporter.Gauge
	metaNodesNotWritable *exporter.Gauge
	dataNodesInactive    *exporter.Gauge
	InactiveDataNodeInfo *exporter.GaugeVec
	metaNodesInactive    *exporter.Gauge
	InactiveMataNodeInfo *exporter.GaugeVec
	dataPartitionCount       *exporter.Gauge
	ReplicaMissingDPCount    *exporter.Gauge
	dataNodesetInactiveCount *exporter.GaugeVec
	metaNodesetInactiveCount *exporter.GaugeVec

	volNames map[string]struct{}
	badDisks map[string]string
	nodesetInactiveDataNodesCount map[uint64]int64
	nodesetInactiveMetaNodesCount map[uint64]int64
	//volNamesMutex sync.Mutex
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{cluster: c,
		volNames:                      make(map[string]struct{}),
		badDisks:                      make(map[string]string),
		nodesetInactiveDataNodesCount: make(map[uint64]int64),
		nodesetInactiveMetaNodesCount: make(map[uint64]int64),
	}
}

type warningMetrics struct {
	cluster    *Cluster
	missingDp  *exporter.GaugeVec
	dpNoLeader *exporter.GaugeVec
	missingMp  *exporter.GaugeVec
	mpNoLeader *exporter.GaugeVec
}

func newWarningMetrics(c *Cluster) *warningMetrics {
	return &warningMetrics{
		cluster:    c,
		missingDp:  exporter.NewGaugeVec(MetricMissingDp, "", []string{"clusterName", "partitionID", "addr"}),
		dpNoLeader: exporter.NewGaugeVec(MetricDpNoLeader, "", []string{"clusterName", "partitionID"}),
		missingMp:  exporter.NewGaugeVec(MetricMissingMp, "", []string{"clusterName", "partitionID", "addr"}),
		mpNoLeader: exporter.NewGaugeVec(MetricMpNoLeader, "", []string{"clusterName", "partitionID"}),
	}
}

func (m *warningMetrics) WarnMissingDp(clusterName, addr string, partitionID uint64) {
	m.missingDp.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10), addr)
}

func (m *warningMetrics) WarnDpNoLeader(clusterName string, partitionID uint64) {
	m.dpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
}

func (m *warningMetrics) WarnMissingMp(clusterName, addr string, partitionID uint64) {
	m.missingMp.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10), addr)
}

func (m *warningMetrics) WarnMpNoLeader(clusterName string, partitionID uint64) {
	m.mpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
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
	mm.dataNodesetInactiveCount = exporter.NewGaugeVec(MetricDataNodesetInactiveCount, "", []string{"nodeset"})
	mm.metaNodesetInactiveCount = exporter.NewGaugeVec(MetricMetaNodesetInactiveCount, "", []string{"nodeset"})
	go mm.statMetrics()
}

func (mm *monitorMetrics) statMetrics() {
	ticker := time.NewTicker(StatPeriod)
	defer func() {
		if err := recover(); err != nil {
			ticker.Stop()
			log.LogErrorf("statMetrics panic,err[%v]", err)
		}
	}()

	for {
		select {
		case <-ticker.C:
			partition := mm.cluster.partition
			if partition != nil && partition.IsRaftLeader() {
				mm.doStat()
			} else {
				mm.resetAllMetrics()
			}
		}
	}
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
	mm.setInactiveDataNodesCountMetric()
	mm.setInactiveMetaNodesCountMetric()
	mm.setDpMetrics()
}

func (mm *monitorMetrics) setDpMetrics() {
	dpCount := 0
	missingReplicaDpCount := 0
	vols := mm.cluster.copyVols()
	for _, vol := range vols {
		var dps *DataPartitionMap
		dps = vol.dataPartitions
		dpCount += len(dps.partitions)
		for _, dp := range dps.partitions {
			if dp.ReplicaNum > uint8(len(dp.liveReplicas(defaultDataPartitionTimeOutSec))) {
				missingReplicaDpCount++
			}
		}
	}

	mm.dataPartitionCount.Set(float64(dpCount))
	mm.ReplicaMissingDPCount.Set(float64(missingReplicaDpCount))

	return
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

func (mm *monitorMetrics) setDiskErrorMetric() {
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
					mm.diskError.SetWithLabelValues(1, dataNode.Addr, badDisk)
					mm.badDisks[badDisk] = dataNode.Addr
					delete(deleteBadDisks, badDisk)
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
		}
		return true
	})
	mm.metaNodesInactive.Set(float64(inactiveMetaNodesCount))
	for id, count := range mm.nodesetInactiveMetaNodesCount {
		mm.metaNodesetInactiveCount.SetWithLabelValues(float64(count), strconv.FormatUint(id, 10))
	}

	for k, _ := range deleteNodesetCount {
		mm.metaNodesetInactiveCount.DeleteLabelValues(strconv.FormatUint(k, 10))
	}
}

func (mm *monitorMetrics) clearInactiveMetaNodesCountMetric() {
	for k, _ := range mm.nodesetInactiveMetaNodesCount {
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
		}
		return true
	})
	mm.dataNodesInactive.Set(float64(inactiveDataNodesCount))
	for id, count := range mm.nodesetInactiveDataNodesCount {
		mm.dataNodesetInactiveCount.SetWithLabelValues(float64(count), strconv.FormatUint(id, 10))
	}

	for k, _ := range deleteNodesetCount {
		mm.dataNodesetInactiveCount.DeleteLabelValues(strconv.FormatUint(k, 10))
	}
}

func (mm *monitorMetrics) clearInactiveDataNodesCountMetric() {
	for k, _ := range mm.nodesetInactiveDataNodesCount {
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

func (mm *monitorMetrics) resetAllMetrics() {
	mm.clearVolMetrics()
	mm.clearDiskErrMetrics()
	mm.clearInactiveMetaNodesCountMetric()
	mm.clearInactiveDataNodesCountMetric()

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
}
