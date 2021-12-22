// Copyright 2018 The Chubao Authors.
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
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
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
	MetricDiskError            = "disk_error"
	MetricDataNodesInactive    = "dataNodes_inactive"
	MetricMetaNodesInactive    = "metaNodes_inactive"
)

type monitorMetrics struct {
	cluster            *Cluster
	dataNodesCount     *exporter.Gauge
	metaNodesCount     *exporter.Gauge
	volCount           *exporter.Gauge
	dataNodesTotal     *exporter.Gauge
	dataNodesUsed      *exporter.Gauge
	dataNodeIncreased  *exporter.Gauge
	metaNodesTotal     *exporter.Gauge
	metaNodesUsed      *exporter.Gauge
	metaNodesIncreased *exporter.Gauge
	volTotalSpace      *exporter.GaugeVec
	volUsedSpace       *exporter.GaugeVec
	volUsage           *exporter.GaugeVec
	volMetaCount       *exporter.GaugeVec
	diskError          *exporter.GaugeVec
	dataNodesInactive  *exporter.Gauge
	metaNodesInactive  *exporter.Gauge

	volNames map[string]struct{}
	badDisks map[string]string
	//volNamesMutex sync.Mutex
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{cluster: c,
		volNames: make(map[string]struct{}),
		badDisks: make(map[string]string),
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
	mm.diskError = exporter.NewGaugeVec(MetricDiskError, "", []string{"addr", "path"})
	mm.dataNodesInactive = exporter.NewGauge(MetricDataNodesInactive)
	mm.metaNodesInactive = exporter.NewGauge(MetricMetaNodesInactive)
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
	mm.setDiskErrorMetric()
	mm.setInactiveDataNodesCount()
	mm.setInactiveMetaNodesCount()
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

		return true
	})

	for volName, vol := range mm.cluster.allVols() {
		inodeCount := uint64(0)
		dentryCount := uint64(0)
		mpCount := uint64(0)
		for _, mpv := range vol.getMetaPartitionsView() {
			inodeCount += mpv.InodeCount
			dentryCount += mpv.DentryCount
			mpCount += 1
		}
		mm.volMetaCount.SetWithLabelValues(float64(inodeCount), volName, "inode")
		mm.volMetaCount.SetWithLabelValues(float64(dentryCount), volName, "dentry")
		mm.volMetaCount.SetWithLabelValues(float64(mpCount), volName, "mp")
		mm.volMetaCount.SetWithLabelValues(float64(vol.getDataPartitionsCount()), volName, "dp")
	}

	for volName := range deleteVolNames {
		mm.deleteVolMetric(volName)
	}
}

func (mm *monitorMetrics) deleteVolMetric(volName string) {
	mm.volTotalSpace.DeleteLabelValues(volName)
	mm.volUsedSpace.DeleteLabelValues(volName)
	mm.volUsage.DeleteLabelValues(volName)
	mm.volMetaCount.DeleteLabelValues(volName, "inode")
	mm.volMetaCount.DeleteLabelValues(volName, "dentry")
	mm.volMetaCount.DeleteLabelValues(volName, "mp")
	mm.volMetaCount.DeleteLabelValues(volName, "dp")
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

func (mm *monitorMetrics) setInactiveMetaNodesCount() {
	var inactiveMetaNodesCount int64
	mm.cluster.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode, ok := node.(*MetaNode)
		if !ok {
			return true
		}
		if !metaNode.IsActive {
			inactiveMetaNodesCount++
		}
		return true
	})
	mm.metaNodesInactive.Set(float64(inactiveMetaNodesCount))
}

func (mm *monitorMetrics) setInactiveDataNodesCount() {
	var inactiveDataNodesCount int64
	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		if !dataNode.isActive {
			inactiveDataNodesCount++
		}
		return true
	})
	mm.dataNodesInactive.Set(float64(inactiveDataNodesCount))
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
}
