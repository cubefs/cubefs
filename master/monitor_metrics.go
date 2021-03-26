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
	MetricDiskError            = "disk_error"
	MetricDataNodesInactive    = "dataNodes_inactive"
	MetricMetaNodesInactive    = "metaNodes_inactive"
)

type monitorMetrics struct {
	cluster            *Cluster
	dataNodesCount     *exporter.GaugeVec
	metaNodesCount     *exporter.GaugeVec
	volCount           *exporter.GaugeVec
	dataNodesTotal     *exporter.GaugeVec
	dataNodesUsed      *exporter.GaugeVec
	dataNodeIncreased  *exporter.GaugeVec
	metaNodesTotal     *exporter.GaugeVec
	metaNodesUsed      *exporter.GaugeVec
	metaNodesIncreased *exporter.GaugeVec
	volTotalSpace      *exporter.GaugeVec
	volUsedSpace       *exporter.GaugeVec
	volUsage           *exporter.GaugeVec
	diskError          *exporter.GaugeVec
	dataNodesInactive  *exporter.GaugeVec
	metaNodesInactive  *exporter.GaugeVec
	volNames           map[string]struct{}
	badDisks           map[string]string
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{cluster: c,
		volNames: make(map[string]struct{}),
		badDisks: make(map[string]string),
	}
}

func (mm *monitorMetrics) start() {
	mm.dataNodesTotal = exporter.NewGaugeVec(MetricDataNodesTotalGB, "", []string{})
	mm.dataNodesUsed = exporter.NewGaugeVec(MetricDataNodesUsedGB, "", []string{})
	mm.dataNodeIncreased = exporter.NewGaugeVec(MetricDataNodesIncreasedGB, "", []string{})
	mm.metaNodesTotal = exporter.NewGaugeVec(MetricMetaNodesTotalGB, "", []string{})
	mm.metaNodesUsed = exporter.NewGaugeVec(MetricMetaNodesUsedGB, "", []string{})
	mm.metaNodesIncreased = exporter.NewGaugeVec(MetricMetaNodesIncreasedGB, "", []string{})
	mm.dataNodesCount = exporter.NewGaugeVec(MetricDataNodesCount, "", []string{})
	mm.metaNodesCount = exporter.NewGaugeVec(MetricMetaNodesCount, "", []string{})
	mm.volCount = exporter.NewGaugeVec(MetricVolCount, "", []string{})
	mm.volTotalSpace = exporter.NewGaugeVec(MetricVolTotalGB, "", []string{"volName"})
	mm.volUsedSpace = exporter.NewGaugeVec(MetricVolUsedGB, "", []string{"volName"})
	mm.volUsage = exporter.NewGaugeVec(MetricVolUsageGB, "", []string{"volName"})
	mm.diskError = exporter.NewGaugeVec(MetricDiskError, "", []string{"addr", "path"})
	mm.dataNodesInactive = exporter.NewGaugeVec(MetricDataNodesInactive, "", []string{})
	mm.metaNodesInactive = exporter.NewGaugeVec(MetricMetaNodesInactive, "", []string{})
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
	mm.dataNodesCount.SetWithLabelValues(float64(dataNodeCount))
	metaNodeCount := mm.cluster.metaNodeCount()
	mm.metaNodesCount.SetWithLabelValues(float64(metaNodeCount))
	volCount := len(mm.cluster.vols)
	mm.volCount.SetWithLabelValues(float64(volCount))
	mm.dataNodesTotal.SetWithLabelValues(float64(mm.cluster.dataNodeStatInfo.TotalGB))
	mm.dataNodesUsed.SetWithLabelValues(float64(mm.cluster.dataNodeStatInfo.UsedGB))
	mm.dataNodeIncreased.SetWithLabelValues(float64(mm.cluster.dataNodeStatInfo.IncreasedGB))
	mm.metaNodesTotal.SetWithLabelValues(float64(mm.cluster.metaNodeStatInfo.TotalGB))
	mm.metaNodesUsed.SetWithLabelValues(float64(mm.cluster.metaNodeStatInfo.UsedGB))
	mm.metaNodesIncreased.SetWithLabelValues(float64(mm.cluster.metaNodeStatInfo.IncreasedGB))
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

	for volName, _ := range deleteVolNames {
		mm.deleteVolMetric(volName)
	}
}

func (mm *monitorMetrics) deleteVolMetric(volName string) {
	mm.volTotalSpace.DeleteLabelValues(volName)
	mm.volUsedSpace.DeleteLabelValues(volName)
	mm.volUsage.DeleteLabelValues(volName)
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
	mm.metaNodesInactive.SetWithLabelValues(float64(inactiveMetaNodesCount))
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
	mm.dataNodesInactive.SetWithLabelValues(float64(inactiveDataNodesCount))
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

	mm.dataNodesCount.SetWithLabelValues(0)
	mm.metaNodesCount.SetWithLabelValues(0)
	mm.volCount.SetWithLabelValues(0)
	mm.dataNodesTotal.SetWithLabelValues(0)
	mm.dataNodesUsed.SetWithLabelValues(0)
	mm.dataNodeIncreased.SetWithLabelValues(0)
	mm.metaNodesTotal.SetWithLabelValues(0)
	mm.metaNodesUsed.SetWithLabelValues(0)
	mm.metaNodesIncreased.SetWithLabelValues(0)
	mm.diskError.SetWithLabelValues(0)
	mm.dataNodesInactive.SetWithLabelValues(0)
	mm.metaNodesInactive.SetWithLabelValues(0)
}
