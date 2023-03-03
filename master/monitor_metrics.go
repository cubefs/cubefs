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
	MetricDiskError            = "disk_error"
	MetricDataNodesInactive    = "dataNodes_inactive"
	MetricMetaNodesInactive    = "metaNodes_inactive"
)

type monitorMetrics struct {
	cluster            *Cluster
	dataNodesCount     exporter.Gauge
	metaNodesCount     exporter.Gauge
	volCount           exporter.Gauge
	dataNodesTotal     exporter.Gauge
	dataNodesUsed      exporter.Gauge
	dataNodeIncreased  exporter.Gauge
	metaNodesTotal     exporter.Gauge
	metaNodesUsed      exporter.Gauge
	metaNodesIncreased exporter.Gauge
	diskError          exporter.Gauge
	dataNodesInactive  exporter.Gauge
	metaNodesInactive  exporter.Gauge
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
	var clusterLV = exporter.LabelValue{
		Label: "cluster",
		Value: mm.cluster.Name,
	}
	mm.dataNodesTotal = exporter.NewGauge(MetricDataNodesTotalGB, clusterLV)
	mm.dataNodesUsed = exporter.NewGauge(MetricDataNodesUsedGB, clusterLV)
	mm.dataNodeIncreased = exporter.NewGauge(MetricDataNodesIncreasedGB, clusterLV)
	mm.metaNodesTotal = exporter.NewGauge(MetricMetaNodesTotalGB, clusterLV)
	mm.metaNodesUsed = exporter.NewGauge(MetricMetaNodesUsedGB, clusterLV)
	mm.metaNodesIncreased = exporter.NewGauge(MetricMetaNodesIncreasedGB, clusterLV)
	mm.dataNodesCount = exporter.NewGauge(MetricDataNodesCount, clusterLV)
	mm.metaNodesCount = exporter.NewGauge(MetricMetaNodesCount, clusterLV)
	mm.volCount = exporter.NewGauge(MetricVolCount, clusterLV)
	mm.dataNodesInactive = exporter.NewGauge(MetricDataNodesInactive, clusterLV)
	mm.metaNodesInactive = exporter.NewGauge(MetricMetaNodesInactive, clusterLV)
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

	var clusterLabelValue = exporter.LabelValue{
		Label: "cluster",
		Value: mm.cluster.Name,
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

		var volumeLabelValue = exporter.LabelValue{
			Label: "volume",
			Value: volName,
		}
		exporter.NewGauge(MetricVolTotalGB, clusterLabelValue, volumeLabelValue).Set(float64(volStatInfo.TotalSize))
		exporter.NewGauge(MetricVolUsedGB, clusterLabelValue, volumeLabelValue).Set(float64(volStatInfo.UsedSize))
		if usedRatio, e := strconv.ParseFloat(volStatInfo.UsedRatio, 64); e == nil {
			exporter.NewGauge(MetricVolUsageGB, clusterLabelValue, volumeLabelValue).Set(usedRatio)
		}

		return true
	})

	for volName, _ := range deleteVolNames {
		mm.deleteVolMetric(volName)
	}
}

func (mm *monitorMetrics) deleteVolMetric(volName string) {
	var lvs = []exporter.LabelValue{
		{
			Label: "cluster",
			Value: mm.cluster.Name,
		},
		{
			Label: "volume",
			Value: volName,
		},
	}
	exporter.DeleteGaugeLabelValues(volName, lvs...)
	exporter.DeleteGaugeLabelValues(volName, lvs...)
	exporter.DeleteGaugeLabelValues(volName, lvs...)
}

func (mm *monitorMetrics) setDiskErrorMetric() {
	deleteBadDisks := make(map[string]string)
	for k, v := range mm.badDisks {
		deleteBadDisks[k] = v
		delete(mm.badDisks, k)
	}
	var clusterLV = exporter.LabelValue{
		Label: "cluster",
		Value: mm.cluster.Name,
	}
	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		for _, badDisk := range dataNode.BadDisks {
			for _, partition := range dataNode.DataPartitionReports {
				if partition.DiskPath == badDisk {
					var addrLV = exporter.LabelValue{
						Label: "addr",
						Value: dataNode.Addr,
					}
					var pathLV = exporter.LabelValue{
						Label: "path",
						Value: badDisk,
					}
					exporter.NewGauge(MetricDiskError, clusterLV, addrLV, pathLV)
					mm.badDisks[badDisk] = dataNode.Addr
					delete(deleteBadDisks, badDisk)
					break
				}
			}
		}

		return true
	})

	for k, v := range deleteBadDisks {
		var addrLV = exporter.LabelValue{
			Label: "addr",
			Value: v,
		}
		var pathLV = exporter.LabelValue{
			Label: "path",
			Value: k,
		}
		exporter.DeleteGaugeLabelValues(MetricDiskError, clusterLV, addrLV, pathLV)
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
	var clusterLV = exporter.LabelValue{
		Label: "cluster",
		Value: mm.cluster.Name,
	}
	for k, v := range mm.badDisks {
		var addrLV = exporter.LabelValue{
			Label: "addr",
			Value: v,
		}
		var pathLV = exporter.LabelValue{
			Label: "path",
			Value: k,
		}
		exporter.DeleteGaugeLabelValues(MetricDiskError, clusterLV, addrLV, pathLV)
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
	mm.diskError.Set(0)
	mm.dataNodesInactive.Set(0)
	mm.metaNodesInactive.Set(0)
}
