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
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"strconv"
	"time"
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
	volTotalSpace      *exporter.Gauge
	volUsedSpace       *exporter.Gauge
	volUsage           *exporter.Gauge
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{cluster: c}
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
	mm.volTotalSpace = exporter.NewGauge(MetricVolTotalGB)
	mm.volUsedSpace = exporter.NewGauge(MetricVolUsedGB)
	mm.volUsage = exporter.NewGauge(MetricVolUsageGB)
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
	mm.dataNodesCount.Set(int64(dataNodeCount))
	metaNodeCount := mm.cluster.metaNodeCount()
	mm.metaNodesCount.Set(int64(metaNodeCount))
	volCount := len(mm.cluster.vols)
	mm.volCount.Set(int64(volCount))
	mm.dataNodesTotal.Set(int64(mm.cluster.dataNodeStatInfo.TotalGB))
	mm.dataNodesUsed.Set(int64(mm.cluster.dataNodeStatInfo.UsedGB))
	mm.dataNodeIncreased.Set(int64(mm.cluster.dataNodeStatInfo.IncreasedGB))
	mm.metaNodesTotal.Set(int64(mm.cluster.metaNodeStatInfo.TotalGB))
	mm.metaNodesUsed.Set(int64(mm.cluster.metaNodeStatInfo.UsedGB))
	mm.metaNodesIncreased.Set(int64(mm.cluster.metaNodeStatInfo.IncreasedGB))
	mm.cluster.volStatInfo.Range(func(key, value interface{}) bool {
		volStatInfo, ok := value.(*volStatInfo)
		if !ok {
			return true
		}
		volName, ok := key.(string)
		if !ok {
			return true
		}
		labels := map[string]string{"volName": volName}
		volTotalGauge := exporter.NewGauge(MetricVolTotalGB)
		volTotalGauge.SetWithLabels(int64(volStatInfo.TotalGB), labels)

		volUsedGauge := exporter.NewGauge(MetricVolUsedGB)
		volUsedGauge.SetWithLabels(int64(volStatInfo.UsedGB), labels)

		volUsageRatioGauge := exporter.NewGauge(MetricVolUsageGB)
		usedRatio, e := strconv.ParseFloat(volStatInfo.UsedRatio, 64)
		if e == nil {
			volUsageRatioGauge.SetWithLabels(int64(usedRatio), labels)
		}

		return true
	})
}

func (mm *monitorMetrics) resetAllMetrics() {
	mm.dataNodesCount.Set(0)
	mm.metaNodesCount.Set(0)
	mm.volCount.Set(0)
	mm.dataNodesTotal.Set(0)
	mm.dataNodesUsed.Set(0)
	mm.dataNodeIncreased.Set(0)
	mm.metaNodesTotal.Set(0)
	mm.metaNodesUsed.Set(0)
	mm.metaNodesIncreased.Set(0)
}
