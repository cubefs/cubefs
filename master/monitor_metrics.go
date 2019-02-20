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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tiglabs/containerfs/util/exporter"
	"github.com/tiglabs/containerfs/util/log"
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
	dataNodesCount     prometheus.Gauge
	metaNodesCount     prometheus.Gauge
	volCount           prometheus.Gauge
	dataNodesTotal     prometheus.Gauge
	dataNodesUsed      prometheus.Gauge
	dataNodeIncreased  prometheus.Gauge
	metaNodesTotal     prometheus.Gauge
	metaNodesUsed      prometheus.Gauge
	metaNodesIncreased prometheus.Gauge
	volTotalSpace      *exporter.PromeMetric
	volUsedSpace       *exporter.PromeMetric
	volUsage           *exporter.PromeMetric
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{cluster: c}
}

func (mm *monitorMetrics) start() {
	mm.dataNodesTotal = exporter.RegisterGauge(MetricDataNodesTotalGB)
	mm.dataNodesUsed = exporter.RegisterGauge(MetricDataNodesUsedGB)
	mm.dataNodeIncreased = exporter.RegisterGauge(MetricDataNodesIncreasedGB)
	mm.metaNodesTotal = exporter.RegisterGauge(MetricMetaNodesTotalGB)
	mm.metaNodesUsed = exporter.RegisterGauge(MetricMetaNodesUsedGB)
	mm.metaNodesIncreased = exporter.RegisterGauge(MetricMetaNodesIncreasedGB)
	mm.dataNodesCount = exporter.RegisterGauge(MetricDataNodesCount)
	mm.metaNodesCount = exporter.RegisterGauge(MetricMetaNodesCount)
	mm.volCount = exporter.RegisterGauge(MetricVolCount)
	mm.volTotalSpace = exporter.RegisterMetric(MetricVolTotalGB, exporter.Gauge)
	mm.volUsedSpace = exporter.RegisterMetric(MetricVolUsedGB, exporter.Gauge)
	mm.volUsage = exporter.RegisterMetric(MetricVolUsageGB, exporter.Gauge)
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
		volTotalGauge := exporter.RegisterMetric(MetricVolTotalGB, exporter.Gauge)
		volTotalGauge.SetWithLabels(float64(volStatInfo.TotalGB), labels)

		volUsedGauge := exporter.RegisterMetric(MetricVolUsedGB, exporter.Gauge)
		volUsedGauge.SetWithLabels(float64(volStatInfo.UsedGB), labels)

		volUsageRatioGauge := exporter.RegisterMetric(MetricVolUsageGB, exporter.Gauge)
		usedRatio, e := strconv.ParseFloat(volStatInfo.UsedRatio, 64)
		if e == nil {
			volUsageRatioGauge.SetWithLabels(usedRatio, labels)
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
