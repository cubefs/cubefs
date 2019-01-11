// Copyright 2018 The Container File System Authors.
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
	"github.com/tiglabs/containerfs/util/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"time"
	"github.com/tiglabs/containerfs/util/log"
	"fmt"
	"strconv"
)

const (
	StatPeriod                 = time.Minute * time.Duration(1)
	MetricDataNodesUsedGB      = "dataNodes_used_GB"
	MetricDataNodesTotalGB     = "dataNodes_total_GB"
	MetricDataNodesIncreasedGB = "dataNodes_increased_GB"
	MetricMetaNodesUsedGB      = "metaNodes_used_GB"
	MetricMetaNodesTotalGB     = "metaNodes_total_GB"
	MetricMetaNodesIncreasedGB = "metaNodes_increased_GB"
	MetricsDataNodesCount      = "dataNodes_count"
	MetricsMetaNodesCount      = "metaNodes_count"
	MetricsVolCount            = "vol_count"
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
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{cluster: c}
}

func (mm *monitorMetrics) start() {
	mm.dataNodesTotal = exporter.RegistGauge(MetricDataNodesTotalGB)
	mm.dataNodesUsed = exporter.RegistGauge(MetricDataNodesUsedGB)
	mm.dataNodeIncreased = exporter.RegistGauge(MetricDataNodesIncreasedGB)
	mm.metaNodesTotal = exporter.RegistGauge(MetricMetaNodesTotalGB)
	mm.metaNodesUsed = exporter.RegistGauge(MetricMetaNodesUsedGB)
	mm.metaNodesIncreased = exporter.RegistGauge(MetricMetaNodesIncreasedGB)
	mm.dataNodesCount = exporter.RegistGauge(MetricsDataNodesCount)
	mm.metaNodesCount = exporter.RegistGauge(MetricsMetaNodesCount)
	mm.volCount = exporter.RegistGauge(MetricsVolCount)
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
		volStatInfo, ok := value.(volStatInfo)
		if !ok {
			return true
		}
		volTotalGauge := exporter.RegistGauge(fmt.Sprintf("vol_%v_total_GB", key))
		volTotalGauge.Set(float64(volStatInfo.TotalGB))

		volUsedGauge := exporter.RegistGauge(fmt.Sprintf("vol_%v_used_GB", key))
		volUsedGauge.Set(float64(volStatInfo.UsedGB))

		volUsageRatioGauge := exporter.RegistGauge(fmt.Sprintf("vol_%v_usage_ratio", key))
		usedRatio, e := strconv.ParseFloat(volStatInfo.UsedRatio, 64)
		if e == nil {
			volUsageRatioGauge.Set(usedRatio)
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
