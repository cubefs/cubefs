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

package metanode

import (
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
)

// metrics
const (
	StatPeriod = time.Minute * time.Duration(1)

	MetricMetaFailedPartition      = "meta_failed_partition"
	MetricMetaPartitionInodeCount  = "mpInodeCount"
	MetricMetaPartitionDentryCount = "mpDentryCount"
	MetricConnectionCount          = "connectionCnt"
	MetricFileStats                = "fileStats"
)

type MetaNodeMetrics struct {
	MetricConnectionCount          *exporter.Gauge
	MetricMetaFailedPartition      *exporter.Gauge
	MetricMetaPartitionInodeCount  *exporter.GaugeVec
	MetricMetaPartitionDentryCount *exporter.GaugeVec
	MetricFileStats                *exporter.GaugeVec

	metricStopCh chan struct{}
}

func (m *MetaNode) startStat() {
	m.metrics = &MetaNodeMetrics{
		metricStopCh: make(chan struct{}),

		MetricConnectionCount:          exporter.NewGauge(MetricConnectionCount),
		MetricMetaFailedPartition:      exporter.NewGauge(MetricMetaFailedPartition),
		MetricMetaPartitionInodeCount:  exporter.NewGaugeVec(MetricMetaPartitionInodeCount, "", []string{"volName"}),
		MetricMetaPartitionDentryCount: exporter.NewGaugeVec(MetricMetaPartitionDentryCount, "", []string{"volName"}),
		MetricFileStats:                exporter.NewGaugeVec(MetricFileStats, "", []string{"volName", "sizeRange"}),
	}

	go m.collectPartitionMetrics()
}

func (m *MetaNode) updatePartitionMetrics() {
	m.metrics.MetricMetaPartitionInodeCount.Reset()
	m.metrics.MetricMetaPartitionDentryCount.Reset()
	volInodeCount := make(map[string]int)
	volDentryCount := make(map[string]int)

	manager, ok := m.metadataManager.(*metadataManager)
	if !ok {
		return
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	for _, p := range manager.partitions {
		mp, ok := p.(*metaPartition)
		if !ok {
			continue
		}
		volName := mp.config.VolName
		if _, exists := volInodeCount[volName]; !exists {
			volInodeCount[volName] = 0
			volDentryCount[volName] = 0
		}
		volInodeCount[volName] += mp.GetInodeTreeLen()
		volDentryCount[volName] += mp.GetDentryTreeLen()
	}

	for volName, inodeCount := range volInodeCount {
		dentryCount := volDentryCount[volName]
		m.metrics.MetricMetaPartitionInodeCount.SetWithLabelValues(float64(inodeCount), volName)
		m.metrics.MetricMetaPartitionDentryCount.SetWithLabelValues(float64(dentryCount), volName)
	}
}

func (m *MetaNode) collectPartitionMetrics() {
	ticker := time.NewTicker(StatPeriod)
	fileStatTicker := time.NewTicker(fileStatsCheckPeriod)
	for {
		select {
		case <-m.metrics.metricStopCh:
			return
		case <-ticker.C:
			m.updatePartitionMetrics()
			m.metrics.MetricConnectionCount.Set(float64(m.connectionCnt))
		case <-fileStatTicker.C:
			m.updateFileStatsMetrics()
		}
	}
}

func (m *MetaNode) updateFileStatsMetrics() {
	m.metrics.MetricFileStats.Reset()
	volFileRange := make(map[string][]int64)

	manager, ok := m.metadataManager.(*metadataManager)
	if !ok {
		return
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	_, labels, _ := manager.GetFileStatsConfig()

	numRanges := len(labels)
	for _, p := range manager.partitions {
		mp, ok := p.(*metaPartition)
		if !ok {
			continue
		}
		fileRange := mp.getFileRange()
		volName := mp.config.VolName
		if _, exists := volFileRange[volName]; !exists {
			volFileRange[volName] = make([]int64, numRanges)
		}
		validLength := util.Min(len(fileRange), numRanges)
		for i := 0; i < validLength; i++ {
			volFileRange[volName][i] += fileRange[i]
		}
	}

	for volName, ranges := range volFileRange {
		for i, val := range ranges {
			sizeRange := labels[i]
			m.metrics.MetricFileStats.SetWithLabelValues(float64(val), volName, sizeRange)
		}
	}
}

func (m *MetaNode) stopStat() {
	m.metrics.metricStopCh <- struct{}{}
}
