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
	"fmt"
	"sync/atomic"
	"time"

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
	MetricMetaPartitionInodeCount  *exporter.Gauge
	MetricMetaPartitionDentryCount *exporter.Gauge
	MetricFileStats                *exporter.GaugeVec

	metricStopCh chan struct{}
}

func (m *MetaNode) startStat() {
	m.metrics = &MetaNodeMetrics{
		metricStopCh: make(chan struct{}),

		MetricConnectionCount:          exporter.NewGauge(MetricConnectionCount),
		MetricMetaFailedPartition:      exporter.NewGauge(MetricMetaFailedPartition),
		MetricMetaPartitionInodeCount:  exporter.NewGauge(MetricMetaPartitionInodeCount),
		MetricMetaPartitionDentryCount: exporter.NewGauge(MetricMetaPartitionDentryCount),
		MetricFileStats:                exporter.NewGaugeVec(MetricFileStats, "", []string{"volName", "sizeRange"}),
	}

	go m.collectPartitionMetrics()
}

func (m *MetaNode) updatePartitionMetrics(mp *metaPartition) {
	labels := map[string]string{
		"partid":     fmt.Sprintf("%d", mp.config.PartitionId),
		exporter.Vol: mp.config.VolName,
	}
	m.metrics.MetricMetaPartitionInodeCount.SetWithLabels(float64(mp.GetInodeTreeLen()), labels)
	m.metrics.MetricMetaPartitionDentryCount.SetWithLabels(float64(mp.GetDentryTreeLen()), labels)
}

func (m *MetaNode) collectPartitionMetrics() {
	ticker := time.NewTicker(StatPeriod)
	fileStatTicker := time.NewTicker(fileStatsCheckPeriod)
	for {
		select {
		case <-m.metrics.metricStopCh:
			return
		case <-ticker.C:
			if manager, ok := m.metadataManager.(*metadataManager); ok {
				manager.mu.RLock()
				for _, p := range manager.partitions {
					if mp, ok := p.(*metaPartition); ok {
						m.updatePartitionMetrics(mp)
					}
				}
				manager.mu.RUnlock()
			}
			m.metrics.MetricConnectionCount.Set(float64(m.connectionCnt))
		case <-fileStatTicker.C:
			m.updateFileStatsMetrics()
		}

	}
}

func (m *MetaNode) updateFileStatsMetrics() {
	m.metrics.MetricFileStats.Reset()
	volFileRange := make(map[string][]int64)
	if manager, ok := m.metadataManager.(*metadataManager); ok {
		manager.mu.RLock()
		defer manager.mu.RUnlock()

		numRanges := len(manager.fileStatsConfig.fileRangeLabels)
		for _, p := range manager.partitions {
			if mp, ok := p.(*metaPartition); ok {
				volName := mp.config.VolName
				if _, exists := volFileRange[volName]; !exists {
					volFileRange[volName] = make([]int64, numRanges)
				}
				for i := 0; i < numRanges; i++ {
					if i < len(mp.fileRange) {
						volFileRange[volName][i] += atomic.LoadInt64(&mp.fileRange[i])
					}
				}
			}
		}

		for volName, ranges := range volFileRange {
			for i, val := range ranges {
				sizeRange := manager.fileStatsConfig.fileRangeLabels[i]
				m.metrics.MetricFileStats.SetWithLabelValues(float64(val), volName, sizeRange)
			}
		}
	}
}

func (m *MetaNode) stopStat() {
	m.metrics.metricStopCh <- struct{}{}
}
