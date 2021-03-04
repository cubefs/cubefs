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

package metanode

import (
	"fmt"
	"time"

	"github.com/chubaofs/chubaofs/util/exporter"
)

//metrics
const (
	StatPeriod = time.Minute * time.Duration(1)
)

type MetaNodeMetrics struct {
	MetricMetaPartitionInodeCount *exporter.Gauge
	metricStopCh                  chan struct{}
}

func (m *MetaNode) startStat() {
	m.metrics = &MetaNodeMetrics{
		MetricMetaPartitionInodeCount: exporter.NewGauge("mpInodeCount"),
		metricStopCh:                  make(chan struct{}, 0),
	}

	go m.collectPartitionMetrics()
}

func (m *MetaNode) upatePartitionMetrics(mp *metaPartition) {
	labels := map[string]string{
		"partid": fmt.Sprintf("%d", mp.config.PartitionId),
	}
	it := mp.GetInodeTree()
	if it != nil {
		m.metrics.MetricMetaPartitionInodeCount.SetWithLabels(float64(it.Len()), labels)
	}
}

func (m *MetaNode) collectPartitionMetrics() {
	ticker := time.NewTicker(StatPeriod)
	for {
		select {
		case <-m.metrics.metricStopCh:
			return
		case <-ticker.C:
			if manager, ok := m.metadataManager.(*metadataManager); ok {
				manager.mu.RLock()
				for _, p := range manager.partitions {
					if mp, ok := p.(*metaPartition); ok {
						m.upatePartitionMetrics(mp)
					}
				}
				manager.mu.RUnlock()
			}
		}
	}
}

func (m *MetaNode) stopStat() {
	m.metrics.metricStopCh <- struct{}{}
}
