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

	MetricMpInodeCntName = "mp_inode_cnt"
	MetricMpOpName       = "mp_op"
)

type MetaNodeMetrics struct {
	MpInodeCount *exporter.GaugeVec
	MpOp         *exporter.TimePointCountVec
	metricStopCh chan struct{}
}

func (m *MetaNode) startStat() {
	if m.metrics == nil {
		m.metrics = &MetaNodeMetrics{
			MpInodeCount: exporter.NewGaugeVec(MetricMpInodeCntName, "", []string{"vol", "partid"}),
			MpOp:         exporter.NewTPCntVec(MetricMpOpName, "", []string{"vol", "partid", "op"}),
			metricStopCh: make(chan struct{}, 1),
		}
	}

	go m.collectPartitionMetrics()
}

func (m *MetaNode) updatePartitionMetrics(mp *metaPartition) {
	if m.metrics == nil {
		return
	}
	it := mp.GetInodeTree()
	if it != nil {
		labelVals := []string{mp.GetBaseConfig().VolName, fmt.Sprintf("%d", mp.config.PartitionId)}
		m.metrics.MpInodeCount.SetWithLabelValues(float64(it.Len()), labelVals...)
	}
}

func (m *MetaNode) collectPartitionMetrics() {
	if m.metrics == nil {
		return
	}

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
						m.updatePartitionMetrics(mp)
					}
				}
				manager.mu.RUnlock()
			}
		}
	}
}

func (m *MetaNode) stopStat() {
	if m.metrics != nil {
		m.metrics.metricStopCh <- struct{}{}
	}
}
