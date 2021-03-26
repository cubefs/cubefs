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

package datanode

import (
	"fmt"

	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/exporter"
)

const (
	MetricDPOpName             = "dp_op"
	MetricPartitionIOName      = "dp_io"
	MetricPartitionIOBytesName = "dp_io_bytes"

	MetricDpIOTypeWrite     = "write"
	MetricDpIOTypeRandWrite = "randwrite"
	MetricDpIOTypeRead      = "read"
)

var (
	MetricDpOpLabelKeys = []string{"type"}
	MetricDpIOLabelKeys = []string{"disk", "vol", "partid", "type"}
)

type DataNodeMetrics struct {
	MetricIOBytes *exporter.CounterVec
	MetricIOTpc   *exporter.TimePointCountVec
	MetricDPOpTpc *exporter.TimePointCountVec
}

func (d *DataNode) registerMetrics() {
	if d.metrics == nil {
		d.metrics = &DataNodeMetrics{}
		d.metrics.MetricDPOpTpc = exporter.NewTPCntVec(MetricDPOpName, "", MetricDpOpLabelKeys)
		d.metrics.MetricIOBytes = exporter.NewCounterVec(MetricPartitionIOBytesName, "", MetricDpIOLabelKeys)
		d.metrics.MetricIOTpc = exporter.NewTPCntVec(MetricPartitionIOName, "", MetricDpIOLabelKeys)
	}
}

func (m *DataNodeMetrics) GetIoMetricLabelVals(partition *DataPartition, tp string) []string {
	return []string{
		partition.disk.Path,
		partition.volumeID,
		fmt.Sprintf("%d", partition.partitionID),
		tp,
	}
}

func (m *DataNodeMetrics) GetPacketTpLabelVals(p *repl.Packet) []string {
	labels := make([]string, 4)
	if part, ok := p.Object.(*DataPartition); ok {
		labels[0] = part.path
		labels[1] = part.volumeID
		labels[2] = fmt.Sprintf("%d", part.partitionID)
		labels[3] = p.GetOpMsg()
	}

	return labels
}
