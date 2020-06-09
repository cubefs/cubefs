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

	"github.com/chubaofs/chubaofs/util/exporter"
)

const (
	MetricPartitionIOName      = "dataPartitionIO"
	MetricPartitionIOBytesName = "dataPartitionIOBytes"
)

type DataNodeMetrics struct {
	MetricIOBytes *exporter.Counter
}

func (d *DataNode) registerMetrics() {
	d.metrics = &DataNodeMetrics{}
	d.metrics.MetricIOBytes = exporter.NewCounter(MetricPartitionIOBytesName)
}

func GetIoMetricLabels(partition *DataPartition, tp string) map[string]string {
	return map[string]string{
		"disk":   partition.disk.Path,
		"vol":    partition.volumeID,
		"partid": fmt.Sprintf("%d", partition.partitionID),
		"type":   tp,
	}
}
