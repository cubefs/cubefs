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

	"github.com/cubefs/cubefs/util/exporter"
)

const (
	MetricPartitionIOName      = "dataPartitionIO"
	MetricPartitionIOBytesName = "dataPartitionIOBytes"
	RoutinesName               = "routines"
)

type DataNodeMetrics struct {
	MetricIOBytes *exporter.Counter
	Routines      *exporter.GaugeVec
}

func (d *DataNode) registerMetrics() {
	d.metrics = &DataNodeMetrics{}
	d.metrics.MetricIOBytes = exporter.NewCounter(MetricPartitionIOBytesName)
	d.metrics.Routines = exporter.NewGaugeVec(RoutinesName, "", []string{"disk"})
}

func GetIoMetricLabels(partition *DataPartition, tp string) map[string]string {
	labels := make(map[string]string)
	labels[exporter.Vol] = partition.volumeID
	labels[exporter.Type] = tp
	labels[exporter.Disk] = partition.disk.Path
	if exporter.EnablePid {
		labels[exporter.PartId] = fmt.Sprintf("%d", partition.partitionID)
	}

	return labels
}
