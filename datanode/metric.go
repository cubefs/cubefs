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

package datanode

import (
	"fmt"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

const (
	StatPeriod                 = time.Minute * time.Duration(1)
	MetricPartitionIOName      = "dataPartitionIO"
	MetricPartitionIOBytesName = "dataPartitionIOBytes"
	MetricLackDpCount          = "lackDataPartitionCount"
)

type DataNodeMetrics struct {
	dataNode      *DataNode
	stopC         chan struct{}
	MetricIOBytes *exporter.Counter
	lackDpCount   *exporter.Gauge
}

func (d *DataNode) registerMetrics() {
	d.metrics = &DataNodeMetrics{
		dataNode: d,
		stopC:    make(chan struct{}),
	}
	d.metrics.MetricIOBytes = exporter.NewCounter(MetricPartitionIOBytesName)
	d.metrics.lackDpCount = exporter.NewGauge(MetricLackDpCount)
}

func (d *DataNode) startMetrics() {
	go d.metrics.statMetrics()
	log.LogInfof("startMetrics")
}

func (d *DataNode) closeMetrics() {
	close(d.metrics.stopC)
	log.LogInfof("closeMetrics")
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

func (dm *DataNodeMetrics) statMetrics() {
	ticker := time.NewTicker(StatPeriod)

	for {
		select {
		case <-dm.stopC:
			ticker.Stop()
			log.LogInfof("stop metrics ticker")
			return
		case <-ticker.C:
			dm.doStat()
		}
	}
}

func (dm *DataNodeMetrics) doStat() {
	dm.setLackDpCountMetrics()
}

func (dm *DataNodeMetrics) setLackDpCountMetrics() {
	lackPartitions := make([]uint64, 0)
	var err error
	lackPartitions, err = dm.dataNode.checkLocalPartitionMatchWithMaster()
	if err != nil {
		log.LogError(err)
		exporter.Warning(err.Error())
	}
	if len(lackPartitions) > 0 {
		err = fmt.Errorf("LackPartitions %v on datanode %v", lackPartitions, dm.dataNode.localServerAddr)
		log.LogErrorf(err.Error())
	}
	dm.lackDpCount.SetWithLabels(float64(len(lackPartitions)), map[string]string{"type": "lack_dp"})
}
