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
	MetricCapacityToCreateDp   = "capacityToCreateDp"
	MetricConnectionCnt        = "connectionCnt"
	MetricDpCount              = "dataPartitionCount"
	MetricTotalDpSize          = "totalDpSize"
	MetricCapacity             = "capacity"
)

type DataNodeMetrics struct {
	dataNode                 *DataNode
	stopC                    chan struct{}
	MetricIOBytes            *exporter.Counter
	MetricLackDpCount        *exporter.Gauge
	MetricCapacityToCreateDp *exporter.GaugeVec
	MetricConnectionCnt      *exporter.Gauge
	MetricDpCount            *exporter.Gauge
	MetricTotalDpSize        *exporter.Gauge
	MetricCapacity           *exporter.GaugeVec
}

func (d *DataNode) registerMetrics() {
	d.metrics = &DataNodeMetrics{
		dataNode: d,
		stopC:    make(chan struct{}),
	}
	d.metrics.MetricIOBytes = exporter.NewCounter(MetricPartitionIOBytesName)
	d.metrics.MetricLackDpCount = exporter.NewGauge(MetricLackDpCount)
	d.metrics.MetricCapacityToCreateDp = exporter.NewGaugeVec(MetricCapacityToCreateDp, "", []string{"type"})
	d.metrics.MetricConnectionCnt = exporter.NewGauge(MetricConnectionCnt)
	d.metrics.MetricDpCount = exporter.NewGauge(MetricDpCount)
	d.metrics.MetricTotalDpSize = exporter.NewGauge(MetricTotalDpSize)
	d.metrics.MetricCapacity = exporter.NewGaugeVec(MetricCapacity, "", []string{"type"})
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
	dm.setCapacityToCreateDpMetrics()
	dm.setConnectionCntMetrics()
	dm.setDpCountMetrics()
	dm.setTotalDpSizeMetrics()
	dm.setCapacityMetrics()
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
	dm.MetricLackDpCount.SetWithLabels(float64(len(lackPartitions)), map[string]string{"type": "lack_dp"})
}

func (dm *DataNodeMetrics) setCapacityToCreateDpMetrics() {
	remainingCapacityToCreateDp := dm.dataNode.space.stats.RemainingCapacityToCreatePartition
	maxCapacityToCreateDp := dm.dataNode.space.stats.MaxCapacityToCreatePartition
	dm.MetricCapacityToCreateDp.SetWithLabelValues(float64(remainingCapacityToCreateDp), "remaining")
	dm.MetricCapacityToCreateDp.SetWithLabelValues(float64(maxCapacityToCreateDp), "max")
}

func (dm *DataNodeMetrics) setConnectionCntMetrics() {
	connectionCnt := dm.dataNode.space.stats.ConnectionCnt
	dm.MetricConnectionCnt.Set(float64(connectionCnt))
}

func (dm *DataNodeMetrics) setDpCountMetrics() {
	dpCount := dm.dataNode.space.stats.CreatedPartitionCnt
	dm.MetricDpCount.Set(float64(dpCount))
}

func (dm *DataNodeMetrics) setTotalDpSizeMetrics() {
	totalDpSize := dm.dataNode.space.stats.TotalPartitionSize
	dm.MetricTotalDpSize.Set(float64(totalDpSize))
}

func (dm *DataNodeMetrics) setCapacityMetrics() {
	total := dm.dataNode.space.stats.Total
	used := dm.dataNode.space.stats.Used
	available := dm.dataNode.space.stats.Available
	dm.MetricCapacity.SetWithLabelValues(float64(total), "total")
	dm.MetricCapacity.SetWithLabelValues(float64(used), "used")
	dm.MetricCapacity.SetWithLabelValues(float64(available), "available")
}
