// Copyright 2018 The CFS Authors.
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

import "sync/atomic"

// DataPartitionMetrics defines the metrics related to the data partition
type DataPartitionMetrics struct {
	WriteCnt          uint64
	ReadCnt           uint64
	TotalWriteLatency uint64
	TotalReadLatency  uint64
	WriteLatency      float64
	ReadLatency       float64
	lastWriteLatency  float64
	lastReadLatency   float64
}

// NewDataPartitionMetrics creates a new DataPartitionMetrics
func NewDataPartitionMetrics() *DataPartitionMetrics {
	metrics := new(DataPartitionMetrics)
	metrics.WriteCnt = 1
	metrics.ReadCnt = 1
	return metrics
}

// TODO should we call it UpdateReadMetrics
// UpdateReadMetrics updates the read-related metrics
func (metrics *DataPartitionMetrics) UpdateReadMetrics(latency uint64) {
	atomic.AddUint64(&metrics.ReadCnt, 1)
	atomic.AddUint64(&metrics.TotalReadLatency, latency)
}

// TODO should we call it UpdateWriteMetrics
// UpdateWriteMetrics updates the write-related  metrics
func (metrics *DataPartitionMetrics) UpdateWriteMetrics(latency uint64) {
	atomic.AddUint64(&metrics.WriteCnt, 1)
	atomic.AddUint64(&metrics.TotalWriteLatency, latency)
}

func (metrics *DataPartitionMetrics) recomputeLatency() {
	metrics.ReadLatency = float64((atomic.LoadUint64(&metrics.TotalReadLatency)) / (atomic.LoadUint64(&metrics.ReadCnt)))
	metrics.WriteLatency = float64((atomic.LoadUint64(&metrics.TotalWriteLatency)) / (atomic.LoadUint64(&metrics.WriteCnt)))
	atomic.StoreUint64(&metrics.TotalReadLatency, 0)
	atomic.StoreUint64(&metrics.TotalWriteLatency, 0)
	atomic.StoreUint64(&metrics.WriteCnt, 1)
	atomic.StoreUint64(&metrics.ReadCnt, 1)
}

// TODO no usage
func (metrics *DataPartitionMetrics) GetWriteLatency() float64 {
	return metrics.WriteLatency
}

// TODO no usage
func (metrics *DataPartitionMetrics) GetReadLatency() float64 {
	return metrics.ReadLatency
}
