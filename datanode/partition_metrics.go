package datanode

import "sync/atomic"

type DataPartitionMetrics struct {
	WriteCnt        uint64
	ReadCnt         uint64
	SumWriteLatency uint64
	SumReadLatency  uint64
	WriteLatency    float64
	ReadLatency     float64
	lastWriteLatency float64
	lastReadLatency  float64
}

func NewDataPartitionMetrics() *DataPartitionMetrics {
	metrics := new(DataPartitionMetrics)
	metrics.WriteCnt = 1
	metrics.ReadCnt = 1
	return metrics
}

func (metrics *DataPartitionMetrics) AddReadMetrics(latency uint64) {
	atomic.AddUint64(&metrics.ReadCnt, 1)
	atomic.AddUint64(&metrics.SumReadLatency, latency)
}

func (metrics *DataPartitionMetrics) AddWriteMetrics(latency uint64) {
	atomic.AddUint64(&metrics.WriteCnt, 1)
	atomic.AddUint64(&metrics.SumWriteLatency, latency)
}

func (metrics *DataPartitionMetrics) recomputLatency() {
	metrics.ReadLatency = float64((atomic.LoadUint64(&metrics.SumReadLatency)) / (atomic.LoadUint64(&metrics.ReadCnt)))
	metrics.WriteLatency = float64((atomic.LoadUint64(&metrics.SumWriteLatency)) / (atomic.LoadUint64(&metrics.WriteCnt)))
	atomic.StoreUint64(&metrics.SumReadLatency, 0)
	atomic.StoreUint64(&metrics.SumWriteLatency, 0)
	atomic.StoreUint64(&metrics.WriteCnt, 1)
	atomic.StoreUint64(&metrics.ReadCnt, 1)
}

func (metrics *DataPartitionMetrics) GetWriteLatency() float64 {
	return metrics.WriteLatency
}

func (metrics *DataPartitionMetrics) GetReadLatency() float64 {
	return metrics.ReadLatency
}
