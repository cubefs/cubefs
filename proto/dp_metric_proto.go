package proto

import (
	"encoding/binary"
	"fmt"
	"sync"
)

const (
	MetricsCountHeader 	= 4

	MetricVersion		= 0
	MetricSizeV1 		= 32

	ReportUrl = "/dpMetrics/report"
	FetchUrl  = "/dpMetrics/fetch"
)

// DataPartitionMetrics defines the wrapper of the metrics related to the data partition.
type DataPartitionMetrics struct {
	sync.RWMutex
	AvgReadLatencyNano  int64
	AvgWriteLatencyNano int64
	SumReadLatencyNano  int64
	SumWriteLatencyNano int64
	ReadOpNum           int64
	WriteOpNum          int64
	PartitionId         uint64
}

// NewDataPartitionMetrics returns a new DataPartitionMetrics instance.
func NewDataPartitionMetrics() *DataPartitionMetrics {
	metrics := new(DataPartitionMetrics)
	return metrics
}

type DataPartitionMetricsMessage struct {
	DpMetrics []*DataPartitionMetrics
}

func (m *DataPartitionMetricsMessage) EncodeBinary() []byte {
	dataLen := MetricsCountHeader + len(m.DpMetrics) * MetricSizeV1
	data := make([]byte, dataLen)
	binary.BigEndian.PutUint32(data[0:MetricsCountHeader], uint32(len(m.DpMetrics)))
	for i := 0; i < len(m.DpMetrics); i++ {
		start := MetricsCountHeader + i * MetricSizeV1
		binary.BigEndian.PutUint64(data[start : start+8], m.DpMetrics[i].PartitionId)
		binary.BigEndian.PutUint64(data[start+8 : start+16], uint64(m.DpMetrics[i].WriteOpNum))
		binary.BigEndian.PutUint64(data[start+16 : start+32], uint64(m.DpMetrics[i].SumWriteLatencyNano))
	}
	return data
}

func (m *DataPartitionMetricsMessage) DecodeBinary(data []byte) error {
	if len(data) < MetricsCountHeader {
		return fmt.Errorf("DecodeBinary dp metrics err: need at least(%v) but buff len(%v)", MetricsCountHeader, len(data))
	}
	metricsLen := binary.BigEndian.Uint32(data[0:MetricsCountHeader])
	needDataLen := MetricsCountHeader + int(metricsLen) * MetricSizeV1
	if len(data) < needDataLen {
		return fmt.Errorf("DecodeBinary dp metrics err: need at least(%v) but buff len(%v)", needDataLen, len(data))
	}
	m.DpMetrics = make([]*DataPartitionMetrics, metricsLen)
	for i := 0; i < len(m.DpMetrics); i++ {
		metric := &DataPartitionMetrics{}
		start := MetricsCountHeader + i * MetricSizeV1
		metric.PartitionId = binary.BigEndian.Uint64(data[start : start+8])
		metric.WriteOpNum = int64(binary.BigEndian.Uint64(data[start+8 : start+16]))
		metric.SumWriteLatencyNano = int64(binary.BigEndian.Uint64(data[start+16 : start+32]))
		m.DpMetrics[i] = metric
	}
	return nil
}
