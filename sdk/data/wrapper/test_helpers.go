package wrapper

import (
	"sync"

	"github.com/cubefs/cubefs/proto"
)

// NewTestWrapperWithDps creates a Wrapper with a DefaultRandomSelector initialized
// with the given data partitions. Intended for use by external test packages only.
func NewTestWrapperWithDps(dps []*DataPartition) *Wrapper {
	s := &DefaultRandomSelector{
		partitions:            dps,
		localLeaderPartitions: make([]*DataPartition, 0),
	}
	w := &Wrapper{}
	w.Lock = sync.RWMutex{}
	w.dpSelector = s
	return w
}

// NewTestDP creates a DataPartition for testing purposes.
func NewTestDP(partitionID uint64, poolId uint8) *DataPartition {
	return &DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: partitionID,
			PoolId:      poolId,
			Hosts:       []string{"127.0.0.1:1"},
		},
		Metrics: new(DataPartitionMetrics),
	}
}
