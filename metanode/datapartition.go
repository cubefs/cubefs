package metanode

import (
	"github.com/chubaoio/cbfs/proto"
	"strings"
	"sync"
)

const (
	DataPartitionViewUrl = "/client/dataPartitions"
)

type DataPartition struct {
	PartitionID   uint32
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
}

func (dp *DataPartition) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

type DataPartitionsView struct {
	DataPartitions []*DataPartition
}

type Vol struct {
	sync.RWMutex
	dataPartitionView map[uint32]*DataPartition
}

func NewVol() *Vol {
	return &Vol{
		dataPartitionView: make(map[uint32]*DataPartition),
	}
}

func (v *Vol) GetPartition(partitionID uint32) *DataPartition {
	v.RLock()
	defer v.RUnlock()
	return v.dataPartitionView[partitionID]
}

func (v *Vol) UpdatePartitions(partitions DataPartitionsView) {
	for _, dp := range partitions.DataPartitions {
		v.replaceOrInsert(dp)
	}
}

func (v *Vol) replaceOrInsert(partition *DataPartition) {
	v.Lock()
	defer v.Unlock()
	v.dataPartitionView[partition.PartitionID] = partition
}
