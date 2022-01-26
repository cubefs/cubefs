// Copyright 2018 The Cubefs Authors.
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

package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"strings"
	"sync"
)

// DataPartition defines the struct of data partition that will be used on the meta node.
type DataPartition struct {
	PartitionID   uint64
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
}

// GetAllAddrs returns all addresses of the data partition.
func (dp *DataPartition) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

// DataPartitionsView defines the view of the data node.
type DataPartitionsView struct {
	DataPartitions []*DataPartition
}

func NewDataPartitionsView() *DataPartitionsView {
	return &DataPartitionsView{}
}

// Vol defines the view of the data partition with the read/write lock.
type Vol struct {
	sync.RWMutex
	dataPartitionView map[uint64]*DataPartition
}

// NewVol returns a new volume instance.
func NewVol() *Vol {
	return &Vol{
		dataPartitionView: make(map[uint64]*DataPartition),
	}
}

// GetPartition returns the data partition based on the given partition ID.
func (v *Vol) GetPartition(partitionID uint64) *DataPartition {
	v.RLock()
	defer v.RUnlock()
	return v.dataPartitionView[partitionID]
}

// UpdatePartitions updates the data partition.
func (v *Vol) UpdatePartitions(partitions *DataPartitionsView) {
	for _, dp := range partitions.DataPartitions {
		v.replaceOrInsert(dp)
	}
}

func (v *Vol) replaceOrInsert(partition *DataPartition) {
	v.Lock()
	defer v.Unlock()
	v.dataPartitionView[partition.PartitionID] = partition
}
