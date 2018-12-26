// Copyright 2018 The Containerfs Authors.
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
	"github.com/tiglabs/containerfs/proto"
	"strings"
	"sync"
)

const (
	DataPartitionViewUrl = "/client/dataPartitions"
)

// DataPartition结构体
type DataPartition struct {
	PartitionID   uint64
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
}

func (dp *DataPartition) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

// DataNode视图
type DataPartitionsView struct {
	DataPartitions []*DataPartition
}

//逻辑卷和DataNode视图关联结构体
type Vol struct {
	sync.RWMutex
	dataPartitionView map[uint64]*DataPartition
}

func NewVol() *Vol {
	return &Vol{
		dataPartitionView: make(map[uint64]*DataPartition),
	}
}

// 获取某个数据分片ID的视图
func (v *Vol) GetPartition(partitionID uint64) *DataPartition {
	v.RLock()
	defer v.RUnlock()
	return v.dataPartitionView[partitionID]
}

// 更新视图
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
