// Copyright 2018 The ChuBao Authors.
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

package master

import (
	"fmt"
	"github.com/chubaoio/cbfs/raftstore"
	"github.com/chubaoio/cbfs/util/log"
	"strconv"
	"sync/atomic"
)

const (
	MaxDataPartitionIDKey = "max_dp_id"
	MaxMetaPartitionIDKey = "max_mp_id"
	MaxMetaNodeIDKey      = "max_metaNode_id"
)

type IDAllocator struct {
	dataPartitionID uint64
	metaPartitionID uint64
	metaNodeID      uint64
	store           *raftstore.RocksDBStore
	partition       raftstore.Partition
}

func newIDAllocator(store *raftstore.RocksDBStore, partition raftstore.Partition) (alloc *IDAllocator) {
	alloc = new(IDAllocator)
	alloc.store = store
	alloc.partition = partition
	return
}

func (alloc *IDAllocator) restore() {
	alloc.restoreMaxDataPartitionID()
	alloc.restoreMaxMetaPartitionID()
	alloc.restoreMaxMetaNodeID()
}

func (alloc *IDAllocator) restoreMaxDataPartitionID() {
	value, err := alloc.store.Get(MaxDataPartitionIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxDataPartitionId,err:%v ", err.Error()))
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		alloc.dataPartitionID = 0
		return
	}
	maxDataPartitionId, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxDataPartitionId,err:%v ", err.Error()))
	}
	alloc.dataPartitionID = maxDataPartitionId
}

func (alloc *IDAllocator) restoreMaxMetaPartitionID() {
	value, err := alloc.store.Get(MaxMetaPartitionIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxPartitionID,err:%v ", err.Error()))
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		alloc.metaPartitionID = 0
		return
	}
	maxPartitionID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxPartitionID,err:%v ", err.Error()))
	}
	alloc.metaPartitionID = maxPartitionID

}

func (alloc *IDAllocator) restoreMaxMetaNodeID() {
	value, err := alloc.store.Get(MaxMetaNodeIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxMetaNodeID,err:%v ", err.Error()))
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		alloc.metaNodeID = 0
		return
	}
	maxMetaNodeID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxMetaNodeID,err:%v ", err.Error()))
	}
	alloc.metaNodeID = maxMetaNodeID

}

func (alloc *IDAllocator) increaseDataPartitionID() {
	atomic.AddUint64(&alloc.dataPartitionID, 1)
}

func (alloc *IDAllocator) increaseMetaPartitionID() {
	atomic.AddUint64(&alloc.metaPartitionID, 1)
}

func (alloc *IDAllocator) increaseMetaNodeID() {
	atomic.AddUint64(&alloc.metaNodeID, 1)
}

func (alloc *IDAllocator) allocateDataPartitionID() (ID uint64, err error) {
	var cmd []byte
	metadata := new(Metadata)
	ID = atomic.AddUint64(&alloc.dataPartitionID, 1)
	metadata.Op = OpSyncAllocDataPartitionID
	metadata.K = MaxDataPartitionIDKey
	value := strconv.FormatUint(uint64(ID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError("action[allocateDataPartitionID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateMetaPartitionID() (partitionID uint64, err error) {
	var cmd []byte
	metadata := new(Metadata)
	metadata.Op = OpSyncAllocMetaPartitionID
	metadata.K = MaxMetaPartitionIDKey
	partitionID = atomic.AddUint64(&alloc.metaPartitionID, 1)
	value := strconv.FormatUint(uint64(partitionID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError("action[allocateMetaPartitionID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateMetaNodeID() (metaNodeID uint64, err error) {
	var cmd []byte
	metadata := new(Metadata)
	metadata.Op = OpSyncAllocMetaNodeID
	metadata.K = MaxMetaNodeIDKey
	metaNodeID = atomic.AddUint64(&alloc.metaNodeID, 1)
	value := strconv.FormatUint(uint64(metaNodeID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError("action[allocateMetaNodeID] err:%v", err.Error())
	return
}
