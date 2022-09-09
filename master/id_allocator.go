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

package master

import (
	"fmt"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"sync"
	"sync/atomic"
)

// IDAllocator generates and allocates ids
type IDAllocator struct {
	dataPartitionID uint64
	metaPartitionID uint64
	commonID        uint64
	clientID        uint64
	store           *raftstore.RocksDBStore
	partition       raftstore.Partition
	dpIDLock        sync.RWMutex
	mpIDLock        sync.RWMutex
	mnIDLock        sync.RWMutex
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
	alloc.restoreMaxCommonID()
	alloc.restoreClientID()
}

func (alloc *IDAllocator) restoreClientID() {
	value, err := alloc.store.Get(maxClientIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxClientID,err:%v ", err.Error()))
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		alloc.clientID = 0
		return
	}
	clientID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxClientID,err:%v ", err.Error()))
	}
	alloc.clientID = clientID
	log.LogInfof("action[restoreClientID] maxClientID[%v]", alloc.clientID)
}

func (alloc *IDAllocator) restoreMaxDataPartitionID() {
	value, err := alloc.store.Get(maxDataPartitionIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxDataPartitionID,err:%v ", err.Error()))
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		alloc.dataPartitionID = 0
		return
	}
	maxDataPartitionID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxDataPartitionID,err:%v ", err.Error()))
	}
	alloc.dataPartitionID = maxDataPartitionID
	log.LogInfof("action[restoreMaxDataPartitionID] maxDpID[%v]", alloc.dataPartitionID)
}

func (alloc *IDAllocator) restoreMaxMetaPartitionID() {
	value, err := alloc.store.Get(maxMetaPartitionIDKey)
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
	log.LogInfof("action[restoreMaxMetaPartitionID] maxMpID[%v]", alloc.metaPartitionID)
}

// The data node, meta node, and node set share the same ID allocator.
func (alloc *IDAllocator) restoreMaxCommonID() {
	value, err := alloc.store.Get(maxCommonIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxCommonID,err:%v ", err.Error()))
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		alloc.commonID = 0
		return
	}
	maxMetaNodeID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxCommonID,err:%v ", err.Error()))
	}
	alloc.commonID = maxMetaNodeID
	log.LogInfof("action[restoreMaxCommonID] maxCommonID[%v]", alloc.commonID)
}

func (alloc *IDAllocator) setDataPartitionID(id uint64) {
	atomic.StoreUint64(&alloc.dataPartitionID, id)
}

func (alloc *IDAllocator) setMetaPartitionID(id uint64) {
	atomic.StoreUint64(&alloc.metaPartitionID, id)
}

func (alloc *IDAllocator) setCommonID(id uint64) {
	atomic.StoreUint64(&alloc.commonID, id)
}

func (alloc *IDAllocator) setClientID(id uint64) {
	atomic.StoreUint64(&alloc.clientID, id)
}

func (alloc *IDAllocator) allocateDataPartitionID() (partitionID uint64, err error) {
	alloc.dpIDLock.Lock()
	defer alloc.dpIDLock.Unlock()
	var cmd []byte
	metadata := new(RaftCmd)
	partitionID = atomic.LoadUint64(&alloc.dataPartitionID) + 1
	metadata.Op = opSyncAllocDataPartitionID
	metadata.K = maxDataPartitionIDKey
	value := strconv.FormatUint(uint64(partitionID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errHandler
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errHandler
	}
	alloc.setDataPartitionID(partitionID)
	return
errHandler:
	log.LogErrorf("action[allocateDataPartitionID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateMetaPartitionID() (partitionID uint64, err error) {
	alloc.mpIDLock.Lock()
	defer alloc.mpIDLock.Unlock()
	var cmd []byte
	metadata := new(RaftCmd)
	metadata.Op = opSyncAllocMetaPartitionID
	metadata.K = maxMetaPartitionIDKey
	partitionID = atomic.LoadUint64(&alloc.metaPartitionID) + 1
	value := strconv.FormatUint(uint64(partitionID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errHandler
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errHandler
	}
	alloc.setMetaPartitionID(partitionID)
	return
errHandler:
	log.LogErrorf("action[allocateMetaPartitionID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateClientID() (clientID uint64, err error) {
	alloc.mpIDLock.Lock()
	defer alloc.mpIDLock.Unlock()
	var cmd []byte
	metadata := new(RaftCmd)
	metadata.Op = opSyncAllocClientID
	metadata.K = maxClientIDKey
	clientID = atomic.LoadUint64(&alloc.clientID) + 1
	value := strconv.FormatUint(uint64(clientID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errHandler
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errHandler
	}
	alloc.setClientID(clientID)
	return
errHandler:
	log.LogErrorf("action[allocateClientID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateCommonID() (id uint64, err error) {
	alloc.mnIDLock.Lock()
	defer alloc.mnIDLock.Unlock()
	var cmd []byte
	metadata := new(RaftCmd)
	metadata.Op = opSyncAllocCommonID
	metadata.K = maxCommonIDKey
	id = atomic.LoadUint64(&alloc.commonID) + 1
	value := strconv.FormatUint(uint64(id), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errHandler
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errHandler
	}
	alloc.setCommonID(id)
	return
errHandler:
	log.LogErrorf("action[allocateCommonID] err:%v", err.Error())
	return
}
