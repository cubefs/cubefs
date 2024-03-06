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
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/raftstore/raftstore_db"
)

// IDAllocator generates and allocates ids
type IDAllocator struct {
	dataPartitionID uint64
	metaPartitionID uint64
	commonID        uint64
	clientID        uint64
	clientIDLimit   uint64
	quotaID         uint32
	store           *raftstore_db.RocksDBStore
	partition       raftstore.Partition
	dpIDLock        sync.RWMutex
	mpIDLock        sync.RWMutex
	mnIDLock        sync.RWMutex
	qaIDLock        sync.RWMutex
}

const clientIDBatchCount = 1000

func newIDAllocator(store *raftstore_db.RocksDBStore, partition raftstore.Partition) (alloc *IDAllocator) {
	alloc = new(IDAllocator)
	alloc.store = store
	alloc.partition = partition
	return
}

func (alloc *IDAllocator) restore(ctx context.Context) {
	alloc.restoreMaxDataPartitionID(ctx)
	alloc.restoreMaxMetaPartitionID(ctx)
	alloc.restoreMaxCommonID(ctx)
	alloc.restoreMaxQuotaID(ctx)
	alloc.restoreClientID(ctx)
}

func (alloc *IDAllocator) restoreMaxDataPartitionID(ctx context.Context) {
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
	span := proto.SpanFromContext(ctx)
	alloc.dataPartitionID = maxDataPartitionID
	span.Infof("action[restoreMaxDataPartitionID] maxDpID[%v]", alloc.dataPartitionID)
}

func (alloc *IDAllocator) restoreMaxMetaPartitionID(ctx context.Context) {
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
	span := proto.SpanFromContext(ctx)
	span.Infof("action[restoreMaxMetaPartitionID] maxMpID[%v]", alloc.metaPartitionID)
}

// The data node, meta node, and node set share the same ID allocator.
func (alloc *IDAllocator) restoreMaxCommonID(ctx context.Context) {
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
	span := proto.SpanFromContext(ctx)
	span.Infof("action[restoreMaxCommonID] maxCommonID[%v]", alloc.commonID)
}

func (alloc *IDAllocator) restoreMaxQuotaID(ctx context.Context) {
	value, err := alloc.store.Get(maxQuotaIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxQuotaID,err:%v ", err.Error()))
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		alloc.quotaID = 0
		return
	}
	maxQuotaID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxQuotaID,err:%v ", err.Error()))
	}

	if maxQuotaID > 0 && maxQuotaID <= math.MaxInt32 {
		alloc.quotaID = uint32(maxQuotaID)
	} else {
		alloc.quotaID = math.MaxInt32
	}
	span := proto.SpanFromContext(ctx)
	span.Infof("action[restoreMaxCommonID] maxQuotaID[%v]", alloc.quotaID)
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

func (alloc *IDAllocator) restoreClientID(ctx context.Context) {
	alloc.mpIDLock.Lock()
	defer alloc.mpIDLock.Unlock()
	value, err := alloc.store.Get(maxClientIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxClientID,err:%v ", err.Error()))
	}
	bytes := value.([]byte)
	if len(bytes) != 0 {
		alloc.clientID, err = strconv.ParseUint(string(bytes), 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Failed to restore maxClientID,err:%v ", err.Error()))
		}
	}
	alloc.clientIDLimit = alloc.clientID
	alloc.clientID += clientIDBatchCount
}

func (alloc *IDAllocator) setQuotaID(id uint32) {
	atomic.StoreUint32(&alloc.quotaID, id)
}

func (alloc *IDAllocator) allocateDataPartitionID(ctx context.Context) (partitionID uint64, err error) {
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
	span := proto.SpanFromContext(ctx)
	span.Errorf("action[allocateDataPartitionID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateMetaPartitionID(ctx context.Context) (partitionID uint64, err error) {
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
	span := proto.SpanFromContext(ctx)
	span.Errorf("action[allocateMetaPartitionID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateClientID(ctx context.Context) (clientID uint64, err error) {
	alloc.mpIDLock.Lock()
	defer alloc.mpIDLock.Unlock()
	clientID = alloc.clientID + 1
	if alloc.clientIDLimit < clientID {
		var cmd []byte
		metadata := new(RaftCmd)
		metadata.Op = opSyncAllocClientID
		metadata.K = maxClientIDKey
		// sync clientID - 1
		value := strconv.FormatUint(uint64(alloc.clientID), 10)
		metadata.V = []byte(value)
		cmd, err = metadata.Marshal()
		if err != nil {
			goto errHandler
		}
		if _, err = alloc.partition.Submit(cmd); err != nil {
			goto errHandler
		}
		alloc.clientIDLimit = alloc.clientID + clientIDBatchCount
	}
	alloc.clientID = clientID
	return
errHandler:
	span := proto.SpanFromContext(ctx)
	span.Errorf("action[allocateClientID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateCommonID(ctx context.Context) (id uint64, err error) {
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
	span := proto.SpanFromContext(ctx)
	span.Errorf("action[allocateCommonID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateQuotaID(ctx context.Context) (id uint32, err error) {
	alloc.qaIDLock.Lock()
	defer alloc.qaIDLock.Unlock()
	var cmd []byte
	metadata := new(RaftCmd)
	metadata.Op = opSyncAllocQuotaID
	metadata.K = maxQuotaIDKey
	id = atomic.LoadUint32(&alloc.quotaID) + 1
	value := strconv.FormatUint(uint64(id), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errHandler
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errHandler
	}
	alloc.setQuotaID(id)
	return
errHandler:
	span := proto.SpanFromContext(ctx)
	span.Errorf("action[allocateQuotaID] err:%v", err.Error())
	return
}
