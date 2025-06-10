package flashgroupmanager

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/cubefs/cubefs/util/log"
)

type IDAllocator struct {
	mnIDLock sync.RWMutex
	commonID uint64

	store     *raftstore_db.RocksDBStore
	partition raftstore.Partition
}

func newIDAllocator(store *raftstore_db.RocksDBStore, partition raftstore.Partition) (alloc *IDAllocator) {
	alloc = new(IDAllocator)
	alloc.store = store
	alloc.partition = partition
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

func (alloc *IDAllocator) setCommonID(id uint64) {
	atomic.StoreUint64(&alloc.commonID, id)
}

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

func (alloc *IDAllocator) restore() {
	alloc.restoreMaxCommonID()
}
