package flashgroupmanager

import (
	"sync"
	"sync/atomic"
)

type IDAllocator struct {
	mnIDLock sync.RWMutex
	commonID uint64
}

// TODO
// func newIDAllocator(store *raftstore_db.RocksDBStore, partition raftstore.Partition) (alloc *IDAllocator) {
func newIDAllocator() (alloc *IDAllocator) {
	alloc = new(IDAllocator)
	return
}

func (alloc *IDAllocator) allocateCommonID() (id uint64, err error) {
	alloc.mnIDLock.Lock()
	defer alloc.mnIDLock.Unlock()
	// TDOD
	//var cmd []byte
	//metadata := new(RaftCmd)
	//metadata.Op = opSyncAllocCommonID
	//metadata.K = maxCommonIDKey
	id = atomic.LoadUint64(&alloc.commonID) + 1
	//value := strconv.FormatUint(uint64(id), 10)
	//metadata.V = []byte(value)
	//cmd, err = metadata.Marshal()
	//if err != nil {
	//	goto errHandler
	//}
	//if _, err = alloc.partition.Submit(cmd); err != nil {
	//	goto errHandler
	//}
	alloc.setCommonID(id)
	return
	// errHandler:
	//
	//	log.LogErrorf("action[allocateCommonID] err:%v", err.Error())
	//	return
}

func (alloc *IDAllocator) setCommonID(id uint64) {
	atomic.StoreUint64(&alloc.commonID, id)
}
