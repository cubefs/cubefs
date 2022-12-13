package metanode

import (
	"sync"
	"sync/atomic"
)

// TxIDAllocator generates and allocates ids
type TxIDAllocator struct {
	mpTxID uint64
	//mpID   uint64
	// store         *raftstore.RocksDBStore
	//partition raftstore.Partition
	txIDLock sync.RWMutex
}

//func newTxIDAllocator(mpID uint64, partition raftstore.Partition) (alloc *TxIDAllocator) {
func newTxIDAllocator() (alloc *TxIDAllocator) {
	alloc = new(TxIDAllocator)
	//alloc.mpID = mpID
	// alloc.store = store
	//alloc.partition = partition
	return
}

/*
func (alloc *TxIDAllocator) restoreTransactionID() {
	// value, err := alloc.store.Get(maxDataPartitionIDKey)
	// if err != nil {
	// 	panic(fmt.Sprintf("Failed to restore maxDataPartitionID,err:%v ", err.Error()))
	// }
	// bytes := value.([]byte)
	// if len(bytes) == 0 {
	// 	alloc.dataPartitionID = 0
	// 	return
	// }
	mpTxID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore transactionID,err:%v ", err.Error()))
	}
	alloc.mpTxID = mpTxID
	log.LogInfof("action[restoreTransactionID] mpTxID[%v]", alloc.mpTxID)
}
*/
func (alloc *TxIDAllocator) setTransactionID(id uint64) {
	atomic.StoreUint64(&alloc.mpTxID, id)
}

func (alloc *TxIDAllocator) getTransactionID() uint64 {
	return atomic.LoadUint64(&alloc.mpTxID)
}

func (alloc *TxIDAllocator) allocateTransactionID() (mpTxID uint64) {
	alloc.txIDLock.Lock()
	defer alloc.txIDLock.Unlock()
	mpTxID = atomic.LoadUint64(&alloc.mpTxID) + 1
	// var cmd []byte
	// metadata := new(RaftCmd)
	// metadata.Op = opSyncAllocDataPartitionID
	// metadata.K = maxDataPartitionIDKey
	// value := strconv.FormatUint(uint64(partitionID), 10)
	// metadata.V = []byte(value)
	// cmd, err = metadata.Marshal()
	// if err != nil {
	// 	goto errHandler
	// }
	// if _, err = alloc.partition.Submit(cmd); err != nil {
	// 	goto errHandler
	// }
	alloc.setTransactionID(mpTxID)
	return
	//errHandler:
	//	log.LogErrorf("action[allocateTransactionID] err:%v", err.Error())
	//return
}
