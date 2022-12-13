package meta

import (
	"errors"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sync"
)

const (
	defaultTransactionTimeout = 5 //seconds
)

//1.first metawrapper call with `Transaction` as a parameter,
//if tmID is not set(tmID == -1), then try to register a transaction to the metapartitions,
//the metapartition will become TM if transaction created successfully. and it will return txid.
//metawrapper call will set tmID before returning
//2.following metawrapper call with `Transaction` as a parameter, will only register rollback item,
//and the metapartition will become RM
type Transaction struct {
	txInfo     *proto.TransactionInfo
	Started    bool
	status     int
	onCommit   func()
	onRollback func()
	sync.RWMutex
}

func (tx *Transaction) setTxID(txID string) {
	//tx.Lock()
	//defer tx.Unlock()
	tx.txInfo.TxID = txID
}

func (tx *Transaction) GetTxID() string {
	tx.RLock()
	defer tx.RUnlock()
	return tx.txInfo.TxID
}

func (tx *Transaction) setTmID(tmID int64) {
	//tx.Lock()
	//defer tx.Unlock()
	tx.txInfo.TmID = tmID
}

func (tx *Transaction) AddInode(inode *proto.TxInodeInfo) error {
	tx.Lock()
	defer tx.Unlock()
	if tx.Started {
		return errors.New("transaction already started")
	} else {
		tx.txInfo.TxInodeInfos[inode.GetKey()] = inode
	}
	return nil
}

func (tx *Transaction) AddDentry(dentry *proto.TxDentryInfo) error {
	tx.Lock()
	defer tx.Unlock()
	if tx.Started {
		return errors.New("transaction already started")
	} else {
		tx.txInfo.TxDentryInfos[dentry.GetKey()] = dentry
	}
	return nil
}

// NewTransaction returns a `Transaction` with a timeout(seconds) duration after which the transaction
// will be rolled back if it has not completed yet
func NewTransaction(timeout uint32, txType uint32) (tx *Transaction) {
	if timeout == 0 {
		timeout = defaultTransactionTimeout
	}
	return &Transaction{
		txInfo: proto.NewTransactionInfo(timeout, txType),
	}
}

// Start a transaction, make sure all the related TxItemInfo(dentry, inode) are added into Transaction
// before invoking Start. `Start` will be invoked by every operation associated with transaction.
// Depending on the state of the `Transaction`, it would register a Transaction in metapartition as TM
// if `tmID` is not set, and set `tmID` to mpID after the registration,
// or it would register an rollback item(`TxRollbackInode` or `TxRollbackDentry`) in following metapartions(RM) if `tmID` is already set
/*
func (tx *Transaction) Start(ino uint64) (txId string, err error) {
	tx.RLock()
	defer tx.RUnlock()
	if tx.txInfo.TmID == -1 {
		//mp := tx.mw.getPartitionByInode(ino)
	} else {

	}
}
*/
func (tx *Transaction) OnExecuted(status int, respTxInfo *proto.TransactionInfo) {
	tx.RLock()
	defer tx.RUnlock()
	tx.status = status
	if tx.status == statusOK {
		if !tx.Started {
			tx.Started = true
		}
		if tx.txInfo.TmID == -1 && respTxInfo != nil {
			tx.txInfo = respTxInfo
			//tx.setTxID(respTxInfo.TxID)
			//tx.setTmID(respTxInfo.TmID)
			//tx.Started = true
		}
	}
}

func (tx *Transaction) SetOnCommit(job func()) {
	tx.onCommit = job
}

func (tx *Transaction) SetOnRollback(job func()) {
	tx.onRollback = job
}

func (tx *Transaction) OnDone(err error, mw *MetaWrapper) {
	//commit or rollback depending on status
	if !tx.Started {
		return
	}
	if err != nil {
		log.LogDebugf("OnDone: rollback")
		tx.Rollback(mw)
	} else {
		log.LogDebugf("OnDone: commit")
		tx.Commit(mw)
	}
}

// Commit will notify all the RM(related metapartitions) that transaction is completed successfully,
// and corresponding transaction items can be removed
func (tx *Transaction) Commit(mw *MetaWrapper) {
	tmMP := mw.getPartitionByInode(uint64(tx.txInfo.TmID))
	if tmMP == nil {
		log.LogErrorf("Transaction commit: No TM partition, TmID(%v), txID(%v)", tx.txInfo.TmID, tx.txInfo.TxID)
		return
	}

	req := &proto.TxApplyRequest{
		TxID:        tx.txInfo.TxID,
		TmID:        uint64(tx.txInfo.TmID),
		TxApplyType: proto.TxCommit,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpTxCommit
	packet.PartitionID = tmMP.PartitionID
	err := packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("Transaction commit: TmID(%v), txID(%v), req(%v) err(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, *req, err)
		return
	}

	packet, err = mw.sendToMetaPartition(tmMP, packet)
	if err != nil {
		log.LogErrorf("Transaction commit: txID(%v), packet(%v) mp(%v) req(%v) err(%v)",
			tx.txInfo.TxID, packet, tmMP, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("Transaction commit failed: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
		return
	}

	if tx.onCommit != nil {
		tx.onCommit()
		log.LogDebugf("onCommit done")
	}

	log.LogDebugf("Transaction commit succesfully: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
		tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
}

// Rollback will notify all the RM(related metapartitions) that transaction is cancelled,
// and corresponding transaction items should be rolled back to previous state(before transaction)
func (tx *Transaction) Rollback(mw *MetaWrapper) {
	//todo_tx: if transaction info in TM is missing, should try to rollback each item
	tmMP := mw.getPartitionByInode(uint64(tx.txInfo.TmID))
	if tmMP == nil {
		log.LogErrorf("Transaction Rollback: No TM partition, TmID(%v), txID(%v)", tx.txInfo.TmID, tx.txInfo.TxID)
		return
	}

	req := &proto.TxApplyRequest{
		TxID:        tx.txInfo.TxID,
		TmID:        uint64(tx.txInfo.TmID),
		TxApplyType: proto.TxRollback,
	}

	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpTxRollback
	packet.PartitionID = tmMP.PartitionID
	err := packet.MarshalData(req)
	if err != nil {
		log.LogErrorf("Transaction Rollback: TmID(%v), txID(%v), req(%v) err(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, *req, err)
		return
	}

	packet, err = mw.sendToMetaPartition(tmMP, packet)
	if err != nil {
		log.LogErrorf("Transaction Rollback: txID(%v), packet(%v) mp(%v) req(%v) err(%v)",
			tx.txInfo.TxID, packet, tmMP, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		log.LogErrorf("Transaction Rollback failed: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
		return
	}

	if tx.onRollback != nil {
		tx.onRollback()
		log.LogDebugf("onRollback done")
	}

	log.LogDebugf("Transaction Rollback succesfully: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
		tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
}

/********Transaction example
func renameTxGenerator(srcParentID uint64, srcName string, dstParentID uint64, dstName string) (tx *Transaction) {
	tx = NewTransaction(5)
	inoTx := &TxInodeInfo{}
	oldDtrTx := &TxDentryInfo{}
	newDtrTx := &TxDentryInfo{}
	tx.AddInode(inoTx)
	tx.AddDentry(oldDtrTx)
	tx.AddDentry(newDtrTx)
	return tx
}

func TestRenameTransaction() {
	err error
	defer func() {
		if err != nil {
			go tx.Rollback()
		} else {
			go tx.Commit()
		}
	}

	tx := renameTxGenerator(1, "oldname", 2, "newname")

	// rename metawrapper invokation...
	mw.ilinkTx(srcMP, inode, tx)
	mw.dcreateTx(dstParentMP, dstParentID, dstName, inode, mode, tx)
	mw.ddeleteTx(srcParentMP, srcParentID, srcName, lastVerSeq, tx)
	mw.iunlinkTx(srcMP, inode, lastVerSeq, tx)

	if err != nil {
		return
	}

}
**********/
