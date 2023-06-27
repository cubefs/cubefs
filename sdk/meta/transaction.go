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
// permissions and limitations under the License.k

package meta

import (
	"errors"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

//const (
//	defaultTransactionTimeout = 5 //seconds
//)

//1.first metawrapper call with `Transaction` as a parameter,
//if tmID is not set(tmID == -1), then try to register a transaction to the metapartitions,
//the metapartition will become TM if transaction created successfully. and it will return txid.
//metawrapper call will set tmID before returning
//2.following metawrapper call with `Transaction` as a parameter, will only register rollback item,
//and the metapartition will become RM
type Transaction struct {
	txInfo          *proto.TransactionInfo
	Started         bool
	status          int
	onCommitFuncs   []func()
	onRollbackFuncs []func()
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
func NewTransaction(timeout int64, txType uint32) (tx *Transaction) {
	if timeout == 0 {
		timeout = proto.DefaultTransactionTimeout
	}
	return &Transaction{
		onCommitFuncs:   make([]func(), 0),
		onRollbackFuncs: make([]func(), 0),
		txInfo:          proto.NewTransactionInfo(timeout, txType),
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

func (tx *Transaction) OnStart() (status int, err error) {
	tx.RLock()
	defer tx.RUnlock()
	now := time.Now().UnixNano()
	if tx.Started && tx.txInfo.Timeout*1e9 <= now-tx.txInfo.CreateTime {
		status = statusTxTimeout
		err = errors.New("transaction already expired")
		return
	}
	return statusOK, nil
}

func (tx *Transaction) OnExecuted(status int, respTxInfo *proto.TransactionInfo) {
	tx.Lock()
	defer tx.Unlock()
	tx.status = status
	if tx.status == statusOK {
		if !tx.Started {
			tx.Started = true
		}
		if tx.txInfo.TmID == -1 && respTxInfo != nil {
			tx.txInfo = respTxInfo
		}
	}
}

func (tx *Transaction) SetOnCommit(job func()) {
	tx.onCommitFuncs = append(tx.onCommitFuncs, job)
	//tx.onCommit = job
}

func (tx *Transaction) SetOnRollback(job func()) {
	tx.onRollbackFuncs = append(tx.onRollbackFuncs, job)
	//tx.onRollback = job
}

func (tx *Transaction) OnDone(err error, mw *MetaWrapper) (newErr error) {
	//commit or rollback depending on status
	newErr = err
	if !tx.Started {
		return
	}
	if err != nil {
		log.LogDebugf("OnDone: rollback")
		tx.Rollback(mw)
	} else {
		log.LogDebugf("OnDone: commit")
		newErr = tx.Commit(mw)
	}
	return
}

// Commit will notify all the RM(related metapartitions) that transaction is completed successfully,
// and corresponding transaction items can be removed
func (tx *Transaction) Commit(mw *MetaWrapper) (err error) {
	tmMP := mw.getPartitionByID(uint64(tx.txInfo.TmID))
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
	err = packet.MarshalData(req)
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
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("Transaction commit failed: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
		return
	}

	/*if tx.onCommit != nil {
		tx.onCommit()
		log.LogDebugf("onCommit done")
	}*/

	for _, job := range tx.onCommitFuncs {
		job()
	}

	log.LogDebugf("Transaction commit succesfully: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
		tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
	return
}

// Rollback will notify all the RM(related metapartitions) that transaction is cancelled,
// and corresponding transaction items should be rolled back to previous state(before transaction)
func (tx *Transaction) Rollback(mw *MetaWrapper) {
	tmMP := mw.getPartitionByID(uint64(tx.txInfo.TmID))
	if tmMP == nil {
		log.LogWarnf("Transaction Rollback: No TM partition, TmID(%v), txID(%v)", tx.txInfo.TmID, tx.txInfo.TxID)
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

	/*if tx.onRollback != nil {
		tx.onRollback()
		log.LogDebugf("onRollback done")
	}*/

	for _, job := range tx.onRollbackFuncs {
		job()
	}

	log.LogDebugf("Transaction Rollback succesfully: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
		tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
}
