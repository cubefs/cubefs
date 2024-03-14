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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

type Transaction struct {
	txInfo          *proto.TransactionInfo
	Started         bool
	status          int
	onCommitFuncs   []func()
	onRollbackFuncs []func()
	sync.RWMutex
}

func (tx *Transaction) SetTxID(clientId uint64) {
	tx.txInfo.TxID = genTransactionId(clientId)
}

func (tx *Transaction) GetTxID() string {
	tx.RLock()
	defer tx.RUnlock()
	return tx.txInfo.TxID
}

func (tx *Transaction) SetTmID(tmID uint64) {
	tx.txInfo.TmID = int64(tmID)
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

func (tx *Transaction) OnExecuted(status int, respTxInfo *proto.TransactionInfo) {
	tx.Lock()
	defer tx.Unlock()
	tx.status = status
	if tx.status == statusOK {
		if !tx.Started {
			tx.Started = true
		}
		if tx.txInfo.TxID == "" && respTxInfo != nil {
			tx.txInfo = respTxInfo
		}
	}
}

func (tx *Transaction) SetOnCommit(job func()) {
	tx.onCommitFuncs = append(tx.onCommitFuncs, job)
}

func (tx *Transaction) SetOnRollback(job func()) {
	tx.onRollbackFuncs = append(tx.onRollbackFuncs, job)
	// tx.onRollback = job
}

func (tx *Transaction) OnDone(ctx context.Context, err error, mw *MetaWrapper) (newErr error) {
	// commit or rollback depending on status
	newErr = err
	if !tx.Started {
		return
	}
	if err != nil {
		log.Debugf("OnDone: rollback, tx %s", tx.txInfo.TxID)
		tx.Rollback(ctx, mw)
	} else {
		log.Debugf("OnDone: commit, tx %s", tx.txInfo.TxID)
		newErr = tx.Commit(ctx, mw)
	}
	return
}

// Commit will notify all the RM(related metapartitions) that transaction is completed successfully,
// and corresponding transaction items can be removed
func (tx *Transaction) Commit(ctx context.Context, mw *MetaWrapper) (err error) {
	span := proto.SpanFromContext(ctx)
	tmMP := mw.getPartitionByID(uint64(tx.txInfo.TmID))
	if tmMP == nil {
		span.Errorf("Transaction commit: No TM partition, TmID(%v), txID(%v)", tx.txInfo.TmID, tx.txInfo.TxID)
		return fmt.Errorf("transaction commit: can't find target mp for tx, mpId %d", tx.txInfo.TmID)
	}

	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txCommit", err, bgTime, 1)
	}()
	metric := exporter.NewTPCnt("OpTxCommit")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	req := &proto.TxApplyRequest{
		TxID:        tx.txInfo.TxID,
		TmID:        uint64(tx.txInfo.TmID),
		TxApplyType: proto.TxCommit,
		// TxInfo:      tx.txInfo,
	}

	packet := proto.NewPacketReqID().WithContext(ctx)
	packet.Opcode = proto.OpTxCommit
	packet.PartitionID = tmMP.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("Transaction commit: TmID(%v), txID(%v), req(%v) err(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, *req, err)
		return
	}

	packet, err = mw.sendToMetaPartition(tmMP, packet)
	if err != nil {
		span.Errorf("Transaction commit: txID(%v), packet(%v) mp(%v) req(%v) err(%v)",
			tx.txInfo.TxID, packet, tmMP, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		span.Errorf("Transaction commit failed: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
		return
	}

	for _, job := range tx.onCommitFuncs {
		job()
	}

	span.Debugf("Transaction commit succesfully: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
		tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())

	return
}

// Rollback will notify all the RM(related metapartitions) that transaction is cancelled,
// and corresponding transaction items should be rolled back to previous state(before transaction)
func (tx *Transaction) Rollback(ctx context.Context, mw *MetaWrapper) {
	span := proto.SpanFromContext(ctx)
	tmMP := mw.getPartitionByID(uint64(tx.txInfo.TmID))
	if tmMP == nil {
		span.Warnf("Transaction Rollback: No TM partition, TmID(%v), txID(%v)", tx.txInfo.TmID, tx.txInfo.TxID)
		return
	}

	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("txRollback", err, bgTime, 1)
	}()

	req := &proto.TxApplyRequest{
		TxID:        tx.txInfo.TxID,
		TmID:        uint64(tx.txInfo.TmID),
		TxApplyType: proto.TxRollback,
		// TxInfo:      tx.txInfo,
	}

	packet := proto.NewPacketReqID().WithContext(ctx)
	packet.Opcode = proto.OpTxRollback
	packet.PartitionID = tmMP.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		span.Errorf("Transaction Rollback: TmID(%v), txID(%v), req(%v) err(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, *req, err)
		return
	}

	metric := exporter.NewTPCnt("OpTxRollback")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	packet, err = mw.sendToMetaPartition(tmMP, packet)
	if err != nil {
		span.Errorf("Transaction Rollback: txID(%v), packet(%v) mp(%v) req(%v) err(%v)",
			tx.txInfo.TxID, packet, tmMP, *req, err)
		return
	}

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		span.Errorf("Transaction Rollback failed: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
			tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
		return
	}

	for _, job := range tx.onRollbackFuncs {
		job()
	}

	span.Debugf("Transaction Rollback successfully: TmID(%v), txID(%v), packet(%v) mp(%v) req(%v) result(%v)",
		tx.txInfo.TmID, tx.txInfo.TxID, packet, tmMP, *req, packet.GetResultMsg())
}
