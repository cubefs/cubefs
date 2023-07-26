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

package metanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
)

//Rollback Type
const (
	TxNoOp uint8 = iota
	TxUpdate
	TxDelete
	TxAdd
)

func (i *TxRollbackInode) ToString() string {
	content := fmt.Sprintf("{inode:[ino:%v, type:%v, nlink:%v], quotaIds:%v, rbType:%v"+
		"txInodeInfo:[Ino:%v, MpID:%v, CreateTime:%v, Timeout:%v, TxID:%v, MpMembers:%v]}",
		i.inode.Inode, i.inode.Type, i.inode.NLink, i.quotaIds, i.rbType, i.txInodeInfo.Ino, i.txInodeInfo.MpID,
		i.txInodeInfo.CreateTime, i.txInodeInfo.Timeout, i.txInodeInfo.TxID, i.txInodeInfo.MpMembers)
	return content
}

type TxRollbackInode struct {
	inode       *Inode
	txInodeInfo *proto.TxInodeInfo
	rbType      uint8 //Rollback Type
	quotaIds    []uint32
}

// Less tests whether the current TxRollbackInode item is less than the given one.
func (i *TxRollbackInode) Less(than btree.Item) bool {
	ti, ok := than.(*TxRollbackInode)
	if !ok {
		return false
	}

	if i.txInodeInfo != nil && ti.txInodeInfo != nil {
		return i.txInodeInfo.Ino < ti.txInodeInfo.Ino
	}

	return i.inode.Inode < ti.inode.Inode
}

// Copy returns a copy of the TxRollbackInode.
func (i *TxRollbackInode) Copy() btree.Item {

	item := i.inode.Copy()
	txInodeInfo := *i.txInodeInfo

	quotaIds := make([]uint32, len(i.quotaIds))
	copy(quotaIds, i.quotaIds)

	return &TxRollbackInode{
		inode:       item.(*Inode),
		quotaIds:    quotaIds,
		txInodeInfo: &txInodeInfo,
		rbType:      i.rbType,
	}
}

func (i *TxRollbackInode) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 256))
	bs, err := i.inode.Marshal()
	if err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return
	}
	if _, err = buff.Write(bs); err != nil {
		return
	}
	bs, err = i.txInodeInfo.Marshal()
	if err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err = buff.Write(bs); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, &i.rbType); err != nil {
		return
	}

	quotaBytes := bytes.NewBuffer(make([]byte, 0, 8))
	for _, quotaId := range i.quotaIds {
		if err = binary.Write(quotaBytes, binary.BigEndian, quotaId); err != nil {
			return
		}
	}

	_, err = buff.Write(quotaBytes.Bytes())
	return buff.Bytes(), err
}

func (i *TxRollbackInode) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)
	var dataLen uint32
	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data := make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}

	ino := NewInode(0, 0)
	if err = ino.Unmarshal(data); err != nil {
		return
	}
	i.inode = ino

	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data = make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}

	txInodeInfo := proto.NewTxInodeInfo("", 0, 0)
	if err = txInodeInfo.Unmarshal(data); err != nil {
		return
	}
	i.txInodeInfo = txInodeInfo

	if err = binary.Read(buff, binary.BigEndian, &i.rbType); err != nil {
		return
	}

	var quotaId uint32
	for {
		if buff.Len() == 0 {
			break
		}
		if err = binary.Read(buff, binary.BigEndian, &quotaId); err != nil {
			return
		}
		i.quotaIds = append(i.quotaIds, quotaId)
	}
	return
}

func NewTxRollbackInode(inode *Inode, quotaIds []uint32, txInodeInfo *proto.TxInodeInfo, rbType uint8) *TxRollbackInode {
	return &TxRollbackInode{
		inode:       inode,
		quotaIds:    quotaIds,
		txInodeInfo: txInodeInfo,
		rbType:      rbType,
	}
}

type TxRollbackDentry struct {
	dentry       *Dentry
	txDentryInfo *proto.TxDentryInfo
	rbType       uint8 //Rollback Type `
}

func (d *TxRollbackDentry) ToString() string {
	content := fmt.Sprintf("{dentry:[ParentId:%v, Name:%v, Inode:%v, Type:%v], rbType:%v, "+
		"txDentryInfo:[ParentId:%v, Name:%v, MpMembers:%v, TxID:%v, MpID:%v, CreateTime:%v, Timeout:%v]}",
		d.dentry.ParentId, d.dentry.Name, d.dentry.Inode, d.dentry.Type, d.rbType, d.txDentryInfo.ParentId, d.txDentryInfo.Name,
		d.txDentryInfo.MpMembers, d.txDentryInfo.TxID, d.txDentryInfo.MpID, d.txDentryInfo.CreateTime, d.txDentryInfo.Timeout)
	return content
}

// Less tests whether the current TxRollbackDentry item is less than the given one.
func (d *TxRollbackDentry) Less(than btree.Item) bool {
	td, ok := than.(*TxRollbackDentry)
	return ok && d.txDentryInfo.GetKey() < td.txDentryInfo.GetKey()
}

// Copy returns a copy of the TxRollbackDentry.
func (d *TxRollbackDentry) Copy() btree.Item {
	item := d.dentry.Copy()
	txDentryInfo := *d.txDentryInfo

	return &TxRollbackDentry{
		dentry:       item.(*Dentry),
		txDentryInfo: &txDentryInfo,
		rbType:       d.rbType,
	}
}

func (d *TxRollbackDentry) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 256))
	bs, err := d.dentry.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}

	bs, err = d.txDentryInfo.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, &d.rbType); err != nil {
		return
	}
	return buff.Bytes(), nil
}

func (d *TxRollbackDentry) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)
	var dataLen uint32
	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data := make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}

	dentry := &Dentry{}
	if err = dentry.Unmarshal(data); err != nil {
		return
	}
	d.dentry = dentry

	if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
		return
	}
	data = make([]byte, int(dataLen))
	if _, err = buff.Read(data); err != nil {
		return
	}

	txDentryInfo := proto.NewTxDentryInfo("", 0, "", 0)
	if err = txDentryInfo.Unmarshal(data); err != nil {
		return
	}
	d.txDentryInfo = txDentryInfo

	if err = binary.Read(buff, binary.BigEndian, &d.rbType); err != nil {
		return
	}
	return
}

func NewTxRollbackDentry(dentry *Dentry, txDentryInfo *proto.TxDentryInfo, rbType uint8) *TxRollbackDentry {
	return &TxRollbackDentry{
		dentry:       dentry,
		txDentryInfo: txDentryInfo,
		rbType:       rbType,
	}
}

//TM
type TransactionManager struct {
	//need persistence and sync to all the raft members of the mp
	txIdAlloc   *TxIDAllocator
	txTree      *BTree
	txProcessor *TransactionProcessor
	blacklist   *util.Set
	opLimiter   *rate.Limiter
	sync.RWMutex
}

//RM
type TransactionResource struct {
	txRbInodeTree  *BTree //key: inode id
	txRbDentryTree *BTree // key: parentId_name
	txProcessor    *TransactionProcessor
	sync.RWMutex
}

type TransactionProcessor struct {
	txManager  *TransactionManager  //TM
	txResource *TransactionResource //RM
	mp         *metaPartition
	mask       proto.TxOpMask
}

func (p *TransactionProcessor) Reset() {
	p.txManager.Reset()
	p.txResource.Reset()
	p.mp = nil
}

func (p *TransactionProcessor) Pause() bool {
	return p.mask == proto.TxPause
}

func NewTransactionManager(txProcessor *TransactionProcessor) *TransactionManager {
	txMgr := &TransactionManager{
		txIdAlloc:   newTxIDAllocator(),
		txTree:      NewBtree(),
		txProcessor: txProcessor,
		blacklist:   util.NewSet(),
		opLimiter:   rate.NewLimiter(rate.Inf, 128),
	}
	return txMgr
}

func NewTransactionResource(txProcessor *TransactionProcessor) *TransactionResource {
	txRsc := &TransactionResource{
		txRbInodeTree:  NewBtree(),
		txRbDentryTree: NewBtree(),
		txProcessor:    txProcessor,
	}
	return txRsc
}

func NewTransactionProcessor(mp *metaPartition) *TransactionProcessor {
	txProcessor := &TransactionProcessor{
		mp: mp,
	}
	txProcessor.txManager = NewTransactionManager(txProcessor)
	txProcessor.txResource = NewTransactionResource(txProcessor)

	if mp.config != nil {
		go txProcessor.txManager.processExpiredTransactions()
	}
	return txProcessor
}

func (tm *TransactionManager) setLimit(val int) string {
	if val > 0 {
		tm.opLimiter.SetLimit(rate.Limit(val))
		return fmt.Sprintf("%v", val)
	}
	tm.opLimiter.SetLimit(rate.Inf)
	return "unlimited"
}

func (tm *TransactionManager) Reset() {
	tm.blacklist.Clear()
	tm.Lock()
	tm.txIdAlloc.Reset()
	tm.txTree.Reset()
	tm.opLimiter.SetLimit(0)
	tm.txProcessor = nil
	tm.Unlock()
}

var test = false

func (tm *TransactionManager) processExpiredTransactions() {
	mpId := tm.txProcessor.mp.config.PartitionId
	log.LogInfof("processExpiredTransactions for mp[%v] started", mpId)
	clearInterval := time.Second * 60
	clearTimer := time.NewTimer(clearInterval)
	txCheckVal := time.Second * 3
	txCheckTimer := time.NewTimer(txCheckVal)

	defer func() {
		log.LogWarnf("processExpiredTransactions for mp[%v] exit", mpId)
		txCheckTimer.Stop()
		clearTimer.Stop()
		return
	}()

	for {
		select {
		case <-tm.txProcessor.mp.stopC:
			log.LogDebugf("[processExpiredTransactions] deleteWorker stop partition: %v", mpId)
			return
		default:
		}

		if _, ok := tm.txProcessor.mp.IsLeader(); !ok && !test {
			log.LogDebugf("processExpiredTransactions: not leader sleep 1s, mp %d", mpId)
			time.Sleep(time.Second * 10)
			continue
		}

		select {
		case <-tm.txProcessor.mp.stopC:
			log.LogWarnf("processExpiredTransactions for mp[%v] stopped", mpId)
			return
		case <-clearTimer.C:
			tm.blacklist.Clear()
			clearTimer.Reset(clearInterval)
			log.LogDebugf("processExpiredTransactions: blacklist cleared, mp %d", mpId)
		case <-txCheckTimer.C:
			if tm.txProcessor.Pause() {
				txCheckTimer.Reset(txCheckVal)
				continue
			}
			tm.processTx()
			txCheckTimer.Reset(txCheckVal)
		}
	}
}

func (tm *TransactionManager) processTx() {
	mpId := tm.txProcessor.mp.config.PartitionId
	start := time.Now()
	log.LogInfof("processTx: mp %v mask %v", mpId, proto.GetMaskString(tm.txProcessor.mask))
	defer func() {
		log.LogInfof("processTx: mp %d total cost %s", mpId, time.Since(start).String())
	}()

	limitCh := make(chan struct{}, 32)
	var wg sync.WaitGroup

	get := func() {
		wg.Add(1)
		limitCh <- struct{}{}
	}
	put := func() {
		<-limitCh
		wg.Done()
	}

	idx := 0
	f := func(i BtreeItem) bool {
		idx++
		if idx%100 == 0 {
			if _, ok := tm.txProcessor.mp.IsLeader(); !ok {
				log.LogWarnf("processExpiredTransactions for mp[%v] already not leader and break tx tree traverse",
					tm.txProcessor.mp.config.PartitionId)
				return false
			}
		}

		tx := i.(*proto.TransactionInfo)
		rollbackFunc := func(skipSetStat bool) {
			defer put()
			status, err := tm.rollbackTx(tx.TxID, skipSetStat)

			if err != nil || status != proto.OpOk {
				log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back failed, status(%v), err(%v)",
					tx, status, err)
				return
			}

			if log.EnableDebug() {
				log.LogDebugf("processExpiredTransactions: transaction (%v) expired, rolling back done", tx)
			}
		}

		commitFunc := func() {
			defer put()
			status, err := tm.commitTx(tx.TxID, true)
			if err != nil || status != proto.OpOk {
				log.LogWarnf("processExpiredTransactions: transaction (%v) expired, commit failed, status(%v), err(%v)",
					tx, status, err)
				return
			}

			if log.EnableDebug() {
				log.LogDebugf("processExpiredTransactions: transaction (%v) expired, commit done", tx)
			}
		}

		delFunc := func() {
			defer put()
			status, err := tm.delTxFromRM(tx.TxID)
			if err != nil || status != proto.OpOk {
				log.LogWarnf("processExpiredTransactions: delTxFromRM (%v) expired, commit failed, status(%v), err(%v)",
					tx, status, err)
				return
			}
			if log.EnableDebug() {
				log.LogDebugf("processExpiredTransactions: transaction (%v) delTxFromRM, commit done", tx)
			}
		}

		if tx.TmID != int64(mpId) {
			if !tx.CanDelete() {
				if log.EnableDebug() {
					log.LogDebugf("processExpiredTransactions: RM transaction (%v) is ongoing", tx)
				}
				return true
			}

			if log.EnableDebug() {
				log.LogDebugf("processExpiredTransactions: transaction (%v) can be deleted", tx)
			}
			get()
			go delFunc()
			return true
		}

		if tx.State == proto.TxStateCommit {
			if log.EnableDebug() {
				log.LogDebugf("processExpiredTransactions: transaction (%v) continue to commit...", tx)
			}
			get()
			go commitFunc()
			return true
		}

		if tx.State == proto.TxStateRollback {
			if log.EnableDebug() {
				log.LogDebugf("processExpiredTransactions: transaction (%v) continue to roll back...", tx)
			}
			get()
			go rollbackFunc(true)
			return true
		}

		if tx.State == proto.TxStatePreCommit {
			if !tx.IsExpired() {
				return true
			}

			if log.EnableDebug() {
				log.LogDebugf("processExpiredTransactions: transaction (%v) expired, rolling back...", tx)
			}
			get()
			go rollbackFunc(false)
			return true
		}

		if tx.IsDone() {
			if !tx.CanDelete() {
				if log.EnableDebug() {
					log.LogDebugf("processExpiredTransactions: transaction (%v) is ongoing", tx)
				}
				return true
			}

			if log.EnableDebug() {
				log.LogDebugf("processExpiredTransactions: transaction (%v) can be deleted", tx)
			}
			get()
			go delFunc()
			return true
		}

		log.LogCriticalf("processExpiredTransactions: transaction (%v) is in state failed", tx)
		return true
	}

	tm.txTree.GetTree().Ascend(f)
	wg.Wait()
}

func (tm *TransactionManager) nextTxID() string {
	id := tm.txIdAlloc.allocateTransactionID()
	txId := fmt.Sprintf("%d_%d", tm.txProcessor.mp.config.PartitionId, id)
	log.LogDebugf("nextTxID: txId:%v", txId)
	return txId
}

func (tm *TransactionManager) txInRMDone(txId string) bool {
	ifo := tm.getTransaction(txId)
	if ifo == nil || ifo.Finish() {
		log.LogWarnf("txInRMDone: tx in rm already done, txId %s, ifo %v", txId, ifo)
		return true
	}
	return false
}

func (tm *TransactionManager) getTransaction(txID string) (txInfo *proto.TransactionInfo) {
	txItem := proto.NewTxInfoBItem(txID)
	item := tm.txTree.Get(txItem)
	if item == nil {
		return nil
	}
	txInfo = item.(*proto.TransactionInfo)
	return
}

func (tm *TransactionManager) copyGetTx(txId string) (txInfo *proto.TransactionInfo) {
	txItem := proto.NewTxInfoBItem(txId)
	item := tm.txTree.CopyGet(txItem)
	if item == nil {
		return nil
	}

	txInfo = item.(*proto.TransactionInfo)
	return
}

func (tm *TransactionManager) updateTxIdCursor(txId string) (err error) {
	arr := strings.Split(txId, "_")
	if len(arr) != 2 {
		return fmt.Errorf("updateTxId: tx[%v] is invalid", txId)
	}
	id, err := strconv.ParseUint(arr[1], 10, 64)
	if err != nil {
		return fmt.Errorf("updateTxId: tx[%v] is invalid", txId)
	}
	if id > tm.txIdAlloc.getTransactionID() {
		tm.txIdAlloc.setTransactionID(id)
	}
	return nil
}

func (tm *TransactionManager) addTxInfo(txInfo *proto.TransactionInfo) {
	tm.txTree.ReplaceOrInsert(txInfo, true)
}

//TM register a transaction, process client transaction
func (tm *TransactionManager) registerTransaction(txInfo *proto.TransactionInfo) (err error) {

	if uint64(txInfo.TmID) == tm.txProcessor.mp.config.PartitionId {
		if err := tm.updateTxIdCursor(txInfo.TxID); err != nil {
			log.LogErrorf("updateTxIdCursor failed, txInfo %s, err %s", txInfo.String(), err.Error())
			return err
		}

		for _, inode := range txInfo.TxInodeInfos {
			inode.SetCreateTime(txInfo.CreateTime)
			inode.SetTimeout(txInfo.Timeout)
			inode.SetTxId(txInfo.TxID)
		}

		for _, dentry := range txInfo.TxDentryInfos {
			dentry.SetCreateTime(txInfo.CreateTime)
			dentry.SetTimeout(txInfo.Timeout)
			dentry.SetTxId(txInfo.TxID)
		}
	}

	if info := tm.getTransaction(txInfo.TxID); info != nil {
		return fmt.Errorf("tx is already exist, txId %s, info %v", txInfo.TxID, info.String())
	}

	tm.addTxInfo(txInfo)

	if log.EnableDebug() {
		log.LogDebugf("registerTransaction: txInfo(%v)", txInfo)
	}

	return
}

func (tm *TransactionManager) deleteTxInfo(txId string) (status uint8) {
	tm.Lock()
	defer tm.Unlock()
	status = proto.OpOk
	txItem := proto.NewTxInfoBItem(txId)
	item := tm.txTree.Delete(txItem)
	if log.EnableDebug() {
		log.LogDebugf("deleteTxInfo: tx[%v] is deleted, item %v", txId, item)
	}
	return
}

func (tm *TransactionManager) rollbackTxInfo(txId string) (status uint8) {
	tm.Lock()
	defer tm.Unlock()
	status = proto.OpOk

	tx := tm.getTransaction(txId)
	if tx == nil {
		status = proto.OpTxInfoNotExistErr
		log.LogWarnf("rollbackTxInfo: rollback tx[%v] failed, not found", txId)
		return
	}

	tx.State = proto.TxStateRollbackDone
	tx.DoneTime = time.Now().Unix()
	log.LogDebugf("rollbackTxInfo: tx[%v] is rolled back", tx)
	return
}

func (tm *TransactionManager) commitTxInfo(txId string) (status uint8, err error) {
	tm.Lock()
	defer tm.Unlock()
	status = proto.OpOk
	tx := tm.getTransaction(txId)
	if tx == nil {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("commitTxInfo: commit tx[%v] failed, not found", txId)
		return
	}

	tx.State = proto.TxStateCommitDone
	tx.DoneTime = time.Now().Unix()
	log.LogDebugf("commitTxInfo: tx[%v] is committed", tx)
	return
}

func buildTxPacket(data interface{}, mp uint64, op uint8) (pkt *proto.Packet, err error) {
	pkt = proto.NewPacketReqID()
	pkt.Opcode = op
	pkt.PartitionID = mp
	err = pkt.MarshalData(data)
	if err != nil {
		errInfo := fmt.Sprintf("buildTxPacket: marshal txInfo [%v] failed", data)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return nil, err
	}

	return
}

func (tm *TransactionManager) setTransactionState(txId string, state int32) (status uint8, err error) {
	var val []byte
	var resp interface{}
	status = proto.OpOk

	stateReq := &proto.TxSetStateRequest{
		TxID:  txId,
		State: state,
	}
	val, _ = json.Marshal(stateReq)

	resp, err = tm.txProcessor.mp.submit(opFSMTxSetState, val)
	if err != nil {
		log.LogWarnf("setTransactionState: set transaction[%v] state to [%v] failed, err[%v]", txId, state, err)
		return proto.OpTxSetStateErr, err
	}
	status = resp.(uint8)

	if status != proto.OpOk {
		errInfo := fmt.Sprintf("setTransactionState: set transaction[%v] state to [%v] failed", txId, state)
		err = errors.New(errInfo)
		log.LogWarnf("%v", errInfo)
	}
	return
}

func (tm *TransactionManager) delTxFromRM(txId string) (status uint8, err error) {
	req := proto.TxApplyRequest{
		TxID: txId,
	}
	val, err := json.Marshal(req)
	if err != nil {
		return
	}

	resp, err := tm.txProcessor.mp.submit(opFSMTxDelete, val)
	if err != nil {
		log.LogWarnf("delTxFromRM: delTxFromRM transaction[%v] failed, err[%v]", txId, err)
		return proto.OpTxCommitErr, err
	}

	status = resp.(uint8)
	if log.EnableDebug() {
		log.LogDebugf("delTxFromRM: tx[%v] is deleted successfully, status (%s)", txId, proto.GetStatusStr(status))
	}

	return
}

func (tm *TransactionManager) commitTx(txId string, skipSetStat bool) (status uint8, err error) {
	tx := tm.getTransaction(txId)
	if tx == nil {
		status = proto.OpTxInfoNotExistErr
		log.LogWarnf("commitTx: tx[%v] not found, already success", txId)
		return
	}

	if tx.IsDone() {
		status = proto.OpTxInfoNotExistErr
		log.LogWarnf("commitTx: tx[%v] is already commit", txId)
		return
	}

	//1.set transaction to TxStateCommit
	if !skipSetStat && tx.State != proto.TxStateCommit {
		status, err = tm.setTransactionState(txId, proto.TxStateCommit)
		if status != proto.OpOk {
			log.LogWarnf("commitTx: set transaction[%v] state to TxStateCommit failed", tx)
			return
		}
	}

	//2. notify all related RMs that a transaction is completed
	status = tm.sendToRM(tx, proto.OpTxCommitRM)
	if status != proto.OpOk {
		return
	}

	//3. TM commit the transaction
	req := proto.TxApplyRequest{
		TxID: txId,
	}
	val, err := json.Marshal(req)
	if err != nil {
		return
	}

	resp, err := tm.txProcessor.mp.submit(opFSMTxCommit, val)
	if err != nil {
		log.LogWarnf("commitTx: commit transaction[%v] failed, err[%v]", txId, err)
		return proto.OpTxCommitErr, err
	}

	status = resp.(uint8)
	log.LogDebugf("commitTx: tx[%v] is commited successfully", txId)

	return
}

func (tm *TransactionManager) sendToRM(txInfo *proto.TransactionInfo, op uint8) (status uint8) {
	status = proto.OpOk
	mpIfos := txInfo.GroupByMp()
	statusCh := make(chan uint8, len(mpIfos))
	wg := sync.WaitGroup{}
	mp := tm.txProcessor.mp

	for mpId, ifo := range mpIfos {
		req := &proto.TxApplyRMRequest{
			VolName:         mp.config.VolName,
			PartitionID:     mpId,
			TransactionInfo: txInfo,
		}

		wg.Add(1)

		pkt, _ := buildTxPacket(req, mpId, op)
		if mp.config.PartitionId == mpId {
			pt := &Packet{*pkt}
			go func() {
				defer wg.Done()
				var err error
				if op == proto.OpTxCommitRM {
					err = mp.TxCommitRM(req, pt)
				} else {
					err = mp.TxRollbackRM(req, pt)
				}
				statusCh <- pt.ResultCode
				if pt.ResultCode != proto.OpOk {
					log.LogWarnf("sendToRM: invoke TxCommitRM failed, ifo %v, pkt %s, err %v", txInfo, pt.GetResultMsg(), err)
				}
			}()
			continue
		}

		members := ifo.Members
		go func() {
			defer wg.Done()
			status := tm.txSendToMpWithAddrs(members, pkt)
			if status != proto.OpOk {
				log.LogWarnf("sendToRM: send to rm failed, addr %s, pkt %s", members, string(pkt.Data))
			}
			statusCh <- status
		}()
	}

	wg.Wait()
	close(statusCh)

	updateStatus := func(st uint8) uint8 {
		if st == proto.OpTxConflictErr || st == proto.OpTxInfoNotExistErr {
			log.LogWarnf("sendToRM: might have already been committed, tx[%v], status (%s)", txInfo, proto.GetStatusStr(st))
			return proto.OpOk
		} else if st == proto.OpTxRbInodeNotExistErr || st == proto.OpTxRbDentryNotExistErr {
			log.LogWarnf("sendToRM: already done before or not add, tx[%v], status (%s)", txInfo, proto.GetStatusStr(st))
			return proto.OpOk
		} else {
			return st
		}
	}

	for st := range statusCh {
		t := updateStatus(st)
		if t != proto.OpOk {
			return t
		}
	}

	return status
}

func (tm *TransactionManager) rollbackTx(txId string, skipSetStat bool) (status uint8, err error) {
	status = proto.OpOk

	tx := tm.getTransaction(txId)
	if tx == nil {
		log.LogWarnf("commitTx: tx[%v] not found, already success", txId)
		return
	}

	if tx.IsDone() {
		status = proto.OpOk
		log.LogWarnf("commitTx: tx[%v] is already commit", txId)
		return
	}

	//1.set transaction to TxStateRollback
	if !skipSetStat && tx.State != proto.TxStateRollback {
		status, err = tm.setTransactionState(txId, proto.TxStateRollback)
		if status != proto.OpOk {
			log.LogWarnf("commitTransaction: set transaction[%v] state to TxStateCommit failed", tx)
			return
		}
	}

	//2. notify all related RMs that a transaction is completed
	status = tm.sendToRM(tx, proto.OpTxRollbackRM)
	if status != proto.OpOk {
		return
	}

	req := proto.TxApplyRequest{
		TxID: txId,
	}
	val, err := json.Marshal(req)
	if err != nil {
		return
	}

	resp, err := tm.txProcessor.mp.submit(opFSMTxRollback, val)

	if err != nil {
		log.LogWarnf("commitTx: rollback transaction[%v]  failed, err[%v]", txId, err)
		return proto.OpTxCommitErr, err
	}

	status = resp.(uint8)
	log.LogDebugf("commitTx: tx[%v] is rollback successfully, msg %s", txId, proto.GetStatusStr(status))

	return
}

func (tm *TransactionManager) sendPacketToMP(addr string, p *proto.Packet) (err error) {
	var (
		mConn *net.TCPConn
		reqID = p.ReqID
		reqOp = p.Opcode
	)

	connPool := tm.txProcessor.mp.manager.connPool
	defer func() {
		connPool.PutConnect(mConn, err != nil)
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			log.LogErrorf("[sendPacketToMP]: req: %d - %v, %v, packet(%v)", p.GetReqID(),
				p.GetOpMsg(), err, p)
			return
		}
	}()

	mConn, err = connPool.GetConnect(addr)
	if err != nil {
		return
	}

	if err = p.WriteToConn(mConn); err != nil {
		return
	}

	// read connection from the master
	if err = p.ReadFromConn(mConn, proto.ReadDeadlineTime); err != nil {
		return
	}

	if reqID != p.ReqID || reqOp != p.Opcode {
		err = fmt.Errorf("sendPacketToMP: send and received packet mismatch: req(%v_%v) resp(%v_%v)",
			reqID, reqOp, p.ReqID, p.Opcode)
		return
	}

	if log.EnableDebug() {
		log.LogDebugf("[sendPacketToMP] req: %d - %v, resp: %v, packet(%v)", p.GetReqID(), p.GetOpMsg(),
			p.GetResultMsg(), p)
	}

	return
}

func (tm *TransactionManager) txSendToMpWithAddrs(addrStr string, p *proto.Packet) (status uint8) {
	addrs := strings.Split(addrStr, ",")
	var err error

	skippedAddrs := make([]string, 0)
	for _, addr := range addrs {
		if tm.blacklist.Has(addr) {
			log.LogWarnf("txSendToMpWithAddrs: addr[%v] is already blacklisted, retry another addr, p %s", addr, string(p.Data))
			skippedAddrs = append(skippedAddrs, addr)
			continue
		}

		newPkt := p.GetCopy()
		err = tm.sendPacketToMP(addr, newPkt)
		if err != nil {
			tm.blacklist.Add(addr)
			log.LogWarnf("txSendToMpWithAddrs: send to %v failed, err(%s), add to blacklist and retry another addr, p %s",
				addr, err.Error(), string(p.Data))
			continue
		}

		status := newPkt.ResultCode
		if status == proto.OpErr || status == proto.OpAgain {
			log.LogWarnf("txSendToMpWithAddrs: sendPacketToMp failed, addr %s, msg %s, data %s, status(%s)",
				addr, newPkt.GetResultMsg(), string(p.Data), proto.GetStatusStr(status))
			continue
		}

		if status == proto.OpOk {
			if log.EnableDebug() {
				log.LogDebugf("txSendToMpWithAddrs: send to %v done with status[%v], tx[%s]",
					addr, status, string(p.Data))
			}
			err = nil
			return status
		}

		log.LogWarnf("txSendToMpWithAddrs: sendPacketToMp failed, addr %s, msg %s, data %s", addr, newPkt.GetResultMsg(), string(p.Data))
		return status
	}

	// try use skipped addr
	for _, addr := range skippedAddrs {
		newPkt := p.GetCopy()
		err = tm.sendPacketToMP(addr, newPkt)
		if err != nil {
			log.LogWarnf("txSendToMpWithAddrs: send to %v failed, err(%s), add to blacklist and retry another addr, p %s",
				addr, err.Error(), string(p.Data))
			continue
		}

		status := newPkt.ResultCode
		if status == proto.OpErr || status == proto.OpAgain {
			log.LogWarnf("txSendToMpWithAddrs: sendPacketToMp failed, addr %s, msg %s, data %s, status(%s)",
				addr, newPkt.GetResultMsg(), string(p.Data), proto.GetStatusStr(status))
			continue
		}

		if status == proto.OpOk {
			if log.EnableDebug() {
				log.LogDebugf("txSendToMpWithAddrs: send to %v done with status[%v], tx[%s]",
					addr, status, string(p.Data))
			}
			err = nil
			return status
		}

		log.LogWarnf("txSendToMpWithAddrs: sendPacketToMp failed, addr %s, msg %s, data %s", addr, newPkt.GetResultMsg(), string(p.Data))
		return status
	}

	log.LogWarnf("txSendToMpWithAddrs: after retry still failed, return opAgain, pkt %s, addrs %v, err %v", string(p.Data), addrs, err)
	return proto.OpAgain
}

func (tm *TransactionManager) txSetState(req *proto.TxSetStateRequest) (status uint8, err error) {
	tm.Lock()
	defer tm.Unlock()
	status = proto.OpOk

	txItem := proto.NewTxInfoBItem(req.TxID)
	item := tm.txTree.CopyGet(txItem)
	if item == nil {
		status = proto.OpTxInfoNotExistErr
		errInfo := fmt.Sprintf("txSetState: set state failed, req[%v] tx not existed", req)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}
	txInfo := item.(*proto.TransactionInfo)

	if req.State < proto.TxStateCommit || req.State > proto.TxStateFailed {
		status = proto.OpTxSetStateErr
		errInfo := fmt.Sprintf("txSetState: set state failed, wrong state, req[%v]", req)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	if req.State == proto.TxStateCommit && txInfo.State != proto.TxStateCommit && txInfo.State != proto.TxStatePreCommit {
		status = proto.OpTxSetStateErr
		errInfo := fmt.Sprintf("txSetState: set state failed, wrong state, tx state[%v], req state[%v], tx[%v]",
			txInfo.State, req.State, req.TxID)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	if req.State == proto.TxStateRollback && txInfo.State != proto.TxStateRollback && txInfo.State != proto.TxStatePreCommit {
		status = proto.OpTxSetStateErr
		errInfo := fmt.Sprintf("txSetState: set state failed, wrong state, tx state[%v], req state[%v], tx[%v]",
			txInfo.State, req.State, req.TxID)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	log.LogDebugf("txSetState: set tx state from [%v] to [%v], tx[%v]", txInfo.State, req.State, req.TxID)
	txInfo.State = req.State
	return
}

func (tr *TransactionResource) Reset() {
	tr.Lock()
	defer tr.Unlock()
	tr.txRbInodeTree.Reset()
	tr.txRbDentryTree.Reset()
	tr.txProcessor = nil
}

//check if item(inode, dentry) is in transaction for modifying
func (tr *TransactionResource) isInodeInTransction(ino *Inode) (inTx bool, txID string) {
	//return true only if specified inode is in an ongoing transaction(not expired yet)
	tr.Lock()
	defer tr.Unlock()

	if rbInode := tr.getTxRbInode(ino.Inode); rbInode != nil {
		inTx = true
		if rbInode.txInodeInfo != nil {
			txID = rbInode.txInodeInfo.TxID
		}
		return
	}
	return false, ""
}

func (tr *TransactionResource) isDentryInTransction(dentry *Dentry) (inTx bool, txID string) {
	tr.Lock()
	defer tr.Unlock()

	if rbDentry := tr.getTxRbDentry(dentry.ParentId, dentry.Name); rbDentry != nil {
		inTx = true
		if rbDentry.txDentryInfo != nil {
			txID = rbDentry.txDentryInfo.TxID
		}
		return
	}
	return false, ""
}

func (tr *TransactionResource) getTxRbInode(ino uint64) (rbInode *TxRollbackInode) {
	keyNode := &TxRollbackInode{
		inode: NewInode(ino, 0),
	}
	item := tr.txRbInodeTree.Get(keyNode)
	if item == nil {
		return nil
	}
	rbInode = item.(*TxRollbackInode)
	return
}

func (tr *TransactionResource) copyGetTxRbInode(ino uint64) (rbInode *TxRollbackInode) {
	keyNode := &TxRollbackInode{
		inode: NewInode(ino, 0),
	}
	item := tr.txRbInodeTree.CopyGet(keyNode)
	if item == nil {
		return nil
	}
	rbInode = item.(*TxRollbackInode)
	return
}

func (tr *TransactionResource) deleteTxRollbackInode(ino uint64, txId string) (status uint8) {
	tr.Lock()
	defer tr.Unlock()

	keyNode := &TxRollbackInode{
		txInodeInfo: proto.NewTxInodeInfo("", ino, 0),
	}

	item := tr.txRbInodeTree.Get(keyNode)
	if item == nil {
		log.LogWarnf("deleteTxRollbackInode: rollback inode may be already been deleted, inode %d, txId %s",
			ino, txId)
		return proto.OpTxRbInodeNotExistErr
	}

	if item.(*TxRollbackInode).txInodeInfo.TxID != txId {
		log.LogWarnf("deleteTxRollbackInode: rollback dentry is already been update by other, txId %s, item %v",
			txId, item)
		return proto.OpTxRbDentryNotExistErr
	}

	tr.txRbInodeTree.Delete(item)
	return proto.OpOk
}

//RM add an `TxRollbackInode` into `txRollbackInodes`
func (tr *TransactionResource) addTxRollbackInode(rbInode *TxRollbackInode) (status uint8) {
	tr.Lock()
	defer tr.Unlock()

	oldRbInode := tr.getTxRbInode(rbInode.inode.Inode)
	if oldRbInode != nil {
		if oldRbInode.txInodeInfo.TxID == rbInode.txInodeInfo.TxID {
			log.LogWarnf("addTxRollbackInode: rollback inode [ino(%v) txID(%v)] is already exists",
				rbInode.inode.Inode, rbInode.txInodeInfo.TxID)
			return proto.OpExistErr
		} else {
			log.LogErrorf("addTxRollbackInode: rollback inode [ino(%v) txID(%v)] "+
				"is conflicted with inode [ino(%v) txID(%v)]",
				rbInode.inode.Inode, rbInode.txInodeInfo.TxID, oldRbInode.inode.Inode, oldRbInode.txInodeInfo.TxID)
			return proto.OpTxConflictErr
		}
	}

	tr.txRbInodeTree.ReplaceOrInsert(rbInode, true)
	log.LogDebugf("addTxRollbackInode: rollback inode [ino(%v) txID(%v)] is added", rbInode.inode.Inode, rbInode.txInodeInfo.TxID)
	return proto.OpOk
}

func (tr *TransactionResource) getTxRbDentry(pId uint64, name string) *TxRollbackDentry {

	keyNode := &TxRollbackDentry{
		txDentryInfo: proto.NewTxDentryInfo("", pId, name, 0),
	}
	item := tr.txRbDentryTree.Get(keyNode)
	if item == nil {
		return nil
	}

	return item.(*TxRollbackDentry)
}

func (tr *TransactionResource) deleteTxRollbackDentry(pid uint64, name, txId string) (status uint8) {
	tr.Lock()
	defer tr.Unlock()

	keyNode := &TxRollbackDentry{
		txDentryInfo: proto.NewTxDentryInfo("", pid, name, 0),
	}

	item := tr.txRbDentryTree.Get(keyNode)
	if item == nil {
		log.LogWarnf("deleteTxRollbackDentry: rollback dentry may be already been deleted, pid %d, name %s, txId %s",
			pid, name, txId)
		return proto.OpTxRbDentryNotExistErr
	}

	if item.(*TxRollbackDentry).txDentryInfo.TxID != txId {
		log.LogWarnf("deleteTxRollbackDentry: rollback dentry is already been update by other, txId %s, item %v",
			txId, name)
		return proto.OpTxRbDentryNotExistErr
	}

	tr.txRbDentryTree.Delete(item)
	return proto.OpOk
}

//RM add a `TxRollbackDentry` into `txRollbackDentries`
func (tr *TransactionResource) addTxRollbackDentry(rbDentry *TxRollbackDentry) (status uint8) {
	tr.Lock()
	defer tr.Unlock()

	oldRbDentry := tr.getTxRbDentry(rbDentry.txDentryInfo.ParentId, rbDentry.dentry.Name)
	if oldRbDentry != nil {
		if oldRbDentry.txDentryInfo.TxID == rbDentry.txDentryInfo.TxID {
			log.LogWarnf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v)] is already exists",
				rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID)
			return proto.OpExistErr
		}
		log.LogWarnf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v) rbType(%v)] "+
			"is conflicted with dentry [pino(%v) name(%v)  txID(%v) rbType(%v)]",
			rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID, rbDentry.rbType,
			oldRbDentry.dentry.ParentId, oldRbDentry.dentry.Name, oldRbDentry.txDentryInfo.TxID, oldRbDentry.rbType)
		return proto.OpTxConflictErr
	}

	tr.txRbDentryTree.ReplaceOrInsert(rbDentry, true)
	log.LogDebugf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v) rbType(%v)] is added",
		rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID, rbDentry.rbType)
	return proto.OpOk
}

func (tr *TransactionResource) rollbackInodeInternal(rbInode *TxRollbackInode) (status uint8, err error) {
	status = proto.OpOk
	mp := tr.txProcessor.mp
	switch rbInode.rbType {
	case TxAdd:
		var ino *Inode
		item := mp.inodeTree.CopyGet(rbInode.inode)
		if item != nil {
			ino = item.(*Inode)
		}

		if item == nil || ino.IsTempFile() || ino.ShouldDelete() {
			mp.freeList.Remove(rbInode.inode.Inode)
			if mp.uidManager != nil {
				mp.uidManager.addUidSpace(rbInode.inode.Uid, rbInode.inode.Inode, rbInode.inode.Extents.eks)
			}
			if mp.mqMgr != nil && len(rbInode.quotaIds) > 0 && item == nil {
				mp.setInodeQuota(rbInode.quotaIds, rbInode.inode.Inode)
				for _, quotaId := range rbInode.quotaIds {
					mp.mqMgr.updateUsedInfo(int64(rbInode.inode.Size), 1, quotaId)
				}
			}
			mp.inodeTree.ReplaceOrInsert(rbInode.inode, true)
		} else {
			ino.IncNLink()
		}

	case TxDelete:
		if rsp := tr.txProcessor.mp.getInode(rbInode.inode); rsp.Status == proto.OpOk {
			if tr.txProcessor.mp.mqMgr != nil && len(rbInode.quotaIds) > 0 {
				for _, quotaId := range rbInode.quotaIds {
					tr.txProcessor.mp.mqMgr.updateUsedInfo(-1*int64(rbInode.inode.Size), -1, quotaId)
				}
			}
			tr.txProcessor.mp.fsmUnlinkInode(rbInode.inode, 0)
			tr.txProcessor.mp.fsmEvictInode(rbInode.inode)
		}

	default:
		status = proto.OpTxRollbackUnknownRbType
		err = fmt.Errorf("rollbackInode: unknown rbType %d", rbInode.rbType)
		return
	}
	tr.txRbInodeTree.Delete(rbInode)
	return
}

//RM roll back an inode, retry if error occours
func (tr *TransactionResource) rollbackInode(req *proto.TxInodeApplyRequest) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	rbInode := tr.getTxRbInode(req.Inode)
	if rbInode == nil {
		status = proto.OpTxRbInodeNotExistErr
		errInfo := fmt.Sprintf("rollbackInode: roll back inode[%v] failed, txID[%v], rb inode not found", req.Inode, req.TxID)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	if rbInode.txInodeInfo.TxID != req.TxID {
		status = proto.OpTxConflictErr
		errInfo := fmt.Sprintf("rollbackInode: txID %v is not matching txInodeInfo txID %v", req.TxID, rbInode.txInodeInfo.TxID)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	status, err = tr.rollbackInodeInternal(rbInode)
	if err != nil {
		log.LogErrorf("rollbackInode: inode[%v] roll back failed in tx[%v], rbType[%v]", req.Inode, req.TxID, rbInode.rbType)
	} else {
		log.LogDebugf("rollbackInode: inode[%v] is rolled back in tx[%v], rbType[%v]", req.Inode, req.TxID, rbInode.rbType)
	}

	return
}

func (tr *TransactionResource) rollbackDentryInternal(rbDentry *TxRollbackDentry) (status uint8, err error) {
	defer func() {
		if status != proto.OpOk {
			log.LogErrorf("rollbackDentryInternal: rollback dentry failed, ifo %v", rbDentry.txDentryInfo)
		}
	}()
	status = proto.OpOk
	switch rbDentry.rbType {
	case TxAdd:
		// need to be true to assert link not change.
		status = tr.txProcessor.mp.fsmCreateDentry(rbDentry.dentry, true)
	case TxDelete:
		resp := tr.txProcessor.mp.fsmDeleteDentry(rbDentry.dentry, true)
		status = resp.Status
	case TxUpdate:
		resp := tr.txProcessor.mp.fsmUpdateDentry(rbDentry.dentry)
		status = resp.Status
	default:
		status = proto.OpTxRollbackUnknownRbType
		err = fmt.Errorf("rollbackDentry: unknown rbType %d", rbDentry.rbType)
		return
	}

	tr.txRbDentryTree.Delete(rbDentry)
	return
}

//RM roll back a dentry, retry if error occours
func (tr *TransactionResource) rollbackDentry(req *proto.TxDentryApplyRequest) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	rbDentry := tr.getTxRbDentry(req.Pid, req.Name)
	if rbDentry == nil {
		status = proto.OpTxRbDentryNotExistErr
		errInfo := fmt.Sprintf("rollbackDentry: roll back dentry[%v_%v] failed, rb inode not found, txID[%v]",
			req.Pid, req.Name, req.TxID)
		err = errors.New(errInfo)
		log.LogWarnf("%v", errInfo)
		return
	}

	if rbDentry.txDentryInfo.TxID != req.TxID {
		status = proto.OpTxConflictErr
		errInfo := fmt.Sprintf("rollbackDentry: txID %v is not matching txInodeInfo txID %v", req.TxID, rbDentry.txDentryInfo.TxID)
		err = errors.New(errInfo)
		log.LogWarnf("%v", errInfo)
		return
	}

	status, err = tr.rollbackDentryInternal(rbDentry)
	if err != nil {
		log.LogErrorf("rollbackDentry: denKey[%v] roll back failed in tx[%v], rbType[%v]",
			rbDentry.txDentryInfo.GetKey(), req.TxID, rbDentry.rbType)
	} else {
		log.LogDebugf("rollbackDentry: denKey[%v] is rolled back in tx[%v], rbType[%v]",
			rbDentry.txDentryInfo.GetKey(), req.TxID, rbDentry.rbType)
	}

	return
}

//RM simplely remove the inode from TransactionResource
func (tr *TransactionResource) commitInode(txID string, inode uint64) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	rbInode := tr.getTxRbInode(inode)
	if rbInode == nil {
		status = proto.OpTxRbInodeNotExistErr
		errInfo := fmt.Sprintf("commitInode: commit inode[%v] failed, rb inode not found", inode)
		err = errors.New(errInfo)
		log.LogWarnf("%v", errInfo)
		return
	}

	if rbInode.txInodeInfo.TxID != txID {
		status = proto.OpTxConflictErr
		errInfo := fmt.Sprintf("commitInode: txID %v is not matching txInodeInfo txID %v", txID, rbInode.txInodeInfo.TxID)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	tr.txRbInodeTree.Delete(rbInode)
	log.LogDebugf("commitInode: inode[%v] is committed", inode)
	return
}

//RM simplely remove the dentry from TransactionResource
func (tr *TransactionResource) commitDentry(txID string, pId uint64, name string) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk

	rbDentry := tr.getTxRbDentry(pId, name)
	if rbDentry == nil {
		status = proto.OpTxRbDentryNotExistErr
		errInfo := fmt.Sprintf("commitDentry: commit dentry[%v_%v] failed, rb dentry not found", pId, name)
		err = errors.New(errInfo)
		log.LogWarnf("%v", errInfo)
		return
	}

	if rbDentry.txDentryInfo.TxID != txID {
		status = proto.OpTxConflictErr
		errInfo := fmt.Sprintf("commitDentry: txID %v is not matching txDentryInfo txID %v", txID, rbDentry.txDentryInfo.TxID)
		err = errors.New(errInfo)
		log.LogWarnf("%v", errInfo)
		return
	}

	tr.txRbDentryTree.Delete(rbDentry)
	// unlink parent inode
	if rbDentry.rbType == TxAdd {
		parInode := NewInode(pId, 0)
		st := tr.txProcessor.mp.fsmUnlinkInode(parInode, 0)
		if st.Status != proto.OpOk {
			log.LogWarnf("commitDentry: try unlink parent inode failed, txId %s, inode %v", txID, parInode)
			return
		}
	}

	log.LogDebugf("commitDentry: dentry[%v] is committed", rbDentry.txDentryInfo.GetKey())
	return
}
