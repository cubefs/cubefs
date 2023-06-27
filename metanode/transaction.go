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
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
)

//Rollback Type
const (
	TxNoOp   uint32 = 0
	TxUpdate uint32 = 1
	TxDelete uint32 = 2
	TxAdd    uint32 = 3
)

//Rollback initiator
const (
	RbFromDummy  uint32 = 0
	RbFromTM     uint32 = 1
	RbFromClient uint32 = 2
)

func (i *TxRollbackInode) ToString() string {
	content := fmt.Sprintf("{inode:[ino:%v, type:%v, nlink:%v], quotaIds:%v, rbType:%v, rbInitiator:%v, rbPlaceholder:%v, rbPlaceholderTimestamp:%v, "+
		"txInodeInfo:[Ino:%v, MpID:%v, CreateTime:%v, Timeout:%v, TxID:%v, MpMembers:%v]}",
		i.inode.Inode, i.inode.Type, i.inode.NLink, i.quotaIds, i.rbType, i.rbInitiator, i.rbPlaceholder, i.rbPlaceholderTimestamp,
		i.txInodeInfo.Ino, i.txInodeInfo.MpID, i.txInodeInfo.CreateTime, i.txInodeInfo.Timeout, i.txInodeInfo.TxID, i.txInodeInfo.MpMembers)
	return content
}

type TxRollbackInode struct {
	inode                  *Inode
	txInodeInfo            *proto.TxInodeInfo
	rbType                 uint32 //Rollback Type
	rbInitiator            uint32 //TM, Client
	rbPlaceholder          bool   //default false
	rbPlaceholderTimestamp int64
	quotaIds               []uint32
}

// Less tests whether the current TxRollbackInode item is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (i *TxRollbackInode) Less(than btree.Item) bool {
	ti, ok := than.(*TxRollbackInode)
	return ok && i.inode.Inode < ti.inode.Inode
}

// Copy returns a copy of the TxRollbackInode.
func (i *TxRollbackInode) Copy() btree.Item {

	item := i.inode.Copy()
	txInodeInfo := *i.txInodeInfo

	return &TxRollbackInode{
		inode:                  item.(*Inode),
		quotaIds:               i.quotaIds,
		txInodeInfo:            &txInodeInfo,
		rbType:                 i.rbType,
		rbInitiator:            i.rbInitiator,
		rbPlaceholder:          i.rbPlaceholder,
		rbPlaceholderTimestamp: i.rbPlaceholderTimestamp,
	}
}

func (i *TxRollbackInode) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 256))
	bs, err := i.inode.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}
	bs, err = i.txInodeInfo.Marshal()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
		return nil, err
	}
	if _, err := buff.Write(bs); err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, &i.rbType); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.rbInitiator); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.rbPlaceholder); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.rbPlaceholderTimestamp); err != nil {
		panic(err)
	}
	quotaBytes := bytes.NewBuffer(make([]byte, 0, 128))
	for _, quotaId := range i.quotaIds {
		if err = binary.Write(quotaBytes, binary.BigEndian, quotaId); err != nil {
			panic(err)
		}
	}
	buff.Write(quotaBytes.Bytes())
	return buff.Bytes(), nil
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
	if err = binary.Read(buff, binary.BigEndian, &i.rbInitiator); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.rbPlaceholder); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.rbPlaceholderTimestamp); err != nil {
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

/*func (i *TxRollbackInode) IsExpired() (expired bool) {
	if !i.rbPlaceholder {
		return false
	}
	now := time.Now().Unix()
	if now < i.rbPlaceholderTimestamp {
		log.LogErrorf("rbInode placeholder time out error, now[%v], rbPlaceholderTimestamp[%v]",
			now, i.rbPlaceholderTimestamp)
		return true
	}
	expired = proto.MaxReplaceholderTimeout <= now-i.rbPlaceholderTimestamp
	if expired {
		log.LogDebugf("rbInode placeholder [%v] is expired, now[%v], rbPlaceholderTimestamp[%v]",
			i, now, i.rbPlaceholderTimestamp)
	}
	return expired
}*/

func NewTxRollbackInode(inode *Inode, quotaIds []uint32, txInodeInfo *proto.TxInodeInfo, rbType uint32) *TxRollbackInode {
	return &TxRollbackInode{
		inode:       inode,
		quotaIds:    quotaIds,
		txInodeInfo: txInodeInfo,
		rbType:      rbType,
	}
}

func NewTxRbInodePlaceholder(ino uint64, txId string) *TxRollbackInode {

	txInodeInfo := &proto.TxInodeInfo{
		Ino:  ino,
		TxID: txId,
	}
	return &TxRollbackInode{
		inode:                  NewInode(ino, 0),
		txInodeInfo:            txInodeInfo,
		rbInitiator:            RbFromTM,
		rbPlaceholder:          true,
		rbPlaceholderTimestamp: time.Now().Unix(),
	}
}

type TxRollbackDentry struct {
	dentry                 *Dentry
	txDentryInfo           *proto.TxDentryInfo
	rbType                 uint32 //Rollback Type
	rbInitiator            uint32 //TM, Client
	rbPlaceholder          bool   //default false
	rbPlaceholderTimestamp int64
}

func (d *TxRollbackDentry) ToString() string {
	content := fmt.Sprintf("{dentry:[ParentId:%v, Name:%v, Inode:%v, Type:%v], rbType:%v, rbInitiator:%v, rbPlaceholder:%v, rbPlaceholderTimestamp:%v, "+
		"txDentryInfo:[ParentId:%v, Name:%v, MpMembers:%v, TxID:%v, MpID:%v, CreateTime:%v, Timeout:%v]}",
		d.dentry.ParentId, d.dentry.Name, d.dentry.Inode, d.dentry.Type, d.rbType, d.rbInitiator, d.rbPlaceholder, d.rbPlaceholderTimestamp,
		d.txDentryInfo.ParentId, d.txDentryInfo.Name, d.txDentryInfo.MpMembers, d.txDentryInfo.TxID, d.txDentryInfo.MpID, d.txDentryInfo.CreateTime, d.txDentryInfo.Timeout)
	return content
}

// Less tests whether the current TxRollbackDentry item is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (d *TxRollbackDentry) Less(than btree.Item) bool {
	td, ok := than.(*TxRollbackDentry)
	return ok && d.txDentryInfo.GetKey() < td.txDentryInfo.GetKey()
}

// Copy returns a copy of the TxRollbackDentry.
func (d *TxRollbackDentry) Copy() btree.Item {

	item := d.dentry.Copy()
	txDentryInfo := *d.txDentryInfo
	//rbType := d.rbType

	return &TxRollbackDentry{
		dentry:                 item.(*Dentry),
		txDentryInfo:           &txDentryInfo,
		rbType:                 d.rbType,
		rbInitiator:            d.rbInitiator,
		rbPlaceholder:          d.rbPlaceholder,
		rbPlaceholderTimestamp: d.rbPlaceholderTimestamp,
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
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &d.rbInitiator); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &d.rbPlaceholder); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &d.rbPlaceholderTimestamp); err != nil {
		panic(err)
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
	if err = binary.Read(buff, binary.BigEndian, &d.rbInitiator); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &d.rbPlaceholder); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &d.rbPlaceholderTimestamp); err != nil {
		return
	}
	return
}

/*func (d *TxRollbackDentry) IsExpired() (expired bool) {
	if !d.rbPlaceholder {
		return false
	}
	now := time.Now().Unix()
	if now < d.rbPlaceholderTimestamp {
		log.LogErrorf("rbDentry placeholder time out error, now[%v], rbPlaceholderTimestamp[%v]",
			now, d.rbPlaceholderTimestamp)
		return true
	}
	expired = proto.MaxReplaceholderTimeout <= now-d.rbPlaceholderTimestamp
	if expired {
		log.LogDebugf("rbDentry placeholder [%v] is expired, now[%v], rbPlaceholderTimestamp[%v]",
			d, now, d.rbPlaceholderTimestamp)
	}
	return expired
}*/

func NewTxRollbackDentry(dentry *Dentry, txDentryInfo *proto.TxDentryInfo, rbType uint32) *TxRollbackDentry {
	return &TxRollbackDentry{
		dentry:       dentry,
		txDentryInfo: txDentryInfo,
		rbType:       rbType,
	}
}

func NewTxRbDentryPlaceholder(pid uint64, name string, txId string) *TxRollbackDentry {
	dentry := &Dentry{
		ParentId: pid,
		Name:     name,
	}
	txDentryInfo := &proto.TxDentryInfo{
		ParentId: pid,
		Name:     name,
		TxID:     txId,
	}
	return &TxRollbackDentry{
		dentry:                 dentry,
		txDentryInfo:           txDentryInfo,
		rbInitiator:            RbFromTM,
		rbPlaceholder:          true,
		rbPlaceholderTimestamp: time.Now().Unix(),
	}
}

//TM
type TransactionManager struct {
	//need persistence and sync to all the raft members of the mp
	txIdAlloc   *TxIDAllocator
	txTree      *BTree
	txProcessor *TransactionProcessor
	started     bool
	blacklist   *util.Set
	//newTxCh     chan struct{}
	leaderChangeCh    chan struct{}
	leaderChangeCheck int32

	opLimiter *rate.Limiter
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
	//connPool   *util.ConnectPool
	mask uint8
}

func (p *TransactionProcessor) Reset() {
	p.txManager.Reset()
	p.txResource.Reset()
	p.mp = nil
}

func NewTransactionManager(txProcessor *TransactionProcessor) *TransactionManager {
	txMgr := &TransactionManager{
		txIdAlloc:      newTxIDAllocator(),
		txTree:         NewBtree(),
		txProcessor:    txProcessor,
		started:        false,
		blacklist:      util.NewSet(),
		leaderChangeCh: make(chan struct{}, 1000),
		opLimiter:      rate.NewLimiter(rate.Inf, 128),
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
	//txProcessor.connPool = util.NewConnectPool()
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
	tm.Stop()
	tm.blacklist.Clear()
	tm.Lock()
	tm.txIdAlloc.Reset()
	tm.txTree.Reset()
	tm.opLimiter.SetLimit(0)
	defer func() {
		tm.Unlock()
		if r := recover(); r != nil {
			log.LogErrorf("transaction manager process closed for mp[%v] ,err:%v", tm.txProcessor.mp.config.PartitionId, r)
		}
	}()

	close(tm.leaderChangeCh)
	tm.txProcessor = nil
}

func (tm *TransactionManager) processExpiredTransactions(wgEt *sync.WaitGroup) {
	//scan transactions periodically, and invoke `rollbackTransaction` to roll back expired transactions
	log.LogInfof("processExpiredTransactions for mp[%v] started", tm.txProcessor.mp.config.PartitionId)
	clearInterval := time.Second * 60
	clearTimer := time.NewTimer(clearInterval)

	txCheckInterval := time.Second
	txCheckTimer := time.NewTimer(txCheckInterval)
	defer func() {
		if wgEt != nil {
			log.LogWarnf("processExpiredTransactions for mp[%v] exit", tm.txProcessor.mp.config.PartitionId)
			wgEt.Done()
		}
		return
	}()

	var counter uint64 = 0
	for {
		select {
		case <-tm.txProcessor.mp.stopC:
			log.LogInfof("processExpiredTransactions for mp[%v] stopped", tm.txProcessor.mp.config.PartitionId)
			tm.started = false
			return
		case <-tm.leaderChangeCh:
			// lock to avoid
			tm.Lock()
			log.LogWarnf("processExpiredTransactions for mp[%v] stopped", tm.txProcessor.mp.config.PartitionId)
			if _, ok := tm.txProcessor.mp.IsLeader(); ok {
				log.LogWarnf("processExpiredTransactions for mp[%v] already be leader again", tm.txProcessor.mp.config.PartitionId)
				tm.Unlock()
				continue
			}
			tm.started = false
			tm.Unlock()
			return
		case <-clearTimer.C:
			tm.blacklist.Clear()
			clearTimer.Reset(clearInterval)
			//log.LogDebugf("processExpiredTransactions: blacklist cleared")
		case <-txCheckTimer.C:
			//tm.notifyNewTransaction()
			if tm.txProcessor.mask == proto.TxPause {
				txCheckTimer.Reset(txCheckInterval)
				continue
			}
			log.LogInfof("processExpiredTransactions. mp %v mask %v", tm.txProcessor.mp.config.PartitionId, proto.GetMaskString(tm.txProcessor.mask))
			if tm.txTree.Len() == 0 {
				counter++
				if counter >= 100 {
					txCheckInterval = 5 * time.Second
				} else {
					txCheckInterval = time.Second
				}
				txCheckTimer.Reset(txCheckInterval)
				continue
			} else {
				counter = 0
			}

			limitCh := make(chan struct{}, 32)
			var wg sync.WaitGroup
			timeNow := time.Now().Unix()
			var delTx []*proto.TransactionInfo

			f := func(i BtreeItem) bool {
				if atomic.CompareAndSwapInt32(&tm.leaderChangeCheck, 1, 0) {
					if _, ok := tm.txProcessor.mp.IsLeader(); !ok {
						log.LogWarnf("processExpiredTransactions for mp[%v] already not leader and break tx tree traverse",
							tm.txProcessor.mp.config.PartitionId)
						return false
					}
				}
				tx := i.(*proto.TransactionInfo)
				rollbackFunc := func(skipSetStat bool) {
					defer func() {
						<-limitCh
					}()
					defer wg.Done()
					req := &proto.TxApplyRequest{
						TxID:        tx.TxID,
						TmID:        uint64(tx.TmID),
						TxApplyType: proto.TxRollback,
					}
					status, err := tm.rollbackTransaction(req, RbFromTM, skipSetStat)
					if err == nil && status == proto.OpOk {
						log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back done", tx)
					} else {
						log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back failed, status(%v), err(%v)",
							tx, status, err)
					}
				}

				commitFunc := func() {
					defer func() {
						<-limitCh
					}()
					defer wg.Done()
					req := &proto.TxApplyRequest{
						TxID:        tx.TxID,
						TmID:        uint64(tx.TmID),
						TxApplyType: proto.TxCommit,
					}
					status, err := tm.commitTransaction(req, true)
					if err == nil && status == proto.OpOk {
						log.LogWarnf("processExpiredTransactions: transaction (%v) commit done", tx)
					} else {
						log.LogWarnf("processExpiredTransactions: transaction (%v) commit failed, status(%v), err(%v)",
							tx, status, err)
					}
				}
				if tx.IsDoneAndNoNeedWait(timeNow) {
					delTx = append(delTx, tx)
					return true
				}
				if tx.State == proto.TxStateCommit {
					log.LogWarnf("processExpiredTransactions: transaction (%v) continue to commit...", tx)
					wg.Add(1)
					limitCh <- struct{}{}
					go commitFunc()
				} else if tx.State == proto.TxStateRollback {
					log.LogWarnf("processExpiredTransactions: transaction (%v) continue to roll back...", tx)
					wg.Add(1)
					limitCh <- struct{}{}
					go rollbackFunc(true)
				} else if tx.State == proto.TxStateFailed {
					log.LogCriticalf("processExpiredTransactions: transaction (%v) is in state failed", tx)
				} else {
					if tx.IsExpired() && tx.State != proto.TxStateCommitDone {
						log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back...", tx)
						wg.Add(1)
						limitCh <- struct{}{}
						go rollbackFunc(false)
					} else {
						log.LogDebugf("processExpiredTransactions: transaction (%v) is ongoing", tx)
					}
				}

				return true
			}

			tm.txTree.GetTree().Ascend(f)
			wg.Wait()
			tm.txTree.Execute(func(tree *btree.BTree) interface{} {
				for _, tx := range delTx {
					if tx == nil {
						return false
					}
					tm.txTree.tree.Delete(tx)
				}
				return true
			})
			txCheckInterval = time.Second
			txCheckTimer.Reset(txCheckInterval)
		}
	}

}

func (tm *TransactionManager) Start() (wg *sync.WaitGroup) {
	//only metapartition raft leader can start scan goroutine
	tm.Lock()
	defer tm.Unlock()
	log.LogInfof("TransactionManager.start  mp %v", tm.txProcessor.mp.config.PartitionId)
	if tm.started {
		log.LogWarnf("TransactionManager.start  mp %v already started", tm.txProcessor.mp.config.PartitionId)
		return
	}
	wg = new(sync.WaitGroup)
	wg.Add(1)
	go tm.processExpiredTransactions(wg)
	tm.started = true
	log.LogInfof("TransactionManager for mp[%v] started", tm.txProcessor.mp.config.PartitionId)
	return
}

func (tm *TransactionManager) stopProcess() {
	select {
	case tm.leaderChangeCh <- struct{}{}:
		log.LogWarnf("stopProcess, mp[%v] notified!", tm.txProcessor.mp.config.PartitionId)
	default:
		log.LogErrorf("stopProcess, mp[%v] failed!", tm.txProcessor.mp.config.PartitionId)
	}
}

func (tm *TransactionManager) Stop() {
	tm.Lock()
	defer tm.Unlock()
	if !tm.started {
		log.LogWarnf("TransactionManager for mp[%v] already stopped", tm.txProcessor.mp.config.PartitionId)
		return
	}

	tm.stopProcess()
	log.LogWarnf("TransactionManager for mp[%v] stopped", tm.txProcessor.mp.config.PartitionId)
	atomic.StoreInt32(&tm.leaderChangeCheck, 1)
}

func (tm *TransactionManager) nextTxID() string {
	id := tm.txIdAlloc.allocateTransactionID()
	txId := fmt.Sprintf("%d_%d", tm.txProcessor.mp.config.PartitionId, id)
	log.LogDebugf("nextTxID: txId:%v", txId)
	return txId
}

func (tm *TransactionManager) getTxInodeInfo(txID string, ino uint64) (info *proto.TxInodeInfo) {
	tm.RLock()
	defer tm.RUnlock()
	var ok bool
	txInfo := tm.getTransaction(txID)
	if txInfo == nil {
		return nil
	}
	if info, ok = txInfo.TxInodeInfos[ino]; !ok {
		return nil
	}
	return
}

func (tm *TransactionManager) getTxDentryInfo(txID string, key string) (info *proto.TxDentryInfo) {
	tm.RLock()
	defer tm.RUnlock()
	var ok bool

	txInfo := tm.getTransaction(txID)
	if txInfo == nil {
		return nil
	}
	if info, ok = txInfo.TxDentryInfos[key]; !ok {
		return nil
	}
	return
}

func (tm *TransactionManager) GetTransaction(txID string) (txInfo *proto.TransactionInfo) {
	tm.RLock()
	defer tm.RUnlock()
	return tm.getTransaction(txID)
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

/*func (tm *TransactionManager) notifyNewTransaction() {
	select {
	case tm.newTxCh <- struct{}{}:
		log.LogDebugf("notifyNewTransaction, scan routine notified!")
	default:
		log.LogDebugf("notifyNewTransaction, skipping notify!")
	}
}*/

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
	//tm.notifyNewTransaction()
}

//TM register a transaction, process client transaction
func (tm *TransactionManager) registerTransaction(txInfo *proto.TransactionInfo) (err error) {
	//1. generate transactionID from TxIDAllocator
	//id := tm.txIdAlloc.allocateTransactionID()
	//txId = fmt.Sprintf("%d_%d", tm.txProcessor.mp.config.PartitionId, id)
	//2. put transaction received from client into TransactionManager.transctions

	if err = tm.updateTxIdCursor(txInfo.TxID); err != nil {
		return err
	}

	if txInfo.TmID != int64(tm.txProcessor.mp.config.PartitionId) {
		return nil
	}

	//if _, ok := tm.transactions[txInfo.TxID]; ok {
	//	return nil
	//}

	if info := tm.getTransaction(txInfo.TxID); info != nil {
		return nil
	}

	tm.Lock()
	//txInfo.TmID = int64(tm.txProcessor.mp.config.PartitionId)
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

	//tm.transactions[txInfo.TxID] = txInfo
	//tm.txTree.ReplaceOrInsert(txInfo, true)
	tm.addTxInfo(txInfo)
	tm.Unlock()
	log.LogDebugf("registerTransaction: txInfo(%v)", txInfo)
	//3. notify new transaction
	//tm.notifyNewTransaction()
	return nil
}

//TM roll back a transaction, sends roll back request to all related RMs,
//a rollback can be initiated by client or triggered by TM scan routine
func (tm *TransactionManager) rollbackTransaction(req *proto.TxApplyRequest, rbFrom uint32, skipSetStat bool) (status uint8, err error) {
	status = proto.OpOk
	var packet *proto.Packet

	//1.set transaction to TxStateRollback
	if !skipSetStat {
		status, err = tm.setTransactionState(req.TxID, proto.TxStateRollback)
		if status != proto.OpOk {
			log.LogWarnf("rollbackTransaction: set transaction[%v] state to TxStateRollback failed", req.TxID)
			return
		}
	}

	//2. notify all related RMs that a transaction is to be rolled back

	txId := req.TxID

	tx := tm.getTransaction(txId)
	if tx == nil {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("rollbackTransaction: tx[%v] not found", txId)
		return
	}

	items := make([]*txApplyItem, 0)
	for _, inoInfo := range tx.TxInodeInfos {
		packet, err = tm.buildInodeApplyPacket(inoInfo, req.TxApplyType, rbFrom)
		if err != nil {
			status = proto.OpTxInternalErr
			return
		}

		item := &txApplyItem{
			txId:     txId,
			itemType: ApplyItemTypeInode,
			key:      strconv.FormatUint(inoInfo.GetKey(), 10),
			p:        packet,
			members:  inoInfo.MpMembers,
		}
		items = append(items, item)
	}

	for _, denInfo := range tx.TxDentryInfos {
		packet, err = tm.buildDentryApplyPacket(denInfo, req.TxApplyType, rbFrom)
		if err != nil {
			status = proto.OpTxInternalErr
			return
		}

		item := &txApplyItem{
			txId:     txId,
			itemType: ApplyItemTypeDentry,
			key:      denInfo.GetKey(),
			p:        packet,
			members:  denInfo.MpMembers,
		}
		items = append(items, item)
	}

	errorsCh := make(chan error, len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go tm.txRollbackToRM(item, &wg, errorsCh)
	}
	wg.Wait()
	close(errorsCh)

	if len(errorsCh) > 0 {
		status = proto.OpTxRollbackItemErr
		err = <-errorsCh
		if tx == nil {
			status = proto.OpTxInfoNotExistErr
			err = fmt.Errorf("rollbackTransaction: tx[%v] not found", txId)
			return
		}
		if tx.State == proto.TxStateFailed {
			log.LogCriticalf("rollbackTransaction: roll back %v failed with conflict", txId)
		} else {
			log.LogWarnf("rollbackTransaction: roll back failed, tx[%v] error[%v], retry later", txId, err)
		}
		return
	}

	//2. TM roll back the transaction
	//submit to all TM metapartition
	val, err := json.Marshal(req)
	if err != nil {
		return
	}

	var resp interface{}
	resp, err = tm.txProcessor.mp.submit(opFSMTxRollback, val)
	if err != nil {
		log.LogWarnf("rollbackTransaction: rollback transaction[%v]  failed, err[%v]", txId, err)
		return proto.OpTxRollbackErr, err
	}
	status = resp.(uint8)

	if status == proto.OpTxInfoNotExistErr {
		status = proto.OpOk
		log.LogWarnf("rollbackTransaction: tx[%v] is already rolled back", txId)
	} else if err == nil && status == proto.OpOk {
		log.LogDebugf("rollbackTransaction: tx[%v] roll back successfully", txId)
	}
	return
}

const (
	ApplyItemTypeInode int = iota
	ApplyItemTypeDentry
)

type txApplyItem struct {
	txId     string
	itemType int
	key      string
	p        *proto.Packet
	members  string
}

func (tm *TransactionManager) rollbackTxInfo(txId string) (status uint8, err error) {
	tm.Lock()
	defer tm.Unlock()
	status = proto.OpOk

	tx := tm.getTransaction(txId)
	if tx == nil {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("rollbackTxInfo: rollback tx[%v] failed, not found", txId)
		return
	}

	//delete(tm.transactions, txId)
	tm.txTree.Delete(tx)
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

	// the status TxStateCommitDone not persist in disk , it will be finished while do expire check after restart
	// since it's status is TxStateCommit,the submit of TxStateCommitDone not necessary and would delay the response
	tx.State = proto.TxStateCommitDone
	tx.DoneTime = time.Now().Unix()
	log.LogDebugf("commitTxInfo: tx[%v] is committed", tx)
	return
}

func (tm *TransactionManager) buildInodeApplyPacket(txInodeInfo *proto.TxInodeInfo, txApplyType int, rbFrom uint32) (packet *proto.Packet, err error) {
	inoReq := &proto.TxInodeApplyRequest{
		TxID:        txInodeInfo.TxID,
		Inode:       txInodeInfo.Ino,
		TxApplyType: txApplyType,
	}
	packet = proto.NewPacketReqID()

	switch txApplyType {
	case proto.TxCommit:
		packet.Opcode = proto.OpTxInodeCommit
	case proto.TxRollback:
		packet.Opcode = proto.OpTxInodeRollback
		inoReq.ApplyFrom = rbFrom
	default:
		errInfo := fmt.Sprintf("buildInodeApplyPacket: marshal apply inode [%v] failed", inoReq)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return nil, err
	}

	//p.Opcode = proto.OpTxInodeCommit
	packet.PartitionID = txInodeInfo.MpID
	err = packet.MarshalData(inoReq)
	if err != nil {
		errInfo := fmt.Sprintf("buildInodeApplyPacket: marshal apply inode [%v] failed", inoReq)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return nil, err
	}
	return packet, nil
}

func (tm *TransactionManager) buildDentryApplyPacket(txDentryInfo *proto.TxDentryInfo, txApplyType int, rbFrom uint32) (packet *proto.Packet, err error) {
	denReq := &proto.TxDentryApplyRequest{
		TxID:        txDentryInfo.TxID,
		Pid:         txDentryInfo.ParentId,
		Name:        txDentryInfo.Name,
		TxApplyType: txApplyType,
	}
	packet = proto.NewPacketReqID()

	switch txApplyType {
	case proto.TxCommit:
		packet.Opcode = proto.OpTxDentryCommit
	case proto.TxRollback:
		packet.Opcode = proto.OpTxDentryRollback
		denReq.ApplyFrom = rbFrom
	default:
		errInfo := fmt.Sprintf("buildDentryApplyPacket: marshal apply dentry [%v] failed", denReq)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return nil, err
	}

	//packet.Opcode = proto.OpTxDentryCommit
	packet.PartitionID = txDentryInfo.MpID
	err = packet.MarshalData(denReq)
	if err != nil {
		errInfo := fmt.Sprintf("buildDentryApplyPacket: marshal apply dentry [%v] failed", denReq)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return nil, err
	}
	return packet, nil
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

//TM notify all related RMs that a transaction is completed,
//and corresponding transaction resources(inode, dentry) can be removed
func (tm *TransactionManager) commitTransaction(req *proto.TxApplyRequest, skipSetStat bool) (status uint8, err error) {
	var packet *proto.Packet
	var val []byte
	var resp interface{}

	txId := req.TxID
	tx := tm.getTransaction(txId)
	if tx == nil {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("commitTransaction: tx[%v] not found", txId)
		return
	}

	if tx.State == proto.TxStateCommitDone {
		status = proto.OpOk
		log.LogWarnf("commitTransaction: tx[%v] is already commited", txId)
		return
	}

	//1.set transaction to TxStateCommit
	if !skipSetStat {
		status, err = tm.setTransactionState(req.TxID, proto.TxStateCommit)
		if status != proto.OpOk {
			log.LogWarnf("commitTransaction: set transaction[%v] state to TxStateCommit failed", req.TxID)
			return
		}
	}

	//2. notify all related RMs that a transaction is completed
	items := make([]*txApplyItem, 0)
	for _, inoInfo := range tx.TxInodeInfos {
		packet, err = tm.buildInodeApplyPacket(inoInfo, req.TxApplyType, RbFromDummy)
		if err != nil {
			status = proto.OpTxInternalErr
			return
		}
		item := &txApplyItem{
			txId:     txId,
			itemType: ApplyItemTypeInode,
			key:      strconv.FormatUint(inoInfo.GetKey(), 10),
			p:        packet,
			members:  inoInfo.MpMembers,
		}
		items = append(items, item)
	}

	for _, denInfo := range tx.TxDentryInfos {
		packet, err = tm.buildDentryApplyPacket(denInfo, req.TxApplyType, RbFromDummy)
		if err != nil {
			status = proto.OpTxInternalErr
			return
		}
		item := &txApplyItem{
			txId:     txId,
			itemType: ApplyItemTypeDentry,
			key:      denInfo.GetKey(),
			p:        packet,
			members:  denInfo.MpMembers,
		}
		items = append(items, item)
	}

	errorsCh := make(chan error, len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go tm.txCommitToRM(item, &wg, errorsCh)
	}
	wg.Wait()

	close(errorsCh)

	if len(errorsCh) > 0 {
		status = proto.OpTxCommitItemErr
		err = <-errorsCh
		tx = tm.getTransaction(txId)
		if tx == nil {
			status = proto.OpTxInfoNotExistErr
			err = fmt.Errorf("commitTransaction: tx[%v] not found", txId)
			return
		}
		if tx.State == proto.TxStateFailed {
			log.LogCriticalf("commitTransaction: commit %v failed with conflict", txId)
		} else {
			log.LogWarnf("commitTransaction: commit failed, tx[%v] error[%v], retry later", txId, err)
		}

		return
	}

	//3. TM commit the transaction
	//submit to all TM metapartition

	val, err = json.Marshal(req)
	if err != nil {
		return
	}

	resp, err = tm.txProcessor.mp.submit(opFSMTxCommit, val)
	if err != nil {
		log.LogWarnf("commitTransaction: commit transaction[%v]  failed, err[%v]", txId, err)
		return proto.OpTxCommitErr, err
	}
	status = resp.(uint8)

	if status == proto.OpTxInfoNotExistErr {
		status = proto.OpOk
		log.LogWarnf("commitTransaction: tx[%v] is already commited", txId)
	} else if err == nil && status == proto.OpOk {
		log.LogDebugf("commitTransaction: tx[%v] is commited successfully", txId)
	}

	return
}

func (tm *TransactionManager) sendPacketToMP(addr string, p *proto.Packet) (err error) {
	var (
		mConn *net.TCPConn
		reqID = p.ReqID
		reqOp = p.Opcode
	)

	connPool := tm.txProcessor.mp.manager.connPool
	//connPool := tm.txProcessor.connPool

	mConn, err = connPool.GetConnect(addr)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}

	// send to master connection
	if err = p.WriteToConn(mConn); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}

	// read connection from the master
	if err = p.ReadFromConn(mConn, proto.NoReadDeadlineTime); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}
	if reqID != p.ReqID || reqOp != p.Opcode {
		log.LogErrorf("sendPacketToMP: send and received packet mismatch: req(%v_%v) resp(%v_%v)",
			reqID, reqOp, p.ReqID, p.Opcode)
	}
	connPool.PutConnect(mConn, NoClosedConnect)
end:
	if err != nil {
		log.LogErrorf("[sendPacketToMP]: req: %d - %v, %v, packet(%v)", p.GetReqID(),
			p.GetOpMsg(), err, p)
	}
	log.LogDebugf("[sendPacketToMP] req: %d - %v, resp: %v, packet(%v)", p.GetReqID(), p.GetOpMsg(),
		p.GetResultMsg(), p)
	return
}

func (tm *TransactionManager) txApplyToRMWithBlacklist(addrs []string, applyItem *txApplyItem, skipBlacklist bool) (skippedAddrNum int, newPacket *proto.Packet, err error) {

	skippedAddrNum = 0
	for _, addr := range addrs {
		if tm.blacklist.Has(addr) && !skipBlacklist {
			log.LogWarnf("txApplyToRM: addr[%v] is already blacklisted, retry another addr, key[%v], tx[%v]",
				addr, applyItem.key, applyItem.txId)
			skippedAddrNum++
			continue
		}
		newPacket = applyItem.p.GetCopy()
		err = tm.sendPacketToMP(addr, newPacket)
		if err != nil {
			tm.blacklist.Add(addr)
			log.LogWarnf("txApplyToRM: apply to %v failed, err(%s), add to blacklist and retry another addr key[%v], tx[%v]",
				addr, err, applyItem.key, applyItem.txId)
		} else {
			status := newPacket.ResultCode
			if status != proto.OpOk {
				if status == proto.OpTxRbInodeNotExistErr || status == proto.OpTxConflictErr ||
					status == proto.OpTxRbDentryNotExistErr || status == proto.OpTxRollbackUnknownRbType {
					log.LogWarnf("txApplyToRM: apply to %v done with status[%v], key[%v], tx[%v]",
						addr, status, applyItem.key, applyItem.txId)
					err = nil
					break
				} else {
					tm.blacklist.Add(addr)
					err = errors.New(newPacket.GetResultMsg())
					log.LogWarnf("txApplyToRM: apply to %v failed, err(%s), add to blacklist and retry another addr key[%v], tx[%v]",
						addr, err, applyItem.key, applyItem.txId)
				}
			} else {
				log.LogDebugf("txApplyToRM: apply to %v done with status[%v], key[%v], tx[%v]",
					addr, status, applyItem.key, applyItem.txId)
				err = nil
				break
			}

		}
	}
	return skippedAddrNum, newPacket, err
}

func (tm *TransactionManager) txRollbackToRM(applyItem *txApplyItem, wg *sync.WaitGroup, errorsCh chan error) {
	defer wg.Done()
	addrs := strings.Split(applyItem.members, ",")

	applyType := ""
	if applyItem.itemType == ApplyItemTypeInode {
		applyType = "inode"
	} else {
		applyType = "dentry"
	}
	skippedAddrNum, newPacket, err := tm.txApplyToRMWithBlacklist(addrs, applyItem, false)
	if err == nil && skippedAddrNum == len(addrs) || err != nil && skippedAddrNum > 0 {
		log.LogInfof("txRollbackToRM: retry roll back %v[%v] without blacklist, tx[%v]",
			applyType, applyItem.key, applyItem.txId)
		_, newPacket, err = tm.txApplyToRMWithBlacklist(addrs, applyItem, true)
	}

	if err != nil {
		log.LogWarnf("txRollbackToRM: roll back %v[%v] failed, retry later, tx[%v]",
			applyType, applyItem.key, applyItem.txId)
		errorsCh <- err
	} else {
		if newPacket.ResultCode == proto.OpTxConflictErr {
			/*_, _ = tm.setTransactionState(applyItem.txId, proto.TxStateFailed)
			log.LogCriticalf("txRollbackToRM: rollback %v[%v] failed with conflict, tx[%v]")
			err = errors.New(newPacket.GetResultMsg())
			errorsCh <- err*/
			log.LogDebugf("txRollbackToRM: %v[%v] might have already been rolled back, tx[%v]",
				applyType, applyItem.key, applyItem.txId)
		} else if newPacket.ResultCode == proto.OpTxRbInodeNotExistErr || newPacket.ResultCode == proto.OpTxRbDentryNotExistErr {
			log.LogWarnf("txRollbackToRM: %v[%v] might have not been added before or rolled back by TM: tx[%v]",
				applyType, applyItem.key, applyItem.txId)
		} else {
			log.LogDebugf("txRollbackToRM: %v[%v] rolled back, tx[%v]", applyType, applyItem.key, applyItem.txId)
		}
	}
}

func (tm *TransactionManager) txCommitToRM(applyItem *txApplyItem, wg *sync.WaitGroup, errorsCh chan error) {
	defer wg.Done()
	addrs := strings.Split(applyItem.members, ",")

	applyType := ""
	if applyItem.itemType == ApplyItemTypeInode {
		applyType = "inode"
	} else {
		applyType = "dentry"
	}
	skippedAddrNum, newPacket, err := tm.txApplyToRMWithBlacklist(addrs, applyItem, false)
	if err == nil && skippedAddrNum == len(addrs) || err != nil && skippedAddrNum > 0 {
		log.LogInfof("txCommitToRM: retry commit %v[%v] without blacklist, tx[%v]",
			applyType, applyItem.key, applyItem.txId)
		_, newPacket, err = tm.txApplyToRMWithBlacklist(addrs, applyItem, true)
	}

	if err != nil {
		log.LogWarnf("txCommitToRM: commit %v[%v] failed, retry later, tx[%v]",
			applyType, applyItem.key, applyItem.txId)
		errorsCh <- err
	} else {
		if newPacket.ResultCode == proto.OpTxConflictErr {
			/*_, _ = tm.setTransactionState(applyItem.txId, proto.TxStateFailed)
			log.LogCriticalf("txCommitToRM: commit %v[%v] failed with conflict, tx[%v]")
			err = errors.New(newPacket.GetResultMsg())
			errorsCh <- err*/
			log.LogDebugf("txRollbackToRM: %v[%v] might have already been committed, tx[%v]",
				applyType, applyItem.key, applyItem.txId)
		} else if newPacket.ResultCode == proto.OpTxRbInodeNotExistErr || newPacket.ResultCode == proto.OpTxRbDentryNotExistErr {
			log.LogWarnf("txCommitToRM: %v[%v] already committed before, tx[%v]", applyType, applyItem.key, applyItem.txId)
		} else {
			log.LogDebugf("txCommitToRM: %v[%v] committed, tx[%v]", applyType, applyItem.key, applyItem.txId)
		}
	}
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
	} else {

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
	}
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

		now := time.Now().UnixNano()
		if now < rbInode.txInodeInfo.CreateTime {
			return true, rbInode.txInodeInfo.TxID
		}
		expired := 2*rbInode.txInodeInfo.Timeout*60*1e9 <= now-rbInode.txInodeInfo.CreateTime

		if expired {
			return false, ""
		} else {
			return true, rbInode.txInodeInfo.TxID
		}

	}
	return false, ""
}

func (tr *TransactionResource) isDentryInTransction(dentry *Dentry) (inTx bool, txID string) {
	//return true only if specified dentry is in an ongoing transaction(not expired yet)
	tr.Lock()
	defer tr.Unlock()

	if rbDentry := tr.getTxRbDentry(dentry.ParentId, dentry.Name); rbDentry != nil {
		return true, rbDentry.txDentryInfo.TxID
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

//RM add an `TxRollbackInode` into `txRollbackInodes`
func (tr *TransactionResource) addTxRollbackInode(rbInode *TxRollbackInode) (status uint8) {
	tr.Lock()
	defer tr.Unlock()

	oldRbInode := tr.getTxRbInode(rbInode.inode.Inode)
	if oldRbInode != nil {
		if oldRbInode.rbPlaceholder {
			if oldRbInode.txInodeInfo.TxID == rbInode.txInodeInfo.TxID {
				log.LogWarnf("addTxRollbackInode: tx[%v] is already expired", rbInode.txInodeInfo.TxID)
				tr.txRbInodeTree.Delete(oldRbInode)
				status = proto.OpTxTimeoutErr
			} else {
				log.LogWarnf("addTxRollbackInode: rollback inode [ino(%v) txID(%v) rbType(%v)] "+
					"is conflicted with placeholder inode [ino(%v) txID(%v) rbType(%v)], replace anyway",
					rbInode.inode.Inode, rbInode.txInodeInfo.TxID, rbInode.rbType,
					oldRbInode.inode.Inode, oldRbInode.txInodeInfo.TxID, oldRbInode.rbType)
				tr.txRbInodeTree.ReplaceOrInsert(rbInode, true)
				status = proto.OpOk
				//status = proto.OpTxConflictErr
			}
			return
		}

		if oldRbInode.rbType == rbInode.rbType && oldRbInode.txInodeInfo.TxID == rbInode.txInodeInfo.TxID {
			log.LogWarnf("addTxRollbackInode: rollback inode [ino(%v) txID(%v)] is already exists",
				rbInode.inode.Inode, rbInode.txInodeInfo.TxID)
			return proto.OpOk
		} else {
			log.LogErrorf("addTxRollbackInode: rollback inode [ino(%v) txID(%v) rbType(%v)] "+
				"is conflicted with inode [ino(%v) txID(%v) rbType(%v)]",
				rbInode.inode.Inode, rbInode.txInodeInfo.TxID, rbInode.rbType,
				oldRbInode.inode.Inode, oldRbInode.txInodeInfo.TxID, oldRbInode.rbType)
			return proto.OpTxConflictErr
		}
	} else {
		tr.txRbInodeTree.ReplaceOrInsert(rbInode, true)
	}
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

//RM add a `TxRollbackDentry` into `txRollbackDentries`
func (tr *TransactionResource) addTxRollbackDentry(rbDentry *TxRollbackDentry) (status uint8) {
	tr.Lock()
	defer tr.Unlock()

	oldRbDentry := tr.getTxRbDentry(rbDentry.txDentryInfo.ParentId, rbDentry.dentry.Name)
	if oldRbDentry != nil {

		if oldRbDentry.rbPlaceholder {
			if oldRbDentry.txDentryInfo.TxID == rbDentry.txDentryInfo.TxID {
				log.LogWarnf("addTxRollbackDentry: tx[%v] is already expired", rbDentry.txDentryInfo.TxID)
				tr.txRbDentryTree.Delete(oldRbDentry)
				status = proto.OpTxTimeoutErr
			} else {
				log.LogWarnf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v) rbType(%v)] "+
					"is conflicted with placeholder dentry [pino(%v) name(%v)  txID(%v) rbType(%v)], replace anyway",
					rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID, rbDentry.rbType,
					oldRbDentry.dentry.ParentId, oldRbDentry.dentry.Name, oldRbDentry.txDentryInfo.TxID, oldRbDentry.rbType)
				tr.txRbDentryTree.ReplaceOrInsert(rbDentry, true)
				status = proto.OpOk
				//status = proto.OpTxConflictErr
			}
			return
		}

		if oldRbDentry.rbType == rbDentry.rbType && oldRbDentry.txDentryInfo.TxID == rbDentry.txDentryInfo.TxID {
			log.LogWarnf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v)] is already exists",
				rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID)
			return proto.OpOk
		} else {
			log.LogErrorf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v) rbType(%v)] "+
				"is conflicted with dentry [pino(%v) name(%v)  txID(%v) rbType(%v)]",
				rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID, rbDentry.rbType,
				oldRbDentry.dentry.ParentId, oldRbDentry.dentry.Name, oldRbDentry.txDentryInfo.TxID, oldRbDentry.rbType)
			return proto.OpTxConflictErr
		}
	} else {
		tr.txRbDentryTree.ReplaceOrInsert(rbDentry, true)
	}
	log.LogDebugf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v) rbType(%v)] is added",
		rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID, rbDentry.rbType)
	return proto.OpOk
}

func (tr *TransactionResource) rollbackInodeInternal(rbInode *TxRollbackInode) (status uint8, err error) {
	status = proto.OpOk
	switch rbInode.rbType {
	case TxAdd:
		tr.txProcessor.mp.freeList.Remove(rbInode.inode.Inode)
		if ino := tr.txProcessor.mp.inodeTree.Get(rbInode.inode); ino != nil {
			if ino.(*Inode).IsTempFile() && tr.txProcessor.mp.uidManager != nil {
				tr.txProcessor.mp.uidManager.addUidSpace(rbInode.inode.Uid, rbInode.inode.Inode, rbInode.inode.Extents.eks)
			}
			if (ino.(*Inode).IsTempFile() || ino.(*Inode).IsEmptyDir()) && tr.txProcessor.mp.mqMgr != nil && len(rbInode.quotaIds) > 0 {
				tr.txProcessor.mp.setInodeQuota(rbInode.quotaIds, rbInode.inode.Inode)
				for _, quotaId := range rbInode.quotaIds {
					tr.txProcessor.mp.mqMgr.updateUsedInfo(int64(rbInode.inode.Size), 1, quotaId)
				}
			}

		}
		tr.txProcessor.mp.inodeTree.ReplaceOrInsert(rbInode.inode, true)

	case TxDelete:
		//_ = tr.txProcessor.mp.fsmUnlinkInode(rbInode.inode)
		if rsp := tr.txProcessor.mp.getInode(rbInode.inode); rsp.Status == proto.OpOk {
			if tr.txProcessor.mp.uidManager != nil {
				tr.txProcessor.mp.uidManager.doMinusUidSpace(rbInode.inode.Uid, rbInode.inode.Inode, rbInode.inode.Size)
			}
			if tr.txProcessor.mp.mqMgr != nil && len(rbInode.quotaIds) > 0 {
				for _, quotaId := range rbInode.quotaIds {
					tr.txProcessor.mp.mqMgr.updateUsedInfo(-1*int64(rbInode.inode.Size), -1, quotaId)
				}
			}
		}

		tr.txProcessor.mp.internalDeleteInode(rbInode.inode)
	case TxUpdate:
		tr.txProcessor.mp.inodeTree.ReplaceOrInsert(rbInode.inode, true)

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
		if req.ApplyFrom == RbFromTM {
			rbPlaceholder := NewTxRbInodePlaceholder(req.Inode, req.TxID)
			//_ = tr.addTxRollbackInode(rbPlaceholder)
			tr.txRbInodeTree.ReplaceOrInsert(rbPlaceholder, true)
			log.LogDebugf("rollbackInode: rbPlaceholder[%v] added, ino[%v] txID[%v]", rbPlaceholder, req.Inode, req.TxID)
		} else {
			status = proto.OpTxRbInodeNotExistErr
			errInfo := fmt.Sprintf("rollbackInode: roll back inode[%v] failed, txID[%v], rb inode not found", req.Inode, req.TxID)
			err = errors.New(errInfo)
			log.LogErrorf("%v", errInfo)
		}
		return
	}

	if rbInode.txInodeInfo.TxID != req.TxID {
		status = proto.OpTxConflictErr
		errInfo := fmt.Sprintf("rollbackInode: txID %v is not matching txInodeInfo txID %v", req.TxID, rbInode.txInodeInfo.TxID)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	if rbInode.rbPlaceholder {
		log.LogDebugf("rollbackInode: rbInode[%v] has already been rolled back with placeholder by TM, ino[%v] txID[%v]",
			rbInode, req.Inode, req.TxID)
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
	status = proto.OpOk
	switch rbDentry.rbType {
	case TxAdd:
		_ = tr.txProcessor.mp.fsmCreateDentry(rbDentry.dentry, true)
	case TxDelete:
		_ = tr.txProcessor.mp.fsmDeleteDentry(rbDentry.dentry, true)
		//resp := tr.txProcessor.mp.fsmUnlinkInode(rbInode.inode)
		//status = resp.Status
	case TxUpdate:
		_ = tr.txProcessor.mp.fsmUpdateDentry(rbDentry.dentry)

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
		if req.ApplyFrom == RbFromTM {
			rbPlaceholder := NewTxRbDentryPlaceholder(req.Pid, req.Name, req.TxID)
			//_ = tr.addTxRollbackDentry(rbPlaceholder)
			tr.txRbDentryTree.ReplaceOrInsert(rbPlaceholder, true)
			log.LogDebugf("rollbackDentry: rbPlaceholder[%v] added, dentry[%v_%v], txID[%v]",
				rbPlaceholder, req.Pid, req.Name, req.TxID)
		} else {
			status = proto.OpTxRbDentryNotExistErr
			errInfo := fmt.Sprintf("rollbackDentry: roll back dentry[%v_%v] failed, rb inode not found, txID[%v]",
				req.Pid, req.Name, req.TxID)
			err = errors.New(errInfo)
			log.LogErrorf("%v", errInfo)
		}

		return
	}

	if rbDentry.txDentryInfo.TxID != req.TxID {
		status = proto.OpTxConflictErr
		errInfo := fmt.Sprintf("rollbackDentry: txID %v is not matching txInodeInfo txID %v", req.TxID, rbDentry.txDentryInfo.TxID)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	if rbDentry.rbPlaceholder {
		log.LogDebugf("rollbackDentry: rbDentry[%v] has already been rolled back with placeholder by TM, dentry[%v_%v], txID[%v]",
			rbDentry, req.Pid, req.Name, req.TxID)
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
	} else if rbInode.rbPlaceholder {
		//reentry
		status = proto.OpTxRbInodeNotExistErr
		errInfo := fmt.Sprintf("commitInode: inode[%v] is committed and rolled back", inode)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
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
	} else if rbDentry.rbPlaceholder {
		//reentry
		status = proto.OpTxRbDentryNotExistErr
		errInfo := fmt.Sprintf("commitDentry: dentry[%v_%v] is committed and rolled back", pId, name)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	if rbDentry.txDentryInfo.TxID != txID {
		status = proto.OpTxConflictErr
		errInfo := fmt.Sprintf("commitDentry: txID %v is not matching txDentryInfo txID %v", txID, rbDentry.txDentryInfo.TxID)
		err = errors.New(errInfo)
		log.LogErrorf("%v", errInfo)
		return
	}

	tr.txRbDentryTree.Delete(rbDentry)
	log.LogDebugf("commitDentry: dentry[%v] is committed", rbDentry.txDentryInfo.GetKey())
	return
}
