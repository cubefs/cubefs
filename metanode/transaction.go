package metanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/btree"

	"github.com/cubefs/cubefs/proto"
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

type TxRollbackInode struct {
	inode                  *Inode
	txInodeInfo            *proto.TxInodeInfo
	rbType                 uint32 //Rollback Type
	rbInitiator            uint32 //TM, Client
	rbPlaceholder          bool   //default false
	rbPlaceholderTimestamp int64
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
	return
}

func (i *TxRollbackInode) IsExpired() (expired bool) {
	if !i.rbPlaceholder {
		return false
	}
	now := time.Now().Unix()
	if now < i.rbPlaceholderTimestamp {
		log.LogErrorf("rbInode placeholder time out error, now[%v], rbPlaceholderTimestamp[%v]",
			now, i.rbPlaceholderTimestamp)
		return true
	}
	expired = proto.MaxTransactionTimeout <= now-i.rbPlaceholderTimestamp
	if expired {
		log.LogDebugf("rbInode placeholder [%v] is expired, now[%v], rbPlaceholderTimestamp[%v]",
			i, now, i.rbPlaceholderTimestamp)
	}
	return expired
}

func NewTxRollbackInode(inode *Inode, txInodeInfo *proto.TxInodeInfo, rbType uint32) *TxRollbackInode {
	return &TxRollbackInode{
		inode:       inode,
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

func (d *TxRollbackDentry) IsExpired() (expired bool) {
	if !d.rbPlaceholder {
		return false
	}
	now := time.Now().Unix()
	if now < d.rbPlaceholderTimestamp {
		log.LogErrorf("rbDentry placeholder time out error, now[%v], rbPlaceholderTimestamp[%v]",
			now, d.rbPlaceholderTimestamp)
		return true
	}
	expired = proto.MaxTransactionTimeout <= now-d.rbPlaceholderTimestamp
	if expired {
		log.LogDebugf("rbDentry placeholder [%v] is expired, now[%v], rbPlaceholderTimestamp[%v]",
			d, now, d.rbPlaceholderTimestamp)
	}
	return expired
}

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
	txIdAlloc *TxIDAllocator
	//transactions map[string]*proto.TransactionInfo //key: metapartitionID_mpTxID
	txTree      *BTree
	txProcessor *TransactionProcessor
	started     bool
	blacklist   *util.Set
	newTxCh     chan struct{}
	exitCh      chan struct{}
	sync.RWMutex
}

//RM
type TransactionResource struct {
	//need persistence and sync to all the raft members of the mp
	//txRollbackInodes   map[uint64]*TxRollbackInode //key: inode id
	txRbInodeTree *BTree //key: inode id
	//txRollbackDentries map[string]*TxRollbackDentry // key: parentId_name
	txRbDentryTree *BTree // key: parentId_name
	txProcessor    *TransactionProcessor
	started        bool
	exitCh         chan struct{}
	sync.RWMutex
}

type TransactionProcessor struct {
	txManager  *TransactionManager  //TM
	txResource *TransactionResource //RM
	mp         *metaPartition
}

func NewTransactionManager(txProcessor *TransactionProcessor) *TransactionManager {
	txMgr := &TransactionManager{
		txIdAlloc: newTxIDAllocator(),
		//transactions: make(map[string]*proto.TransactionInfo, 0),
		txTree:      NewBtree(),
		txProcessor: txProcessor,
		started:     false,
		blacklist:   util.NewSet(),
		exitCh:      make(chan struct{}),
		newTxCh:     make(chan struct{}, 1),
	}
	//txMgr.Start()
	return txMgr
}

func NewTransactionResource(txProcessor *TransactionProcessor) *TransactionResource {
	txRsc := &TransactionResource{
		//txRollbackInodes:   make(map[uint64]*TxRollbackInode, 0),
		txRbInodeTree: NewBtree(),
		//txRollbackDentries: make(map[string]*TxRollbackDentry, 0),
		txRbDentryTree: NewBtree(),
		txProcessor:    txProcessor,
		started:        false,
		exitCh:         make(chan struct{}),
	}
	txRsc.Start()
	return txRsc
}

func NewTransactionProcessor(mp *metaPartition) *TransactionProcessor {
	txProcessor := &TransactionProcessor{
		mp: mp,
	}
	txProcessor.txManager = NewTransactionManager(txProcessor)
	txProcessor.txResource = NewTransactionResource(txProcessor)
	return txProcessor
}

//func (tm *TransactionManager) copyTransactions() map[string]*proto.TransactionInfo {
//	transactions := make(map[string]*proto.TransactionInfo, 0)
//	tm.Lock()
//	defer tm.Unlock()
//
//	for key, tx := range tm.transactions {
//		transactions[key] = tx
//	}
//	return transactions
//}
/*
func (tm *TransactionManager) processExpiredTransactions() {
	//scan transctions periodically, and invoke `rollbackTransaction` to roll back expired transctions
	log.LogDebugf("processExpiredTransactions for mp[%v] started", tm.txProcessor.mp.config.PartitionId)
	for {
		select {
		case <-tm.exitCh:
			log.LogDebugf("processExpiredTransactions for mp[%v] stopped", tm.txProcessor.mp.config.PartitionId)
			return
		case <-tm.newTxCh:
			scanTimer := time.NewTimer(time.Second)
		LOOP:
			for {
				select {
				case <-tm.exitCh:
					return
				case <-scanTimer.C:
					if len(tm.transactions) == 0 {
						break LOOP
					}

					transactions := tm.copyTransactions()
					var wg sync.WaitGroup
					for _, tx := range transactions {
						now := time.Now().Unix()
						if tx.Timeout <= uint32(now-tx.CreateTime) {
							log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back...", tx)
							wg.Add(1)
							go func() {
								defer wg.Done()
								req := &proto.TxApplyRequest{
									TxID:        tx.TxID,
									TmID:        uint64(tx.TmID),
									TxApplyType: proto.TxRollback,
								}
								status, err := tm.rollbackTransaction(req)
								if err == nil && status == proto.OpOk {
									tm.Lock()
									delete(tm.transactions, tx.TxID)
									tm.Unlock()
									log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back done", tx)
								} else {
									log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back failed, status(%v), err(%v)",
										tx, status, err)
								}
							}()

						} else {
							log.LogDebugf("processExpiredTransactions: transaction (%v) is ongoing", tx)
						}
					}
					wg.Wait()

					scanTimer.Reset(time.Second)
				}

			}
		}
	}
}
*/
func (tm *TransactionManager) processExpiredTransactions() {
	//scan transactions periodically, and invoke `rollbackTransaction` to roll back expired transactions
	log.LogDebugf("processExpiredTransactions for mp[%v] started", tm.txProcessor.mp.config.PartitionId)
	clearInterval := time.Second * 60
	clearTimer := time.NewTimer(clearInterval)

	for {
		select {
		case <-tm.txProcessor.mp.stopC:
			log.LogDebugf("processExpiredTransactions for mp[%v] stopped", tm.txProcessor.mp.config.PartitionId)
			return
		case <-tm.exitCh:
			log.LogDebugf("processExpiredTransactions for mp[%v] stopped", tm.txProcessor.mp.config.PartitionId)
			return
		case <-clearTimer.C:
			tm.blacklist.Clear()
			clearTimer.Reset(clearInterval)
			//log.LogDebugf("processExpiredTransactions: blacklist cleared")
		case <-tm.newTxCh:
			scanTimer := time.NewTimer(0)
		LOOP:
			for {
				select {
				case <-tm.txProcessor.mp.stopC:
					log.LogDebugf("processExpiredTransactions for mp[%v] stopped", tm.txProcessor.mp.config.PartitionId)
					return
				case <-tm.exitCh:
					return
				case <-scanTimer.C:
					if tm.txTree.Len() == 0 {
						break LOOP
					}

					var wg sync.WaitGroup
					f := func(i BtreeItem) bool {
						tx := i.(*proto.TransactionInfo)

						//now := time.Now().Unix()
						if tx.IsExpired() {
							log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back...", tx)
							wg.Add(1)
							go func() {
								defer wg.Done()
								req := &proto.TxApplyRequest{
									TxID:        tx.TxID,
									TmID:        uint64(tx.TmID),
									TxApplyType: proto.TxRollback,
								}
								status, err := tm.rollbackTransaction(req, RbFromTM)
								if err == nil && status == proto.OpOk {
									//tm.Lock()
									//tm.txTree.Delete(tx)
									//tm.Unlock()
									log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back done", tx)
								} else {
									log.LogWarnf("processExpiredTransactions: transaction (%v) expired, rolling back failed, status(%v), err(%v)",
										tx, status, err)
								}
							}()

						} else {
							log.LogDebugf("processExpiredTransactions: transaction (%v) is ongoing", tx)
						}
						return true
					}

					tm.txTree.GetTree().Ascend(f)
					wg.Wait()
					scanTimer.Reset(100 * time.Millisecond)
				}
			}
		}
	}

}

func (tm *TransactionManager) Start() {
	//only metapartition raft leader can start scan goroutine
	tm.Lock()
	defer tm.Unlock()
	if tm.started {
		return
	}
	//if _, ok := tm.txProcessor.mp.IsLeader(); ok {
	go tm.processExpiredTransactions()
	tm.notifyNewTransaction()
	//}
	tm.started = true
	log.LogDebugf("TransactionManager for mp[%v] started", tm.txProcessor.mp.config.PartitionId)
}

func (tm *TransactionManager) Stop() {
	//log.LogDebugf("TransactionManager for mp[%v] enter", tm.txProcessor.mp.config.PartitionId)
	tm.Lock()
	defer tm.Unlock()
	//log.LogDebugf("TransactionManager for mp[%v] enter2", tm.txProcessor.mp.config.PartitionId)
	if !tm.started {
		log.LogDebugf("TransactionManager for mp[%v] already stopped", tm.txProcessor.mp.config.PartitionId)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("transaction manager process Stop,err:%v", r)
		}
	}()

	close(tm.exitCh)
	tm.started = false
	log.LogDebugf("TransactionManager for mp[%v] stopped", tm.txProcessor.mp.config.PartitionId)
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

func (tm *TransactionManager) notifyNewTransaction() {
	select {
	case tm.newTxCh <- struct{}{}:
		log.LogDebugf("notifyNewTransaction, scan routine notified!")
	default:
		log.LogDebugf("notifyNewTransaction, skipping notify!")
	}
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
	tm.notifyNewTransaction()
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
func (tm *TransactionManager) rollbackTransaction(req *proto.TxApplyRequest, rbFrom uint32) (status uint8, err error) {
	status = proto.OpOk
	//1. notify all related RMs that a transaction is to be rolled back

	txId := req.TxID
	//var (
	//	tx *proto.TransactionInfo
	//	ok bool
	//)
	tx := tm.getTransaction(txId)
	if tx == nil {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("rollbackTransaction: tx[%v] not found", txId)
		return
	}

	items := make([]*txApplyItem, 0)
	for ino, inoInfo := range tx.TxInodeInfos {
		inoReq := &proto.TxInodeApplyRequest{
			TxID:        inoInfo.TxID,
			Inode:       ino,
			TxApplyType: req.TxApplyType,
			ApplyFrom:   rbFrom,
		}
		packet := proto.NewPacketReqID()
		packet.Opcode = proto.OpTxInodeRollback
		packet.PartitionID = inoInfo.MpID
		err = packet.MarshalData(inoReq)
		if err != nil {
			status = proto.OpTxInternalErr
			err = fmt.Errorf("rollbackTransaction: marshal commit inode [%v] failed", inoReq)
			return
		}
		item := &txApplyItem{
			p:       packet,
			members: inoInfo.MpMembers,
		}
		items = append(items, item)
	}

	for _, denInfo := range tx.TxDentryInfos {
		denReq := &proto.TxDentryApplyRequest{
			TxID: denInfo.TxID,
			//DenKey:      key,
			Pid:         denInfo.ParentId,
			Name:        denInfo.Name,
			TxApplyType: req.TxApplyType,
		}
		packet := proto.NewPacketReqID()
		packet.Opcode = proto.OpTxDentryRollback
		packet.PartitionID = denInfo.MpID
		err = packet.MarshalData(denReq)
		if err != nil {
			status = proto.OpTxInternalErr
			err = fmt.Errorf("rollbackTransaction: marshal commit dentry [%v] failed", denReq)
			return
		}
		item := &txApplyItem{
			p:       packet,
			members: denInfo.MpMembers,
		}
		items = append(items, item)
	}

	errorsCh := make(chan error, len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go tm.txApplyToRM(item.members, item.p, &wg, errorsCh)
	}
	wg.Wait()
	close(errorsCh)

	//todo_tx: what if some of them failed??? retry?
	if len(errorsCh) > 0 {
		status = proto.OpTxRollbackItemErr
		err = <-errorsCh
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
	status = resp.(uint8)
	return
}

type txApplyItem struct {
	p       *proto.Packet
	members string
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

	//delete(tm.transactions, txId)
	tm.txTree.Delete(tx)
	log.LogDebugf("commitTxInfo: tx[%v] is committed", tx)
	return
}

//TM notify all related RMs that a transaction is completed,
//and corresponding transaction resources(inode, dentry) can be removed
func (tm *TransactionManager) commitTransaction(req *proto.TxApplyRequest) (status uint8, err error) {
	status = proto.OpOk
	//1. notify all related RMs that a transaction is completed

	txId := req.TxID
	//var (
	//	tx *proto.TransactionInfo
	//	ok bool
	//)

	tx := tm.getTransaction(txId)
	if tx == nil {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("commitTransaction: tx[%v] not found", txId)
		return
	}

	items := make([]*txApplyItem, 0)
	for ino, inoInfo := range tx.TxInodeInfos {
		inoReq := &proto.TxInodeApplyRequest{
			TxID:        inoInfo.TxID,
			Inode:       ino,
			TxApplyType: req.TxApplyType,
		}
		packet := proto.NewPacketReqID()
		packet.Opcode = proto.OpTxInodeCommit
		packet.PartitionID = inoInfo.MpID
		err = packet.MarshalData(inoReq)
		if err != nil {
			status = proto.OpTxInternalErr
			err = fmt.Errorf("commitTransaction: marshal commit inode [%v] failed", inoReq)
			return
		}
		item := &txApplyItem{
			p:       packet,
			members: inoInfo.MpMembers,
		}
		items = append(items, item)
	}

	for _, denInfo := range tx.TxDentryInfos {
		denReq := &proto.TxDentryApplyRequest{
			TxID: denInfo.TxID,
			//DenKey:      key,
			Pid:         denInfo.ParentId,
			Name:        denInfo.Name,
			TxApplyType: req.TxApplyType,
		}
		packet := proto.NewPacketReqID()
		packet.Opcode = proto.OpTxDentryCommit
		packet.PartitionID = denInfo.MpID
		err = packet.MarshalData(denReq)
		if err != nil {
			status = proto.OpTxInternalErr
			err = fmt.Errorf("commitTransaction: marshal commit dentry [%v] failed", denReq)
			return
		}
		item := &txApplyItem{
			p:       packet,
			members: denInfo.MpMembers,
		}
		items = append(items, item)
	}

	errorsCh := make(chan error, len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go tm.txApplyToRM(item.members, item.p, &wg, errorsCh)
	}
	wg.Wait()

	close(errorsCh)

	if len(errorsCh) > 0 {
		status = proto.OpTxCommitItemErr
		err = <-errorsCh
		return
	}

	//2. TM commit the transaction
	//submit to all TM metapartition

	val, err := json.Marshal(req)
	if err != nil {
		return
	}

	var resp interface{}
	resp, err = tm.txProcessor.mp.submit(opFSMTxCommit, val)
	status = resp.(uint8)
	return
}

func (tm *TransactionManager) sendPacketToMP(addr string, p *proto.Packet) (err error) {
	var (
		mConn *net.TCPConn
		reqID = p.ReqID
		reqOp = p.Opcode
	)

	connPool := tm.txProcessor.mp.manager.connPool

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

func (tm *TransactionManager) txApplyToRMWithBlacklist(addrs []string, p *proto.Packet, skipBlacklist bool) (skippedAddrNum int, newPacket *proto.Packet, err error) {
	//addrs := strings.Split(members, ",")
	//var newPacket *proto.Packet
	skippedAddrNum = 0
	for _, addr := range addrs {
		if tm.blacklist.Has(addr) && !skipBlacklist {
			log.LogWarnf("txApplyToRM: addr[%v] is already blacklisted, retry another addr", addr)
			skippedAddrNum++
			continue
		}
		newPacket = p.GetCopy()
		err = tm.sendPacketToMP(addr, newPacket)
		if err != nil {
			tm.blacklist.Add(addr)
			//errorsCh <- err
			log.LogWarnf("txApplyToRM: apply to %v fail packet(%v) err(%s), add to blacklist and retry another addr",
				addr, newPacket, err)
			continue
		}

		status := newPacket.ResultCode
		if status != proto.OpOk {
			err = errors.New(newPacket.GetResultMsg())
			log.LogErrorf("txApplyToRM: packet(%v) err(%v) members(%v) retry another addr", newPacket, err, addr)
			//errorsCh <- err
		} else {
			break
		}
	}
	return skippedAddrNum, newPacket, err
}

func (tm *TransactionManager) txApplyToRM(members string, p *proto.Packet, wg *sync.WaitGroup, errorsCh chan error) {
	defer wg.Done()

	//var err error

	/*addrs := strings.Split(members, ",")
	var newPacket *proto.Packet
	skippedAddrNum := 0
	for _, addr := range addrs {
		if tm.blacklist.Has(addr) && !skipBlacklist {
			log.LogDebugf("txApplyToRM: addr[%v] is already blacklisted, retry another addr", addr)
			skippedAddrNum++
			continue
		}
		newPacket = p.GetCopy()
		err = tm.sendPacketToMP(addr, newPacket)
		if err != nil {
			tm.blacklist.Add(addr)
			//errorsCh <- err
			log.LogErrorf("txApplyToRM: apply to %v fail, packet(%v) err(%s) retry another addr", addr, newPacket, err)
			continue
		}

		status := newPacket.ResultCode
		if status != proto.OpOk {
			err = errors.New(newPacket.GetResultMsg())
			log.LogErrorf("txApplyToRM: packet(%v) err(%v) members(%v) retry another addr", newPacket, err, addr)
			//errorsCh <- err
		} else {
			break
		}
	}*/
	addrs := strings.Split(members, ",")
	skippedAddrNum, newPacket, err := tm.txApplyToRMWithBlacklist(addrs, p, false)
	if err == nil && skippedAddrNum == len(addrs) || err != nil && skippedAddrNum > 0 {
		log.LogInfof("txApplyToRM: retry send packet[%v] without blacklist", p)
		_, newPacket, err = tm.txApplyToRMWithBlacklist(addrs, p, true)
	}

	if err != nil {
		if (newPacket.Opcode == proto.OpTxInodeRollback && newPacket.ResultCode == proto.OpTxRbInodeNotExistErr) ||
			(newPacket.Opcode == proto.OpTxDentryRollback && newPacket.ResultCode == proto.OpTxRbDentryNotExistErr) {
			log.LogWarnf("txApplyToRM: rollback item might have not been added before: data: %v", newPacket)
		} else {
			log.LogErrorf("txApplyToRM: apply failed with members(%v), packet(%v) err(%s)", members, newPacket, err)
			errorsCh <- err
		}

	}
}

func (tr *TransactionResource) clearExpiredPlaceholder() {
	log.LogDebugf("clearExpiredPlaceholder for mp[%v] started", tr.txProcessor.mp.config.PartitionId)
	clearInterval := time.Second * proto.MaxTransactionTimeout
	clearTimer := time.NewTimer(clearInterval)
	for {
		select {
		case <-tr.txProcessor.mp.stopC:
			log.LogDebugf("clearExpiredPlaceholder for mp[%v] stopped", tr.txProcessor.mp.config.PartitionId)
			return
		case <-tr.exitCh:
			log.LogDebugf("clearExpiredPlaceholder for mp[%v] stopped", tr.txProcessor.mp.config.PartitionId)
			return
		case <-clearTimer.C:

			f := func(i BtreeItem) bool {
				rbInode := i.(*TxRollbackInode)
				if !rbInode.rbPlaceholder {
					return true
				}
				if rbInode.IsExpired() {
					tr.txRbInodeTree.Delete(rbInode)
					log.LogWarnf("clearExpiredPlaceholder: deleting expired rbInode placeholder (%v) ", rbInode)

				} else {
					log.LogWarnf("clearExpiredPlaceholder: rbInode placeholder (%v) is not expired yet", rbInode)
				}
				return true
			}
			tr.txRbInodeTree.GetTree().Ascend(f)

			f = func(i BtreeItem) bool {
				rbDentry := i.(*TxRollbackDentry)
				if !rbDentry.rbPlaceholder {
					return true
				}
				if rbDentry.IsExpired() {
					tr.txRbDentryTree.Delete(rbDentry)
					log.LogWarnf("clearExpiredPlaceholder: deleting expired rbDentry placeholder (%v) ", rbDentry)

				} else {
					log.LogWarnf("clearExpiredPlaceholder: rbDentry placeholder (%v) is not expired yet", rbDentry)
				}
				return true
			}
			tr.txRbDentryTree.GetTree().Ascend(f)
			clearTimer.Reset(clearInterval)

		}
	}

}

func (tr *TransactionResource) Start() {
	tr.Lock()
	defer tr.Unlock()
	if tr.started {
		return
	}
	go tr.clearExpiredPlaceholder()
	tr.started = true
}

func (tr *TransactionResource) Stop() {
	tr.Lock()
	defer tr.Unlock()
	if !tr.started {
		log.LogDebugf("clearExpiredPlaceholder for mp[%v] already stopped", tr.txProcessor.mp.config.PartitionId)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("TransactionResource Stop,err:%v", r)
		}
	}()

	close(tr.exitCh)
	tr.started = false
	log.LogDebugf("clearExpiredPlaceholder for mp[%v] stopped", tr.txProcessor.mp.config.PartitionId)
}

//check if item(inode, dentry) is in transaction for modifying
func (tr *TransactionResource) isInodeInTransction(ino *Inode) (inTx bool, txID string) {
	//return true only if specified inode is in an ongoing transaction(not expired yet)
	tr.Lock()
	defer tr.Unlock()

	//if rbInode, ok := tr.txRollbackInodes[ino.Inode]; ok {
	//	return true, rbInode.txInodeInfo.TxID
	//}
	if rbInode := tr.getTxRbInode(ino.Inode); rbInode != nil {
		return true, rbInode.txInodeInfo.TxID
	}
	//todo_tx: if existed transaction item is expired due to commit or rollback failure,
	// corresponding item should be rolled back? what if part of items is committed or rolled back,
	// and others are not?
	return false, ""
}

func (tr *TransactionResource) isDentryInTransction(dentry *Dentry) (inTx bool, txID string) {
	//return true only if specified dentry is in an ongoing transaction(not expired yet)
	tr.Lock()
	defer tr.Unlock()
	//key := fmt.Sprintf("%d_%s", dentry.ParentId, dentry.Name)
	//if rbDentry, ok := tr.txRollbackDentries[key]; ok {
	//	return true, rbDentry.txDentryInfo.TxID
	//}
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
				log.LogErrorf("addTxRollbackInode: rollback inode [ino(%v) txID(%v) rbType(%v)] "+
					"is conflicted with inode [ino(%v) txID(%v) rbType(%v)]",
					rbInode.inode.Inode, rbInode.txInodeInfo.TxID, rbInode.rbType,
					oldRbInode.inode.Inode, oldRbInode.txInodeInfo.TxID, oldRbInode.rbType)
				status = proto.OpTxConflictErr
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
			//return fmt.Errorf("inode(%v) is already in another transaction", rbInode.inode.Inode)
		}
	} else {
		//tr.txRollbackInodes[rbInode.inode.Inode] = rbInode
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
				log.LogErrorf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v) rbType(%v)] "+
					"is conflicted with dentry [pino(%v) name(%v)  txID(%v) rbType(%v)]",
					rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID, rbDentry.rbType,
					oldRbDentry.dentry.ParentId, oldRbDentry.dentry.Name, oldRbDentry.txDentryInfo.TxID, oldRbDentry.rbType)
				return proto.OpTxConflictErr
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
			//return fmt.Errorf("dentry [pino(%v) name(%v)] is already in another transaction",
			//	rbDentry.dentry.ParentId, rbDentry.dentry.Name)
		}
	} else {
		//tr.txRollbackDentries[rbDentry.txDentryInfo.GetKey()] = rbDentry
		tr.txRbDentryTree.ReplaceOrInsert(rbDentry, true)
	}
	log.LogDebugf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v) rbType(%v)] is added",
		rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID, rbDentry.rbType)
	return proto.OpOk
}

//RM roll back an inode, retry if error occours
func (tr *TransactionResource) rollbackInode(req *proto.TxInodeApplyRequest) (status uint8, err error) {
	//todo_tx: 添加，删除，修改，不存在item几种情况
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	//todo_tx: what if rbInode is missing for whatever non-intended reason? transaction consistency will be broken?!
	//rbItem is guaranteed by raft, if rbItem is missing, then most mp members are corrupted
	//rbInode, ok := tr.txRollbackInodes[inode]
	rbInode := tr.getTxRbInode(req.Inode)
	if rbInode == nil {
		if req.ApplyFrom == RbFromTM {
			rbPlaceholder := NewTxRbInodePlaceholder(req.Inode, req.TxID)
			//_ = tr.addTxRollbackInode(rbPlaceholder)
			tr.txRbInodeTree.ReplaceOrInsert(rbPlaceholder, true)
			log.LogDebugf("rollbackInode: rbPlaceholder[%v] added", rbPlaceholder)
		} else {
			status = proto.OpTxRbInodeNotExistErr
			err = fmt.Errorf("rollbackInode: roll back inode[%v] failed, rb inode not found", req.Inode)
		}
		return
	}

	if rbInode.txInodeInfo.TxID != req.TxID {
		status = proto.OpTxConflictErr
		err = fmt.Errorf("rollbackInode: txID %v is not matching txInodeInfo txID %v", req.TxID, rbInode.txInodeInfo.TxID)
		return
	}

	switch rbInode.rbType {
	case TxAdd:
		tr.txProcessor.mp.freeList.Remove(rbInode.inode.Inode)
		_, ok := tr.txProcessor.mp.inodeTree.ReplaceOrInsert(rbInode.inode, false)
		if ok && tr.txProcessor.mp.uidManager != nil {
			tr.txProcessor.mp.uidManager.addUidSpace(rbInode.inode.Uid, rbInode.inode.Inode, rbInode.inode.Extents.eks)
		}

		//_ = tr.txProcessor.mp.fsmCreateInode(rbInode.inode)
	case TxDelete:
		//todo_tx: fsmUnlinkInode or internalDelete?
		//_ = tr.txProcessor.mp.fsmUnlinkInode(rbInode.inode)
		if rsp := tr.txProcessor.mp.getInode(rbInode.inode); tr.txProcessor.mp.uidManager != nil && rsp.Status == proto.OpOk {
			tr.txProcessor.mp.uidManager.doMinusUidSpace(rbInode.inode.Uid, rbInode.inode.Inode, rbInode.inode.Size)
		}
		tr.txProcessor.mp.internalDeleteInode(rbInode.inode)
	case TxUpdate:
		if _, ok := tr.txProcessor.mp.inodeTree.ReplaceOrInsert(rbInode.inode, true); !ok {
			return
		}
	default:
		status = proto.OpTxRollbackUnknownRbType
		err = fmt.Errorf("rollbackInode: unknown rbType %d", rbInode.rbType)
		return
	}
	//delete(tr.txRollbackInodes, inode)
	tr.txRbInodeTree.Delete(rbInode)
	log.LogDebugf("rollbackInode: inode[%v] is rolled back in tx[%v], rbType[%v]", req.Inode, req.TxID, rbInode.rbType)
	return
}

//RM roll back a dentry, retry if error occours
func (tr *TransactionResource) rollbackDentry(req *proto.TxDentryApplyRequest) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	//todo_tx: what if rbDentry is missing for whatever non-intended reason? transaction consistency will be broken?!
	//rbDentry, ok := tr.txRollbackDentries[denKey]
	rbDentry := tr.getTxRbDentry(req.Pid, req.Name)
	if rbDentry == nil {
		if req.ApplyFrom == RbFromTM {
			rbPlaceholder := NewTxRbDentryPlaceholder(req.Pid, req.Name, req.TxID)
			//_ = tr.addTxRollbackDentry(rbPlaceholder)
			tr.txRbDentryTree.ReplaceOrInsert(rbPlaceholder, true)
			log.LogDebugf("rollbackDentry: rbPlaceholder[%v] added", rbPlaceholder)
		} else {
			status = proto.OpTxRbDentryNotExistErr
			err = fmt.Errorf("rollbackDentry: roll back dentry[%v_%v] failed, rb inode not found", req.Pid, req.Name)
		}

		return
	}

	if rbDentry.txDentryInfo.TxID != req.TxID {
		status = proto.OpTxConflictErr
		err = fmt.Errorf("rollbackDentry: txID %v is not matching txInodeInfo txID %v", req.TxID, rbDentry.txDentryInfo.TxID)
		return
	}

	switch rbDentry.rbType {
	case TxAdd:
		_ = tr.txProcessor.mp.fsmCreateDentry(rbDentry.dentry, true)
	case TxDelete:
		//todo_tx: fsmUnlinkInode or internalDelete?
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

	//delete(tr.txRollbackDentries, denKey)
	tr.txRbDentryTree.Delete(rbDentry)
	log.LogDebugf("rollbackDentry: denKey[%v] is rolled back in tx[%v], rbType[%v]",
		rbDentry.txDentryInfo.GetKey(), req.TxID, rbDentry.rbType)
	return
}

//RM simplely remove the inode from TransactionResource
func (tr *TransactionResource) commitInode(txID string, inode uint64) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	rbInode := tr.getTxRbInode(inode)
	//rbInode, ok := tr.txRollbackInodes[inode]
	if rbInode == nil {
		status = proto.OpTxRbInodeNotExistErr
		err = fmt.Errorf("commitInode: commit inode[%v] failed, rb inode not found", inode)
		return
	}

	if rbInode.txInodeInfo.TxID != txID {
		status = proto.OpTxConflictErr
		err = fmt.Errorf("commitInode: txID %v is not matching txInodeInfo txID %v", txID, rbInode.txInodeInfo.TxID)
		return
	}

	//delete(tr.txRollbackInodes, inode)
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
	//rbDentry, ok := tr.txRollbackDentries[denKey]
	if rbDentry == nil {
		status = proto.OpTxRbDentryNotExistErr
		err = fmt.Errorf("commitDentry: commit dentry[%v_%v] failed, rb dentry not found", pId, name)
		return
	}

	if rbDentry.txDentryInfo.TxID != txID {
		status = proto.OpTxConflictErr
		err = fmt.Errorf("commitDentry: txID %v is not matching txDentryInfo txID %v", txID, rbDentry.txDentryInfo.TxID)
		return
	}

	//delete(tr.txRollbackDentries, denKey)
	tr.txRbDentryTree.Delete(rbDentry)
	log.LogDebugf("commitDentry: dentry[%v] is committed", rbDentry.txDentryInfo.GetKey())
	return
}
