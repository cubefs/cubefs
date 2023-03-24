package metanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

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

type TxRollbackInode struct {
	inode       *Inode
	txInodeInfo *proto.TxInodeInfo
	rbType      uint32 //Rollback Type
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
	return
}

func NewTxRollbackInode(inode *Inode, txInodeInfo *proto.TxInodeInfo, rbType uint32) *TxRollbackInode {
	return &TxRollbackInode{
		inode:       inode,
		txInodeInfo: txInodeInfo,
		rbType:      rbType,
	}
}

type TxRollbackDentry struct {
	dentry       *Dentry
	txDentryInfo *proto.TxDentryInfo
	rbType       uint32 //Rollback Type
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

func NewTxRollbackDentry(dentry *Dentry, txDentryInfo *proto.TxDentryInfo, rbType uint32) *TxRollbackDentry {
	return &TxRollbackDentry{
		dentry:       dentry,
		txDentryInfo: txDentryInfo,
		rbType:       rbType,
	}
}

//TM
type TransactionManager struct {
	//need persistence and sync to all the raft members of the mp
	txIdAlloc    *TxIDAllocator
	transactions map[string]*proto.TransactionInfo //key: metapartitionID_mpTxID
	txProcessor  *TransactionProcessor
	started      bool
	newTxCh      chan struct{}
	exitCh       chan struct{}
	sync.RWMutex
}

//RM
type TransactionResource struct {
	//need persistence and sync to all the raft members of the mp
	txRollbackInodes   map[uint64]*TxRollbackInode  //key: inode id
	txRollbackDentries map[string]*TxRollbackDentry // key: parentId_name
	txProcessor        *TransactionProcessor
	sync.RWMutex
}

type TransactionProcessor struct {
	txManager  *TransactionManager  //TM
	txResource *TransactionResource //RM
	mp         *metaPartition
}

func NewTransactionManager(txProcessor *TransactionProcessor) *TransactionManager {
	txMgr := &TransactionManager{
		txIdAlloc:    newTxIDAllocator(),
		transactions: make(map[string]*proto.TransactionInfo, 0),
		txProcessor:  txProcessor,
		started:      false,
		exitCh:       make(chan struct{}),
		newTxCh:      make(chan struct{}),
	}
	//txMgr.Start()
	return txMgr
}

func NewTransactionResource(txProcessor *TransactionProcessor) *TransactionResource {
	return &TransactionResource{
		txRollbackInodes:   make(map[uint64]*TxRollbackInode, 0),
		txRollbackDentries: make(map[string]*TxRollbackDentry, 0),
		txProcessor:        txProcessor,
	}
}

func NewTransactionProcessor(mp *metaPartition) *TransactionProcessor {
	txProcessor := &TransactionProcessor{
		mp: mp,
	}
	txProcessor.txManager = NewTransactionManager(txProcessor)
	txProcessor.txResource = NewTransactionResource(txProcessor)
	return txProcessor
}

func (tm *TransactionManager) copyTransactions() map[string]*proto.TransactionInfo {
	transactions := make(map[string]*proto.TransactionInfo, 0)
	tm.Lock()
	defer tm.Unlock()

	for key, tx := range tm.transactions {
		transactions[key] = tx
	}
	return transactions
}

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

func (tm *TransactionManager) Start() {
	//only metapartition raft leader can start scan goroutine
	tm.Lock()
	defer tm.Unlock()
	if tm.started {
		return
	}
	//if _, ok := tm.txProcessor.mp.IsLeader(); ok {
	go tm.processExpiredTransactions()
	//}
	tm.started = true
	log.LogDebugf("TransactionManager for mp[%v] started", tm.txProcessor.mp.config.PartitionId)
}

func (tm *TransactionManager) Stop() {
	log.LogDebugf("TransactionManager for mp[%v] enter", tm.txProcessor.mp.config.PartitionId)
	tm.Lock()
	defer tm.Unlock()
	log.LogDebugf("TransactionManager for mp[%v] enter2", tm.txProcessor.mp.config.PartitionId)
	if !tm.started {
		log.LogDebugf("TransactionManager for mp[%v] enter3", tm.txProcessor.mp.config.PartitionId)
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
	var txInfo *proto.TransactionInfo
	var ok bool
	if txInfo, ok = tm.transactions[txID]; !ok {
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
	var txInfo *proto.TransactionInfo
	var ok bool
	if txInfo, ok = tm.transactions[txID]; !ok {
		return nil
	}
	if info, ok = txInfo.TxDentryInfos[key]; !ok {
		return nil
	}
	return
}

func (tm *TransactionManager) getTransaction(txID string) (txInfo *proto.TransactionInfo) {
	tm.RLock()
	defer tm.RUnlock()
	var ok bool
	if txInfo, ok = tm.transactions[txID]; !ok {
		return nil
	}
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

	if _, ok := tm.transactions[txInfo.TxID]; ok {
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

	tm.transactions[txInfo.TxID] = txInfo
	tm.Unlock()
	log.LogDebugf("registerTransaction: txInfo(%v)", txInfo)
	//3. notify new transaction
	tm.notifyNewTransaction()
	return nil
}

//TM roll back a transaction, sends roll back request to all related RMs,
//a rollback can be initiated by client or triggered by TM scan routine
func (tm *TransactionManager) rollbackTransaction(req *proto.TxApplyRequest) (status uint8, err error) {
	status = proto.OpOk
	//1. notify all related RMs that a transaction is to be rolled back

	txId := req.TxID
	var (
		tx *proto.TransactionInfo
		ok bool
	)
	if tx, ok = tm.transactions[txId]; !ok {
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

	for key, denInfo := range tx.TxDentryInfos {
		denReq := &proto.TxDentryApplyRequest{
			TxID:        denInfo.TxID,
			DenKey:      key,
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
	txInfo, ok := tm.transactions[txId]
	if !ok {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("rollbackTxInfo: rollback tx[%v] failed, not found", txId)
		return
	}

	delete(tm.transactions, txId)
	log.LogDebugf("rollbackTxInfo: tx[%v] is rolled back", txInfo)
	return
}

func (tm *TransactionManager) commitTxInfo(txId string) (status uint8, err error) {
	tm.Lock()
	defer tm.Unlock()
	status = proto.OpOk
	txInfo, ok := tm.transactions[txId]
	if !ok {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("commitTxInfo: commit tx[%v] failed, not found", txId)
		return
	}

	delete(tm.transactions, txId)
	log.LogDebugf("commitTxInfo: tx[%v] is committed", txInfo)
	return
}

//TM notify all related RMs that a transaction is completed,
//and corresponding transaction resources(inode, dentry) can be removed
func (tm *TransactionManager) commitTransaction(req *proto.TxApplyRequest) (status uint8, err error) {
	status = proto.OpOk
	//1. notify all related RMs that a transaction is completed

	txId := req.TxID
	var (
		tx *proto.TransactionInfo
		ok bool
	)
	if tx, ok = tm.transactions[txId]; !ok {
		status = proto.OpTxInfoNotExistErr
		err = fmt.Errorf("commitTransaction: tx[%v] not found", txId)
		return
	}

	//mpAddr->TransactionInfo
	//mpTxs := make(map[string]*proto.TransactionInfo, 0)
	//
	//for ino, inoInfo := range tx.TxInodeInfos {
	//	if txInfo, ok := mpTxs[inoInfo.MpMembers]; ok {
	//		txInfo.TxInodeInfos[ino] = inoInfo
	//	} else {
	//		newTxInfo := proto.NewTransactionInfo(0)
	//		newTxInfo.TxInodeInfos[ino] = inoInfo
	//		mpTxs[inoInfo.MpMembers] = newTxInfo
	//	}
	//}
	//
	//for denKey, denInfo := range tx.TxDentryInfos {
	//	if txInfo, ok := mpTxs[denInfo.MpMembers]; ok {
	//		txInfo.TxDentryInfos[denKey] = denInfo
	//	} else {
	//		newTxInfo := proto.NewTransactionInfo(0)
	//		newTxInfo.TxDentryInfos[denKey] = denInfo
	//		mpTxs[denInfo.MpMembers] = newTxInfo
	//	}
	//}
	//errorsCh := make(chan error, len(mpTxs))

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

	for key, denInfo := range tx.TxDentryInfos {
		denReq := &proto.TxDentryApplyRequest{
			TxID:        denInfo.TxID,
			DenKey:      key,
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

func (tm *TransactionManager) txApplyToRM(members string, p *proto.Packet, wg *sync.WaitGroup, errorsCh chan error) {
	defer wg.Done()

	var err error

	addrs := strings.Split(members, ",")
	for _, addr := range addrs {
		err = tm.sendPacketToMP(addr, p)
		if err != nil {
			//errorsCh <- err
			log.LogErrorf("txApplyToRM: apply to %v fail, packet(%v) err(%s) retry another addr", addr, p, err)
			continue
		}

		status := p.ResultCode
		if status != proto.OpOk {
			err = errors.New(p.GetResultMsg())
			log.LogErrorf("txApplyToRM: packet(%v) err(%v) members(%v) retry another addr", p, err, addr)
			//errorsCh <- err
		} else {
			break
		}
	}

	if err != nil {
		if (p.Opcode == proto.OpTxInodeRollback && p.ResultCode == proto.OpTxRbInodeNotExistErr) ||
			(p.Opcode == proto.OpTxDentryRollback && p.ResultCode == proto.OpTxRbDentryNotExistErr) {
			log.LogWarnf("txApplyToRM: rollback item might have not been added before: data: %v", p)
		} else {
			log.LogErrorf("txApplyToRM: apply failed with members(%v), packet(%v) err(%s)", members, p, err)
			errorsCh <- err
		}

	}
}

//check if item(inode, dentry) is in transaction for modifying
func (tr *TransactionResource) isInodeInTransction(ino *Inode) (inTx bool, txID string) {
	//return true only if specified inode is in an ongoing transaction(not expired yet)
	tr.Lock()
	defer tr.Unlock()

	if rbInode, ok := tr.txRollbackInodes[ino.Inode]; ok {
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
	key := fmt.Sprintf("%d_%s", dentry.ParentId, dentry.Name)
	if rbDentry, ok := tr.txRollbackDentries[key]; ok {
		return true, rbDentry.txDentryInfo.TxID
	}
	return false, ""
}

//RM add an `TxRollbackInode` into `txRollbackInodes`
func (tr *TransactionResource) addTxRollbackInode(rbInode *TxRollbackInode) (status uint8) {
	tr.Lock()
	defer tr.Unlock()

	now := time.Now().Unix()
	if rbInode.txInodeInfo.Timeout <= uint32(now-rbInode.txInodeInfo.CreateTime) {
		log.LogWarnf("addTxRollbackInode: transaction(%v) is expired for rollback inode(%v), create time(%v), now(%v)",
			rbInode.txInodeInfo.TxID, rbInode.inode.Inode, rbInode.txInodeInfo.CreateTime, now)
		return proto.OpTxTimeoutErr
	}

	if oldRbInode, ok := tr.txRollbackInodes[rbInode.inode.Inode]; ok {
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
		tr.txRollbackInodes[rbInode.inode.Inode] = rbInode
	}
	log.LogDebugf("addTxRollbackInode: rollback inode [ino(%v) txID(%v)] is added", rbInode.inode.Inode, rbInode.txInodeInfo.TxID)
	return proto.OpOk
}

//RM add a `TxRollbackDentry` into `txRollbackDentries`
func (tr *TransactionResource) addTxRollbackDentry(rbDentry *TxRollbackDentry) (status uint8) {
	tr.Lock()
	defer tr.Unlock()

	now := time.Now().Unix()
	if rbDentry.txDentryInfo.Timeout <= uint32(now-rbDentry.txDentryInfo.CreateTime) {
		log.LogWarnf("addTxRollbackDentry: transaction(%v) is expired for rollback dentry [pino(%v) name(%v)], create time(%v), now(%v)",
			rbDentry.txDentryInfo.TxID, rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.CreateTime, now)
		return proto.OpTxTimeoutErr
	}

	if oldRbDentry, ok := tr.txRollbackDentries[rbDentry.txDentryInfo.GetKey()]; ok {
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
		tr.txRollbackDentries[rbDentry.txDentryInfo.GetKey()] = rbDentry
	}
	log.LogDebugf("addTxRollbackDentry: rollback dentry [pino(%v) name(%v) txID(%v) rbType(%v)] is added",
		rbDentry.dentry.ParentId, rbDentry.dentry.Name, rbDentry.txDentryInfo.TxID, rbDentry.rbType)
	return proto.OpOk
}

//RM roll back an inode, retry if error occours
func (tr *TransactionResource) rollbackInode(txID string, inode uint64) (status uint8, err error) {
	//todo_tx: 添加，删除，修改，不存在item几种情况
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	//todo_tx: what if rbInode is missing for whatever non-intended reason? transaction consistency will be broken?!
	//rbItem is guaranteed by raft, if rbItem is missing, then most mp members are corrupted
	rbInode, ok := tr.txRollbackInodes[inode]
	if !ok {
		status = proto.OpTxRbInodeNotExistErr
		err = fmt.Errorf("rollbackInode: roll back inode[%v] failed, rb inode not found", inode)
		return
	}

	if rbInode.txInodeInfo.TxID != txID {
		status = proto.OpTxConflictErr
		err = fmt.Errorf("rollbackInode: txID %v is not matching txInodeInfo txID %v", txID, rbInode.txInodeInfo.TxID)
		return
	}

	switch rbInode.rbType {
	case TxAdd:
		tr.txProcessor.mp.freeList.Remove(rbInode.inode.Inode)
		tr.txProcessor.mp.inodeTree.ReplaceOrInsert(rbInode.inode, true)
		//_ = tr.txProcessor.mp.fsmCreateInode(rbInode.inode)
	case TxDelete:
		//todo_tx: fsmUnlinkInode or internalDelete?
		//_ = tr.txProcessor.mp.fsmUnlinkInode(rbInode.inode)
		tr.txProcessor.mp.internalDeleteInode(rbInode.inode)
	case TxUpdate:
		if _, ok = tr.txProcessor.mp.inodeTree.ReplaceOrInsert(rbInode.inode, true); !ok {
			return
		}
	default:
		status = proto.OpTxRollbackUnknownRbType
		err = fmt.Errorf("rollbackInode: unknown rbType %d", rbInode.rbType)
		return
	}
	delete(tr.txRollbackInodes, inode)
	log.LogDebugf("rollbackInode: inode[%v] is rolled back in tx[%v], rbType[%v]", inode, txID, rbInode.rbType)
	return
}

//RM roll back a dentry, retry if error occours
func (tr *TransactionResource) rollbackDentry(txID string, denKey string) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	//todo_tx: what if rbDentry is missing for whatever non-intended reason? transaction consistency will be broken?!
	rbDentry, ok := tr.txRollbackDentries[denKey]
	if !ok {
		status = proto.OpTxRbDentryNotExistErr
		err = fmt.Errorf("rollbackDentry: roll back dentry[%v] failed, rb inode not found", denKey)
		return
	}

	if rbDentry.txDentryInfo.TxID != txID {
		status = proto.OpTxConflictErr
		err = fmt.Errorf("rollbackDentry: txID %v is not matching txInodeInfo txID %v", txID, rbDentry.txDentryInfo.TxID)
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

	delete(tr.txRollbackDentries, denKey)
	log.LogDebugf("rollbackDentry: denKey[%v] is rolled back in tx[%v], rbType[%v]", denKey, txID, rbDentry.rbType)
	return
}

//RM simplely remove the inode from TransactionResource
func (tr *TransactionResource) commitInode(txID string, inode uint64) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk
	rbInode, ok := tr.txRollbackInodes[inode]
	if !ok {
		status = proto.OpTxRbInodeNotExistErr
		err = fmt.Errorf("commitInode: commit inode[%v] failed, rb inode not found", inode)
		return
	}

	if rbInode.txInodeInfo.TxID != txID {
		status = proto.OpTxConflictErr
		err = fmt.Errorf("commitInode: txID %v is not matching txInodeInfo txID %v", txID, rbInode.txInodeInfo.TxID)
		return
	}

	delete(tr.txRollbackInodes, inode)
	log.LogDebugf("commitInode: inode[%v] is committed", inode)
	return
}

//RM simplely remove the dentry from TransactionResource
func (tr *TransactionResource) commitDentry(txID string, denKey string) (status uint8, err error) {
	tr.Lock()
	defer tr.Unlock()
	status = proto.OpOk

	rbDentry, ok := tr.txRollbackDentries[denKey]
	if !ok {
		status = proto.OpTxRbDentryNotExistErr
		err = fmt.Errorf("commitDentry: commit dentry[%v] failed, rb dentry not found", denKey)
		return
	}

	if rbDentry.txDentryInfo.TxID != txID {
		status = proto.OpTxConflictErr
		err = fmt.Errorf("commitDentry: txID %v is not matching txDentryInfo txID %v", txID, rbDentry.txDentryInfo.TxID)
		return
	}

	delete(tr.txRollbackDentries, denKey)
	log.LogDebugf("commitDentry: dentry[%v] is committed", denKey)
	return
}
