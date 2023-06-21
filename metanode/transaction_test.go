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
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

//var manager = &metadataManager{}
var mp1 *metaPartition
var mp2 *metaPartition
var mp3 *metaPartition
var DirModeType uint32 = 2147484141
var FileModeType uint32 = 420

const (
	MemberAddrs = "127.0.0.1:17210,127.0.0.2:17210,127.0.0.3:17210"
	inodeNum    = 1001
	pInodeNum   = 1002
	inodeNum2   = 1003
	inodeNum3   = 1004
	dentryName  = "parent"
)

func newMetaPartition(PartitionId uint64, manager *metadataManager) (mp *metaPartition) {

	var metaConf = &MetaPartitionConfig{
		PartitionId:   PartitionId,
		VolName:       "testVol",
		PartitionType: proto.VolumeTypeHot,
	}

	mp = &metaPartition{
		config:        metaConf,
		dentryTree:    NewBtree(),
		inodeTree:     NewBtree(),
		extendTree:    NewBtree(),
		multipartTree: NewBtree(),
		stopC:         make(chan bool),
		storeChan:     make(chan *storeMsg, 100),
		freeList:      newFreeList(),
		extDelCh:      make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		extReset:      make(chan struct{}),
		vol:           NewVol(),
		manager:       manager,
	}
	mp.config.Cursor = 1000
	mp.config.End = 100000

	mp.txProcessor = NewTransactionProcessor(mp)
	return mp
}

func initMps(t *testing.T) {

	mp1 = newMetaPartition(10001, &metadataManager{})
	mp2 = newMetaPartition(10002, &metadataManager{})
	mp3 = newMetaPartition(10003, &metadataManager{})

	//txMgr := NewTransactionManager(nil)
	//txRsc := NewTransactionResource(nil)
	//ino := testCreateInode(nil, DirModeType)
	//t.Logf("cursor %v create ino %v", mp.config.Cursor, ino)
}

func (i *Inode) Equal(inode *Inode) bool {
	i.RLock()
	if inode.Uid != i.Uid || inode.Gid != i.Gid || inode.Size != i.Size || inode.Generation != i.Generation ||
		inode.CreateTime != i.CreateTime || inode.ModifyTime != i.ModifyTime || inode.AccessTime != i.AccessTime ||
		inode.NLink != i.NLink || inode.Flag != i.Flag || inode.Reserved != i.Reserved {
		return false
	}
	if !reflect.DeepEqual(inode.LinkTarget, i.LinkTarget) {
		return false
	}

	if !reflect.DeepEqual(inode.Extents.eks, i.Extents.eks) {
		return false
	}

	if !reflect.DeepEqual(inode.ObjExtents.eks, i.ObjExtents.eks) {
		return false
	}
	i.RUnlock()
	return true
}

func (i *TxRollbackInode) Equal(txRbInode *TxRollbackInode) bool {
	if i.rbType != txRbInode.rbType {
		return false
	}
	if !i.inode.Equal(txRbInode.inode) {
		return false
	}
	if !reflect.DeepEqual(i.txInodeInfo, txRbInode.txInodeInfo) {
		return false
	}
	return true
}

func TestRollbackInodeSerialization(t *testing.T) {
	txInodeInfo := proto.NewTxInodeInfo(MemberAddrs, inodeNum, 10001)
	inode := NewInode(inodeNum, FileModeType)
	rbInode := NewTxRollbackInode(inode, []uint32{}, txInodeInfo, TxAdd)
	var data []byte
	data, _ = rbInode.Marshal()

	txRbInode := NewTxRollbackInode(nil, []uint32{}, nil, 0)
	txRbInode.Unmarshal(data)
	assert.True(t, rbInode.Equal(txRbInode))
}

func TestRollbackDentrySerialization(t *testing.T) {
	txDentryInfo := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	dentry := &Dentry{
		ParentId: pInodeNum,
		Name:     dentryName,
		Inode:    inodeNum,
		Type:     FileModeType,
	}
	rbDentry := NewTxRollbackDentry(dentry, txDentryInfo, TxAdd)
	var data []byte
	data, _ = rbDentry.Marshal()

	txRbDentry := NewTxRollbackDentry(nil, nil, 0)
	txRbDentry.Unmarshal(data)
	assert.True(t, reflect.DeepEqual(rbDentry, txRbDentry))
}

func TestNextTxID(t *testing.T) {
	initMps(t)
	txMgr := mp1.txProcessor.txManager

	var id uint64 = 2
	expectedId := fmt.Sprintf("%d_%d", mp1.config.PartitionId, id+1)
	txMgr.txIdAlloc.setTransactionID(id)
	assert.Equal(t, expectedId, txMgr.nextTxID())
}

func TestTxMgrOp(t *testing.T) {
	initMps(t)
	txInfo := proto.NewTransactionInfo(5, proto.TxTypeCreate)
	txDentryInfo := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txInfo.TxDentryInfos[txDentryInfo.GetKey()] = txDentryInfo
	if !txInfo.IsInitialized() {
		mp1.initTxInfo(txInfo)
	}
	txId := txInfo.TxID

	txMgr := mp1.txProcessor.txManager

	//register
	id := txMgr.txIdAlloc.getTransactionID()
	expectedId := fmt.Sprintf("%d_%d", mp1.config.PartitionId, id)
	assert.Equal(t, expectedId, txId)
	txMgr.registerTransaction(txInfo)

	//get
	gotTxInfo := txMgr.getTransaction(txId)
	assert.Equal(t, txInfo, gotTxInfo)

	//get Tx Inode Info
	gotTxInodeInfo := txMgr.getTxInodeInfo(txId, inodeNum)
	assert.True(t, nil == gotTxInodeInfo)

	gotTxDentryInfo := txMgr.getTxDentryInfo(txId, txDentryInfo.GetKey())
	assert.True(t, gotTxDentryInfo == txDentryInfo)

	//rollback
	txMgr.rollbackTxInfo(txId)
	gotTxInfo = txMgr.getTransaction(txId)
	assert.True(t, nil == gotTxInfo)

	//commit
	status, _ := txMgr.commitTxInfo("dummy_txId")
	assert.Equal(t, proto.OpTxInfoNotExistErr, status)
}

func TestTxRscOp(t *testing.T) {
	initMps(t)
	txMgr := mp1.txProcessor.txManager

	//rbInode
	txInodeInfo1 := proto.NewTxInodeInfo(MemberAddrs, inodeNum, 10001)
	txInodeInfo1.TxID = txMgr.nextTxID()
	txInodeInfo1.Timeout = 5
	txInodeInfo1.CreateTime = time.Now().UnixNano()
	inode1 := NewInode(inodeNum, FileModeType)
	rbInode1 := NewTxRollbackInode(inode1, []uint32{}, txInodeInfo1, TxAdd)

	txInodeInfo2 := proto.NewTxInodeInfo(MemberAddrs, inodeNum, 10001)
	txInodeInfo2.TxID = txMgr.nextTxID()
	txInodeInfo2.Timeout = 5
	txInodeInfo2.CreateTime = time.Now().UnixNano()
	rbInode2 := NewTxRollbackInode(inode1, []uint32{}, txInodeInfo2, TxAdd)

	txRsc := mp1.txProcessor.txResource
	status := txRsc.addTxRollbackInode(rbInode1)
	assert.Equal(t, proto.OpOk, status)
	status = txRsc.addTxRollbackInode(rbInode1)
	assert.Equal(t, proto.OpOk, status)

	inTx, _ := txRsc.isInodeInTransction(inode1)
	assert.True(t, inTx)

	status = txRsc.addTxRollbackInode(rbInode2)
	assert.Equal(t, proto.OpTxConflictErr, status)

	//rbDentry
	txDentryInfo1 := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	dentry := &Dentry{
		ParentId: pInodeNum,
		Name:     dentryName,
		Inode:    inodeNum,
		Type:     FileModeType,
	}
	txDentryInfo1.TxID = txMgr.nextTxID()
	txDentryInfo1.Timeout = 5
	txDentryInfo1.CreateTime = time.Now().UnixNano()
	rbDentry1 := NewTxRollbackDentry(dentry, txDentryInfo1, TxAdd)

	txDentryInfo2 := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txDentryInfo2.TxID = txMgr.nextTxID()
	txDentryInfo2.Timeout = 5
	txDentryInfo2.CreateTime = time.Now().UnixNano()
	rbDentry2 := NewTxRollbackDentry(dentry, txDentryInfo2, TxAdd)

	status = txRsc.addTxRollbackDentry(rbDentry1)
	assert.Equal(t, proto.OpOk, status)
	status = txRsc.addTxRollbackDentry(rbDentry1)
	assert.Equal(t, proto.OpOk, status)

	inTx, _ = txRsc.isDentryInTransction(dentry)
	assert.True(t, inTx)

	status = txRsc.addTxRollbackDentry(rbDentry2)
	assert.Equal(t, proto.OpTxConflictErr, status)
}

func mockAddTxInode(mp *metaPartition) *TxRollbackInode {
	txMgr := mp.txProcessor.txManager
	txInodeInfo1 := proto.NewTxInodeInfo(MemberAddrs, inodeNum, 10001)
	txInodeInfo1.TxID = txMgr.nextTxID()
	txInodeInfo1.Timeout = 5
	txInodeInfo1.CreateTime = time.Now().UnixNano()
	inode1 := NewInode(inodeNum, FileModeType)
	rbInode := NewTxRollbackInode(inode1, []uint32{}, txInodeInfo1, TxDelete)
	txRsc := mp.txProcessor.txResource
	txRsc.addTxRollbackInode(rbInode)

	mp.inodeTree.ReplaceOrInsert(inode1, true)
	return rbInode
}

func mockDeleteTxInode(mp *metaPartition) *TxRollbackInode {
	inode2 := NewInode(inodeNum2, FileModeType)
	mp.inodeTree.ReplaceOrInsert(inode2, true)

	txMgr := mp.txProcessor.txManager
	txInodeInfo2 := proto.NewTxInodeInfo(MemberAddrs, inodeNum2, 10001)
	txInodeInfo2.TxID = txMgr.nextTxID()
	txInodeInfo2.Timeout = 5
	txInodeInfo2.CreateTime = time.Now().UnixNano()
	rbInode := NewTxRollbackInode(inode2, []uint32{}, txInodeInfo2, TxAdd)
	txRsc := mp.txProcessor.txResource
	txRsc.addTxRollbackInode(rbInode)

	mp.inodeTree.Delete(inode2)
	return rbInode
}

//func mockUpdateTxInode(mp *metaPartition) *TxRollbackInode {
//	inode3 := NewInode(inodeNum3, FileModeType)
//	oldInode, ok := mp.inodeTree.ReplaceOrInsert(inode3, true)
//
//	txMgr := mp.txProcessor.txManager
//	txInodeInfo3 := proto.NewTxInodeInfo(MemberAddrs, inodeNum3, 10001)
//	txInodeInfo3.TxID = txMgr.nextTxID()
//	rbInode := NewTxRollbackInode(inode3, txInodeInfo3, TxUpdate)
//}

func mockAddTxDentry(mp *metaPartition) *TxRollbackDentry {
	txMgr := mp.txProcessor.txManager
	txDentryInfo1 := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txDentryInfo1.TxID = txMgr.nextTxID()
	txDentryInfo1.Timeout = 5
	txDentryInfo1.CreateTime = time.Now().UnixNano()
	dentry1 := &Dentry{
		ParentId: pInodeNum,
		Name:     dentryName,
		Inode:    1001,
		Type:     0,
	}
	rbDentry := NewTxRollbackDentry(dentry1, txDentryInfo1, TxDelete)
	txRsc := mp.txProcessor.txResource
	txRsc.addTxRollbackDentry(rbDentry)

	mp.dentryTree.ReplaceOrInsert(dentry1, true)
	return rbDentry
}

func mockDeleteTxDentry(mp *metaPartition) *TxRollbackDentry {
	dentry2 := &Dentry{
		ParentId: pInodeNum,
		Name:     dentryName,
		Inode:    1001,
		Type:     0,
	}
	mp.dentryTree.ReplaceOrInsert(dentry2, true)

	txMgr := mp.txProcessor.txManager
	txDentryInfo2 := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txDentryInfo2.TxID = txMgr.nextTxID()
	txDentryInfo2.Timeout = 5
	txDentryInfo2.CreateTime = time.Now().UnixNano()
	rbDentry := NewTxRollbackDentry(dentry2, txDentryInfo2, TxAdd)
	txRsc := mp.txProcessor.txResource
	txRsc.addTxRollbackDentry(rbDentry)

	mp.dentryTree.Delete(dentry2)
	return rbDentry
}

func TestTxRscRollback(t *testing.T) {
	initMps(t)
	//roll back add inode
	rbInode1 := mockAddTxInode(mp1)
	txRsc := mp1.txProcessor.txResource
	req1 := &proto.TxInodeApplyRequest{
		TxID:  rbInode1.txInodeInfo.TxID,
		Inode: rbInode1.inode.Inode,
	}
	status, err := txRsc.rollbackInode(req1)
	assert.True(t, status == proto.OpOk && err == nil)

	//roll back delete inode
	rbInode2 := mockDeleteTxInode(mp1)
	req2 := &proto.TxInodeApplyRequest{
		TxID:  rbInode2.txInodeInfo.TxID,
		Inode: rbInode2.inode.Inode,
	}
	status, err = txRsc.rollbackInode(req2)
	assert.True(t, status == proto.OpOk && err == nil)

	//roll back add dentry
	rbDentry1 := mockAddTxDentry(mp1)
	req3 := &proto.TxDentryApplyRequest{
		TxID: rbDentry1.txDentryInfo.TxID,
		Pid:  rbDentry1.txDentryInfo.ParentId,
		Name: rbDentry1.txDentryInfo.Name,
	}
	status, err = txRsc.rollbackDentry(req3)
	assert.True(t, status == proto.OpOk && err == nil)

	//roll back delete dentry
	rbDentry2 := mockDeleteTxDentry(mp1)
	req4 := &proto.TxDentryApplyRequest{
		TxID: rbDentry2.txDentryInfo.TxID,
		Pid:  rbDentry2.txDentryInfo.ParentId,
		Name: rbDentry2.txDentryInfo.Name,
	}
	status, err = txRsc.rollbackDentry(req4)
	assert.True(t, status == proto.OpOk && err == nil)
}

func TestTxRscCommit(t *testing.T) {
	initMps(t)
	//commit add inode
	rbInode1 := mockAddTxInode(mp1)
	txRsc := mp1.txProcessor.txResource
	status, err := txRsc.commitInode(rbInode1.txInodeInfo.TxID, rbInode1.inode.Inode)
	assert.True(t, status == proto.OpOk && err == nil)

	//commit delete inode
	rbInode2 := mockDeleteTxInode(mp1)
	status, err = txRsc.commitInode(rbInode2.txInodeInfo.TxID, rbInode2.inode.Inode)
	assert.True(t, status == proto.OpOk && err == nil)

	//commit add dentry
	rbDentry1 := mockAddTxDentry(mp1)
	status, err = txRsc.commitDentry(rbDentry1.txDentryInfo.TxID, rbDentry1.txDentryInfo.ParentId, rbDentry1.txDentryInfo.Name)
	assert.True(t, status == proto.OpOk && err == nil)

	//commit delete dentry
	rbDentry2 := mockDeleteTxDentry(mp1)
	status, err = txRsc.commitDentry(rbDentry2.txDentryInfo.TxID, rbDentry2.txDentryInfo.ParentId, rbDentry2.txDentryInfo.Name)
	assert.True(t, status == proto.OpOk && err == nil)
}

func TestTxTreeRollback(t *testing.T) {
	initMps(t)

	txInfo := proto.NewTransactionInfo(0, proto.TxTypeCreate)
	txDentryInfo := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txInfo.TxDentryInfos[txDentryInfo.GetKey()] = txDentryInfo
	if !txInfo.IsInitialized() {
		mp1.initTxInfo(txInfo)
	}
	txId := txInfo.TxID

	txMgr := mp1.txProcessor.txManager

	//register
	id := txMgr.txIdAlloc.getTransactionID()
	expectedId := fmt.Sprintf("%d_%d", mp1.config.PartitionId, id)
	assert.Equal(t, expectedId, txId)
	txMgr.registerTransaction(txInfo)

	txInfo.DoneTime = time.Now().Unix() - 70
	txInfo.State = proto.TxStateCommitDone
	go txMgr.processExpiredTransactions(nil)

	time.Sleep(2 * time.Second)
	assert.True(t, txMgr.txTree.Len() == 0)

	txMgr.registerTransaction(txInfo)
	txMgr.txProcessor.mask |= proto.TxPause
	time.Sleep(2 * time.Second)
	assert.True(t, txMgr.txTree.Len() == 1)
}

func TestMultiStartExpireCheck(t *testing.T) {
	initMps(t)

	txInfo := proto.NewTransactionInfo(0, proto.TxTypeCreate)
	txDentryInfo := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txInfo.TxDentryInfos[txDentryInfo.GetKey()] = txDentryInfo
	if !txInfo.IsInitialized() {
		mp1.initTxInfo(txInfo)
	}

	txMgr := mp1.txProcessor.txManager
	wg := txMgr.Start()
	assert.True(t, wg != nil)
	assert.True(t, nil == txMgr.Start())

	var exit int32
	go func() {
		wg.Wait()
		atomic.StoreInt32(&exit, 1)
	}()

	cFunc := func(wgx *sync.WaitGroup) {
		txMgr.stopProcess()
		i := 1
		for {
			time.Sleep(time.Millisecond * 50)
			if atomic.LoadInt32(&exit) == 1 {
				wgx.Done()
				break
			}
			i++
			if i > 100 {
				wgx.Done()
				break
			}
		}
	}
	//time.Sleep(time.Second*5)
	var wg1 sync.WaitGroup
	wg1.Add(1)
	cFunc(&wg1)

	wg1.Wait()
	assert.True(t, txMgr.started == false)
}
