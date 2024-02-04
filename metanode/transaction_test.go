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
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

var (
	mp1 *metaPartition
	mp2 *metaPartition
	mp3 *metaPartition
)

const FileModeType uint32 = 420

const (
	MemberAddrs         = "127.0.0.1:17210,127.0.0.2:17210,127.0.0.3:17210"
	inodeNum            = 1001
	pInodeNum           = 1002
	inodeNum2           = 1003
	dentryName          = "parent"
	RocksdbTransTestDir = "/tmp/cfs/tx_test"
	TransactionTestLog  = "/tmp/cfs/tx_logs"
)

func init() {
	log.InitLog(TransactionTestLog, "test", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
}

func newMetaPartition(PartitionId uint64, manager *metadataManager, storeMode proto.StoreMode) (mp *metaPartition) {
	metaConf := &MetaPartitionConfig{
		PartitionId:   PartitionId,
		VolName:       "testVol",
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	metaConf.RocksDBDir = fmt.Sprintf("%v/%v_%v", RocksdbTransTestDir, partitionId, time.Now().UnixMilli())

	if manager == nil {
		manager = &metadataManager{}
	}

	mp = &metaPartition{
		config:         metaConf,
		stopC:          make(chan bool),
		storeChan:      make(chan *storeMsg, 100),
		freeList:       newFreeList(),
		freeHybridList: newFreeList(),
		extDelCh:       make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		extReset:       make(chan struct{}),
		vol:            NewVol(),
		manager:        manager,
		rocksdbManager: NewPerDiskRocksdbManager(0, 0, 0, 0, 0),
	}
	err := mp.rocksdbManager.Register(metaConf.RocksDBDir)
	if err != nil {
		panic(err)
	}
	mp.config.Cursor = 1000
	mp.config.End = 100000

	err = mp.initObjects(true)
	if err != nil {
		panic(err)
	}
	mp.uidManager = NewUidMgr(mp.config.VolName, mp.config.PartitionId)
	mp.manager.initFileStatsConfig()
	return mp
}

func initMps(t *testing.T, storeMode proto.StoreMode) {
	test = true
	mp1 = newMetaPartition(10001, &metadataManager{}, storeMode)
	mp2 = newMetaPartition(10002, &metadataManager{}, storeMode)
	mp3 = newMetaPartition(10003, &metadataManager{}, storeMode)
}

func (i *Inode) Equal(inode *Inode) bool {
	return reflect.DeepEqual(i, inode)
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

func TestRollbackInodeLess(t *testing.T) {
	inode := NewInode(101, 0)
	txInodeInfo := proto.NewTxInodeInfo(MemberAddrs, inodeNum, 10001)
	rbInode := NewTxRollbackInode(inode, []uint32{}, txInodeInfo, TxAdd)

	rbInode2 := &TxRollbackInode{
		inode: NewInode(100, 0),
	}
	assert.False(t, rbInode.Less(rbInode2))

	rbInode2.txInodeInfo = proto.NewTxInodeInfo("", inodeNum+1, 0)
	assert.True(t, rbInode.Less(rbInode2))
}

func TestRollbackInodeSerialization(t *testing.T) {
	inode := &Inode{
		Inode:                       1024,
		Gid:                         11,
		Uid:                         10,
		Size:                        101,
		Type:                        0o755,
		Generation:                  13,
		CreateTime:                  102,
		AccessTime:                  104,
		ModifyTime:                  107,
		LinkTarget:                  []byte("link target"),
		NLink:                       7,
		Flag:                        1,
		Reserved:                    3,
		StorageClass:                proto.StorageClass_Replica_HDD,
		HybridCloudExtents:          NewSortedHybridCloudExtents(),
		HybridCloudExtentsMigration: NewSortedHybridCloudExtentsMigration(),
		//Extents: NewSortedExtentsFromEks([]proto.ExtentKey{
		//	{FileOffset: 11, PartitionId: 12, ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0},
		//}),
	}
	inode.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 12, ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0}})
	ids := []uint32{11, 13}

	txInodeInfo := proto.NewTxInodeInfo(MemberAddrs, inodeNum, 10001)
	rbInode := NewTxRollbackInode(inode, ids, txInodeInfo, TxAdd)
	var data []byte
	data, _ = rbInode.Marshal()

	txRbInode := NewTxRollbackInode(nil, []uint32{}, nil, 0)
	txRbInode.Unmarshal(data)
	assert.True(t, rbInode.Equal(txRbInode))

	inode.Inode = 1023
	assert.False(t, rbInode.Equal(txRbInode))

	cpRbInode := rbInode.Copy()
	assert.True(t, rbInode.Equal(cpRbInode.(*TxRollbackInode)))
}

func TestTxRollbackDentry_Less(t *testing.T) {
	rb1 := &TxRollbackDentry{
		txDentryInfo: &proto.TxDentryInfo{ParentId: 1001, Name: "tt"},
	}

	rb2 := &TxRollbackDentry{
		txDentryInfo: &proto.TxDentryInfo{ParentId: 1002, Name: "tt"},
	}

	assert.True(t, rb1.Less(rb2))

	rb3 := &TxRollbackDentry{
		txDentryInfo: &proto.TxDentryInfo{ParentId: 1001, Name: "ta"},
	}
	assert.False(t, rb1.Less(rb3))
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

	assert.True(t, reflect.DeepEqual(rbDentry.dentry, txRbDentry.dentry))
	assert.True(t, reflect.DeepEqual(rbDentry.txDentryInfo, txRbDentry.txDentryInfo))
	assert.True(t, reflect.DeepEqual(rbDentry, txRbDentry))

	txDentryInfo.MpMembers = "tttt"
	assert.False(t, reflect.DeepEqual(rbDentry, txRbDentry))

	cpDentryInfo := rbDentry.Copy()
	assert.True(t, reflect.DeepEqual(rbDentry, cpDentryInfo.(*TxRollbackDentry)))
}

func testNextTxID(t *testing.T) {
	txMgr := mp1.txProcessor.txManager

	var id uint64 = 2
	expectedId := fmt.Sprintf("%d_%d", mp1.config.PartitionId, id+1)
	txMgr.txIdAlloc.setTransactionID(id)
	txMgr.txTree.SetTxId(id)
	assert.Equal(t, expectedId, txMgr.nextTxID())
}

func TestNextTxID(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testNextTxID(t)
}

func TestNextTxID_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testNextTxID(t)
}

func testTxMgrOp(t *testing.T) {
	txInfo := proto.NewTransactionInfo(5, proto.TxTypeCreate)
	assert.True(t, txInfo.State == proto.TxStateInit)

	txDentryInfo := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txInfo.TxDentryInfos[txDentryInfo.GetKey()] = txDentryInfo
	if !txInfo.IsInitialized() {
		mp1.initTxInfo(txInfo)
	}

	assert.True(t, txInfo.State == proto.TxStatePreCommit)

	txId := txInfo.TxID
	txMgr := mp1.txProcessor.txManager

	// register
	id := txMgr.txIdAlloc.getTransactionID()
	expectedId := fmt.Sprintf("%d_%d", mp1.config.PartitionId, id)
	assert.Equal(t, expectedId, txId)
	txMgr.registerTransaction(txInfo)

	// get
	gotTxInfo := txMgr.getTransaction(txId)
	assert.Equal(t, txInfo, gotTxInfo)

	// rollback
	txMgr.rollbackTxInfo(txId)
	gotTxInfo = txMgr.getTransaction(txId)
	assert.True(t, gotTxInfo.IsDone())

	// commit
	status, _ := txMgr.commitTxInfo("dummy_txId")
	assert.Equal(t, proto.OpTxInfoNotExistErr, status)
}

func TestTxMgrOp(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testTxMgrOp(t)
}

func TestTxMgrOp_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testTxMgrOp(t)
}

func testTxRscOp(t *testing.T) {
	txMgr := mp1.txProcessor.txManager

	// rbInode
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
	assert.Equal(t, proto.OpExistErr, status)

	inTx, _, _ := txRsc.isInodeInTransction(inode1)
	assert.True(t, inTx)

	status = txRsc.addTxRollbackInode(rbInode2)
	assert.Equal(t, proto.OpTxConflictErr, status)

	// rbDentry
	txDentryInfo1 := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	dentry := &Dentry{
		ParentId: pInodeNum,
		Name:     dentryName,
		Inode:    inodeNum,
		Type:     FileModeType,
	}
	txDentryInfo1.TxID = txMgr.nextTxID()
	txDentryInfo1.Timeout = 5
	txDentryInfo1.CreateTime = time.Now().Unix()
	rbDentry1 := NewTxRollbackDentry(dentry, txDentryInfo1, TxAdd)

	txDentryInfo2 := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txDentryInfo2.TxID = txMgr.nextTxID()
	txDentryInfo2.Timeout = 5
	txDentryInfo2.CreateTime = time.Now().Unix()
	rbDentry2 := NewTxRollbackDentry(dentry, txDentryInfo2, TxAdd)

	status = txRsc.addTxRollbackDentry(rbDentry1)
	assert.Equal(t, proto.OpOk, status)
	status = txRsc.addTxRollbackDentry(rbDentry1)
	assert.Equal(t, proto.OpExistErr, status)

	inTx, _, _ = txRsc.isDentryInTransction(dentry)
	assert.True(t, inTx)

	status = txRsc.addTxRollbackDentry(rbDentry2)
	assert.Equal(t, proto.OpTxConflictErr, status)
}

func TestTxRscOp(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testTxRscOp(t)
}

func TestTxRscOp_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testTxRscOp(t)
}

func mockAddTxInode(mp *metaPartition, t *testing.T) *TxRollbackInode {
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
	txDentryInfo1.CreateTime = time.Now().Unix()
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
	txDentryInfo2.CreateTime = time.Now().Unix()
	rbDentry := NewTxRollbackDentry(dentry2, txDentryInfo2, TxAdd)
	txRsc := mp.txProcessor.txResource
	txRsc.addTxRollbackDentry(rbDentry)

	mp.dentryTree.Delete(dentry2)
	return rbDentry
}

func testTxRscRollback(t *testing.T) {
	// roll back add inode
	rbInode1 := mockAddTxInode(mp1, t)
	txRsc := mp1.txProcessor.txResource

	err := txRsc.txProcessor.mp.inodeTree.Put(NewInode(pInodeNum, DirModeType))
	require.NoError(t, err)
	req1 := &proto.TxInodeApplyRequest{
		TxID:  rbInode1.txInodeInfo.TxID,
		Inode: rbInode1.inode.Inode,
	}
	status, err := txRsc.rollbackInode(req1)
	assert.True(t, status == proto.OpOk && err == nil)

	// roll back delete inode
	rbInode2 := mockDeleteTxInode(mp1)
	req2 := &proto.TxInodeApplyRequest{
		TxID:  rbInode2.txInodeInfo.TxID,
		Inode: rbInode2.inode.Inode,
	}
	status, err = txRsc.rollbackInode(req2)
	assert.True(t, status == proto.OpOk && err == nil)

	// roll back add dentry
	rbDentry1 := mockAddTxDentry(mp1)
	req3 := &proto.TxDentryApplyRequest{
		TxID: rbDentry1.txDentryInfo.TxID,
		Pid:  rbDentry1.txDentryInfo.ParentId,
		Name: rbDentry1.txDentryInfo.Name,
	}
	status, err = txRsc.rollbackDentry(req3)
	assert.True(t, status == proto.OpOk && err == nil)

	// roll back delete dentry
	rbDentry2 := mockDeleteTxDentry(mp1)
	req4 := &proto.TxDentryApplyRequest{
		TxID: rbDentry2.txDentryInfo.TxID,
		Pid:  rbDentry2.txDentryInfo.ParentId,
		Name: rbDentry2.txDentryInfo.Name,
	}
	status, err = txRsc.rollbackDentry(req4)
	assert.True(t, status == proto.OpOk && err == nil)
}

func TestTxRscRollback(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testTxRscRollback(t)
}

func TestTxRscRollback_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testTxRscRollback(t)
}

func testTxRscCommit(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	// commit add inode
	rbInode1 := mockAddTxInode(mp1, t)
	txRsc := mp1.txProcessor.txResource
	status, err := txRsc.commitInode(rbInode1.txInodeInfo.TxID, rbInode1.inode.Inode)
	assert.True(t, status == proto.OpOk && err == nil)

	// commit delete inode
	rbInode2 := mockDeleteTxInode(mp1)
	status, err = txRsc.commitInode(rbInode2.txInodeInfo.TxID, rbInode2.inode.Inode)
	assert.True(t, status == proto.OpOk && err == nil)

	// commit add dentry
	rbDentry1 := mockAddTxDentry(mp1)
	status, err = txRsc.commitDentry(rbDentry1.txDentryInfo.TxID, rbDentry1.txDentryInfo.ParentId, rbDentry1.txDentryInfo.Name)
	assert.True(t, status == proto.OpOk && err == nil)

	// commit delete dentry
	rbDentry2 := mockDeleteTxDentry(mp1)
	status, err = txRsc.commitDentry(rbDentry2.txDentryInfo.TxID, rbDentry2.txDentryInfo.ParentId, rbDentry2.txDentryInfo.Name)
	assert.True(t, status == proto.OpOk && err == nil)
}

func TestTxRscCommit(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testTxRscCommit(t)
}

func TestTxRscCommit_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testTxRscCommit(t)
}

func testTxTreeRollback(t *testing.T) {
	txInfo := proto.NewTransactionInfo(0, proto.TxTypeCreate)
	txDentryInfo := proto.NewTxDentryInfo(MemberAddrs, pInodeNum+1, dentryName, 10001)
	txInfo.TxDentryInfos[txDentryInfo.GetKey()] = txDentryInfo
	if !txInfo.IsInitialized() {
		mp1.initTxInfo(txInfo)
	}

	txId := txInfo.TxID
	txInfo.TmID = int64(mp1.config.PartitionId)
	txMgr := mp1.txProcessor.txManager

	// register
	id := txMgr.txIdAlloc.getTransactionID()
	expectedId := fmt.Sprintf("%d_%d", mp1.config.PartitionId, id)
	assert.Equal(t, expectedId, txId)
	txMgr.registerTransaction(txInfo)

	txMgr.registerTransaction(txInfo)
	txMgr.txProcessor.mask |= proto.TxPause
	time.Sleep(2 * time.Second)
	assert.True(t, txMgr.txTree.Len() == 1)
}

func TestTxTreeRollback(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testTxTreeRollback(t)
}

func TestTxTreeRollback_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testTxTreeRollback(t)
}

func testCheckTxLimit(t *testing.T) {
	txMgr := mp1.txProcessor.txManager
	// txMgr.Start()
	txMgr.setLimit(10)
	txMgr.opLimiter.SetBurst(1)
	txInfo := proto.NewTransactionInfo(0, proto.TxTypeCreate)
	txDentryInfo := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txInfo.TxDentryInfos[txDentryInfo.GetKey()] = txDentryInfo
	err := mp1.initTxInfo(txInfo)
	assert.NoError(t, err)

	err = mp1.initTxInfo(txInfo)
	assert.Error(t, err)
}

func TestCheckTxLimit(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testCheckTxLimit(t)
}

func TestCheckTxLimit_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testCheckTxLimit(t)
}

func testGetTxHandler(t *testing.T) {
	txMgr := mp1.txProcessor.txManager
	// txMgr.Start()

	txInfo := proto.NewTransactionInfo(0, proto.TxTypeCreate)
	txDentryInfo := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txInfo.TxDentryInfos[txDentryInfo.GetKey()] = txDentryInfo
	if !txInfo.IsInitialized() {
		mp1.initTxInfo(txInfo)
	}

	// register
	txMgr.registerTransaction(txInfo)
	var (
		req = &proto.TxGetInfoRequest{
			TxID: txInfo.TxID,
			Pid:  mp1.config.PartitionId,
		}
		p = new(Packet)
	)

	assert.True(t, mp1.TxGetInfo(req, p) == nil)
	assert.True(t, p.ResultCode == proto.OpOk)
}

func TestGetTxHandler(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testGetTxHandler(t)
}

func TestGetTxHandler_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testGetTxHandler(t)
}

func TestCleanTransactionTestDir(t *testing.T) {
	os.RemoveAll(RocksdbTransTestDir)
	os.RemoveAll(TransactionTestLog)
}
