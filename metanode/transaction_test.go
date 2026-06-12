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

	raftstoremock "github.com/cubefs/cubefs/metanode/mocktest/raftstore"
	"github.com/golang/mock/gomock"
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
		rocksdbManager: NewPerDiskRocksdbManager(&RocksdbManagerConfig{}),
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

// initMpsForProcessTx is like initMps but attaches a local raft mock on mp1 so async
// rollback/commit/delete paths in processTx can call submit without panicking.
func initMpsForProcessTx(t *testing.T, storeMode proto.StoreMode) {
	t.Helper()
	initMps(t, storeMode)
	attachMockRaftApply(t, mp1)
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
	inode.PoolId = proto.DefaultSSDPoolId
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
		PoolId:                      proto.DefaultHDDPoolId,
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
	handle, err := txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	id := txMgr.txIdAlloc.getTransactionID()
	expectedId := fmt.Sprintf("%d_%d", mp1.config.PartitionId, id)
	assert.Equal(t, expectedId, txId)
	err = txMgr.registerTransaction(handle, txInfo)
	require.NoError(t, err)
	err = txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	// get
	gotTxInfo, err := txMgr.getTransaction(txId)
	require.NoError(t, err)
	assert.Equal(t, txInfo, gotTxInfo)

	// rollback
	handle, err = txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = txMgr.rollbackTxInfo(handle, txId)
	require.NoError(t, err)
	err = txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	gotTxInfo, err = txMgr.getTransaction(txId)
	require.NoError(t, err)
	assert.True(t, gotTxInfo.IsDone())

	// commit
	handle, err = txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txMgr.commitTxInfo(handle, txId)
	require.NoError(t, err)
	err = txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.Equal(t, proto.OpOk, status)
}

func TestTxMgrOp(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	testTxMgrOp(t)
}

func TestTxMgrOp_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	testTxMgrOp(t)
}

// commitTxInfo/rollbackTxInfo use copyGetTx; mutating a stale getTransaction pointer must not
// change the outcome of commit/rollback on the tree copy.
func TestTxMgrCommitRollbackUseCopyGetTx(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	txMgr := mp1.txProcessor.txManager

	txInfo := proto.NewTransactionInfo(5, proto.TxTypeCreate)
	txInfo.TxID = txMgr.nextTxID()
	txInfo.State = proto.TxStatePreCommit
	txInfo.CreateTime = time.Now().Unix()
	if !txInfo.IsInitialized() {
		mp1.initTxInfo(txInfo)
	}

	handle, err := txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = txMgr.registerTransaction(handle, txInfo)
	require.NoError(t, err)
	require.NoError(t, txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false))

	stale, err := txMgr.getTransaction(txInfo.TxID)
	require.NoError(t, err)
	require.NotNil(t, stale)
	stale.State = proto.TxStateInit

	handle, err = txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txMgr.commitTxInfo(handle, txInfo.TxID)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	require.NoError(t, txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false))

	committed, err := txMgr.getTransaction(txInfo.TxID)
	require.NoError(t, err)
	require.NotNil(t, committed)
	require.True(t, committed.IsDone())
	require.EqualValues(t, proto.TxStateCommitDone, committed.State)

	txInfo2 := proto.NewTransactionInfo(5, proto.TxTypeCreate)
	txInfo2.TxID = txMgr.nextTxID()
	txInfo2.State = proto.TxStatePreCommit
	txInfo2.CreateTime = time.Now().Unix()
	if !txInfo2.IsInitialized() {
		mp1.initTxInfo(txInfo2)
	}

	handle, err = txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = txMgr.registerTransaction(handle, txInfo2)
	require.NoError(t, err)
	require.NoError(t, txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false))

	stale2, err := txMgr.getTransaction(txInfo2.TxID)
	require.NoError(t, err)
	stale2.State = proto.TxStateCommitDone

	handle, err = txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = txMgr.rollbackTxInfo(handle, txInfo2.TxID)
	require.NoError(t, err)
	require.NoError(t, txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false))

	rolled, err := txMgr.getTransaction(txInfo2.TxID)
	require.NoError(t, err)
	require.NotNil(t, rolled)
	require.EqualValues(t, proto.TxStateRollbackDone, rolled.State)
}

func TestRollbackInodeInternalIncNLinkPersists(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	const ino = 22001

	handle, err := mp1.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	inode := NewInode(ino, FileModeType)
	initTestInodeStorage(inode)
	inode.NLink = 1
	_, _, err = mp1.inodeTree.ReplaceOrInsert(handle, inode, true)
	require.NoError(t, err)
	require.NoError(t, mp1.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false))

	txMgr := mp1.txProcessor.txManager
	txInodeInfo := proto.NewTxInodeInfo(MemberAddrs, ino, 10001)
	txInodeInfo.TxID = txMgr.nextTxID()
	rbInode := NewTxRollbackInode(inode, nil, txInodeInfo, TxAdd)

	txRsc := mp1.txProcessor.txResource
	handle, err = txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.addTxRollbackInode(handle, rbInode)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	require.NoError(t, txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false))

	handle, err = txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.rollbackInodeInternal(handle, rbInode)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
	require.NoError(t, txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false))

	got, err := mp1.inodeTree.Get(NewInode(ino, 0))
	require.NoError(t, err)
	require.NotNil(t, got)
	require.EqualValues(t, 2, got.NLink)
}

func testTxRscOp(t *testing.T) {
	txMgr := mp1.txProcessor.txManager

	// rbInode
	txInodeInfo1 := proto.NewTxInodeInfo(MemberAddrs, inodeNum, 10001)
	txInodeInfo1.TxID = txMgr.nextTxID()
	txInodeInfo1.Timeout = 5
	txInodeInfo1.CreateTime = time.Now().UnixNano()
	inode1 := NewInode(inodeNum, FileModeType)
	inode1.PoolId = proto.DefaultSSDPoolId
	rbInode1 := NewTxRollbackInode(inode1, []uint32{}, txInodeInfo1, TxAdd)

	txInodeInfo2 := proto.NewTxInodeInfo(MemberAddrs, inodeNum, 10001)
	txInodeInfo2.TxID = txMgr.nextTxID()
	txInodeInfo2.Timeout = 5
	txInodeInfo2.CreateTime = time.Now().UnixNano()
	rbInode2 := NewTxRollbackInode(inode1, []uint32{}, txInodeInfo2, TxAdd)

	txRsc := mp1.txProcessor.txResource
	handle, err := txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.addTxRollbackInode(handle, rbInode1)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.Equal(t, proto.OpOk, status)
	handle, err = txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.addTxRollbackInode(handle, rbInode1)
	require.NoError(t, err)
	assert.Equal(t, proto.OpExistErr, status)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	inTx, _, err := txRsc.isInodeInTransction(inode1)
	require.NoError(t, err)
	assert.True(t, inTx)

	handle, err = txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.addTxRollbackInode(handle, rbInode2)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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

	handle, err = txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.addTxRollbackDentry(handle, rbDentry1)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.Equal(t, proto.OpOk, status)
	handle, err = txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.addTxRollbackDentry(handle, rbDentry1)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.Equal(t, proto.OpExistErr, status)

	inTx, _, err = txRsc.isDentryInTransction(dentry)
	require.NoError(t, err)
	assert.True(t, inTx)

	handle, err = txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.addTxRollbackDentry(handle, rbDentry2)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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
	initTestInodeStorage(inode1)
	rbInode := NewTxRollbackInode(inode1, []uint32{}, txInodeInfo1, TxDelete)
	txRsc := mp.txProcessor.txResource

	handle, err := txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = txRsc.addTxRollbackInode(handle, rbInode)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = mp.inodeTree.Put(handle, inode1)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	return rbInode
}

func mockDeleteTxInode(mp *metaPartition, t *testing.T) *TxRollbackInode {
	inode2 := NewInode(inodeNum2, FileModeType)
	initTestInodeStorage(inode2)
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = mp.inodeTree.Put(handle, inode2)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	txMgr := mp.txProcessor.txManager
	txInodeInfo2 := proto.NewTxInodeInfo(MemberAddrs, inodeNum2, 10001)
	txInodeInfo2.TxID = txMgr.nextTxID()
	txInodeInfo2.Timeout = 5
	txInodeInfo2.CreateTime = time.Now().UnixNano()
	rbInode := NewTxRollbackInode(inode2, []uint32{}, txInodeInfo2, TxAdd)
	txRsc := mp.txProcessor.txResource
	handle, err = txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = txRsc.addTxRollbackInode(handle, rbInode)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = mp.inodeTree.Delete(handle, inode2)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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

func mockAddTxDentry(mp *metaPartition, t *testing.T) *TxRollbackDentry {
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
	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = txRsc.addTxRollbackDentry(handle, rbDentry)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = mp.dentryTree.Put(handle, dentry1)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	return rbDentry
}

func mockDeleteTxDentry(mp *metaPartition, t *testing.T) *TxRollbackDentry {
	handle, err := mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)

	dentry2 := &Dentry{
		ParentId: pInodeNum,
		Name:     dentryName,
		Inode:    1001,
		Type:     0,
	}
	err = mp.dentryTree.Put(handle, dentry2)
	require.NoError(t, err)
	err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	txMgr := mp.txProcessor.txManager
	txDentryInfo2 := proto.NewTxDentryInfo(MemberAddrs, pInodeNum, dentryName, 10001)
	txDentryInfo2.TxID = txMgr.nextTxID()
	txDentryInfo2.Timeout = 5
	txDentryInfo2.CreateTime = time.Now().Unix()
	rbDentry := NewTxRollbackDentry(dentry2, txDentryInfo2, TxAdd)
	txRsc := mp.txProcessor.txResource
	handle, err = txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = txRsc.addTxRollbackDentry(handle, rbDentry)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	handle, err = mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = mp.dentryTree.Delete(handle, dentry2)
	require.NoError(t, err)
	err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	return rbDentry
}

func testTxRscRollback(t *testing.T) {
	// roll back add inode
	rbInode1 := mockAddTxInode(mp1, t)
	txRsc := mp1.txProcessor.txResource

	// NOTE: add dentry parent inode
	handle, err := txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	tmpIno := NewInode(pInodeNum, DirModeType)
	initTestInodeStorage(tmpIno)
	err = txRsc.txProcessor.mp.inodeTree.Put(handle, tmpIno)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	req1 := &proto.TxInodeApplyRequest{
		TxID:  rbInode1.txInodeInfo.TxID,
		Inode: rbInode1.inode.Inode,
	}
	handle, err = txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.rollbackInode(handle, req1)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.True(t, status == proto.OpOk && err == nil)

	// roll back delete inode
	rbInode2 := mockDeleteTxInode(mp1, t)
	req2 := &proto.TxInodeApplyRequest{
		TxID:  rbInode2.txInodeInfo.TxID,
		Inode: rbInode2.inode.Inode,
	}
	handle, err = txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.rollbackInode(handle, req2)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.True(t, status == proto.OpOk && err == nil)

	// roll back add dentry
	rbDentry1 := mockAddTxDentry(mp1, t)
	req3 := &proto.TxDentryApplyRequest{
		TxID: rbDentry1.txDentryInfo.TxID,
		Pid:  rbDentry1.txDentryInfo.ParentId,
		Name: rbDentry1.txDentryInfo.Name,
	}
	handle, err = txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.rollbackDentry(handle, req3)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.True(t, status == proto.OpOk && err == nil)

	// roll back delete dentry
	rbDentry2 := mockDeleteTxDentry(mp1, t)
	req4 := &proto.TxDentryApplyRequest{
		TxID: rbDentry2.txDentryInfo.TxID,
		Pid:  rbDentry2.txDentryInfo.ParentId,
		Name: rbDentry2.txDentryInfo.Name,
	}
	handle, err = txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.rollbackDentry(handle, req4)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	require.EqualValues(t, proto.OpOk, status)
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
	handle, err := txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.commitInode(handle, rbInode1.txInodeInfo.TxID, rbInode1.inode.Inode)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.True(t, status == proto.OpOk && err == nil)

	// commit delete inode
	rbInode2 := mockDeleteTxInode(mp1, t)
	handle, err = txRsc.txRbInodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.commitInode(handle, rbInode2.txInodeInfo.TxID, rbInode2.inode.Inode)
	require.NoError(t, err)
	err = txRsc.txRbInodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	assert.True(t, status == proto.OpOk && err == nil)

	// commit add dentry
	rbDentry1 := mockAddTxDentry(mp1, t)
	handle, err = txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.commitDentry(handle, rbDentry1.txDentryInfo.TxID, rbDentry1.txDentryInfo.ParentId, rbDentry1.txDentryInfo.Name)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	assert.True(t, status == proto.OpOk && err == nil)

	// commit delete dentry (TxAdd): commitDentry decrements parent nlink, parent inode must exist
	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 3
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp1, parent)

	rbDentry2 := mockDeleteTxDentry(mp1, t)
	handle, err = txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err = txRsc.commitDentry(handle, rbDentry2.txDentryInfo.TxID, rbDentry2.txDentryInfo.ParentId, rbDentry2.txDentryInfo.Name)
	require.NoError(t, err)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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

func putInodeForTxTest(t *testing.T, mp *metaPartition, ino *Inode) {
	t.Helper()
	initTestInodeStorage(ino)
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, _, err = mp.inodeTree.ReplaceOrInsert(handle, ino, true)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

// commitDentry(TxAdd) should only DecNLink the parent directory, not unlink/evict it from inode tree.
func TestCommitDentryTxAddDecNLinkKeepsParentInode(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	mp := mp1

	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 3
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp, parent)

	rbDentry := mockDeleteTxDentry(mp, t)
	txRsc := mp.txProcessor.txResource

	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.NoError(t, err)
	require.Equal(t, proto.OpOk, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	parentAfter, err := mp.inodeTree.Get(NewInode(pInodeNum, 0))
	require.NoError(t, err)
	require.NotNil(t, parentAfter, "parent directory inode must not be removed on commitDentry")
	assert.Equal(t, uint32(2), parentAfter.NLink)

	rbAfter, err := txRsc.getTxRbDentry(pInodeNum, dentryName)
	require.NoError(t, err)
	assert.Nil(t, rbAfter)
}

// commitDentry for TxDelete rb (tx create dentry) must not change parent nlink.
func TestCommitDentryTxDeleteSkipsParentNLink(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	mp := mp1

	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 5
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp, parent)

	rbDentry := mockAddTxDentry(mp, t)
	require.Equal(t, TxDelete, rbDentry.rbType)

	txRsc := mp.txProcessor.txResource
	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.NoError(t, err)
	require.Equal(t, proto.OpOk, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	parentAfter, err := mp.inodeTree.Get(NewInode(pInodeNum, 0))
	require.NoError(t, err)
	require.NotNil(t, parentAfter)
	assert.Equal(t, uint32(5), parentAfter.NLink)
}

func TestCommitDentryParentShouldDeleteReturnsNotExist(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	mp := mp1

	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 3
	parent.Flag |= DeleteMarkFlag
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp, parent)

	rbDentry := mockDeleteTxDentry(mp, t)
	txRsc := mp.txProcessor.txResource

	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.NoError(t, err)
	require.Equal(t, proto.OpNotExistErr, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	parentAfter, err := mp.inodeTree.Get(NewInode(pInodeNum, 0))
	require.NoError(t, err)
	require.NotNil(t, parentAfter)
	assert.Equal(t, uint32(3), parentAfter.NLink)

	rbAfter, err := txRsc.getTxRbDentry(pInodeNum, dentryName)
	require.NoError(t, err)
	assert.Nil(t, rbAfter)
}

func TestCommitDentryRbNotExist(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	txRsc := mp1.txProcessor.txResource
	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.commitDentry(handle, "missing-tx", pInodeNum, dentryName)
	require.Error(t, err)
	assert.Equal(t, proto.OpTxRbDentryNotExistErr, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func TestCommitDentryParentInodeNotExist(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	mp := mp1

	rbDentry := mockDeleteTxDentry(mp, t)
	txRsc := mp.txProcessor.txResource

	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, _ := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.Equal(t, proto.OpNotExistErr, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func TestCommitDentryParentNLinkLow(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	mp := mp1

	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 2
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp, parent)

	rbDentry := mockDeleteTxDentry(mp, t)
	txRsc := mp.txProcessor.txResource

	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.NoError(t, err)
	require.Equal(t, proto.OpNotExistErr, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	parentAfter, err := mp.inodeTree.Get(NewInode(pInodeNum, 0))
	require.NoError(t, err)
	require.NotNil(t, parentAfter)
	assert.Equal(t, uint32(2), parentAfter.NLink)
}

func TestCommitDentryParentNLinkAboveTwoDecNLink(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	mp := mp1

	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 4
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp, parent)

	rbDentry := mockDeleteTxDentry(mp, t)
	txRsc := mp.txProcessor.txResource

	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.NoError(t, err)
	require.Equal(t, proto.OpOk, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	parentAfter, err := mp.inodeTree.Get(NewInode(pInodeNum, 0))
	require.NoError(t, err)
	require.NotNil(t, parentAfter)
	assert.Equal(t, uint32(3), parentAfter.NLink)
}

func TestCommitDentryParentNLinkLow_Rocksdb(t *testing.T) {
	initMps(t, proto.StoreModeRocksDb)
	mp := mp1

	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 2
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp, parent)

	rbDentry := mockDeleteTxDentry(mp, t)
	txRsc := mp.txProcessor.txResource

	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.NoError(t, err)
	require.Equal(t, proto.OpNotExistErr, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func TestCommitDentryGetParentInodeError(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	mp := mp1

	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 4
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp, parent)

	rbDentry := mockDeleteTxDentry(mp, t)
	txRsc := mp.txProcessor.txResource

	base := mp.inodeTree
	mp.inodeTree = &errInjectInodeTree{
		InodeTree:  base,
		copyGetErr: fmt.Errorf("inode get failed"),
	}
	defer func() { mp.inodeTree = base }()

	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, _ := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.Equal(t, proto.OpErr, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func TestCommitDentryUpdateParentInodeError(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	mp := mp1

	parent := NewInode(pInodeNum, DirModeType)
	parent.NLink = 4
	parent.PoolId = proto.DefaultSSDPoolId
	putInodeForTxTest(t, mp, parent)

	rbDentry := mockDeleteTxDentry(mp, t)
	txRsc := mp.txProcessor.txResource

	base := mp.inodeTree
	mp.inodeTree = &errInjectInodeTree{
		InodeTree: base,
		updateErr: fmt.Errorf("inode update failed"),
	}
	defer func() { mp.inodeTree = base }()

	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, _ := txRsc.commitDentry(handle, rbDentry.txDentryInfo.TxID, rbDentry.txDentryInfo.ParentId, rbDentry.txDentryInfo.Name)
	require.Equal(t, proto.OpErr, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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
	handle, err := txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	id := txMgr.txIdAlloc.getTransactionID()
	expectedId := fmt.Sprintf("%d_%d", mp1.config.PartitionId, id)
	assert.Equal(t, expectedId, txId)
	err = txMgr.registerTransaction(handle, txInfo)
	require.NoError(t, err)
	err = txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	handle, err = txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = txMgr.registerTransaction(handle, txInfo)
	require.Error(t, err)
	err = txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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
	handle, err := txMgr.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = txMgr.registerTransaction(handle, txInfo)
	require.NoError(t, err)
	err = txMgr.txTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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

// collectTxIDsViaProcessTxMemPath mirrors processTx mem-mode traversal (GetTree + Range).
func collectTxIDsViaProcessTxMemPath(t *testing.T, tm *TransactionManager) []string {
	t.Helper()
	txBT, ok := tm.txTree.(*TransactionBTree)
	require.True(t, ok)
	cloned := &TransactionBTree{txBT.GetTree()}
	var ids []string
	err := cloned.Range(nil, nil, func(tx *proto.TransactionInfo) bool {
		ids = append(ids, tx.TxID)
		return true
	})
	require.NoError(t, err)
	return ids
}

func registerTxForProcessTest(t *testing.T, tm *TransactionManager, tx *proto.TransactionInfo) {
	t.Helper()
	handle, err := tm.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = tm.registerTransaction(handle, tx)
	require.NoError(t, err)
	err = tm.txTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func deleteTxForProcessTest(t *testing.T, tm *TransactionManager, txID string) {
	t.Helper()
	handle, err := tm.txTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	_, err = tm.txTree.Delete(handle, txID)
	require.NoError(t, err)
	err = tm.txTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func collectTxIDsViaSnapshotForProcessTest(t *testing.T, mp *metaPartition) []string {
	t.Helper()
	var ids []string
	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
	defer snap.Close()
	err = snap.Range(TransactionType, func(item interface{}) bool {
		ids = append(ids, item.(*proto.TransactionInfo).TxID)
		return true
	})
	require.NoError(t, err)
	return ids
}

func TestProcessTxMemRangeMatchesSnapshot(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	tm := mp1.txProcessor.txManager

	tx1 := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx1.TxID = tm.nextTxID()
	tx1.State = proto.TxStatePreCommit
	tx1.TmID = int64(mp1.config.PartitionId)
	tx1.CreateTime = time.Now().Unix()
	require.NoError(t, mp1.initTxInfo(tx1))
	registerTxForProcessTest(t, tm, tx1)

	tx2 := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx2.TxID = tm.nextTxID()
	tx2.State = proto.TxStateCommit
	tx2.TmID = int64(mp1.config.PartitionId)
	tx2.CreateTime = time.Now().Unix()
	require.NoError(t, mp1.initTxInfo(tx2))
	registerTxForProcessTest(t, tm, tx2)

	memIDs := collectTxIDsViaProcessTxMemPath(t, tm)
	snapIDs := collectTxIDsViaSnapshotForProcessTest(t, mp1)
	assert.ElementsMatch(t, []string{tx1.TxID, tx2.TxID}, memIDs)
	assert.Equal(t, memIDs, snapIDs)
}

func TestProcessTxMemCloneKeepsTxDuringScanAfterLiveDelete(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx.TxID = tm.nextTxID()
	tx.State = proto.TxStatePreCommit
	tx.TmID = int64(mp1.config.PartitionId)
	tx.CreateTime = time.Now().Unix()
	require.NoError(t, mp1.initTxInfo(tx))
	registerTxForProcessTest(t, tm, tx)

	var seen bool
	txBT := tm.txTree.(*TransactionBTree)
	cloned := &TransactionBTree{txBT.GetTree()}
	err := cloned.Range(nil, nil, func(item *proto.TransactionInfo) bool {
		if item.TxID != tx.TxID {
			return true
		}
		seen = true
		live, err := tm.getTransaction(tx.TxID)
		require.NoError(t, err)
		require.NotNil(t, live)

		deleteTxForProcessTest(t, tm, tx.TxID)
		live, err = tm.getTransaction(tx.TxID)
		require.NoError(t, err)
		require.Nil(t, live)
		assert.Equal(t, tx.TxID, item.TxID)
		return true
	})
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestProcessTxOnlyNonExpiredPreCommitCompletes(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx.TxID = tm.nextTxID()
	tx.State = proto.TxStatePreCommit
	tx.TmID = int64(mp1.config.PartitionId)
	tx.CreateTime = time.Now().Unix()
	require.NoError(t, mp1.initTxInfo(tx))
	registerTxForProcessTest(t, tm, tx)

	done := make(chan struct{})
	go func() {
		tm.processTx()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processTx did not finish for non-expired precommit tx")
	}
}

func TestProcessTxExpiredPreCommitCompletes(t *testing.T) {
	initMpsForProcessTx(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(1, proto.TxTypeCreate)
	tx.TxID = tm.nextTxID()
	tx.State = proto.TxStatePreCommit
	tx.TmID = int64(mp1.config.PartitionId)
	tx.CreateTime = time.Now().Unix() - 120
	require.NoError(t, mp1.initTxInfo(tx))
	registerTxForProcessTest(t, tm, tx)

	done := make(chan struct{})
	go func() {
		tm.processTx()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processTx did not finish for expired precommit tx")
	}
}

func TestProcessTxCommitStateInvokesCommit(t *testing.T) {
	initMpsForProcessTx(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx.TxID = tm.nextTxID()
	tx.State = proto.TxStateCommit
	tx.TmID = int64(mp1.config.PartitionId)
	tx.CreateTime = time.Now().Unix()
	require.NoError(t, mp1.initTxInfo(tx))
	registerTxForProcessTest(t, tm, tx)

	tm.processTx()
}

func TestProcessTxRollbackStateInvokesRollback(t *testing.T) {
	initMpsForProcessTx(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx.TxID = tm.nextTxID()
	tx.State = proto.TxStateRollback
	tx.TmID = int64(mp1.config.PartitionId)
	tx.CreateTime = time.Now().Unix()
	require.NoError(t, mp1.initTxInfo(tx))
	registerTxForProcessTest(t, tm, tx)

	tm.processTx()
}

func TestProcessTxForeignCommitDoesNotBlock(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx.TxID = "10002_foreign"
	tx.State = proto.TxStateCommit
	tx.TmID = int64(mp2.config.PartitionId)
	tx.CreateTime = time.Now().Unix()
	require.NoError(t, mp1.initTxInfo(tx))
	registerTxForProcessTest(t, tm, tx)

	done := make(chan struct{})
	go func() {
		tm.processTx()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processTx blocked on foreign commit tx")
	}
}

// attachMockRaftApply wires a raft mock that applies FSM ops locally (for delTxFromRM / submit paths).
func attachMockRaftApply(t *testing.T, mp *metaPartition) {
	t.Helper()
	ctrl := gomock.NewController(t)
	if mp.config.NodeId == 0 {
		mp.config.NodeId = 1
	}
	if len(mp.config.Peers) == 0 {
		mp.config.Peers = []proto.Peer{{ID: mp.config.NodeId, Addr: "127.0.0.1:1"}}
	}
	raft := raftstoremock.NewMockPartition(ctrl)
	var idx uint64
	raft.EXPECT().Submit(gomock.Any()).DoAndReturn(func(cmd []byte) (interface{}, error) {
		idx++
		return mp.Apply(cmd, idx)
	}).AnyTimes()
	raft.EXPECT().IsRaftLeader().Return(true).AnyTimes()
	raft.EXPECT().LeaderTerm().Return(mp.config.NodeId, uint64(1)).AnyTimes()
	mp.raftPartition = raft
	t.Cleanup(ctrl.Finish)
}

func TestProcessTxDeletableLocalTxRemoved(t *testing.T) {
	initMpsForProcessTx(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx.TmID = int64(mp1.config.PartitionId)
	require.NoError(t, mp1.initTxInfo(tx))
	tx.State = proto.TxStateCommitDone
	tx.RMFinish = true
	tx.DoneTime = time.Now().Unix() - int64(proto.DefaultTxDeleteTime+10)
	registerTxForProcessTest(t, tm, tx)

	tm.processTx()

	got, err := tm.getTransaction(tx.TxID)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestProcessTxDeletableForeignTxRemoved(t *testing.T) {
	initMpsForProcessTx(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx.TmID = int64(mp2.config.PartitionId)
	require.NoError(t, mp1.initTxInfo(tx))
	tx.State = proto.TxStateRollbackDone
	tx.RMFinish = true
	tx.DoneTime = time.Now().Unix() - int64(proto.DefaultTxDeleteTime+10)
	registerTxForProcessTest(t, tm, tx)

	tm.processTx()

	got, err := tm.getTransaction(tx.TxID)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestProcessTxMemTraverseStopsWhenNotLeaderAtCheckpoint(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	for i := 0; i < 104; i++ {
		tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
		tx.TmID = int64(mp2.config.PartitionId)
		require.NoError(t, mp1.initTxInfo(tx))
		registerTxForProcessTest(t, tm, tx)
	}

	tail := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tail.TmID = int64(mp2.config.PartitionId)
	require.NoError(t, mp1.initTxInfo(tail))
	registerTxForProcessTest(t, tm, tail)
	tailID := tail.TxID

	tm.processTx()

	got, err := tm.getTransaction(tailID)
	require.NoError(t, err)
	require.NotNil(t, got, "traverse should stop before tail tx when IsLeader fails at idx 100")
}

func TestProcessTxForeignOngoingPreCommitDoesNotBlock(t *testing.T) {
	initMps(t, proto.StoreModeMem)
	test = true
	tm := mp1.txProcessor.txManager

	tx := proto.NewTransactionInfo(60, proto.TxTypeCreate)
	tx.TxID = "10002_foreign_ongoing"
	tx.State = proto.TxStatePreCommit
	tx.TmID = int64(mp2.config.PartitionId)
	tx.CreateTime = time.Now().Unix()
	require.NoError(t, mp1.initTxInfo(tx))
	registerTxForProcessTest(t, tm, tx)

	done := make(chan struct{})
	go func() {
		tm.processTx()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processTx blocked on foreign ongoing precommit tx")
	}
}

func TestCleanTransactionTestDir(t *testing.T) {
	os.RemoveAll(RocksdbTransTestDir)
	os.RemoveAll(TransactionTestLog)
}
