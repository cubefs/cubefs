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
// permissions and limitations under the License.

package metanode

import (
	"errors"

	"github.com/cubefs/cubefs/proto"
	_ "github.com/cubefs/cubefs/proto"
)

type (
	// BtreeItem type alias google btree Item
	TreeType  uint8
	CountType uint8
)

const (
	BaseInfoType TreeType = iota
	DentryType
	InodeType
	ExtendType
	MultipartType
	TransactionType
	TransactionRollbackInodeType
	TransactionRollbackDentryType
	DeletedExtentsType
	DeletedObjExtentsType
	MaxType
)

func (t TreeType) String() string {
	switch t {
	case DentryType:
		return "dentry tree"
	case InodeType:
		return "inode tree"
	case ExtendType:
		return "extend tree"
	case MultipartType:
		return "multipart tree"
	case TransactionType:
		return "transaction tree"
	case TransactionRollbackInodeType:
		return "transaction rollback inode tree"
	case TransactionRollbackDentryType:
		return "transaction rollback dentry tree"
	case DeletedExtentsType:
		return "deleted extents"
	case DeletedObjExtentsType:
		return "deleted obj extents"
	default:
		return "unknown"
	}
}

var (
	baseInfoKey     = []byte{byte(BaseInfoType)}
	ErrOpenSnapshot = errors.New("failed to open snapshot")
)

func NewSnapshot(mp *metaPartition) (snap Snapshot, err error) {
	if mp.HasMemStore() {
		snap = &MemSnapShot{
			applyID:             mp.GetAppliedID(),
			txID:                mp.txProcessor.txManager.txIdAlloc.getTransactionID(),
			inode:               &InodeBTree{mp.inodeTree.(*InodeBTree).GetTree()},
			dentry:              &DentryBTree{mp.dentryTree.(*DentryBTree).GetTree()},
			extend:              &ExtendBTree{mp.extendTree.(*ExtendBTree).GetTree()},
			multipart:           &MultipartBTree{mp.multipartTree.(*MultipartBTree).GetTree()},
			transaction:         &TransactionBTree{mp.txProcessor.txManager.txTree.(*TransactionBTree).GetTree()},
			transactionRbInode:  &TransactionRollbackInodeBTree{mp.txProcessor.txResource.txRbInodeTree.(*TransactionRollbackInodeBTree).GetTree()},
			transactionRbDentry: &TransactionRollbackDentryBTree{mp.txProcessor.txResource.txRbDentryTree.(*TransactionRollbackDentryBTree).GetTree()},
		}
	}

	if mp.HasRocksDBStore() {
		snap = NewRocksSnapShot(mp)
	}

	if snap == nil {
		err = ErrOpenSnapshot
	}
	return
}

type Snapshot interface {
	Range(tp TreeType, cb func(item interface{}) bool) error
	RangeWithScope(tp TreeType, start, end interface{}, cb func(item interface{}) bool) error
	Close()
	Count(tp TreeType) uint64
	CrcSum(tp TreeType) (uint32, error)
	ApplyID() uint64
	TxID() uint64
	DeletedExtentId() uint64
}

type Tree interface {
	SetApplyID(index uint64)
	GetApplyID() uint64
	Flush() error
	Execute(fn func(tree interface{}) interface{}) interface{}
	CreateBatchWriteHandle() (interface{}, error)
	CommitBatchWrite(handle interface{}, needCommitApplyID bool) error
	CommitAndReleaseBatchWriteHandle(handle interface{}, needCommitApplyID bool) error
	CommitAndReleaseBatchWriteForClear(handle interface{}) error
	ReleaseBatchWriteHandle(handle interface{}) error
	ClearBatchWriteHandle(handle interface{}) error
	PersistBaseInfo() error
	GetPersistentApplyID() uint64
	SetCursor(cursor uint64)
	GetCursor() uint64
	SetTxId(txid uint64)
	GetTxId() uint64
	GetDeletedExtentId() uint64
	SetDeletedExtentId(id uint64)
	Clear(handle interface{}) (err error)
	DeleteMetadata(handle interface{}) (err error)
}

type InodeTree interface {
	Tree
	Get(ino *Inode) (*Inode, error)
	CopyGet(ino *Inode) (*Inode, error)
	Put(inode *Inode) error
	Update(inode *Inode) error
	ReplaceOrInsert(inode *Inode, replace bool) (*Inode, bool, error)
	Delete(inode *Inode) (bool, error)
	Range(start, end *Inode, cb func(i *Inode) bool) error
	Count() uint64
	Len() int
	RealCount() uint64
	MaxItem() *Inode
	GetMaxInode() (uint64, error)
	BatchPut(handle interface{}, inode *Inode) error
	BatchReplaceOrInsert(handle interface{}, inode *Inode, replace bool) (*Inode, bool, error)
	BatchUpdate(handle interface{}, inode *Inode) error
}

type DentryTree interface {
	Tree
	Get(dent *Dentry) (*Dentry, error)
	CopyGet(dent *Dentry) (*Dentry, error)
	Update(dentry *Dentry) error
	Put(dentry *Dentry) error
	ReplaceOrInsert(dentry *Dentry, replace bool) (*Dentry, bool, error)
	Delete(dentry *Dentry) (bool, error)
	Range(start, end *Dentry, cb func(d *Dentry) bool) error
	RangeWithPrefix(prefix, start, end *Dentry, cb func(d *Dentry) bool) error
	RealCount() uint64
	Count() uint64
	Len() int
	BatchPut(handle interface{}, dentry *Dentry) error
	BatchReplaceOrInsert(handle interface{}, dentry *Dentry, replace bool) (*Dentry, bool, error)
}

type ExtendTree interface {
	Tree
	Get(extent *Extend) (*Extend, error)
	CopyGet(extent *Extend) (*Extend, error)
	Put(extend *Extend) error
	Update(extend *Extend) error
	ReplaceOrInsert(ext *Extend, replace bool) (*Extend, bool, error)
	Delete(extend *Extend) (bool, error)
	Range(start, end *Extend, cb func(e *Extend) bool) error
	RealCount() uint64
	Count() uint64
	Len() int
	BatchPut(handle interface{}, extend *Extend) error
	BatchReplaceOrInsert(handle interface{}, ext *Extend, replace bool) (*Extend, bool, error)
}

type MultipartTree interface {
	Tree
	Get(multi *Multipart) (*Multipart, error)
	CopyGet(multi *Multipart) (*Multipart, error)
	Put(mutipart *Multipart) error
	Update(mutipart *Multipart) error
	ReplaceOrInsert(mul *Multipart, replace bool) (*Multipart, bool, error)
	Delete(mutipart *Multipart) (bool, error)
	Range(start, end *Multipart, cb func(m *Multipart) bool) error
	RangeWithPrefix(prefix, start, end *Multipart, cb func(m *Multipart) bool) error
	RealCount() uint64
	Count() uint64
	Len() int
	BatchPut(handle interface{}, mutipart *Multipart) error
	BatchReplaceOrInsert(handle interface{}, mul *Multipart, replace bool) (*Multipart, bool, error)
}

// NOTE: transaction
type TransactionTree interface {
	Tree
	Get(tx *proto.TransactionInfo) (*proto.TransactionInfo, error)
	CopyGet(tx *proto.TransactionInfo) (*proto.TransactionInfo, error)
	Put(tx *proto.TransactionInfo) error
	Update(tx *proto.TransactionInfo) error
	ReplaceOrInsert(tx *proto.TransactionInfo, replace bool) (*proto.TransactionInfo, bool, error)
	Delete(txId string) (bool, error)
	Range(start, end *proto.TransactionInfo, cb func(t *proto.TransactionInfo) bool) error
	RealCount() uint64
	Count() uint64
	Len() int
	BatchPut(handle interface{}, tx *proto.TransactionInfo) error
	BatchReplaceOrInsert(handle interface{}, tx *proto.TransactionInfo, replace bool) (*proto.TransactionInfo, bool, error)
}

type TransactionRollbackInodeTree interface {
	Tree
	Get(inode *TxRollbackInode) (*TxRollbackInode, error)
	CopyGet(inode *TxRollbackInode) (*TxRollbackInode, error)
	Put(inode *TxRollbackInode) error
	Update(inode *TxRollbackInode) error
	ReplaceOrInsert(inode *TxRollbackInode, replace bool) (*TxRollbackInode, bool, error)
	Delete(inode *TxRollbackInode) (bool, error)
	Range(start, end *TxRollbackInode, cb func(i *TxRollbackInode) bool) error
	Count() uint64
	Len() int
	RealCount() uint64
	BatchPut(handle interface{}, inode *TxRollbackInode) error
	BatchReplaceOrInsert(handle interface{}, inode *TxRollbackInode, replace bool) (*TxRollbackInode, bool, error)
}

type TransactionRollbackDentryTree interface {
	Tree
	Get(dentry *TxRollbackDentry) (*TxRollbackDentry, error)
	CopyGet(dentry *TxRollbackDentry) (*TxRollbackDentry, error)
	Update(dentry *TxRollbackDentry) error
	Put(dentry *TxRollbackDentry) error
	ReplaceOrInsert(dentry *TxRollbackDentry, replace bool) (*TxRollbackDentry, bool, error)
	Delete(dentry *TxRollbackDentry) (bool, error)
	Range(start, end *TxRollbackDentry, cb func(d *TxRollbackDentry) bool) error
	RangeWithPrefix(prefix, start, end *TxRollbackDentry, cb func(d *TxRollbackDentry) bool) error
	RealCount() uint64
	Count() uint64
	Len() int
	BatchPut(handle interface{}, dentry *TxRollbackDentry) error
	BatchReplaceOrInsert(handle interface{}, dentry *TxRollbackDentry, replace bool) (*TxRollbackDentry, bool, error)
}
