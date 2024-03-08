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
			deletedExtents:      &DeletedExtentsBTree{mp.deletedExtentsTree.(*DeletedExtentsBTree).GetTree()},
			deletedObjExtents:   &DeletedObjExtentsBTree{mp.deletedObjExtentsTree.(*DeletedObjExtentsBTree).GetTree()},
			deletedExtentId:     mp.GetDeletedExtentId(),
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
	Range(tp TreeType, cb func(item interface{}) (bool, error)) error
	RangeWithScope(tp TreeType, start, end interface{}, cb func(item interface{}) (bool, error)) error
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
	ReleaseBatchWriteHandle(handle interface{}) error
	BatchWriteCount(handle interface{}) (int, error)
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
	//RefGet, Get: error always is nil when mem mode, whether the item is nil needs to be determined
	RefGet(ino uint64) (*Inode, error)
	Get(ino uint64) (*Inode, error)
	Put(dbHandle interface{}, inode *Inode) error
	Update(dbHandle interface{}, inode *Inode) error
	Create(dbHandle interface{}, inode *Inode, replace bool) (*Inode, bool, error)
	Delete(dbHandle interface{}, ino uint64) (bool, error)
	Range(start, end *Inode, cb func(i *Inode) (bool, error)) error
	Count() uint64
	Len() int
	RealCount() uint64
	MaxItem() *Inode
	GetMaxInode() (uint64, error)
}

type DentryTree interface {
	Tree
	//RefGet, Get: error always is nil when mem mode, whether the item is nil needs to be determined
	RefGet(ino uint64, name string) (*Dentry, error)
	Get(pino uint64, name string) (*Dentry, error)
	Update(dbHandle interface{}, dentry *Dentry) error
	Put(dbHandle interface{}, dentry *Dentry) error
	Create(dbHandle interface{}, dentry *Dentry, replace bool) (*Dentry, bool, error)
	Delete(dbHandle interface{}, pid uint64, name string) (bool, error)
	Range(start, end *Dentry, cb func(d *Dentry) (bool, error)) error
	RangeWithPrefix(prefix, start, end *Dentry, cb func(d *Dentry) (bool, error)) error
	RealCount() uint64
	Count() uint64
	Len() int
}

type ExtendTree interface {
	Tree
	//RefGet, Get: error always is nil when mem mode, whether the item is nil needs to be determined
	RefGet(ino uint64) (*Extend, error)
	Get(ino uint64) (*Extend, error)
	Put(dbHandle interface{}, extend *Extend) error
	Update(dbHandle interface{}, extend *Extend) error
	Create(dbHandle interface{}, ext *Extend, replace bool) (*Extend, bool, error)
	Delete(dbHandle interface{}, ino uint64) (bool, error)
	Range(start, end *Extend, cb func(e *Extend) (bool, error)) error
	RealCount() uint64
	Count() uint64
	Len() int
}

type MultipartTree interface {
	Tree
	//RefGet, Get: error always is nil when mem mode, whether the item is nil needs to be determined
	RefGet(key, id string) (*Multipart, error)
	Get(key, id string) (*Multipart, error)
	Put(dbHandle interface{}, mutipart *Multipart) error
	Update(dbHandle interface{}, mutipart *Multipart) error
	Create(dbHandle interface{}, mul *Multipart, replace bool) (*Multipart, bool, error)
	Delete(dbHandle interface{}, key, id string) (bool, error)
	Range(start, end *Multipart, cb func(m *Multipart) (bool, error)) error
	RangeWithPrefix(prefix, start, end *Multipart, cb func(m *Multipart) (bool, error)) error
	RealCount() uint64
	Count() uint64
	Len() int
}

// NOTE: transaction
type TransactionTree interface {
	Tree
	RefGet(txId string) (*proto.TransactionInfo, error)
	Get(txId string) (*proto.TransactionInfo, error)
	Put(dbHandle interface{}, tx *proto.TransactionInfo) error
	Update(dbHandle interface{}, tx *proto.TransactionInfo) error
	Create(dbHandle interface{}, tx *proto.TransactionInfo, replace bool) (*proto.TransactionInfo, bool, error)
	Delete(dbHandle interface{}, txId string) (bool, error)
	Range(start, end *proto.TransactionInfo, cb func(t *proto.TransactionInfo) (bool, error)) error
	RealCount() uint64
	Count() uint64
	Len() int
}

type TransactionRollbackInodeTree interface {
	Tree
	RefGet(ino uint64) (*TxRollbackInode, error)
	Get(ino uint64) (*TxRollbackInode, error)
	Put(dbHandle interface{}, inode *TxRollbackInode) error
	Update(dbHandle interface{}, inode *TxRollbackInode) error
	Create(dbHandle interface{}, inode *TxRollbackInode, replace bool) (*TxRollbackInode, bool, error)
	Delete(dbHandle interface{}, ino uint64) (bool, error)
	Range(start, end *TxRollbackInode, cb func(i *TxRollbackInode) (bool, error)) error
	Count() uint64
	Len() int
	RealCount() uint64
}

type TransactionRollbackDentryTree interface {
	Tree
	RefGet(ino uint64, name string) (*TxRollbackDentry, error)
	Get(pino uint64, name string) (*TxRollbackDentry, error)
	Update(dbHandle interface{}, dentry *TxRollbackDentry) error
	Put(dbHandle interface{}, dentry *TxRollbackDentry) error
	Create(dbHandle interface{}, dentry *TxRollbackDentry, replace bool) (*TxRollbackDentry, bool, error)
	Delete(dbHandle interface{}, pid uint64, name string) (bool, error)
	Range(start, end *TxRollbackDentry, cb func(d *TxRollbackDentry) (bool, error)) error
	RangeWithPrefix(prefix, start, end *TxRollbackDentry, cb func(d *TxRollbackDentry) (bool, error)) error
	RealCount() uint64
	Count() uint64
	Len() int
}

// NOTE: deleted extent
type DeletedExtentsTree interface {
	Tree
	Put(dbHandle interface{}, dek *DeletedExtentKey) error
	Delete(dbHandle interface{}, dek *DeletedExtentKey) (bool, error)
	Range(start, end *DeletedExtentKey, cb func(dek *DeletedExtentKey) (bool, error)) error
	RangeWithPrefix(prefix, start, end *DeletedExtentKey, cb func(dek *DeletedExtentKey) (bool, error)) error
	RealCount() uint64
	Count() uint64
	Len() int
}

type DeletedObjExtentsTree interface {
	Tree
	Put(dbHandle interface{}, doek *DeletedObjExtentKey) error
	Delete(dbHandle interface{}, doek *DeletedObjExtentKey) (bool, error)
	Range(start, end *DeletedObjExtentKey, cb func(doek *DeletedObjExtentKey) (bool, error)) error
	RangeWithPrefix(prefix, start, end *DeletedObjExtentKey, cb func(doek *DeletedObjExtentKey) (bool, error)) error
	RealCount() uint64
	Count() uint64
	Len() int
}
