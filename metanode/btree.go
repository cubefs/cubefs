// Copyright 2018 The Chubao Authors.
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
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	_ "github.com/chubaofs/chubaofs/proto"
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
	DelDentryType
	DelInodeType
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
	case DelDentryType:
		return "deleted dentry tree"
	case DelInodeType:
		return "deleted inode tree"
	default:
		return "unknown"
	}
}

var (
	existsError    = fmt.Errorf("exists error")
	baseInfoKey    = []byte{byte(BaseInfoType)}
)

func NewSnapshot(mp *metaPartition) Snapshot {
	if mp.HasMemStore() {
		return &MemSnapShot{
			applyID:   mp.GetAppliedID(),
			inode:     &InodeBTree{mp.inodeTree.(*InodeBTree).GetTree()},
			dentry:    &DentryBTree{mp.dentryTree.(*DentryBTree).GetTree()},
			extend:    &ExtendBTree{mp.extendTree.(*ExtendBTree).GetTree()},
			multipart: &MultipartBTree{mp.multipartTree.(*MultipartBTree).GetTree()},
			delDentry: &DeletedDentryBTree{mp.dentryDeletedTree.(*DeletedDentryBTree).GetTree()},
			delInode:  &DeletedInodeBTree{mp.inodeDeletedTree.(*DeletedInodeBTree).GetTree()},
		}
	}
	if mp.HasRocksDBStore() {
		return NewRocksSnapShot(mp)
	}

	return nil
}

type Snapshot interface {
	Range(tp TreeType, cb func(item interface{}) (bool, error)) error
	Close()
	Count(tp TreeType) uint64
	CrcSum(tp TreeType) (uint32, error)
	ApplyID() uint64
}

type Tree interface {
	Release()
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
}

type InodeTree interface {
	Tree
	RefGet(ino uint64) (*Inode, error)
	Get(ino uint64) (*Inode, error)
	Put(dbHandle interface{}, inode *Inode) error
	Update(dbHandle interface{}, inode *Inode) error
	Create(dbHandle interface{}, inode *Inode, replace bool) (*Inode, bool, error)
	Delete(dbHandle interface{}, ino uint64) (bool, error)
	Range(start, end *Inode, cb func(i *Inode) (bool, error)) error
	Count() uint64
	RealCount() uint64
	MaxItem() *Inode
	GetMaxInode() (uint64, error)
}

type DentryTree interface {
	Tree
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
}

type ExtendTree interface {
	Tree
	RefGet(ino uint64) (*Extend, error)
	Get(ino uint64) (*Extend, error)
	Put(dbHandle interface{}, extend *Extend) error
	Update(dbHandle interface{}, extend *Extend) error
	Create(dbHandle interface{}, ext *Extend, replace bool) (*Extend, bool, error)
	Delete(dbHandle interface{}, ino uint64) (bool, error)
	Range(start, end *Extend, cb func(e *Extend) (bool, error)) error
	RealCount() uint64
	Count() uint64
}

type MultipartTree interface {
	Tree
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
}

//get refget create delete range realcount count
type DeletedDentryTree interface {
	Tree
	RefGet(pino uint64, name string, timeStamp int64) (*DeletedDentry, error)
	Get(pino uint64, name string, timeStamp int64) (*DeletedDentry, error)
	Create(dbHandle interface{}, delDentry *DeletedDentry, replace bool) (*DeletedDentry, bool, error)
	Delete(dbHandle interface{}, pino uint64, name string, timeStamp int64) (bool, error)
	Range(start, end *DeletedDentry, cb func(dd *DeletedDentry) (bool, error)) error
	RangeWithPrefix(prefix, start, end *DeletedDentry, cb func(dd *DeletedDentry) (bool, error)) error
	RealCount() uint64
	Count() uint64
}

type DeletedInodeTree interface {
	Tree
	RefGet(ino uint64) (*DeletedINode, error)
	Get(ino uint64) (*DeletedINode, error)
	Create(dbHandle interface{}, delIno *DeletedINode, replace bool) (*DeletedINode, bool, error)
	Delete(dbHandle interface{}, ino uint64) (bool, error)
	Range(start, end *DeletedINode, cb func(di *DeletedINode) (bool, error)) error
	RealCount() uint64
	Count() uint64
	Update(dbHandle interface{}, delIno *DeletedINode) error
}

type MetaTree struct {
	InodeTree
	DentryTree
	ExtendTree
	MultipartTree
	DeletedDentryTree
	DeletedInodeTree
}

func newMetaTree(storeMode proto.StoreMode, db *RocksDbInfo) *MetaTree {
	if (storeMode & proto.StoreModeMem) != 0 {
		return &MetaTree{
			InodeTree:         &InodeBTree{NewBtree()},
			DentryTree:        &DentryBTree{NewBtree()},
			ExtendTree:        &ExtendBTree{NewBtree()},
			MultipartTree:     &MultipartBTree{NewBtree()},
			DeletedDentryTree: &DeletedDentryBTree{NewBtree()},
			DeletedInodeTree:  &DeletedInodeBTree{NewBtree()},
		}
	} else {
		tree, err := DefaultRocksTree(db)
		if err != nil {
			return nil
		}
		inodeTree, _ := NewInodeRocks(tree)
		dentryTree, _ := NewDentryRocks(tree)
		extendTree, _ := NewExtendRocks(tree)
		multipartTree, _ := NewMultipartRocks(tree)
		delDentryTree, _ := NewDeletedDentryRocks(tree)
		delInodeTree, _ := NewDeletedInodeRocks(tree)
		return &MetaTree{
			InodeTree:         inodeTree,
			DentryTree:        dentryTree,
			ExtendTree:        extendTree,
			MultipartTree:     multipartTree,
			DeletedDentryTree: delDentryTree,
			DeletedInodeTree:  delInodeTree,
		}
	}
}
