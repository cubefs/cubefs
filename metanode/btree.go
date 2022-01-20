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
)

var (
	existsError    = fmt.Errorf("exists error")
	notExistsError = fmt.Errorf("not exists error")
	rocksdbError   = fmt.Errorf("rocksdb operation error")
	baseInfoKey    = []byte{byte(BaseInfoType)}
)

func NewSnapshot(mp *metaPartition) Snapshot {
	if mp.HasMemStore() {
		return &BTreeSnapShot{
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
	Range(tp TreeType, cb func(v []byte) (bool, error)) error
	Close()
	Count(tp TreeType) (uint64)
}

type Tree interface {
	Release()
	SetApplyID(index uint64)
	GetApplyID() uint64
	Flush() error
	Clear() error
	Execute(fn func(tree interface{}) interface{}) interface{}
	PersistBaseInfo() error
}

type InodeTree interface {
	Tree
	RefGet(ino uint64) (*Inode, error)
	Get(ino uint64) (*Inode, error)
	Put(inode *Inode) error
	Update(inode *Inode) error
	Create(inode *Inode, replace bool) error
	Delete(ino uint64) (bool, error)
	Range(start, end *Inode, cb func(v []byte) (bool, error)) error
	Count() uint64
	RealCount() uint64
	MaxItem() *Inode
	GetMaxInode() (uint64, error) //TODO: ANSJ
}

type DentryTree interface {
	Tree
	RefGet(ino uint64, name string) (*Dentry, error)
	Get(pino uint64, name string) (*Dentry, error)
	Put(dentry *Dentry) error
	Create(dentry *Dentry, replace bool) error
	Delete(pid uint64, name string) (bool, error)
	Range(start, end *Dentry, cb func(v []byte) (bool, error)) error
	RealCount() uint64
	Count() uint64
}

type ExtendTree interface {
	Tree
	RefGet(ino uint64) (*Extend, error)
	Get(ino uint64) (*Extend, error)
	Put(extend *Extend) error
	Update(extend *Extend) error
	Create(ext *Extend, replace bool) error
	Delete(ino uint64) (bool, error)
	Range(start, end *Extend, cb func(v []byte) (bool, error)) error
	RealCount() uint64
	Count() uint64
}

type MultipartTree interface {
	Tree
	RefGet(key, id string) (*Multipart, error)
	Get(key, id string) (*Multipart, error)
	Put(mutipart *Multipart) error
	Update(mutipart *Multipart) error
	Create(mul *Multipart, replace bool) error
	Delete(key, id string) (bool, error)
	Range(start, end *Multipart, cb func(v []byte) (bool, error)) error
	RealCount() uint64
	Count() uint64
}

//get refget create delete range realcount count
type DeletedDentryTree interface {
	Tree
	RefGet(pino uint64, name string, timeStamp int64) (*DeletedDentry, error)
	Get(pino uint64, name string, timeStamp int64) (*DeletedDentry, error)
	Create(delDentry *DeletedDentry, replace bool) error
	Delete(pino uint64, name string, timeStamp int64) (bool, error)
	Range(start, end *DeletedDentry, cb func(v []byte) (bool, error)) error
	RealCount() uint64
	Count() uint64
}

type DeletedInodeTree interface {
	Tree
	RefGet(ino uint64) (*DeletedINode, error)
	Get(ino uint64) (*DeletedINode, error)
	Create(delIno *DeletedINode, replace bool) error
	Delete(ino uint64) (bool, error)
	Range(start, end *DeletedINode, cb func(v []byte) (bool, error)) error
	RealCount() uint64
	Count() uint64
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
