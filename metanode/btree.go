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

import "fmt"

type (
	// BtreeItem type alias google btree Item
	TreeType uint8
)

const (
	ApplyIDType TreeType = iota
	DentryType
	InodeType
	ExtendType
	MultipartType
)

var (
	existsError = fmt.Errorf("exists error")
	applyIDKey  = []byte{byte(ApplyIDType)}
)

func NewSnapshot(mp *MetaPartition) Snapshot {
	if mp.config.Store == 0 {
		return &BTreeSnapShot{
			inode:     &InodeBTree{mp.inodeTree.(*InodeBTree).GetTree()},
			dentry:    &DentryBTree{mp.dentryTree.(*DentryBTree).GetTree()},
			extend:    &ExtendBTree{mp.extendTree.(*ExtendBTree).GetTree()},
			multipart: &MultipartBTree{mp.multipartTree.(*MultipartBTree).GetTree()},
		}
	} else {
		return &RocksSnapShot{
			snap: mp.inodeTree.(*InodeRocks).db.NewSnapshot(),
			tree: mp.inodeTree.(*InodeRocks).RocksTree,
		}
	}
}

type Snapshot interface {
	Range(tp TreeType, cb func(v []byte) (bool, error)) error
	Close()
	Count(tp TreeType) (uint64, error)
}

type Tree interface {
	Release()
	SetApplyID(index uint64)
	GetApplyID() (uint64, error)
	Flush() error
}

type InodeTree interface {
	Tree
	RefGet(ino uint64) (*Inode, error)
	Get(ino uint64) (*Inode, error)
	Put(inode *Inode) error
	Update(inode *Inode) error
	Create(inode *Inode) error
	Delete(ino uint64) error
	Range(start, end *Inode, cb func(v []byte) (bool, error)) error
	Count() uint64
}

type DentryTree interface {
	Tree
	RefGet(ino uint64, name string) (*Dentry, error)
	Get(ino uint64, name string) (*Dentry, error)
	Put(dentry *Dentry) error
	Create(dentry *Dentry) error
	Delete(pid uint64, name string) error
	Range(start, end *Dentry, cb func(v []byte) (bool, error)) error
	Count() uint64
}

type ExtendTree interface {
	Tree
	RefGet(ino uint64) (*Extend, error)
	Get(ino uint64) (*Extend, error)
	Put(extend *Extend) error
	Update(extend *Extend) error
	Create(ext *Extend) error
	Delete(ino uint64) error
	Range(start, end *Extend, cb func(v []byte) (bool, error)) error
}

type MultipartTree interface {
	Tree
	RefGet(key, id string) (*Multipart, error)
	Get(key, id string) (*Multipart, error)
	Put(mutipart *Multipart) error
	Update(mutipart *Multipart) error
	Create(mul *Multipart) error
	Delete(key, id string) error
	Range(start, end *Multipart, cb func(v []byte) (bool, error)) error
}
