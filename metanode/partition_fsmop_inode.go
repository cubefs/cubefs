// Copyright 2018 The Containerfs Authors.
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
	"bytes"
	"encoding/binary"
	"github.com/tiglabs/containerfs/proto"
	"io"
)

type ResponseInode struct {
	Status uint8
	Msg    *Inode
	AuthID uint64
}

func NewResponseInode() *ResponseInode {
	return &ResponseInode{}
}

// CreateInode create inode to inode tree.
func (mp *metaPartition) createInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}
	return
}

func (mp *metaPartition) createLinkInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if proto.IsDir(i.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}
	if i.IsDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	if mp.isDump.Bool() {
		i = i.Copy()
		mp.inodeTree.ReplaceOrInsert(i, true)
	}
	i.IncrNLink()
	resp.Msg = i
	return
}

// GetInode query inode from InodeTree with specified inode info;
func (mp *metaPartition) getInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.IsDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = i
	return
}

func (mp *metaPartition) hasInode(ino *Inode) (ok bool) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		ok = false
		return
	}
	i := item.(*Inode)
	if i.IsDelete() {
		ok = false
		return
	}
	ok = true
	return
}

func (mp *metaPartition) internalHasInode(ino *Inode) bool {
	return mp.inodeTree.Has(ino)
}

func (mp *metaPartition) getInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

func (mp *metaPartition) RangeInode(f func(i BtreeItem) bool) {
	mp.inodeTree.Ascend(f)
}

// DeleteInode delete specified inode item from inode tree.
func (mp *metaPartition) deleteInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	isFind := false
	isDelete := false
	mp.inodeTree.Find(ino, func(i BtreeItem) {
		isFind = true
		inode := i.(*Inode)
		resp.Msg = inode
		if proto.IsRegular(inode.Type) {
			inode.DecrNLink()
			return
		}
		// should delete inode
		isDelete = true
	})
	if !isFind {
		resp.Status = proto.OpNotExistErr
		return
	}
	if isDelete {
		mp.inodeTree.Delete(ino)
	}
	return
}

func (mp *metaPartition) internalDelete(val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	for {
		err = binary.Read(buf, binary.BigEndian, &ino.Inode)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		mp.internalDeleteInode(ino)
	}
}

func (mp *metaPartition) internalDeleteInode(ino *Inode) {
	mp.inodeTree.Delete(ino)
	return
}

func (mp *metaPartition) appendExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := item.(*Inode)
	if ino2.IsDelete() {
		status = proto.OpNotExistErr
		return
	}
	if mp.isDump.Bool() {
		ino2 = ino2.Copy()
		mp.inodeTree.ReplaceOrInsert(ino2, true)
	}
	if !ino2.CanWrite(ino.AuthID, ino.AccessTime) {
		status = proto.OpNotPerm
		return
	}
	var items []BtreeItem
	ino.Extents.Range(func(item BtreeItem) bool {
		items = append(items, item)
		return true
	})
	items = ino2.AppendExtents(items, ino.ModifyTime)
	for _, item := range items {
		mp.extDelCh <- item
	}
	return
}

func (mp *metaPartition) extentsTruncate(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	isFind := false
	mp.inodeTree.Find(ino, func(item BtreeItem) {
		var delExtents []BtreeItem
		isFind = true
		i := item.(*Inode)
		if proto.IsDir(i.Type) {
			resp.Status = proto.OpArgMismatchErr
			return
		}
		if i.IsDelete() {
			resp.Status = proto.OpNotExistErr
			return
		}
		if !i.CanWrite(ino.AuthID, ino.AccessTime) {
			resp.Status = proto.OpNotPerm
			return
		}
		i.Extents.AscendGreaterOrEqual(&proto.ExtentKey{FileOffset: ino.Size},
			func(item BtreeItem) bool {
				delExtents = append(delExtents, item)
				return true
			})
		i.ExtentsTruncate(delExtents, ino.Size, ino.ModifyTime)
		// now we should delete the extent
		for _, ext := range delExtents {
			mp.extDelCh <- ext
		}
	})
	if !isFind {
		resp.Status = proto.OpNotExistErr
		return
	}

	return
}

func (mp *metaPartition) evictInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	isFind := false
	isDelete := false
	mp.inodeTree.Find(ino, func(item BtreeItem) {
		isFind = true
		i := item.(*Inode)
		if mp.isDump.Bool() {
			i = i.Copy()
			mp.inodeTree.ReplaceOrInsert(i, true)
		}
		if proto.IsDir(i.Type) {
			if i.GetNLink() < 2 {
				isDelete = true
			}
			return
		}

		if i.IsDelete() {
			return
		}
		if i.GetNLink() < 1 {
			i.SetDeleteMark()
			// push to free list
			mp.freeList.Push(i)
		}
	})
	if !isFind {
		resp.Status = proto.OpNotExistErr
		return
	}
	if isDelete {
		mp.inodeTree.Delete(ino)
	}
	return
}

func (mp *metaPartition) checkAndInsertFreeList(ino *Inode) {
	if proto.IsDir(ino.Type) {
		return
	}
	if ino.IsDelete() {
		mp.freeList.Push(ino)
	}
}

func (mp *metaPartition) setAttr(req *SetattrRequest) (err error) {
	// get Inode
	ino := NewInode(req.Inode, req.Mode)
	item := mp.inodeTree.Get(ino)
	if item == nil {
		return
	}
	ino = item.(*Inode)
	if mp.isDump.Bool() {
		ino = ino.Copy()
		mp.inodeTree.ReplaceOrInsert(ino, true)
	}
	ino.SetAttr(req.Valid, req.Mode, req.Uid, req.Gid)
	return
}
