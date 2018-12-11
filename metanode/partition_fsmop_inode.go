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
	"github.com/tiglabs/containerfs/third_party/btree"
	"io"
)

type ResponseInode struct {
	Status uint8
	Msg    *Inode
}

func NewResponseInode() *ResponseInode {
	return &ResponseInode{
		Msg: NewInode(0, 0),
	}
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
	if i.MarkDelete == 1 {
		resp.Status = proto.OpNotExistErr
		return
	}
	i.NLink++
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
	if i.MarkDelete == 1 {
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
	if i.MarkDelete == 1 {
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

func (mp *metaPartition) RangeInode(f func(i btree.Item) bool) {
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
			inode.NLink--
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
	if ino2.MarkDelete == 1 {
		status = proto.OpNotExistErr
		return
	}
	var delItems []BtreeItem
	ino.Extents.Range(func(item BtreeItem) bool {
		delItems = append(delItems, ino2.AppendExtents(item)...)
		return true
	})
	ino2.ModifyTime = ino.ModifyTime
	ino2.Generation++
	for _, item := range delItems {
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
		if i.MarkDelete == 1 {
			resp.Status = proto.OpNotExistErr
			return
		}
		i.Extents.Extents.AscendGreaterOrEqual(&proto.
			ExtentKey{FileOffset: ino.Size},
			func(item BtreeItem) bool {
				delExtents = append(delExtents, item)
				return true
			})
		// delete
		for _, ext := range delExtents {
			i.Extents.Delete(ext)
			mp.extDelCh <- ext
		}
		// check max
		extItem := i.Extents.Max()
		if extItem != nil {
			ext := extItem.(*proto.ExtentKey)
			if (ext.FileOffset + uint64(ext.Size)) > ino.Size {
				ext.Size = uint32(ino.Size - ext.FileOffset)
			}
		}
		i.Size = ino.Size
		i.ModifyTime = ino.ModifyTime
		i.Generation++
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
		if proto.IsDir(i.Type) {
			if i.NLink < 2 {
				isDelete = true
			}
			return
		}

		if i.MarkDelete == 1 {
			return
		}
		if i.NLink < 1 {
			i.MarkDelete = 1
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
	if ino.MarkDelete == 1 {
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
	if req.Valid&proto.AttrMode != 0 {
		ino.Type = req.Mode
	}
	if req.Valid&proto.AttrUid != 0 {
		ino.Uid = req.Uid
	}
	if req.Valid&proto.AttrGid != 0 {
		ino.Gid = req.Gid
	}
	return
}
