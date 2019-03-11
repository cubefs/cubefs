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
	"bytes"
	"encoding/binary"
	"github.com/chubaofs/cfs/proto"
	"io"
)

type InodeResponse struct {
	Status uint8
	Msg    *Inode
	AuthID uint64
}

func NewInodeResponse() *InodeResponse {
	return &InodeResponse{}
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}
	return
}

func (mp *metaPartition) fsmCreateLinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if proto.IsDir(i.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	i.IncNLink()
	resp.Msg = i
	return
}

func (mp *metaPartition) getInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
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
	if i.ShouldDelete() {
		ok = false
		return
	}
	ok = true
	return
}

func (mp *metaPartition) getInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

// Ascend is the wrapper of inodeTree.Ascend
func (mp *metaPartition) Ascend(f func(i BtreeItem) bool) {
	mp.inodeTree.Ascend(f)
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	isFound := false
	shouldDelete := false
	mp.inodeTree.CopyFind(ino, func(i BtreeItem) {
		isFound = true
		inode := i.(*Inode)
		inode.ModifyTime = ino.ModifyTime
		resp.Msg = inode
		if !proto.IsDir(inode.Type) {
			inode.DecNLink()
			if inode.GetNLink() == 0 {
				mp.freeList.Push(ino)
			}
			return
		}

		//TODO: isDir should record subDir for fsck
		if inode.IsEmptyDir() {
			shouldDelete = true
		} else {
			resp.Status = proto.OpNotEmtpy
		}

	})
	if !isFound {
		resp.Status = proto.OpNotExistErr
		return
	}
	if shouldDelete {
		mp.inodeTree.Delete(ino)
	}
	return
}

func (mp *metaPartition) internalHasInode(ino *Inode) bool {
	return mp.inodeTree.Has(ino)
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

func (mp *metaPartition) fsmAppendExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := item.(*Inode)
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
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

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk
	isFound := false
	mp.inodeTree.CopyFind(ino, func(item BtreeItem) {
		var delExtents []BtreeItem
		isFound = true
		i := item.(*Inode)
		if proto.IsDir(i.Type) {
			resp.Status = proto.OpArgMismatchErr
			return
		}
		if i.ShouldDelete() {
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
	if !isFound {
		resp.Status = proto.OpNotExistErr
		return
	}

	return
}

func (mp *metaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk
	isFound := false
	shouldDelete := false
	mp.inodeTree.CopyFind(ino, func(item BtreeItem) {
		isFound = true
		i := item.(*Inode)
		if proto.IsDir(i.Type) {
			if i.GetNLink() < 1 {
				shouldDelete = true
			}
			return
		}

		if i.ShouldDelete() {
			return
		}
		if i.GetNLink() < 1 {
			i.SetDeleteMark()
			// push to free list
			mp.freeList.Push(i)
		}
	})
	if !isFound {
		resp.Status = proto.OpNotExistErr
		return
	}
	if shouldDelete {
		mp.inodeTree.Delete(ino)
	}
	return
}

func (mp *metaPartition) checkAndInsertFreeList(ino *Inode) {
	if proto.IsDir(ino.Type) {
		return
	}
	if ino.ShouldDelete() {
		mp.freeList.Push(ino)
	}
}

func (mp *metaPartition) fsmSetAttr(req *SetattrRequest) (err error) {
	// get Inode

	ino := NewInode(req.Inode, req.Mode)
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		return
	}
	ino = item.(*Inode)
	ino.SetAttr(req.Valid, req.Mode, req.Uid, req.Gid)
	return
}
