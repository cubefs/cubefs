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
	"github.com/tiglabs/containerfs/proto"
)

type ResponseDentry struct {
	Status uint8
	Msg    *Dentry
}

func NewResponseDentry() *ResponseDentry {
	return &ResponseDentry{
		Msg: &Dentry{},
	}
}

// CreateDentry insert dentry into dentry tree.
func (mp *metaPartition) createDentry(dentry *Dentry) (status uint8) {
	status = proto.OpOk
	// check inode
	inoItem := mp.inodeTree.Get(NewInode(dentry.ParentId, 0))
	if inoItem == nil {
		status = proto.OpNotExistErr
		return
	}
	ino := inoItem.(*Inode)
	if !proto.IsDir(ino.Type) {
		status = proto.OpArgMismatchErr
		return
	}
	if ino.GetNLink() < 2 {
		status = proto.OpNotPerm
		return
	}
	if item, ok := mp.dentryTree.ReplaceOrInsert(dentry, false); !ok {
		status = proto.OpExistErr
		d := item.(*Dentry)
		if dentry.Type != d.Type {
			status = proto.OpArgMismatchErr
		}
	}
	return
}

// GetDentry query dentry from DentryTree with specified dentry info;
func (mp *metaPartition) getDentry(dentry *Dentry) (*Dentry, uint8) {
	status := proto.OpOk
	item := mp.dentryTree.Get(dentry)
	if item == nil {
		status = proto.OpNotExistErr
		return nil, status
	}
	dentry = item.(*Dentry)
	return dentry, status
}

// DeleteDentry delete dentry from dentry tree.
func (mp *metaPartition) deleteDentry(dentry *Dentry) (resp *ResponseDentry) {
	resp = NewResponseDentry()
	resp.Status = proto.OpOk
	item := mp.dentryTree.Delete(dentry)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = item.(*Dentry)
	return
}

func (mp *metaPartition) updateDentry(dentry *Dentry) (resp *ResponseDentry) {
	resp = NewResponseDentry()
	resp.Status = proto.OpOk
	item := mp.dentryTree.Get(dentry)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	d := item.(*Dentry)
	d.Inode, dentry.Inode = dentry.Inode, d.Inode
	resp.Msg = dentry
	return
}

func (mp *metaPartition) getDentryTree() *BTree {
	return mp.dentryTree.GetTree()
}

func (mp *metaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp) {
	resp = &ReadDirResp{}
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(begDentry, endDentry, func(i BtreeItem) bool {
		d := i.(*Dentry)
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		return true
	})
	return
}
