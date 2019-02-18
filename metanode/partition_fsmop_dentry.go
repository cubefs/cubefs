// Copyright 2018 The Container File System Authors.
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

type DentryResponse struct {
	Status uint8
	Msg    *Dentry
}

func NewDentryResponse() *DentryResponse {
	return &DentryResponse{
		Msg: &Dentry{},
	}
}

// Insert a dentry into the dentry tree.
func (mp *metaPartition) fsmCreateDentry(dentry *Dentry,
	forceUpdate bool) (status uint8) {
	status = proto.OpOk
	if !forceUpdate {
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

// Query a dentry from the dentry tree with specified dentry info.
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

// Delete dentry from the dentry tree.
func (mp *metaPartition) fsmDeleteDentry(dentry *Dentry) (
	resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	item := mp.dentryTree.Delete(dentry)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = item.(*Dentry)
	return
}

func (mp *metaPartition) fsmUpdateDentry(dentry *Dentry) (
	resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	item := mp.dentryTree.CopyGet(dentry)
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
