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
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
)

// CreateDentry returns a new dentry.
func (mp *metaPartition) CreateDentry(req *CreateDentryReq, p *Packet) (err error) {
	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}

	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	val, err := dentry.Marshal()
	if err != nil {
		return
	}
	resp, err := mp.Put(opFSMCreateDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.ResultCode = resp.(uint8)
	return
}

// DeleteDentry deletes a dentry.
func (mp *metaPartition) DeleteDentry(req *DeleteDentryReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	val, err := dentry.Marshal()
	if err != nil {
		p.ResultCode = proto.OpErr
		return
	}
	r, err := mp.Put(opFSMDeleteDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	retMsg := r.(*DentryResponse)
	p.ResultCode = retMsg.Status
	dentry = retMsg.Msg
	if p.ResultCode == proto.OpOk {
		var reply []byte
		resp := &DeleteDentryResp{
			Inode: dentry.Inode,
		}
		reply, err = json.Marshal(resp)
		p.PacketOkWithBody(reply)
	}
	return
}

// UpdateDentry updates a dentry.
func (mp *metaPartition) UpdateDentry(req *UpdateDentryReq, p *Packet) (err error) {
	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}

	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
	}
	val, err := dentry.Marshal()
	if err != nil {
		p.ResultCode = proto.OpErr
		return
	}
	resp, err := mp.Put(opFSMUpdateDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*DentryResponse)
	p.ResultCode = msg.Status
	if msg.Status == proto.OpOk {
		var reply []byte
		m := &UpdateDentryResp{
			Inode: msg.Msg.Inode,
		}
		reply, err = json.Marshal(m)
		p.PacketOkWithBody(reply)
	}
	return
}

// ReadDir reads the directory based on the given request.
func (mp *metaPartition) ReadDir(req *ReadDirReq, p *Packet) (err error) {
	resp := mp.readDir(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PacketOkWithBody(reply)
	return
}

// Lookup looks up the given dentry from the request.
func (mp *metaPartition) Lookup(req *LookupReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	dentry, status := mp.getDentry(dentry)
	var reply []byte
	if status == proto.OpOk {
		resp := &LookupResp{
			Inode: dentry.Inode,
			Mode:  dentry.Type,
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// GetDentryTree returns the dentry tree stored in the meta partition.
func (mp *metaPartition) GetDentryTree() *BTree {
	return mp.dentryTree.GetTree()
}
