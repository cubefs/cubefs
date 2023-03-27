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
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
)

// CreateDentry returns a new dentry.
func (mp *metaPartition) CreateDentry(req *CreateDentryReq, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.ParentID); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}

	var (
		parIno *Inode
		exist    bool
	)
	if exist, parIno = mp.hasInode(NewInode(req.ParentID, 0)); !exist || parIno == nil {
		err = fmt.Errorf("parent inode (%v) not exist", req.ParentID)
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	}

	if mp.config.ChildFileMaxCount > 0 && parIno.NLink > mp.config.ChildFileMaxCount {
		err = fmt.Errorf("child file count reach max count:%v", mp.config.ChildFileMaxCount)
		p.PacketErrorWithBody(proto.OpNotPerm, []byte(err.Error()))
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
	resp, err := mp.submit(p.Ctx(), opFSMCreateDentry, p.RemoteWithReqID(), val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.ResultCode = resp.(uint8)
	return
}

// DeleteDentry deletes a dentry.
func (mp *metaPartition) DeleteDentry(req *DeleteDentryReq, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.ParentID); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	val, err := dentry.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	var r interface{}
	if req.NoTrash || mp.isTrashDisable() {
		r, err = mp.submit(p.Ctx(), opFSMDeleteDentry, p.RemoteWithReqID(), val)
	} else {
		r, err = mp.submitTrash(p.Ctx(), opFSMDeleteDentry, p.RemoteWithReqID(), val)
	}
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

// DeleteDentry deletes a dentry.
func (mp *metaPartition) DeleteDentryBatch(req *BatchDeleteDentryReq, p *Packet) (err error) {

	db := make(DentryBatch, 0, len(req.Dens))

	for _, d := range req.Dens {
		db = append(db, &Dentry{
			ParentId: req.ParentID,
			Name:     d.Name,
			Inode:    d.Inode,
			Type:     d.Type,
		})
	}

	val, err := db.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	var r interface{}
	if req.NoTrash || mp.isTrashDisable() {
		r, err = mp.submit(p.Ctx(), opFSMDeleteDentryBatch, p.RemoteWithReqID(), val)
	} else {
		r, err = mp.submitTrash(p.Ctx(), opFSMDeleteDentryBatch, p.RemoteWithReqID(), val)
	}
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	retMsg := r.([]*DentryResponse)
	p.ResultCode = proto.OpOk

	bddr := &BatchDeleteDentryResp{}

	for _, m := range retMsg {
		if m.Status != proto.OpOk {
			p.ResultCode = proto.OpErr
		}

		if dentry := m.Msg; dentry != nil {
			bddr.Items = append(bddr.Items, &struct {
				Inode  uint64 `json:"ino"`
				Status uint8  `json:"status"`
			}{
				Inode:  dentry.Inode,
				Status: m.Status,
			})
		} else {
			bddr.Items = append(bddr.Items, &struct {
				Inode  uint64 `json:"ino"`
				Status uint8  `json:"status"`
			}{
				Status: m.Status,
			})
		}

	}

	reply, err := json.Marshal(bddr)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	if p.ResultCode != proto.OpOk {
		p.PacketErrorWithBody(p.ResultCode, reply)
		return
	}
	p.PacketOkWithBody(reply)

	return
}

// UpdateDentry updates a dentry.
func (mp *metaPartition) UpdateDentry(req *UpdateDentryReq, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.ParentID); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

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
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(p.Ctx(), opFSMUpdateDentry, p.RemoteWithReqID(), val)
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

	if _, err = mp.isInoOutOfRange(req.ParentID); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	mp.monitorData[proto.ActionMetaReadDir].UpdateData(0)

	var (
		resp  *ReadDirResp
		reply []byte
	)
	resp, err = mp.readDir(p.Ctx(), req)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	reply, err = json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(reply)
	return
}

// Lookup looks up the given dentry from the request.
func (mp *metaPartition) Lookup(req *LookupReq, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.ParentID); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	mp.monitorData[proto.ActionMetaLookup].UpdateData(0)

	var (
		dentry *Dentry
		status uint8
	)
	dentry, status, err = mp.getDentry(&Dentry{ParentId: req.ParentID, Name: req.Name})
	if err != nil {
		p.PacketErrorWithBody(status, []byte(err.Error()))
		return
	}
	var reply []byte
	if status == proto.OpOk {
		resp := &LookupResp{
			Inode: dentry.Inode,
			Mode:  dentry.Type,
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) GetDentryTree() DentryTree {
	return mp.dentryTree
}

func (mp *metaPartition) GetTrashCleanItemMaxTotalCountForEachTime() (maxTotalCount uint64) {
	maxTotalCount = defCleanTrashItemMaxTotalCountEachTime

	return
}
