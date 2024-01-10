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
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) TxCreateDentry(req *proto.TxCreateDentryRequest, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogDentryOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.Name, req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, 0)
		}()
	}
	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}

	for _, quotaId := range req.QuotaIds {
		status := mp.mqMgr.IsOverQuota(false, true, quotaId)
		if status != 0 {
			err = errors.New("create dentry is over quota")
			reply := []byte(err.Error())
			p.PacketErrorWithBody(status, reply)
			return
		}
	}

	var parIno *Inode
	item := mp.inodeTree.Get(NewInode(req.ParentID, 0))
	if item == nil {
		err = fmt.Errorf("parent inode not exists")
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	}

	parIno = item.(*Inode)
	quota := atomic.LoadUint32(&dirChildrenNumLimit)
	if parIno.NLink >= quota {
		err = fmt.Errorf("parent dir quota limitation reached")
		p.PacketErrorWithBody(proto.OpDirQuota, []byte(err.Error()))
		return
	}

	txInfo := req.TxInfo.GetCopy()
	txDentry := NewTxDentry(req.ParentID, req.Name, req.Inode, req.Mode, parIno, txInfo)
	val, err := txDentry.Marshal()
	if err != nil {
		return
	}

	status, err := mp.submit(opFSMTxCreateDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	p.ResultCode = status.(uint8)
	return
}

// CreateDentry returns a new dentry.
func (mp *metaPartition) CreateDentry(req *CreateDentryReq, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogDentryOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.Name, req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, req.ParentID)
		}()
	}
	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}

	item := mp.inodeTree.CopyGet(NewInode(req.ParentID, 0))
	if item == nil {
		err = fmt.Errorf("parent inode not exists")
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	} else {
		parIno := item.(*Inode)
		quota := atomic.LoadUint32(&dirChildrenNumLimit)
		if parIno.NLink >= quota {
			err = fmt.Errorf("parent dir quota limitation reached")
			p.PacketErrorWithBody(proto.OpDirQuota, []byte(err.Error()))
			return
		}
	}

	dentry := &Dentry{
		ParentId:  req.ParentID,
		Name:      req.Name,
		Inode:     req.Inode,
		Type:      req.Mode,
		multiSnap: NewDentrySnap(mp.GetVerSeq()),
	}
	val, err := dentry.Marshal()
	if err != nil {
		return
	}
	resp, err := mp.submit(opFSMCreateDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.ResultCode = resp.(uint8)
	return
}

func (mp *metaPartition) QuotaCreateDentry(req *proto.QuotaCreateDentryRequest, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogDentryOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.Name, req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, req.ParentID)
		}()
	}
	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}
	for _, quotaId := range req.QuotaIds {
		status := mp.mqMgr.IsOverQuota(false, true, quotaId)
		if status != 0 {
			err = errors.New("create dentry is over quota")
			reply := []byte(err.Error())
			p.PacketErrorWithBody(status, reply)
			return
		}
	}
	item := mp.inodeTree.CopyGet(NewInode(req.ParentID, 0))
	if item == nil {
		err = fmt.Errorf("parent inode not exists")
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	} else {
		parIno := item.(*Inode)
		quota := atomic.LoadUint32(&dirChildrenNumLimit)
		if parIno.NLink >= quota {
			err = fmt.Errorf("parent dir quota limitation reached")
			p.PacketErrorWithBody(proto.OpDirQuota, []byte(err.Error()))
			return
		}
	}

	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	dentry.setVerSeq(mp.verSeq)
	log.LogDebugf("action[CreateDentry] mp[%v] with seq [%v],dentry [%v]", mp.config.PartitionId, mp.verSeq, dentry)
	val, err := dentry.Marshal()
	if err != nil {
		return
	}
	resp, err := mp.submit(opFSMCreateDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.ResultCode = resp.(uint8)
	return
}

func (mp *metaPartition) TxDeleteDentry(req *proto.TxDeleteDentryRequest, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogDentryOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.Name, req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Ino, req.ParentID)
		}()
	}
	txInfo := req.TxInfo.GetCopy()
	den := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}

	defer func() {
		if p.ResultCode == proto.OpOk {
			var reply []byte
			resp := &proto.TxDeleteDentryResponse{
				Inode: req.Ino,
			}
			reply, err = json.Marshal(resp)
			p.PacketOkWithBody(reply)
		}
	}()

	dentry, status := mp.getDentry(den)
	if status != proto.OpOk {
		if mp.txDentryInRb(req.ParentID, req.Name, req.TxInfo.TxID) {
			p.ResultCode = proto.OpOk
			log.LogWarnf("TxDeleteDentry: dentry is already been deleted before, req %v", req)
			return
		}

		err = fmt.Errorf("dentry[%v] not exists", den)
		log.LogWarn(err)
		p.PacketErrorWithBody(status, []byte(err.Error()))
		return
	}

	if dentry.Inode != req.Ino {
		err = fmt.Errorf("target name ino is not right, par %d, name %s, want %d, got %d",
			req.PartitionID, req.Name, req.Ino, dentry.Inode)
		log.LogWarn(err)
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}
	parIno := NewInode(req.ParentID, 0)
	inoResp := mp.getInode(parIno, false)
	if inoResp.Status != proto.OpOk {
		err = fmt.Errorf("parIno[%v] not exists", parIno.Inode)
		p.PacketErrorWithBody(inoResp.Status, []byte(err.Error()))
		return
	}

	txDentry := &TxDentry{
		// ParInode: inoResp.Msg,
		Dentry: dentry,
		TxInfo: txInfo,
	}

	val, err := txDentry.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	r, err := mp.submit(opFSMTxDeleteDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	retMsg := r.(*DentryResponse)
	p.ResultCode = retMsg.Status
	return
}

// DeleteDentry deletes a dentry.
func (mp *metaPartition) DeleteDentry(req *DeleteDentryReq, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogDentryOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.Name, req.GetFullPath(), err, time.Since(start).Milliseconds(), 0, req.ParentID)
		}()
	}
	if req.InodeCreateTime > 0 {
		if mp.vol.volDeleteLockTime > 0 && req.InodeCreateTime+mp.vol.volDeleteLockTime*60*60 > time.Now().Unix() {
			err = errors.NewErrorf("the current Inode[%v] is still locked for deletion", req.Name)
			log.LogDebugf("DeleteDentry: the current Inode is still locked for deletion, inode[%v] createTime(%v) mw.volDeleteLockTime(%v) now(%v)", req.Name, req.InodeCreateTime, mp.vol.volDeleteLockTime, time.Now().Unix())
			p.PacketErrorWithBody(proto.OpNotPerm, []byte(err.Error()))
			return
		}
	}
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	dentry.setVerSeq(req.Verseq)
	log.LogDebugf("action[DeleteDentry] den param(%v)", dentry)

	val, err := dentry.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	if mp.verSeq == 0 && dentry.getSeqFiled() > 0 {
		err = fmt.Errorf("snapshot not enabled")
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	log.LogDebugf("action[DeleteDentry] submit!")
	r, err := mp.submit(opFSMDeleteDentry, val)
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
func (mp *metaPartition) DeleteDentryBatch(req *BatchDeleteDentryReq, p *Packet, remoteAddr string) (err error) {
	db := make(DentryBatch, 0, len(req.Dens))
	start := time.Now()
	for i, d := range req.Dens {
		db = append(db, &Dentry{
			ParentId: req.ParentID,
			Name:     d.Name,
			Inode:    d.Inode,
			Type:     d.Type,
		})
		den := &d
		fullPath := ""
		if len(req.FullPaths) > i {
			fullPath = req.FullPaths[i]
		}
		if mp.IsEnableAuditLog() {
			defer func() {
				auditlog.LogDentryOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), den.Name, fullPath, err, time.Since(start).Milliseconds(), den.Inode, req.ParentID)
			}()
		}
	}

	val, err := db.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err := mp.submit(opFSMDeleteDentryBatch, val)
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
	p.PacketOkWithBody(reply)

	return
}

func (mp *metaPartition) TxUpdateDentry(req *proto.TxUpdateDentryRequest, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogDentryOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.Name, req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, req.ParentID)
		}()
	}
	if req.ParentID == req.Inode {
		err = fmt.Errorf("parentId is equal inodeId")
		p.PacketErrorWithBody(proto.OpExistErr, []byte(err.Error()))
		return
	}

	txInfo := req.TxInfo.GetCopy()

	defer func() {
		if p.ResultCode == proto.OpOk {
			var reply []byte
			m := &proto.TxUpdateDentryResponse{
				Inode: req.OldIno,
			}
			reply, _ = json.Marshal(m)
			p.PacketOkWithBody(reply)
		}
	}()

	newDentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
	}
	oldDentry, status := mp.getDentry(newDentry)
	if status != proto.OpOk {
		if mp.txDentryInRb(req.ParentID, req.Name, req.TxInfo.TxID) {
			p.ResultCode = proto.OpOk
			log.LogWarnf("TxDeleteDentry: dentry is already been deleted before, req %v", req)
			return
		}
		err = fmt.Errorf("oldDentry[%v] not exists", oldDentry)
		p.PacketErrorWithBody(status, []byte(err.Error()))
		return
	}

	if oldDentry.Inode != req.OldIno {
		err = fmt.Errorf("oldDentry is alredy updated, req %v, old [%v]", req, oldDentry)
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	}

	txDentry := &TxUpdateDentry{
		OldDentry: oldDentry,
		NewDentry: newDentry,
		TxInfo:    txInfo,
	}
	val, err := txDentry.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMTxUpdateDentry, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	msg := resp.(*DentryResponse)
	p.ResultCode = msg.Status
	return
}

// UpdateDentry updates a dentry.
func (mp *metaPartition) UpdateDentry(req *UpdateDentryReq, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogDentryOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.Name, req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, req.ParentID)
		}()
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
	dentry.setVerSeq(mp.verSeq)
	val, err := dentry.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMUpdateDentry, val)
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

func (mp *metaPartition) ReadDirOnly(req *ReadDirOnlyReq, p *Packet) (err error) {
	resp := mp.readDirOnly(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(reply)
	return
}

// ReadDir reads the directory based on the given request.
func (mp *metaPartition) ReadDir(req *ReadDirReq, p *Packet) (err error) {
	resp := mp.readDir(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(reply)
	return
}

func (mp *metaPartition) ReadDirLimit(req *ReadDirLimitReq, p *Packet) (err error) {
	log.LogInfof("action[ReadDirLimit] read seq [%v], request[%v]", req.VerSeq, req)
	resp := mp.readDirLimit(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
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
	dentry.setVerSeq(req.VerSeq)
	var denList []proto.DetryInfo
	if req.VerAll {
		denList = mp.getDentryList(dentry)
	}
	dentry, status := mp.getDentry(dentry)

	var reply []byte
	if status == proto.OpOk || req.VerAll {
		var resp *LookupResp
		if status == proto.OpOk {
			resp = &LookupResp{
				Inode:  dentry.Inode,
				Mode:   dentry.Type,
				VerSeq: dentry.getSeqFiled(),
				LayAll: denList,
			}
		} else {
			resp = &LookupResp{
				Inode:  0,
				Mode:   0,
				VerSeq: 0,
				LayAll: denList,
			}
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

// GetDentryTree returns the dentry tree stored in the meta partition.
func (mp *metaPartition) GetDentryTree() *BTree {
	return mp.dentryTree.GetTree()
}

// GetDentryTreeLen returns the dentry tree length.
func (mp *metaPartition) GetDentryTreeLen() int {
	if mp.dentryTree == nil {
		return 0
	}
	return mp.dentryTree.Len()
}
