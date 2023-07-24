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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func replyInfo(info *proto.InodeInfo, ino *Inode, quotaIds []uint32) bool {
	ino.RLock()
	defer ino.RUnlock()
	if ino.Flag&DeleteMarkFlag > 0 {
		return false
	}
	info.Inode = ino.Inode
	info.Mode = ino.Type
	info.Size = ino.Size
	info.Nlink = ino.NLink
	info.Uid = ino.Uid
	info.Gid = ino.Gid
	info.Generation = ino.Generation
	if length := len(ino.LinkTarget); length > 0 {
		info.Target = make([]byte, length)
		copy(info.Target, ino.LinkTarget)
	}
	info.CreateTime = time.Unix(ino.CreateTime, 0)
	info.AccessTime = time.Unix(ino.AccessTime, 0)
	info.ModifyTime = time.Unix(ino.ModifyTime, 0)
	info.QuotaIds = quotaIds
	return true
}

func txReplyInfo(inode *Inode, txInfo *proto.TransactionInfo) (resp *proto.TxCreateInodeResponse) {
	inoInfo := &proto.InodeInfo{
		Inode:      inode.Inode,
		Mode:       inode.Type,
		Nlink:      inode.NLink,
		Size:       inode.Size,
		Uid:        inode.Uid,
		Gid:        inode.Gid,
		Generation: inode.Generation,
		ModifyTime: time.Unix(inode.ModifyTime, 0),
		CreateTime: time.Unix(inode.CreateTime, 0),
		AccessTime: time.Unix(inode.AccessTime, 0),
		Target:     nil,
	}
	if length := len(inode.LinkTarget); length > 0 {
		inoInfo.Target = make([]byte, length)
		copy(inoInfo.Target, inode.LinkTarget)
	}

	resp = &proto.TxCreateInodeResponse{
		Info:   inoInfo,
		TxInfo: txInfo,
	}
	return
}

// CreateInode returns a new inode.
func (mp *metaPartition) CreateInode(req *CreateInoReq, p *Packet) (err error) {
	var (
		status = proto.OpNotExistErr
		reply  []byte
		resp   interface{}
		qinode *MetaQuotaInode
	)
	inoID, err := mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}
	ino := NewInode(inoID, req.Mode)
	ino.Uid = req.Uid
	ino.Gid = req.Gid
	ino.LinkTarget = req.Target

	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return err
	}
	resp, err = mp.submit(opFSMCreateInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	if resp.(uint8) == proto.OpOk {
		resp := &CreateInoResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, ino, make([]uint32, 0)) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
	}
	p.PacketErrorWithBody(status, reply)
	log.LogInfof("CreateInode req [%v] qinode [%v] success.", req, qinode)
	return
}

func (mp *metaPartition) QuotaCreateInode(req *proto.QuotaCreateInodeRequest, p *Packet) (err error) {
	var (
		status = proto.OpNotExistErr
		reply  []byte
		resp   interface{}
		qinode *MetaQuotaInode
	)
	inoID, err := mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}
	ino := NewInode(inoID, req.Mode)
	ino.Uid = req.Uid
	ino.Gid = req.Gid
	ino.LinkTarget = req.Target

	for _, quotaId := range req.QuotaIds {
		status = mp.mqMgr.IsOverQuota(false, true, quotaId)
		if status != 0 {
			err = errors.New("create inode is over quota")
			reply = []byte(err.Error())
			p.PacketErrorWithBody(status, reply)
			return
		}
	}
	qinode = &MetaQuotaInode{
		inode:    ino,
		quotaIds: req.QuotaIds,
	}
	val, err := qinode.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return err
	}
	resp, err = mp.submit(opFSMCreateInodeQuota, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	if resp.(uint8) == proto.OpOk {
		resp := &CreateInoResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, ino, req.QuotaIds) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
	}
	p.PacketErrorWithBody(status, reply)
	log.LogInfof("QuotaCreateInode req [%v] qinode [%v] success.", req, qinode)
	return
}

func (mp *metaPartition) TxUnlinkInode(req *proto.TxUnlinkInodeRequest, p *Packet) (err error) {

	txInfo := req.TxInfo.GetCopy()
	var status uint8
	var respIno *Inode

	defer func() {
		var reply []byte
		if status == proto.OpOk {
			resp := &proto.TxUnlinkInodeResponse{
				Info: &proto.InodeInfo{},
			}

			replyInfo(resp.Info, respIno, make([]uint32, 0))
			if reply, err = json.Marshal(resp); err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
		p.PacketErrorWithBody(status, reply)
	}()

	ino := NewInode(req.Inode, 0)
	inoResp := mp.getInode(ino)
	if inoResp.Status != proto.OpOk {
		if rbIno := mp.txInodeInRb(req.Inode, req.TxInfo.TxID); rbIno {
			status = proto.OpOk
			item := mp.inodeTree.Get(NewInode(req.Inode, 0))
			if item != nil {
				respIno = item.(*Inode)
			}

			log.LogWarnf("TxUnlinkInode: inode is already unlink before, req %v, rbIno %s", req, respIno.String())
			return nil
		}

		err = fmt.Errorf("ino[%v] not exists", ino.Inode)
		p.PacketErrorWithBody(inoResp.Status, []byte(err.Error()))
		return
	}

	respIno = inoResp.Msg
	ti := &TxInode{
		Inode:  inoResp.Msg,
		TxInfo: txInfo,
	}

	val, err := ti.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	r, err := mp.submit(opFSMTxUnlinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	msg := r.(*InodeResponse)
	status = msg.Status
	if msg.Msg != nil {
		respIno = msg.Msg
	}
	return
}

// DeleteInode deletes an inode.
func (mp *metaPartition) UnlinkInode(req *UnlinkInoReq, p *Packet) (err error) {
	var r interface{}
	var val []byte
	if req.UniqID > 0 {
		val = InodeOnceUnlinkMarshal(req)
		r, err = mp.submit(opFSMUnlinkInodeOnce, val)
	} else {
		ino := NewInode(req.Inode, 0)
		val, err = ino.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		r, err = mp.submit(opFSMUnlinkInode, val)
	}

	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := r.(*InodeResponse)
	status := msg.Status
	var reply []byte
	if status == proto.OpOk {
		resp := &UnlinkInoResp{
			Info: &proto.InodeInfo{},
		}
		replyInfo(resp.Info, msg.Msg, make([]uint32, 0))
		if reply, err = json.Marshal(resp); err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// DeleteInode deletes an inode.
func (mp *metaPartition) UnlinkInodeBatch(req *BatchUnlinkInoReq, p *Packet) (err error) {

	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	val, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err := mp.submit(opFSMUnlinkInodeBatch, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	result := &BatchUnlinkInoResp{}
	status := proto.OpOk
	for _, ir := range r.([]*InodeResponse) {
		if ir.Status != proto.OpOk {
			status = ir.Status
		}

		info := &proto.InodeInfo{}
		replyInfo(info, ir.Msg, make([]uint32, 0))
		result.Items = append(result.Items, &struct {
			Info   *proto.InodeInfo `json:"info"`
			Status uint8            `json:"status"`
		}{
			Info:   info,
			Status: ir.Status,
		})
	}

	reply, err := json.Marshal(result)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// InodeGet executes the inodeGet command from the client.
func (mp *metaPartition) InodeGet(req *InodeGetReq, p *Packet) (err error) {
	var (
		reply    []byte
		status   = proto.OpNotExistErr
		quotaIds []uint32
	)
	if mp.mqMgr.EnableQuota() {
		quotaIds, err = mp.getInodeQuotaIds(req.Inode)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
			p.PacketErrorWithBody(status, reply)
			return
		}
	}
	ino := NewInode(req.Inode, 0)
	retMsg := mp.getInode(ino)
	ino = retMsg.Msg
	if retMsg.Status == proto.OpOk {
		resp := &proto.InodeGetResponse{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, retMsg.Msg, quotaIds) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// InodeGetBatch executes the inodeBatchGet command from the client.
func (mp *metaPartition) InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error) {
	resp := &proto.BatchInodeGetResponse{}
	ino := NewInode(0, 0)
	for _, inoId := range req.Inodes {
		var quotaIds []uint32
		ino.Inode = inoId
		retMsg := mp.getInode(ino)
		if mp.mqMgr.EnableQuota() {
			quotaIds, err = mp.getInodeQuotaIds(inoId)
			if err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
		}
		if retMsg.Status == proto.OpOk {
			inoInfo := &proto.InodeInfo{}
			if replyInfo(inoInfo, retMsg.Msg, quotaIds) {
				resp.Infos = append(resp.Infos, inoInfo)
			}
		}
	}
	data, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (mp *metaPartition) TxCreateInodeLink(req *proto.TxLinkInodeRequest, p *Packet) (err error) {
	txInfo := req.TxInfo.GetCopy()
	ino := NewInode(req.Inode, 0)
	inoResp := mp.getInode(ino)
	if inoResp.Status != proto.OpOk {
		err = fmt.Errorf("ino[%v] not exists", ino.Inode)
		p.PacketErrorWithBody(inoResp.Status, []byte(err.Error()))
		return
	}

	ti := &TxInode{
		Inode:  inoResp.Msg,
		TxInfo: txInfo,
	}

	val, err := ti.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	resp, err := mp.submit(opFSMTxCreateLinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	retMsg := resp.(*InodeResponse)
	status := retMsg.Status
	var reply []byte
	if retMsg.Status == proto.OpOk {
		resp := &proto.TxLinkInodeResponse{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, retMsg.Msg, make([]uint32, 0)) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// CreateInodeLink creates an inode link (e.g., soft link).
func (mp *metaPartition) CreateInodeLink(req *LinkInodeReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMCreateLinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	retMsg := resp.(*InodeResponse)
	status := proto.OpNotExistErr
	var reply []byte
	if retMsg.Status == proto.OpOk {
		resp := &LinkInodeResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, retMsg.Msg, make([]uint32, 0)) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}

	}
	p.PacketErrorWithBody(status, reply)
	return
}

// EvictInode evicts an inode.
func (mp *metaPartition) EvictInode(req *EvictInodeReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMEvictInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

// EvictInode evicts an inode.
func (mp *metaPartition) EvictInodeBatch(req *BatchEvictInodeReq, p *Packet) (err error) {

	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	val, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMEvictInodeBatch, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	status := proto.OpOk
	for _, m := range resp.([]*InodeResponse) {
		if m.Status != proto.OpOk {
			status = m.Status
		}
	}

	p.PacketErrorWithBody(status, nil)
	return
}

// SetAttr set the inode attributes.
func (mp *metaPartition) SetAttr(reqData []byte, p *Packet) (err error) {
	_, err = mp.submit(opFSMSetAttr, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

// GetInodeTree returns the inode tree.
func (mp *metaPartition) GetInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

// GetInodeTreeLen returns the inode tree length.
func (mp *metaPartition) GetInodeTreeLen() int {
	if mp.inodeTree == nil {
		return 0
	}
	return mp.inodeTree.Len()
}

func (mp *metaPartition) DeleteInode(req *proto.DeleteInodeRequest, p *Packet) (err error) {
	var bytes = make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, req.Inode)
	_, err = mp.submit(opFSMInternalDeleteInode, bytes)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) DeleteInodeBatch(req *proto.DeleteInodeBatchRequest, p *Packet) (err error) {
	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	encoded, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	_, err = mp.submit(opFSMInternalDeleteInodeBatch, encoded)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

// ClearInodeCache clear a inode's cbfs extent but keep ebs extent.
func (mp *metaPartition) ClearInodeCache(req *proto.ClearInodeCacheRequest, p *Packet) (err error) {
	if len(mp.extDelCh) > defaultDelExtentsCnt-100 {
		err = fmt.Errorf("extent del chan full")
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMClearInodeCache, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// TxCreateInode returns a new inode.
func (mp *metaPartition) TxCreateInode(req *proto.TxCreateInodeRequest, p *Packet) (err error) {
	var (
		status = proto.OpNotExistErr
		reply  []byte
		resp   interface{}
	)

	inoID, err := mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}

	req.TxInfo.SetCreateInodeId(inoID)
	createTxReq := &proto.TxCreateRequest{
		VolName:         req.VolName,
		PartitionID:     req.PartitionID,
		TransactionInfo: req.TxInfo,
	}
	err = mp.TxCreate(createTxReq, p)
	if err != nil || p.ResultCode != proto.OpOk {
		return
	}

	createResp := &proto.TxCreateResponse{}
	err = json.Unmarshal(p.Data, createResp)
	if err != nil || createResp.TxInfo == nil {
		err = fmt.Errorf("TxCreateInode: unmarshal txInfo failed, data %s, err %v", string(p.Data), err)
		log.LogWarn(err)
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	txIno := NewTxInode(inoID, req.Mode, createResp.TxInfo)
	txIno.Inode.Uid = req.Uid
	txIno.Inode.Gid = req.Gid
	txIno.Inode.LinkTarget = req.Target

	if log.EnableDebug() {
		log.LogDebugf("NewTxInode: TxInode: %v", txIno)
	}

	if defaultQuotaSwitch {
		for _, quotaId := range req.QuotaIds {
			status = mp.mqMgr.IsOverQuota(false, true, quotaId)
			if status != 0 {
				err = errors.New("tx create inode is over quota")
				reply = []byte(err.Error())
				p.PacketErrorWithBody(status, reply)
				return
			}
		}

		qinode := &TxMetaQuotaInode{
			txinode:  txIno,
			quotaIds: req.QuotaIds,
		}
		val, err := qinode.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp, err = mp.submit(opFSMTxCreateInodeQuota, val)
		if err != nil {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
			return err
		}
	} else {
		val, err := txIno.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp, err = mp.submit(opFSMTxCreateInode, val)
		if err != nil {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
			return err
		}
	}

	if resp == proto.OpOk {
		resp := txReplyInfo(txIno.Inode, createResp.TxInfo)
		status = proto.OpOk
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}
