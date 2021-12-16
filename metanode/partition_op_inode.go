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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/tracing"
	"github.com/chubaofs/chubaofs/util/statistics"
)

func replyInfo(info *proto.InodeInfo, ino *Inode) bool {
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
	return true
}

// CreateInode returns a new inode.
func (mp *metaPartition) CreateInode(req *CreateInoReq, p *Packet) (err error) {
	var (
		tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.CreateInode")
		inoID uint64
		val []byte
		resp interface{}
	)
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	inoID, err = mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}
	ino := NewInode(inoID, req.Mode)
	ino.Uid = req.Uid
	ino.Gid = req.Gid
	ino.LinkTarget = req.Target
	val, err = ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err = mp.submit(p.Ctx(), opFSMCreateInode, p.Remote(), val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	var (
		status = proto.OpNotExistErr
		reply  []byte
	)
	if resp.(uint8) == proto.OpOk {
		resp := &CreateInoResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, ino) {
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

// DeleteInode deletes an inode.
func (mp *metaPartition) UnlinkInode(req *UnlinkInoReq, p *Packet) (err error) {
	var (
		tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.UnlinkInode")
		r interface{}
		val []byte
	)
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	if err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	isExist, ino := mp.hasInode(&Inode{Inode: req.Inode})
	if !isExist {
		ino = NewInode(req.Inode, 0)
	}

	val, err = ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err = mp.submit(p.Ctx(), opFSMUnlinkInode, p.Remote(), val)
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
		replyInfo(resp.Info, msg.Msg)
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
	var (
		tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.UnlinkInodeBatch")
		r interface{}
		reply []byte
		val []byte
	)
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	val, err = inodes.Marshal(p.Ctx())
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err = mp.submit(p.Ctx(), opFSMUnlinkInodeBatch, p.Remote(), val)
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
		replyInfo(info, ir.Msg)
		result.Items = append(result.Items, &struct {
			Info   *proto.InodeInfo `json:"info"`
			Status uint8            `json:"status"`
		}{
			Info:   info,
			Status: ir.Status,
		})
	}

	reply, err = json.Marshal(result)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// InodeGet executes the inodeGet command from the client.
func (mp *metaPartition) InodeGet(req *InodeGetReq, p *Packet, version uint8) (err error) {
	var tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.InodeGet")
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	mp.monitorData[statistics.ActionMetaInodeGet].UpdateData(0)
	var (
		reply  []byte
	)

	ino := NewInode(req.Inode, 0)
	retMsg := mp.getInode(ino)
	if version == proto.OpInodeGetVersion1 && retMsg.Status == proto.OpInodeOutOfRange {
		retMsg.Status = proto.OpNotExistErr
	}

	if retMsg.Status != proto.OpOk {
		p.PacketErrorWithBody(retMsg.Status, []byte("get inode err"))
		return fmt.Errorf("errCode:%d, ino:%v, mp has inodes[%v, %v]\n",
				retMsg.Status, req.Inode, mp.config.Start, mp.config.Cursor)
	}

	status := proto.OpOk
	resp := &proto.InodeGetResponse{
		Info: &proto.InodeInfo{},
	}

	if replyInfo(resp.Info, retMsg.Msg) {
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

// InodeGetBatch executes the inodeBatchGet command from the client.
func (mp *metaPartition) InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error) {
	var (
		tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.InodeGetBatch")
		data []byte
	)
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	mp.monitorData[statistics.ActionMetaBatchInodeGet].UpdateData(0)

	resp := &proto.BatchInodeGetResponse{}
	ino := NewInode(0, 0)
	for _, inoId := range req.Inodes {
		ino.Inode = inoId
		retMsg := mp.getInode(ino)
		if retMsg.Status == proto.OpOk {
			inoInfo := &proto.InodeInfo{}
			if replyInfo(inoInfo, retMsg.Msg) {
				resp.Infos = append(resp.Infos, inoInfo)
			}
		}
	}
	data, err = json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(data)
	return
}

// CreateInodeLink creates an inode link (e.g., soft link).
func (mp *metaPartition) CreateInodeLink(req *LinkInodeReq, p *Packet) (err error) {
	var (
		tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.CreateInodeLink")
		resp interface{}
		val []byte
	)
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	if err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	isExist, ino := mp.hasInode(&Inode{Inode: req.Inode})
	if !isExist {
		ino = NewInode(req.Inode, 0)
	}

	val, err = ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err = mp.submit(p.Ctx(), opFSMCreateLinkInode, p.Remote(), val)
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
		if replyInfo(resp.Info, retMsg.Msg) {
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
	var (
		tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.EvictInode")
		resp interface{}
		val []byte
	)
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	if err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	ino := NewInode(req.Inode, 0)
	val, err = ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err = mp.submit(p.Ctx(), opFSMEvictInode, p.Remote(), val)
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
	var (
		tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.EvictInodeBatch")
		resp interface{}
	)
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	val, err := inodes.Marshal(p.Ctx())
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err = mp.submit(p.Ctx(), opFSMEvictInodeBatch, p.Remote(), val)
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
	var (
		tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.SetAttr")
		resp interface{}
	)
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	resp, err = mp.submit(p.Ctx(), opFSMSetAttr, p.Remote(), reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	if (resp.(*InodeResponse)).Status != proto.OpOk {
		p.PacketErrorWithBody(resp.(*InodeResponse).Status, []byte("Apply set attr failed"))
		return
	}

	p.PacketOkReply()
	return
}

// GetInodeTree returns the inode tree.
func (mp *metaPartition) GetInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

func (mp *metaPartition) DeleteInode(req *proto.DeleteInodeRequest, p *Packet) (err error) {
	var tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.DeleteInode")
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	if err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	var bytes = make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, req.Inode)
	_, err = mp.submit(p.Ctx(), opFSMInternalDeleteInode, p.Remote(), bytes)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) CursorReset(ctx context.Context, req *proto.CursorResetRequest) (uint64, error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("metaPartition.CursorReset")
	defer tracer.Finish()
	ctx = tracer.Context()
	maxIno := mp.config.Start
	maxInode := mp.inodeTree.MaxItem()
	if maxInode != nil {
		maxIno = maxInode.(*Inode).Inode
	}

	req.Cursor = atomic.LoadUint64(&mp.config.Cursor)

	status, _ := mp.calcMPStatus()
	if status != proto.ReadOnly {
		log.LogInfof("mp[%v] status[%d] is not readonly, can not reset cursor[%v]",
			mp.config.PartitionId, status, mp.config.Cursor)
		return mp.config.Cursor, fmt.Errorf("mp[%v] status[%d] is not readonly, can not reset cursor[%v]",
			mp.config.PartitionId, status, mp.config.Cursor)
	}

	if req.Inode == 0 {
		req.Inode = maxIno + mpResetInoStep
	}

	if req.Inode <= maxIno || req.Inode >= mp.config.End {
		log.LogInfof("mp[%v] req[%d] ino is out of max[%d]~end[%d]",
			mp.config.PartitionId, req.Inode, maxIno, mp.config.End)
		return mp.config.Cursor, fmt.Errorf("mp[%v] req[%d] ino is out of max[%d]~end[%d]",
			mp.config.PartitionId, req.Inode, maxIno, mp.config.End)
	}

	willFree := mp.config.End - req.Inode
	if !req.Force && willFree < mpResetInoLimited {
		log.LogInfof("mp[%v] max inode[%v] is too high, no need reset",
			mp.config.PartitionId, maxIno)
		return mp.config.Cursor, fmt.Errorf("mp[%v] max inode[%v] is too high, no need reset",
									mp.config.PartitionId, maxIno)
	}

	data, err := json.Marshal(req)
	if err != nil {
		log.LogInfof("mp[%v] reset cursor failed, json marshal failed:%v",
			mp.config.PartitionId, err.Error())
		return mp.config.Cursor, err
	}
	cursor, err := mp.submit(ctx, opFSMCursorReset, "", data)
	if err != nil {
		return mp.config.Cursor, err
	}
	return cursor.(uint64), nil
}

func (mp *metaPartition) DeleteInodeBatch(req *proto.DeleteInodeBatchRequest, p *Packet) (err error) {
	var tracer = tracing.TracerFromContext(p.Ctx()).ChildTracer("metaPartition.DeleteInodeBatch")
	defer tracer.Finish()
	p.SetCtx(tracer.Context())

	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch

	for _, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
	}

	encoded, err := inodes.Marshal(p.Ctx())
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	_, err = mp.submit(p.Ctx(), opFSMInternalDeleteInodeBatch, p.Remote(), encoded)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}
