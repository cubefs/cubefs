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
	"os"

	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/statistics"
)

// ExtentAppend appends an extent.
func (mp *metaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	ino := NewInode(req.Inode, 0)
	ino.Flag = 0
	if req.IsPreExtent {
		ino.Flag = proto.CheckPreExtentExist
	}
	ext := req.Extent
	ino.Extents.Append(p.Ctx(), ext, ino.Inode)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(p.Ctx(), opFSMExtentsAdd, p.RemoteWithReqID(), val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) ExtentInsert(req *proto.InsertExtentKeyRequest, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	ino := NewInode(req.Inode, 0)
	ext := req.Extent
	ino.Flag = 0
	if req.IsPreExtent {
		ino.Flag = proto.CheckPreExtentExist
	}
	ino.Extents.Insert(p.Ctx(), ext, ino.Inode)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(p.Ctx(), opFSMExtentsInsert, p.RemoteWithReqID(), val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// ExtentsList returns the list of extents.
func (mp *metaPartition) ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	mp.monitorData[statistics.ActionMetaExtentsList].UpdateData(0)

	var retMsg *InodeResponse
	retMsg, err = mp.getInode(NewInode(req.Inode, 0))
	if err != nil {
		p.PacketErrorWithBody(retMsg.Status, []byte(err.Error()))
		return
	}
	ino := retMsg.Msg
	var (
		reply  []byte
		status = retMsg.Status
	)
	if status == proto.OpOk {
		resp := &proto.GetExtentsResponse{}
		ino.DoReadFunc(func() {

			resp.Generation = ino.Generation
			resp.Size = ino.Size
			ino.Extents.Range(func(ek proto.ExtentKey) bool {
				resp.Extents = append(resp.Extents, ek)
				return true
			})
		})
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// ExtentsTruncate truncates an extent.
func (mp *metaPartition) ExtentsTruncate(req *ExtentsTruncateReq, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	log.LogDebugf("partition(%v) extents truncate (reqID: %v, inode: %v, version %v, oldSize %v, size: %v)",
		mp.config.PartitionId, p.ReqID, req.Inode, req.Version, req.OldSize, req.Size)

	ino := NewInode(req.Inode, proto.Mode(os.ModePerm))
	ino.Size = req.Size
	// we use CreateTime store req.Version in opFSMExtentTruncate request
	ino.CreateTime = int64(req.Version)
	// we use AccessTime store req.OldSize in opFSMExtentTruncate request
	ino.AccessTime = int64(req.OldSize)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(p.Ctx(), opFSMExtentTruncate, p.RemoteWithReqID(), val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

func (mp *metaPartition) BatchExtentAppend(req *proto.AppendExtentKeysRequest, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	ino := NewInode(req.Inode, 0)
	ino.Flag = 0
	if req.IsPreExtent {
		ino.Flag = proto.CheckPreExtentExist
	}
	extents := req.Extents
	for _, extent := range extents {
		ino.Extents.Append(p.Ctx(), extent, ino.Inode)
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(p.Ctx(), opFSMExtentsAdd, p.RemoteWithReqID(), val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) MergeExtents(req *proto.InodeMergeExtentsRequest, p *Packet) (err error) {
	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}
	if len(req.NewExtents) == 0 || len(req.OldExtents) <= 1 {
		msg := fmt.Sprintf("inode(%v) newExtents length(%v) oldExtents length(%v)", req.Inode, len(req.NewExtents), len(req.OldExtents))
		p.PacketErrorWithBody(proto.OpArgMismatchErr, []byte(msg))
		return
	}
	im := &InodeMerge{
		Inode: req.Inode,
		NewExtents: req.NewExtents,
		OldExtents: req.OldExtents,
	}

	val, err := im.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(p.Ctx(), opFSMExtentMerge, p.RemoteWithReqID(), val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}
