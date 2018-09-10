// Copyright 2018 The ChuBao Authors.
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

	"encoding/binary"
	"github.com/tiglabs/containerfs/proto"
)

func (mp *metaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ino.Extents.Put(req.Extent)
	val, err := ino.Marshal()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opExtentsAdd, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PackErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) ExtentsList(req *proto.GetExtentsRequest,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	retMsg := mp.getInode(ino)
	ino = retMsg.Msg
	var (
		reply  []byte
		status = retMsg.Status
	)
	if status == proto.OpOk {
		resp := &proto.GetExtentsResponse{}
		ino.Extents.Range(func(i int, ext proto.ExtentKey) bool {
			resp.Extents = append(resp.Extents, ext)
			return true
		})
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) ExtentsTruncate(req *ExtentsTruncateReq,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, proto.ModeRegular)
	nextIno, err := mp.nextInodeID()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	ino.LinkTarget = make([]byte, 8)
	binary.BigEndian.PutUint64(ino.LinkTarget, nextIno)
	val, err := ino.Marshal()
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opFSMExtentTruncate, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*ResponseInode)
	p.PackErrorWithBody(msg.Status, nil)
	return
}
