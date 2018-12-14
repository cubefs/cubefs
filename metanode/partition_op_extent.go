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
	"encoding/json"

	"github.com/tiglabs/containerfs/proto"
	"os"
)

func (mp *metaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ext := req.Extent
	ino.Extents.Append(&ext)
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
		resp.Generation = ino.Generation
		resp.Size = ino.Size
		ino.Extents.Range(func(item BtreeItem) bool {
			ext := item.(*proto.ExtentKey)
			resp.Extents = append(resp.Extents, *ext)
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
	ino := NewInode(req.Inode, proto.Mode(os.ModePerm))
	ino.Size = req.Size
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
