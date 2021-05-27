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
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
)

func (mp *metaPartition) UpdateXAttr(req *proto.UpdateXAttrRequest, p *Packet) (err error) {
	newValueList := strings.Split(req.Value, ",")
	filesInc, _ := strconv.ParseInt(newValueList[0], 10, 64)
	dirsInc, _ := strconv.ParseInt(newValueList[1], 10, 64)
	bytesInc, _ := strconv.ParseInt(newValueList[2], 10, 64)

	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		if value, exist := extend.Get([]byte(req.Key)); exist {
			oldValueList := strings.Split(string(value), ",")
			oldFiles, _ := strconv.ParseInt(oldValueList[0], 10, 64)
			oldDirs, _ := strconv.ParseInt(oldValueList[1], 10, 64)
			oldBytes, _ := strconv.ParseInt(oldValueList[2], 10, 64)
			newFiles := oldFiles + filesInc
			newDirs := oldDirs + dirsInc
			newBytes := oldBytes + bytesInc
			newValue := strconv.FormatInt(int64(newFiles),10) + "," +
				strconv.FormatInt(int64(newDirs),10) + "," +
				strconv.FormatInt(int64(newBytes),10)
			var extend = NewExtend(req.Inode)
			extend.Put([]byte(req.Key), []byte(newValue))
			if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			p.PacketOkReply()
			return
		} else {
			extend.Put([]byte(req.Key), []byte(req.Value))
			if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			p.PacketOkReply()
			return
		}
	} else {
		var extend = NewExtend(req.Inode)
		extend.Put([]byte(req.Key), []byte(req.Value))
		if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		p.PacketOkReply()
		return
	}
	return
}

func (mp *metaPartition) SetXAttr(req *proto.SetXAttrRequest, p *Packet) (err error) {
	var extend = NewExtend(req.Inode)
	extend.Put([]byte(req.Key), []byte(req.Value))
	if _, err = mp.putExtend(opFSMSetXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) GetXAttr(req *proto.GetXAttrRequest, p *Packet) (err error) {
	var response = &proto.GetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Key:         req.Key,
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		if value, exist := extend.Get([]byte(req.Key)); exist {
			response.Value = string(value)
		}
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) BatchGetXAttr(req *proto.BatchGetXAttrRequest, p *Packet) (err error) {
	var response = &proto.BatchGetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		XAttrs:      make([]*proto.XAttrInfo, 0, len(req.Inodes)),
	}
	for _, inode := range req.Inodes {
		treeItem := mp.extendTree.Get(NewExtend(inode))
		if treeItem != nil {
			extend := treeItem.(*Extend)
			info := &proto.XAttrInfo{
				Inode:  inode,
				XAttrs: make(map[string]string),
			}
			for _, key := range req.Keys {
				if val, exist := extend.Get([]byte(key)); exist {
					info.XAttrs[key] = string(val)
				}
			}
			response.XAttrs = append(response.XAttrs, info)
		}
	}
	var encoded []byte
	if encoded, err = json.Marshal(response); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) RemoveXAttr(req *proto.RemoveXAttrRequest, p *Packet) (err error) {
	var extend = NewExtend(req.Inode)
	extend.Put([]byte(req.Key), nil)
	if _, err = mp.putExtend(opFSMRemoveXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) ListXAttr(req *proto.ListXAttrRequest, p *Packet) (err error) {
	var response = &proto.ListXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		XAttrs:      make([]string, 0),
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		extend.Range(func(key, value []byte) bool {
			response.XAttrs = append(response.XAttrs, string(key))
			return true
		})
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) putExtend(op uint32, extend *Extend) (resp interface{}, err error) {
	var marshaled []byte
	if marshaled, err = extend.Bytes(); err != nil {
		return
	}
	resp, err = mp.submit(op, marshaled)
	return
}
