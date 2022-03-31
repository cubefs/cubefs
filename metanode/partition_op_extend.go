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

	"github.com/cubefs/cubefs/proto"
)

func (mp *metaPartition) UpdateSummaryInfo(req *proto.UpdateSummaryInfoRequest, p *Packet) (err error) {
	fileInc := req.FileInc
	dirInc := req.DirInc
	byteInc := req.ByteInc
	var builder strings.Builder

	mp.summaryLock.Lock()
	defer mp.summaryLock.Unlock()
	treeItem := mp.extendTree.GetForRead(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		if value, exist := extend.Get([]byte(req.Key)); exist {
			oldValueList := strings.Split(string(value), ",")
			oldFile, _ := strconv.ParseInt(oldValueList[0], 10, 64)
			oldDir, _ := strconv.ParseInt(oldValueList[1], 10, 64)
			oldByte, _ := strconv.ParseInt(oldValueList[2], 10, 64)
			newFile := oldFile + fileInc
			newDir := oldDir + dirInc
			newByte := oldByte + byteInc
			builder.Reset()
			builder.WriteString(strconv.FormatInt(newFile, 10))
			builder.WriteString(",")
			builder.WriteString(strconv.FormatInt(newDir, 10))
			builder.WriteString(",")
			builder.WriteString(strconv.FormatInt(newByte, 10))
			var extend = NewExtend(req.Inode)
			extend.Put([]byte(req.Key), []byte(builder.String()))
			if _, err = mp.putExtend(opFSMUpdateSummaryInfo, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			p.PacketOkReply()
			return
		} else {
			builder.Reset()
			builder.WriteString(strconv.FormatInt(req.FileInc, 10))
			builder.WriteString(",")
			builder.WriteString(strconv.FormatInt(req.DirInc, 10))
			builder.WriteString(",")
			builder.WriteString(strconv.FormatInt(req.ByteInc, 10))
			extend.Put([]byte(req.Key), []byte(builder.String()))
			if _, err = mp.putExtend(opFSMUpdateSummaryInfo, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			p.PacketOkReply()
			return
		}
	} else {
		builder.Reset()
		builder.WriteString(strconv.FormatInt(req.FileInc, 10))
		builder.WriteString(",")
		builder.WriteString(strconv.FormatInt(req.DirInc, 10))
		builder.WriteString(",")
		builder.WriteString(strconv.FormatInt(req.ByteInc, 10))
		var extend = NewExtend(req.Inode)
		extend.Put([]byte(req.Key), []byte(builder.String()))
		if _, err = mp.putExtend(opFSMUpdateSummaryInfo, extend); err != nil {
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
	treeItem := mp.extendTree.GetForRead(NewExtend(req.Inode))
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
		treeItem := mp.extendTree.GetForRead(NewExtend(inode))
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
	treeItem := mp.extendTree.GetForRead(NewExtend(req.Inode))
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
