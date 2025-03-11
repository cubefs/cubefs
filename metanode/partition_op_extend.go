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
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) UpdateXAttr(req *proto.UpdateXAttrRequest, p *Packet) (err error) {
	newValueList := strings.Split(req.Value, ",")
	if len(newValueList) != 7 {
		log.LogWarnf("UpdateXAttr: value is invalid, maybe version inconsistency, value=%s", req.Value)
		p.PacketErrorWithBody(proto.OpErr, []byte("version inconsistency"))
		return
	}
	filesHddInc, _ := strconv.ParseInt(newValueList[0], 10, 64)
	filesSsdInc, _ := strconv.ParseInt(newValueList[1], 10, 64)
	filesBlobStoreInc, _ := strconv.ParseInt(newValueList[2], 10, 64)
	bytesHddInc, _ := strconv.ParseInt(newValueList[3], 10, 64)
	bytesSsdInc, _ := strconv.ParseInt(newValueList[4], 10, 64)
	bytesBlobStoreInc, _ := strconv.ParseInt(newValueList[5], 10, 64)
	dirsInc, _ := strconv.ParseInt(newValueList[6], 10, 64)

	mp.xattrLock.Lock()
	defer mp.xattrLock.Unlock()
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		if value, exist := extend.Get([]byte(req.Key)); exist {
			oldValueList := strings.Split(string(value), ",")
			oldFilesHdd, _ := strconv.ParseInt(oldValueList[0], 10, 64)
			oldFilesSsd, _ := strconv.ParseInt(oldValueList[1], 10, 64)
			oldFilesBlobStore, _ := strconv.ParseInt(oldValueList[2], 10, 64)
			oldBytesHdd, _ := strconv.ParseInt(oldValueList[3], 10, 64)
			oldBytesSsd, _ := strconv.ParseInt(oldValueList[4], 10, 64)
			oldBytesBlobStore, _ := strconv.ParseInt(oldValueList[5], 10, 64)
			oldDirs, _ := strconv.ParseInt(oldValueList[6], 10, 64)

			newFilesHdd := oldFilesHdd + filesHddInc
			newFilesSsd := oldFilesSsd + filesSsdInc
			newFilesBlobStore := oldFilesBlobStore + filesBlobStoreInc
			newBytesHdd := oldBytesHdd + bytesHddInc
			newBytesSsd := oldBytesSsd + bytesSsdInc
			newBytesBlobStore := oldBytesBlobStore + bytesBlobStoreInc
			newDirs := oldDirs + dirsInc

			newValue := strconv.FormatInt(int64(newFilesHdd), 10) + "," +
				strconv.FormatInt(int64(newFilesSsd), 10) + "," +
				strconv.FormatInt(int64(newFilesBlobStore), 10) + "," +
				strconv.FormatInt(int64(newBytesHdd), 10) + "," +
				strconv.FormatInt(int64(newBytesSsd), 10) + "," +
				strconv.FormatInt(int64(newBytesBlobStore), 10) + "," +
				strconv.FormatInt(int64(newDirs), 10)

			extend := NewExtend(req.Inode)
			extend.Put([]byte(req.Key), []byte(newValue), mp.verSeq)
			if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			p.PacketOkReply()
			return
		} else {
			extend.Put([]byte(req.Key), []byte(req.Value), mp.verSeq)
			if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			p.PacketOkReply()
			return
		}
	} else {
		extend := NewExtend(req.Inode)
		extend.Put([]byte(req.Key), []byte(req.Value), mp.verSeq)
		if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		p.PacketOkReply()
		return
	}
}

func (mp *metaPartition) SetXAttr(req *proto.SetXAttrRequest, p *Packet) (err error) {
	extend := NewExtend(req.Inode)
	extend.Put([]byte(req.Key), []byte(req.Value), mp.verSeq)
	if _, err = mp.putExtend(opFSMSetXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) BatchSetXAttr(req *proto.BatchSetXAttrRequest, p *Packet) (err error) {
	extend := NewExtend(req.Inode)
	for key, val := range req.Attrs {
		extend.Put([]byte(key), []byte(val), mp.verSeq)
	}

	if _, err = mp.putExtend(opFSMSetXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) GetXAttr(req *proto.GetXAttrRequest, p *Packet) (err error) {
	response := &proto.GetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Key:         req.Key,
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		if extend := treeItem.(*Extend).GetExtentByVersion(req.VerSeq); extend != nil {
			if value, exist := extend.Get([]byte(req.Key)); exist {
				response.Value = string(value)
			}
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

func (mp *metaPartition) GetAllXAttr(req *proto.GetAllXAttrRequest, p *Packet) (err error) {
	response := &proto.GetAllXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Attrs:       make(map[string]string),
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		if extend := treeItem.(*Extend).GetExtentByVersion(req.VerSeq); extend != nil {
			for key, val := range extend.dataMap {
				response.Attrs[key] = string(val)
			}
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
	response := &proto.BatchGetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		XAttrs:      make([]*proto.XAttrInfo, 0, len(req.Inodes)),
	}
	for _, inode := range req.Inodes {
		treeItem := mp.extendTree.Get(NewExtend(inode))
		if treeItem != nil {
			info := &proto.XAttrInfo{
				Inode:  inode,
				XAttrs: make(map[string]string),
			}

			var extend *Extend
			if extend = treeItem.(*Extend).GetExtentByVersion(req.VerSeq); extend != nil {
				for _, key := range req.Keys {
					if val, exist := extend.Get([]byte(key)); exist {
						info.XAttrs[key] = string(val)
					}
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
	extend := NewExtend(req.Inode)
	extend.Put([]byte(req.Key), nil, req.VerSeq)
	if _, err = mp.putExtend(opFSMRemoveXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) ListXAttr(req *proto.ListXAttrRequest, p *Packet) (err error) {
	response := &proto.ListXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		XAttrs:      make([]string, 0),
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		if extend := treeItem.(*Extend).GetExtentByVersion(req.VerSeq); extend != nil {
			extend.Range(func(key, value []byte) bool {
				response.XAttrs = append(response.XAttrs, string(key))
				return true
			})
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

func (mp *metaPartition) putExtend(op uint32, extend *Extend) (resp interface{}, err error) {
	var marshaled []byte
	if marshaled, err = extend.Bytes(); err != nil {
		return
	}
	resp, err = mp.submit(op, marshaled)
	return
}

func (mp *metaPartition) LockDir(req *proto.LockDirRequest, p *Packet) (err error) {
	req.SubmitTime = time.Now()
	if req.LockId == 0 && req.Lease != 0 {
		req.LockId = proto.GenerateRequestID()
		log.LogWarnf("LockDir: req %s has empyt lkId from old verion.", req.String())
	}

	val, err := json.Marshal(req)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return err
	}

	r, err := mp.submit(opFSMLockDir, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return err
	}

	resp := r.(*proto.LockDirResponse)
	status := resp.Status
	var reply []byte
	reply, err = json.Marshal(resp)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
	}
	p.PacketErrorWithBody(status, reply)
	return
}
