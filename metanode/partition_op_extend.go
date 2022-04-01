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
	"github.com/chubaofs/chubaofs/util/exporter"
	"golang.org/x/net/context"

	"github.com/chubaofs/chubaofs/proto"
)

func (mp *metaPartition) SetXAttr(req *proto.SetXAttrRequest, p *Packet) (err error) {
	var (
		resp interface{}
	)

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	var extend = NewExtend(req.Inode)
	extend.Put([]byte(req.Key), []byte(req.Value))

	resp, err = mp.putExtend(p.Ctx(), opFSMSetXAttr, p.Remote(), extend)
	if  err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	if resp.(*proto.XAttrRaftResponse).Status != proto.OpOk {
		p.PacketErrorWithBody(resp.(*proto.XAttrRaftResponse).Status, []byte("raft set xAttr failed"))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) GetXAttr(req *proto.GetXAttrRequest, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	var response = &proto.GetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Key:         req.Key,
	}
	var extend *Extend
	extend, err = mp.extendTree.RefGet(req.Inode)
	if err != nil {
		if err == rocksDBError {
			exporter.WarningRocksdbError(fmt.Sprintf("action[GetXAttr] clusterID[%s] volumeName[%s] partitionID[%v]" +
				" get extend failed witch rocksdb error[Inode:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId, req.Inode))
		}
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	if extend != nil {
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
		var extend *Extend
		extend, err = mp.extendTree.RefGet(inode)
		if err != nil {
			if err == rocksDBError {
				exporter.WarningRocksdbError(fmt.Sprintf("action[BatchGetXAttr] clusterID[%s] volumeName[%s] partitionID[%v]" +
					" get extend failed witch rocksdb error[Inode:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
					mp.config.PartitionId, inode))
			}
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		if extend != nil {
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
	var (
		resp interface{}
	)

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	var extend = NewExtend(req.Inode)
	extend.Put([]byte(req.Key), nil)
	resp, err = mp.putExtend(p.Ctx(), opFSMRemoveXAttr, p.Remote(), extend)
	if  err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	if resp.(*proto.XAttrRaftResponse).Status != proto.OpOk {
		p.PacketErrorWithBody(resp.(*proto.XAttrRaftResponse).Status, nil)
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) ListXAttr(req *proto.ListXAttrRequest, p *Packet) (err error) {

	if _, err = mp.isInoOutOfRange(req.Inode); err != nil {
		p.PacketErrorWithBody(proto.OpInodeOutOfRange, []byte(err.Error()))
		return
	}

	var response = &proto.ListXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		XAttrs:      make([]string, 0),
	}
	var extend *Extend
	extend, err = mp.extendTree.RefGet(req.Inode)
	if err != nil {
		if err == rocksDBError {
			exporter.WarningRocksdbError(fmt.Sprintf("action[ListXAttr] clusterID[%s] volumeName[%s] partitionID[%v]" +
				" get extend failed witch rocksdb error[Inode:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId, req.Inode))
		}
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	if extend != nil {
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

func (mp *metaPartition) putExtend(ctx context.Context, op uint32, remote string, extend *Extend) (resp interface{}, err error) {
	var marshaled []byte
	if marshaled, err = extend.Bytes(); err != nil {
		return
	}
	resp, err = mp.submit(ctx, op, remote, marshaled)
	return
}
