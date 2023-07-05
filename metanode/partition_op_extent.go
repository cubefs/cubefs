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
	"os"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) CheckQuota(inodeId uint64, p *Packet) (iParm *Inode, inode *Inode, err error) {
	iParm = NewInode(inodeId, 0)
	status := mp.isOverQuota(inodeId, true, false)
	if status != 0 {
		log.LogErrorf("CheckQuota dir quota fail inode [%v] status [%v]", inodeId, status)
		err = errors.New("CheckQuota dir quota is over quota")
		reply := []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}

	item := mp.inodeTree.Get(iParm)
	if item == nil {
		err = fmt.Errorf("inode[%v] not exist", iParm)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	inode = item.(*Inode)
	mp.uidManager.acLock.Lock()
	if mp.uidManager.getUidAcl(inode.Uid) {
		log.LogWarnf("CheckQuota UidSpace.vol %v mp[%v] uid %v be set full", mp.uidManager.mpID, mp.uidManager.volName, inode.Uid)
		mp.uidManager.acLock.Unlock()
		status = proto.OpNoSpaceErr
		err = errors.New("CheckQuota UidSpace is over quota")
		reply := []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}
	mp.uidManager.acLock.Unlock()
	return
}

// ExtentAppend appends an extent.
func (mp *metaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
	if !proto.IsHot(mp.volType) {
		err = fmt.Errorf("only support hot vol")
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	ino := NewInode(req.Inode, 0)
	if _, _, err = mp.CheckQuota(req.Inode, p); err != nil {
		log.LogErrorf("ExtentAppend fail status [%v]", err)
		return
	}
	ext := req.Extent
	ino.Extents.Append(ext)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMExtentsAdd, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// ExtentAppendWithCheck appends an extent with discard extents check.
// Format: one valid extent key followed by non or several discard keys.
func (mp *metaPartition) ExtentAppendWithCheck(req *proto.AppendExtentKeyWithCheckRequest, p *Packet) (err error) {
	status := mp.isOverQuota(req.Inode, true, false)
	if status != 0 {
		log.LogErrorf("ExtentAppendWithCheck fail status [%v]", status)
		err = errors.New("ExtentAppendWithCheck is over quota")
		reply := []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}
	var (
		inoParm *Inode
		i       *Inode
	)
	if inoParm, i, err = mp.CheckQuota(req.Inode, p); err != nil {
		log.LogErrorf("ExtentAppendWithCheck CheckQuota fail err [%v]", err)
		return
	}

	// check volume's Type: if volume's type is cold, cbfs' extent can be modify/add only when objextent exist
	if proto.IsCold(mp.volType) {
		i.RLock()
		exist, idx := i.ObjExtents.FindOffsetExist(req.Extent.FileOffset)
		if !exist {
			i.RUnlock()
			err = fmt.Errorf("ebs's objextent not exist with offset[%v]", req.Extent.FileOffset)
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		if i.ObjExtents.eks[idx].Size != uint64(req.Extent.Size) {
			err = fmt.Errorf("ebs's objextent size[%v] isn't equal to the append size[%v]", i.ObjExtents.eks[idx].Size, req.Extent.Size)
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			i.RUnlock()
			return
		}
		i.RUnlock()
	}

	ext := req.Extent
	inoParm.Extents.Append(ext)
	//log.LogInfof("ExtentAppendWithCheck: ino(%v) ext(%v) discard(%v) eks(%v)", req.Inode, ext, req.DiscardExtents, ino.Extents.eks)
	// Store discard extents right after the append extent key.
	if len(req.DiscardExtents) != 0 {
		inoParm.Extents.eks = append(inoParm.Extents.eks, req.DiscardExtents...)
	}
	val, err := inoParm.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMExtentsAddWithCheck, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) SetTxInfo(info []*proto.TxInfo) {
	for _, txInfo := range info {
		if txInfo.Volume != mp.config.VolName {
			continue
		}
		mp.txProcessor.mask = txInfo.Mask
		mp.txProcessor.txManager.setLimit(txInfo.OpLimitVal)
		log.LogInfof("SetTxInfo mp %v mask %v limit %v", mp.config.PartitionId, proto.GetMaskString(txInfo.Mask), txInfo.OpLimitVal)
	}
}

func (mp *metaPartition) SetUidLimit(info []*proto.UidSpaceInfo) {
	mp.uidManager.volName = mp.config.VolName
	mp.uidManager.setUidAcl(info)
}

func (mp *metaPartition) GetUidInfo() (info []*proto.UidReportSpaceInfo) {
	return mp.uidManager.getAllUidSpace()
}

// ExtentsList returns the list of extents.
func (mp *metaPartition) ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	retMsg := mp.getInode(ino)
	ino = retMsg.Msg
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

// ObjExtentsList returns the list of obj extents and extents.
func (mp *metaPartition) ObjExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	retMsg := mp.getInode(ino)
	ino = retMsg.Msg
	var (
		reply  []byte
		status = retMsg.Status
	)
	if status == proto.OpOk {
		resp := &proto.GetObjExtentsResponse{}
		ino.DoReadFunc(func() {
			resp.Generation = ino.Generation
			resp.Size = ino.Size
			ino.Extents.Range(func(ek proto.ExtentKey) bool {
				resp.Extents = append(resp.Extents, ek)
				return true
			})
			ino.ObjExtents.Range(func(ek proto.ObjExtentKey) bool {
				resp.ObjExtents = append(resp.ObjExtents, ek)
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
	if !proto.IsHot(mp.volType) {
		err = fmt.Errorf("only support hot vol")
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	ino := NewInode(req.Inode, proto.Mode(os.ModePerm))
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		err = fmt.Errorf("inode %v is not exist", req.Inode)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	i := item.(*Inode)
	status := mp.isOverQuota(req.Inode, req.Size > i.Size, false)
	if status != 0 {
		log.LogErrorf("ExtentsTruncate fail status [%v]", status)
		err = errors.New("ExtentsTruncate is over quota")
		reply := []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}

	ino.Size = req.Size
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMExtentTruncate, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

func (mp *metaPartition) BatchExtentAppend(req *proto.AppendExtentKeysRequest, p *Packet) (err error) {
	if !proto.IsHot(mp.volType) {
		err = fmt.Errorf("only support hot vol")
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	var ino *Inode
	if ino, _, err = mp.CheckQuota(req.Inode, p); err != nil {
		log.LogErrorf("BatchExtentAppend fail err [%v]", err)
		return
	}

	extents := req.Extents
	for _, extent := range extents {
		ino.Extents.Append(extent)
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMExtentsAdd, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) BatchObjExtentAppend(req *proto.AppendObjExtentKeysRequest, p *Packet) (err error) {

	var ino *Inode
	if ino, _, err = mp.CheckQuota(req.Inode, p); err != nil {
		log.LogErrorf("BatchObjExtentAppend fail status [%v]", err)
		return
	}

	objExtents := req.Extents
	for _, objExtent := range objExtents {
		err = ino.ObjExtents.Append(objExtent)
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMObjExtentsAdd, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// func (mp *metaPartition) ExtentsDelete(req *proto.DelExtentKeyRequest, p *Packet) (err error) {
// 	ino := NewInode(req.Inode, 0)
// 	inode := mp.inodeTree.Get(ino).(*Inode)
// 	inode.Extents.Delete(req.Extents)
// 	curTime := Now.GetCurrentTime().Unix()
// 	if inode.ModifyTime < curTime {
// 		inode.ModifyTime = curTime
// 	}
// 	val, err := inode.Marshal()
// 	if err != nil {
// 		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
// 		return
// 	}
// 	resp, err := mp.submit(opFSMExtentsDel, val)
// 	if err != nil {
// 		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
// 		return
// 	}
// 	p.PacketErrorWithBody(resp.(uint8), nil)
// 	return
// }

// ExtentsEmpty only use in datalake situation
func (mp *metaPartition) ExtentsEmpty(req *proto.EmptyExtentKeyRequest, p *Packet, ino *Inode) (err error) {
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMExtentsEmpty, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) sendExtentsToChan(eks []proto.ExtentKey) (err error) {
	if len(eks) == 0 {
		return
	}

	sortExts := NewSortedExtentsFromEks(eks)
	val, err := sortExts.MarshalBinary()
	if err != nil {
		return fmt.Errorf("[delExtents] marshal binary fail, %s", err.Error())
	}

	_, err = mp.submit(opFSMSentToChan, val)

	return
}
