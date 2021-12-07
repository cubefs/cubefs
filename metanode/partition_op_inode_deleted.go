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
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/btree"
	"github.com/chubaofs/chubaofs/util/log"
	"math"
	"time"
)

func (mp *metaPartition) GetDeletedInode(req *GetDeletedInodeReq, p *Packet) (err error) {
	srcIno, delIno, status := mp.getDeletedInode(req.Inode)
	var reply []byte
	if status != proto.OpOk {
		p.PacketErrorWithBody(status, reply)
		return
	}

	resp := new(proto.GetDeletedInodeResponse)
	resp.Info = buildProtoDeletedInodeInfo(srcIno, delIno)
	reply, err = json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, reply)
		return
	}
	p.PacketOkWithBody(reply)
	return
}

func (mp *metaPartition) RecoverDeletedInode(req *proto.RecoverDeletedInodeRequest, p *Packet) (err error) {
	ino := new(FSMDeletedINode)
	ino.inode = req.Inode
	var data []byte
	data, err = ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	var (
		resp  interface{}
		reply []byte
	)

	resp, err = mp.submit(p.Ctx(), opFSMRecoverDeletedInode, p.Remote(), data)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	status := resp.(*fsmOpDeletedInodeResponse).Status
	p.PacketErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) BatchRecoverDeletedInode(req *proto.BatchRecoverDeletedInodeRequest, p *Packet) (err error) {
	inos := make(FSMDeletedINodeBatch, 0, len(req.Inodes))
	for _, ino := range req.Inodes {
		inos = append(inos, NewFSMDeletedINode(ino))
	}

	var data []byte
	data, err = inos.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	var (
		resp  interface{}
		reply []byte
	)
	resp, err = mp.submit(p.Ctx(), opFSMBatchRecoverDeletedInode, p.Remote(), data)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	userRsp := buildProtoBatchOpDeletedINodeRsp(resp.([]*fsmOpDeletedInodeResponse))
	reply, err = json.Marshal(userRsp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, reply)
		return
	}
	p.PacketOkWithBody(reply)
	return
}

func (mp *metaPartition) BatchCleanDeletedInode(req *proto.BatchCleanDeletedInodeRequest, p *Packet) (err error) {
	inos := make(FSMDeletedINodeBatch, 0, len(req.Inodes))
	for _, ino := range req.Inodes {
		inos = append(inos, NewFSMDeletedINode(ino))
	}

	var data []byte
	data, err = inos.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	var (
		resp  interface{}
		reply []byte
	)
	resp, err = mp.submit(p.Ctx(), opFSMBatchCleanDeletedInode, p.Remote(), data)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	userRsp := buildProtoBatchOpDeletedINodeRsp(resp.([]*fsmOpDeletedInodeResponse))
	reply, err = json.Marshal(userRsp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, reply)
		return
	}
	p.PacketOkWithBody(reply)
	return
}

func (mp *metaPartition) CleanDeletedInode(req *proto.CleanDeletedInodeRequest, p *Packet) (err error) {
	di := NewFSMDeletedINode(req.Inode)
	var data []byte
	data, err = di.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	var (
		resp  interface{}
		reply []byte
	)
	resp, err = mp.submit(p.Ctx(), opFSMCleanDeletedInode, p.Remote(), data)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	status := resp.(*fsmOpDeletedInodeResponse).Status
	p.PacketErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) getDeletedInode(ino uint64) (srcIno *Inode, di *DeletedINode, status uint8) {
	status = proto.OpOk
	inode := NewInode(ino, 0)
	item := mp.inodeTree.Get(inode)
	if item != nil {
		srcIno = item.(*Inode)
		/*
			if srcIno.ShouldDelete() {
				log.LogErrorf("getDeletedInode, inode: %v should not be deleted.", srcIno)
				status = proto.OpNotExistErr
				return
			}

		*/
		return
	}

	dino := NewDeletedInodeByID(ino)
	item = mp.inodeDeletedTree.Get(dino)
	if item == nil {
		log.LogDebugf("[getDeletedInode], not found delete inode: %v", ino)
		status = proto.OpNotExistErr
		return
	}
	di = item.(*DeletedINode)
	return
}

func (mp *metaPartition) StatDeletedFileInfo(p *Packet) (err error) {
	var (
		resp  *StatDeletedFileResp
		reply []byte
	)
	resp = new(StatDeletedFileResp)
	resp.StatInfo = mp.statDeletedFileInfo()
	reply, err = json.Marshal(resp)
	if err != nil {
		status := proto.OpErr
		reply = []byte(err.Error())
		p.PacketErrorWithBody(status, reply)
		return
	}

	p.PacketOkWithBody(reply)
	return
}

func (mp *metaPartition) statDeletedFileInfo() (statInfo map[string]*proto.DeletedFileInfo) {
	statInfo = make(map[string]*proto.DeletedFileInfo, 0)
	var getDateStr = func(ts int64) string {
		return time.Unix(ts/1000/1000, 0).Format("2006-01-02")
	}
	tempTree := mp.dentryDeletedTree.GetTree()
	tempTree.Ascend(func(i btree.Item) bool {
		dateStr := getDateStr(i.(*DeletedDentry).Timestamp)
		e, ok := statInfo[dateStr]
		if !ok {
			e = new(proto.DeletedFileInfo)
			statInfo[dateStr] = e
		}
		e.DentrySum++
		return true
	})

	tempTree = mp.inodeDeletedTree.GetTree()
	tempTree.Ascend(func(i btree.Item) bool {
		dino := i.(*DeletedINode)
		dateStr := getDateStr(dino.Timestamp)
		e, ok := statInfo[dateStr]
		if !ok {
			e = new(proto.DeletedFileInfo)
			statInfo[dateStr] = e
		}
		e.InodeSum++
		e.Size += dino.Size
		return true
	})

	return
}

func (mp *metaPartition) BatchGetDeletedInode(req *BatchGetDeletedInodeReq, p *Packet) (err error) {
	resp := new(BatchGetDeletedInodeResp)
	resp.Infos = make([]*proto.DeletedInodeInfo, 0)
	for _, inoId := range req.Inodes {
		srcIno, delIno, status := mp.getDeletedInode(inoId)
		if status == proto.OpOk {
			dino := buildProtoDeletedInodeInfo(srcIno, delIno)
			resp.Infos = append(resp.Infos, dino)
		}
	}
	var data []byte
	data, err = json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (mp *metaPartition) CleanExpiredDeletedINode() (err error) {
	ctx := context.Background()
	fsmFunc := func(inodes []uint64) (err error) {
		batchIno := make(FSMDeletedINodeBatch, 0, len(inodes))
		for _, ino := range inodes {
			fsmIno := new(FSMDeletedINode)
			fsmIno.inode = ino
			batchIno = append(batchIno, fsmIno)
		}

		var data []byte
		data, err = batchIno.Marshal()
		if err != nil {
			log.LogError(err.Error())
			return
		}

		_, err = mp.submit(ctx, opFSMCleanExpiredInode, mp.manager.metaNode.localAddr, data)
		if err != nil {
			log.LogErrorf("[CleanExpiredDeletedINode], vol: %v, err: %v", mp.config.VolName, err.Error())
		}
		return
	}
	if mp.config.TrashRemainingDays < 0 {
		err = fmt.Errorf("[CleanExpiredDeletedDentry], vol: %v, pid: %v, trashDays: %v is invalid",
			mp.config.VolName, mp.config.PartitionId, mp.config.TrashRemainingDays)
		return
	}

	var expires int64 = math.MaxInt64
	if mp.config.TrashRemainingDays > 0 {
		expires = time.Now().AddDate(0, 0, 0-int(mp.config.TrashRemainingDays)).UnixNano() / 1000
	}
	log.LogDebugf("[CleanExpiredDeletedINode] vol: %v, expires: %v", mp.config.VolName, expires)

	total := 0
	defer log.LogDebugf("[CleanExpiredDeletedINode], cleaned %v until %v", total, expires)
	batch := 128
	inos := make([]uint64, 0, batch)
	tree := mp.inodeDeletedTree.GetTree()
	tree.Ascend(func(i BtreeItem) bool {
		_, ok := mp.IsLeader()
		if !ok {
			return false
		}
		di := i.(*DeletedINode)
		if di.Timestamp >= expires {
			return true
		}

		inos = append(inos, di.Inode.Inode)
		if len(inos) < batch {
			return true
		}

		err = fsmFunc(inos)
		if err != nil {
			log.LogErrorf("[CleanExpiredDeletedINode], vol:%v, err: %v", mp.config.VolName, err.Error())
			return false
		}
		total += batch
		inos = make([]uint64, 0, batch)
		if mp.config.TrashRemainingDays > 0 {
			expires = time.Now().AddDate(0, 0, 0-int(mp.config.TrashRemainingDays)).UnixNano() / 1000
		} else {
			expires = math.MaxInt64
		}
		time.Sleep(1 * time.Second)
		return true
	})

	_, ok := mp.IsLeader()
	if !ok {
		return
	}

	if len(inos) == 0 {
		return
	}

	err = fsmFunc(inos)
	if err != nil {
		log.LogErrorf("[CleanExpiredDeletedINode], %v, err: %v", mp.config.VolName, err.Error())
		return
	}
	total += len(inos)
	return
}

func buildProtoDeletedInodeInfo(ino *Inode, di *DeletedINode) (info *proto.DeletedInodeInfo) {
	info = new(proto.DeletedInodeInfo)
	if ino != nil {
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
		info.IsDeleted = false
		return
	}

	if di != nil {
		info.Inode = di.Inode.Inode
		info.Mode = di.Type
		info.Size = di.Size
		info.Nlink = di.NLink
		info.Uid = di.Uid
		info.Gid = di.Gid
		info.Generation = di.Generation
		if length := len(di.LinkTarget); length > 0 {
			info.Target = make([]byte, length)
			copy(info.Target, di.LinkTarget)
		}
		info.CreateTime = time.Unix(di.CreateTime, 0)
		info.AccessTime = time.Unix(di.AccessTime, 0)
		info.ModifyTime = time.Unix(di.ModifyTime, 0)
		info.DeleteTime = di.Timestamp
		info.IsDeleted = true
		return
	}
	return
}

func buildProtoBatchOpDeletedINodeRsp(resp []*fsmOpDeletedInodeResponse) (ret *proto.BatchOpDeletedINodeRsp) {
	ret = new(proto.BatchOpDeletedINodeRsp)
	if resp == nil {
		ret.Inos = make([]*proto.OpDeletedINodeRsp, 0)
		return
	}
	ret.Inos = make([]*proto.OpDeletedINodeRsp, 0, len(resp))
	for _, r := range resp {
		var ino proto.DeletedInodeInfo
		ino.Inode = r.Inode

		var inoRsp proto.OpDeletedINodeRsp
		inoRsp.Status = r.Status
		inoRsp.Inode = &ino

		ret.Inos = append(ret.Inos, &inoRsp)
	}
	return
}
