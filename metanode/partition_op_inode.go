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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func replyInfoNoCheck(info *proto.InodeInfo, ino *Inode) bool {
	ino.RLock()
	defer ino.RUnlock()

	info.Inode = ino.Inode
	info.Mode = ino.Type
	info.Size = ino.Size
	info.Nlink = ino.NLink
	info.Uid = ino.Uid
	info.Gid = ino.Gid
	info.Generation = ino.Generation
	info.VerSeq = ino.getVer()
	if length := len(ino.LinkTarget); length > 0 {
		info.Target = make([]byte, length)
		copy(info.Target, ino.LinkTarget)
	}
	info.CreateTime = time.Unix(ino.CreateTime, 0)
	info.AccessTime = time.Unix(ino.AccessTime, 0)
	info.ModifyTime = time.Unix(ino.ModifyTime, 0)
	info.StorageClass = ino.StorageClass
	info.MigrationStorageClass = ino.HybridCouldExtentsMigration.storageClass
	info.WriteGen = atomic.LoadUint64(&ino.WriteGeneration)
	if atomic.LoadUint32(&ino.ForbiddenMigration) == ForbiddenToMigration {
		info.ForbiddenLc = true
	}
	return true
}

func replyInfo(info *proto.InodeInfo, ino *Inode, quotaInfos map[uint32]*proto.MetaQuotaInfo) bool {
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
	info.VerSeq = ino.getVer()
	if length := len(ino.LinkTarget); length > 0 {
		info.Target = make([]byte, length)
		copy(info.Target, ino.LinkTarget)
	}
	info.CreateTime = time.Unix(ino.CreateTime, 0)
	info.AccessTime = time.Unix(ino.AccessTime, 0)
	info.ModifyTime = time.Unix(ino.ModifyTime, 0)
	info.QuotaInfos = quotaInfos
	info.StorageClass = ino.StorageClass
	info.WriteGen = atomic.LoadUint64(&ino.WriteGeneration)
	if atomic.LoadUint32(&ino.ForbiddenMigration) == ForbiddenToMigration {
		info.ForbiddenLc = true
	}
	info.MigrationStorageClass = ino.HybridCouldExtentsMigration.storageClass
	if ino.HybridCouldExtentsMigration.sortedEks != nil {
		info.HasMigrationEk = true
	}
	info.MigrationExtentKeyExpiredTime = time.Unix(ino.HybridCouldExtentsMigration.expiredTime, 0)
	return true
}

func txReplyInfo(inode *Inode, txInfo *proto.TransactionInfo, quotaInfos map[uint32]*proto.MetaQuotaInfo) (resp *proto.TxCreateInodeResponse) {
	inoInfo := &proto.InodeInfo{
		Inode:      inode.Inode,
		Mode:       inode.Type,
		Nlink:      inode.NLink,
		Size:       inode.Size,
		Uid:        inode.Uid,
		Gid:        inode.Gid,
		Generation: inode.Generation,
		ModifyTime: time.Unix(inode.ModifyTime, 0),
		CreateTime: time.Unix(inode.CreateTime, 0),
		AccessTime: time.Unix(inode.AccessTime, 0),
		QuotaInfos: quotaInfos,
		Target:     nil,
	}
	if length := len(inode.LinkTarget); length > 0 {
		inoInfo.Target = make([]byte, length)
		copy(inoInfo.Target, inode.LinkTarget)
	}

	resp = &proto.TxCreateInodeResponse{
		Info:   inoInfo,
		TxInfo: txInfo,
	}
	return
}

// for compatibility: handle req from old version client without filed StorageType
func (mp *metaPartition) checkCreateInoStorageClassFroCompatibility(reqStorageClass uint32, inodeId uint64) (err error, resultStorageClass uint32) {
	if proto.IsValidStorageClass(reqStorageClass) {
		resultStorageClass = reqStorageClass
		return
	}

	if reqStorageClass != proto.StorageClass_Unspecified {
		err = fmt.Errorf("unknown req storageClass(%v)", reqStorageClass)
		return
	}

	if proto.IsHot(mp.volType) {
		if !proto.IsValidStorageClass(legacyReplicaStorageClass) {
			err = fmt.Errorf("CreateInode req without StorageType, but metanode config legacyReplicaStorageClass not set")
			return
		}

		resultStorageClass = legacyReplicaStorageClass
		log.LogDebugf("legacy CreateInode req, hot vol(%v), mpId(%v), ino(%v), set ino storageClass as config legacyReplicaStorageClass(%v)",
			mp.config.VolName, mp.config.PartitionId, inodeId, proto.StorageClassString(legacyReplicaStorageClass))
	} else {
		resultStorageClass = proto.StorageClass_BlobStore
		log.LogDebugf("legacy CreateInode req, cold vol(%v), mpId(%v), ino(%v), set ino storageClass as blobstore",
			mp.config.VolName, mp.config.PartitionId, inodeId)
	}

	return
}

// CreateInode returns a new inode.
func (mp *metaPartition) CreateInode(req *CreateInoReq, p *Packet, remoteAddr string) (err error) {
	var (
		status = proto.OpNotExistErr
		reply  []byte
		resp   interface{}
		qinode *MetaQuotaInode
		inoID  uint64
	)
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), inoID, 0)
		}()
	}
	inoID, err = mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}
	ino := NewInode(inoID, req.Mode)
	ino.Uid = req.Uid
	ino.Gid = req.Gid
	ino.setVer(mp.verSeq)
	ino.LinkTarget = req.Target

	if err, ino.StorageClass = mp.checkCreateInoStorageClassFroCompatibility(req.StorageType, inoID); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		log.LogErrorf("[CreateInode] %v, req(%+v)", err.Error(), req)
		return
	}

	if proto.IsStorageClassReplica(ino.StorageClass) {
		ino.HybridCouldExtents.sortedEks = NewSortedExtents()
	} else if ino.StorageClass == proto.StorageClass_BlobStore {
		ino.HybridCouldExtents.sortedEks = NewSortedObjExtents()
	} else {
		p.PacketErrorWithBody(proto.OpErr, []byte(fmt.Sprintf("storage type %v not support", ino.StorageClass)))
		return
	}

	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return err
	}
	resp, err = mp.submit(opFSMCreateInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	if resp.(uint8) == proto.OpOk {
		resp := &CreateInoResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, ino, make(map[uint32]*proto.MetaQuotaInfo, 0)) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
	}
	p.PacketErrorWithBody(status, reply)
	log.LogInfof("CreateInode req [%v] qinode [%v] success.", req, qinode)
	return
}

func (mp *metaPartition) QuotaCreateInode(req *proto.QuotaCreateInodeRequest, p *Packet, remoteAddr string) (err error) {
	var (
		status = proto.OpNotExistErr
		reply  []byte
		resp   interface{}
		qinode *MetaQuotaInode
		inoID  uint64
	)
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), inoID, 0)
		}()
	}
	inoID, err = mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}
	ino := NewInode(inoID, req.Mode)
	ino.Uid = req.Uid
	ino.Gid = req.Gid
	ino.LinkTarget = req.Target

	if err, ino.StorageClass = mp.checkCreateInoStorageClassFroCompatibility(req.StorageType, inoID); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		log.LogErrorf("[QuotaCreateInode] %v, req(%+v)", err.Error(), req)
		return
	}

	for _, quotaId := range req.QuotaIds {
		status = mp.mqMgr.IsOverQuota(false, true, quotaId)
		if status != 0 {
			err = errors.New("create inode is over quota")
			reply = []byte(err.Error())
			p.PacketErrorWithBody(status, reply)
			return
		}
	}
	qinode = &MetaQuotaInode{
		inode:    ino,
		quotaIds: req.QuotaIds,
	}
	val, err := qinode.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return err
	}
	resp, err = mp.submit(opFSMCreateInodeQuota, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return err
	}

	if resp.(uint8) == proto.OpOk {
		resp := &CreateInoResp{
			Info: &proto.InodeInfo{},
		}
		quotaInfos := make(map[uint32]*proto.MetaQuotaInfo)
		for _, quotaId := range req.QuotaIds {
			quotaInfos[quotaId] = &proto.MetaQuotaInfo{
				RootInode: false,
			}
		}
		if replyInfo(resp.Info, ino, quotaInfos) {
			status = proto.OpOk
			reply, err = json.Marshal(resp)
			if err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
	}
	p.PacketErrorWithBody(status, reply)
	log.LogInfof("QuotaCreateInode req [%v] qinode [%v] success.", req, qinode)
	return
}

func (mp *metaPartition) TxUnlinkInode(req *proto.TxUnlinkInodeRequest, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, 0)
		}()
	}
	txInfo := req.TxInfo.GetCopy()
	var status uint8
	var respIno *Inode
	defer func() {
		var reply []byte
		if status == proto.OpOk {
			resp := &proto.TxUnlinkInodeResponse{
				Info: &proto.InodeInfo{},
			}
			if respIno != nil {
				replyInfo(resp.Info, respIno, make(map[uint32]*proto.MetaQuotaInfo, 0))
				if reply, err = json.Marshal(resp); err != nil {
					status = proto.OpErr
					reply = []byte(err.Error())
				}
			}
			p.PacketErrorWithBody(status, reply)
		}
	}()

	ino := NewInode(req.Inode, 0)
	inoResp := mp.getInode(ino, true)
	if inoResp.Status != proto.OpOk {
		if rbIno := mp.txInodeInRb(req.Inode, req.TxInfo.TxID); rbIno != nil {
			respIno = rbIno.inode
			status = proto.OpOk

			item := mp.inodeTree.Get(NewInode(req.Inode, 0))
			if item != nil {
				respIno = item.(*Inode)
			}

			p.ResultCode = status
			log.LogWarnf("TxUnlinkInode: inode is already unlink before, req %v, rbIno %v, item %v", req, respIno, item)
			return nil
		}

		err = fmt.Errorf("ino[%v] not exists", ino.Inode)
		p.PacketErrorWithBody(inoResp.Status, []byte(err.Error()))
		return
	}

	respIno = inoResp.Msg
	createTime := respIno.CreateTime
	deleteLockTime := mp.vol.volDeleteLockTime * 60 * 60
	if deleteLockTime > 0 && createTime+deleteLockTime > time.Now().Unix() {
		err = fmt.Errorf("the current Inode[%v] is still locked for deletion", req.Inode)
		log.LogDebugf("TxUnlinkInode: the current Inode is still locked for deletion, inode(%v) createTime(%v) mw.volDeleteLockTime(%v) now(%v)", respIno.Inode, createTime, deleteLockTime, time.Now())
		p.PacketErrorWithBody(proto.OpNotPerm, []byte(err.Error()))
		return
	}

	ti := &TxInode{
		Inode:  inoResp.Msg,
		TxInfo: txInfo,
	}

	val, err := ti.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	r, err := mp.submit(opFSMTxUnlinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	msg := r.(*InodeResponse)
	status = msg.Status
	if msg.Msg != nil {
		respIno = msg.Msg
	}
	p.ResultCode = status
	return
}

// DeleteInode deletes an inode.
func (mp *metaPartition) UnlinkInode(req *UnlinkInoReq, p *Packet, remoteAddr string) (err error) {
	var (
		msg   *InodeResponse
		reply []byte
		r     interface{}
		val   []byte
	)
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, 0)
		}()
	}
	makeRspFunc := func() {
		status := msg.Status
		if status == proto.OpOk {
			resp := &UnlinkInoResp{
				Info: &proto.InodeInfo{},
			}
			replyInfo(resp.Info, msg.Msg, make(map[uint32]*proto.MetaQuotaInfo, 0))
			if reply, err = json.Marshal(resp); err != nil {
				status = proto.OpErr
				reply = []byte(err.Error())
			}
		}
		p.PacketErrorWithBody(status, reply)
	}
	ino := NewInode(req.Inode, 0)
	if item := mp.inodeTree.Get(ino); item == nil {
		err = fmt.Errorf("mp %v inode %v reqeust cann't found", mp.config.PartitionId, ino)
		log.LogErrorf("action[UnlinkInode] %v", err)
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	} else {
		ino.UpdateHybridCloudParams(item.(*Inode))
	}

	if req.UniqID > 0 {
		val = InodeOnceUnlinkMarshal(req)
		r, err = mp.submit(opFSMUnlinkInodeOnce, val)
	} else {
		ino.setVer(req.VerSeq)
		log.LogDebugf("action[UnlinkInode] mp %v verseq %v ino %v", mp.config.PartitionId, req.VerSeq, ino)
		val, err = ino.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		log.LogDebugf("action[UnlinkInode] mp %v ino %v submit", mp.config.PartitionId, ino)
		r, err = mp.submit(opFSMUnlinkInode, val)
	}

	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	msg = r.(*InodeResponse)
	makeRspFunc()

	return
}

// DeleteInode deletes an inode.
func (mp *metaPartition) UnlinkInodeBatch(req *BatchUnlinkInoReq, p *Packet, remoteAddr string) (err error) {

	if len(req.Inodes) == 0 {
		return nil
	}

	var inodes InodeBatch
	start := time.Now()
	for i, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
		ino := id
		fullPath := ""
		if len(req.FullPaths) > i {
			fullPath = req.FullPaths[i]
		}
		if mp.IsEnableAuditLog() {
			defer func() {
				auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), fullPath, err, time.Since(start).Milliseconds(), ino, 0)
			}()
		}
	}

	val, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	r, err := mp.submit(opFSMUnlinkInodeBatch, val)
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
		replyInfo(info, ir.Msg, make(map[uint32]*proto.MetaQuotaInfo, 0))
		result.Items = append(result.Items, &struct {
			Info   *proto.InodeInfo `json:"info"`
			Status uint8            `json:"status"`
		}{
			Info:   info,
			Status: ir.Status,
		})
	}

	reply, err := json.Marshal(result)
	if err != nil {
		status = proto.OpErr
		reply = []byte(err.Error())
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// InodeGet executes the inodeGet command from the client.
func (mp *metaPartition) InodeGetSplitEk(req *InodeGetSplitReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ino.setVer(req.VerSeq)

	getAllVerInfo := req.VerAll
	retMsg := mp.getInode(ino, getAllVerInfo)

	log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
		ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)

	ino = retMsg.Msg
	var (
		reply  []byte
		status = proto.OpNotExistErr
	)
	if retMsg.Status == proto.OpOk {
		resp := &proto.InodeGetSplitResponse{
			Info: &proto.InodeSplitInfo{
				Inode:  ino.Inode,
				VerSeq: ino.getVer(),
			},
		}
		multiSnap := retMsg.Msg.multiSnap
		if multiSnap != nil && multiSnap.ekRefMap != nil {
			multiSnap.ekRefMap.Range(func(key, value interface{}) bool {
				dpID, extID := proto.ParseFromId(key.(uint64))
				resp.Info.SplitArr = append(resp.Info.SplitArr, proto.SimpleExtInfo{
					ID:          key.(uint64),
					PartitionID: uint32(dpID),
					ExtentID:    uint32(extID),
				})
				return true
			})
		}
		log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
			ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)
		status = proto.OpOk
		reply, err = json.Marshal(resp)
		if err != nil {
			log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
				ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)
			status = proto.OpErr
			reply = []byte(err.Error())
		}
		log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
			ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)
	}
	log.LogDebugf("action[InodeGetSplitEk] %v seq %v retMsg.Status %v, getAllVerInfo %v",
		ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)
	p.PacketErrorWithBody(status, reply)
	return
}

// InodeGet executes the inodeGet command from the client.
func (mp *metaPartition) InodeGet(req *InodeGetReq, p *Packet) (err error) {

	ino := NewInode(req.Inode, 0)
	ino.setVer(req.VerSeq)
	getAllVerInfo := req.VerAll
	retMsg := mp.getInode(ino, getAllVerInfo)

	log.LogDebugf("action[Inode] %v seq %v retMsg.Status %v, getAllVerInfo %v",
		ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)

	ino = retMsg.Msg

	var (
		reply      []byte
		status     = proto.OpNotExistErr
		quotaInfos map[uint32]*proto.MetaQuotaInfo
	)
	if mp.mqMgr.EnableQuota() {
		quotaInfos, err = mp.getInodeQuotaInfos(req.Inode)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
			p.PacketErrorWithBody(status, reply)
			return
		}
	}

	ino = retMsg.Msg
	if retMsg.Status == proto.OpOk {
		resp := &proto.InodeGetResponse{
			Info: &proto.InodeInfo{},
		}
		if getAllVerInfo {
			replyInfoNoCheck(resp.Info, retMsg.Msg)
		} else {
			if !replyInfo(resp.Info, retMsg.Msg, quotaInfos) {
				p.PacketErrorWithBody(status, reply)
				return

			}
		}

		status = proto.OpOk
		if getAllVerInfo {
			inode := mp.getInodeTopLayer(ino)
			log.LogDebugf("req ino %v, toplayer ino %v", retMsg.Msg, inode)
			resp.LayAll = inode.Msg.getAllInodesInfo()
		}
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

	resp := &proto.BatchInodeGetResponse{}
	ino := NewInode(0, 0)
	for _, inoId := range req.Inodes {
		var quotaInfos map[uint32]*proto.MetaQuotaInfo
		ino.Inode = inoId
		ino.setVer(req.VerSeq)
		retMsg := mp.getInode(ino, false)
		if mp.mqMgr.EnableQuota() {
			quotaInfos, err = mp.getInodeQuotaInfos(inoId)
			if err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
		}
		if retMsg.Status == proto.OpOk {
			inoInfo := &proto.InodeInfo{}
			if replyInfo(inoInfo, retMsg.Msg, quotaInfos) {
				resp.Infos = append(resp.Infos, inoInfo)
			}
		}
	}
	data, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (mp *metaPartition) TxCreateInodeLink(req *proto.TxLinkInodeRequest, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, 0)
		}()
	}
	txInfo := req.TxInfo.GetCopy()
	ino := NewInode(req.Inode, 0)
	inoResp := mp.getInode(ino, true)
	if inoResp.Status != proto.OpOk {
		err = fmt.Errorf("ino[%v] not exists", ino.Inode)
		p.PacketErrorWithBody(inoResp.Status, []byte(err.Error()))
		return
	}

	ti := &TxInode{
		Inode:  inoResp.Msg,
		TxInfo: txInfo,
	}

	val, err := ti.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	resp, err := mp.submit(opFSMTxCreateLinkInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	retMsg := resp.(*InodeResponse)
	status := retMsg.Status
	var reply []byte
	if retMsg.Status == proto.OpOk {
		resp := &proto.TxLinkInodeResponse{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, retMsg.Msg, make(map[uint32]*proto.MetaQuotaInfo, 0)) {
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

// CreateInodeLink creates an inode link (e.g., soft link).
func (mp *metaPartition) CreateInodeLink(req *LinkInodeReq, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, 0)
		}()
	}
	var r interface{}
	var val []byte
	if req.UniqID > 0 {
		val = InodeOnceLinkMarshal(req)
		r, err = mp.submit(opFSMCreateLinkInodeOnce, val)
	} else {
		ino := NewInode(req.Inode, 0)
		ino.setVer(mp.verSeq)
		val, err = ino.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		r, err = mp.submit(opFSMCreateLinkInode, val)

	}

	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	retMsg := r.(*InodeResponse)
	status := proto.OpNotExistErr
	var reply []byte
	if retMsg.Status == proto.OpOk {
		resp := &LinkInodeResp{
			Info: &proto.InodeInfo{},
		}
		if replyInfo(resp.Info, retMsg.Msg, make(map[uint32]*proto.MetaQuotaInfo, 0)) {
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
func (mp *metaPartition) EvictInode(req *EvictInodeReq, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, 0)
		}()
	}
	ino := NewInode(req.Inode, 0)
	if item := mp.inodeTree.Get(ino); item == nil {
		err = fmt.Errorf("mp %v inode %v reqeust cann't found", mp.config.PartitionId, ino)
		log.LogErrorf("action[RenewalForbiddenMigration] %v", err)
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	} else {
		ino.UpdateHybridCloudParams(item.(*Inode))
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMEvictInode, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

// EvictInode evicts an inode.
func (mp *metaPartition) EvictInodeBatch(req *BatchEvictInodeReq, p *Packet, remoteAddr string) (err error) {

	if len(req.Inodes) == 0 {
		return nil
	}

	start := time.Now()
	var inodes InodeBatch

	for i, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
		ino := id
		fullPath := ""
		if len(req.FullPaths) > i {
			fullPath = req.FullPaths[i]
		}
		if mp.IsEnableAuditLog() {
			defer func() {
				auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), fullPath, err, time.Since(start).Milliseconds(), ino, 0)
			}()
		}
	}

	val, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMEvictInodeBatch, val)
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
func (mp *metaPartition) SetAttr(req *SetattrRequest, reqData []byte, p *Packet) (err error) {
	if mp.verSeq != 0 {
		req.VerSeq = mp.GetVerSeq()
		reqData, err = json.Marshal(req)
		if err != nil {
			log.LogErrorf("setattr: marshal err(%v)", err)
			return
		}
	}
	_, err = mp.submit(opFSMSetAttr, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	log.LogDebugf("action[SetAttr] inode %v ver %v exit", req.Inode, req.VerSeq)
	p.PacketOkReply()
	return
}

// GetInodeTree returns the inode tree.
func (mp *metaPartition) GetInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

// GetInodeTreeLen returns the inode tree length.
func (mp *metaPartition) GetInodeTreeLen() int {
	if mp.inodeTree == nil {
		return 0
	}
	return mp.inodeTree.Len()
}

func (mp *metaPartition) DeleteInode(req *proto.DeleteInodeRequest, p *Packet, remoteAddr string) (err error) {
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, 0)
		}()
	}
	var bytes = make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, req.Inode)
	_, err = mp.submit(opFSMInternalDeleteInode, bytes)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) DeleteInodeBatch(req *proto.DeleteInodeBatchRequest, p *Packet, remoteAddr string) (err error) {
	if len(req.Inodes) == 0 {
		return nil
	}
	start := time.Now()
	var inodes InodeBatch

	for i, id := range req.Inodes {
		inodes = append(inodes, NewInode(id, 0))
		ino := id
		fullPath := ""
		if len(req.FullPaths) > i {
			fullPath = req.FullPaths[i]
		}
		if mp.IsEnableAuditLog() {
			defer func() {
				auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), fullPath, err, time.Since(start).Milliseconds(), ino, 0)
			}()
		}
	}

	encoded, err := inodes.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	_, err = mp.submit(opFSMInternalDeleteInodeBatch, encoded)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

// ClearInodeCache clear a inode's cbfs extent but keep ebs extent.
func (mp *metaPartition) ClearInodeCache(req *proto.ClearInodeCacheRequest, p *Packet) (err error) {
	if len(mp.extDelCh) > defaultDelExtentsCnt-100 {
		err = fmt.Errorf("extent del chan full")
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	ino := NewInode(req.Inode, 0)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMClearInodeCache, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// TxCreateInode returns a new inode.
func (mp *metaPartition) TxCreateInode(req *proto.TxCreateInodeRequest, p *Packet, remoteAddr string) (err error) {
	var (
		status = proto.OpNotExistErr
		reply  []byte
		resp   interface{}
		inoID  uint64
	)
	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogInodeOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), inoID, 0)
		}()
	}
	inoID, err = mp.nextInodeID()
	if err != nil {
		p.PacketErrorWithBody(proto.OpInodeFullErr, []byte(err.Error()))
		return
	}

	req.TxInfo.SetCreateInodeId(inoID)
	createTxReq := &proto.TxCreateRequest{
		VolName:         req.VolName,
		PartitionID:     req.PartitionID,
		TransactionInfo: req.TxInfo,
	}
	err = mp.TxCreate(createTxReq, p)
	if err != nil || p.ResultCode != proto.OpOk {
		return
	}

	createResp := &proto.TxCreateResponse{}
	err = json.Unmarshal(p.Data, createResp)
	if err != nil || createResp.TxInfo == nil {
		err = fmt.Errorf("TxCreateInode: unmarshal txInfo failed, data %s, err %v", string(p.Data), err)
		log.LogWarn(err)
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	txIno := NewTxInode(inoID, req.Mode, createResp.TxInfo)
	txIno.Inode.Uid = req.Uid
	txIno.Inode.Gid = req.Gid
	txIno.Inode.LinkTarget = req.Target
	txIno.Inode.StorageClass = req.StorageType

	if log.EnableDebug() {
		log.LogDebugf("NewTxInode: TxInode: %v", txIno)
	}

	if defaultQuotaSwitch {
		for _, quotaId := range req.QuotaIds {
			status = mp.mqMgr.IsOverQuota(false, true, quotaId)
			if status != 0 {
				err = errors.New("tx create inode is over quota")
				reply = []byte(err.Error())
				p.PacketErrorWithBody(status, reply)
				return
			}
		}

		qinode := &TxMetaQuotaInode{
			txinode:  txIno,
			quotaIds: req.QuotaIds,
		}
		val, err := qinode.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp, err = mp.submit(opFSMTxCreateInodeQuota, val)
		if err != nil {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
			return err
		}
	} else {
		val, err := txIno.Marshal()
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return err
		}
		resp, err = mp.submit(opFSMTxCreateInode, val)
		if err != nil {
			p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
			return err
		}
	}

	if resp == proto.OpOk {
		quotaInfos := make(map[uint32]*proto.MetaQuotaInfo)
		for _, quotaId := range req.QuotaIds {
			quotaInfos[quotaId] = &proto.MetaQuotaInfo{
				RootInode: false,
			}
		}
		resp := txReplyInfo(txIno.Inode, createResp.TxInfo, quotaInfos)
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

func (mp *metaPartition) RenewalForbiddenMigration(req *proto.RenewalForbiddenMigrationRequest,
	p *Packet, remoteAddr string) (err error) {
	ino := NewInode(req.Inode, 0)
	if item := mp.inodeTree.Get(ino); item == nil {
		err = fmt.Errorf("mp %v inode %v reqeust cann't found", mp.config.PartitionId, ino)
		log.LogErrorf("action[RenewalForbiddenMigration] %v", err)
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	} else {
		ino.UpdateHybridCloudParams(item.(*Inode))
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMRenewalForbiddenMigration, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

func (mp *metaPartition) UpdateExtentKeyAfterMigration(req *proto.UpdateExtentKeyAfterMigrationRequest, p *Packet,
	remoteAddr string) (err error) {
	ino := NewInode(req.Inode, 0)
	var item BtreeItem
	if item = mp.inodeTree.Get(ino); item == nil {
		err = fmt.Errorf("mp %v inode %v reqeust cann't found", mp.config.PartitionId, ino.Inode)
		log.LogErrorf("action[UpdateExtentKeyAfterMigration] %v", err)
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	} else {
		ino.UpdateHybridCloudParams(item.(*Inode))
	}

	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogMigrationOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(), err, time.Since(start).Milliseconds(), req.Inode, ino.StorageClass, req.StorageClass)
		}()
	}

	defer func() {
		//delete migration extent key if encounter an error
		if err != nil {
			delMigrationIno := item.(*Inode)
			//prepare HybridCouldExtentsMigration info for extent key delete
			if req.StorageClass == proto.StorageClass_BlobStore {
				log.LogDebugf("action[UpdateExtentKeyAfterMigration] prepare to delete migration obj extent key for "+
					"inode %v", delMigrationIno.Inode)
				delMigrationIno.HybridCouldExtentsMigration.storageClass = req.StorageClass
				delMigrationIno.HybridCouldExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(req.NewObjExtentKeys)
			}
			//notify follower to delete migration extent key
			mp.internalNotifyFollowerToDeleteExtentKey(delMigrationIno)
		}
	}()

	if atomic.LoadUint32(&ino.ForbiddenMigration) == ForbiddenToMigration {
		err = fmt.Errorf("mp %v inode %v is forbidden to migration", mp.config.PartitionId, ino.Inode)
		log.LogErrorf("action[UpdateExtentKeyAfterMigration] %v", err)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	writeGen := atomic.LoadUint64(&ino.WriteGeneration)
	if writeGen > req.WriteGen {
		err = fmt.Errorf("mp %v inode %v write generation is %v now: receive %v",
			mp.config.PartitionId, ino.Inode, writeGen, req.WriteGen)
		log.LogErrorf("action[UpdateExtentKeyAfterMigration] %v", err)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	//store ek after migration in HybridCouldExtentsMigration
	ino.HybridCouldExtentsMigration.storageClass = req.StorageClass
	ino.HybridCouldExtentsMigration.expiredTime = time.Now().Add(time.Duration(req.DelayDeleteMinute) * time.Minute).Unix()
	if req.StorageClass == proto.StorageClass_BlobStore {
		//may be
		ino.HybridCouldExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(req.NewObjExtentKeys)
	} else if req.StorageClass == proto.StorageClass_Replica_HDD {
		if item.(*Inode).HybridCouldExtentsMigration.sortedEks == nil &&
			item.(*Inode).HybridCouldExtentsMigration.storageClass == proto.StorageClass_Unspecified {
			log.LogDebugf("action[UpdateExtentKeyAfterMigration] ino %v has no migration data", ino.Inode)
			ino.HybridCouldExtentsMigration.sortedEks = NewSortedExtents()
		} else {
			if item.(*Inode).HybridCouldExtentsMigration.storageClass != proto.StorageClass_Replica_HDD {
				err = fmt.Errorf("mp %v inode %v migration class now is %v",
					mp.config.PartitionId, ino.Inode, item.(*Inode).HybridCouldExtentsMigration.storageClass)
				log.LogErrorf("action[UpdateExtentKeyAfterMigration] %v", err)
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			ino.HybridCouldExtentsMigration.sortedEks = item.(*Inode).HybridCouldExtentsMigration.sortedEks
		}
	} else {
		err = fmt.Errorf("mp %v inode %v unsupport new migration storage class %v",
			mp.config.PartitionId, ino.Inode, req.StorageClass)
		log.LogErrorf("action[UpdateExtentKeyAfterMigration] %v", err)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	val, err := ino.Marshal()
	if err != nil {
		log.LogErrorf("action[UpdateExtentKeyAfterMigration] ino %v marshall failed %v", ino.Inode, err)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMUpdateExtentKeyAfterMigration, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

func (mp *metaPartition) InodeGetWithEk(req *InodeGetReq, p *Packet) (err error) {

	ino := NewInode(req.Inode, 0)
	ino.setVer(req.VerSeq)
	getAllVerInfo := req.VerAll
	retMsg := mp.getInode(ino, getAllVerInfo)

	log.LogDebugf("action[InodeGetWithEk] inode %v seq %v retMsg.Status %v, getAllVerInfo %v",
		ino.Inode, req.VerSeq, retMsg.Status, getAllVerInfo)

	ino = retMsg.Msg

	var (
		reply      []byte
		status     = proto.OpNotExistErr
		quotaInfos map[uint32]*proto.MetaQuotaInfo
	)
	if mp.mqMgr.EnableQuota() {
		quotaInfos, err = mp.getInodeQuotaInfos(req.Inode)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
			p.PacketErrorWithBody(status, reply)
			return
		}
	}

	ino = retMsg.Msg
	if retMsg.Status == proto.OpOk {
		resp := &proto.InodeGetWithEkResponse{
			Info: &proto.InodeInfo{},
		}
		if getAllVerInfo {
			replyInfoNoCheck(resp.Info, retMsg.Msg)
		} else {
			if !replyInfo(resp.Info, retMsg.Msg, quotaInfos) {
				p.PacketErrorWithBody(status, reply)
				return

			}
		}

		status = proto.OpOk
		if getAllVerInfo {
			inode := mp.getInodeTopLayer(ino)
			log.LogDebugf("req ino %v, toplayer ino %v", retMsg.Msg, inode)
			resp.LayAll = inode.Msg.getAllInodesInfo()
		}
		// get cache ek
		ino.Extents.Range(func(ek proto.ExtentKey) bool {
			resp.CacheExtents = append(resp.CacheExtents, ek)
			log.LogInfof("action[InodeGetWithEk] Cache Extents append ek %v", ek)
			return true
		})
		//get EK
		if ino.HybridCouldExtents.sortedEks != nil {
			if proto.IsStorageClassReplica(ino.StorageClass) {
				extents := ino.HybridCouldExtents.sortedEks.(*SortedExtents)
				extents.Range(func(ek proto.ExtentKey) bool {
					resp.HybridCloudExtents = append(resp.HybridCloudExtents, ek)
					log.LogInfof("action[InodeGetWithEk] Extents append ek %v", ek)
					return true
				})
			} else if proto.IsStorageClassBlobStore(ino.StorageClass) {
				objEks := ino.HybridCouldExtents.sortedEks.(*SortedObjExtents)
				objEks.Range(func(ek proto.ObjExtentKey) bool {
					resp.HybridCloudObjExtents = append(resp.HybridCloudObjExtents, ek)
					log.LogInfof("action[InodeGetWithEk] ObjExtents append ek %v", ek)
					return true
				})
			}
		}
		if ino.HybridCouldExtentsMigration.sortedEks != nil {
			if proto.IsStorageClassReplica(ino.HybridCouldExtentsMigration.storageClass) {
				extents := ino.HybridCouldExtentsMigration.sortedEks.(*SortedExtents)
				extents.Range(func(ek proto.ExtentKey) bool {
					resp.MigrationExtents = append(resp.MigrationExtents, ek)
					log.LogInfof("action[ExtentsList] migrationExtents append ek %v", ek)
					return true
				})
			} else if proto.IsStorageClassBlobStore(ino.HybridCouldExtentsMigration.storageClass) {
				objEks := ino.HybridCouldExtentsMigration.sortedEks.(*SortedObjExtents)
				objEks.Range(func(ek proto.ObjExtentKey) bool {
					resp.MigrationCloudObjExtents = append(resp.MigrationCloudObjExtents, ek)
					log.LogInfof("action[InodeGetWithEk] migrationObjExtents append ek %v", ek)
					return true
				})
			}
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}

	}
	p.PacketErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) SetCreateTime(req *SetCreateTimeRequest, reqData []byte, p *Packet) (err error) {
	log.LogInfof("[SetCreateTime] mpId(%v), ino(%v), to set createTime(%v)",
		mp.config.PartitionId, req.Inode, req.CreateTime)
	if mp.verSeq != 0 {
		req.VerSeq = mp.GetVerSeq()
		reqData, err = json.Marshal(req)
		if err != nil {
			log.LogErrorf("[SetCreateTime] mpId(%v) ino(%v) createTime(%v), marshal err(%v) ",
				mp.config.PartitionId, req.Inode, req.CreateTime, err)
			return
		}
	}

	_, err = mp.submit(opFSMSetInodeCreateTime, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	p.PacketOkReply()
	return
}

func (mp *metaPartition) DeleteMigrationExtentKey(req *proto.DeleteMigrationExtentKeyRequest, p *Packet,
	remoteAddr string) (err error) {
	ino := NewInode(req.Inode, 0)
	var item BtreeItem
	if item = mp.inodeTree.Get(ino); item == nil {
		err = fmt.Errorf("mp %v inode %v reqeust cann't found", mp.config.PartitionId, ino.Inode)
		log.LogErrorf("action[UpdateExtentKeyAfterMigration] %v", err)
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		return
	} else {
		ino.UpdateHybridCloudParams(item.(*Inode))
	}

	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			auditlog.LogMigrationOp(remoteAddr, mp.GetVolName(), p.GetOpMsg(), req.GetFullPath(),
				err, time.Since(start).Milliseconds(), req.Inode, ino.StorageClass, ino.StorageClass)
		}()
	}
	//no migration extent key to delete
	if ino.HybridCouldExtentsMigration.storageClass == proto.StorageClass_Unspecified {
		p.PacketOkReply()
		return
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	resp, err := mp.submit(opFSMDeleteMigrationExtentKey, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

func (mp *metaPartition) internalNotifyFollowerToDeleteExtentKey(ino *Inode) {
	val, err := ino.Marshal()
	if err != nil {
		log.LogErrorf("action[internalNotifyFollowerToDeleteExtentKey] ino %v marshal failed:%v", ino.Inode, err)
		return
	}
	_, err = mp.submit(opFSMInternalDeleteMigrationExtentKey, val)
	if err != nil {
		log.LogErrorf("action[internalNotifyFollowerToDeleteExtentKey] ino %v submit opFSMDeleteMigrationExtentKey "+
			"failed:%v", ino.Inode, err)
		return
	}
	log.LogDebugf("action[internalNotifyFollowerToDeleteExtentKey] submit ino %v", ino.Inode)
}
