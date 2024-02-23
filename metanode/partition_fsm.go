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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// Apply applies the given operational commands.
func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	msg := &MetaItem{}
	mp.nonIdempotent.Lock()
	defer mp.nonIdempotent.Unlock()

	var dbWriteHandle interface{}

	defer func() {
		if err == nil {
			mp.uploadApplyID(index)
		}
	}()
	if err = msg.UnmarshalJson(command); err != nil {
		return
	}

	if dbWriteHandle, err = mp.inodeTree.CreateBatchWriteHandle(); err != nil {
		log.LogErrorf("action[Apply] create write batch handle failed:%v", err)
		return
	}

	mp.waitPersistCommitCnt++

	mp.inodeTree.SetApplyID(index)

	if mp.applyID >= index {
		log.LogErrorf("[Apply] skip apply duplicated log entry")
		return
	}

	//commit db
	defer func() {
		if err != nil {
			_ = mp.inodeTree.ReleaseBatchWriteHandle(dbWriteHandle)
			return
		}

		log.LogDebugf("[Apply] mp(%v) commit write handle", mp.config.PartitionId)
		err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, true)
		if err != nil {
			// NOTE: try to rollback?
			log.LogErrorf("[Apply] failed to commit write batch, is disk broken? err(%v)", err)
		}
	}()
	log.LogInfof("[Apply] apply mp(%v) op(%v)", mp.config.PartitionId, msg.Op)
	switch msg.Op {
	case opFSMCreateInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
			mp.inodeTree.SetCursor(ino.Inode)
		}
		resp, err = mp.fsmCreateInode(dbWriteHandle, ino)
	case opFSMCreateInodeQuota:
		qinode := &MetaQuotaInode{}
		if err = qinode.Unmarshal(msg.V); err != nil {
			return
		}
		ino := qinode.inode
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
			mp.inodeTree.SetCursor(ino.Inode)
		}
		if len(qinode.quotaIds) > 0 {
			mp.setInodeQuota(dbWriteHandle, qinode.quotaIds, ino.Inode)
		}
		resp, err = mp.fsmCreateInode(dbWriteHandle, ino)
		if resp == proto.OpOk {
			for _, quotaId := range qinode.quotaIds {
				mp.mqMgr.updateUsedInfo(0, 1, quotaId)
			}
		}
	case opFSMUnlinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		var status uint8
		status, err = mp.inodeInTx(ino.Inode)
		if err != nil {
			status = proto.OpErr
			return
		}
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp, err = mp.fsmUnlinkInode(dbWriteHandle, ino, 1, 0)
	case opFSMUnlinkInodeOnce:
		var inoOnce *InodeOnce
		if inoOnce, err = InodeOnceUnmarshal(msg.V); err != nil {
			return
		}
		ino := NewInode(inoOnce.Inode, 0)
		ino.setVer(inoOnce.VerSeq)
		resp, err = mp.fsmUnlinkInode(dbWriteHandle, ino, 1, inoOnce.UniqID)
	case opFSMUnlinkInodeBatch:
		var inodes InodeBatch
		inodes, err = InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmUnlinkInodeBatch(dbWriteHandle, inodes)
	case opFSMExtentTruncate:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmExtentsTruncate(dbWriteHandle, ino)
	case opFSMCreateLinkInode:
		var status uint8
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		status, err = mp.inodeInTx(ino.Inode)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp, err = mp.fsmCreateLinkInode(dbWriteHandle, ino, 0)
	case opFSMCreateLinkInodeOnce:
		var inoOnce *InodeOnce
		if inoOnce, err = InodeOnceUnmarshal(msg.V); err != nil {
			return
		}
		ino := NewInode(inoOnce.Inode, 0)
		resp, err = mp.fsmCreateLinkInode(dbWriteHandle, ino, inoOnce.UniqID)
	case opFSMEvictInode:
		var status uint8
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		status, err = mp.inodeInTx(ino.Inode)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp, err = mp.fsmEvictInode(dbWriteHandle, ino)
	case opFSMEvictInodeBatch:
		inodes, err := InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmBatchEvictInode(dbWriteHandle, inodes)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		resp, err = mp.fsmSetAttr(dbWriteHandle, req)
	case opFSMCreateDentry:
		var status uint8
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status, err = mp.dentryInTx(den.ParentId, den.Name)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = status
			return
		}

		resp, err = mp.fsmCreateDentry(dbWriteHandle, den, false)
	case opFSMDeleteDentry:
		var status uint8
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status, err = mp.dentryInTx(den.ParentId, den.Name)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = status
			return
		}

		resp, err = mp.fsmDeleteDentry(dbWriteHandle, den, false)
	case opFSMDeleteDentryBatch:
		db, err := DentryBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmBatchDeleteDentry(db)
	case opFSMUpdateDentry:
		var status uint8
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status, err = mp.dentryInTx(den.ParentId, den.Name)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = &DentryResponse{Status: status}
			return
		}

		resp, err = mp.fsmUpdateDentry(dbWriteHandle, den)
	case opFSMUpdatePartition:
		req := &UpdatePartitionReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmUpdatePartition(req.End)
	case opFSMExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendExtents(dbWriteHandle, ino)
	case opFSMExtentsAddWithCheck:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendExtentsWithCheck(dbWriteHandle, ino, false)
	case opFSMExtentSplit:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendExtentsWithCheck(dbWriteHandle, ino, true)
	case opFSMObjExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendObjExtents(dbWriteHandle, ino)
	case opFSMExtentsEmpty:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmExtentsEmpty(dbWriteHandle, ino)
	case opFSMClearInodeCache:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmClearInodeCache(dbWriteHandle, ino)
	case opFSMSentToChan:
		resp, err = mp.fsmSendToChan(dbWriteHandle, msg.V, true)
		if err != nil {
			return
		}
	case opFSMStoreTick:
		log.LogInfof("MP [%d] store tick wait:%d, water level:%d", mp.config.PartitionId, mp.waitPersistCommitCnt, GetDumpWaterLevel())
		if mp.waitPersistCommitCnt > GetDumpWaterLevel() {
			mp.waitPersistCommitCnt = 0
		}
		quotaRebuild := mp.mqMgr.statisticRebuildStart()
		uidRebuild := mp.acucumRebuildStart()
		uniqChecker := mp.uniqChecker.clone()
		// NOTE: already got lock
		var snap Snapshot
		snap, err = mp.GetSnapShot()
		if err != nil {
			log.LogErrorf("[Apply]: failed to open snapshot for mp(%v), store(%v), err(%v)", mp.config.PartitionId, mp.config.StoreMode, err)
			return
		}
		snapMsg := &storeMsg{
			command:      opFSMStoreTick,
			snap:         snap,
			quotaRebuild: quotaRebuild,
			uidRebuild:   uidRebuild,
			uniqChecker:  uniqChecker,
			multiVerList: mp.GetAllVerList(),
		}

		log.LogDebugf("opFSMStoreTick: quotaRebuild [%v] uidRebuild [%v]", quotaRebuild, uidRebuild)
		mp.storeChan <- snapMsg
	case opFSMInternalDeleteInode:
		err = mp.internalDelete(dbWriteHandle, msg.V)
	case opFSMInternalDeleteInodeBatch:
		err = mp.internalDeleteBatch(dbWriteHandle, msg.V)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmSetXAttr(dbWriteHandle, extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmRemoveXAttr(dbWriteHandle, extend)
	case opFSMUpdateXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmSetXAttr(dbWriteHandle, extend)
	case opFSMCreateMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmCreateMultipart(dbWriteHandle, multipart)
	case opFSMRemoveMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmRemoveMultipart(dbWriteHandle, multipart)
	case opFSMAppendMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmAppendMultipart(dbWriteHandle, multipart)
	case opFSMSyncCursor:
		var cursor uint64
		cursor = binary.BigEndian.Uint64(msg.V)
		if cursor > mp.config.Cursor {
			mp.config.Cursor = cursor
			mp.inodeTree.SetCursor(cursor)
		}
	case opFSMSyncTxID:
		var txID uint64
		txID = binary.BigEndian.Uint64(msg.V)
		if txID > mp.txProcessor.txManager.txIdAlloc.getTransactionID() {
			mp.txProcessor.txManager.txIdAlloc.setTransactionID(txID)
			mp.txProcessor.txManager.txTree.SetTxId(txID)
		}
	case opFSMTxInit:
		txInfo := proto.NewTransactionInfo(0, 0)
		if err = txInfo.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxInit(dbWriteHandle, txInfo)
	case opFSMTxCreateInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < txIno.Inode.Inode {
			mp.config.Cursor = txIno.Inode.Inode
			mp.inodeTree.SetCursor(txIno.Inode.Inode)
		}
		resp, err = mp.fsmTxCreateInode(dbWriteHandle, txIno, []uint32{})
	case opFSMTxCreateInodeQuota:
		qinode := &TxMetaQuotaInode{}
		if err = qinode.Unmarshal(msg.V); err != nil {
			return
		}
		txIno := qinode.txinode
		if mp.config.Cursor < txIno.Inode.Inode {
			mp.config.Cursor = txIno.Inode.Inode
			mp.inodeTree.SetCursor(txIno.Inode.Inode)
		}
		if len(qinode.quotaIds) > 0 {
			mp.setInodeQuota(dbWriteHandle, qinode.quotaIds, txIno.Inode.Inode)
		}
		resp, err = mp.fsmTxCreateInode(dbWriteHandle, txIno, qinode.quotaIds)
		if resp == proto.OpOk {
			for _, quotaId := range qinode.quotaIds {
				mp.mqMgr.updateUsedInfo(0, 1, quotaId)
			}
		}
	case opFSMTxCreateDentry:
		txDen := NewTxDentry(0, "", 0, 0, nil, nil)
		if err = txDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxCreateDentry(dbWriteHandle, txDen)
	case opFSMTxSetState:
		req := &proto.TxSetStateRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmTxSetState(dbWriteHandle, req)
	case opFSMTxCommitRM:
		req := &proto.TransactionInfo{}
		if err = req.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxCommitRM(dbWriteHandle, req)
	case opFSMTxRollbackRM:
		req := &proto.TransactionInfo{}
		if err = req.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxRollbackRM(dbWriteHandle, req)
	case opFSMTxCommit:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmTxCommit(dbWriteHandle, req.TxID)
	case opFSMTxRollback:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmTxRollback(dbWriteHandle, req.TxID)
	case opFSMTxDelete:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmTxDelete(dbWriteHandle, req.TxID)
	case opFSMTxDeleteDentry:
		txDen := NewTxDentry(0, "", 0, 0, nil, nil)
		if err = txDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxDeleteDentry(dbWriteHandle, txDen)
	case opFSMTxUnlinkInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxUnlinkInode(dbWriteHandle, txIno)
	case opFSMTxUpdateDentry:
		// txDen := NewTxDentry(0, "", 0, 0, nil)
		txUpdateDen := NewTxUpdateDentry(nil, nil, nil)
		if err = txUpdateDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxUpdateDentry(dbWriteHandle, txUpdateDen)
	case opFSMTxCreateLinkInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmTxCreateLinkInode(dbWriteHandle, txIno)
	case opFSMSetInodeQuotaBatch:
		req := &proto.BatchSetMetaserverQuotaReuqest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmSetInodeQuotaBatch(dbWriteHandle, req)
	case opFSMDeleteInodeQuotaBatch:
		req := &proto.BatchDeleteMetaserverQuotaReuqest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmDeleteInodeQuotaBatch(dbWriteHandle, req)
	case opFSMUniqID:
		resp = mp.fsmUniqID(msg.V)
	case opFSMUniqCheckerEvict:
		req := &fsmEvictUniqCheckerRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		err = mp.fsmUniqCheckerEvict(req)
	case opFSMVersionOp:
		err = mp.fsmVersionOp(msg.V)
	default:
		// do nothing
	}

	return
}

func (mp *metaPartition) runVersionOp() {
	mp.verUpdateChan = make(chan []byte, 100)
	for {
		select {
		case verData := <-mp.verUpdateChan:
			mp.submit(opFSMVersionOp, verData)
		case <-mp.stopC:
			log.LogWarnf("runVersionOp exit!")
			return
		}
	}
}

func (mp *metaPartition) fsmVersionOp(reqData []byte) (err error) {
	mp.multiVersionList.RWLock.Lock()
	defer mp.multiVersionList.RWLock.Unlock()

	var opData VerOpData
	if err = json.Unmarshal(reqData, &opData); err != nil {
		log.LogErrorf("action[fsmVersionOp] mp[%v] unmarshal error %v", mp.config.PartitionId, err)
		return
	}

	log.LogInfof("action[fsmVersionOp] volname [%v] mp[%v] seq [%v], op [%v]", mp.config.VolName, mp.config.PartitionId, opData.VerSeq, opData.Op)
	if opData.Op == proto.CreateVersionPrepare {
		cnt := len(mp.multiVersionList.VerList)
		if cnt > 0 {
			lastVersion := mp.multiVersionList.VerList[cnt-1]
			if lastVersion.Ver > opData.VerSeq {
				log.LogWarnf("action[HandleVersionOp] createVersionPrepare reqeust seq [%v] less than last exist snapshot seq [%v]", opData.VerSeq, lastVersion.Ver)
				return
			} else if lastVersion.Ver == opData.VerSeq {
				log.LogWarnf("action[HandleVersionOp] CreateVersionPrepare request seq [%v] already exist status [%v]", opData.VerSeq, lastVersion.Status)
				return
			}
		}
		newVer := &proto.VolVersionInfo{
			Status: proto.VersionPrepare,
			Ver:    opData.VerSeq,
		}
		mp.verSeq = opData.VerSeq
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, newVer)

		log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] seq [%v], op [%v], seqArray size %v", mp.config.PartitionId, opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList))
	} else if opData.Op == proto.CreateVersionCommit {
		cnt := len(mp.multiVersionList.VerList)
		if cnt > 0 {
			if mp.multiVersionList.VerList[cnt-1].Ver > opData.VerSeq {
				log.LogWarnf("action[fsmVersionOp] mp[%v] reqeust seq [%v] less than last exist snapshot seq [%v]", mp.config.PartitionId,
					opData.VerSeq, mp.multiVersionList.VerList[cnt-1].Ver)
				return
			}
			if mp.multiVersionList.VerList[cnt-1].Ver == opData.VerSeq {
				if mp.multiVersionList.VerList[cnt-1].Status != proto.VersionPrepare {
					log.LogWarnf("action[fsmVersionOp] mp[%v] reqeust seq [%v] Equal last exist snapshot seq [%v] but with status [%v]", mp.config.PartitionId,
						mp.multiVersionList.VerList[cnt-1].Ver, opData.VerSeq, mp.multiVersionList.VerList[cnt-1].Status)
				}
				mp.multiVersionList.VerList[cnt-1].Status = proto.VersionNormal
				return
			}
		}
		newVer := &proto.VolVersionInfo{
			Status: proto.VersionNormal,
			Ver:    opData.VerSeq,
		}
		mp.verSeq = opData.VerSeq
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, newVer)

		log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] seq [%v], op [%v], seqArray size %v", mp.config.PartitionId, opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList))
	} else if opData.Op == proto.DeleteVersion {
		for i, ver := range mp.multiVersionList.VerList {
			if i == len(mp.multiVersionList.VerList)-1 {
				log.LogWarnf("action[fsmVersionOp] mp[%v] seq [%v], op [%v], seqArray size %v newest ver [%v] reque ver [%v]",
					mp.config.PartitionId, opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList), ver.Ver, opData.VerSeq)
				break
			}
			if ver.Ver == opData.VerSeq {
				log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] seq [%v], op [%v], VerList %v",
					mp.config.PartitionId, opData.VerSeq, opData.Op, mp.multiVersionList.VerList)
				// mp.multiVersionList = append(mp.multiVersionList[:i], mp.multiVersionList[i+1:]...)
				mp.multiVersionList.VerList = append(mp.multiVersionList.VerList[:i], mp.multiVersionList.VerList[i+1:]...)
				log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] seq [%v], op [%v], VerList %v",
					mp.config.PartitionId, opData.VerSeq, opData.Op, mp.multiVersionList.VerList)
				break
			}
		}
	} else if opData.Op == proto.SyncBatchVersionList {
		log.LogInfof("action[fsmVersionOp] mp[%v] before update:with seq [%v] verlist %v opData.VerList %v",
			mp.config.PartitionId, mp.verSeq, mp.multiVersionList.VerList, opData.VerList)

		lastVer := mp.multiVersionList.GetLastVer()
		for _, info := range opData.VerList {
			if info.Ver > lastVer {
				mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, info)
				log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] after update:with seq [%v] verlist %v",
					mp.config.PartitionId, mp.verSeq, mp.multiVersionList.VerList)
			}
		}
		mp.verSeq = mp.multiVersionList.GetLastVer()
		log.LogInfof("action[fsmVersionOp] updateVerList mp[%v] after update:with seq [%v] verlist %v",
			mp.config.PartitionId, mp.verSeq, mp.multiVersionList.VerList)
	} else {
		log.LogErrorf("action[fsmVersionOp] mp[%v] with seq [%v] process op type %v seq [%v] not found",
			mp.config.PartitionId, mp.verSeq, opData.Op, opData.VerSeq)
	}
	return
}

// ApplyMemberChange  apply changes to the raft member.
func (mp *metaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func() {
		if err == nil {
			mp.uploadApplyID(index)
		}
	}()
	// change memory status
	var (
		updated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		req := &proto.AddMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confAddNode(req, index)
	case raftproto.ConfRemoveNode:
		req := &proto.RemoveMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confRemoveNode(req, index)
	case raftproto.ConfUpdateNode:
		// updated, err = mp.confUpdateNode(req, index)
	default:
		// do nothing
	}
	if err != nil {
		return
	}
	if updated {
		mp.config.sortPeers()
		if err = mp.persistMetadata(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] err[%v].", err)
			return
		}
	}
	return
}

// Snapshot returns the snapshot of the current meta partition.
func (mp *metaPartition) Snapshot() (snap raftproto.Snapshot, err error) {
	snap, err = newMetaItemIterator(mp)
	return
}

func (mp *metaPartition) ResetDbByNewDir(newDBDir string) (err error) {
	if err = mp.db.CloseDb(); err != nil {
		log.LogErrorf("[ResetDbByNewDir] Close old db failed:%v", err.Error())
		return
	}

	if err = mp.db.ReleaseRocksDb(); err != nil {
		log.LogErrorf("[ResetDbByNewDir] release db dir failed:%v", err.Error())
		return
	}

	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		log.LogErrorf("[ResetDbByNewDir] failed to create tmp dir")
		return
	}
	tmpDir = path.Join(tmpDir, "rocksdb")

	// NOTE: atomic rename dir
	if err = os.Rename(mp.getRocksDbRootDir(), tmpDir); err != nil && os.IsNotExist(err) {
		log.LogErrorf("[ResetDbByNewDir] failed to rename dir, err(%v)", err)
		return
	}

	if err = os.Rename(newDBDir, mp.getRocksDbRootDir()); err != nil {
		log.LogErrorf("[ResetDbByNewDir] rename db dir[%s --> %s] failed:%v", newDBDir, mp.getRocksDbRootDir(), err.Error())
		return
	}

	if err = os.RemoveAll(tmpDir); err != nil {
		log.LogErrorf("[ResetDbByNewDir] failed to remove garbage, err(%v)", err)
		err = nil
	}

	if err = mp.db.ReOpenDb(mp.getRocksDbRootDir(), mp.config.RocksWalFileSize, mp.config.RocksWalMemSize,
		mp.config.RocksLogFileSize, mp.config.RocksLogReversedTime, mp.config.RocksLogReVersedCnt, mp.config.RocksWalTTL); err != nil {
		log.LogErrorf("[ResetDbByNewDir] reopen db[%s] failed:%v", mp.getRocksDbRootDir(), err.Error())
		return
	}

	partitionId := strconv.FormatUint(mp.config.PartitionId, 10)
	fileInfoList, err := ioutil.ReadDir(path.Join(mp.config.RocksDBDir, partitionPrefix+partitionId))
	if err != nil {
		return nil
	}

	for _, file := range fileInfoList {
		if file.IsDir() && strings.HasPrefix(file.Name(), "db_") {
			oldName := path.Join(mp.config.RocksDBDir, partitionPrefix+partitionId, file.Name())
			//last snap shot failed, exist db_timestamp dir, remove it
			//newName := path.Join(mp.config.RocksDBDir, partitionPrefix + partitionId, "expired_" + file.Name())
			os.RemoveAll(oldName)
		}
	}
	return nil
}

func newRocksdbHandle(newDir string) (db *RocksDbInfo, err error) {
	if _, err = os.Stat(newDir); err == nil {
		os.RemoveAll(newDir)
	}
	os.MkdirAll(newDir, 0x755)
	err = nil

	db = NewRocksDb()
	//apply snapshot, tmp rocks db
	if err = db.OpenDb(newDir, 0, 0, 0, 0, 0, 0); err != nil {
		log.LogErrorf("open db failed, error(%v)", err)
		return
	}
	return
}

func (mp *metaPartition) resetMetaTree(metaTree *MetaTree) (err error) {
	if mp.HasMemStore() {
		mp.inodeTree = metaTree.InodeTree
		mp.dentryTree = metaTree.DentryTree
		mp.extendTree = metaTree.ExtendTree
		mp.multipartTree = metaTree.MultipartTree
	}

	if mp.HasRocksDBStore() {
		if err = mp.initRocksDBTree(); err != nil {
			return
		}
	}
	return
}

func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) (err error) {
	var (
		data          []byte
		index         int
		appIndexID    uint64
		txID          uint64
		uniqID        uint64
		cursor        uint64
		uniqChecker   = newUniqChecker()
		verList       []*proto.VolVersionInfo
		db            *RocksDbInfo
		dbWriteHandle interface{}
	)

	nowStr := strconv.FormatInt(time.Now().Unix(), 10)
	var txTree TransactionTree
	var txRbInodeTree TransactionRollbackInodeTree
	var txRbDentryTree TransactionRollbackDentryTree
	newDBDir := ""

	// NOTE: for rocksdb
	// open a temp db for write
	if mp.HasRocksDBStore() {
		newDBDir = mp.getRocksDbRootDir() + "_" + nowStr
		db, err = newRocksdbHandle(newDBDir)
		if err != nil {
			log.LogErrorf("ApplyBaseSnapshot: new rocksdb handle failed, metaPartition id(%d) error(%v)", mp.config.PartitionId, err)
			return
		}
	}

	// NOTE: init meta trees
	metaTree := newMetaTree(mp.config.StoreMode, db)
	if metaTree == nil {
		log.LogErrorf("ApplyBaseSnapshot: new meta tree for mp[%v] failed", mp.config.PartitionId)
		err = fmt.Errorf("mp[%v] new meta tree failed", mp.config.PartitionId)
		return
	}

	// NOTE: init transaction trees
	if mp.HasRocksDBStore() {
		inodeRocks := metaTree.InodeTree.(*InodeRocks)
		tree := inodeRocks.RocksTree
		txTree, _ = NewTransactionRocks(tree)
		txRbInodeTree, _ = NewTransactionRollbackInodeRocks(tree)
		txRbDentryTree, _ = NewTransactionRollbackDentryRocks(tree)
	}

	if mp.HasMemStore() {
		txTree = &TransactionBTree{NewBtree()}
		txRbInodeTree = &TransactionRollbackInodeBTree{NewBtree()}
		txRbDentryTree = &TransactionRollbackDentryBTree{NewBtree()}
	}

	// NOTE: open write batch for write
	dbWriteHandle, err = metaTree.InodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) create batch write handle failed:%v", mp.config.PartitionId, err)
		return
	}

	blockUntilStoreSnapshot := func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		log.LogWarnf("ApplySnapshot: start to block until store snapshot to disk, mp[%v], appid %d", mp.config.PartitionId, appIndexID)
		start := time.Now()

		for {
			select {
			case <-ticker.C:
				if time.Since(start) > time.Minute*20 {
					msg := fmt.Sprintf("ApplySnapshot: wait store snapshot timeout after 20 minutes, mp %d, appId %d, storeId %d",
						mp.config.PartitionId, appIndexID, mp.storedApplyId)
					log.LogErrorf(msg)
					err = fmt.Errorf(msg)
					return
				}

				msg := fmt.Sprintf("ApplySnapshot: start check storedApplyId, mp %d appId %d, storeAppId %d, cost %s",
					mp.config.PartitionId, appIndexID, mp.storedApplyId, time.Since(start).String())
				if time.Since(start) > time.Minute {
					log.LogWarnf("still block after one minute, msg %s", msg)
				} else {
					log.LogInfo(msg)
				}

				if mp.storedApplyId >= appIndexID {
					log.LogWarnf("ApplySnapshot: store snapshot success, msg %s", msg)
					return
				}
			case <-mp.stopC:
				log.LogWarnf("ApplySnapshot: revice stop signal, exit now, partition(%d), applyId(%d)", mp.config.PartitionId, mp.applyID)
				err = errors.New("server has been shutdown when block")
				return
			}
		}
	}

	defer func() {
		if err != nil && err != io.EOF {
			if err == ErrRocksdbOperation {
				log.LogErrorf("[ApplySnapshot] failed to operate rocksdb, err(%v)", err)
				exporter.WarningRocksdbError(fmt.Sprintf("action[ApplyBaseSnapshot] clusterID[%s] volumeName[%s] partitionID[%v]"+
					" apply base snapshot failed witch rocksdb error", mp.manager.metaNode.clusterId, mp.config.VolName,
					mp.config.PartitionId))
			}
			log.LogErrorf("ApplyBaseSnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
			if db != nil {
				_ = db.CloseDb()
			}
			return
		}
		if err == io.EOF {
			log.LogDebugf("[ApplySnapshot] mp(%v) apply snapshot finish!", mp.config.PartitionId)
			if db != nil {
				if err = db.CloseDb(); err != nil {
					log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) recover from snap failed; Close new db(dir:%s) failed:%s", mp.config.PartitionId, db.dir, err.Error())
					return
				}

				log.LogDebugf("[ApplySnapshot] mp(%v) publish rocksdb", mp.config.PartitionId)
				if err = mp.ResetDbByNewDir(newDBDir); err != nil {
					log.LogErrorf("afterApplySnapshotHandle: metaPartition(%v) recover from snap failed; Reset db failed:%s", mp.config.PartitionId, err.Error())
					return
				}
			}

			mp.freeList = newFreeList()

			mp.applyID = appIndexID
			mp.config.UniqId = uniqID
			mp.txProcessor.txManager.txIdAlloc.setTransactionID(txID)
			if err = mp.resetMetaTree(metaTree); err != nil {
				log.LogErrorf("afterApplySnapshotHandle: metaPartition(%v) recover from snap failed; reset meta tree failed:%s", mp.config.PartitionId, err.Error())
				return
			}
			mp.config.Cursor = cursor
			mp.inodeTree.SetCursor(cursor)
			mp.txProcessor.txManager.txTree = txTree
			mp.txProcessor.txResource.txRbInodeTree = txRbInodeTree
			mp.txProcessor.txResource.txRbDentryTree = txRbDentryTree
			mp.txProcessor.txManager.txTree.SetTxId(txID)
			mp.uniqChecker = uniqChecker
			mp.multiVersionList.VerList = make([]*proto.VolVersionInfo, len(verList))
			copy(mp.multiVersionList.VerList, verList)
			mp.verSeq = mp.multiVersionList.GetLastVer()
			log.LogInfof("mp[%v] updateVerList (%v) seq [%v]", mp.config.PartitionId, mp.multiVersionList.VerList, mp.verSeq)
			err = nil
			// store message
			snap, err := mp.GetSnapShot()
			if err != nil {
				log.LogErrorf("[ApplySnapshot]: failed to open snapshot for mp(%v), store(%v), err(%v)", mp.config.PartitionId, mp.config.StoreMode, err)
				return
			}
			mp.storeChan <- &storeMsg{
				command:      opFSMStoreTick,
				snap:         snap,
				uniqChecker:  uniqChecker.clone(),
				multiVerList: mp.GetVerList(),
			}
			select {
			case mp.extReset <- struct{}{}:
				log.LogDebugf("ApplySnapshot: finish with EOF: partitionID(%v) applyID(%v), txID(%v), uniqID(%v), cursor(%v)",
					mp.config.PartitionId, mp.applyID, mp.txProcessor.txManager.txIdAlloc.getTransactionID(), mp.config.UniqId, mp.config.Cursor)
				blockUntilStoreSnapshot()
				return
			case <-mp.stopC:
				log.LogWarnf("ApplySnapshot: revice stop signal, exit now, partition(%d), applyId(%d)", mp.config.PartitionId, mp.applyID)
				err = errors.New("server has been shutdown")
				return
			}
		}
		log.LogErrorf("ApplySnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
	}()

	leaderSnapFormatVer := uint32(math.MaxUint32)

	for {
		data, err = iter.Next()
		if err != nil {
			// NOTE: handle error in defer
			return
		}

		if index == 0 {
			appIndexID = binary.BigEndian.Uint64(data)
			log.LogDebugf("ApplySnapshot: partitionID(%v), temporary uint64 appIndexID:%v", mp.config.PartitionId, appIndexID)
		}

		snap := NewMetaItem(0, nil, nil)
		if err = snap.UnmarshalBinary(data); err != nil {
			if index == 0 {
				// for compatibility, if leader send snapshot format int version_0, index=0 is applyId in uint64 and
				// will cause snap.UnmarshalBinary err, then just skip index=0 and continue with the other fields
				log.LogInfof("ApplySnapshot: snap.UnmarshalBinary failed in index=0, partitionID(%v), assuming snapshot format version_0",
					mp.config.PartitionId)
				index++
				leaderSnapFormatVer = SnapFormatVersion_0
				continue
			}

			log.LogInfof("ApplySnapshot: snap.UnmarshalBinary failed, partitionID(%v) index(%v)", mp.config.PartitionId, index)
			err = errors.New("unmarshal snap data failed")
			return
		}

		log.LogDebugf("[ApplySnapshot] mp(%v) index(%v) apply(%v)", mp.config.PartitionId, index, snap.Op)

		if index == 0 {
			if snap.Op != opFSMSnapFormatVersion {
				// check whether the snapshot format matches, if snap.UnmarshalBinary has no err for index 0, it should be opFSMSnapFormatVersion
				err = fmt.Errorf("ApplySnapshot: snapshot format not match, partitionID(%v), index:%v, expect snap.Op:%v, actual snap.Op:%v",
					mp.config.PartitionId, index, opFSMSnapFormatVersion, snap.Op)
				log.LogWarn(err.Error())
				return
			}

			// check whether the snapshot format version number matches
			leaderSnapFormatVer = binary.BigEndian.Uint32(snap.V)
			if leaderSnapFormatVer != mp.manager.metaNode.raftSyncSnapFormatVersion {
				log.LogWarnf("ApplySnapshot: snapshot format not match, partitionID(%v), index:%v, expect ver:%v, actual ver:%v",
					mp.config.PartitionId, index, mp.manager.metaNode.raftSyncSnapFormatVersion, leaderSnapFormatVer)
			}

			index++
			continue
		}

		index++
		switch snap.Op {
		case opFSMApplyId:
			appIndexID = binary.BigEndian.Uint64(snap.V)
			log.LogDebugf("ApplySnapshot: partitionID(%v) appIndexID:%v", mp.config.PartitionId, appIndexID)
		case opFSMTxId:
			txID = binary.BigEndian.Uint64(snap.V)
			log.LogDebugf("ApplySnapshot: partitionID(%v) txID:%v", mp.config.PartitionId, txID)
		case opFSMCursor:
			cursor = binary.BigEndian.Uint64(snap.V)
			log.LogDebugf("ApplySnapshot: partitionID(%v) cursor:%v", mp.config.PartitionId, cursor)
		case opFSMUniqIDSnap:
			uniqID = binary.BigEndian.Uint64(snap.V)
			log.LogDebugf("ApplySnapshot: partitionID(%v) uniqId:%v", mp.config.PartitionId, uniqID)
		case opFSMCreateInode:
			ino := NewInode(0, 0)

			// TODO Unhandled errors
			ino.UnmarshalKey(snap.K)
			ino.UnmarshalValue(snap.V)
			if cursor < ino.Inode {
				cursor = ino.Inode
			}
			if _, _, err = metaTree.InodeTree.Create(dbWriteHandle, ino, true); err != nil {
				log.LogErrorf("ApplyBaseSnapshot: create inode failed, partitionID(%v) inode(%v)", mp.config.PartitionId, ino)
				return
			}
			log.LogDebugf("ApplySnapshot: create inode: partitonID(%v) inode[%v].", mp.config.PartitionId, ino)
		case opFSMCreateDentry:
			dentry := &Dentry{}
			if err = dentry.UnmarshalKey(snap.K); err != nil {
				return
			}
			if err = dentry.UnmarshalValue(snap.V); err != nil {
				return
			}
			if _, _, err = metaTree.DentryTree.Create(dbWriteHandle, dentry, true); err != nil {
				log.LogErrorf("ApplyBaseSnapshot: create dentry failed, partitionID(%v) dentry(%v) error(%v)", mp.config.PartitionId, dentry, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
		case opFSMSetXAttr:
			var extend *Extend
			if extend, err = NewExtendFromBytes(snap.V); err != nil {
				return
			}
			if _, _, err = metaTree.ExtendTree.Create(dbWriteHandle, extend, true); err != nil {
				log.LogErrorf("ApplyBaseSnapshot: create extentd attributes failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, extend, err)
				return
			}
			log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)",
				mp.config.PartitionId, extend)
		case opFSMCreateMultipart:
			multipart := MultipartFromBytes(snap.V)
			if _, _, err = metaTree.MultipartTree.Create(dbWriteHandle, multipart, true); err != nil {
				log.LogErrorf("ApplySnapshot: create multipart failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, multipart, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
		case opFSMTxSnapshot:
			txInfo := proto.NewTransactionInfo(0, proto.TxTypeUndefined)
			txInfo.Unmarshal(snap.V)
			err = txTree.Put(dbWriteHandle, txInfo)
			if err != nil {
				log.LogErrorf("ApplySnapshot: put tx failed, partitionID(%v) tx(%v) err(%v)", mp.config.PartitionId, txInfo, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create transaction: partitionID(%v) txInfo(%v)", mp.config.PartitionId, txInfo)
		case opFSMTxRbInodeSnapshot:
			txRbInode := NewTxRollbackInode(nil, []uint32{}, nil, 0)
			txRbInode.Unmarshal(snap.V)
			err = txRbInodeTree.Put(dbWriteHandle, txRbInode)
			if err != nil {
				log.LogErrorf("ApplySnapshot: put rb inode failed, partitionID(%v) rb inode(%v) err(%v)", mp.config.PartitionId, txRbInode, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create txRbInode: partitionID(%v) txRbinode[%v]", mp.config.PartitionId, txRbInode)
		case opFSMTxRbDentrySnapshot:
			txRbDentry := NewTxRollbackDentry(nil, nil, 0)
			txRbDentry.Unmarshal(snap.V)
			err = txRbDentryTree.Put(dbWriteHandle, txRbDentry)
			if err != nil {
				log.LogErrorf("ApplySnapshot: put rb dentry failed, partitionID(%v) rb dentry(%v) err(%v)", mp.config.PartitionId, txRbDentry, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create txRbDentry: partitionID(%v) txRbDentry(%v)", mp.config.PartitionId, txRbDentry)
		case opFSMVerListSnapShot:
			json.Unmarshal(snap.V, &verList)
			log.LogDebugf("ApplySnapshot: create verList: partitionID(%v) snap.V(%v) verList(%v)", mp.config.PartitionId, snap.V, verList)
		case opExtentFileSnapshot:
			fileName := string(snap.K)
			fileName = path.Join(mp.config.RootDir, fileName)
			if err = os.WriteFile(fileName, snap.V, 0o644); err != nil {
				log.LogErrorf("ApplySnapshot: write snap extent delete file fail: partitionID(%v) err(%v)",
					mp.config.PartitionId, err)
			}
			log.LogDebugf("ApplySnapshot: write snap extent delete file: partitonID(%v) filename(%v).",
				mp.config.PartitionId, fileName)
		case opFSMUniqCheckerSnap:
			if err = uniqChecker.UnMarshal(snap.V); err != nil {
				log.LogErrorf("ApplyUniqChecker: write snap uniqChecker fail")
				return
			}
			log.LogDebugf("ApplySnapshot: write snap uniqChecker")

		default:
			if leaderSnapFormatVer != math.MaxUint32 && leaderSnapFormatVer > mp.manager.metaNode.raftSyncSnapFormatVersion {
				log.LogWarnf("ApplySnapshot: unknown op=%d, leaderSnapFormatVer:%v, mySnapFormatVer:%v, skip it",
					snap.Op, leaderSnapFormatVer, mp.manager.metaNode.raftSyncSnapFormatVersion)
			} else {
				err = fmt.Errorf("unknown Op=%d", snap.Op)
				return
			}
		}

		// if count, err = metaTree.InodeTree.BatchWriteCount(dbWriteHandle); err != nil {
		// 	return
		// }

		// if count < DefMaxWriteBatchCount {
		// 	continue
		// }

		if err = metaTree.InodeTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, true); err != nil {
			log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) commit write handle failed:%v", mp.config.PartitionId, err)
			dbWriteHandle = nil
			return
		}
		if dbWriteHandle, err = metaTree.InodeTree.CreateBatchWriteHandle(); err != nil {
			log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) create batch write handle failed:%v", mp.config.PartitionId, err)
			return
		}
	}
}

// HandleFatalEvent handles the fatal errors.
func (mp *metaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	exporter.Warning(fmt.Sprintf("action[HandleFatalEvent] err[%v].", err))
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
	panic(err.Err)
}

// HandleLeaderChange handles the leader changes.
func (mp *metaPartition) HandleLeaderChange(leader uint64) {
	exporter.Warning(fmt.Sprintf("metaPartition(%v) changeLeader to (%v)", mp.config.PartitionId, leader))
	if mp.config.NodeId == leader {
		localIp := mp.manager.metaNode.localAddr
		if localIp == "" {
			localIp = "127.0.0.1"
		}

		conn, err := net.DialTimeout("tcp", net.JoinHostPort(localIp, serverPort), time.Second)
		if err != nil {
			log.LogErrorf(fmt.Sprintf("HandleLeaderChange serverPort not exsit ,error %v", err))
			exporter.Warning(fmt.Sprintf("mp[%v] HandleLeaderChange serverPort not exsit ,error %v", mp.config.PartitionId, err))
			go mp.raftPartition.TryToLeader(mp.config.PartitionId)
			return
		}
		log.LogDebugf("[metaPartition] HandleLeaderChange close conn %v, nodeId: %v, leader: %v", serverPort, mp.config.NodeId, leader)
		exporter.Warning(fmt.Sprintf("[metaPartition]mp[%v] HandleLeaderChange close conn %v, nodeId: %v, leader: %v", mp.config.PartitionId, serverPort, mp.config.NodeId, leader))
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
	}
	if mp.config.NodeId != leader {
		log.LogDebugf("[metaPartition] pid: %v HandleLeaderChange become unleader nodeId: %v, leader: %v", mp.config.PartitionId, mp.config.NodeId, leader)
		exporter.Warning(fmt.Sprintf("[metaPartition] pid: %v HandleLeaderChange become unleader nodeId: %v, leader: %v", mp.config.PartitionId, mp.config.NodeId, leader))
		mp.storeChan <- &storeMsg{
			command: stopStoreTick,
		}
		return
	}
	mp.storeChan <- &storeMsg{
		command: startStoreTick,
	}

	log.LogDebugf("[metaPartition] pid: %v HandleLeaderChange become leader conn %v, nodeId: %v, leader: %v", mp.config.PartitionId, serverPort, mp.config.NodeId, leader)
	exporter.Warning(fmt.Sprintf("[metaPartition] pid: %v HandleLeaderChange become leader conn %v, nodeId: %v, leader: %v", mp.config.PartitionId, serverPort, mp.config.NodeId, leader))
	if mp.config.Start == 0 && mp.config.Cursor == 0 {
		id, err := mp.nextInodeID()
		if err != nil {
			log.LogFatalf("[HandleLeaderChange] init root inode id: %s.", err.Error())
			exporter.Warning(fmt.Sprintf("[HandleLeaderChange] pid %v init root inode id: %s.", mp.config.PartitionId, err.Error()))
		}
		ino := NewInode(id, proto.Mode(os.ModePerm|os.ModeDir))
		go mp.initInode(ino)
	}
}

// Put puts the given key-value pair (operation key and operation request) into the raft store.
func (mp *metaPartition) submit(op uint32, data []byte) (resp interface{}, err error) {
	log.LogDebugf("submit. op [%v]", op)
	snap := NewMetaItem(0, nil, nil)
	snap.Op = op
	if data != nil {
		snap.V = data
	}
	cmd, err := snap.MarshalJson()
	if err != nil {
		return
	}

	// submit to the raft store
	resp, err = mp.raftPartition.Submit(cmd)
	log.LogDebugf("submit. op [%v] done", op)
	return
}

func (mp *metaPartition) uploadApplyID(applyId uint64) {
	atomic.StoreUint64(&mp.applyID, applyId)
	if mp.HasRocksDBStore() {
		if math.Abs(float64(mp.applyID-mp.inodeTree.GetPersistentApplyID())) > maximumApplyIdDifference {
			//persist to rocksdb
			if err := mp.inodeTree.PersistBaseInfo(); err != nil {
				log.LogErrorf("action[uploadApplyID] persist base info failed:%v", err)
			}
		}
	}
}

func (mp *metaPartition) getApplyID() (applyId uint64) {
	return atomic.LoadUint64(&mp.applyID)
}

func (mp *metaPartition) getCommittedID() (committedId uint64) {
	status := mp.raftPartition.Status()
	return status.Commit
}
