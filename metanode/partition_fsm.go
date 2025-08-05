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
	"math"
	"net"
	"os"
	"path"
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
	defer func() {
		if r := recover(); r != nil {
			panicMsg := fmt.Sprintf("[metaPartition.Apply] mpId(%v) op(%v) occurred panic, err(%v), ",
				mp.config.PartitionId, msg.Op, r)
			log.LogWarn(panicMsg)
			panic(panicMsg)
		}

		if err == nil {
			mp.uploadApplyID(index)
		}
	}()
	if err = msg.UnmarshalJson(command); err != nil {
		return
	}

	mp.nonIdempotent.Lock()
	defer mp.nonIdempotent.Unlock()

	switch msg.Op {
	case opFSMCreateInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		resp = mp.fsmCreateInode(ino)
	case opFSMCreateInodeQuota:
		qinode := &MetaQuotaInode{}
		if err = qinode.Unmarshal(msg.V); err != nil {
			return
		}
		ino := qinode.inode
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		if len(qinode.quotaIds) > 0 {
			mp.setInodeQuota(qinode.quotaIds, ino.Inode)
		}
		resp = mp.fsmCreateInode(ino)
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

		status := mp.inodeInTx(ino.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp = mp.fsmUnlinkInode(ino, 0)
	case opFSMUnlinkInodeOnce:
		var inoOnceWithVersion *InodeOnceWithVersion
		if inoOnceWithVersion, err = InodeOnceUnmarshal(msg.V); err != nil {
			return
		}

		status := mp.inodeInTx(inoOnceWithVersion.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		ino := NewInode(inoOnceWithVersion.Inode, 0)
		ino.setVer(inoOnceWithVersion.VerSeq)
		resp = mp.fsmUnlinkInode(ino, inoOnceWithVersion.UniqID)
	case opFSMUnlinkInodeBatch:
		inodes, err := InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmUnlinkInodeBatch(inodes)
	case opFSMExtentTruncate:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmExtentsTruncate(ino)
	case opFSMCreateLinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		status := mp.inodeInTx(ino.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp = mp.fsmCreateLinkInode(ino, 0)
	case opFSMCreateLinkInodeOnce:
		var inoOnceWithVersion *InodeOnceWithVersion
		if inoOnceWithVersion, err = InodeOnceUnmarshal(msg.V); err != nil {
			return
		}
		status := mp.inodeInTx(inoOnceWithVersion.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		ino := NewInode(inoOnceWithVersion.Inode, 0)
		ino.setVer(inoOnceWithVersion.VerSeq)
		resp = mp.fsmCreateLinkInode(ino, inoOnceWithVersion.UniqID)
	case opFSMEvictInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		status := mp.inodeInTx(ino.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp = mp.fsmEvictInode(ino)
	case opFSMEvictInodeBatch:
		inodes, err := InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmBatchEvictInode(inodes)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.fsmSetAttr(req)
	case opFSMCreateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status := mp.dentryInTx(den.ParentId, den.Name)
		if status != proto.OpOk {
			resp = status
			return
		}

		resp = mp.fsmCreateDentry(den, false)
	case opFSMDeleteDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status := mp.dentryInTx(den.ParentId, den.Name)
		if status != proto.OpOk {
			resp = &DentryResponse{
				Status: status,
			}
			return
		}

		resp = mp.fsmDeleteDentry(den, false)
	case opFSMDeleteDentryBatch:
		db, err := DentryBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmBatchDeleteDentry(db)
	case opFSMUpdateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status := mp.dentryInTx(den.ParentId, den.Name)
		if status != proto.OpOk {
			resp = &DentryResponse{Status: status}
			return
		}

		resp = mp.fsmUpdateDentry(den)
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
		resp = mp.fsmAppendExtents(ino)
	case opFSMExtentsAddWithCheck:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendExtentsWithCheck(ino, false)
	case opFSMExtentSplit:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendExtentsWithCheck(ino, true)
	case opFSMObjExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendObjExtents(ino)
	case opFSMExtentsEmpty:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmExtentsEmpty(ino)
	case opFSMClearInodeCache:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmClearInodeCache(ino)
	case opFSMSentToChan:
		resp = mp.fsmSendToChan(msg.V, false)
	case opFSMSentToChanWithVer:
		resp = mp.fsmSendToChan(msg.V, true)
	case opFSMStoreTick:
		inodeTree := mp.inodeTree.GetTree()
		dentryTree := mp.dentryTree.GetTree()
		extendTree := mp.extendTree.GetTree()
		multipartTree := mp.multipartTree.GetTree()
		txTree := mp.txProcessor.txManager.txTree.GetTree()
		txRbInodeTree := mp.txProcessor.txResource.txRbInodeTree.GetTree()
		txRbDentryTree := mp.txProcessor.txResource.txRbDentryTree.GetTree()
		txId := mp.txProcessor.txManager.txIdAlloc.getTransactionID()
		quotaRebuild := mp.mqMgr.statisticRebuildStart()
		uidRebuild := mp.acucumRebuildStart()
		uniqId := mp.GetUniqId()
		uniqChecker := mp.uniqChecker.clone()
		msg := &storeMsg{
			command:        opFSMStoreTick,
			applyIndex:     index,
			txId:           txId,
			inodeTree:      inodeTree,
			dentryTree:     dentryTree,
			extendTree:     extendTree,
			multipartTree:  multipartTree,
			txTree:         txTree,
			txRbInodeTree:  txRbInodeTree,
			txRbDentryTree: txRbDentryTree,
			quotaRebuild:   quotaRebuild,
			uidRebuild:     uidRebuild,
			uniqId:         uniqId,
			uniqChecker:    uniqChecker,
			multiVerList:   mp.GetAllVerList(),
		}
		log.LogDebugf("opFSMStoreTick: quotaRebuild [%v] uidRebuild [%v]", quotaRebuild, uidRebuild)
		mp.storeChan <- msg
	case opFSMInternalDeleteInode:
		err = mp.internalDelete(msg.V)
	case opFSMInternalDeleteInodeBatch:
		err = mp.internalDeleteBatch(msg.V)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmSetXAttr(extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmRemoveXAttr(extend)
	case opFSMUpdateXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmSetXAttr(extend)
	case opFSMLockDir:
		req := &proto.LockDirRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmLockDir(req)
	case opFSMCreateMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmCreateMultipart(multipart)
	case opFSMRemoveMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmRemoveMultipart(multipart)
	case opFSMAppendMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmAppendMultipart(multipart)
	case opFSMSyncCursor:
		var cursor uint64
		cursor = binary.BigEndian.Uint64(msg.V)
		if cursor > mp.config.Cursor {
			mp.config.Cursor = cursor
		}
	case opFSMSyncTxID:
		var txID uint64
		txID = binary.BigEndian.Uint64(msg.V)
		if txID > mp.txProcessor.txManager.txIdAlloc.getTransactionID() {
			mp.txProcessor.txManager.txIdAlloc.setTransactionID(txID)
		}
	case opFSMTxInit:
		txInfo := proto.NewTransactionInfo(0, 0)
		if err = txInfo.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxInit(txInfo)
	case opFSMTxCreateInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < txIno.Inode.Inode {
			mp.config.Cursor = txIno.Inode.Inode
		}
		resp = mp.fsmTxCreateInode(txIno, []uint32{})
	case opFSMTxCreateInodeQuota:
		qinode := &TxMetaQuotaInode{}
		if err = qinode.Unmarshal(msg.V); err != nil {
			return
		}
		txIno := qinode.txinode
		if mp.config.Cursor < txIno.Inode.Inode {
			mp.config.Cursor = txIno.Inode.Inode
		}
		if len(qinode.quotaIds) > 0 {
			mp.setInodeQuota(qinode.quotaIds, txIno.Inode.Inode)
		}
		resp = mp.fsmTxCreateInode(txIno, qinode.quotaIds)
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
		resp = mp.fsmTxCreateDentry(txDen)
	case opFSMTxSetState:
		req := &proto.TxSetStateRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmTxSetState(req)
	case opFSMTxCommitRM:
		req := &proto.TransactionInfo{}
		if err = req.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxCommitRM(req)
	case opFSMTxRollbackRM:
		req := &proto.TransactionInfo{}
		if err = req.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxRollbackRM(req)
	case opFSMTxCommit:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmTxCommit(req.TxID)
	case opFSMTxRollback:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmTxRollback(req.TxID)
	case opFSMTxDelete:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmTxDelete(req.TxID)
	case opFSMTxDeleteDentry:
		txDen := NewTxDentry(0, "", 0, 0, nil, nil)
		if err = txDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxDeleteDentry(txDen)
	case opFSMTxUnlinkInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxUnlinkInode(txIno)
	case opFSMTxUpdateDentry:
		// txDen := NewTxDentry(0, "", 0, 0, nil)
		txUpdateDen := NewTxUpdateDentry(nil, nil, nil)
		if err = txUpdateDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxUpdateDentry(txUpdateDen)
	case opFSMTxCreateLinkInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxCreateLinkInode(txIno)
	case opFSMSetInodeQuotaBatch:
		req := &proto.BatchSetMetaserverQuotaReuqest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmSetInodeQuotaBatch(req)
	case opFSMDeleteInodeQuotaBatch:
		req := &proto.BatchDeleteMetaserverQuotaReuqest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmDeleteInodeQuotaBatch(req)
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
	case opFSMRenewalForbiddenMigration:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmRenewalInodeForbiddenMigration(ino)
	case opFSMUpdateExtentKeyAfterMigration:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			log.LogWarnf("[Apply] mp(%v) opFSMUpdateExtentKeyAfterMigration Unmarshal inode failed: %v",
				mp.config.PartitionId, err.Error())
			return
		}
		resp = mp.fsmUpdateExtentKeyAfterMigration(ino)
	case opFSMSetInodeCreateTime:
		req := &SetCreateTimeRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.fsmSetCreateTime(req)
	case opFSMInternalBatchFreeInodeMigrationExtentKey:
		err = mp.fsmInternalBatchFreeMigrationExtentKey(msg.V)
	case opFSMSetMigrationExtentKeyDeleteImmediately:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			log.LogWarnf("[Apply] mp(%v) opFSMSetMigrationExtentKeyDeleteImmediately Unmarshal inode failed: %v",
				mp.config.PartitionId, err.Error())
			return
		}
		resp = mp.fsmSetMigrationExtentKeyDeleteImmediately(ino)
	case opFSMUpdateInodeMeta:
		req := &UpdateInodeMetaRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.fsmUpdateInodeMeta(req)
	default:
		// do nothing
	case opFSMSyncInodeAccessTime:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmSyncInodeAccessTime(ino)
	case opFSMBatchSyncInodeATime:
		if len(msg.V) < 8 || len(msg.V)%8 != 0 {
			err = fmt.Errorf("opFSMBatchSyncInodeATime: msg is not valid, mp %d, len(%d)", mp.config.PartitionId, len(msg.V))
			return
		}

		resp = mp.fsmBatchSyncInodeAccessTime(msg.V)
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
	if mp.manager != nil && mp.manager.metaNode != nil && !mp.manager.metaNode.clusterEnableSnapshot {
		err = fmt.Errorf("clusterEnableSnapshot not enabled")
		log.LogErrorf("action[fsmVersionOp] mp[%v] err %v", mp.config.PartitionId, err)
		return nil
	}
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
	mp.nonIdempotent.Lock()
	defer mp.nonIdempotent.Unlock()

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

func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) (err error) {
	var (
		data           []byte
		index          int
		appIndexID     uint64
		txID           uint64
		uniqID         uint64
		cursor         uint64
		inodeTree      = NewBtree()
		dentryTree     = NewBtree()
		extendTree     = NewBtree()
		multipartTree  = NewBtree()
		txTree         = NewBtree()
		txRbInodeTree  = NewBtree()
		txRbDentryTree = NewBtree()
		uniqChecker    = newUniqChecker()
		verList        []*proto.VolVersionInfo
	)

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
		if err == io.EOF {
			mp.applyID = appIndexID
			mp.config.UniqId = uniqID
			mp.txProcessor.txManager.txIdAlloc.setTransactionID(txID)
			mp.inodeTree = inodeTree
			mp.dentryTree = dentryTree
			mp.extendTree = extendTree
			mp.multipartTree = multipartTree
			mp.config.Cursor = cursor
			mp.txProcessor.txManager.txTree = txTree
			mp.txProcessor.txResource.txRbInodeTree = txRbInodeTree
			mp.txProcessor.txResource.txRbDentryTree = txRbDentryTree
			mp.uniqChecker = uniqChecker
			mp.multiVersionList.VerList = make([]*proto.VolVersionInfo, len(verList))
			copy(mp.multiVersionList.VerList, verList)
			mp.verSeq = mp.multiVersionList.GetLastVer()
			log.LogInfof("mp[%v] updateVerList (%v) seq [%v]", mp.config.PartitionId, mp.multiVersionList.VerList, mp.verSeq)
			err = nil
			// store message
			mp.storeChan <- &storeMsg{
				command:        opFSMStoreTick,
				applyIndex:     mp.applyID,
				txId:           mp.txProcessor.txManager.txIdAlloc.getTransactionID(),
				inodeTree:      mp.inodeTree.GetTree(),
				dentryTree:     mp.dentryTree.GetTree(),
				extendTree:     mp.extendTree.GetTree(),
				multipartTree:  mp.multipartTree.GetTree(),
				txTree:         mp.txProcessor.txManager.txTree.GetTree(),
				txRbInodeTree:  mp.txProcessor.txResource.txRbInodeTree.GetTree(),
				txRbDentryTree: mp.txProcessor.txResource.txRbDentryTree.GetTree(),
				uniqId:         mp.GetUniqId(),
				uniqChecker:    uniqChecker.clone(),
				multiVerList:   mp.GetVerList(),
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

	var leaderSnapFormatVer uint32
	leaderSnapFormatVer = math.MaxUint32

	for {
		data, err = iter.Next()
		if err != nil {
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
			inodeTree.ReplaceOrInsert(ino, true)
			log.LogDebugf("ApplySnapshot: create inode: partitonID(%v) inode[%v].", mp.config.PartitionId, ino)
		case opFSMCreateDentry:
			dentry := &Dentry{}
			if err = dentry.UnmarshalKey(snap.K); err != nil {
				return
			}
			if err = dentry.UnmarshalValue(snap.V); err != nil {
				return
			}
			dentryTree.ReplaceOrInsert(dentry, true)
			log.LogDebugf("ApplySnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
		case opFSMSetXAttr:
			var extend *Extend
			if extend, err = NewExtendFromBytes(snap.V); err != nil {
				return
			}
			extendTree.ReplaceOrInsert(extend, true)
			log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)",
				mp.config.PartitionId, extend)
		case opFSMCreateMultipart:
			multipart := MultipartFromBytes(snap.V)
			multipartTree.ReplaceOrInsert(multipart, true)
			log.LogDebugf("ApplySnapshot: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
		case opFSMTxSnapshot:
			txInfo := proto.NewTransactionInfo(0, proto.TxTypeUndefined)
			txInfo.Unmarshal(snap.V)
			txTree.ReplaceOrInsert(txInfo, true)
			log.LogDebugf("ApplySnapshot: create transaction: partitionID(%v) txInfo(%v)", mp.config.PartitionId, txInfo)
		case opFSMTxRbInodeSnapshot:
			txRbInode := NewTxRollbackInode(nil, []uint32{}, nil, 0)
			txRbInode.Unmarshal(snap.V)
			txRbInodeTree.ReplaceOrInsert(txRbInode, true)
			log.LogDebugf("ApplySnapshot: create txRbInode: partitionID(%v) txRbinode[%v]", mp.config.PartitionId, txRbInode)
		case opFSMTxRbDentrySnapshot:
			txRbDentry := NewTxRollbackDentry(nil, nil, 0)
			txRbDentry.Unmarshal(snap.V)
			txRbDentryTree.ReplaceOrInsert(txRbDentry, true)
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
	log.LogDebugf(fmt.Sprintf("metaPartition(%v) changeLeader to (%v)", mp.config.PartitionId, leader))
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
		log.LogDebugf("[metaPartition] pid: %v HandleLeaderChange close conn %v, nodeId: %v, leader: %v",
			mp.config.PartitionId, serverPort, mp.config.NodeId, leader)
		exporter.Warning(fmt.Sprintf("[metaPartition]mp[%v] HandleLeaderChange close conn %v, nodeId: %v, leader: %v",
			mp.config.PartitionId, serverPort, mp.config.NodeId, leader))
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
	}
	if mp.config.NodeId != leader {
		log.LogDebugf("[metaPartition] pid: %v HandleLeaderChange become unleader nodeId: %v, leader: %v",
			mp.config.PartitionId, mp.config.NodeId, leader)
		exporter.Warning(fmt.Sprintf("[metaPartition] pid: %v HandleLeaderChange become unleader nodeId: %v, leader: %v",
			mp.config.PartitionId, mp.config.NodeId, leader))
		mp.storeChan <- &storeMsg{
			command: stopStoreTick,
		}
		return
	}
	mp.storeChan <- &storeMsg{
		command: startStoreTick,
	}

	log.LogDebugf("[metaPartition] pid: %v HandleLeaderChange become leader conn %v, nodeId: %v, leader: %v",
		mp.config.PartitionId, serverPort, mp.config.NodeId, leader)
	exporter.Warning(fmt.Sprintf("[metaPartition] pid: %v HandleLeaderChange become leader conn %v, nodeId: %v, leader: %v",
		mp.config.PartitionId, serverPort, mp.config.NodeId, leader))
	if mp.config.Start == 0 && mp.config.Cursor == 0 {
		id, err := mp.nextInodeID()
		if err != nil {
			log.LogFatalf("[HandleLeaderChange] init root inode id: %s.", err.Error())
			exporter.Warning(fmt.Sprintf("[HandleLeaderChange] pid %v init root inode id: %s.", mp.config.PartitionId, err.Error()))
		}
		ino := NewInode(id, proto.Mode(os.ModePerm|os.ModeDir))
		ino.StorageClass = mp.GetVolStorageClass()
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
}

func (mp *metaPartition) getApplyID() (applyId uint64) {
	return atomic.LoadUint64(&mp.applyID)
}

func (mp *metaPartition) getCommittedID() (committedId uint64) {
	status := mp.raftPartition.Status()
	return status.Commit
}
