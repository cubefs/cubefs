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
	"context"
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
		if err == nil {
			mp.uploadApplyID(index)
		}
	}()
	if err = msg.UnmarshalJson(command); err != nil {
		return
	}

	// TODO: apply with traceid.
	span, ctx := proto.StartSpanFromContext(context.TODO(), "")
	ctx = proto.ContextWithSpan(ctx, span)

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
		resp = mp.fsmCreateInode(ctx, ino)
	case opFSMCreateInodeQuota:
		qinode := &MetaQuotaInode{}
		if err = qinode.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		ino := qinode.inode
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		if len(qinode.quotaIds) > 0 {
			mp.setInodeQuota(ctx, qinode.quotaIds, ino.Inode)
		}
		resp = mp.fsmCreateInode(ctx, ino)
		if resp == proto.OpOk {
			for _, quotaId := range qinode.quotaIds {
				mp.mqMgr.updateUsedInfo(ctx, 0, 1, quotaId)
			}
		}
	case opFSMUnlinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}

		status := mp.inodeInTx(ctx, ino.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp = mp.fsmUnlinkInode(ctx, ino, 0)
	case opFSMUnlinkInodeOnce:
		var inoOnce *InodeOnce
		if inoOnce, err = InodeOnceUnmarshal(msg.V); err != nil {
			return
		}
		ino := NewInode(inoOnce.Inode, 0)
		ino.setVer(inoOnce.VerSeq)
		resp = mp.fsmUnlinkInode(ctx, ino, inoOnce.UniqID)
	case opFSMUnlinkInodeBatch:
		inodes, err := InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmUnlinkInodeBatch(ctx, inodes)
	case opFSMExtentTruncate:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmExtentsTruncate(ctx, ino)
	case opFSMCreateLinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		status := mp.inodeInTx(ctx, ino.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp = mp.fsmCreateLinkInode(ctx, ino, 0)
	case opFSMCreateLinkInodeOnce:
		var inoOnce *InodeOnce
		if inoOnce, err = InodeOnceUnmarshal(msg.V); err != nil {
			return
		}
		ino := NewInode(inoOnce.Inode, 0)
		resp = mp.fsmCreateLinkInode(ctx, ino, inoOnce.UniqID)
	case opFSMEvictInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		status := mp.inodeInTx(ctx, ino.Inode)
		if status != proto.OpOk {
			resp = &InodeResponse{Status: status}
			return
		}
		resp = mp.fsmEvictInode(ctx, ino)
	case opFSMEvictInodeBatch:
		inodes, err := InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmBatchEvictInode(ctx, inodes)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.fsmSetAttr(ctx, req)
	case opFSMCreateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status := mp.dentryInTx(ctx, den.ParentId, den.Name)
		if status != proto.OpOk {
			resp = status
			return
		}

		resp = mp.fsmCreateDentry(ctx, den, false)
	case opFSMDeleteDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status := mp.dentryInTx(ctx, den.ParentId, den.Name)
		if status != proto.OpOk {
			resp = status
			return
		}

		resp = mp.fsmDeleteDentry(ctx, den, false)
	case opFSMDeleteDentryBatch:
		db, err := DentryBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmBatchDeleteDentry(ctx, db)
	case opFSMUpdateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}

		status := mp.dentryInTx(ctx, den.ParentId, den.Name)
		if status != proto.OpOk {
			resp = &DentryResponse{Status: status}
			return
		}

		resp = mp.fsmUpdateDentry(ctx, den)
	case opFSMUpdatePartition:
		req := &UpdatePartitionReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmUpdatePartition(ctx, req.End)
	case opFSMExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendExtents(ctx, ino)
	case opFSMExtentsAddWithCheck:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendExtentsWithCheck(ctx, ino, false)
	case opFSMExtentSplit:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendExtentsWithCheck(ctx, ino, true)
	case opFSMObjExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendObjExtents(ctx, ino)
	case opFSMExtentsEmpty:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmExtentsEmpty(ctx, ino)
	case opFSMClearInodeCache:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmClearInodeCache(ctx, ino)
	case opFSMSentToChan:
		resp = mp.fsmSendToChan(ctx, msg.V, true)
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
			uniqChecker:    uniqChecker,
			multiVerList:   mp.GetAllVerList(),
		}
		span.Debugf("opFSMStoreTick: quotaRebuild [%v] uidRebuild [%v]", quotaRebuild, uidRebuild)
		mp.storeChan <- msg
	case opFSMInternalDeleteInode:
		err = mp.internalDelete(ctx, msg.V)
	case opFSMInternalDeleteInodeBatch:
		err = mp.internalDeleteBatch(ctx, msg.V)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(ctx, msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(ctx, msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmSetXAttr(ctx, extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmRemoveXAttr(ctx, extend)
	case opFSMUpdateXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmSetXAttr(ctx, extend)
	case opFSMCreateMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmCreateMultipart(ctx, multipart)
	case opFSMRemoveMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmRemoveMultipart(ctx, multipart)
	case opFSMAppendMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmAppendMultipart(ctx, multipart)
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
		resp = mp.fsmTxInit(ctx, txInfo)
	case opFSMTxCreateInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < txIno.Inode.Inode {
			mp.config.Cursor = txIno.Inode.Inode
		}
		resp = mp.fsmTxCreateInode(ctx, txIno, []uint32{})
	case opFSMTxCreateInodeQuota:
		qinode := &TxMetaQuotaInode{}
		if err = qinode.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		txIno := qinode.txinode
		if mp.config.Cursor < txIno.Inode.Inode {
			mp.config.Cursor = txIno.Inode.Inode
		}
		if len(qinode.quotaIds) > 0 {
			mp.setInodeQuota(ctx, qinode.quotaIds, txIno.Inode.Inode)
		}
		resp = mp.fsmTxCreateInode(ctx, txIno, qinode.quotaIds)
		if resp == proto.OpOk {
			for _, quotaId := range qinode.quotaIds {
				mp.mqMgr.updateUsedInfo(ctx, 0, 1, quotaId)
			}
		}
	case opFSMTxCreateDentry:
		txDen := NewTxDentry(0, "", 0, 0, nil, nil)
		if err = txDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxCreateDentry(ctx, txDen)
	case opFSMTxSetState:
		req := &proto.TxSetStateRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmTxSetState(ctx, req)
	case opFSMTxCommitRM:
		req := &proto.TransactionInfo{}
		if err = req.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxCommitRM(ctx, req)
	case opFSMTxRollbackRM:
		req := &proto.TransactionInfo{}
		if err = req.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxRollbackRM(ctx, req)
	case opFSMTxCommit:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmTxCommit(ctx, req.TxID)
	case opFSMTxRollback:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmTxRollback(ctx, req.TxID)
	case opFSMTxDelete:
		req := &proto.TxApplyRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmTxDelete(ctx, req.TxID)
	case opFSMTxDeleteDentry:
		txDen := NewTxDentry(0, "", 0, 0, nil, nil)
		if err = txDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxDeleteDentry(ctx, txDen)
	case opFSMTxUnlinkInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxUnlinkInode(ctx, txIno)
	case opFSMTxUpdateDentry:
		// txDen := NewTxDentry(0, "", 0, 0, nil)
		txUpdateDen := NewTxUpdateDentry(nil, nil, nil)
		if err = txUpdateDen.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxUpdateDentry(ctx, txUpdateDen)
	case opFSMTxCreateLinkInode:
		txIno := NewTxInode(0, 0, nil)
		if err = txIno.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmTxCreateLinkInode(ctx, txIno)
	case opFSMSetInodeQuotaBatch:
		req := &proto.BatchSetMetaserverQuotaReuqest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmSetInodeQuotaBatch(ctx, req)
	case opFSMDeleteInodeQuotaBatch:
		req := &proto.BatchDeleteMetaserverQuotaReuqest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmDeleteInodeQuotaBatch(ctx, req)
	case opFSMUniqID:
		resp = mp.fsmUniqID(ctx, msg.V)
	case opFSMUniqCheckerEvict:
		req := &fsmEvictUniqCheckerRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		err = mp.fsmUniqCheckerEvict(ctx, req)
	case opFSMVersionOp:
		err = mp.fsmVersionOp(ctx, msg.V)
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
			mp.submit(context.Background(), opFSMVersionOp, verData)
		case <-mp.stopC:
			log.Warn("runVersionOp exit!")
			return
		}
	}
}

func (mp *metaPartition) fsmVersionOp(ctx context.Context, reqData []byte) (err error) {
	span := getSpan(ctx).WithOperation(fmt.Sprintf("fsmVersionOp-vol(%s)mp(%d)", mp.config.VolName, mp.config.PartitionId))
	mp.multiVersionList.RWLock.Lock()
	defer mp.multiVersionList.RWLock.Unlock()

	var opData VerOpData
	if err = json.Unmarshal(reqData, &opData); err != nil {
		span.Errorf("unmarshal error %v", err)
		return
	}

	span.Infof("seq [%v], op [%v]", opData.VerSeq, opData.Op)
	if opData.Op == proto.CreateVersionPrepare {
		cnt := len(mp.multiVersionList.VerList)
		if cnt > 0 {
			lastVersion := mp.multiVersionList.VerList[cnt-1]
			if lastVersion.Ver > opData.VerSeq {
				span.Warnf("reqeust seq [%v] less than last exist snapshot seq [%v]", opData.VerSeq, lastVersion.Ver)
				return
			} else if lastVersion.Ver == opData.VerSeq {
				span.Warnf("request seq [%v] already exist status [%v]", opData.VerSeq, lastVersion.Status)
				return
			}
		}
		newVer := &proto.VolVersionInfo{
			Status: proto.VersionPrepare,
			Ver:    opData.VerSeq,
		}
		mp.verSeq = opData.VerSeq
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, newVer)

		span.Infof("updateVerList seq [%v], op [%v], seqArray size %v", opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList))
	} else if opData.Op == proto.CreateVersionCommit {
		cnt := len(mp.multiVersionList.VerList)
		if cnt > 0 {
			if mp.multiVersionList.VerList[cnt-1].Ver > opData.VerSeq {
				span.Warnf("reqeust seq [%v] less than last exist snapshot seq [%v]",
					opData.VerSeq, mp.multiVersionList.VerList[cnt-1].Ver)
				return
			}
			if mp.multiVersionList.VerList[cnt-1].Ver == opData.VerSeq {
				if mp.multiVersionList.VerList[cnt-1].Status != proto.VersionPrepare {
					span.Warnf("reqeust seq [%v] Equal last exist snapshot seq [%v] but with status [%v]",
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

		span.Infof("updateVerList seq [%v], op [%v], seqArray size %v", opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList))
	} else if opData.Op == proto.DeleteVersion {
		for i, ver := range mp.multiVersionList.VerList {
			if i == len(mp.multiVersionList.VerList)-1 {
				span.Warnf("seq [%v], op [%v], seqArray size %v newest ver [%v] reque ver [%v]",
					opData.VerSeq, opData.Op, len(mp.multiVersionList.VerList), ver.Ver, opData.VerSeq)
				break
			}
			if ver.Ver == opData.VerSeq {
				span.Infof("updateVerList seq [%v], op [%v], VerList %v", opData.VerSeq, opData.Op, mp.multiVersionList.VerList)
				// mp.multiVersionList = append(mp.multiVersionList[:i], mp.multiVersionList[i+1:]...)
				mp.multiVersionList.VerList = append(mp.multiVersionList.VerList[:i], mp.multiVersionList.VerList[i+1:]...)
				span.Infof("updateVerList seq [%v], op [%v], VerList %v", opData.VerSeq, opData.Op, mp.multiVersionList.VerList)
				break
			}
		}
	} else if opData.Op == proto.SyncBatchVersionList {
		span.Infof("before update: seq [%v] verlist %v opData.VerList %v", mp.verSeq, mp.multiVersionList.VerList, opData.VerList)

		lastVer := mp.multiVersionList.GetLastVer()
		for _, info := range opData.VerList {
			if info.Ver > lastVer {
				mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, info)
				span.Infof("updateVerList after update: seq [%v] verlist %v", mp.verSeq, mp.multiVersionList.VerList)
			}
		}
		mp.verSeq = mp.multiVersionList.GetLastVer()
		span.Infof("updateVerList after update: seq [%v] verlist %v", mp.verSeq, mp.multiVersionList.VerList)
	} else {
		span.Errorf("with seq [%v] process op type %v seq [%v] not found", mp.verSeq, opData.Op, opData.VerSeq)
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
	span, ctx := spanContext()
	switch confChange.Type {
	case raftproto.ConfAddNode:
		req := &proto.AddMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confAddNode(ctx, req, index)
	case raftproto.ConfRemoveNode:
		req := &proto.RemoveMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confRemoveNode(ctx, req, index)
	case raftproto.ConfUpdateNode:
		// updated, err = mp.confUpdateNode(req, index)
	default:
		// do nothing
	}
	if err != nil {
		span.Error(err)
		return
	}
	if updated {
		mp.config.sortPeers()
		if err = mp.persistMetadata(ctx); err != nil {
			span.Errorf("action[ApplyMemberChange] err[%v].", err)
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

	span, _ := spanContextPrefix("ApplySnapshot-")

	blockUntilStoreSnapshot := func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		span.Warnf("start to block until store snapshot to disk, mp[%v], appid %d", mp.config.PartitionId, appIndexID)
		start := time.Now()

		for {
			select {
			case <-ticker.C:
				if time.Since(start) > time.Minute*20 {
					msg := fmt.Sprintf("ApplySnapshot: wait store snapshot timeout after 20 minutes, mp %d, appId %d, storeId %d",
						mp.config.PartitionId, appIndexID, mp.storedApplyId)
					span.Error(msg)
					err = fmt.Errorf(msg)
					return
				}

				msg := fmt.Sprintf("ApplySnapshot: start check storedApplyId, mp %d appId %d, storeAppId %d, cost %s",
					mp.config.PartitionId, appIndexID, mp.storedApplyId, time.Since(start).String())
				if time.Since(start) > time.Minute {
					span.Warnf("still block after one minute, msg %s", msg)
				} else {
					span.Info(msg)
				}

				if mp.storedApplyId >= appIndexID {
					span.Warnf("store snapshot success, msg %s", msg)
					return
				}
			case <-mp.stopC:
				span.Warnf("revice stop signal, exit now, partition(%d), applyId(%d)", mp.config.PartitionId, mp.applyID)
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
			span.Infof("mp[%v] updateVerList (%v) seq [%v]", mp.config.PartitionId, mp.multiVersionList.VerList, mp.verSeq)
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
				uniqChecker:    uniqChecker.clone(),
				multiVerList:   mp.GetVerList(),
			}
			select {
			case mp.extReset <- struct{}{}:
				span.Debugf("finish with EOF: partitionID(%v) applyID(%v), txID(%v), uniqID(%v), cursor(%v)",
					mp.config.PartitionId, mp.applyID, mp.txProcessor.txManager.txIdAlloc.getTransactionID(), mp.config.UniqId, mp.config.Cursor)
				blockUntilStoreSnapshot()
				return
			case <-mp.stopC:
				span.Warnf("revice stop signal, exit now, partition(%d), applyId(%d)", mp.config.PartitionId, mp.applyID)
				err = errors.New("server has been shutdown")
				return
			}
		}
		span.Errorf("stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
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
			span.Debugf("partitionID(%v), temporary uint64 appIndexID:%v", mp.config.PartitionId, appIndexID)
		}

		snap := NewMetaItem(0, nil, nil)
		if err = snap.UnmarshalBinary(data); err != nil {
			if index == 0 {
				// for compatibility, if leader send snapshot format int version_0, index=0 is applyId in uint64 and
				// will cause snap.UnmarshalBinary err, then just skip index=0 and continue with the other fields
				span.Infof("snap.UnmarshalBinary failed in index=0, partitionID(%v), assuming snapshot format version_0",
					mp.config.PartitionId)
				index++
				leaderSnapFormatVer = SnapFormatVersion_0
				continue
			}

			span.Infof("snap.UnmarshalBinary failed, partitionID(%v) index(%v)", mp.config.PartitionId, index)
			err = errors.New("unmarshal snap data failed")
			return
		}

		if index == 0 {
			if snap.Op != opFSMSnapFormatVersion {
				// check whether the snapshot format matches, if snap.UnmarshalBinary has no err for index 0, it should be opFSMSnapFormatVersion
				err = fmt.Errorf("ApplySnapshot: snapshot format not match, partitionID(%v), index:%v, expect snap.Op:%v, actual snap.Op:%v",
					mp.config.PartitionId, index, opFSMSnapFormatVersion, snap.Op)
				span.Warn(err.Error())
				return
			}

			// check whether the snapshot format version number matches
			leaderSnapFormatVer = binary.BigEndian.Uint32(snap.V)
			if leaderSnapFormatVer != mp.manager.metaNode.raftSyncSnapFormatVersion {
				span.Warnf("snapshot format not match, partitionID(%v), index:%v, expect ver:%v, actual ver:%v",
					mp.config.PartitionId, index, mp.manager.metaNode.raftSyncSnapFormatVersion, leaderSnapFormatVer)
			}

			index++
			continue
		}

		index++
		switch snap.Op {
		case opFSMApplyId:
			appIndexID = binary.BigEndian.Uint64(snap.V)
			span.Debugf("partitionID(%v) appIndexID:%v", mp.config.PartitionId, appIndexID)
		case opFSMTxId:
			txID = binary.BigEndian.Uint64(snap.V)
			span.Debugf("partitionID(%v) txID:%v", mp.config.PartitionId, txID)
		case opFSMCursor:
			cursor = binary.BigEndian.Uint64(snap.V)
			span.Debugf("partitionID(%v) cursor:%v", mp.config.PartitionId, cursor)
		case opFSMUniqIDSnap:
			uniqID = binary.BigEndian.Uint64(snap.V)
			span.Debugf("partitionID(%v) uniqId:%v", mp.config.PartitionId, uniqID)
		case opFSMCreateInode:
			ino := NewInode(0, 0)

			// TODO Unhandled errors
			ino.UnmarshalKey(snap.K)
			ino.UnmarshalValue(snap.V)
			if cursor < ino.Inode {
				cursor = ino.Inode
			}
			inodeTree.ReplaceOrInsert(ino, true)
			span.Debugf("create inode: partitonID(%v) inode[%v].", mp.config.PartitionId, ino)
		case opFSMCreateDentry:
			dentry := &Dentry{}
			if err = dentry.UnmarshalKey(snap.K); err != nil {
				return
			}
			if err = dentry.UnmarshalValue(snap.V); err != nil {
				return
			}
			dentryTree.ReplaceOrInsert(dentry, true)
			span.Debugf("create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
		case opFSMSetXAttr:
			var extend *Extend
			if extend, err = NewExtendFromBytes(snap.V); err != nil {
				return
			}
			extendTree.ReplaceOrInsert(extend, true)
			span.Debugf("set extend attributes: partitionID(%v) extend(%v)",
				mp.config.PartitionId, extend)
		case opFSMCreateMultipart:
			multipart := MultipartFromBytes(snap.V)
			multipartTree.ReplaceOrInsert(multipart, true)
			span.Debugf("create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
		case opFSMTxSnapshot:
			txInfo := proto.NewTransactionInfo(0, proto.TxTypeUndefined)
			txInfo.Unmarshal(snap.V)
			txTree.ReplaceOrInsert(txInfo, true)
			span.Debugf("create transaction: partitionID(%v) txInfo(%v)", mp.config.PartitionId, txInfo)
		case opFSMTxRbInodeSnapshot:
			txRbInode := NewTxRollbackInode(nil, []uint32{}, nil, 0)
			txRbInode.Unmarshal(snap.V)
			txRbInodeTree.ReplaceOrInsert(txRbInode, true)
			span.Debugf("create txRbInode: partitionID(%v) txRbinode[%v]", mp.config.PartitionId, txRbInode)
		case opFSMTxRbDentrySnapshot:
			txRbDentry := NewTxRollbackDentry(nil, nil, 0)
			txRbDentry.Unmarshal(snap.V)
			txRbDentryTree.ReplaceOrInsert(txRbDentry, true)
			span.Debugf("create txRbDentry: partitionID(%v) txRbDentry(%v)", mp.config.PartitionId, txRbDentry)
		case opFSMVerListSnapShot:
			json.Unmarshal(snap.V, &verList)
			span.Debugf("create verList: partitionID(%v) snap.V(%v) verList(%v)", mp.config.PartitionId, snap.V, verList)
		case opExtentFileSnapshot:
			fileName := string(snap.K)
			fileName = path.Join(mp.config.RootDir, fileName)
			if err = os.WriteFile(fileName, snap.V, 0o644); err != nil {
				span.Errorf("write snap extent delete file fail: partitionID(%v) err(%v)", mp.config.PartitionId, err)
			}
			span.Debugf("write snap extent delete file: partitonID(%v) filename(%v).",
				mp.config.PartitionId, fileName)
		case opFSMUniqCheckerSnap:
			if err = uniqChecker.UnMarshal(snap.V); err != nil {
				span.Errorf("ApplyUniqChecker: write snap uniqChecker fail")
				return
			}
			span.Debug("write snap uniqChecker")

		default:
			if leaderSnapFormatVer != math.MaxUint32 && leaderSnapFormatVer > mp.manager.metaNode.raftSyncSnapFormatVersion {
				span.Warnf("unknown op=%d, leaderSnapFormatVer:%v, mySnapFormatVer:%v, skip it",
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
	log.Fatalf("action[HandleFatalEvent] err[%v].", err)
	panic(err.Err)
}

// HandleLeaderChange handles the leader changes.
func (mp *metaPartition) HandleLeaderChange(leader uint64) {
	span, ctx := spanContext()
	exporter.Warning(fmt.Sprintf("metaPartition(%v) changeLeader to (%v)", mp.config.PartitionId, leader))
	if mp.config.NodeId == leader {
		localIp := mp.manager.metaNode.localAddr
		if localIp == "" {
			localIp = "127.0.0.1"
		}

		conn, err := net.DialTimeout("tcp", net.JoinHostPort(localIp, serverPort), time.Second)
		if err != nil {
			span.Errorf(fmt.Sprintf("HandleLeaderChange serverPort not exsit ,error %v", err))
			exporter.Warning(fmt.Sprintf("mp[%v] HandleLeaderChange serverPort not exsit ,error %v", mp.config.PartitionId, err))
			go mp.raftPartition.TryToLeader(mp.config.PartitionId)
			return
		}
		span.Debugf("[metaPartition] HandleLeaderChange close conn %v, nodeId: %v, leader: %v", serverPort, mp.config.NodeId, leader)
		exporter.Warning(fmt.Sprintf("[metaPartition]mp[%v] HandleLeaderChange close conn %v, nodeId: %v, leader: %v", mp.config.PartitionId, serverPort, mp.config.NodeId, leader))
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
	}
	if mp.config.NodeId != leader {
		span.Debugf("[metaPartition] pid: %v HandleLeaderChange become unleader nodeId: %v, leader: %v", mp.config.PartitionId, mp.config.NodeId, leader)
		exporter.Warning(fmt.Sprintf("[metaPartition] pid: %v HandleLeaderChange become unleader nodeId: %v, leader: %v", mp.config.PartitionId, mp.config.NodeId, leader))
		mp.storeChan <- &storeMsg{
			command: stopStoreTick,
		}
		return
	}
	mp.storeChan <- &storeMsg{
		command: startStoreTick,
	}

	span.Debugf("[metaPartition] pid: %v HandleLeaderChange become leader conn %v, nodeId: %v, leader: %v", mp.config.PartitionId, serverPort, mp.config.NodeId, leader)
	exporter.Warning(fmt.Sprintf("[metaPartition] pid: %v HandleLeaderChange become leader conn %v, nodeId: %v, leader: %v", mp.config.PartitionId, serverPort, mp.config.NodeId, leader))
	if mp.config.Start == 0 && mp.config.Cursor == 0 {
		id, err := mp.nextInodeID()
		if err != nil {
			span.Fatalf("[HandleLeaderChange] init root inode id: %s.", err.Error())
			exporter.Warning(fmt.Sprintf("[HandleLeaderChange] pid %v init root inode id: %s.", mp.config.PartitionId, err.Error()))
		}
		ino := NewInode(id, proto.Mode(os.ModePerm|os.ModeDir))
		go mp.initInode(ctx, ino)
	}
}

// Put puts the given key-value pair (operation key and operation request) into the raft store.
func (mp *metaPartition) submit(ctx context.Context, op uint32, data []byte) (resp interface{}, err error) {
	span := getSpan(ctx)
	span.Debugf("submit op [%d] ...", op)
	snap := NewMetaItem(0, nil, nil)
	snap.Op = op
	if data != nil {
		snap.V = data
	}
	cmd, err := snap.MarshalJson()
	if err != nil {
		span.Warnf("submit op [%d] %s", op, err.Error())
		return
	}

	// submit to the raft store
	resp, err = mp.raftPartition.Submit(cmd)
	span.Debugf("submit op [%d] done", op)
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
