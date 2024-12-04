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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

type GetInodeReq struct {
	Ino      *Inode
	ListAll  bool
	InnerReq bool
}

type InodeResponse struct {
	Status uint8
	Msg    *Inode
}

func NewInodeResponse() *InodeResponse {
	return &InodeResponse{}
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmTxCreateInode(txIno *TxInode, quotaIds []uint32) (status uint8) {
	status = proto.OpOk
	if mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID) {
		log.LogWarnf("fsmTxCreateInode: tx is already finish. txId %s", txIno.TxInfo.TxID)
		return proto.OpTxInfoNotExistErr
	}

	// inodeInfo := mp.txProcessor.txManager.getTxInodeInfo(txIno.TxInfo.TxID, txIno.Inode.Inode)
	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		status = proto.OpTxInodeInfoNotExistErr
		return
	}

	rbInode := NewTxRollbackInode(txIno.Inode, quotaIds, inodeInfo, TxDelete)
	status = mp.txProcessor.txResource.addTxRollbackInode(rbInode)
	if status != proto.OpOk {
		return
	}

	defer func() {
		if status != proto.OpOk {
			mp.txProcessor.txResource.deleteTxRollbackInode(txIno.Inode.Inode, txIno.TxInfo.TxID)
		}
	}()
	// 3.insert inode in inode tree
	return mp.fsmCreateInode(txIno.Inode)
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(ino *Inode) (status uint8) {
	if status = mp.uidManager.addUidSpace(ino.Uid, ino.Inode, nil); status != proto.OpOk {
		return
	}

	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}

	return
}

func (mp *metaPartition) fsmTxCreateLinkInode(txIno *TxInode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	if mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID) {
		log.LogWarnf("fsmTxCreateLinkInode: tx is already finish. txId %s", txIno.TxInfo.TxID)
		resp.Status = proto.OpTxInfoNotExistErr
		return
	}

	// 2.register rollback item
	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		resp.Status = proto.OpTxInodeInfoNotExistErr
		return
	}

	rbInode := NewTxRollbackInode(txIno.Inode, []uint32{}, inodeInfo, TxDelete)
	resp.Status = mp.txProcessor.txResource.addTxRollbackInode(rbInode)
	if resp.Status == proto.OpExistErr {
		resp.Status = proto.OpOk
		resp.Msg = txIno.Inode
		return
	}

	if resp.Status != proto.OpOk {
		return
	}

	defer func() {
		if resp.Status != proto.OpOk {
			mp.txProcessor.txResource.deleteTxRollbackInode(txIno.Inode.Inode, txIno.TxInfo.TxID)
		}
	}()

	return mp.fsmCreateLinkInode(txIno.Inode, 0)
}

func (mp *metaPartition) fsmCreateLinkInode(ino *Inode, uniqID uint64) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = i
	if !mp.uniqChecker.legalIn(uniqID) {
		log.LogWarnf("fsmCreateLinkInode repeated, ino[%v] uniqID %v nlink %v", ino.Inode, uniqID, ino.GetNLink())
		return
	}
	i.IncNLink(ino.getVer())
	return
}

func (mp *metaPartition) getInodeByVer(ino *Inode) (i *Inode) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		log.LogDebugf("action[getInodeByVer] not found ino[%v] verseq [%v]", ino.Inode, ino.getVer())
		return
	}
	i, _ = item.(*Inode).getInoByVer(ino.getVer(), false)
	return
}

func (mp *metaPartition) getInodeTopLayer(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		log.LogDebugf("action[getInodeTopLayer] not found ino[%v] verseq [%v]", ino.Inode, ino.getVer())
		return
	}
	i := item.(*Inode)
	// ctime := timeutil.GetCurrentTimeUnix()
	// /*
	//  * FIXME: not protected by lock yet, since nothing is depending on atime.
	//  * Shall add inode lock in the future.
	//  */
	// if ctime > i.AccessTime {
	// 	i.AccessTime = ctime
	// }

	resp.Msg = i
	return
}

func (mp *metaPartition) getInode(ino *Inode, listAll bool) (resp *InodeResponse) {
	req := &GetInodeReq{
		Ino:     ino,
		ListAll: listAll,
	}
	return mp.getInodeExt(req)
}

func (mp *metaPartition) getInodeExt(req *GetInodeReq) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	ino := req.Ino
	i := mp.getInodeByVer(ino)
	if i == nil {
		log.LogDebugf("action[getInode] mp(%v) ino(%v) not found", mp.config.PartitionId, ino.Inode)
		resp.Status = proto.OpNotExistErr
		return
	}

	if i.ShouldDelete() {
		log.LogDebugf("action[getInode] mp(%v) ino(%v): shouldDelete(true) listAll(%v)",
			mp.config.PartitionId, ino.Inode, req.ListAll)
		if !req.ListAll {
			resp.Status = proto.OpNotExistErr
			return
		}
	}

	// ctime := timeutil.GetCurrentTimeUnix()
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	// if ctime > i.AccessTime {
	//	i.AccessTime = ctime
	// }

	if req.InnerReq {
		resp.Msg = i
		return
	}

	resp.Msg = i.Copy().(*Inode)
	resp.Msg.AccessTime = timeutil.GetCurrentTimeUnix()
	return
}

func (mp *metaPartition) hasInode(ino *Inode) (ok bool) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		return
	}
	i := mp.getInodeByVer(ino)
	if i == nil || i.ShouldDelete() {
		return
	}
	ok = true
	return
}

// Ascend is the wrapper of inodeTree.Ascend
func (mp *metaPartition) Ascend(f func(i BtreeItem) bool) {
	mp.inodeTree.Ascend(f)
}

func (mp *metaPartition) fsmTxUnlinkInode(txIno *TxInode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if proto.IsDir(txIno.Inode.Type) && txIno.TxInfo.TxType == proto.TxTypeRemove && txIno.Inode.NLink > 2 {
		resp.Status = proto.OpNotEmpty
		log.LogWarnf("fsmTxUnlinkInode: dir is not empty, can't remove it, txinode[%v]", txIno)
		return
	}

	if mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID) {
		log.LogWarnf("fsmTxUnlinkInode: tx is already finish. txId %s", txIno.TxInfo.TxID)
		resp.Status = proto.OpTxInfoNotExistErr
		return
	}

	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		resp.Status = proto.OpTxInodeInfoNotExistErr
		return
	}
	var quotaIds []uint32
	quotaIds, _ = mp.isExistQuota(txIno.Inode.Inode)

	rbInode := NewTxRollbackInode(txIno.Inode, quotaIds, inodeInfo, TxAdd)
	resp.Status = mp.txProcessor.txResource.addTxRollbackInode(rbInode)
	if resp.Status == proto.OpExistErr {
		resp.Status = proto.OpOk
		item := mp.inodeTree.Get(txIno.Inode)
		if item != nil {
			resp.Msg = item.(*Inode)
		}
		return
	}
	if resp.Status != proto.OpOk {
		return
	}

	defer func() {
		if resp.Status != proto.OpOk {
			mp.txProcessor.txResource.deleteTxRollbackInode(txIno.Inode.Inode, txIno.TxInfo.TxID)
		}
	}()

	item := mp.inodeTree.Get(txIno.Inode)
	if item == nil || item.(*Inode).IsTempFile() {
		resp.Status = proto.OpNotExistErr
		log.LogWarnf("fsmTxUnlinkInode: inode may be already not exist or link 0, txInode %v, item %v", txIno, item)
		return
	}

	resp = mp.fsmUnlinkInode(txIno.Inode, 0)
	if resp.Status != proto.OpOk {
		return
	}

	if txIno.TxInfo.TxType == proto.TxTypeRename {
		mp.fsmEvictInode(txIno.Inode)
	}

	return
}

// normal unlink seq is 0
// snapshot unlink seq is snapshotVersion
// fsmUnlinkInode delete the specified inode from inode tree.

func (mp *metaPartition) fsmUnlinkInode(ino *Inode, uniqID uint64) (resp *InodeResponse) {
	log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, ino)
	var ext2Del []proto.ExtentKey

	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, ino)
		resp.Status = proto.OpNotExistErr
		return
	}
	inode := item.(*Inode)
	if ino.getVer() == 0 && inode.ShouldDelete() {
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, ino)
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = inode
	if !mp.uniqChecker.legalIn(uniqID) {
		log.LogWarnf("fsmUnlinkInode repeat, mp[%v] ino[%v] uniqID %v nlink %v", mp.config.PartitionId, ino.Inode, uniqID, ino.GetNLink())
		return
	}

	log.LogDebugf("action[fsmUnlinkInode] mp[%v] get inode[%v]", mp.config.PartitionId, inode)
	var (
		doMore bool
		status = proto.OpOk
	)

	if ino.getVer() == 0 {
		ext2Del, doMore, status = inode.unlinkTopLayer(mp.config.PartitionId, ino, mp.verSeq, mp.multiVersionList)
	} else { // means drop snapshot
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] req drop assigned snapshot reqseq [%v] inode seq [%v]", mp.config.PartitionId, ino.getVer(), inode.getVer())
		if ino.getVer() > inode.getVer() && !isInitSnapVer(ino.getVer()) {
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] inode[%v] unlink not exist snapshot and return do nothing.reqseq [%v] larger than inode seq [%v]",
				mp.config.PartitionId, ino.Inode, ino.getVer(), inode.getVer())
			return
		} else {
			ext2Del, doMore, status = inode.unlinkVerInList(mp.config.PartitionId, ino, mp.verSeq, mp.multiVersionList)
		}
	}
	if !doMore {
		resp.Status = status
		return
	}

	if inode.IsEmptyDirAndNoSnapshot() {
		if inode.NLink < 2 { // snapshot deletion
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v] really be deleted, empty dir", mp.config.PartitionId, inode)
			mp.inodeTree.Delete(inode)
			mp.updateUsedInfo(0, -1, inode.Inode)
		}
	} else if inode.IsTempFile() {
		// all snapshot between create to last deletion cleaned
		if inode.NLink == 0 && inode.getLayerLen() == 0 {
			mp.updateUsedInfo(-1*int64(inode.Size), -1, inode.Inode)
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] unlink inode[%v] and push to freeList", mp.config.PartitionId, inode)
			inode.AccessTime = time.Now().Unix()
			mp.freeList.Push(inode.Inode)
			mp.uidManager.doMinusUidSpace(inode.Uid, inode.Inode, inode.Size)
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, inode)
		}
	}

	if len(ext2Del) > 0 {
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v] DecSplitExts ext2Del %v", mp.config.PartitionId, ino, ext2Del)
		inode.DecSplitExts(mp.config.PartitionId, ext2Del)
		mp.extDelCh <- ext2Del
	}
	log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v] left", mp.config.PartitionId, inode)
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInodeBatch(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		status := mp.inodeInTx(ino.Inode)
		if status != proto.OpOk {
			resp = append(resp, &InodeResponse{Status: status})
			continue
		}
		resp = append(resp, mp.fsmUnlinkInode(ino, 0))
	}
	return
}

func (mp *metaPartition) internalHasInode(ino *Inode) bool {
	return mp.inodeTree.Has(ino)
}

func (mp *metaPartition) internalDelete(val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	for {
		err = binary.Read(buf, binary.BigEndian, &ino.Inode)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode[%v]",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}
}

func (mp *metaPartition) internalDeleteBatch(val []byte) error {
	if len(val) == 0 {
		return nil
	}
	inodes, err := InodeBatchUnmarshal(val)
	if err != nil {
		return nil
	}

	for _, ino := range inodes {
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode[%v]",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}

	return nil
}

func (mp *metaPartition) internalDeleteInode(ino *Inode) {
	log.LogDebugf("action[internalDeleteInode] vol(%v) mp(%v) ino[%v] really be deleted", mp.config.VolName, mp.config.PartitionId, ino)
	mp.inodeTree.Delete(ino)
	mp.freeList.Remove(ino.Inode)
	mp.extendTree.Delete(&Extend{inode: ino.Inode}) // Also delete extend attribute.
	return
}

func (mp *metaPartition) fsmAppendExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := item.(*Inode)
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	oldSize := int64(ino2.Size)
	eks := ino.HybridCloudExtents.sortedEks.(*SortedExtents).CopyExtents()
	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks); status != proto.OpOk {
		return
	}
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime, mp.volType)
	mp.updateUsedInfo(int64(ino2.Size)-oldSize, 0, ino2.Inode)
	log.LogInfof("fsmAppendExtents mpId[%v].inode[%v] deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, delExtents)

	log.LogInfof("fsmAppendExtents mpId[%v].inode[%v] DecSplitExts deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	ino2.DecSplitExts(mp.config.PartitionId, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(ino *Inode, isSplit bool) (status uint8) {
	var (
		delExtents       []proto.ExtentKey
		discardExtentKey []proto.ExtentKey
	)

	if mp.verSeq < ino.getVer() {
		status = proto.OpArgMismatchErr
		log.LogErrorf("fsmAppendExtentsWithCheck.mp[%v] param ino[%v] mp seq [%v]", mp.config.PartitionId, ino, mp.verSeq)
		return
	}
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)

	if item == nil {
		log.LogInfof("fsmAppendExtentsWithCheck: inode already not exist, mp %d, ino %d", mp.config.PartitionId, ino.Inode)
		status = proto.OpNotExistErr
		return
	}

	fsmIno := item.(*Inode)
	if fsmIno.ShouldDelete() {
		log.LogInfof("fsmAppendExtentsWithCheck: inode already not exist, mp %d, ino %d", mp.config.PartitionId, ino.Inode)
		status = proto.OpNotExistErr
		return
	}

	oldSize := int64(fsmIno.Size)
	// get eks from inoParm, so do not need transform from HybridCloudExtents
	var (
		eks         []proto.ExtentKey
		isCache     bool
		isMigration bool
	)
	storageClass := ino.StorageClass
	if len(ino.Extents.eks) != 0 {
		isCache = true
		eks = ino.Extents.CopyExtents()
	} else if ino.HybridCloudExtents.sortedEks != nil && len(ino.HybridCloudExtents.sortedEks.(*SortedExtents).eks) != 0 {
		isCache = false
		eks = ino.HybridCloudExtents.sortedEks.(*SortedExtents).CopyExtents()
	} else if ino.HybridCloudExtentsMigration.sortedEks != nil && len(ino.HybridCloudExtentsMigration.sortedEks.(*SortedExtents).eks) != 0 {
		isMigration = true
		storageClass = ino.HybridCloudExtentsMigration.storageClass
		eks = ino.HybridCloudExtentsMigration.sortedEks.(*SortedExtents).CopyExtents()
	}

	if err := fsmIno.updateStorageClass(storageClass, isCache, isMigration); err != nil {
		log.LogErrorf("action[fsmAppendExtentsWithCheck] updateStorageClass inode(%v) isCache(%v) isMigration(%v), failed: %v",
			ino.Inode, isCache, isMigration, err.Error())
		status = proto.OpMismatchStorageClass
		return
	}

	log.LogDebugf("action[fsmAppendExtentsWithCheck] inode %v hist len %v,eks %v, isCache %v isMigration %v",
		fsmIno.Inode, fsmIno.getLayerLen(), eks, isCache, isMigration)
	if len(eks) < 1 {
		log.LogWarnf("fsmAppendExtentsWithCheck: recive eks less than 1, may be wrong, mp %d, ino %d",
			mp.config.PartitionId, ino.Inode)
		return
	}
	if len(eks) > 1 {
		discardExtentKey = eks[1:]
	}

	if status = mp.uidManager.addUidSpace(fsmIno.Uid, fsmIno.Inode, eks[:1]); status != proto.OpOk {
		log.LogErrorf("fsmAppendExtentsWithCheck.mp[%v] addUidSpace status [%v]", mp.config.PartitionId, status)
		return
	}

	log.LogDebugf("action[fsmAppendExtentsWithCheck] mp[%v] ver [%v] ino[%v] isSplit %v ek [%v] hist len %v discardExtentKey %v, gen %d",
		mp.config.PartitionId, mp.verSeq, fsmIno.Inode, isSplit, eks[0], fsmIno.getLayerLen(), discardExtentKey, fsmIno.Generation)

	appendExtParam := &AppendExtParam{
		mpId:             mp.config.PartitionId,
		mpVer:            mp.verSeq,
		ek:               eks[0],
		ct:               ino.ModifyTime,
		discardExtents:   discardExtentKey,
		volType:          mp.volType,
		multiVersionList: mp.multiVersionList,
		isCache:          isCache,
		isMigration:      isMigration,
	}

	if !isSplit {
		delExtents, status = fsmIno.AppendExtentWithCheck(appendExtParam)
		if status == proto.OpOk {
			log.LogInfof("action[fsmAppendExtentsWithCheck] mp[%v] DecSplitExts delExtents [%v]", mp.config.PartitionId, delExtents)
			fsmIno.DecSplitExts(appendExtParam.mpId, delExtents)
			mp.extDelCh <- delExtents
		}
		// conflict need delete eks[0], to clear garbage data
		if status == proto.OpConflictExtentsErr {
			log.LogInfof("action[fsmAppendExtentsWithCheck] mp[%v] OpConflictExtentsErr [%v]", mp.config.PartitionId, eks[:1])
			if !storage.IsTinyExtent(eks[0].ExtentId) && eks[0].ExtentOffset >= util.ExtentSize && clusterEnableSnapshot {
				eks[0].SetSplit(true)
			}
			mp.extDelCh <- eks[:1]
		}
	} else {
		if !clusterEnableSnapshot {
			status = proto.OpArgMismatchErr
			log.LogErrorf("action[fsmAppendExtentsWithCheck] mp[%v] snapshot not enabled", mp.config.PartitionId)
			return
		}
		// only the ek itself will be moved to level before
		// ino verseq be set with mp ver before submit in case other mp be updated while on flight, which will lead to
		// inconsistent between raft pairs
		delExtents, status = fsmIno.SplitExtentWithCheck(appendExtParam)
		log.LogInfof("action[fsmAppendExtentsWithCheck] mp[%v] DecSplitExts delExtents [%v]", mp.config.PartitionId, delExtents)
		fsmIno.DecSplitExts(mp.config.PartitionId, delExtents)
		mp.extDelCh <- delExtents
		mp.uidManager.minusUidSpace(fsmIno.Uid, fsmIno.Inode, delExtents)
	}

	// conflict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		mp.extDelCh <- eks[:1]
		mp.uidManager.minusUidSpace(fsmIno.Uid, fsmIno.Inode, eks[:1])
		log.LogDebugf("fsmAppendExtentsWithCheck mp[%v] delExtents inode[%v] ek(%v)", mp.config.PartitionId, fsmIno.Inode, delExtents)
	}

	mp.updateUsedInfo(int64(fsmIno.Size)-oldSize, 0, fsmIno.Inode)
	log.LogInfof("fsmAppendExtentsWithCheck mp[%v] inode[%v] ek(%v) deleteExtents(%v) discardExtents(%v) status(%v), gen %d",
		mp.config.PartitionId, fsmIno.Inode, eks[0], delExtents, discardExtentKey, status, fsmIno.Generation)

	return
}

func (mp *metaPartition) fsmAppendObjExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}

	inode := item.(*Inode)
	if inode.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	if err := inode.updateStorageClass(ino.StorageClass, false, false); err != nil {
		status = proto.OpMismatchStorageClass
		return
	}
	// eks := ino.ObjExtents.CopyExtents()
	eks := ino.HybridCloudExtents.sortedEks.(*SortedObjExtents).CopyExtents()
	err := inode.AppendObjExtents(eks, ino.ModifyTime)
	// if err is not nil, means obj eks exist overlap.
	if err != nil {
		log.LogErrorf("fsmAppendExtents inode[%v] err(%v)", inode.Inode, err)
		status = proto.OpConflictExtentsErr
	}
	return
}

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
	var err error
	resp = NewInodeResponse()
	log.LogDebugf("fsmExtentsTruncate. req ino[%v] mpId(%v)", ino, mp.config.PartitionId)
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if !proto.IsStorageClassReplica(i.StorageClass) {
		log.LogWarnf("[fsmExtentsTruncate] mpId(%v) ino(%v) inoParamStorageClass(%v), but actual storageClass is %v, not allowed truncate. ",
			mp.config.PartitionId, i.Inode, proto.StorageClassString(i.StorageClass), proto.StorageClassString(i.StorageClass))
		resp.Status = proto.OpArgMismatchErr
		return
	}
	if i.HybridCloudExtents.sortedEks != nil {
		if value, ok := i.HybridCloudExtents.sortedEks.(*SortedExtents); !ok {
			log.LogWarnf("[fsmExtentsTruncate] mpId(%v) ino(%v) storageClass(%v), extent actualType is [%T] but expect SortedExtents",
				mp.config.PartitionId, i.Inode, i.StorageClass, value)
			resp.Status = proto.OpArgMismatchErr
			return
		}
	}

	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(i.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}

	doOnLastKey := func(lastKey *proto.ExtentKey) {
		var eks []proto.ExtentKey
		eks = append(eks, *lastKey)
		mp.uidManager.minusUidSpace(i.Uid, i.Inode, eks)
	}

	insertSplitKey := func(ek *proto.ExtentKey) {
		i.insertEkRefMap(mp.config.PartitionId, ek)
	}

	if i.getVer() != mp.verSeq {
		i.CreateVer(mp.verSeq)
	}
	i.Lock()
	defer i.Unlock()

	if err = i.CreateLowerVersion(i.getVer(), mp.multiVersionList); err != nil {
		return
	}
	oldSize := int64(i.Size)
	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime, doOnLastKey, insertSplitKey)

	if len(delExtents) == 0 {
		return
	}

	if delExtents, err = i.RestoreExts2NextLayer(mp.config.PartitionId, delExtents, mp.verSeq, 0); err != nil {
		panic("RestoreExts2NextLayer should not be error")
	}
	mp.updateUsedInfo(int64(i.Size)-oldSize, 0, i.Inode)

	// now we should delete the extent
	log.LogInfof("fsmExtentsTruncate.mp (%v) inode[%v] DecSplitExts exts(%v)", mp.config.PartitionId, i.Inode, delExtents)
	i.DecSplitExts(mp.config.PartitionId, delExtents)
	mp.extDelCh <- delExtents
	mp.uidManager.minusUidSpace(i.Uid, i.Inode, delExtents)
	return
}

func (mp *metaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	log.LogDebugf("action[fsmEvictInode] inode[%v]", ino)
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		log.LogDebugf("action[fsmEvictInode] inode[%v] already be mark delete", ino)
		return
	}
	if proto.IsDir(i.Type) {
		if i.IsEmptyDirAndNoSnapshot() {
			i.SetDeleteMark()
		}
		return
	}

	if i.IsTempFile() {
		log.LogDebugf("action[fsmEvictInode] inode[%v] already linke zero and be set mark delete and be put to freelist", ino)
		if i.isEmptyVerList() {
			i.SetDeleteMark()
			mp.freeList.Push(i.Inode)
		}
	}
	return
}

func (mp *metaPartition) fsmBatchEvictInode(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		status := mp.inodeInTx(ino.Inode)
		if status != proto.OpOk {
			resp = append(resp, &InodeResponse{Status: status})
			return
		}
		resp = append(resp, mp.fsmEvictInode(ino))
	}
	return
}

func (mp *metaPartition) checkAndInsertFreeList(ino *Inode) {
	if proto.IsDir(ino.Type) {
		return
	}
	if ino.ShouldDelete() {
		mp.freeList.Push(ino.Inode)
	} else if ino.IsTempFile() {
		ino.AccessTime = time.Now().Unix()
		mp.freeList.Push(ino.Inode)
	} else if ino.ShouldDeleteMigrationExtentKey(true) {
		mp.freeHybridList.Push(ino.Inode)
	}
}

func (mp *metaPartition) fsmSetAttr(req *SetattrRequest) (err error) {
	log.LogDebugf("action[fsmSetAttr] req %v", req)
	ino := NewInode(req.Inode, req.Mode)
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		return
	}
	ino = item.(*Inode)
	if ino.ShouldDelete() {
		return
	}
	ino.SetAttr(req)
	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmExtentsEmpty(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(i.Type) {
		status = proto.OpArgMismatchErr
		return
	}
	log.LogDebugf("action[fsmExtentsEmpty] mp[%v] ino[%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	log.LogDebugf("action[fsmExtentsEmpty] mp[%v] ino[%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		mp.uidManager.minusUidSpace(i.Uid, i.Inode, tinyEks)
		log.LogDebugf("fsmExtentsEmpty mp[%v] inode[%d] tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	i.EmptyExtents(ino.ModifyTime)

	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmDelVerExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(i.Type) {
		status = proto.OpArgMismatchErr
		return
	}
	log.LogDebugf("action[fsmExtentsEmpty] mp[%v] ino[%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	log.LogDebugf("action[fsmExtentsEmpty] mp[%v] ino[%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		log.LogDebugf("fsmExtentsEmpty mp[%v] inode[%d] tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	i.EmptyExtents(ino.ModifyTime)

	return
}

func (mp *metaPartition) fsmClearInodeCache(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := item.(*Inode)
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	delExtents := ino2.EmptyExtents(ino.ModifyTime)
	log.LogInfof("fsmClearInodeCache.mp[%v] inode[%v] DecSplitExts delExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	if len(delExtents) > 0 {
		ino2.DecSplitExts(mp.config.PartitionId, delExtents)
		mp.extDelCh <- delExtents
	}
	return
}

// attion: unmarshal error will disard extent
func (mp *metaPartition) fsmSendToChan(val []byte, v3 bool) (status uint8) {
	sortExtents := NewSortedExtents()
	// ek for del don't need version info
	err, _ := sortExtents.UnmarshalBinary(val, v3 && clusterEnableSnapshot)
	if err != nil {
		panic(fmt.Errorf("[fsmDelExtents] unmarshal sortExtents error, mp[%v], err(%s)", mp.config.PartitionId, err.Error()))
	}

	log.LogWarnf("fsmDelExtents mp[%v] delExtents(%v)", mp.config.PartitionId, len(sortExtents.eks))
	mp.extDelCh <- sortExtents.eks
	return
}

func (mp *metaPartition) fsmSetInodeQuotaBatch(req *proto.BatchSetMetaserverQuotaReuqest) (resp *proto.BatchSetMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchSetMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8, 0)
	for _, ino := range req.Inodes {
		var isExist bool
		var err error

		extend := NewExtend(ino)
		treeItem := mp.extendTree.Get(extend)
		inode := NewInode(ino, 0)
		retMsg := mp.getInode(inode, false)

		if retMsg.Status != proto.OpOk {
			log.LogErrorf("fsmSetInodeQuotaBatch get inode[%v] fail.", ino)
			resp.InodeRes[ino] = retMsg.Status
			continue
		}
		inode = retMsg.Msg
		quotaInfos := &proto.MetaQuotaInfos{
			QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
		}
		quotaInfo := &proto.MetaQuotaInfo{
			RootInode: req.IsRoot,
		}

		if treeItem == nil {
			quotaInfos.QuotaInfoMap[req.QuotaId] = quotaInfo
			mp.extendTree.ReplaceOrInsert(extend, true)
		} else {
			extend = treeItem.(*Extend)
			value := extend.Quota
			if len(value) > 0 {
				if err = json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
					log.LogErrorf("set quota Unmarshal quotaInfos fail [%v]", err)
					resp.InodeRes[ino] = proto.OpErr
					continue
				}
				oldQuotaInfo, ok := quotaInfos.QuotaInfoMap[req.QuotaId]
				if ok {
					isExist = true
					quotaInfo = oldQuotaInfo
				}
			}
			quotaInfos.QuotaInfoMap[req.QuotaId] = quotaInfo
		}
		value, err := json.Marshal(quotaInfos.QuotaInfoMap)
		if err != nil {
			log.LogErrorf("set quota marsha1 quotaInfos [%v] fail [%v]", quotaInfos, err)
			resp.InodeRes[ino] = proto.OpErr
			continue
		}
		extend.Quota = value
		if mp.verSeq > 0 {
			extend.setVersion(mp.verSeq)
		}
		resp.InodeRes[ino] = proto.OpOk
		if !isExist {
			files += 1
			bytes += int64(inode.Size)
		}
	}
	mp.mqMgr.updateUsedInfo(bytes, files, req.QuotaId)
	return
}

func (mp *metaPartition) fsmDeleteInodeQuotaBatch(req *proto.BatchDeleteMetaserverQuotaReuqest) (resp *proto.BatchDeleteMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchDeleteMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8, 0)

	for _, ino := range req.Inodes {
		var err error
		extend := NewExtend(ino)
		treeItem := mp.extendTree.Get(extend)
		inode := NewInode(ino, 0)
		retMsg := mp.getInode(inode, false)
		if retMsg.Status != proto.OpOk {
			log.LogErrorf("fsmDeleteInodeQuotaBatch get inode[%v] fail.", ino)
			resp.InodeRes[ino] = retMsg.Status
			continue
		}
		inode = retMsg.Msg
		log.LogDebugf("fsmDeleteInodeQuotaBatch msg [%v] inode[%v]", retMsg, inode)
		quotaInfos := &proto.MetaQuotaInfos{
			QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
		}

		if treeItem == nil {
			log.LogDebugf("fsmDeleteInodeQuotaBatch inode[%v] not has extend ", ino)
			resp.InodeRes[ino] = proto.OpOk
			continue
		} else {
			extend = treeItem.(*Extend)
			value := extend.Quota
			if len(value) > 0 {
				if err = json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
					log.LogErrorf("fsmDeleteInodeQuotaBatch ino[%v] Unmarshal quotaInfos fail [%v]", ino, err)
					resp.InodeRes[ino] = proto.OpErr
					continue
				}

				_, ok := quotaInfos.QuotaInfoMap[req.QuotaId]
				if ok {
					delete(quotaInfos.QuotaInfoMap, req.QuotaId)
					if len(quotaInfos.QuotaInfoMap) == 0 {
						extend.Quota = nil
					} else {
						value, err = json.Marshal(quotaInfos.QuotaInfoMap)
						if err != nil {
							log.LogErrorf("fsmDeleteInodeQuotaBatch marsha1 quotaInfos [%v] fail [%v]", quotaInfos, err)
							resp.InodeRes[ino] = proto.OpErr
							continue
						}
						extend.Quota = value
						if mp.verSeq > 0 {
							extend.setVersion(mp.verSeq)
						}
					}
				} else {
					log.LogDebugf("fsmDeleteInodeQuotaBatch QuotaInfoMap can not find inode[%v] quota [%v]", ino, req.QuotaId)
					resp.InodeRes[ino] = proto.OpOk
					continue
				}
			} else {
				resp.InodeRes[ino] = proto.OpOk
				continue
			}
		}
		files -= 1
		bytes -= int64(inode.Size)
	}
	mp.mqMgr.updateUsedInfo(bytes, files, req.QuotaId)
	log.LogInfof("fsmDeleteInodeQuotaBatch quotaId [%v] resp [%v] success.", req.QuotaId, resp)
	return
}

func (mp *metaPartition) fsmSyncInodeAccessTime(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	i.AccessTime = ino.AccessTime
	log.LogDebugf("fsmSyncInodeAccessTime inode [%v] AccessTime update to [%v] success.", i.Inode, ino.AccessTime)
	return
}

func (mp *metaPartition) fsmBatchSyncInodeAccessTime(bufSlice []byte) (status uint8) {
	status = proto.OpOk
	start := time.Now()
	mpId := mp.config.PartitionId
	idx := 8

	atime := binary.BigEndian.Uint64(bufSlice[0:8])
	for ; idx+8 <= len(bufSlice); idx += 8 {
		ino := binary.BigEndian.Uint64(bufSlice[idx : idx+8])
		item := mp.inodeTree.CopyGet(NewInode(ino, 0))
		if item == nil {
			log.LogWarnf("fsmBatchSyncInodeAccessTime: mp(%d) inode %d not found", mpId, ino)
			continue
		}

		i := item.(*Inode)
		i.AccessTime = int64(atime)
		log.LogDebugf("fsmBatchSyncInodeAccessTime: mp(%d) inode (%v) AccessTime (%d) update success.", mpId, i.Inode, atime)
	}

	if log.EnableDebug() {
		log.LogDebugf("fsmBatchSyncInodeAccessTime: batch inode accessTime finish. mp(%d), cnt(%d), cost(%d)us",
			mpId, idx/8-1, time.Since(start).Microseconds())
	}
	return
}

func (mp *metaPartition) fsmRenewalInodeForbiddenMigration(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	i.LeaseExpireTime = ino.LeaseExpireTime
	log.LogDebugf("action[fsmRenewalInodeForbiddenMigration] inode %v is renewal, expireTime %d", i.Inode, ino.LeaseExpireTime)
	return
}

func (mp *metaPartition) fsmUpdateExtentKeyAfterMigration(inoParam *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(inoParam)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		log.LogWarnf("fsmUpdateExtentKeyAfterMigration: inode already been deleted %d", inoParam.Inode)
		return
	}
	i := item.(*Inode)

	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		log.LogWarnf("fsmUpdateExtentKeyAfterMigration: inode been deleted %d", inoParam.Inode)
		return
	}

	if i.LeaseExpireTime != inoParam.LeaseExpireTime {
		log.LogErrorf("fsmUpdateExtentKeyAfterMigration: inode is forbidden to migrate. gen %d, reqGen %d, ino %d",
			i.LeaseExpireTime, inoParam.LeaseExpireTime, i.Inode)
		resp.Status = proto.OpLeaseOccupiedByOthers
		return
	}

	// for empty file, HybridCloudExtents.sortedEks is nil and StorageClass_Unspecified
	// but HybridCloudExtentsMigration.sortedEks for inoParam is always not nil
	if i.HybridCloudExtents.sortedEks == nil && i.StorageClass != proto.StorageClass_Unspecified && inoParam.HybridCloudExtentsMigration.sortedEks != nil {
		log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) extent key is empty, but extent key "+
			"for migration storageClass(%v) is not empty",
			mp.config.PartitionId, i.Inode, i.StorageClass, i.HybridCloudExtentsMigration.storageClass)
		resp.Status = proto.OpNotPerm
		return
	}

	if i.HybridCloudExtents.sortedEks != nil && inoParam.HybridCloudExtentsMigration.sortedEks == nil {
		log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) migrate extent key for migration "+
			"storageClass(%v) is empty ",
			mp.config.PartitionId, i.Inode, i.StorageClass, i.HybridCloudExtentsMigration.storageClass)
		resp.Status = proto.OpNotPerm
		return
	}

	if (!i.HybridCloudExtents.Empty() && i.HybridCloudExtentsMigration.Empty()) || (i.HybridCloudExtents.Empty() && !i.HybridCloudExtentsMigration.Empty()) {
		log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) migrate extent key for migration "+
			"storageClass(%v) is empty, eks(%v), migrateEks(%v) ",
			mp.config.PartitionId, i.Inode, i.StorageClass, i.HybridCloudExtentsMigration.storageClass, i.HybridCloudExtents.Empty(), i.HybridCloudExtentsMigration.Empty())
		resp.Status = proto.OpNotPerm
		return
	}

	// if StorageClass is the same, check if sortedEks is the same
	if i.StorageClass == inoParam.HybridCloudExtentsMigration.storageClass &&
		i.HybridCloudExtents.sortedEks != nil &&
		inoParam.HybridCloudExtentsMigration.sortedEks != nil {
		if proto.IsStorageClassReplica(i.StorageClass) {
			inoExtents := i.HybridCloudExtents.sortedEks.(*SortedExtents)
			mExtents := inoParam.HybridCloudExtentsMigration.sortedEks.(*SortedExtents)
			if inoExtents.Equals(mExtents) {
				log.LogInfof("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) and extents same with req",
					mp.config.PartitionId, i.Inode, i.StorageClass)
				return
			} else {
				log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) is already the same with req storageClass, but extents different",
					mp.config.PartitionId, i.Inode, i.StorageClass)
				resp.Status = proto.OpNotPerm
				return
			}
		} else if proto.IsStorageClassBlobStore(i.StorageClass) {
			inoObjExt := i.HybridCloudExtents.sortedEks.(*SortedObjExtents)
			mObjExt := inoParam.HybridCloudExtentsMigration.sortedEks.(*SortedObjExtents)
			if inoObjExt.Equals(mObjExt) {
				log.LogInfof("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) and objExtents same with req",
					mp.config.PartitionId, i.Inode, i.StorageClass)
				return
			} else {
				log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) is already the same with req storageClass, but objExtents different",
					mp.config.PartitionId, i.Inode, i.StorageClass)
				resp.Status = proto.OpNotPerm
				return
			}
		}
	}

	// store old storage ek in HybridCloudExtentsMigration
	i.HybridCloudExtentsMigration.storageClass = inoParam.StorageClass
	i.HybridCloudExtentsMigration.sortedEks = inoParam.HybridCloudExtents.sortedEks
	i.HybridCloudExtentsMigration.expiredTime = inoParam.HybridCloudExtentsMigration.expiredTime
	// store new storage ek  in HybridCloudExtents
	i.StorageClass = inoParam.HybridCloudExtentsMigration.storageClass
	i.HybridCloudExtents.sortedEks = inoParam.HybridCloudExtentsMigration.sortedEks
	// delete migration ek in future
	i.Flag |= DeleteMigrationExtentKeyFlag
	log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storage class change from %v to %v",
		mp.config.PartitionId, i.Inode, i.HybridCloudExtentsMigration.storageClass, i.StorageClass)
	logCurrentExtentKeys(i.StorageClass, i.HybridCloudExtents.sortedEks, i.Inode)
	logCurrentExtentKeys(i.HybridCloudExtentsMigration.storageClass, i.HybridCloudExtentsMigration.sortedEks, i.Inode)
	if log.EnableInfo() {
		log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) migration ek will be deleted at %v",
			mp.config.PartitionId, i.Inode, time.Unix(i.HybridCloudExtentsMigration.expiredTime, 0).Format("2006-01-02 15:04:05"))
	}
	mp.freeHybridList.Push(i.Inode)
	if !proto.IsValidStorageClass(i.StorageClass) {
		panicMsg := fmt.Sprintf("[fsmUpdateExtentKeyAfterMigration]  mp(%v) inode(%v): invalid storageClass(%v)",
			mp.config.PartitionId, i.Inode, i.StorageClass)
		panic(panicMsg)
	}
	return
}

func logCurrentExtentKeys(storageClass uint32, sortedEks interface{}, inode uint64) {
	if !log.EnableInfo() {
		return
	}
	if sortedEks == nil {
		log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] inode(%v) storageClass(%v) current ek empty",
			inode, storageClass)
	} else {
		if proto.IsStorageClassReplica(storageClass) {
			log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] inode(%v) storageClass(%v) current ek %v",
				inode, storageClass, sortedEks.(*SortedExtents).eks)
		} else if proto.IsStorageClassBlobStore(storageClass) {
			log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] inode(%v) storageClass(%v) current ek %v",
				inode, storageClass, sortedEks.(*SortedObjExtents).eks)
		}
	}
}

func (mp *metaPartition) fsmSetCreateTime(req *SetCreateTimeRequest) (err error) {
	log.LogDebugf("[fsmSetCreateTime] req %v", req)
	ino := NewInode(req.Inode, 0)
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		err = fmt.Errorf("[fsmSetCreateTime] inode(%v) not found", req.Inode)
		return
	}
	ino = item.(*Inode)
	if ino.ShouldDelete() {
		return
	}
	ino.SetCreateTime(req)
	return
}

func (mp *metaPartition) fsmInternalBatchFreeMigrationExtentKey(val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	inoParam := NewInode(0, 0)
	for {
		err = binary.Read(buf, binary.BigEndian, &inoParam.Inode)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		log.LogDebugf("fsmInternalBatchFreeMigrationExtentKey: mpId(%v) inode(%v)",
			mp.config.PartitionId, inoParam.Inode)

		item := mp.inodeTree.CopyGet(inoParam)
		if item == nil {
			err = fmt.Errorf("mpId(%v) inode(%v) not found",
				mp.config.PartitionId, inoParam.Inode)
			log.LogErrorf("[fsmInternalBatchFreeMigrationExtentKey] %v", err)
			return
		}
		ino := item.(*Inode)

		if proto.IsStorageClassReplica(ino.HybridCloudExtentsMigration.storageClass) {
			if ino.HybridCloudExtentsMigration.sortedEks == nil {
				log.LogWarnf("[fsmInternalBatchFreeMigrationExtentKey] mpId(%v) inode(%v) storageClass(%v) migration sortedEks is nil",
					mp.config.PartitionId, ino.Inode, proto.StorageClassString(ino.StorageClass))
			} else {
				mp.putReplicaMigrationExtentKeyToDelChannel(ino)
				log.LogInfof("[fsmInternalBatchFreeMigrationExtentKey] mpId(%v) inode(%v) storageClass(%v) migration SortedExtents pushed into extDelCh",
					mp.config.PartitionId, ino.Inode, proto.StorageClassString(ino.StorageClass))
			}
		}

		mp.internalDeleteInodeMigrationExtentKey(ino)
	}
}

func (mp *metaPartition) internalDeleteInodeMigrationExtentKey(ino *Inode) {
	ino.Lock()
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_Unspecified
	ino.HybridCloudExtentsMigration.expiredTime = 0
	ino.HybridCloudExtentsMigration.sortedEks = nil
	ino.Flag ^= DeleteMigrationExtentKeyFlag // reset DeleteMigrationExtentKeyFlag for future deletion of inode
	ino.Unlock()

	mp.freeHybridList.Remove(ino.Inode)

	log.LogDebugf("[internalDeleteInodeMigrationExtentKey] partitionID(%v) inode(%v) storageClass(%v)",
		mp.config.PartitionId, ino.Inode, proto.StorageClassString(ino.StorageClass))
}

func (mp *metaPartition) fsmSetMigrationExtentKeyDeleteImmediately(inoParam *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(inoParam)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)

	if proto.IsStorageClassBlobStore(i.HybridCloudExtentsMigration.storageClass) {
		i.SetDeleteMigrationExtentKeyImmediately()
		log.LogInfof("[fsmSetMigrationExtentKeyDeleteImmediately] mpId(%v) inode(%v) storageClass(%v) migration objExtents will be deleted immediately",
			mp.config.PartitionId, i.Inode, proto.StorageClassString(i.StorageClass))
		mp.freeList.Push(i.Inode)
		return
	}

	if proto.IsStorageClassReplica(i.HybridCloudExtentsMigration.storageClass) {
		if i.HybridCloudExtentsMigration.sortedEks == nil {
			log.LogInfof("[fsmSetMigrationExtentKeyDeleteImmediately] mpId(%v)  inode(%v) storageClass(%v) migration SortedExtents is nil",
				mp.config.PartitionId, i.Inode, proto.StorageClassString(i.StorageClass))
		} else {
			migrateExtents := i.HybridCloudExtentsMigration.sortedEks.(*SortedExtents)
			mp.extDelCh <- migrateExtents.CopyExtents()
			log.LogInfof("[fsmSetMigrationExtentKeyDeleteImmediately] mpId(%v) inode(%v) storageClass(%v) migration SortedExtents pushed into extDelCh",
				mp.config.PartitionId, i.Inode, proto.StorageClassString(i.StorageClass))
		}
	} else {
		log.LogErrorf("[fsmSetMigrationExtentKeyDeleteImmediately] mpId(%v) inode(%v) storageClass(%v), migrationStorageClass is %v",
			mp.config.PartitionId, i.Inode, proto.StorageClassString(i.StorageClass), proto.StorageClassString(i.HybridCloudExtentsMigration.storageClass))
		return
	}
	mp.internalDeleteInodeMigrationExtentKey(i)
	return
}

func (mp *metaPartition) putReplicaMigrationExtentKeyToDelChannel(ino *Inode) {
	if ino.HybridCloudExtentsMigration.sortedEks == nil {
		return
	}

	migrateExtents := ino.HybridCloudExtentsMigration.sortedEks.(*SortedExtents)
	mp.extDelCh <- migrateExtents.CopyExtents()
}
