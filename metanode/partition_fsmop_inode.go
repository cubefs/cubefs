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
	"sync/atomic"

	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/timeutil"

	"io"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

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

	//inodeInfo := mp.txProcessor.txManager.getTxInodeInfo(txIno.TxInfo.TxID, txIno.Inode.Inode)
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
	//3.insert inode in inode tree
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

	//2.register rollback item
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
		log.LogWarnf("fsmCreateLinkInode repeated, ino %v uniqID %v nlink %v", ino.Inode, uniqID, ino.GetNLink())
		return
	}
	i.IncNLink(ino.getVer())
	return
}

func (mp *metaPartition) getInodeByVer(ino *Inode) (i *Inode) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		log.LogDebugf("action[getInodeByVer] not found ino %v verseq %v", ino.Inode, ino.getVer())
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
		log.LogDebugf("action[getInodeTopLayer] not found ino %v verseq %v", ino.Inode, ino.getVer())
		return
	}
	i := item.(*Inode)
	ctime := timeutil.GetCurrentTimeUnix()
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	if ctime > i.AccessTime {
		i.AccessTime = ctime
	}

	resp.Msg = i
	return
}

func (mp *metaPartition) getInode(ino *Inode, listAll bool) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	i := mp.getInodeByVer(ino)
	if i == nil || (listAll == false && i.ShouldDelete()) {
		log.LogDebugf("action[getInode] ino  %v not found", ino)
		resp.Status = proto.OpNotExistErr
		return
	}

	ctime := timeutil.GetCurrentTimeUnix()

	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	if ctime > i.AccessTime {
		i.AccessTime = ctime
	}

	resp.Msg = i
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
		log.LogWarnf("fsmTxUnlinkInode: dir is not empty, can't remove it, txInode %v", txIno)
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
	log.LogDebugf("action[fsmUnlinkInode] mp %v ino %v", mp.config.PartitionId, ino)
	var (
		ext2Del []proto.ExtentKey
	)

	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		log.LogDebugf("action[fsmUnlinkInode] mp %v ino %v", mp.config.PartitionId, ino)
		resp.Status = proto.OpNotExistErr
		return
	}
	inode := item.(*Inode)
	if ino.getVer() == 0 && inode.ShouldDelete() {
		log.LogDebugf("action[fsmUnlinkInode] mp %v ino %v", mp.config.PartitionId, ino)
		resp.Status = proto.OpNotExistErr
		return
	}

	topLayerEmpty := inode.IsTopLayerEmptyDir()

	resp.Msg = inode

	if !mp.uniqChecker.legalIn(uniqID) {
		log.LogWarnf("fsmUnlinkInode repeat, mp %v ino %v uniqID %v nlink %v", mp.config.PartitionId, ino.Inode, uniqID, ino.GetNLink())
		return
	}

	log.LogDebugf("action[fsmUnlinkInode] mp %v get inode %v", mp.config.PartitionId, inode)
	var (
		doMore bool
		status = proto.OpOk
	)

	if ino.getVer() == 0 {
		ext2Del, doMore, status = inode.unlinkTopLayer(mp.config.PartitionId, ino, mp.verSeq, mp.multiVersionList)
	} else { // means drop snapshot
		log.LogDebugf("action[fsmUnlinkInode] mp %v req drop assigned snapshot reqseq %v inode seq %v", mp.config.PartitionId, ino.getVer(), inode.getVer())
		if ino.getVer() > inode.getVer() && !isInitSnapVer(ino.getVer()) {
			log.LogDebugf("action[fsmUnlinkInode] mp %v inode %v unlink not exist snapshot and return do nothing.reqSeq %v larger than inode seq %v",
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

	if ino.getVer() == 0 && topLayerEmpty && inode.IsEmptyDirAndNoSnapshot() { // normal deletion
		log.LogDebugf("action[fsmUnlinkInode] mp %v ino %v really be deleted, empty dir", mp.config.PartitionId, inode)
		mp.inodeTree.Delete(inode)
	} else if ino.getVer() > 0 && inode.IsEmptyDirAndNoSnapshot() { // snapshot deletion
		log.LogDebugf("action[fsmUnlinkInode] mp %v ino %v really be deleted, empty dir", mp.config.PartitionId, inode)
		mp.inodeTree.Delete(inode)
		mp.updateUsedInfo(0, -1, inode.Inode)
	}

	//Fix#760: when nlink == 0, push into freeList and delay delete inode after 7 days
	if inode.IsTempFile() {
		// all snapshot between create to last deletion cleaned
		if inode.NLink == 0 && inode.getLayerLen() == 0 {
			mp.updateUsedInfo(-1*int64(inode.Size), -1, inode.Inode)
			log.LogDebugf("action[fsmUnlinkInode] mp %v unlink inode %v and push to freeList", mp.config.PartitionId, inode)
			inode.AccessTime = time.Now().Unix()
			mp.freeList.Push(inode.Inode)
			mp.uidManager.doMinusUidSpace(inode.Uid, inode.Inode, inode.Size)
			log.LogDebugf("action[fsmUnlinkInode] mp %v ino %v", mp.config.PartitionId, inode)
		}
	}

	if len(ext2Del) > 0 {
		log.LogDebugf("action[fsmUnlinkInode] mp %v ino %v DecSplitExts ext2Del %v", mp.config.PartitionId, ino, ext2Del)
		inode.DecSplitExts(mp.config.PartitionId, ext2Del)
		mp.extDelCh <- ext2Del
	}
	log.LogDebugf("action[fsmUnlinkInode] mp %v ino %v left", mp.config.PartitionId, inode)
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
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
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
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}

	return nil
}

func (mp *metaPartition) internalDeleteInode(ino *Inode) {
	log.LogDebugf("action[internalDeleteInode] ino %v really be deleted", ino)
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
	eks := ino.HybridCouldExtents.sortedEks.(*SortedExtents).CopyExtents()
	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks); status != proto.OpOk {
		return
	}
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime, mp.volType)
	mp.updateUsedInfo(int64(ino2.Size)-oldSize, 0, ino2.Inode)
	log.LogInfof("fsmAppendExtents mpId[%v].inode(%v) deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, delExtents)

	log.LogInfof("fsmAppendExtents mpId[%v].inode(%v) DecSplitExts deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	ino2.DecSplitExts(mp.config.PartitionId, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(ino *Inode, isSplit bool) (status uint8) {
	var (
		delExtents []proto.ExtentKey
	)

	if mp.verSeq < ino.getVer() {
		status = proto.OpArgMismatchErr
		log.LogErrorf("fsmAppendExtentsWithCheck.mp %v param ino %v mp seq %v", mp.config.PartitionId, ino, mp.verSeq)
		return
	}
	status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)

	if item == nil {
		status = proto.OpNotExistErr
		return
	}

	fsmIno := item.(*Inode)
	if fsmIno.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	//defer func() {
	//	log.LogErrorf("fsmAppendExtentsWithCheck.exist mp %v param ino %v mp seq %v eks [%v]",
	//		mp.config.PartitionId, ino, mp.verSeq, fsmIno.Extents)
	//}()

	if err := fsmIno.updateStorageClass(ino.StorageClass); err != nil {
		status = proto.OpDismatchStorageClass
		return
	}

	var (
		discardExtentKey []proto.ExtentKey
	)
	oldSize := int64(fsmIno.Size)
	//get eks from inoParm, so do not need transform from HybridCouldExtents
	var (
		eks     []proto.ExtentKey
		isCache bool
	)
	if len(ino.Extents.eks) != 0 {
		isCache = true
		eks = ino.Extents.CopyExtents()
	} else {
		isCache = false
		eks = ino.HybridCouldExtents.sortedEks.(*SortedExtents).CopyExtents()
	}
	log.LogDebugf("action[fsmAppendExtentsWithCheck] inode %v hist len %v,eks %v, isCache %v",
		fsmIno.Inode, fsmIno.getLayerLen(), eks, isCache)
	if len(eks) < 1 {
		return
	}
	if len(eks) > 1 {
		discardExtentKey = eks[1:]
	}

	if status = mp.uidManager.addUidSpace(fsmIno.Uid, fsmIno.Inode, eks[:1]); status != proto.OpOk {
		log.LogErrorf("fsmAppendExtentsWithCheck.mp %v addUidSpace status %v", mp.config.PartitionId, status)
		return
	}

	log.LogDebugf("action[fsmAppendExtentsWithCheck] mp %v ino %v isSplit %v ek %v hist len %v discardExtentKey %v",
		mp.config.PartitionId, fsmIno.Inode, isSplit, eks[0], fsmIno.getLayerLen(), discardExtentKey)

	appendExtParam := &AppendExtParam{
		mpId:             mp.config.PartitionId,
		mpVer:            mp.verSeq,
		ek:               eks[0],
		ct:               ino.ModifyTime,
		discardExtents:   discardExtentKey,
		volType:          mp.volType,
		multiVersionList: mp.multiVersionList,
		isCache:          isCache,
	}

	if !isSplit {
		delExtents, status = fsmIno.AppendExtentWithCheck(appendExtParam)
		if status == proto.OpOk {
			log.LogInfof("action[fsmAppendExtentsWithCheck] mp %v DecSplitExts delExtents [%v]", mp.config.PartitionId, delExtents)
			fsmIno.DecSplitExts(appendExtParam.mpId, delExtents)
			mp.extDelCh <- delExtents
		}
		// conflict need delete eks[0], to clear garbage data
		if status == proto.OpConflictExtentsErr {
			log.LogInfof("action[fsmAppendExtentsWithCheck] mp %v OpConflictExtentsErr [%v]", mp.config.PartitionId, eks[:1])
			if !storage.IsTinyExtent(eks[0].ExtentId) && eks[0].ExtentOffset >= util.ExtentSize {
				eks[0].SetSplit(true)
			}
			mp.extDelCh <- eks[:1]
		}
	} else {
		//TODO: support hybrid-cloud
		// only the ek itself will be moved to level before
		// ino verseq be set with mp ver before submit in case other mp be updated while on flight, which will lead to
		// inconsistent between raft pairs
		delExtents, status = fsmIno.SplitExtentWithCheck(appendExtParam)
		log.LogInfof("action[fsmAppendExtentsWithCheck] mp %v DecSplitExts delExtents [%v]", mp.config.PartitionId, delExtents)
		fsmIno.DecSplitExts(mp.config.PartitionId, delExtents)
		mp.extDelCh <- delExtents
		mp.uidManager.minusUidSpace(fsmIno.Uid, fsmIno.Inode, delExtents)
	}

	// conflict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		mp.extDelCh <- eks[:1]
		mp.uidManager.minusUidSpace(fsmIno.Uid, fsmIno.Inode, eks[:1])
		log.LogDebugf("fsmAppendExtentsWithCheck mp %v delExtents inode(%v) ek(%v)", mp.config.PartitionId, fsmIno.Inode, delExtents)
	}

	mp.updateUsedInfo(int64(fsmIno.Size)-oldSize, 0, fsmIno.Inode)
	log.LogInfof("fsmAppendExtentWithCheck mp %v inode(%v) ek(%v) deleteExtents(%v) discardExtents(%v) status(%v)",
		mp.config.PartitionId, fsmIno.Inode, eks[0], delExtents, discardExtentKey, status)

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
	if err := inode.updateStorageClass(ino.StorageClass); err != nil {
		status = proto.OpDismatchStorageClass
		return
	}
	//eks := ino.ObjExtents.CopyExtents()
	eks := ino.HybridCouldExtents.sortedEks.(*SortedObjExtents).CopyExtents()
	err := inode.AppendObjExtents(eks, ino.ModifyTime)

	// if err is not nil, means obj eks exist overlap.
	if err != nil {
		log.LogErrorf("fsmAppendExtents inode(%v) err(%v)", inode.Inode, err)
		status = proto.OpConflictExtentsErr
	}
	return
}

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
	var err error
	resp = NewInodeResponse()
	log.LogDebugf("fsmExtentsTruncate. req ino %v", ino)
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
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
	log.LogInfof("fsmExtentsTruncate.mp (%v) inode(%v) DecSplitExts exts(%v)", mp.config.PartitionId, i.Inode, delExtents)
	i.DecSplitExts(mp.config.PartitionId, delExtents)
	mp.extDelCh <- delExtents
	mp.uidManager.minusUidSpace(i.Uid, i.Inode, delExtents)
	return
}

func (mp *metaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	log.LogDebugf("action[fsmEvictInode] inode %v", ino)
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		log.LogDebugf("action[fsmEvictInode] inode %v already be mark delete", ino)
		return
	}
	if proto.IsDir(i.Type) {
		if i.IsEmptyDirAndNoSnapshot() {
			i.SetDeleteMark()
		}
		return
	}

	if i.IsTempFile() {
		log.LogDebugf("action[fsmEvictInode] inode %v already linke zero and be set mark delete and be put to freelist", ino)
		i.SetDeleteMark()
		if i.isEmptyVerList() {
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
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		mp.uidManager.minusUidSpace(i.Uid, i.Inode, tinyEks)
		log.LogDebugf("fsmExtentsEmpty mp(%d) inode(%d) tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
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
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		log.LogDebugf("fsmExtentsEmpty mp(%d) inode(%d) tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
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
	log.LogInfof("fsmClearInodeCache.mp(%v) inode(%v) DecSplitExts delExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
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
	err, _ := sortExtents.UnmarshalBinary(val, v3)
	if err != nil {
		panic(fmt.Errorf("[fsmDelExtents] unmarshal sortExtents error, mp(%d), err(%s)", mp.config.PartitionId, err.Error()))
	}

	log.LogInfof("fsmDelExtents mp(%d) delExtents(%v)", mp.config.PartitionId, len(sortExtents.eks))
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

		var extend = NewExtend(ino)
		treeItem := mp.extendTree.Get(extend)
		inode := NewInode(ino, 0)
		retMsg := mp.getInode(inode, false)

		if retMsg.Status != proto.OpOk {
			log.LogErrorf("fsmSetInodeQuotaBatch get inode [%v] fail.", ino)
			resp.InodeRes[ino] = retMsg.Status
			continue
		}
		inode = retMsg.Msg
		log.LogDebugf("fsmSetInodeQuotaBatch msg [%v] inode [%v]", retMsg, inode)
		var quotaInfos = &proto.MetaQuotaInfos{
			QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
		}
		var quotaInfo = &proto.MetaQuotaInfo{
			RootInode: req.IsRoot,
		}

		if treeItem == nil {
			quotaInfos.QuotaInfoMap[req.QuotaId] = quotaInfo
			mp.extendTree.ReplaceOrInsert(extend, true)
		} else {
			extend = treeItem.(*Extend)
			value, exist := extend.Get([]byte(proto.QuotaKey))
			if exist {
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

		extend.Put([]byte(proto.QuotaKey), value, mp.verSeq)
		resp.InodeRes[ino] = proto.OpOk
		if !isExist {
			files += 1
			bytes += int64(inode.Size)
		}
	}
	mp.mqMgr.updateUsedInfo(bytes, files, req.QuotaId)
	log.LogInfof("fsmSetInodeQuotaBatch quotaId [%v] resp [%v] success.", req.QuotaId, resp)
	return
}

func (mp *metaPartition) fsmDeleteInodeQuotaBatch(req *proto.BatchDeleteMetaserverQuotaReuqest) (resp *proto.BatchDeleteMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchDeleteMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8, 0)

	for _, ino := range req.Inodes {
		var err error
		var extend = NewExtend(ino)
		treeItem := mp.extendTree.Get(extend)
		inode := NewInode(ino, 0)
		retMsg := mp.getInode(inode, false)
		if retMsg.Status != proto.OpOk {
			log.LogErrorf("fsmDeleteInodeQuotaBatch get inode [%v] fail.", ino)
			resp.InodeRes[ino] = retMsg.Status
			continue
		}
		inode = retMsg.Msg
		log.LogDebugf("fsmDeleteInodeQuotaBatch msg [%v] inode [%v]", retMsg, inode)
		var quotaInfos = &proto.MetaQuotaInfos{
			QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
		}

		if treeItem == nil {
			log.LogDebugf("fsmDeleteInodeQuotaBatch inode [%v] not has extend ", ino)
			resp.InodeRes[ino] = proto.OpOk
			continue
		} else {
			extend = treeItem.(*Extend)
			value, exist := extend.Get([]byte(proto.QuotaKey))
			if exist {
				if err = json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
					log.LogErrorf("fsmDeleteInodeQuotaBatch ino [%v] Unmarshal quotaInfos fail [%v]", ino, err)
					resp.InodeRes[ino] = proto.OpErr
					continue
				}

				_, ok := quotaInfos.QuotaInfoMap[req.QuotaId]
				if ok {
					delete(quotaInfos.QuotaInfoMap, req.QuotaId)
					if len(quotaInfos.QuotaInfoMap) == 0 {
						extend.Remove([]byte(proto.QuotaKey))
					} else {
						value, err = json.Marshal(quotaInfos.QuotaInfoMap)
						if err != nil {
							log.LogErrorf("fsmDeleteInodeQuotaBatch marsha1 quotaInfos [%v] fail [%v]", quotaInfos, err)
							resp.InodeRes[ino] = proto.OpErr
							continue
						}
						extend.Put([]byte(proto.QuotaKey), value, mp.verSeq)
					}
				} else {
					log.LogDebugf("fsmDeleteInodeQuotaBatch QuotaInfoMap can not find inode [%v] quota [%v]", ino, req.QuotaId)
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

func (mp *metaPartition) internalFreeForbiddenMigrationInode(val []byte) (err error) {
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
		log.LogDebugf("internalFreeForbiddenMigration: received internal free forbidden migration"+
			": partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		mp.freeForbiddenMigrationInode(ino)
	}
}

func (mp *metaPartition) freeForbiddenMigrationInode(ino *Inode) {
	log.LogDebugf("action[internalFreeForbiddenMigrationInode] ino %v really be freed", ino)
	atomic.StoreUint32(&ino.ForbiddenMigration, ApproverToMigration)
	mp.fmList.Delete(ino.Inode)
	return
}

func (mp *metaPartition) fsmForbiddenInodeMigration(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	log.LogDebugf("action[fsmForbiddenInodeMigration] inode %v", ino)
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	atomic.StoreUint32(&i.ForbiddenMigration, ForbiddenToMigration)
	atomic.AddUint64(&i.WriteGeneration, 1)
	mp.fmList.Put(i.Inode)
	return
}

func (mp *metaPartition) fsmRenewalInodeForbiddenMigration(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	log.LogDebugf("action[fsmForbiddenInodeMigration] inode %v", ino)
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	mp.fmList.Put(i.Inode)
	return
}
