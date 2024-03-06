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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/timeutil"
)

type InodeResponse struct {
	Status uint8
	Msg    *Inode
}

func NewInodeResponse() *InodeResponse {
	return &InodeResponse{}
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmTxCreateInode(ctx context.Context, txIno *TxInode, quotaIds []uint32) (status uint8) {
	status = proto.OpOk
	if mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID) {
		getSpan(ctx).Warnf("tx is already finish. txId %s", txIno.TxInfo.TxID)
		return proto.OpTxInfoNotExistErr
	}

	// inodeInfo := mp.txProcessor.txManager.getTxInodeInfo(txIno.TxInfo.TxID, txIno.Inode.Inode)
	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		status = proto.OpTxInodeInfoNotExistErr
		return
	}

	rbInode := NewTxRollbackInode(txIno.Inode, quotaIds, inodeInfo, TxDelete)
	status = mp.txProcessor.txResource.addTxRollbackInode(ctx, rbInode)
	if status != proto.OpOk {
		return
	}

	defer func() {
		if status != proto.OpOk {
			mp.txProcessor.txResource.deleteTxRollbackInode(ctx, txIno.Inode.Inode, txIno.TxInfo.TxID)
		}
	}()
	// 3.insert inode in inode tree
	return mp.fsmCreateInode(ctx, txIno.Inode)
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(ctx context.Context, ino *Inode) (status uint8) {
	if status = mp.uidManager.addUidSpace(ino.Uid, ino.Inode, nil); status != proto.OpOk {
		return
	}

	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}

	return
}

func (mp *metaPartition) fsmTxCreateLinkInode(ctx context.Context, txIno *TxInode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	if mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID) {
		getSpan(ctx).Warnf("tx is already finish. txId %s", txIno.TxInfo.TxID)
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
	resp.Status = mp.txProcessor.txResource.addTxRollbackInode(ctx, rbInode)
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
			mp.txProcessor.txResource.deleteTxRollbackInode(ctx, txIno.Inode.Inode, txIno.TxInfo.TxID)
		}
	}()

	return mp.fsmCreateLinkInode(ctx, txIno.Inode, 0)
}

func (mp *metaPartition) fsmCreateLinkInode(ctx context.Context, ino *Inode, uniqID uint64) (resp *InodeResponse) {
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
		getSpan(ctx).Warnf("repeated, ino[%v] uniqID %v nlink %v", ino.Inode, uniqID, ino.GetNLink())
		return
	}
	i.IncNLink(ctx, ino.getVer())
	return
}

func (mp *metaPartition) getInodeByVer(ctx context.Context, ino *Inode) (i *Inode) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		return
	}
	i, _ = item.(*Inode).getInoByVer(ctx, ino.getVer(), false)
	return
}

func (mp *metaPartition) getInodeTopLayer(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
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

func (mp *metaPartition) getInode(ctx context.Context, ino *Inode, listAll bool) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	i := mp.getInodeByVer(ctx, ino)
	if i == nil || (!listAll && i.ShouldDelete()) {
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

func (mp *metaPartition) hasInode(ctx context.Context, ino *Inode) (ok bool) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		return
	}
	i := mp.getInodeByVer(ctx, ino)
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

func (mp *metaPartition) fsmTxUnlinkInode(ctx context.Context, txIno *TxInode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if proto.IsDir(txIno.Inode.Type) && txIno.TxInfo.TxType == proto.TxTypeRemove && txIno.Inode.NLink > 2 {
		resp.Status = proto.OpNotEmpty
		getSpan(ctx).Warnf("dir is not empty, can't remove it, txinode[%v]", txIno)
		return
	}

	if mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID) {
		getSpan(ctx).Warnf("tx is already finish. txId %s", txIno.TxInfo.TxID)
		resp.Status = proto.OpTxInfoNotExistErr
		return
	}

	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		resp.Status = proto.OpTxInodeInfoNotExistErr
		return
	}
	var quotaIds []uint32
	quotaIds, _ = mp.isExistQuota(ctx, txIno.Inode.Inode)

	rbInode := NewTxRollbackInode(txIno.Inode, quotaIds, inodeInfo, TxAdd)
	resp.Status = mp.txProcessor.txResource.addTxRollbackInode(ctx, rbInode)
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
			mp.txProcessor.txResource.deleteTxRollbackInode(ctx, txIno.Inode.Inode, txIno.TxInfo.TxID)
		}
	}()

	resp = mp.fsmUnlinkInode(ctx, txIno.Inode, 0)
	if resp.Status != proto.OpOk {
		return
	}

	if txIno.TxInfo.TxType == proto.TxTypeRename {
		mp.fsmEvictInode(ctx, txIno.Inode)
	}

	return
}

// normal unlink seq is 0
// snapshot unlink seq is snapshotVersion
// fsmUnlinkInode delete the specified inode from inode tree.

func (mp *metaPartition) fsmUnlinkInode(ctx context.Context, ino *Inode, uniqID uint64) (resp *InodeResponse) {
	span := getSpan(ctx).WithOperation(fmt.Sprintf("fsmUnlinkInode-mp.%d", mp.config.PartitionId))
	span.Debugf("ino[%v]", ino)
	var ext2Del []proto.ExtentKey

	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		span.Debugf("ino[%v] not found", ino)
		resp.Status = proto.OpNotExistErr
		return
	}
	inode := item.(*Inode)
	if ino.getVer() == 0 && inode.ShouldDelete() {
		span.Debugf("ino[%v] not found", ino)
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = inode
	if !mp.uniqChecker.legalIn(uniqID) {
		span.Warnf("repeat, ino[%v] uniqID %v nlink %v", ino.Inode, uniqID, ino.GetNLink())
		return
	}

	span.Debugf("get inode[%v]", inode)
	var (
		doMore bool
		status = proto.OpOk
	)

	if ino.getVer() == 0 {
		ext2Del, doMore, status = inode.unlinkTopLayer(ctx, mp.config.PartitionId, ino, mp.verSeq, mp.multiVersionList)
	} else { // means drop snapshot
		span.Debugf("req drop assigned snapshot reqseq [%v] inode seq [%v]", ino.getVer(), inode.getVer())
		if ino.getVer() > inode.getVer() && !isInitSnapVer(ino.getVer()) {
			span.Debugf("inode[%v] unlink not exist snapshot and return do nothing.reqseq [%v] larger than inode seq [%v]",
				ino.Inode, ino.getVer(), inode.getVer())
			return
		} else {
			ext2Del, doMore, status = inode.unlinkVerInList(ctx, mp.config.PartitionId, ino, mp.verSeq, mp.multiVersionList)
		}
	}
	if !doMore {
		resp.Status = status
		return
	}

	if inode.IsEmptyDirAndNoSnapshot() {
		if ino.NLink < 2 { // snapshot deletion
			span.Debugf("ino[%v] really be deleted, empty dir", inode)
			mp.inodeTree.Delete(inode)
			mp.updateUsedInfo(ctx, 0, -1, inode.Inode)
		}
	} else if inode.IsTempFile() {
		// all snapshot between create to last deletion cleaned
		if inode.NLink == 0 && inode.getLayerLen() == 0 {
			mp.updateUsedInfo(ctx, -1*int64(inode.Size), -1, inode.Inode)
			span.Debugf("unlink inode[%v] and push to freeList", inode)
			inode.AccessTime = time.Now().Unix()
			mp.freeList.Push(inode.Inode)
			mp.uidManager.doMinusUidSpace(inode.Uid, inode.Inode, inode.Size)
			span.Debugf("ino[%v]", inode)
		}
	}

	if len(ext2Del) > 0 {
		span.Debugf("ino[%v] DecSplitExts ext2Del %v", ino, ext2Del)
		inode.DecSplitExts(ctx, mp.config.PartitionId, ext2Del)
		mp.extDelCh <- ext2Del
	}
	span.Debugf("ino[%v] left", inode)
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInodeBatch(ctx context.Context, ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		status := mp.inodeInTx(ctx, ino.Inode)
		if status != proto.OpOk {
			resp = append(resp, &InodeResponse{Status: status})
			continue
		}
		resp = append(resp, mp.fsmUnlinkInode(ctx, ino, 0))
	}
	return
}

func (mp *metaPartition) internalHasInode(ino *Inode) bool {
	return mp.inodeTree.Has(ino)
}

func (mp *metaPartition) internalDelete(ctx context.Context, val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	span := getSpan(ctx)
	for {
		err = binary.Read(buf, binary.BigEndian, &ino.Inode)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		span.Debugf("internalDelete: received internal delete: partitionID(%v) inode[%v]",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}
}

func (mp *metaPartition) internalDeleteBatch(ctx context.Context, val []byte) error {
	if len(val) == 0 {
		return nil
	}
	inodes, err := InodeBatchUnmarshal(val)
	if err != nil {
		return nil
	}

	span := getSpan(ctx)
	for _, ino := range inodes {
		span.Debugf("internalDelete: received internal delete: partitionID(%v) inode[%v]",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}

	return nil
}

func (mp *metaPartition) internalDeleteInode(ino *Inode) {
	mp.inodeTree.Delete(ino)
	mp.freeList.Remove(ino.Inode)
	mp.extendTree.Delete(&Extend{inode: ino.Inode}) // Also delete extend attribute.
}

func (mp *metaPartition) fsmAppendExtents(ctx context.Context, ino *Inode) (status uint8) {
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
	eks := ino.Extents.CopyExtents()
	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks); status != proto.OpOk {
		return
	}
	span := getSpan(ctx)
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime, mp.volType)
	mp.updateUsedInfo(ctx, int64(ino2.Size)-oldSize, 0, ino2.Inode)
	span.Infof("fsmAppendExtents mpId[%v].inode[%v] deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, delExtents)

	span.Infof("fsmAppendExtents mpId[%v].inode[%v] DecSplitExts deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	ino2.DecSplitExts(ctx, mp.config.PartitionId, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(ctx context.Context, ino *Inode, isSplit bool) (status uint8) {
	var (
		delExtents       []proto.ExtentKey
		discardExtentKey []proto.ExtentKey
	)

	if mp.verSeq < ino.getVer() {
		status = proto.OpArgMismatchErr
		getSpan(ctx).Errorf("mp[%v] param ino[%v] mp seq [%v]", mp.config.PartitionId, ino, mp.verSeq)
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

	oldSize := int64(fsmIno.Size)
	eks := ino.Extents.CopyExtents()

	if len(eks) < 1 {
		return
	}
	if len(eks) > 1 {
		discardExtentKey = eks[1:]
	}

	if status = mp.uidManager.addUidSpace(fsmIno.Uid, fsmIno.Inode, eks[:1]); status != proto.OpOk {
		getSpan(ctx).Errorf("mp[%v] addUidSpace status [%v]", mp.config.PartitionId, status)
		return
	}

	span := getSpan(ctx).WithOperation("fsmAppendExtentsWithCheck")
	span.Debugf("mp[%v] ver [%v] ino[%v] isSplit %v ek [%v] hist len %v discardExtentKey %v",
		mp.config.PartitionId, mp.verSeq, fsmIno.Inode, isSplit, eks[0], fsmIno.getLayerLen(), discardExtentKey)

	appendExtParam := &AppendExtParam{
		mpId:             mp.config.PartitionId,
		mpVer:            mp.verSeq,
		ek:               eks[0],
		ct:               ino.ModifyTime,
		discardExtents:   discardExtentKey,
		volType:          mp.volType,
		multiVersionList: mp.multiVersionList,
	}

	if !isSplit {
		delExtents, status = fsmIno.AppendExtentWithCheck(ctx, appendExtParam)
		if status == proto.OpOk {
			span.Infof("mp[%v] DecSplitExts delExtents [%v]", mp.config.PartitionId, delExtents)
			fsmIno.DecSplitExts(ctx, appendExtParam.mpId, delExtents)
			mp.extDelCh <- delExtents
		}
		// conflict need delete eks[0], to clear garbage data
		if status == proto.OpConflictExtentsErr {
			span.Infof("mp[%v] OpConflictExtentsErr [%v]", mp.config.PartitionId, eks[:1])
			if !storage.IsTinyExtent(eks[0].ExtentId) && eks[0].ExtentOffset >= util.ExtentSize {
				eks[0].SetSplit(true)
			}
			mp.extDelCh <- eks[:1]
		}
	} else {
		// only the ek itself will be moved to level before
		// ino verseq be set with mp ver before submit in case other mp be updated while on flight, which will lead to
		// inconsistent between raft pairs
		delExtents, status = fsmIno.SplitExtentWithCheck(ctx, appendExtParam)
		span.Infof("mp[%v] DecSplitExts delExtents [%v]", mp.config.PartitionId, delExtents)
		fsmIno.DecSplitExts(ctx, mp.config.PartitionId, delExtents)
		mp.extDelCh <- delExtents
		mp.uidManager.minusUidSpace(fsmIno.Uid, fsmIno.Inode, delExtents)
	}

	// conflict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		mp.extDelCh <- eks[:1]
		mp.uidManager.minusUidSpace(fsmIno.Uid, fsmIno.Inode, eks[:1])
		span.Debugf("mp[%v] delExtents inode[%v] ek(%v)", mp.config.PartitionId, fsmIno.Inode, delExtents)
	}

	mp.updateUsedInfo(ctx, int64(fsmIno.Size)-oldSize, 0, fsmIno.Inode)
	span.Infof("mp[%v] inode[%v] ek(%v) deleteExtents(%v) discardExtents(%v) status(%v)",
		mp.config.PartitionId, fsmIno.Inode, eks[0], delExtents, discardExtentKey, status)
	return
}

func (mp *metaPartition) fsmAppendObjExtents(ctx context.Context, ino *Inode) (status uint8) {
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

	eks := ino.ObjExtents.CopyExtents()
	err := inode.AppendObjExtents(eks, ino.ModifyTime)
	// if err is not nil, means obj eks exist overlap.
	if err != nil {
		getSpan(ctx).Errorf("fsmAppendExtents inode[%v] err(%v)", inode.Inode, err)
		status = proto.OpConflictExtentsErr
	}
	return
}

func (mp *metaPartition) fsmExtentsTruncate(ctx context.Context, ino *Inode) (resp *InodeResponse) {
	var err error
	resp = NewInodeResponse()
	span := getSpan(ctx)
	span.Debugf("fsmExtentsTruncate. req ino[%v]", ino)
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
		i.insertEkRefMap(ctx, mp.config.PartitionId, ek)
	}

	if i.getVer() != mp.verSeq {
		i.CreateVer(ctx, mp.verSeq)
	}
	i.Lock()
	defer i.Unlock()

	if err = i.CreateLowerVersion(ctx, i.getVer(), mp.multiVersionList); err != nil {
		return
	}
	oldSize := int64(i.Size)
	delExtents := i.ExtentsTruncate(ctx, ino.Size, ino.ModifyTime, doOnLastKey, insertSplitKey)

	if len(delExtents) == 0 {
		return
	}

	if delExtents, err = i.RestoreExts2NextLayer(ctx, mp.config.PartitionId, delExtents, mp.verSeq, 0); err != nil {
		panic("RestoreExts2NextLayer should not be error")
	}
	mp.updateUsedInfo(ctx, int64(i.Size)-oldSize, 0, i.Inode)

	// now we should delete the extent
	span.Infof("fsmExtentsTruncate.mp (%v) inode[%v] DecSplitExts exts(%v)", mp.config.PartitionId, i.Inode, delExtents)
	i.DecSplitExts(ctx, mp.config.PartitionId, delExtents)
	mp.extDelCh <- delExtents
	mp.uidManager.minusUidSpace(i.Uid, i.Inode, delExtents)
	return
}

func (mp *metaPartition) fsmEvictInode(ctx context.Context, ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	span := getSpan(ctx)
	span.Debugf("action[fsmEvictInode] inode[%v]", ino)
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		span.Debugf("action[fsmEvictInode] inode[%v] already be mark delete", ino)
		return
	}
	if proto.IsDir(i.Type) {
		if i.IsEmptyDirAndNoSnapshot() {
			i.SetDeleteMark()
		}
		return
	}

	if i.IsTempFile() {
		span.Debugf("action[fsmEvictInode] inode[%v] already linke zero and be set mark delete and be put to freelist", ino)
		if i.isEmptyVerList() {
			i.SetDeleteMark()
			mp.freeList.Push(i.Inode)
		}
	}
	return
}

func (mp *metaPartition) fsmBatchEvictInode(ctx context.Context, ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		status := mp.inodeInTx(ctx, ino.Inode)
		if status != proto.OpOk {
			resp = append(resp, &InodeResponse{Status: status})
			return
		}
		resp = append(resp, mp.fsmEvictInode(ctx, ino))
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

func (mp *metaPartition) fsmSetAttr(ctx context.Context, req *SetattrRequest) (err error) {
	getSpan(ctx).Debugf("action[fsmSetAttr] req %v", req)
	ino := NewInode(req.Inode, req.Mode)
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		return
	}
	ino = item.(*Inode)
	if ino.ShouldDelete() {
		return
	}
	ino.SetAttr(ctx, req)
	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmExtentsEmpty(ctx context.Context, ino *Inode) (status uint8) {
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
	span := getSpan(ctx)
	span.Debugf("action[fsmExtentsEmpty] mp[%v] ino[%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	span.Debugf("action[fsmExtentsEmpty] mp[%v] ino[%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		mp.uidManager.minusUidSpace(i.Uid, i.Inode, tinyEks)
		span.Debugf("fsmExtentsEmpty mp[%v] inode[%d] tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	i.EmptyExtents(ino.ModifyTime)
	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmDelVerExtents(ctx context.Context, ino *Inode) (status uint8) {
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
	span := getSpan(ctx)
	span.Debugf("action[fsmExtentsEmpty] mp[%v] ino[%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	span.Debugf("action[fsmExtentsEmpty] mp[%v] ino[%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		span.Debugf("fsmExtentsEmpty mp[%v] inode[%d] tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	i.EmptyExtents(ino.ModifyTime)
	return
}

func (mp *metaPartition) fsmClearInodeCache(ctx context.Context, ino *Inode) (status uint8) {
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
	getSpan(ctx).Infof("fsmClearInodeCache.mp[%v] inode[%v] DecSplitExts delExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	if len(delExtents) > 0 {
		ino2.DecSplitExts(ctx, mp.config.PartitionId, delExtents)
		mp.extDelCh <- delExtents
	}
	return
}

// attion: unmarshal error will disard extent
func (mp *metaPartition) fsmSendToChan(ctx context.Context, val []byte, v3 bool) (status uint8) {
	sortExtents := NewSortedExtents()
	// ek for del don't need version info
	err, _ := sortExtents.UnmarshalBinary(val, v3)
	if err != nil {
		panic(fmt.Errorf("[fsmDelExtents] unmarshal sortExtents error, mp[%v], err(%s)", mp.config.PartitionId, err.Error()))
	}

	getSpan(ctx).Infof("fsmDelExtents mp[%v] delExtents(%v)", mp.config.PartitionId, len(sortExtents.eks))
	mp.extDelCh <- sortExtents.eks
	return
}

func (mp *metaPartition) fsmSetInodeQuotaBatch(ctx context.Context, req *proto.BatchSetMetaserverQuotaReuqest,
) (resp *proto.BatchSetMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchSetMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8, 0)
	span := getSpan(ctx)
	for _, ino := range req.Inodes {
		var isExist bool
		var err error

		extend := NewExtend(ino)
		treeItem := mp.extendTree.Get(extend)
		inode := NewInode(ino, 0)
		retMsg := mp.getInode(ctx, inode, false)

		if retMsg.Status != proto.OpOk {
			span.Errorf("fsmSetInodeQuotaBatch get inode[%v] fail.", ino)
			resp.InodeRes[ino] = retMsg.Status
			continue
		}
		inode = retMsg.Msg
		span.Debugf("fsmSetInodeQuotaBatch msg [%v] inode[%v]", retMsg, inode)
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
			value, exist := extend.Get([]byte(proto.QuotaKey))
			if exist {
				if err = json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
					span.Errorf("set quota Unmarshal quotaInfos fail [%v]", err)
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
			span.Errorf("set quota marsha1 quotaInfos [%v] fail [%v]", quotaInfos, err)
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
	mp.mqMgr.updateUsedInfo(ctx, bytes, files, req.QuotaId)
	span.Infof("fsmSetInodeQuotaBatch quotaId [%v] resp [%v] success.", req.QuotaId, resp)
	return
}

func (mp *metaPartition) fsmDeleteInodeQuotaBatch(ctx context.Context, req *proto.BatchDeleteMetaserverQuotaReuqest,
) (resp *proto.BatchDeleteMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchDeleteMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8, 0)
	span := getSpan(ctx)
	for _, ino := range req.Inodes {
		var err error
		extend := NewExtend(ino)
		treeItem := mp.extendTree.Get(extend)
		inode := NewInode(ino, 0)
		retMsg := mp.getInode(ctx, inode, false)
		if retMsg.Status != proto.OpOk {
			span.Errorf("fsmDeleteInodeQuotaBatch get inode[%v] fail.", ino)
			resp.InodeRes[ino] = retMsg.Status
			continue
		}
		inode = retMsg.Msg
		span.Debugf("fsmDeleteInodeQuotaBatch msg [%v] inode[%v]", retMsg, inode)
		quotaInfos := &proto.MetaQuotaInfos{
			QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
		}

		if treeItem == nil {
			span.Debugf("fsmDeleteInodeQuotaBatch inode[%v] not has extend ", ino)
			resp.InodeRes[ino] = proto.OpOk
			continue
		} else {
			extend = treeItem.(*Extend)
			value, exist := extend.Get([]byte(proto.QuotaKey))
			if exist {
				if err = json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
					span.Errorf("fsmDeleteInodeQuotaBatch ino[%v] Unmarshal quotaInfos fail [%v]", ino, err)
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
							span.Errorf("fsmDeleteInodeQuotaBatch marsha1 quotaInfos [%v] fail [%v]", quotaInfos, err)
							resp.InodeRes[ino] = proto.OpErr
							continue
						}
						extend.Put([]byte(proto.QuotaKey), value, mp.verSeq)
					}
				} else {
					span.Debugf("fsmDeleteInodeQuotaBatch QuotaInfoMap can not find inode[%v] quota [%v]", ino, req.QuotaId)
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
	mp.mqMgr.updateUsedInfo(ctx, bytes, files, req.QuotaId)
	span.Infof("fsmDeleteInodeQuotaBatch quotaId [%v] resp [%v] success.", req.QuotaId, resp)
	return
}
