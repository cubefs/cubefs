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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
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
func (mp *metaPartition) fsmTxCreateInode(dbHandle interface{}, txIno *TxInode, quotaIds []uint32) (status uint8, err error) {
	status = proto.OpOk
	done, err := mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID)
	if err != nil {
		return
	}
	if done {
		log.LogWarnf("fsmTxCreateInode: tx is already finish. txId %s", txIno.TxInfo.TxID)
		return proto.OpTxInfoNotExistErr, nil
	}

	// inodeInfo := mp.txProcessor.txManager.getTxInodeInfo(txIno.TxInfo.TxID, txIno.Inode.Inode)
	inodeInfo, ok := txIno.TxInfo.TxInodeInfos[txIno.Inode.Inode]
	if !ok {
		status = proto.OpTxInodeInfoNotExistErr
		return
	}

	rbInode := NewTxRollbackInode(txIno.Inode, quotaIds, inodeInfo, TxDelete)
	status, err = mp.txProcessor.txResource.addTxRollbackInode(dbHandle, rbInode)
	if err != nil {
		return
	}
	if status != proto.OpOk {
		return
	}

	defer func() {
		if status != proto.OpOk {
			_, err = mp.txProcessor.txResource.deleteTxRollbackInode(dbHandle, txIno.Inode.Inode, txIno.TxInfo.TxID)
			if err != nil {
				log.LogErrorf("[fsmTxCreateInode] failed to delete rb inode(%v), err(%v)", txIno, err)
				return
			}
		}
	}()
	// 3.insert inode in inode tree
	return mp.fsmCreateInode(dbHandle, txIno.Inode)
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(dbHandle interface{}, ino *Inode) (status uint8, err error) {
	if status = mp.uidManager.addUidSpace(ino.Uid, ino.Inode, nil); status != proto.OpOk {
		return
	}

	status = proto.OpOk
	if _, _, err = mp.inodeTree.Create(dbHandle, ino, false); err != nil {
		status = proto.OpErr
		return
	}

	return
}

func (mp *metaPartition) fsmTxCreateLinkInode(dbHandle interface{}, txIno *TxInode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	done, err := mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID)
	if done {
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
	resp.Status, err = mp.txProcessor.txResource.addTxRollbackInode(dbHandle, rbInode)
	if err != nil {
		return
	}
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
			_, err = mp.txProcessor.txResource.deleteTxRollbackInode(dbHandle, txIno.Inode.Inode, txIno.TxInfo.TxID)
			if err != nil {
				log.LogErrorf("[fsmTxCreateLinkInode] failed to delete rb inode(%v), err(%v)", txIno, err)
				return
			}
		}
	}()

	return mp.fsmCreateLinkInode(dbHandle, txIno.Inode, 0)
}

func (mp *metaPartition) fsmCreateLinkInode(dbHandle interface{}, ino *Inode, uniqID uint64) (resp *InodeResponse, err error) {
	var i *Inode
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	i, err = mp.inodeTree.Get(ino.Inode)
	if i == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

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
	if err = mp.inodeTree.Update(dbHandle, i); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}

func (mp *metaPartition) getInodeByVer(ino *Inode) (i *Inode) {
	item, _ := mp.inodeTree.Get(ino.Inode)
	if item == nil {
		log.LogDebugf("action[getInodeByVer] not found ino[%v] verseq [%v]", ino.Inode, ino.getVer())
		return
	}
	i, _ = item.getInoByVer(ino.getVer(), false)
	return
}

func (mp *metaPartition) getInodeTopLayer(ino *Inode) (resp *InodeResponse) {

	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	i, err := mp.inodeTree.Get(ino.Inode)
	if err != nil {
		log.LogErrorf("[getInodeTopLayer] failed to get inode(%v), err(%v)", ino, err)
		return
	}
	if i == nil {
		resp.Status = proto.OpNotExistErr
		log.LogDebugf("action[getInodeTopLayer] not found ino[%v] verseq [%v]", ino.Inode, ino.getVer())
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
	item, _ := mp.inodeTree.Get(ino.Inode)
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
//func (mp *metaPartition) Ascend(f func(i BtreeItem) bool) {
//	mp.inodeTree.R(f)
//}

func (mp *metaPartition) fsmTxUnlinkInode(dbHandle interface{}, txIno *TxInode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if proto.IsDir(txIno.Inode.Type) && txIno.TxInfo.TxType == proto.TxTypeRemove && txIno.Inode.NLink > 2 {
		resp.Status = proto.OpNotEmpty
		log.LogWarnf("fsmTxUnlinkInode: dir is not empty, can't remove it, txinode[%v]", txIno)
		return
	}

	done, err := mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID)
	if err != nil {
		return
	}
	if done {
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
	resp.Status, err = mp.txProcessor.txResource.addTxRollbackInode(dbHandle, rbInode)
	if err != nil {
		return
	}
	if resp.Status == proto.OpExistErr {
		resp.Status = proto.OpOk
		item, _ := mp.inodeTree.Get(txIno.Inode.Inode)
		if item != nil {
			resp.Msg = item
		}
		return
	}
	if resp.Status != proto.OpOk {
		return
	}

	defer func() {
		if resp.Status != proto.OpOk {
			_, err = mp.txProcessor.txResource.deleteTxRollbackInode(dbHandle, txIno.Inode.Inode, txIno.TxInfo.TxID)
			if err != nil {
				log.LogErrorf("[fsmTxUnlinkInode] failed to delete rb inode(%v), err(%v)", txIno, err)
			}
		}
	}()

	log.LogDebugf("action[fsmTxUnlinkInode] invoke fsmUnlinkInode")
	resp, err = mp.fsmUnlinkInode(dbHandle, txIno.Inode, 1, 0)
	if resp.Status != proto.OpOk {
		return
	}

	if txIno.TxInfo.TxType == proto.TxTypeRename {
		mp.fsmEvictInode(dbHandle, txIno.Inode)
	}

	return
}

// normal unlink seq is 0
// snapshot unlink seq is snapshotVersion
// fsmUnlinkInode delete the specified inode from inode tree.

func (mp *metaPartition) fsmUnlinkInode(dbHandle interface{}, ino *Inode, unlinkNum int, uniqID uint64) (resp *InodeResponse, err error) {
	log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, ino)
	var ext2Del []proto.ExtentKey

	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	inode, _ := mp.inodeTree.Get(ino.Inode)
	// NOTE: if not found
	if inode == nil {
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, ino)
		resp.Status = proto.OpNotExistErr
		return
	}
	// NOTE: no snapshot and should delete
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

	// NOTE: remove top level
	if ino.getVer() == 0 {
		ext2Del, doMore, status = inode.unlinkTopLayer(mp.config.PartitionId, ino, mp.verSeq, mp.multiVersionList)
	} else { // means drop snapshot
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] req drop assigned snapshot reqseq [%v] inode seq [%v]", mp.config.PartitionId, ino.getVer(), inode.getVer())
		if ino.getVer() > inode.getVer() && !isInitSnapVer(ino.getVer()) {
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] inode[%v] unlink not exist snapshot and return do nothing.reqseq [%v] larger than inode seq [%v]",
				mp.config.PartitionId, ino.Inode, ino.getVer(), inode.getVer())
			return
		}
		ext2Del, doMore, status = inode.unlinkVerInList(mp.config.PartitionId, ino, mp.verSeq, mp.multiVersionList)
	}
	if !doMore {
		if err = mp.inodeTree.Update(dbHandle, inode); err != nil {
			resp.Status = proto.OpErr
			return
		}
		resp.Status = status
		return
	}
	deleted := false
	log.LogDebugf("[fsmUnlinkInode] unlink ino(%v)", inode)
	if inode.IsEmptyDirAndNoSnapshot() { // normal deletion
		if inode.NLink < 2 { // snapshot deletion
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v] really be deleted, empty dir", mp.config.PartitionId, inode)
			_, err = mp.inodeTree.Delete(dbHandle, inode.Inode)
			if err != nil {
				resp.Status = proto.OpErr
				return
			}
			mp.updateUsedInfo(0, -1, inode.Inode)
			deleted = true
		}
	} else if inode.IsTempFile() {
		// all snapshot between create to last deletion cleaned
		if inode.NLink == 0 && inode.getLayerLen() == 0 {
			mp.updateUsedInfo(-1*int64(inode.Size), -1, inode.Inode)
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] unlink inode[%v] and push to freeList", mp.config.PartitionId, inode)
			inode.AccessTime = time.Now().Unix()
			mp.uidManager.doMinusUidSpace(inode.Uid, inode.Inode, inode.Size)
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, inode)

			delExtents := inode.ClearAllExtsOfflineInode(mp.GetBaseConfig().PartitionId)
			for _, eks := range delExtents {
				for _, ek := range eks {
					dek := NewDeletedExtentKey(ek, inode.Inode, mp.AllocDeletedExtentId())
					err = mp.deletedExtentsTree.Put(dbHandle, dek)
					if err != nil {
						return
					}
				}
			}
			// mp.freeList.Push(inode.Inode)
		}
	}

	if len(ext2Del) > 0 {
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v] DecSplitExts ext2Del %v", mp.config.PartitionId, ino, ext2Del)
		inode.DecSplitExts(mp.config.PartitionId, ext2Del)
		for _, ek := range ext2Del {
			dek := NewDeletedExtentKey(&ek, inode.Inode, mp.AllocDeletedExtentId())
			mp.deletedExtentsTree.Put(dbHandle, dek)
		}
		// mp.extDelCh <- ext2Del
	}
	if !deleted {
		if err = mp.inodeTree.Update(dbHandle, inode); err != nil {
			resp.Status = proto.OpErr
			return
		}
	}
	log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v] left", mp.config.PartitionId, inode)
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
// todo: mul version need update, unlink can not support
func (mp *metaPartition) fsmUnlinkInodeBatch(dbHandle interface{}, batchInode InodeBatch) (resp []*InodeResponse, err error) {
	defer func() {
		if err != nil {
			for index := 0; index < len(batchInode); index++ {
				resp = append(resp, &InodeResponse{Status: proto.OpErr, Msg: batchInode[index]})
			}
		}
	}()
	resp = make([]*InodeResponse, 0)
	inodeUnlinkNumMap := make(map[uint64]int, len(batchInode))
	for _, inode := range batchInode {
		if _, ok := inodeUnlinkNumMap[inode.Inode]; !ok {
			inodeUnlinkNumMap[inode.Inode] = 1
			continue
		}
		inodeUnlinkNumMap[inode.Inode]++
	}

	for inodeID, unlinkNum := range inodeUnlinkNumMap {
		var status uint8
		status, err = mp.inodeInTx(inodeID)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = append(resp, &InodeResponse{Status: status})
			continue
		}
		var rsp *InodeResponse
		//todo inode info miss
		log.LogDebugf("[fsmUnlinkInodeBatch] unlink inode(%v)", inodeID)
		rsp, err = mp.fsmUnlinkInode(dbHandle, NewInode(inodeID, 0), unlinkNum, 0)
		if err != nil {
			resp = resp[:0]
			return
		}
		resp = append(resp, rsp)
	}
	return
}

func (mp *metaPartition) internalDelete(dbHandle interface{}, val []byte) (err error) {
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
		mp.internalDeleteInode(dbHandle, ino)
	}
}

func (mp *metaPartition) internalDeleteBatch(dbHandle interface{}, val []byte) error {
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
		mp.internalDeleteInode(dbHandle, ino)
	}

	return nil
}

func (mp *metaPartition) internalDeleteInode(dbHandle interface{}, ino *Inode) (err error) {
	log.LogDebugf("action[internalDeleteInode] ino[%v] really be deleted", ino)
	_, err = mp.inodeTree.Delete(dbHandle, ino.Inode)
	if err != nil {
		return
	}
	var snap Snapshot
	start := NewDeletedExtentKeyPrefix(mp.config.PartitionId, ino.Inode)
	end := NewDeletedExtentKeyPrefix(mp.config.PartitionId, ino.Inode+1)
	snap, err = NewSnapshot(mp)
	if err != nil {
		log.LogErrorf("[internalDeleteInode] failed to get snapshot, mp(%v), err(%v)", mp.config.PartitionId, err)
		return
	}
	defer snap.Close()
	// NOTE: delete from dek tree
	err = snap.RangeWithScope(DeletedExtentsType, start, end, func(item interface{}) (bool, error) {
		dek := item.(*DeletedExtentKey)
		if _, err = mp.deletedExtentsTree.Delete(dbHandle, dek); err != nil {
			return false, err
		}
		return true, nil
	})

	if err != nil {
		log.LogErrorf("[internalDeleteInode] failed to delete dek from dek tree, err(%v)", err)
		return
	}
	// mp.freeList.Remove(ino.Inode)
	_, err = mp.extendTree.Delete(dbHandle, ino.Inode) // Also delete extend attribute.
	return
}

func (mp *metaPartition) fsmAppendExtents(dbHandle interface{}, ino *Inode) (status uint8, err error) {
	status = proto.OpOk
	var ino2 *Inode
	ino2, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}

	if ino2 == nil {
		status = proto.OpNotExistErr
		return
	}
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	oldSize := int64(ino2.Size)
	eks := ino.Extents.CopyExtents()
	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks); status != proto.OpOk {
		return
	}
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime, mp.volType)
	mp.updateUsedInfo(int64(ino2.Size)-oldSize, 0, ino2.Inode)
	log.LogInfof("fsmAppendExtents mpId[%v].inode[%v] deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, delExtents)

	log.LogInfof("fsmAppendExtents mpId[%v].inode[%v] DecSplitExts deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	ino2.DecSplitExts(mp.config.PartitionId, delExtents)

	if err = mp.inodeTree.Put(dbHandle, ino2); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, ino2.Inode, delExtents, err)
		return
	}

	for _, ek := range delExtents {
		dek := NewDeletedExtentKey(&ek, ino2.Inode, mp.AllocDeletedExtentId())
		err = mp.deletedExtentsTree.Put(dbHandle, dek)
		if err != nil {
			status = proto.OpErr
			return
		}
	}
	// mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(dbHandle interface{}, ino *Inode, isSplit bool) (status uint8, err error) {
	var (
		delExtents       []proto.ExtentKey
		discardExtentKey []proto.ExtentKey
		delEkFlag        bool
		fsmIno           *Inode
	)

	if mp.verSeq < ino.getVer() {
		status = proto.OpArgMismatchErr
		log.LogErrorf("[fsmAppendExtentsWithCheck] mp[%v] param ino[%v] mp seq [%v]", mp.config.PartitionId, ino, mp.verSeq)
		return
	}
	status = proto.OpOk
	fsmIno, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}

	if fsmIno == nil || fsmIno.ShouldDelete() {
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
		log.LogErrorf("[fsmAppendExtentsWithCheck] mp[%v] addUidSpace status [%v]", mp.config.PartitionId, status)
		return
	}

	log.LogDebugf("action[fsmAppendExtentsWithCheck] mp[%v] ver [%v] ino[%v] isSplit %v ek [%v] hist len %v discardExtentKey %v",
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
		delExtents, status = fsmIno.AppendExtentWithCheck(appendExtParam)
		if status == proto.OpOk {
			log.LogInfof("action[fsmAppendExtentsWithCheck] mp[%v] DecSplitExts delExtents [%v]", mp.config.PartitionId, delExtents)
			fsmIno.DecSplitExts(appendExtParam.mpId, delExtents)
			delEkFlag = true
		}
		// conflict need delete eks[0], to clear garbage data
		if status == proto.OpConflictExtentsErr {
			log.LogInfof("action[fsmAppendExtentsWithCheck] mp[%v] OpConflictExtentsErr [%v]", mp.config.PartitionId, eks[:1])
			if !storage.IsTinyExtent(eks[0].ExtentId) && eks[0].ExtentOffset >= util.ExtentSize {
				eks[0].SetSplit(true)
			}
			for _, ek := range eks[:1] {
				dek := NewDeletedExtentKey(&ek, fsmIno.Inode, mp.AllocDeletedExtentId())
				err = mp.deletedExtentsTree.Put(dbHandle, dek)
				if err != nil {
					status = proto.OpErr
					return
				}
			}
			// mp.extDelCh <- eks[:1]
		}
	} else {
		// only the ek itself will be moved to level before
		// ino verseq be set with mp ver before submit in case other mp be updated while on flight, which will lead to
		// inconsistent between raft pairs
		delExtents, status = fsmIno.SplitExtentWithCheck(appendExtParam)
		log.LogInfof("action[fsmAppendExtentsWithCheck] mp[%v] DecSplitExts delExtents [%v]", mp.config.PartitionId, delExtents)
		fsmIno.DecSplitExts(mp.config.PartitionId, delExtents)
		delEkFlag = true
		mp.uidManager.minusUidSpace(fsmIno.Uid, fsmIno.Inode, delExtents)
	}

	// conflict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		// mp.extDelCh <- eks[:1]
		for _, ek := range eks[:1] {
			dek := NewDeletedExtentKey(&ek, fsmIno.Inode, mp.AllocDeletedExtentId())
			err = mp.deletedExtentsTree.Put(dbHandle, dek)
			if err != nil {
				status = proto.OpErr
				return
			}
		}
		mp.uidManager.minusUidSpace(fsmIno.Uid, fsmIno.Inode, eks[:1])
		log.LogDebugf("fsmAppendExtentsWithCheck mp[%v] delExtents inode[%v] ek(%v)", mp.config.PartitionId, fsmIno.Inode, delExtents)
	}

	mp.updateUsedInfo(int64(fsmIno.Size)-oldSize, 0, fsmIno.Inode)
	log.LogInfof("fsmAppendExtentWithCheck mp[%v] inode[%v] ek(%v) deleteExtents(%v) discardExtents(%v) status(%v)",
		mp.config.PartitionId, fsmIno.Inode, eks[0], delExtents, discardExtentKey, status)

	if err = mp.inodeTree.Update(dbHandle, fsmIno); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, fsmIno.Inode, delExtents, err)
		return
	}

	if delEkFlag {
		for _, ek := range delExtents {
			dek := NewDeletedExtentKey(&ek, ino.Inode, mp.AllocDeletedExtentId())
			err = mp.deletedExtentsTree.Put(dbHandle, dek)
			if err != nil {
				status = proto.OpErr
				return
			}
		}
		// mp.extDelCh <- delExtents
	}
	return
}

func (mp *metaPartition) fsmAppendObjExtents(dbHandle interface{}, ino *Inode) (status uint8, err error) {
	var inode *Inode
	status = proto.OpOk
	inode, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}

	if inode.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	eks := ino.ObjExtents.CopyExtents()
	err = inode.AppendObjExtents(eks, ino.ModifyTime)

	// if err is not nil, means obj eks exist overlap.
	if err != nil {
		log.LogErrorf("fsmAppendExtents inode[%v] err(%v)", inode.Inode, err)
		status = proto.OpConflictExtentsErr
		return
	}
	return
}

func (mp *metaPartition) fsmExtentsTruncate(dbHandle interface{}, ino *Inode) (resp *InodeResponse, err error) {
	var i *Inode
	resp = NewInodeResponse()
	log.LogDebugf("[fsmExtentsTruncate] req ino[%v]", ino)
	resp.Status = proto.OpOk
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil || i.ShouldDelete() {
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
	// i.Lock()
	// defer i.Unlock()

	if err = i.CreateLowerVersion(i.getVer(), mp.multiVersionList); err != nil {
		return
	}
	oldSize := int64(i.Size)
	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime, doOnLastKey, insertSplitKey)

	if len(delExtents) == 0 {
		// goto submit
		err = mp.inodeTree.Put(dbHandle, i)
		if err != nil {
			log.LogErrorf("[fsmExtentsTruncate] failed to update inode(%v)", i)
			return
		}
		return
	}

	if delExtents, err = i.RestoreExts2NextLayer(mp.config.PartitionId, delExtents, mp.verSeq, 0); err != nil {
		panic("[RestoreExts2NextLayer] should not be error")
	}
	mp.updateUsedInfo(int64(i.Size)-oldSize, 0, i.Inode)
	// now we should delete the extent
	log.LogInfof("[fsmExtentsTruncate] mp (%v) inode[%v] DecSplitExts exts(%v)", mp.config.PartitionId, i.Inode, delExtents)
	i.DecSplitExts(mp.config.PartitionId, delExtents)
	log.LogDebugf("[fsmExtentsTruncate] mp (%v) inode(%v), put inode", mp.config.PartitionId, i.Inode)
	// submit:
	if err = mp.inodeTree.Put(dbHandle, i); err != nil {
		resp.Status = proto.OpErr
		log.LogErrorf("[fsmExtentsTruncate] fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, i.Inode, delExtents, err)
		return
	}

	log.LogInfof("[fsmExtentsTruncate] truncate ino(%v), delete extent(%v)", i.Inode, delExtents)
	for _, ek := range delExtents {
		dek := NewDeletedExtentKey(&ek, i.Inode, mp.AllocDeletedExtentId())
		err = mp.deletedExtentsTree.Put(dbHandle, dek)
		if err != nil {
			resp.Status = proto.OpErr
			return
		}
	}
	// mp.extDelCh <- delExtents
	mp.uidManager.minusUidSpace(i.Uid, i.Inode, delExtents)
	log.LogInfof("[fsmExtentsTruncate] truncate ino(%v) success", i.Inode)
	return
}

func (mp *metaPartition) fsmEvictInode(dbHandle interface{}, ino *Inode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	var i *Inode
	log.LogDebugf("action[fsmEvictInode] inode[%v]", ino)
	resp.Status = proto.OpOk
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil || i.ShouldDelete() {
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
			delExtents := i.ClearAllExtsOfflineInode(mp.config.PartitionId)
			for _, eks := range delExtents {
				for _, ek := range eks {
					dek := NewDeletedExtentKey(ek, i.Inode, mp.AllocDeletedExtentId())
					err = mp.deletedExtentsTree.Put(dbHandle, dek)
					if err != nil {
						return
					}
				}
			}
			// mp.freeList.Push(i.Inode)
		}
	}
	mp.inodeTree.Put(dbHandle, i)
	return
}

func (mp *metaPartition) fsmBatchEvictInode(dbHandle interface{}, ib InodeBatch) (resp []*InodeResponse, err error) {
	defer func() {
		if err != nil {
			for index := 0; index < len(ib); index++ {
				resp = append(resp, &InodeResponse{Status: proto.OpErr})
			}
		}
	}()
	for _, ino := range ib {
		var status uint8
		status, err = mp.inodeInTx(ino.Inode)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = append(resp, &InodeResponse{Status: status})
			//todo: return--> continue
			continue
		}
		var rsp *InodeResponse
		rsp, err = mp.fsmEvictInode(dbHandle, ino)
		if err == ErrRocksdbOperation {
			resp = resp[:0]
			return
		}
		resp = append(resp, rsp)
	}
	return
}

// func (mp *metaPartition) checkAndInsertFreeList(ino *Inode) {
// 	if proto.IsDir(ino.Type) {
// 		return
// 	}
// 	if ino.ShouldDelete() {
// 		mp.freeList.Push(ino.Inode)
// 	} else if ino.IsTempFile() {
// 		ino.AccessTime = time.Now().Unix()
// 		mp.freeList.Push(ino.Inode)
// 	}
// }

func (mp *metaPartition) fsmSetAttr(dbHandle interface{}, req *SetattrRequest) (resp *InodeResponse, err error) {
	log.LogDebugf("action[fsmSetAttr] req %v", req)
	var ino *Inode
	ino, err = mp.inodeTree.Get(req.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if ino == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	if ino.ShouldDelete() {
		return
	}
	log.LogDebugf("[fsmSetAttr] set attr for ino(%v)", ino.Inode)
	ino.SetAttr(req)
	if err = mp.inodeTree.Update(dbHandle, ino); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmExtentsEmpty(dbHandle interface{}, ino *Inode) (status uint8, err error) {
	var i *Inode
	status = proto.OpOk
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}
	if i == nil {
		status = proto.OpNotExistErr
		return
	}
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

	i.EmptyExtents(ino.ModifyTime)
	if err = mp.inodeTree.Put(dbHandle, i); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, i.Inode, tinyEks, err)
		return
	}

	if len(tinyEks) > 0 {
		for _, ek := range tinyEks {
			dek := NewDeletedExtentKey(&ek, i.Inode, mp.AllocDeletedExtentId())
			err = mp.deletedExtentsTree.Put(dbHandle, dek)
			if err != nil {
				status = proto.OpErr
				return
			}
		}
		// mp.extDelCh <- tinyEks
		mp.uidManager.minusUidSpace(i.Uid, i.Inode, tinyEks)
		log.LogDebugf("fsmExtentsEmpty mp[%v] inode[%d] tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmDelVerExtents(dbHandle interface{}, ino *Inode) (status uint8, err error) {
	var i *Inode
	status = proto.OpOk
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}
	if i == nil {
		status = proto.OpNotExistErr
		return
	}
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
	i.EmptyExtents(ino.ModifyTime)

	if err = mp.inodeTree.Put(dbHandle, i); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, i.Inode, tinyEks, err)
		return
	}

	if len(tinyEks) > 0 {
		for _, ek := range tinyEks {
			dek := NewDeletedExtentKey(&ek, ino.Inode, mp.AllocDeletedExtentId())
			err = mp.deletedExtentsTree.Put(dbHandle, dek)
			if err != nil {
				status = proto.OpErr
				return
			}
		}
		// mp.extDelCh <- tinyEks
		log.LogDebugf("fsmExtentsEmpty mp[%v] inode[%d] tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	return
}

func (mp *metaPartition) fsmClearInodeCache(dbHandle interface{}, ino *Inode) (status uint8, err error) {
	status = proto.OpOk
	var ino2 *Inode
	ino2, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}
	if ino2 == nil {
		status = proto.OpNotExistErr
		return
	}

	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	delExtents := ino2.EmptyExtents(ino.ModifyTime)
	log.LogInfof("fsmClearInodeCache.mp[%v] inode[%v] DecSplitExts delExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)

	if err = mp.inodeTree.Put(dbHandle, ino2); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, ino2.Inode, delExtents, err)
		return
	}

	if len(delExtents) > 0 {
		ino2.DecSplitExts(mp.config.PartitionId, delExtents)
		for _, ek := range delExtents {
			dek := NewDeletedExtentKey(&ek, ino2.Inode, mp.AllocDeletedExtentId())
			err = mp.deletedExtentsTree.Put(dbHandle, dek)
			if err != nil {
				status = proto.OpErr
				return
			}
		}
		// mp.extDelCh <- delExtents
	}
	return
}

// attion: unmarshal error will disard extent
func (mp *metaPartition) fsmSendToChan(dbHandle interface{}, val []byte, v3 bool) (status uint8, err error) {
	sortExtents := NewSortedExtents()
	// ek for del don't need version info
	err, _ = sortExtents.UnmarshalBinary(val, v3)
	if err != nil {
		panic(fmt.Errorf("[fsmDelExtents] unmarshal sortExtents error, mp[%v], err(%s)", mp.config.PartitionId, err.Error()))
	}

	log.LogInfof("fsmDelExtents mp[%v] delExtents(%v)", mp.config.PartitionId, len(sortExtents.eks))
	// NOTE: only replay old version rpc
	// will enter here, we never invoke the rpc in
	// new version
	for _, ek := range sortExtents.eks {
		dek := NewDeletedExtentKey(&ek, 0, mp.AllocDeletedExtentId())
		err = mp.deletedExtentsTree.Put(dbHandle, dek)
		if err != nil {
			return
		}
	}
	// mp.extDelCh <- sortExtents.eks
	return
}

func (mp *metaPartition) fsmSetInodeQuotaBatch(dbHandle interface{}, req *proto.BatchSetMetaserverQuotaReuqest) (resp *proto.BatchSetMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchSetMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8, 0)
	for _, ino := range req.Inodes {
		var isExist bool
		var err error

		extend := NewExtend(ino)
		treeItem, _ := mp.extendTree.Get(ino)
		inode := NewInode(ino, 0)
		retMsg := mp.getInode(inode, false)

		if retMsg.Status != proto.OpOk {
			log.LogErrorf("fsmSetInodeQuotaBatch get inode[%v] fail.", ino)
			resp.InodeRes[ino] = retMsg.Status
			continue
		}
		inode = retMsg.Msg
		log.LogDebugf("fsmSetInodeQuotaBatch msg [%v] inode[%v]", retMsg, inode)
		quotaInfos := &proto.MetaQuotaInfos{
			QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
		}
		quotaInfo := &proto.MetaQuotaInfo{
			RootInode: req.IsRoot,
		}

		if treeItem == nil {
			quotaInfos.QuotaInfoMap[req.QuotaId] = quotaInfo
			mp.extendTree.Create(dbHandle, extend, true)
		} else {
			extend = treeItem
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
		mp.extendTree.Put(dbHandle, extend)
	}
	mp.mqMgr.updateUsedInfo(bytes, files, req.QuotaId)
	log.LogInfof("fsmSetInodeQuotaBatch quotaId [%v] resp [%v] success.", req.QuotaId, resp)
	return
}

func (mp *metaPartition) fsmDeleteInodeQuotaBatch(dbHandle interface{}, req *proto.BatchDeleteMetaserverQuotaReuqest) (resp *proto.BatchDeleteMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchDeleteMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8, 0)

	for _, ino := range req.Inodes {
		var err error
		var extend = NewExtend(ino)
		treeItem, _ := mp.extendTree.Get(ino)
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
			extend = treeItem
			value, exist := extend.Get([]byte(proto.QuotaKey))
			if exist {
				if err = json.Unmarshal(value, &quotaInfos.QuotaInfoMap); err != nil {
					log.LogErrorf("fsmDeleteInodeQuotaBatch ino[%v] Unmarshal quotaInfos fail [%v]", ino, err)
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
					log.LogDebugf("fsmDeleteInodeQuotaBatch QuotaInfoMap can not find inode[%v] quota [%v]", ino, req.QuotaId)
					resp.InodeRes[ino] = proto.OpOk
					continue
				}
			} else {
				resp.InodeRes[ino] = proto.OpOk
				continue
			}
			mp.extendTree.Put(dbHandle, extend)
		}
		files -= 1
		bytes -= int64(inode.Size)
	}
	mp.mqMgr.updateUsedInfo(bytes, files, req.QuotaId)
	log.LogInfof("fsmDeleteInodeQuotaBatch quotaId [%v] resp [%v] success.", req.QuotaId, resp)
	return
}
