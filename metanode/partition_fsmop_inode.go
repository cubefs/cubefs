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
	if _, _, err = mp.inodeTree.ReplaceOrInsert(dbHandle, ino, false); err != nil {
		status = proto.OpErr
		return
	}

	return
}

func (mp *metaPartition) fsmTxCreateLinkInode(dbHandle interface{}, txIno *TxInode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	done, err := mp.txProcessor.txManager.txInRMDone(txIno.TxInfo.TxID)
	if err != nil {
		log.LogErrorf("[fsmTxCreateLinkInode] txInRMDone(%v), err(%v)", txIno.TxInfo.TxID, err)
		resp.Status = proto.OpErr
		return
	}
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
	i, err = mp.inodeTree.CopyGet(ino)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = i
	if !mp.uniqChecker.legalIn(uniqID, mp.applyID) {
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
	item, err := mp.inodeTree.Get(ino)
	if err != nil {
		log.LogErrorf("getInodeByVer inode(%v) err: %s", ino.Inode, err.Error())
		return nil
	}
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

	i, err := mp.inodeTree.Get(ino)
	if err != nil {
		log.LogErrorf("[getInodeTopLayer] failed to get inode(%v), err(%v)", ino, err)
		resp.Status = proto.OpErr
		return
	}
	if i == nil {
		resp.Status = proto.OpNotExistErr
		log.LogDebugf("action[getInodeTopLayer] not found ino[%v] verseq [%v]", ino.Inode, ino.getVer())
		return
	}

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

func (mp *metaPartition) getInodeSimpleInfo(ino *Inode) (status uint8) {
	status = proto.OpOk
	i := mp.getInodeByVer(ino)
	if i == nil || i.ShouldDelete() {
		log.LogDebugf("action[getInode] ino  %v not found", ino)
		status = proto.OpNotExistErr
		return
	}
	ino.Size = i.Size
	ino.NLink = i.NLink
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
	item, err := mp.inodeTree.Get(ino)
	if err != nil {
		log.LogErrorf("[hasInode] failed to get inode(%v), err(%v)", ino.Inode, err)
		return
	}
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
		log.LogErrorf("fsmTxUnlinkInode txInRMDone(%v), err(%v)", txIno.TxInfo.TxID, err)
		resp.Status = proto.OpErr
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
		log.LogErrorf("fsmTxUnlinkInode addTxRollbackInode(%v), err(%v)", txIno, err)
		resp.Status = proto.OpErr
		return
	}
	if resp.Status == proto.OpExistErr {
		resp.Status = proto.OpOk
		var item *Inode
		item, err = mp.inodeTree.Get(txIno.Inode)
		if err != nil {
			resp.Status = proto.OpErr
			log.LogErrorf("get inode(%d) err: %s", txIno.Inode.Inode, err.Error())
			return
		}
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
			_, err1 := mp.txProcessor.txResource.deleteTxRollbackInode(dbHandle, txIno.Inode.Inode, txIno.TxInfo.TxID)
			if err1 != nil {
				log.LogErrorf("[fsmTxUnlinkInode] failed to delete rb inode(%v), err(%v)", txIno, err1)
			}
		}
	}()

	item, err := mp.inodeTree.Get(txIno.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		log.LogErrorf("get txIno inode(%d) err: %s", txIno.Inode.Inode, err.Error())
		return
	}
	if item == nil || item.IsTempFile() {
		resp.Status = proto.OpNotExistErr
		log.LogWarnf("fsmTxUnlinkInode: inode may be already not exist or link 0, txInode %v, item %v", txIno, item)
		return
	}

	resp, err = mp.fsmUnlinkInode(dbHandle, txIno.Inode, 0)
	if err != nil {
		log.LogErrorf("fsmTxUnlinkInode: failed to unlink inode(%v), err(%v)", txIno.Inode, err)
		resp.Status = proto.OpErr
		return
	}
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

func (mp *metaPartition) fsmUnlinkInode(dbHandle interface{}, ino *Inode, uniqID uint64) (resp *InodeResponse, err error) {
	log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, ino)
	var ext2Del []proto.ExtentKey

	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	inode, err := mp.inodeTree.CopyGet(ino)
	if err != nil {
		log.LogErrorf("Failed to get inode(%d) err: %s", ino.Inode, err.Error())
		resp.Status = proto.OpErr
		return
	}
	if inode == nil {
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, ino)
		resp.Status = proto.OpNotExistErr
		return
	}

	if ino.getVer() == 0 && inode.ShouldDelete() {
		log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v]", mp.config.PartitionId, ino)
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = inode
	if !mp.uniqChecker.legalIn(uniqID, mp.applyID) {
		log.LogWarnf("fsmUnlinkInode repeat, mp[%v] ino[%v] uniqID %v nlink %v", mp.config.PartitionId, ino.Inode, uniqID, ino.GetNLink())
		return
	}

	log.LogDebugf("action[fsmUnlinkInode] mp[%v] get inode[%v]", mp.config.PartitionId, inode)
	var (
		doMore bool
		status uint8
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

	if err = mp.inodeTree.Update(dbHandle, inode); err != nil {
		resp.Status = proto.OpErr
		return
	}

	if !doMore {
		resp.Status = status
		return
	}

	if inode.IsEmptyDirAndNoSnapshot() {
		if inode.NLink < 2 { // snapshot deletion
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] ino[%v] really be deleted, empty dir", mp.config.PartitionId, inode)
			_, err = mp.inodeTree.Delete(dbHandle, inode)
			if err != nil {
				resp.Status = proto.OpErr
				return
			}
		}
	} else if inode.IsTempFile() {
		// all snapshot between create to last deletion cleaned
		if inode.NLink == 0 && inode.getLayerLen() == 0 {
			log.LogDebugf("action[fsmUnlinkInode] mp[%v] unlink inode[%v] and push to freeList", mp.config.PartitionId, inode)
			inode.AccessTime = time.Now().Unix()
			mp.freeList.Push(inode.Inode)
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
func (mp *metaPartition) fsmUnlinkInodeBatch(dbHandle interface{}, ib InodeBatch) (resp []*InodeResponse, err error) {
	rsp := &InodeResponse{Status: proto.OpOk}
	defer func() {
		if rsp.Status != proto.OpOk {
			for index := 0; index < len(ib); index++ {
				resp = append(resp, &InodeResponse{Status: proto.OpErr, Msg: ib[index]})
			}
		}
	}()
	resp = make([]*InodeResponse, 0)
	inodeUnlinkNumMap := make(map[uint64]int, len(ib))
	for _, inode := range ib {
		if _, ok := inodeUnlinkNumMap[inode.Inode]; !ok {
			inodeUnlinkNumMap[inode.Inode] = 1
			continue
		}
		inodeUnlinkNumMap[inode.Inode]++
	}

	for inodeID := range inodeUnlinkNumMap {
		var status uint8
		status, err = mp.inodeInTx(inodeID)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = append(resp, &InodeResponse{Status: status})
			continue
		}

		// todo inode info miss
		log.LogDebugf("[fsmUnlinkInodeBatch] unlink inode(%v)", inodeID)
		rsp, err = mp.fsmUnlinkInode(dbHandle, NewInode(inodeID, 0), 0)
		if rsp.Status != proto.OpOk {
			resp = resp[:0]
			return
		}
		resp = append(resp, rsp)
	}
	return
}

func (mp *metaPartition) internalHasInode(ino *Inode) bool {
	item, err := mp.inodeTree.Get(ino)
	if err != nil {
		return false
	}
	return item != nil
}

func (mp *metaPartition) internalDelete(val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	var handle interface{}
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("internalDelete: create batch write handle err(%v)", err)
		return err
	}
	defer func() {
		_ = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	}()
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
		mp.internalDeleteInode(handle, ino)
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

func (mp *metaPartition) internalDeleteInode(dbHandle interface{}, ino *Inode) {
	log.LogDebugf("action[internalDeleteInode] vol(%v) mp(%v) ino[%v] really be deleted", mp.config.VolName, mp.config.PartitionId, ino)
	mp.inodeTree.Delete(dbHandle, ino)
	mp.freeList.Remove(ino.Inode)
	mp.extendTree.Delete(dbHandle, &Extend{inode: ino.Inode}) // Also delete extend attribute.
}

func (mp *metaPartition) fsmAppendExtents(dbHandle interface{}, ino *Inode) (status uint8, err error) {
	status = proto.OpOk
	var ino2 *Inode
	ino2, err = mp.inodeTree.CopyGet(ino)
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

	if ino.HybridCloudExtents.Empty() {
		log.LogWarnf("fsmAppendExtents: extents is empty, %v", ino.Inode)
		return
	}

	eks := ino.HybridCloudExtents.sortedEks.(*SortedExtents).CopyExtents()
	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks); status != proto.OpOk {
		return
	}
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime, mp.volType)
	log.LogInfof("fsmAppendExtents mpId[%v].inode[%v] DecSplitExts deleteExtents(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	ino2.DecSplitExts(mp.config.PartitionId, delExtents)

	if err = mp.inodeTree.Put(dbHandle, ino2); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, ino2.Inode, delExtents, err)
		return
	}

	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(dbHandle interface{}, inoParam *Inode, isSplit bool) (status uint8, err error) {
	var (
		delExtents       []proto.ExtentKey
		discardExtentKey []proto.ExtentKey
		fsmIno           *Inode
	)

	if mp.verSeq < inoParam.getVer() {
		status = proto.OpArgMismatchErr
		log.LogErrorf("fsmAppendExtentsWithCheck.mp[%v] param ino[%v] mp seq [%v]", mp.config.PartitionId, inoParam, mp.verSeq)
		return
	}
	status = proto.OpOk
	fsmIno, err = mp.inodeTree.CopyGet(inoParam)
	if err != nil {
		status = proto.OpErr
		log.LogErrorf("fsmAppendExtentsWithCheck inode(%d) rocksdb op error", inoParam.Inode)
		return
	}

	if fsmIno == nil || fsmIno.ShouldDelete() {
		log.LogInfof("fsmAppendExtentsWithCheck: inode already not exist, mp %d, ino %d", mp.config.PartitionId, inoParam.Inode)
		status = proto.OpNotExistErr
		return
	}

	// get eks from inoParm, so do not need transform from HybridCloudExtents
	var (
		eks         []proto.ExtentKey
		isMigration bool
	)
	storageClass := inoParam.StorageClass
	poolId := inoParam.PoolId
	if inoParam.HybridCloudExtents.sortedEks != nil && len(inoParam.HybridCloudExtents.sortedEks.(*SortedExtents).eks) != 0 {
		eks = inoParam.HybridCloudExtents.sortedEks.(*SortedExtents).CopyExtents()
	} else if inoParam.HybridCloudExtentsMigration.sortedEks != nil && len(inoParam.HybridCloudExtentsMigration.sortedEks.(*SortedExtents).eks) != 0 {
		isMigration = true
		storageClass = inoParam.HybridCloudExtentsMigration.storageClass
		poolId = inoParam.HybridCloudExtentsMigration.poolId
		eks = inoParam.HybridCloudExtentsMigration.sortedEks.(*SortedExtents).CopyExtents()
	}

	if isMigration && fsmIno.NeedDeleteMigrationExtentKey() {
		log.LogWarnf("fsmAppendExtentsWithCheck: inode(%v) need delete migration extent key, can't append migration extent", inoParam.Inode)
		status = proto.OpMismatchStorageClass
		return
	}

	if err = fsmIno.updateStorageClass(storageClass, poolId, isMigration); err != nil {
		log.LogErrorf("action[fsmAppendExtentsWithCheck] updateStorageClass inode(%v) isMigration(%v), failed: %v",
			inoParam.Inode, isMigration, err.Error())
		status = proto.OpMismatchStorageClass
		return
	}

	log.LogDebugf("action[fsmAppendExtentsWithCheck] inode %v hist len %v,eks %v, isMigration %v",
		fsmIno.Inode, fsmIno.getLayerLen(), eks, isMigration)
	if len(eks) < 1 {
		log.LogWarnf("fsmAppendExtentsWithCheck: recive eks less than 1, may be wrong, mp %d, ino %d",
			mp.config.PartitionId, inoParam.Inode)
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
		ct:               inoParam.ModifyTime,
		discardExtents:   discardExtentKey,
		volType:          mp.volType,
		multiVersionList: mp.multiVersionList,
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
			log.LogWarnf("action[fsmAppendExtentsWithCheck] mp[%v] OpConflictExtentsErr [%v]", mp.config.PartitionId, eks[:1])
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
	}

	// conflict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		mp.extDelCh <- eks[:1]
		log.LogWarnf("fsmAppendExtentsWithCheck mp[%v] delExtents inode[%v] ek(%v)", mp.config.PartitionId, fsmIno.Inode, delExtents)
	}

	log.LogInfof("fsmAppendExtentsWithCheck mp[%v] inode[%v] ek(%v) deleteExtents(%v) discardExtents(%v) status(%v), gen %d",
		mp.config.PartitionId, fsmIno.Inode, eks[0], delExtents, discardExtentKey, status, fsmIno.Generation)

	if err = mp.inodeTree.Update(dbHandle, fsmIno); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, fsmIno.Inode, delExtents, err)
		return
	}

	return
}

func (mp *metaPartition) fsmAppendObjExtents(dbHandle interface{}, inoParam *Inode) (status uint8, err error) {
	var inode *Inode
	status = proto.OpOk
	inode, err = mp.inodeTree.CopyGet(inoParam)
	if err != nil {
		status = proto.OpErr
		return
	}
	if inode == nil {
		status = proto.OpNotExistErr
		return
	}

	if inode.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	if inoParam.HybridCloudExtents.Empty() {
		log.LogWarnf("fsmAppendObjExtents: objexts is empty %d", inoParam.Inode)
		return
	}

	if err = inode.updateStorageClass(inoParam.StorageClass, inoParam.PoolId, false); err != nil {
		log.LogErrorf("fsmAppendObjExtents: storage class not equal, new %d now %d, ino %d", inoParam.StorageClass, inode.StorageClass, inode.Inode)
		status = proto.OpMismatchStorageClass
		return
	}
	// eks := ino.ObjExtents.CopyExtents()
	eks := inoParam.HybridCloudExtents.sortedEks.(*SortedObjExtents).CopyExtents()
	err = inode.AppendObjExtents(eks, inoParam.ModifyTime)
	// if err is not nil, means obj eks exist overlap.
	if err != nil {
		log.LogErrorf("fsmAppendExtents inode[%v] err(%v)", inode.Inode, err)
		status = proto.OpConflictExtentsErr
		return
	}

	if err = mp.inodeTree.Put(dbHandle, inode); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsmAppendObjExtents mp(%v) inode(%v) Put error: %v", mp.config.PartitionId, inode.Inode, err)
		return
	}

	return
}

func (mp *metaPartition) fsmExtentsTruncate(dbHandle interface{}, ino *Inode) (resp *InodeResponse, err error) {
	var i *Inode
	resp = NewInodeResponse()
	log.LogDebugf("fsmExtentsTruncate. req ino[%v] mpId(%v)", ino, mp.config.PartitionId)
	resp.Status = proto.OpOk
	i, err = mp.inodeTree.Get(ino)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}

	if i == nil || i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
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

	insertSplitKey := func(ek *proto.ExtentKey) {
		i.insertEkRefMap(mp.config.PartitionId, ek)
	}

	if i.getVer() != mp.verSeq {
		i.CreateVer(mp.verSeq)
	}

	if err = i.CreateLowerVersion(i.getVer(), mp.multiVersionList); err != nil {
		return
	}

	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime, insertSplitKey)
	if len(delExtents) == 0 {
		// goto submit
		err = mp.inodeTree.Put(dbHandle, i)
		if err != nil {
			log.LogErrorf("[fsmExtentsTruncate] failed to update inode(%v)", i)
			resp.Status = proto.OpErr
			return
		}
		return
	}

	if delExtents, err = i.RestoreExts2NextLayer(mp.config.PartitionId, delExtents, mp.verSeq, 0); err != nil {
		panic("RestoreExts2NextLayer should not be error")
	}
	// now we should delete the extent
	log.LogInfof("fsmExtentsTruncate.mp (%v) inode[%v] DecSplitExts exts(%v)", mp.config.PartitionId, i.Inode, delExtents)
	i.DecSplitExts(mp.config.PartitionId, delExtents)
	log.LogDebugf("[fsmExtentsTruncate] mp (%v) inode(%v), put inode", mp.config.PartitionId, i.Inode)
	// submit:
	if err = mp.inodeTree.Put(dbHandle, i); err != nil {
		resp.Status = proto.OpErr
		log.LogErrorf("[fsmExtentsTruncate] fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, i.Inode, delExtents, err)
		return
	}

	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmEvictInode(dbHandle interface{}, ino *Inode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	log.LogDebugf("action[fsmEvictInode] inode[%v]", ino)
	resp.Status = proto.OpOk
	var i *Inode
	i, err = mp.inodeTree.CopyGet(ino)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil || i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		log.LogDebugf("action[fsmEvictInode] inode[%v] already be mark delete", ino)
		return
	}
	needUpdate := false
	if proto.IsDir(i.Type) {
		if i.IsEmptyDirAndNoSnapshot() {
			i.SetDeleteMark()
			needUpdate = true
		}
	} else if i.IsTempFile() {
		log.LogDebugf("action[fsmEvictInode] inode[%v] already linke zero and be set mark delete and be put to freelist", ino)
		if i.isEmptyVerList() {
			i.SetDeleteMark()
			needUpdate = true
			mp.freeList.Push(i.Inode)
		}
	}
	if needUpdate {
		err = mp.inodeTree.Update(dbHandle, i)
		if err != nil {
			log.LogErrorf("[fsmEvictInode] mp(%v) failed to evict inode(%v), err(%v)", mp.config.PartitionId, i, err)
			resp.Status = proto.OpErr
			return
		}
	}

	return
}

func (mp *metaPartition) fsmBatchEvictInode(dbHandle interface{}, ib InodeBatch) (resp []*InodeResponse, err error) {
	for _, ino := range ib {
		var status uint8
		status, err = mp.inodeInTx(ino.Inode)
		if err != nil {
			status = proto.OpErr
		}
		if status != proto.OpOk {
			resp = append(resp, &InodeResponse{Status: status})
			continue
		}
		var rsp *InodeResponse
		rsp, err = mp.fsmEvictInode(dbHandle, ino)
		if err == ErrRocksdbOperation {
			resp = resp[:0]
			goto err
		}
		resp = append(resp, rsp)
	}
	return

err:
	for index := 0; index < len(ib); index++ {
		resp = append(resp, &InodeResponse{Status: proto.OpErr})
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

func (mp *metaPartition) fsmSetAttr(handle interface{}, req *SetattrRequest) (err error) {
	log.LogDebugf("action[fsmSetAttr] req %v", req)
	ino := NewInode(req.Inode, req.Mode)
	ino, err = mp.inodeTree.CopyGet(ino)
	if err != nil {
		log.LogErrorf("inode(%d) mode(%x) rockdsdb err: %s", req.Inode, req.Mode, err.Error())
		return
	}
	if ino == nil || ino.ShouldDelete() {
		return
	}
	ino.SetAttr(req)
	if err = mp.inodeTree.Update(handle, ino); err != nil {
		return
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

func (mp *metaPartition) fsmSetInodeQuotaBatch(handle interface{}, req *proto.BatchSetMetaserverQuotaReuqest) (resp *proto.BatchSetMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchSetMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8)
	for _, ino := range req.Inodes {
		var isExist bool
		var err error

		extend := NewExtendWithQuota(ino)
		treeItem, err := mp.extendTree.Get(extend)
		if err != nil {
			resp.InodeRes[ino] = proto.OpErr
			log.LogErrorf("fsmSetInodeQuotaBatch get inode[%v] fail.", ino)
			continue
		}
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
			mp.extendTree.ReplaceOrInsert(handle, extend, true)
		} else {
			extend = treeItem
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
		mp.extendTree.Put(handle, extend)
	}
	return
}

func (mp *metaPartition) fsmDeleteInodeQuotaBatch(handle interface{}, req *proto.BatchDeleteMetaserverQuotaReuqest) (resp *proto.BatchDeleteMetaserverQuotaResponse) {
	var files int64
	var bytes int64
	resp = &proto.BatchDeleteMetaserverQuotaResponse{}
	resp.InodeRes = make(map[uint64]uint8)
	extend := NewExtendWithQuota(0)
	extTmp := extend
	inode := NewSimpleInode(0)
	for _, ino := range req.Inodes {
		var err error
		extend = extTmp
		extend.inode = ino
		treeItem, err := mp.extendTree.Get(extend)
		if err != nil {
			resp.InodeRes[ino] = proto.OpErr
			log.LogErrorf("fsmDeleteInodeQuotaBatch get inode[%v] fail.", ino)
			continue
		}
		inode.Inode = ino
		status := mp.getInodeSimpleInfo(inode)
		if status != proto.OpOk {
			log.LogErrorf("fsmDeleteInodeQuotaBatch get inode[%v] fail.", ino)
			resp.InodeRes[ino] = status
			continue
		}

		quotaInfos := &proto.MetaQuotaInfos{
			QuotaInfoMap: make(map[uint32]*proto.MetaQuotaInfo),
		}

		if treeItem == nil {
			log.LogDebugf("fsmDeleteInodeQuotaBatch inode[%v] not has extend ", ino)
			resp.InodeRes[ino] = proto.OpOk
			continue
		} else {
			extend = treeItem
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
			mp.extendTree.Put(handle, extend)
		}
		files -= 1
		bytes -= int64(inode.Size)
	}
	log.LogInfof("fsmDeleteInodeQuotaBatch quotaId [%v] resp [%v] success.", req.QuotaId, resp)
	return
}

func (mp *metaPartition) fsmSyncInodeAccessTime(handle interface{}, ino *Inode) (status uint8) {
	status = proto.OpOk
	i, err := mp.inodeTree.CopyGet(ino)
	if err != nil {
		status = proto.OpErr
		return
	}
	if i == nil {
		status = proto.OpNotExistErr
		return
	}

	i.AccessTime = ino.AccessTime
	err = mp.inodeTree.Update(handle, i)
	if err != nil {
		status = proto.OpErr
		log.LogErrorf("fsmSyncInodeAccessTime inode [%v] err: %s", i.Inode, err.Error())
		return
	}
	log.LogDebugf("fsmSyncInodeAccessTime inode [%v] AccessTime update to [%v] success.", i.Inode, ino.AccessTime)
	return
}

func (mp *metaPartition) fsmBatchSyncInodeAccessTime(handle interface{}, bufSlice []byte) (status uint8) {
	status = proto.OpOk
	start := time.Now()
	mpId := mp.config.PartitionId
	idx := 8

	atime := binary.BigEndian.Uint64(bufSlice[0:8])
	for ; idx+8 <= len(bufSlice); idx += 8 {
		ino := binary.BigEndian.Uint64(bufSlice[idx : idx+8])
		i, err := mp.inodeTree.CopyGet(NewInode(ino, 0))
		if err != nil {
			log.LogWarnf("fsmBatchSyncInodeAccessTime: mp(%d) inode %d err: %s", mpId, ino, err.Error())
			continue
		}
		if i == nil {
			log.LogWarnf("fsmBatchSyncInodeAccessTime: mp(%d) inode %d not found", mpId, ino)
			continue
		}

		i.AccessTime = int64(atime)
		err = mp.inodeTree.Update(handle, i)
		if err != nil {
			log.LogErrorf("fsmBatchSyncInodeAccessTime mp(%d) BatchUpdate err: %s", mpId, err.Error())
			continue
		}
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
	i, err := mp.inodeTree.CopyGet(ino)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	i.LeaseExpireTime = ino.LeaseExpireTime
	log.LogDebugf("action[fsmRenewalInodeForbiddenMigration] inode %v is renewal, expireTime %d", i.Inode, ino.LeaseExpireTime)
	return
}

func (mp *metaPartition) fsmUpdateExtentKeyAfterMigration(inoParam *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	i, err := mp.inodeTree.CopyGet(inoParam)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil {
		resp.Status = proto.OpNotExistErr
		log.LogWarnf("fsmUpdateExtentKeyAfterMigration: inode already been deleted %d", inoParam.Inode)
		return
	}

	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		log.LogWarnf("fsmUpdateExtentKeyAfterMigration: inode been deleted %d", inoParam.Inode)
		return
	}

	if i.LeaseExpireTime != inoParam.LeaseExpireTime {
		log.LogWarnf("fsmUpdateExtentKeyAfterMigration: inode is forbidden to migrate. gen %d, reqGen %d, ino %d",
			i.LeaseExpireTime, inoParam.LeaseExpireTime, i.Inode)
		resp.Status = proto.OpLeaseOccupiedByOthers
		return
	}

	// for empty file, HybridCloudExtents.sortedEks is nil and StorageClass_Unspecified
	// but HybridCloudExtentsMigration.sortedEks for inoParam is always not nil
	if i.EmptyHybridExtents() && i.StorageClass != proto.StorageClass_Unspecified && !inoParam.HybridCloudExtentsMigration.Empty() {
		log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) extent key is empty, but extent key "+
			"for migration storageClass(%v) is not empty",
			mp.config.PartitionId, i.Inode, i.StorageClass, i.HybridCloudExtentsMigration.storageClass)
		resp.Status = proto.OpNotPerm
		return
	}

	if !i.EmptyHybridExtents() && inoParam.HybridCloudExtentsMigration.Empty() {
		log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) migrate extent key for migration "+
			"storageClass(%v) is empty ",
			mp.config.PartitionId, i.Inode, i.StorageClass, i.HybridCloudExtentsMigration.storageClass)
		resp.Status = proto.OpNotPerm
		return
	}

	if (!i.EmptyHybridExtents() && i.HybridCloudExtentsMigration.Empty()) || (i.EmptyHybridExtents() && !i.HybridCloudExtentsMigration.Empty()) {
		log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) migrate extent key for migration "+
			"storageClass(%v) is empty, eks(%v), migrateEks(%v) ",
			mp.config.PartitionId, i.Inode, i.StorageClass, i.HybridCloudExtentsMigration.storageClass, i.EmptyHybridExtents(), i.HybridCloudExtentsMigration.Empty())
		resp.Status = proto.OpNotPerm
		return
	}

	// if StorageClass is the same, check if sortedEks is the same
	if i.PoolId == inoParam.HybridCloudExtentsMigration.poolId &&
		i.HybridCloudExtents.sortedEks != nil &&
		inoParam.HybridCloudExtentsMigration.sortedEks != nil {
		if proto.IsStorageClassReplica(i.StorageClass) {
			inoExtents := i.HybridCloudExtents.sortedEks.(*SortedExtents)
			mExtents := inoParam.HybridCloudExtentsMigration.sortedEks.(*SortedExtents)
			if inoExtents.Equals(mExtents) {
				log.LogInfof("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) and extents same with req",
					mp.config.PartitionId, i.Inode, i.StorageClass)
				return
			}
			log.LogWarnf("[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storageClass(%v) is already the same with req storageClass, but extents different",
				mp.config.PartitionId, i.Inode, i.StorageClass)
			resp.Status = proto.OpNotPerm
			return
		}

		if proto.IsStorageClassBlobStore(i.StorageClass) {
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
	i.HybridCloudExtentsMigration.poolId = inoParam.PoolId
	i.HybridCloudExtentsMigration.sortedEks = inoParam.HybridCloudExtents.sortedEks
	i.HybridCloudExtentsMigration.expiredTime = inoParam.HybridCloudExtentsMigration.expiredTime

	// store new storage ek  in HybridCloudExtents
	i.StorageClass = inoParam.HybridCloudExtentsMigration.storageClass
	i.PoolId = inoParam.HybridCloudExtentsMigration.poolId
	i.HybridCloudExtents.sortedEks = inoParam.HybridCloudExtentsMigration.sortedEks

	// delete migration ek in future
	i.Flag |= DeleteMigrationExtentKeyFlag
	log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] mp(%v) inode(%v) storage class change from %v to %v",
		mp.config.PartitionId, i.Inode, i.HybridCloudExtentsMigration.poolId, i.PoolId)

	if log.EnableInfo() {
		logCurrentExtentKeys(i.StorageClass, i.PoolId, i.HybridCloudExtents.sortedEks, i.Inode)
		logCurrentExtentKeys(i.HybridCloudExtentsMigration.storageClass, i.HybridCloudExtentsMigration.poolId, i.HybridCloudExtentsMigration.sortedEks, i.Inode)
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

func logCurrentExtentKeys(storageClass uint32, poolId uint8, sortedEks interface{}, inode uint64) {
	if !log.EnableInfo() {
		return
	}
	if sortedEks == nil {
		log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] inode(%v) poolId(%v) storageClass(%v) current ek empty",
			inode, poolId, storageClass)
	} else {
		if proto.IsStorageClassReplica(storageClass) {
			log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] inode(%v) poolId(%v) storageClass(%v) current ek %v",
				inode, poolId, storageClass, sortedEks.(*SortedExtents).eks)
		} else if proto.IsStorageClassBlobStore(storageClass) {
			log.LogInfof("action[fsmUpdateExtentKeyAfterMigration] inode(%v) poolId(%v) storageClass(%v) current ek %v",
				inode, poolId, storageClass, sortedEks.(*SortedObjExtents).eks)
		}
	}
}

func (mp *metaPartition) fsmSetCreateTime(handle interface{}, req *SetCreateTimeRequest) (err error) {
	log.LogDebugf("[fsmSetCreateTime] req %v", req)
	ino := NewInode(req.Inode, 0)
	item, err := mp.inodeTree.CopyGet(ino)
	if err != nil {
		log.LogErrorf("[fsmSetCreateTime] inode(%v) copyget err:%s", req.Inode, err.Error())
		return err
	}
	if item == nil {
		err = fmt.Errorf("[fsmSetCreateTime] inode(%v) not found", req.Inode)
		return
	}
	ino = item
	if ino.ShouldDelete() {
		return
	}
	ino.SetCreateTime(req)

	err = mp.inodeTree.Update(handle, ino)
	if err != nil {
		log.LogErrorf("[fsmSetCreateTime] inode(%v) update err:%s", req.Inode, err.Error())
		return
	}
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

		var ino *Inode
		ino, err = mp.inodeTree.CopyGet(inoParam)
		if err != nil {
			log.LogErrorf("[fsmInternalBatchFreeMigrationExtentKey] %v", err)
			return err
		}
		if ino == nil {
			err = fmt.Errorf("mpId(%v) inode(%v) not found",
				mp.config.PartitionId, inoParam.Inode)
			log.LogWarnf("[fsmInternalBatchFreeMigrationExtentKey] %v", err)
			continue
		}

		mp.internalDeleteInodeMigrationExtentKey(ino)
	}
}

func (mp *metaPartition) internalDeleteInodeMigrationExtentKey(ino *Inode) {
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_Unspecified
	ino.HybridCloudExtentsMigration.poolId = 0
	ino.HybridCloudExtentsMigration.expiredTime = 0
	ino.HybridCloudExtentsMigration.sortedEks = nil
	if ino.NeedDeleteMigrationExtentKey() {
		ino.Flag ^= DeleteMigrationExtentKeyFlag // reset DeleteMigrationExtentKeyFlag for future deletion of inode
	}

	mp.freeHybridList.Remove(ino.Inode)

	log.LogDebugf("[internalDeleteInodeMigrationExtentKey] partitionID(%v) inode(%v) poolId(%v) storageClass(%v)",
		mp.config.PartitionId, ino.Inode, ino.PoolId, proto.StorageClassString(ino.StorageClass))
}

func (mp *metaPartition) fsmSetMigrationExtentKeyDeleteImmediately(inoParam *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	i, err := mp.inodeTree.CopyGet(inoParam)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

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

func (mp *metaPartition) fsmUpdateInodeMeta(req *UpdateInodeMetaRequest) (err error) {
	log.LogDebugf("action[fsmUpdateInodeMeta] req %v", req)
	ino := NewInode(req.Inode, 0)
	i, err := mp.inodeTree.CopyGet(ino)
	if err != nil {
		return
	}
	if i == nil {
		err = fmt.Errorf("ino %v not exist", ino.Inode)
		return
	}
	if i.ShouldDelete() {
		err = fmt.Errorf("ino %v marked delete", ino.Inode)
		return
	}

	i.Lock()
	defer i.Unlock()
	i.Generation++
	i.ModifyTime = ino.ModifyTime
	return
}
