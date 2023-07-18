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
	log.LogDebugf("action[fsmCreateInode] inode  %v be created", ino.Inode)

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
	i.IncNLink()
	return
}

func (mp *metaPartition) getInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
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
	ctime := Now.GetCurrentTime().Unix()
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
		ok = false
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		ok = false
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

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInode(ino *Inode, uniqID uint64) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	inode := item.(*Inode)
	if inode.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = inode
	if !mp.uniqChecker.legalIn(uniqID) {
		log.LogWarnf("fsmUnlinkInode repeat, ino %v uniqID %v nlink %v", ino.Inode, uniqID, ino.GetNLink())
		return
	}

	if inode.IsEmptyDir() {
		mp.inodeTree.Delete(inode)
		mp.updateUsedInfo(0, -1, inode.Inode)
	}

	inode.DecNLink()

	//Fix#760: when nlink == 0, push into freeList and delay delete inode after 7 days
	if inode.IsTempFile() {
		mp.updateUsedInfo(-1*int64(inode.Size), -1, inode.Inode)
		inode.DoWriteFunc(func() {
			if inode.NLink == 0 {
				inode.AccessTime = time.Now().Unix()
				mp.freeList.Push(inode.Inode)
				mp.uidManager.doMinusUidSpace(inode.Uid, inode.Inode, inode.Size)
			}
		})
	}

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
	eks := ino.Extents.CopyExtents()
	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks); status != proto.OpOk {
		return
	}
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime, mp.volType)
	mp.updateUsedInfo(int64(ino2.Size)-oldSize, 0, ino2.Inode)
	log.LogInfof("fsmAppendExtents inode(%v) deleteExtents(%v)", ino2.Inode, delExtents)
	mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(ino *Inode) (status uint8) {
	status = proto.OpOk
	log.LogInfof("fsmAppendExtentsWithCheck ino %v", ino.Inode)
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
	var (
		discardExtentKey []proto.ExtentKey
	)
	oldSize := int64(ino2.Size)
	eks := ino.Extents.CopyExtents()
	if len(eks) < 1 {
		return
	}
	if len(eks) > 1 {
		discardExtentKey = eks[1:]
	}

	if status = mp.uidManager.addUidSpace(ino2.Uid, ino2.Inode, eks[:1]); status != proto.OpOk {
		log.LogErrorf("fsmAppendExtentsWithCheck.addUidSpace status %v", status)
		return
	}

	delExtents, status := ino2.AppendExtentWithCheck(eks[0], ino.ModifyTime, discardExtentKey, mp.volType)
	if status == proto.OpOk {
		mp.extDelCh <- delExtents
		mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, delExtents)
	}

	// conflict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		log.LogInfof("action[fsmAppendExtentsWithCheck] OpConflictExtentsErr [%v]", eks[:1])
		mp.extDelCh <- eks[:1]
		mp.uidManager.minusUidSpace(ino2.Uid, ino2.Inode, eks[:1])
	}

	// confict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		mp.extDelCh <- eks[:1]
	}

	mp.updateUsedInfo(int64(ino2.Size)-oldSize, 0, ino2.Inode)
	log.LogInfof("fsmAppendExtentWithCheck inode(%v) ek(%v) deleteExtents(%v) discardExtents(%v) status(%v)", ino2.Inode, eks[0], delExtents, discardExtentKey, status)
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

	eks := ino.ObjExtents.CopyExtents()
	err := inode.AppendObjExtents(eks, ino.ModifyTime)

	// if err is not nil, means obj eks exist overlap.
	if err != nil {
		log.LogErrorf("fsmAppendExtents inode(%v) err(%v)", inode.Inode, err)
		status = proto.OpConflictExtentsErr
	}
	return
}

// ino is not point to the member of inodeTree
// it's inode is same with inodeTree,not the extent
// func (mp *metaPartition) fsmDelExtents(ino *Inode) (status uint8) {
// 	status = proto.OpOk
// 	item := mp.inodeTree.CopyGet(ino)
// 	if item == nil {
// 		status = proto.OpNotExistErr
// 		return
// 	}
// 	ino2 := item.(*Inode)
// 	if ino2.ShouldDelete() {
// 		status = proto.OpNotExistErr
// 		return
// 	}
// 	eks := ino.Extents.CopyExtents()
// 	delExtents := ino2.ReplaceExtents(eks, ino.ModifyTime)
// 	log.LogInfof("fsmDelExtents inode(%v) curExtent(%v) delExtents(%v)", ino2.Inode, eks, delExtents)
// 	mp.extDelCh <- delExtents
// 	return
// }

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
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
	if proto.IsDir(i.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}

	doOnLastKey := func(lastKey *proto.ExtentKey) {
		var eks []proto.ExtentKey
		eks = append(eks, *lastKey)
		mp.uidManager.minusUidSpace(i.Uid, i.Inode, eks)
	}

	oldSize := int64(i.Size)
	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime, doOnLastKey)
	mp.updateUsedInfo(int64(i.Size)-oldSize, 0, i.Inode)
	// now we should delete the extent
	log.LogInfof("fsmExtentsTruncate inode(%v) exts(%v)", i.Inode, delExtents)
	mp.extDelCh <- delExtents
	mp.uidManager.minusUidSpace(i.Uid, i.Inode, delExtents)
	return
}

func (mp *metaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		return
	}
	if proto.IsDir(i.Type) {
		if i.IsEmptyDir() {
			i.SetDeleteMark()
		}
		return
	}

	if i.IsTempFile() {
		i.SetDeleteMark()
		mp.freeList.Push(i.Inode)
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
	log.LogInfof("fsmClearInodeCache inode(%v) delExtents(%v)", ino2.Inode, delExtents)
	if len(delExtents) > 0 {
		mp.extDelCh <- delExtents
	}
	return
}

// attion: unmarshal error will disard extent
func (mp *metaPartition) fsmSendToChan(val []byte) (status uint8) {
	sortExtents := NewSortedExtents()
	err := sortExtents.UnmarshalBinary(val)
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
		retMsg := mp.getInode(inode)

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
		extend.Put([]byte(proto.QuotaKey), value)
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
		retMsg := mp.getInode(inode)
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
						extend.Put([]byte(proto.QuotaKey), value)
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
