// Copyright 2018 The Chubao Authors.
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
	"fmt"
	"github.com/chubaofs/chubaofs/util/exporter"
	"io"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

type InodeResponse struct {
	Status uint8
	Msg    *Inode
}

func NewInodeResponse() *InodeResponse {
	return &InodeResponse{}
}

type CursorResetResponse struct {
	Status uint8
	Msg    string
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(dbHandle interface{}, ino *Inode) (status uint8, err error) {
	var ok bool
	status = proto.OpOk
	mp.setAllocatorIno(ino.Inode)
	if _, ok, err = mp.inodeTree.Create(dbHandle, ino, false); err != nil {
		status = proto.OpErr
		return
	}

	if !ok {
		msg := fmt.Sprintf("action[fsmCreateInode] clusterID[%s] volumeName[%s] partitionID[%v], create same inode(%v)",
			mp.manager.metaNode.clusterId, mp.config.VolName, mp.config.PartitionId, ino.Inode)
		exporter.WarningRocksdbError(msg)
		log.LogErrorf("fsmCreateInode error: inode(%v) already exist in mp(%v)", ino.Inode, mp.config.PartitionId)
		status = proto.OpExistErr
		return
	}
	return
}

func (mp *metaPartition) fsmCreateLinkInode(dbHandle interface{}, ino *Inode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if outOfRange, _ := mp.isInoOutOfRange(ino.Inode); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var existIno *Inode
	existIno, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if existIno == nil || existIno.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	existIno.IncNLink()
	if err = mp.inodeTree.Update(dbHandle, existIno); err != nil {
		resp.Status = proto.OpErr
		return
	}
	resp.Msg = existIno
	return
}

func (mp *metaPartition) getInode(ino *Inode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if isOutOfRange, _ := mp.isInoOutOfRange(ino.Inode); isOutOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var i *Inode
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		if err == rocksDBError {
			exporter.WarningRocksdbError(fmt.Sprintf("action[getInode] clusterID[%s] volumeName[%s] partitionID[%v]"+
				" get inode failed witch rocksdb error", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId))
		}
		resp.Status = proto.OpErr
		return
	}

	if i == nil || i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	i.AccessTime = Now.GetCurrentTime().Unix()
	resp.Msg = i
	return
}

func (mp *metaPartition) hasInode(ino *Inode) (ok bool, inode *Inode) {
	var err error
	inode, err = mp.inodeTree.Get(ino.Inode)
	if err == rocksDBError {
		exporter.WarningRocksdbError(fmt.Sprintf("action[Has] clusterID[%s] volumeName[%s] partitionID[%v]"+
			" get inode failed witch rocksdb error[inode:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
			mp.config.PartitionId, ino.Inode))
	}

	if inode == nil || inode.ShouldDelete() {
		ok = false
		return
	}

	ok = true
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInode(dbHandle interface{}, inodeID uint64, unlinkNum int, timestamp int64, trashEnable bool) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if outOfRange, _ := mp.isInoOutOfRange(inodeID); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var (
		inode *Inode
		st    uint8
	)
	inode, err = mp.inodeTree.Get(inodeID)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}

	if inode == nil || inode.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = inode
	inode.DecNlinkNum(uint32(unlinkNum - 1))
	if inode.IsEmptyDir() {
		st, err = mp.mvToDeletedInodeTree(dbHandle, inode, timestamp)
		if err != nil {
			log.LogDebugf("fsmUnlinkInode: failed to move inode to deletedInode tree, inode: %v, status: %v",
				inode, st)
			return
		}
		log.LogDebugf("fsmUnlinkInode: inode: %v, status: %v", inode, st)
		return
	}
	inode.DecNLink()
	if err = mp.inodeTree.Update(dbHandle, inode); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInodeBatch(dbHandle interface{}, batchInode InodeBatch, timestamp int64, trashEnable bool) (resp []*InodeResponse, err error) {
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
		var rsp *InodeResponse
		rsp, err = mp.fsmUnlinkInode(dbHandle, inodeID, unlinkNum, timestamp, trashEnable)
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
		return nil
	}

	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	for {
		if err = binary.Read(buf, binary.BigEndian, &ino.Inode); err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return
		}
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)

		if err = mp.internalDeleteInode(dbHandle, ino); err != nil {
			return
		}
	}
	return
}

func (mp *metaPartition) internalCursorReset(req *proto.CursorResetRequest) (resp *CursorResetResponse, err error) {
	resp = new(CursorResetResponse)
	resp.Status = proto.OpErr

	switch CursorResetMode(req.CursorResetType) {
	case SubCursor:
		if ok := atomic.CompareAndSwapUint64(&mp.config.Cursor, req.Cursor, req.NewCursor); !ok {
			log.LogInfof("mp[%v] reset cursor, failed: cursor changed", mp.config.PartitionId)
			resp.Msg = fmt.Sprintf("mp[%v] reset cursor, failed: cursor changed", mp.config.PartitionId)
			return
		}
	case AddCursor:
		atomic.StoreUint64(&mp.config.Cursor, req.NewCursor)
	default:
		resp.Msg = fmt.Sprintf("mp[%v] with error reset type[%v]", mp.config.PartitionId, req.CursorResetType)
		return
	}

	mp.inodeTree.SetCursor(mp.config.Cursor)
	if err = mp.inodeTree.PersistBaseInfo(); err != nil {
		resp.Msg = fmt.Sprintf("mp[%v] reset cursor failed, [error:%s]", mp.config.PartitionId, err.Error())
		return
	}

	resp.Status = proto.OpOk
	log.LogInfof("internalCursorReset: partitionID(%v) reset to (%v) ", mp.config.PartitionId, mp.config.Cursor)
	return
}

func (mp *metaPartition) internalDeleteBatch(dbHandle interface{}, inodes InodeBatch) (err error) {
	for _, ino := range inodes {
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		if err = mp.internalDeleteInode(dbHandle, ino); err != nil {
			break
		}
	}
	return
}

func (mp *metaPartition) internalDeleteInode(dbHandle interface{}, ino *Inode) (err error) {
	_, err = mp.inodeTree.Delete(dbHandle, ino.Inode)
	mp.freeList.Remove(ino.Inode)
	_, err = mp.extendTree.Delete(dbHandle, ino.Inode) // Also delete extend attribute.
	return
}

func (mp *metaPartition) fsmAppendExtents(ctx context.Context, dbHandle interface{}, ino *Inode) (status uint8, err error) {
	status = proto.OpOk

	if outOfRange, _ := mp.isInoOutOfRange(ino.Inode); outOfRange {
		status = proto.OpInodeOutOfRange
		return
	}

	var existInode *Inode
	existInode, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}
	if existInode == nil || existInode.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	eks := ino.Extents.CopyExtents()

	if ino.Flag == proto.CheckPreExtentExist {
		// need check ek exist
		for _, ek := range eks {
			if ok, _ := existInode.Extents.HasExtent(ek); !ok {
				status = proto.OpNotExistErr
				log.LogWarnf("fsm(%v) AppendExtents pre check failed, inode(%v) ek(insert: %v)",
					mp.config.PartitionId, existInode.Inode, ek)
				return
			}
		}
	}

	delExtents := existInode.InsertExtents(ctx, eks, ino.ModifyTime)
	if err = mp.inodeTree.Put(dbHandle, existInode); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, existInode.Inode, delExtents, err)
		return
	}
	if err = mp.inodeTree.CommitBatchWrite(dbHandle, true); err != nil {
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Commit error:%v",
			mp.config.PartitionId, existInode.Inode, eks, err)
		status = proto.OpErr
		return
	}
	_ = mp.inodeTree.ClearBatchWriteHandle(dbHandle)
	log.LogInfof("fsm(%v) AppendExtents inode(%v) exts(%v) extDelChLen(%v)", mp.config.PartitionId, existInode.Inode, delExtents, len(mp.extDelCh))

	select {
	case mp.extDelCh <- delExtents:
	default:
		log.LogWarnf("fsm(%v) AppendExtents inode(%v) delEks(%v)", mp.config.PartitionId, existInode.Inode, delExtents)
	}
	return
}

func (mp *metaPartition) fsmInsertExtents(ctx context.Context, dbHandle interface{}, ino *Inode) (status uint8, err error) {
	status = proto.OpOk

	if outOfRange, _ := mp.isInoOutOfRange(ino.Inode); outOfRange {
		status = proto.OpInodeOutOfRange
		return
	}

	var existIno *Inode
	existIno, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}
	if existIno == nil || existIno.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	eks := ino.Extents.CopyExtents()

	if ino.Flag == proto.CheckPreExtentExist {
		// need check ek exist
		for _, ek := range eks {
			if ok, _ := existIno.Extents.HasExtent(ek); !ok {
				status = proto.OpNotExistErr
				log.LogWarnf("fsm(%v) InsertExtents pre check failed, inode(%v) ek(insert: %v)",
					mp.config.PartitionId, existIno.Inode, ek)
				return
			}
		}
	}

	oldSize := existIno.Size
	delExtents := existIno.InsertExtents(ctx, eks, ino.ModifyTime)
	newSize := existIno.Size
	if err = mp.inodeTree.Put(dbHandle, existIno); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(InsertExtents) inode(%v) eks(insert: %v, deleted: %v) size(old: %v, new: %v) Put error:%v",
			mp.config.PartitionId, existIno.Inode, eks, delExtents, oldSize, newSize, err)
		return
	}
	if err = mp.inodeTree.CommitBatchWrite(dbHandle, true); err != nil {
		log.LogErrorf("fsm(%v) action(InsertExtents) inode(%v) eks(insert: %v, deleted: %v) size(old: %v, new: %v) Commit error:%v",
			mp.config.PartitionId, existIno.Inode, eks, delExtents, oldSize, newSize, err)
		status = proto.OpErr
		return
	}
	_ = mp.inodeTree.ClearBatchWriteHandle(dbHandle)
	log.LogInfof("fsm(%v) InsertExtents inode(%v) eks(insert: %v, deleted: %v) size(old: %v, new: %v) extDelChLen(%v)",
		mp.config.PartitionId, existIno.Inode, eks, delExtents, oldSize, newSize, len(mp.extDelCh))
	select {
	case mp.extDelCh <- delExtents:
	default:
		log.LogWarnf("fsm(%v) InsertExtents inode(%v) delEks(%v)", mp.config.PartitionId, existIno.Inode, delExtents)
	}
	return
}

func (mp *metaPartition) fsmExtentsTruncate(dbHandle interface{}, ino *Inode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if outOfRange, _ := mp.isInoOutOfRange(ino.Inode); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var i *Inode
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
	oldSize := i.Size
	// we use CreateTime store req.Version in opFSMExtentTruncate request
	// we use AccessTime store req.OldSize in opFSMExtentTruncate request
	if ino.CreateTime == proto.TruncateRequestVersion_1 && oldSize != uint64(ino.AccessTime) {
		if ino.Size < i.Size {
			// due to high latency request was processed here
			log.LogWarnf("fsm(%v) ExtentsTruncate fotal error, may cause data lost here, "+
				"inode(%v) req [oldSize(%v) ==> newSize(%v)] mismatch file size(%v)",
				mp.config.PartitionId, i.Inode, ino.AccessTime, ino.Size, i.Size)
		}
		// otherwise may caused by repeat execute
		log.LogWarnf("fsm(%v) ExtentsTruncate error, inode(%v) req [oldSize(%v) ==> newSize(%v)] mismatch file size(%v)",
			mp.config.PartitionId, i.Inode, ino.AccessTime, ino.Size, i.Size)
		resp.Status = proto.OpArgMismatchErr
		return
	}
	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime)
	newSize := i.Size

	if err = mp.inodeTree.Put(dbHandle, i); err != nil {
		resp.Status = proto.OpErr
		return
	}
	if err = mp.inodeTree.CommitBatchWrite(dbHandle, true); err != nil {
		log.LogErrorf("fsm(%v) action(ExtentsTruncate) inode(%v) size(old: %v, new: %v, req: %v) Commit error:%v",
			mp.config.PartitionId, ino.Inode, oldSize, newSize, ino.Size, err)
		resp.Status = proto.OpErr
		return
	}
	_ = mp.inodeTree.ClearBatchWriteHandle(dbHandle)
	// now we should delete the extent
	log.LogInfof("fsm(%v) ExtentsTruncate inode(%v) size(old: %v, new: %v, req: %v) delExtents(%v) extDelChLen(%v)",
		mp.config.PartitionId, i.Inode, oldSize, newSize, ino.Size, delExtents, len(mp.extDelCh))
	select {
	case mp.extDelCh <- delExtents:
	default:
		log.LogWarnf("fsm(%v) ExtentsTruncate inode(%v) delEks(%v)", mp.config.PartitionId, i.Inode, delExtents)
	}
	return
}

func (mp *metaPartition) fsmEvictInode(dbHandle interface{}, ino *Inode, timestamp int64, trashEnable bool) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if outOfRange, _ := mp.isInoOutOfRange(ino.Inode); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var (
		i  *Inode
		st uint8
	)
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil {
		return
	}
	defer mp.monitorData[proto.ActionMetaEvictInode].UpdateData(i.Size)

	if i.IsEmptyDir() || i.IsDeleting() {
		i.SetDeleteMark()
		st, err = mp.mvToDeletedInodeTree(dbHandle, i, timestamp)
		if err != nil {
			log.LogErrorf("fsmEvictInode: failed to move inode to deletedInode tree, inode:%v, status:%v",
				ino, st)
			resp.Status = proto.OpErr
		}
		log.LogDebugf("fsmEvictInode: inode: %v, status: %v", ino, st)
		return
	}
	return
}

func (mp *metaPartition) fsmBatchEvictInode(dbHandle interface{}, ib InodeBatch, timestamp int64, trashEnable bool) (resp []*InodeResponse, err error) {
	defer func() {
		if err != nil {
			for index := 0; index < len(ib); index++ {
				resp = append(resp, &InodeResponse{Status: proto.OpErr})
			}
		}
	}()
	for _, ino := range ib {
		var rsp *InodeResponse
		rsp, err = mp.fsmEvictInode(dbHandle, ino, timestamp, trashEnable)
		if err == rocksDBError {
			resp = resp[:0]
			return
		}
		resp = append(resp, rsp)
	}
	return
}

func (mp *metaPartition) checkAndInsertFreeList(ino *Inode) {
	if proto.IsDir(ino.Type) {
		return
	}
	if ino.ShouldDelete() {
		dbWriteHandle, _ := mp.inodeTree.CreateBatchWriteHandle()
		defer mp.inodeTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, false)
		st, _ := mp.mvToDeletedInodeTree(dbWriteHandle, ino, time.Now().UnixNano()/1000)
		log.LogDebugf("checkAndInsertFreeList moveToDeletedInodeTree: mp:%v, inode: %v, status: %v", mp.config.PartitionId, ino, st)
	}
}

func (mp *metaPartition) fsmSetAttr(dbHandle interface{}, req *SetattrRequest) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if outOfRange, _ := mp.isInoOutOfRange(req.Inode); outOfRange {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	ino := NewInode(req.Inode, req.Mode)
	ino, err = mp.inodeTree.Get(ino.Inode)
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
	ino.SetAttr(req)
	if err = mp.inodeTree.Update(dbHandle, ino); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}

func (mp *metaPartition) fsmExtentsMerge(dbHandle interface{}, im *InodeMerge) (status uint8, err error) {
	status = proto.OpOk
	var ino *Inode
	inodeId := im.Inode
	newExtents := im.NewExtents
	oldExtents := im.OldExtents
	var delExtents []proto.MetaDelExtentKey

	//var delExtents = newExtents
	defer func() {
		if len(delExtents) > 0 {
			select {
			case mp.extDelCh <- delExtents:
			default:
				log.LogWarnf("fsm(%v) ExtentsMerge inode(%v) delEks(%v)", mp.config.PartitionId, inodeId, delExtents)
			}
			log.LogInfof("fsm(%v) ExtentsMerge inode(%v) delExtents(%v) newExtents(%v) extDelChLen(%v)",
				mp.config.PartitionId, inodeId, delExtents, newExtents, len(mp.extDelCh))
		}
	}()
	var outOfRange bool
	if outOfRange, _ = mp.isInoOutOfRange(inodeId); outOfRange {
		status = proto.OpInodeOutOfRange
		return
	}
	ino, err = mp.inodeTree.Get(im.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}
	if ino == nil {
		status = proto.OpNotExistErr
		return
	}
	if ino.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	var merged bool
	var msg string
	delExtents, merged, msg = ino.MergeExtents(newExtents, oldExtents)
	if !merged {
		log.LogWarnf("InodeMergeFailed inode(%v) merge msg(%v)", inodeId, msg)
		status = proto.OpInodeMergeErr
		return
	}
	if err = mp.inodeTree.Put(dbHandle, ino); err != nil {
		//unknown other apply status, do not del ek
		status = proto.OpErr
		delExtents = delExtents[:0]
		return
	}
	if err = mp.inodeTree.CommitBatchWrite(dbHandle, true); err != nil {
		log.LogErrorf("fsm(%v) action(MergeExtents) inode(%v) oldEks(%v) newEks(%v) Commit error:%v",
			mp.config.PartitionId, ino.Inode, oldExtents, newExtents, err)
		status = proto.OpErr
		delExtents = delExtents[:0]
		return
	}
	_ = mp.inodeTree.ClearBatchWriteHandle(dbHandle)
	return
}
