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
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"io"
	"sync/atomic"
)

type InodeResponse struct {
	Status uint8
	Msg    *Inode
}

func NewInodeResponse() *InodeResponse {
	return &InodeResponse{}
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}
	return
}

func (mp *metaPartition) fsmCreateLinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if err := mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

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
	i.IncNLink()
	resp.Msg = i
	return
}

func (mp *metaPartition) getInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if err := mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

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
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	i.AccessTime = Now.GetCurrentTime().Unix()
	resp.Msg = i
	return
}

func (mp *metaPartition) hasInode(ino *Inode) (ok bool, inode *Inode) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		ok = false
		return
	}
	inode = item.(*Inode)
	if inode.ShouldDelete() {
		ok = false
		return
	}
	ok = true
	return
}

func (mp *metaPartition) getInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

// Ascend is the wrapper of inodeTree.Ascend
func (mp *metaPartition) Ascend(f func(i BtreeItem) bool) {
	mp.inodeTree.Ascend(f)
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if err := mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

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

	if inode.IsEmptyDir() {
		mp.inodeTree.Delete(inode)
	}
	inode.DecNLink()
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInodeBatch(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		resp = append(resp, mp.fsmUnlinkInode(ino))
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

func (mp *metaPartition) internalCursorReset(val []byte) (uint64, error) {
	req := &proto.CursorResetRequest{}
	if err := json.Unmarshal(val, req); err != nil {
		log.LogInfof("mp[%v] reset cursor, json unmarshal failed:%s", mp.config.PartitionId, err.Error())
		return mp.config.Cursor, err
	}

	if ok := atomic.CompareAndSwapUint64(&mp.config.Cursor, req.Cursor, req.Inode); !ok {
		log.LogInfof("mp[%v] reset cursor, failed: cursor changed", mp.config.PartitionId)
		return mp.config.Cursor, fmt.Errorf("mp[%v] reset cursor, failed: cursor changed", mp.config.PartitionId)
	}

	log.LogInfof("internalCursorReset: partitionID(%v) reset to (%v) ", mp.config.PartitionId, mp.config.Cursor)
	return mp.config.Cursor, nil
}

func (mp *metaPartition) internalDeleteBatch(ctx context.Context, val []byte) error {
	if len(val) == 0 {
		return nil
	}
	inodes, err := InodeBatchUnmarshal(ctx, val)
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

func (mp *metaPartition) fsmAppendExtents(ctx context.Context, ino *Inode) (status uint8) {
	status = proto.OpOk

	if err := mp.isInoOutOfRange(ino.Inode); err != nil {
		status = proto.OpInodeOutOfRange
		return
	}

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
	eks := ino.Extents.CopyExtents()

	if ino.Flag == proto.CheckPreExtentExist {
		// need check ek exist
		for _, ek := range eks {
			if ok, _ := ino2.Extents.HasExtent(ek); !ok {
				status = proto.OpErr
				log.LogWarnf("fsm(%v) AppendExtents pre check failed, inode(%v) ek(insert: %v)",
					mp.config.PartitionId, ino2.Inode, ek)
				return
			}
		}
	}

	delExtents := ino2.AppendExtents(ctx, eks, ino.ModifyTime)
	log.LogInfof("fsm(%v) AppendExtents inode(%v) exts(%v)", mp.config.PartitionId, ino2.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmInsertExtents(ctx context.Context, ino *Inode) (status uint8) {
	status = proto.OpOk

	if err := mp.isInoOutOfRange(ino.Inode); err != nil {
		status = proto.OpInodeOutOfRange
		return
	}

	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	existIno := item.(*Inode)
	if existIno.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	eks := ino.Extents.CopyExtents()

	if ino.Flag == proto.CheckPreExtentExist {
		// need check ek exist
		for _, ek := range eks {
			if ok, _ := existIno.Extents.HasExtent(ek); !ok {
				status = proto.OpErr
				log.LogWarnf("fsm(%v) InsertExtents pre check failed, inode(%v) ek(insert: %v)",
					mp.config.PartitionId, existIno.Inode, ek)
				return
			}
		}
	}

	oldSize := existIno.Size
	delExtents := existIno.InsertExtents(ctx, eks, ino.ModifyTime)
	newSize := existIno.Size
	log.LogInfof("fsm(%v) InsertExtents inode(%v) eks(insert: %v, deleted: %v) size(old: %v, new: %v)",
		mp.config.PartitionId, existIno.Inode, eks, delExtents, oldSize, newSize)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk

	if err := mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

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

	// now we should delete the extent
	log.LogInfof("fsm(%v) ExtentsTruncate inode(%v) size(old: %v, new: %v, req: %v) delExtents(%v)",
		mp.config.PartitionId, i.Inode, oldSize, newSize, ino.Size, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk

	if err := mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

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
	}
}

func (mp *metaPartition) fsmSetAttr(req *SetattrRequest) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if interErr := mp.isInoOutOfRange(req.Inode); interErr != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

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
