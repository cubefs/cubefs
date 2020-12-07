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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"

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
		return 0, err
	}
	if mp.config.VolName != req.VolName {
		return 0, fmt.Errorf("partition:[%d] reset vol name not equal mp:[%s] req:[%s]", mp.config.PartitionId, mp.config.VolName, req.VolName)
	}

	if mp.freeList.Len() != 0 {
		return 0, fmt.Errorf("partition:[%d] freeList len must 0, but got [%d]", mp.config.PartitionId, mp.inodeTree.Len())
	}

	if mp.inodeTree.Len() != 0 {
		if mp.inodeTree.Len() == 1 && mp.inodeTree.Has(NewInode(1, 0)) { // if is root inode
			atomic.StoreUint64(&mp.config.Cursor, mp.config.Start+2)
		} else {
			return 0, fmt.Errorf("partition:[%d] reset inode len must 0, but got [%d]", mp.config.PartitionId, mp.inodeTree.Len())
		}
	} else {
		atomic.StoreUint64(&mp.config.Cursor, mp.config.Start+1)
	}

	log.LogInfof("internalCursorReset: partitionID(%v) reset to ", mp.config.PartitionId, mp.config.Cursor)
	return mp.config.Cursor, nil
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
	eks := ino.Extents.CopyExtents()
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime)
	log.LogInfof("fsmAppendExtents inode(%v) exts(%v)", ino2.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

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

	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime)

	// now we should delete the extent
	log.LogInfof("fsmExtentsTruncate inode(%v) exts(%v)", i.Inode, delExtents)
	mp.extDelCh <- delExtents
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
