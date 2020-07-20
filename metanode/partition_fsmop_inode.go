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
	"io"

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
func (mp *MetaPartition) fsmCreateInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	log.LogDebugf("[fsmCreateInode] add inode into inode table: %v", ino)
	if err := mp.inodeTree.Create(ino); err != nil {
		if err == existsError {
			status = proto.OpExistErr
		} else {
			log.LogIfNotNil(err)
			status = proto.OpErr
		}
	}
	return
}

func (mp *MetaPartition) fsmCreateLinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item, err := mp.inodeTree.Get(ino.Inode)
	if item == nil {
		log.LogIfNotNil(err)
		resp.Status = proto.OpNotExistErr
		return
	}
	if item.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	item.IncNLink()
	log.LogIfNotNil(mp.inodeTree.Update(item))
	resp.Msg = item
	return
}

func (mp *MetaPartition) getInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item, err := mp.inodeTree.RefGet(ino.Inode)
	if item == nil {
		log.LogIfNotNil(err)
		resp.Status = proto.OpNotExistErr
		return
	}
	if item.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	item.AccessTime = Now.GetCurrentTime().Unix()
	resp.Msg = item
	return
}

func (mp *MetaPartition) hasInode(ino *Inode) (ok bool) {
	item, err := mp.inodeTree.RefGet(ino.Inode)
	if item == nil {
		ok = false
		return
	}
	if err != nil {
		log.LogIfNotNil(err)
		ok = false
		return
	}
	if item.ShouldDelete() {
		ok = false
		return
	}
	ok = true
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *MetaPartition) fsmUnlinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	inode, err := mp.inodeTree.Get(ino.Inode)
	if err != nil {
		log.LogErrorf("get inode has err:[%s]", err.Error())
		resp.Status = proto.OpNotExistErr
		return
	}

	if inode.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = inode
	inode.DoWriteFunc(func() {
		inode.ModifyTime = ino.ModifyTime
	})

	if inode.IsEmptyDir() {
		log.LogIfNotNil(mp.inodeTree.Delete(inode.Inode))
	}

	inode.DecNLink()
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *MetaPartition) fsmUnlinkInodeBatch(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		resp = append(resp, mp.fsmUnlinkInode(ino))
	}
	return
}

func (mp *MetaPartition) internalDelete(val []byte) (err error) {
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

func (mp *MetaPartition) internalDeleteBatch(val []byte) error {
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

func (mp *MetaPartition) internalDeleteInode(ino *Inode) {
	log.LogIfNotNil(mp.inodeTree.Delete(ino.Inode))
	mp.freeList.Remove(ino.Inode)
	log.LogIfNotNil(mp.extendTree.Delete(ino.Inode)) // Also delete extend attribute.
	return
}

func (mp *MetaPartition) fsmAppendExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	ino2, err := mp.inodeTree.Get(ino.Inode)
	if err != nil {
		log.LogErrorf("get inode has err:[%s]", err.Error())
		status = proto.OpNotExistErr
		return
	}
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	eks := ino.Extents.CopyExtents()
	delExtents := ino2.AppendExtents(eks, ino.ModifyTime)

	log.LogIfNotNil(mp.inodeTree.Put(ino2))
	log.LogDebugf("[fsmAppendExtents] inode input: %v newInode: %v, eks: %v", ino, ino2, eks)
	mp.extDelCh <- delExtents
	return
}

func (mp *MetaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk
	item, err := mp.inodeTree.Get(ino.Inode)
	if err != nil {
		log.LogErrorf("get inode has err:[%s]", err.Error())
		resp.Status = proto.OpNotExistErr
		return
	}
	if item.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(item.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}

	delExtents := item.ExtentsTruncate(ino.Size, ino.ModifyTime)
	log.LogIfNotNil(mp.inodeTree.Update(item))
	// now we should delete the extent
	log.LogInfof("fsmExtentsTruncate inode(%v) exts(%v)", item.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *MetaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk
	item, err := mp.inodeTree.Get(ino.Inode)
	if err != nil {
		log.LogErrorf("get inode has err:[%s]", err.Error())
		resp.Status = proto.OpNotExistErr
		return
	}
	if item.ShouldDelete() {
		return
	}
	if proto.IsDir(item.Type) {
		if item.IsEmptyDir() {
			item.SetDeleteMark()
			log.LogIfNotNil(mp.inodeTree.Update(item))
		}
		return
	}

	if item.IsTempFile() {
		item.SetDeleteMark()
		mp.freeList.Push(item.Inode)
		log.LogIfNotNil(mp.inodeTree.Update(item))
	}
	return
}

func (mp *MetaPartition) fsmBatchEvictInode(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		resp = append(resp, mp.fsmEvictInode(ino))
	}
	return
}

func (mp *MetaPartition) checkAndInsertFreeList(ino *Inode) {
	if proto.IsDir(ino.Type) {
		return
	}
	if ino.ShouldDelete() {
		mp.freeList.Push(ino.Inode)
	}
}

func (mp *MetaPartition) fsmSetAttr(req *SetattrRequest) (err error) {
	item, err := mp.inodeTree.Get(req.Inode)
	if err != nil {
		log.LogErrorf("get inode has err:[%s]", err.Error())
		return
	}
	if item.ShouldDelete() {
		return
	}
	item.SetAttr(req.Valid, req.Mode, req.Uid, req.Gid)
	log.LogIfNotNil(mp.inodeTree.Update(item))
	return
}
