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

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(ino *Inode) (status uint8, err error) {
	status = proto.OpOk
	if err = mp.inodeTree.Create(ino, false); err != nil {
		if err == existsError {
			status = proto.OpExistErr
		} else {
			status = proto.OpErr
		}
	}
	return
}

func (mp *metaPartition) fsmCreateLinkInode(ino *Inode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if err = mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var existIno *Inode
	existIno, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if existIno == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	if existIno.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	existIno.IncNLink()
	if err = mp.inodeTree.Put(existIno); err != nil {
		resp.Status = proto.OpErr
		return
	}
	resp.Msg = existIno
	return
}

func (mp *metaPartition) getInode(ino *Inode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if err = mp.isInoOutOfRange(ino.Inode); err != nil {
		log.LogErrorf(err.Error())
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var i *Inode
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		if err == rocksdbError {
			exporter.WarningRocksdbError(fmt.Sprintf("action[getInode] clusterID[%s] volumeName[%s] partitionID[%v]" +
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
	if err == rocksdbError {
		exporter.WarningRocksdbError(fmt.Sprintf("action[Has] clusterID[%s] volumeName[%s] partitionID[%v]" +
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
func (mp *metaPartition) fsmUnlinkInode(ino *Inode, timestamp int64, trashEnable bool) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if err = mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var (
		inode *Inode
		st    uint8
	)
	inode, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}

	if inode == nil || inode.ShouldDelete(){
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = inode
	if inode.IsEmptyDir() {
		if trashEnable {
			st, err = mp.mvToDeletedInodeTree(inode, timestamp)
			if err != nil {
				log.LogDebugf("fsmUnlinkInode: failed to move inode to deletedInode tree, inode: %v, status: %v",
					ino, st)
				return
			}
			log.LogDebugf("fsmUnlinkInode: inode: %v, status: %v", ino, st)
		} else {
			if _, err = mp.inodeTree.Delete(inode.Inode); err != nil && err != notExistsError {
				log.LogErrorf("fsmUnlinInode: inode:%v delete failed:%v", ino, err)
				resp.Status = proto.OpErr
			}
		}
		return
	}
	inode.DecNLink()
	if err = mp.inodeTree.Put(inode); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInodeBatch(ib InodeBatch, timestamp int64, trashEnable bool) (resp []*InodeResponse, err error) {
	resp = make([]*InodeResponse, 0)
	wrongIndex := len(ib)
	defer func() {
		for index := wrongIndex; index < len(ib); index++ {
			resp = append(resp, &InodeResponse{Status: proto.OpErr, Msg: ib[index]})
		}
	}()

	for index := 0; index < len(ib); index++ {
		var rsp *InodeResponse
		rsp, err = mp.fsmUnlinkInode(ib[index], timestamp, trashEnable)
		if err == rocksdbError {
			wrongIndex = index
			break
		}
		resp = append(resp, rsp)
	}
	return
}

func (mp *metaPartition) internalDelete(val []byte) error {
	if len(val) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	for {
		if err := binary.Read(buf, binary.BigEndian, &ino.Inode); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)

		if err := mp.internalDeleteInode(ino); err == rocksdbError {
			return err
		}
	}
}

func (mp *metaPartition) internalCursorReset(req *proto.CursorResetRequest) (uint64, error) {
	if ok := atomic.CompareAndSwapUint64(&mp.config.Cursor, req.Cursor, req.Inode); !ok {
		log.LogInfof("mp[%v] reset cursor, failed: cursor changed", mp.config.PartitionId)
		return mp.config.Cursor, fmt.Errorf("mp[%v] reset cursor, failed: cursor changed", mp.config.PartitionId)
	}

	log.LogInfof("internalCursorReset: partitionID(%v) reset to (%v) ", mp.config.PartitionId, mp.config.Cursor)
	return mp.config.Cursor, nil
}

func (mp *metaPartition) internalDeleteBatch(inodes InodeBatch) error {
	for _, ino := range inodes {
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		if err := mp.internalDeleteInode(ino); err == rocksdbError {
			return err
		}
	}

	return nil
}

func (mp *metaPartition) internalDeleteInode(ino *Inode) (err error){
	_, err = mp.inodeTree.Delete(ino.Inode)
	mp.freeList.Remove(ino.Inode)
	_, err = mp.extendTree.Delete(ino.Inode) // Also delete extend attribute.
	return
}

func (mp *metaPartition) fsmAppendExtents(ctx context.Context, ino *Inode) (status uint8, err error) {
	status = proto.OpOk

	if err = mp.isInoOutOfRange(ino.Inode); err != nil {
		status = proto.OpInodeOutOfRange
		return
	}

	var existInode *Inode
	existInode, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}
	if existInode == nil || existInode.ShouldDelete(){
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

	delExtents := existInode.AppendExtents(ctx, eks, ino.ModifyTime)
	if err = mp.inodeTree.Put(existInode); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(AppendExtents) inode(%v) exts(%v) Put error:%v",
			mp.config.PartitionId, existInode.Inode, delExtents, err)
		return
	}
	log.LogInfof("fsm(%v) AppendExtents inode(%v) exts(%v)", mp.config.PartitionId, existInode.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmInsertExtents(ctx context.Context, ino *Inode) (status uint8, err error) {
	status = proto.OpOk

	if err = mp.isInoOutOfRange(ino.Inode); err != nil {
		status = proto.OpInodeOutOfRange
		return
	}

	var existIno *Inode
	existIno, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		status = proto.OpErr
		return
	}
	if existIno == nil || existIno.ShouldDelete(){
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
	if err = mp.inodeTree.Put(existIno); err != nil {
		status = proto.OpErr
		log.LogErrorf("fsm(%v) action(InsertExtents) inode(%v) eks(insert: %v, deleted: %v) size(old: %v, new: %v) Put error:%v",
			mp.config.PartitionId, existIno.Inode, eks, delExtents, oldSize, newSize, err)
		return
	}
	log.LogInfof("fsm(%v) InsertExtents inode(%v) eks(insert: %v, deleted: %v) size(old: %v, new: %v)",
		mp.config.PartitionId, existIno.Inode, eks, delExtents, oldSize, newSize)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk

	if err = mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var i *Inode
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil || i.ShouldDelete(){
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

	if err = mp.inodeTree.Put(i); err != nil {
		resp.Status = proto.OpErr
		return
	}
	// now we should delete the extent
	log.LogInfof("fsm(%v) ExtentsTruncate inode(%v) size(old: %v, new: %v, req: %v) delExtents(%v)",
		mp.config.PartitionId, i.Inode, oldSize, newSize, ino.Size, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmEvictInode(ino *Inode, timestamp int64, trashEnable bool) (resp *InodeResponse, err error) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk

	if err = mp.isInoOutOfRange(ino.Inode); err != nil {
		resp.Status = proto.OpInodeOutOfRange
		return
	}

	var (
		i *Inode
		st uint8
	)
	i, err = mp.inodeTree.Get(ino.Inode)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if i == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	if i.ShouldDelete() {
		return
	}
	defer func() {
		//inode info maybe change
		if !trashEnable {
			//if trash enable, inode should be deleted from inode tree
			if err = mp.inodeTree.Put(i); err != nil {
				resp.Status = proto.OpErr
			}
		}
	}()

	if proto.IsDir(i.Type) {
		if i.IsEmptyDir() {
			i.SetDeleteMark()
		}
		return
	}

	if i.IsTempFile() {
		i.SetDeleteMark()
		if trashEnable {
			st, err = mp.mvToDeletedInodeTree(i, timestamp)
			if err != nil {
				log.LogErrorf("fsmEvictInode: failed to move inode to deletedInode tree, inode:%v, status:%v",
					ino, st)
				resp.Status = proto.OpErr
			}
			log.LogDebugf("fsmEvictInode: inode: %v, status: %v", ino, st)
		} else {
			mp.freeList.Push(i.Inode)
		}
	}
	return
}

func (mp *metaPartition) fsmBatchEvictInode(ib InodeBatch, timestamp int64, trashEnable bool) (resp []*InodeResponse, err error) {
	var wrongIndex = len(ib)
	defer func() {
		for index := wrongIndex; index < len(ib); index++ {
			resp = append(resp, &InodeResponse{Status: proto.OpErr})
		}
	}()
	for index, ino := range ib {
		var rsp *InodeResponse
		rsp, err = mp.fsmEvictInode(ino, timestamp, trashEnable)
		if err == rocksdbError {
			wrongIndex = index
			break
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
		st, _ := mp.mvToDeletedInodeTree(ino, time.Now().UnixNano() / 1000)
		log.LogDebugf("checkAndInsertFreeList moveToDeletedInodeTree: inode: %v, status: %v", ino, st)
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
	if err = mp.inodeTree.Put(ino); err != nil {
		resp.Status = proto.OpErr
		return
	}
	return
}
