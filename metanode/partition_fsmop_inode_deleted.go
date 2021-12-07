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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"io"
)

type FSMDeletedINode struct {
	inode uint64
}

func NewFSMDeletedINode(ino uint64) *FSMDeletedINode {
	fi := new(FSMDeletedINode)
	fi.inode = ino
	return fi
}

func (i *FSMDeletedINode) Marshal() (res []byte, err error) {
	res = make([]byte, 8)
	binary.BigEndian.PutUint64(res, i.inode)
	return
}

func (i *FSMDeletedINode) Unmarshal(data []byte) (err error) {
	i.inode = binary.BigEndian.Uint64(data)
	return
}

type FSMDeletedINodeBatch []*FSMDeletedINode

func (db FSMDeletedINodeBatch) Marshal() (data []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	err = binary.Write(buff, binary.BigEndian, uint32(len(db)))
	if err != nil {
		return
	}

	for _, di := range db {
		var bs []byte
		bs, err = di.Marshal()
		if err != nil {
			return
		}
		err = binary.Write(buff, binary.BigEndian, uint32(len(bs)))
		if err != nil {
			return
		}
		_, err = buff.Write(bs)
		if err != nil {
			return
		}
	}
	data = buff.Bytes()
	return
}

func FSMDeletedINodeBatchUnmarshal(raw []byte) (FSMDeletedINodeBatch, error) {
	buff := bytes.NewBuffer(raw)
	var batchLen uint32
	if err := binary.Read(buff, binary.BigEndian, &batchLen); err != nil {
		return nil, err
	}

	result := make(FSMDeletedINodeBatch, 0, int(batchLen))

	var dataLen uint32
	for j := 0; j < int(batchLen); j++ {
		if err := binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		data := make([]byte, int(dataLen))
		if _, err := buff.Read(data); err != nil {
			return nil, err
		}
		ino := new(FSMDeletedINode)
		if err := ino.Unmarshal(data); err != nil {
			return nil, err
		}
		result = append(result, ino)
	}

	return result, nil
}

type fsmOpDeletedInodeResponse struct {
	Status uint8  `json:"st"`
	Inode  uint64 `json:"ino"`
}

func (mp *metaPartition) mvToDeletedInodeTree(inode *Inode, timestamp int64) (status uint8) {
	status = proto.OpOk
	dino := NewDeletedInode(inode, timestamp)
	status = mp.fsmCreateDeletedInode(dino).Status
	if status != proto.OpOk && status != proto.OpNotExistErr {
		log.LogErrorf("[mvToDeletedInodeTree], inode: %v, status: %v", inode, status)
		return
	}
	mp.inodeTree.Delete(inode)
	return
}

func (mp *metaPartition) fsmCreateDeletedInode(dino *DeletedINode) (rsp *fsmOpDeletedInodeResponse) {
	rsp = new(fsmOpDeletedInodeResponse)
	rsp.Inode = dino.Inode.Inode
	rsp.Status = proto.OpOk
	_, ok := mp.inodeDeletedTree.ReplaceOrInsert(dino, false)
	if !ok {
		rsp.Status = proto.OpExistErr
	}
	return
}

func (mp *metaPartition) fsmBatchRecoverDeletedInode(inos FSMDeletedINodeBatch) (rsp []*fsmOpDeletedInodeResponse) {
	rsp = make([]*fsmOpDeletedInodeResponse, 0)
	for _, ino := range inos {
		resp := mp.recoverDeletedInode(ino.inode)
		if resp.Status != proto.OpOk {
			rsp = append(rsp, resp)
		}
	}
	return
}

func (mp *metaPartition) fsmRecoverDeletedInode(ino *FSMDeletedINode) (
	resp *fsmOpDeletedInodeResponse) {
	return mp.recoverDeletedInode(ino.inode)
}

func (mp *metaPartition) recoverDeletedInode(inode uint64) (
	resp *fsmOpDeletedInodeResponse) {
	resp = new(fsmOpDeletedInodeResponse)
	resp.Inode = inode
	resp.Status = proto.OpOk

	var (
		currInode interface{}
		deletedInode  interface{}
	)

	ino := NewInode(inode, 0)
	defer func() {
		if resp.Status != proto.OpOk {
			log.LogDebugf("[recoverDeletedInode], partitionID(%v), inode(%v), status: %v",
				mp.config.PartitionId, ino.Inode, resp.Status)
		}
	}()

	dino := NewDeletedInodeByID(inode)
	currInode = mp.inodeTree.CopyGet(ino)
	deletedInode = mp.inodeDeletedTree.CopyGet(dino)
	if currInode != nil {
		i := currInode.(*Inode)
		if deletedInode != nil {
			_ = mp.inodeDeletedTree.Delete(dino)
			return
		}

		if i.ShouldDelete() {
			log.LogDebugf("[recoverDeletedInode], the inode[%v] 's deleted flag is invalid", ino)
			i.CancelDeleteMark()
		}
		if !proto.IsDir(i.Type)  {
			i.IncNLink() // TODO: How to handle idempotent?
		}
		log.LogDebugf("[recoverDeletedInode], success to increase the link of inode[%v]", inode)
		return
	}

	if deletedInode == nil {
		log.LogErrorf("[recoverDeletedInode], not found the inode[%v] from deletedTree", dino)
		resp.Status = proto.OpNotExistErr
		return
	}

	if deletedInode.(*DeletedINode).IsExpired {
		log.LogWarnf("[recoverDeletedInode], inode: [%v] is expired", deletedInode.(*DeletedINode))
		resp.Status = proto.OpNotExistErr
		return
	}

	inoPtr := deletedInode.(*DeletedINode).buildInode()
	inoPtr.CancelDeleteMark()
	if inoPtr.IsEmptyDir() {
		inoPtr.NLink = 2
	} else {
		inoPtr.IncNLink()
	}
	_, ok := mp.inodeTree.ReplaceOrInsert(inoPtr, false)
	if !ok {
		log.LogErrorf("[[recoverDeletedInode], failed to add inode to inodeTree, inode: [%v]", inoPtr)
		resp.Status = proto.OpErr
		return
	}
	mp.inodeDeletedTree.Delete(dino)
	return
}

func (mp *metaPartition) fsmBatchCleanDeletedInode(inos FSMDeletedINodeBatch) (rsp []*fsmOpDeletedInodeResponse) {
	rsp = make([]*fsmOpDeletedInodeResponse, 0)
	for _, ino := range inos {
		resp := mp.cleanDeletedInode(ino.inode)
		if resp.Status != proto.OpOk {
			rsp = append(rsp, resp)
		}
	}
	return
}

func (mp *metaPartition) fsmCleanDeletedInode(ino *FSMDeletedINode) (
	resp *fsmOpDeletedInodeResponse) {
	return mp.cleanDeletedInode(ino.inode)
}

func (mp *metaPartition) cleanDeletedInode(inode uint64) (
	resp *fsmOpDeletedInodeResponse) {
	resp = new(fsmOpDeletedInodeResponse)
	resp.Inode = inode
	resp.Status = proto.OpOk
	ino := NewInode(inode, 0)
	defer func() {
		log.LogDebugf("[cleanDeletedInode], inode: (%v), status:[%v]", ino.Inode, resp.Status)
	}()

	dino := NewDeletedInodeByID(inode)
	item := mp.inodeDeletedTree.CopyGet(dino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	begDen := newPrimaryDeletedDentry(dino.Inode.Inode, "", 0, 0)
	endDen := newPrimaryDeletedDentry(dino.Inode.Inode+1, "", 0, 0)
	var children int
	mp.dentryDeletedTree.AscendRange(begDen, endDen, func(i BtreeItem) bool {
		children++
		if children > 0 {
			return false
		}
		return true
	})

	if children > 0 {
		resp.Status = proto.OpExistErr
		return
	}

	i := item.(*DeletedINode)
	if i.IsEmptyDir() {
		mp.inodeDeletedTree.Delete(dino)
		return
	}

	if i.IsTempFile() {
		i.setExpired()
		mp.freeList.Push(i.Inode.Inode)
		return
	}
	resp.Status = proto.OpErr
	return
}

func (mp *metaPartition) fsmCleanExpiredInode(inos FSMDeletedINodeBatch) (rsp []*fsmOpDeletedInodeResponse) {
	rsp = make([]*fsmOpDeletedInodeResponse, 0)
	for _, ino := range inos {
		resp := mp.cleanExpiredInode(ino.inode)
		if resp.Status != proto.OpOk {
			rsp = append(rsp, resp)
		}
	}
	return
}

func (mp *metaPartition) cleanExpiredInode(ino uint64) (
	resp *fsmOpDeletedInodeResponse) {
	resp = new(fsmOpDeletedInodeResponse)
	resp.Inode = ino
	resp.Status = proto.OpOk
	defer func() {
		log.LogDebugf("[cleanExpiredInode], inode: %v, status: %v", ino, resp.Status)
	}()

	di := NewDeletedInodeByID(ino)
	item := mp.inodeDeletedTree.CopyGet(di)
	if item == nil {
		return
	}

	origin := item.(*DeletedINode)
	if origin.IsEmptyDir() {
		mp.inodeDeletedTree.Delete(di)
		return
	}

	if origin.IsTempFile() {
		origin.setExpired()
		mp.freeList.Push(di.Inode.Inode)
		return
	}

	resp.Status = proto.OpErr
	return
}

func (mp *metaPartition) internalClean(val []byte) (err error) {
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
		log.LogDebugf("internalClean: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		mp.internalCleanDeletedInode(ino)
	}
}

func (mp *metaPartition) internalCleanDeletedInode(ino *Inode) {
	dino := NewDeletedInode(ino, 0)
	if mp.inodeDeletedTree.Delete(dino) == nil {
		mp.inodeTree.Delete(ino)
		log.LogDebugf("[internalCleanDeletedInode], ino: %v", ino)
	} else {
		log.LogDebugf("[internalCleanDeletedInode], dino: %v", ino)
	}
	mp.freeList.Remove(ino.Inode)
	mp.extendTree.Delete(&Extend{inode: ino.Inode}) // Also delete extend attribute.
	return
}

func (mp *metaPartition) checkExpiredAndInsertFreeList(di *DeletedINode) {
	if proto.IsDir(di.Type) {
		return
	}
	if di.IsExpired {
		mp.freeList.Push(di.Inode.Inode)
	}
}
