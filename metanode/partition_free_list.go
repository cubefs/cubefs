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
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	AsyncDeleteInterval           = 10 * time.Second
	UpdateVolTicket               = 2 * time.Minute
	BatchCounts                   = 128
	OpenRWAppendOpt               = os.O_CREATE | os.O_RDWR | os.O_APPEND
	TempFileValidTime             = 86400 //units: sec
	DeleteInodeFileExtension      = "INODE_DEL"
	DeleteWorkerCnt               = 10
	InodeNLink0DelayDeleteSeconds = 24 * 3600
	maxDeleteInodeFileSize        = MB
)

func (mp *metaPartition) startFreeList() (err error) {
	if mp.delInodeFp, err = os.OpenFile(path.Join(mp.config.RootDir,
		DeleteInodeFileExtension), OpenRWAppendOpt, 0644); err != nil {
		return
	}

	// start vol update ticket
	go mp.updateVolWorker()
	go mp.deleteWorker()
	mp.startToDeleteExtents()
	return
}

func (mp *metaPartition) updateVolView(convert func(view *proto.DataPartitionsView) *DataPartitionsView) (err error) {
	volName := mp.config.VolName
	dataView, err := masterClient.ClientAPI().GetDataPartitions(volName)
	if err != nil {
		err = fmt.Errorf("updateVolWorker: get meta partitions view fail: volume(%v) err(%v)",
			volName, err)
		log.LogErrorf(err.Error())
		return
	}
	mp.vol.UpdatePartitions(convert(dataView))
	return nil
}

func (mp *metaPartition) updateVolWorker() {
	t := time.NewTicker(UpdateVolTicket)
	var convert = func(view *proto.DataPartitionsView) *DataPartitionsView {
		newView := &DataPartitionsView{
			DataPartitions: make([]*DataPartition, len(view.DataPartitions)),
		}
		for i := 0; i < len(view.DataPartitions); i++ {
			newView.DataPartitions[i] = &DataPartition{
				PartitionID: view.DataPartitions[i].PartitionID,
				Status:      view.DataPartitions[i].Status,
				Hosts:       view.DataPartitions[i].Hosts,
				ReplicaNum:  view.DataPartitions[i].ReplicaNum,
			}
		}
		return newView
	}
	mp.updateVolView(convert)
	for {
		select {
		case <-mp.stopC:
			t.Stop()
			return
		case <-t.C:
			mp.updateVolView(convert)
		}
	}
}

const (
	MinDeleteBatchCounts = 100
	MaxSleepCnt          = 10
)

func (mp *metaPartition) deleteWorker() {
	var (
		idx      uint64
		isLeader bool
		sleepCnt uint64
	)
	buffSlice := make([]uint64, 0, DeleteBatchCount())
	for {
		buffSlice = buffSlice[:0]
		select {
		case <-mp.stopC:
			return
		default:
		}

		if _, isLeader = mp.IsLeader(); !isLeader {
			time.Sleep(AsyncDeleteInterval)
			continue
		}

		DeleteWorkerSleepMs()

		//TODO: add sleep time value
		isForceDeleted := sleepCnt%MaxSleepCnt == 0
		if !isForceDeleted && mp.freeList.Len() < MinDeleteBatchCounts {
			time.Sleep(AsyncDeleteInterval)
			sleepCnt++
			continue
		}

		batchCount := DeleteBatchCount()
		for idx = 0; idx < batchCount; idx++ {
			// batch get free inoded from the freeList
			ino := mp.freeList.Pop()
			if ino == 0 {
				break
			}
			buffSlice = append(buffSlice, ino)
		}
		mp.persistDeletedInodes(buffSlice)
		mp.deleteMarkedInodes(context.Background(), buffSlice)
		sleepCnt++
	}
}

// delete Extents by Partition,and find all successDelete inode
func (mp *metaPartition) batchDeleteExtentsByPartition(ctx context.Context, partitionDeleteExtents map[uint64][]*proto.ExtentKey,
	allInodes []*Inode) (shouldCommit []*Inode, shouldPushToFreeList []*Inode) {
	occurErrors := make(map[uint64]error)
	shouldCommit = make([]*Inode, 0, DeleteBatchCount())
	shouldPushToFreeList = make([]*Inode, 0)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)

	//wait all Partition do BatchDeleteExtents fininsh
	for partitionID, extents := range partitionDeleteExtents {
		wg.Add(1)
		go func(partitionID uint64, extents []*proto.ExtentKey) {
			perr := mp.doBatchDeleteExtentsByPartition(ctx, partitionID, extents)
			lock.Lock()
			occurErrors[partitionID] = perr
			lock.Unlock()
			wg.Done()
		}(partitionID, extents)
	}
	wg.Wait()

	//range AllNode,find all Extents delete success on inode,it must to be append shouldCommit
	for i := 0; i < len(allInodes); i++ {
		successDeleteExtentCnt := 0
		inode := allInodes[i]
		inode.Extents.Range(func(ek proto.ExtentKey) bool {
			if occurErrors[ek.PartitionId] == nil {
				successDeleteExtentCnt++
				return true
			} else {
				log.LogWarnf("deleteInode Inode(%v) error(%v)", inode.Inode, occurErrors[ek.PartitionId])
				return false
			}
		})
		if successDeleteExtentCnt == inode.Extents.Len() {
			shouldCommit = append(shouldCommit, inode)
		} else {
			shouldPushToFreeList = append(shouldPushToFreeList, inode)
		}
	}

	return
}

// Delete the marked inodes.
func (mp *metaPartition) deleteMarkedInodes(ctx context.Context, inoSlice []uint64) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf(fmt.Sprintf("metaPartition(%v) deleteMarkedInodes panic (%v)", mp.config.PartitionId, r))
		}
	}()
	shouldCommit := make([]*Inode, 0, DeleteBatchCount())
	shouldRePushToFreeList := make([]*Inode, 0)
	allDeleteExtents := make(map[string]uint64)
	deleteExtentsByPartition := make(map[uint64][]*proto.ExtentKey)
	allInodes := make([]*Inode, 0)
	for _, ino := range inoSlice {
		ref := NewDeletedInodeByID(ino)
		var inodeVal *Inode
		dio, ok := mp.inodeDeletedTree.CopyGet(ref).(*DeletedINode)
		if ok && dio != nil {
			inodeVal = dio.buildInode()
		} else {
			var inoRef Inode
			inoRef.Inode = ino
			inodeVal, ok = mp.inodeTree.CopyGet(&inoRef).(*Inode)
			if !ok || inodeVal == nil {
				log.LogWarnf("[deleteMarkedInodes], not found the deleted inode: %v", ino)
				continue
			}
		}
		inodeVal.Extents.Range(func(ek proto.ExtentKey) bool {
			ext := &ek
			_, ok := allDeleteExtents[ext.GetExtentKey()]
			if !ok {
				allDeleteExtents[ext.GetExtentKey()] = ino
			}
			exts, ok := deleteExtentsByPartition[ext.PartitionId]
			if !ok {
				exts = make([]*proto.ExtentKey, 0)
			}
			exts = append(exts, ext)
			deleteExtentsByPartition[ext.PartitionId] = exts
			return true
		})
		allInodes = append(allInodes, inodeVal)
	}
	shouldCommit, shouldRePushToFreeList = mp.batchDeleteExtentsByPartition(ctx, deleteExtentsByPartition, allInodes)
	bufSlice := make([]byte, 0, 8*len(shouldCommit))
	for _, inode := range shouldCommit {
		bufSlice = append(bufSlice, inode.MarshalKey()...)
	}
	err := mp.syncToRaftFollowersFreeInode(ctx, bufSlice)
	if err != nil {
		log.LogWarnf("[deleteInodeTreeOnRaftPeers] raft commit inode list: %v, "+
			"response %s", shouldCommit, err.Error())
	}
	for _, inode := range shouldCommit {
		if err != nil {
			mp.freeList.Push(inode.Inode)
		}
	}
	log.LogInfof("metaPartition(%v) deleteInodeCnt(%v) inodeCnt(%v)", mp.config.PartitionId, len(shouldCommit), mp.inodeTree.Len())
	for _, inode := range shouldRePushToFreeList {
		mp.freeList.Push(inode.Inode)
	}
}

func (mp *metaPartition) syncToRaftFollowersFreeInode(ctx context.Context, hasDeleteInodes []byte) (err error) {
	if len(hasDeleteInodes) == 0 {
		return
	}
	//_, err = mp.submit(opFSMInternalDeleteInode, hasDeleteInodes)
	_, err = mp.submit(ctx, opFSMInternalCleanDeletedInode, "", hasDeleteInodes)

	return
}

const (
	notifyRaftFollowerToFreeInodesTimeOut = 60 * 2
)

func (mp *metaPartition) notifyRaftFollowerToFreeInodes(ctx context.Context, wg *sync.WaitGroup, target string, hasDeleteInodes []byte) (err error) {
	var conn *net.TCPConn
	conn, err = mp.config.ConnPool.GetConnect(target)
	defer func() {
		wg.Done()
		if err != nil {
			log.LogWarnf(err.Error())
			mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
		} else {
			mp.config.ConnPool.PutConnect(conn, NoClosedConnect)
		}
	}()
	if err != nil {
		return
	}
	request := NewPacketToFreeInodeOnRaftFollower(ctx, mp.config.PartitionId, hasDeleteInodes)
	if err = request.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}

	if err = request.ReadFromConn(conn, notifyRaftFollowerToFreeInodesTimeOut); err != nil {
		return
	}

	if request.ResultCode != proto.OpOk {
		err = fmt.Errorf("request(%v) error(%v)", request.GetUniqueLogId(), string(request.Data[:request.Size]))
	}

	return
}

func (mp *metaPartition) doDeleteMarkedInodes(ctx context.Context, ext *proto.ExtentKey) (err error) {
	// get the data node view
	dp := mp.vol.GetPartition(ext.PartitionId)
	if dp == nil {
		err = errors.NewErrorf("unknown dataPartitionID=%d in vol",
			ext.PartitionId)
		return
	}
	// delete the data node
	conn, err := mp.config.ConnPool.GetConnect(dp.Hosts[0])

	defer func() {
		if err != nil {
			mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
		} else {
			mp.config.ConnPool.PutConnect(conn, NoClosedConnect)
		}
	}()

	if err != nil {
		err = errors.NewErrorf("get conn from pool %s, "+
			"extents partitionId=%d, extentId=%d",
			err.Error(), ext.PartitionId, ext.ExtentId)
		return
	}
	p := NewPacketToDeleteExtent(ctx, dp, ext)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.NewErrorf("write to dataNode %s, %s", p.GetUniqueLogId(),
			err.Error())
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		err = errors.NewErrorf("read response from dataNode %s, %s",
			p.GetUniqueLogId(), err.Error())
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("[deleteMarkedInodes] %s response: %s", p.GetUniqueLogId(),
			p.GetResultMsg())
	}
	return
}

func (mp *metaPartition) doBatchDeleteExtentsByPartition(ctx context.Context, partitionID uint64, exts []*proto.ExtentKey) (err error) {
	// get the data node view
	dp := mp.vol.GetPartition(partitionID)
	if dp == nil {
		err = errors.NewErrorf("unknown dataPartitionID=%d in vol",
			partitionID)
		return
	}
	for _, ext := range exts {
		if ext.PartitionId != partitionID {
			err = errors.NewErrorf("BatchDeleteExtent do batchDelete on PartitionID(%v) but unexpect Extent(%v)", partitionID, ext)
			return
		}
	}

	// delete the data node
	conn, err := mp.config.ConnPool.GetConnect(dp.Hosts[0])

	defer func() {
		if err != nil {
			mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
		} else {
			mp.config.ConnPool.PutConnect(conn, NoClosedConnect)
		}
	}()

	if err != nil {
		err = errors.NewErrorf("get conn from pool %s, "+
			"extents partitionId=%d",
			err.Error(), partitionID)
		return
	}
	p := NewPacketToBatchDeleteExtent(ctx, dp, exts)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.NewErrorf("write to dataNode %s, %s", p.GetUniqueLogId(),
			err.Error())
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		err = errors.NewErrorf("read response from dataNode %s, %s",
			p.GetUniqueLogId(), err.Error())
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("[deleteMarkedInodes] %s response: %s", p.GetUniqueLogId(),
			p.GetResultMsg())
	}
	return
}

func (mp *metaPartition) persistDeletedInodes(inos []uint64) {
	if fileInfo, err := mp.delInodeFp.Stat(); err == nil && fileInfo.Size() >= maxDeleteInodeFileSize {
		mp.delInodeFp.Truncate(0)
	}
	for _, ino := range inos {
		if _, err := mp.delInodeFp.WriteString(fmt.Sprintf("%v\n", ino)); err != nil {
			log.LogWarnf("[persistDeletedInodes] failed store ino=%v", ino)
		}
	}
}
