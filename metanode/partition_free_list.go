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
	AsyncDeleteInterval      = 10 * time.Second
	UpdateVolTicket          = 2 * time.Minute
	BatchCounts              = 500
	OpenRWAppendOpt          = os.O_CREATE | os.O_RDWR | os.O_APPEND
	TempFileValidTime        = 86400 //units: sec
	DeleteInodeFileExtension = "INODE_DEL"
	DeleteWorkerCnt          = 10
)

var (
	flMu             sync.RWMutex
	deleteBatchCount = uint64(BatchCounts)
)

func DeleteBatchCount() (batchCount uint64) {
	flMu.RLock()
	flMu.RUnlock()
	return deleteBatchCount
}

func SetDeleteBatchCount(batchCount uint64) {
	flMu.Lock()
	deleteBatchCount = batchCount
	flMu.Unlock()
}

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

func (mp *metaPartition)updateVolView(convert func(view *proto.DataPartitionsView) *DataPartitionsView)(err error) {
	volName := mp.config.VolName
	dataView, err := masterClient.ClientAPI().GetDataPartitions(volName)
	if err != nil {
		err=fmt.Errorf("updateVolWorker: get data partitions view fail: volume(%v) err(%v)",
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
		idx      int
		isLeader bool
	)
	buffSlice := make([]uint64, 0, DeleteBatchCount())
	var sleepCnt uint64
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
		isForceDeleted := sleepCnt%MaxSleepCnt == 0
		if !isForceDeleted && mp.freeList.Len() < MinDeleteBatchCounts {
			time.Sleep(AsyncDeleteInterval)
			sleepCnt++
			continue
		}

		batchCount := DeleteBatchCount()
		for idx = 0; idx < int(batchCount); idx++ {
			// batch get free inoded from the freeList
			ino := mp.freeList.Pop()
			if ino == 0 {
				break
			}
			buffSlice = append(buffSlice, ino)
		}
		mp.persistDeletedInodes(buffSlice)
		mp.deleteMarkedInodes(buffSlice)
		sleepCnt++
	}
}

// Delete the marked inodes.
func (mp *metaPartition) deleteMarkedInodes(inoSlice []uint64) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf(fmt.Sprintf("metaPartition(%v) deleteMarkedInodes panic (%v)", mp.config.PartitionId, r))
		}
	}()
	shouldCommit := make([]*Inode, 0, BatchCounts)

	for _, ino := range inoSlice {
		wg.Add(1)

		ref := &Inode{Inode: ino}
		inode, ok := mp.inodeTree.CopyGet(ref).(*Inode)
		if !ok {
			continue
		}
		go func(i *Inode) {
			defer wg.Done()

			var dirtyExt []*proto.ExtentKey

			i.Extents.Range(func(item BtreeItem) bool {
				ext := item.(*proto.ExtentKey)
				if err := mp.doDeleteMarkedInodes(ext); err != nil {
					dirtyExt = append(dirtyExt, ext)
					log.LogWarnf("[deleteMarkedInodes] delete failed extents: ino(%v) ext(%s), err(%s)", i.Inode, ext.String(), err.Error())
				}
				return true
			})
			if len(dirtyExt) == 0 {
				mu.Lock()
				shouldCommit = append(shouldCommit, i)
				mu.Unlock()
			} else {
				mp.freeList.Push(i.Inode)
			}
		}(inode)
	}

	wg.Wait()

	if len(shouldCommit) > 0 {
		bufSlice := make([]byte, 0, 8*len(shouldCommit))
		for _, inode := range shouldCommit {
			bufSlice = append(bufSlice, inode.MarshalKey()...)
		}
		err := mp.syncToRaftFollowersFreeInode(bufSlice)
		if err != nil {
			log.LogWarnf("[deleteInodeTreeOnRaftPeers] raft commit inode list: %v, "+
				"response %s", shouldCommit, err.Error())
		}
		for _, inode := range shouldCommit {
			if err == nil {
				mp.internalDeleteInode(inode)
			} else {
				mp.freeList.Push(inode.Inode)
			}
		}
		log.LogInfof("metaPartition(%v) deleteInodeCnt(%v) inodeCnt(%v)", mp.config.PartitionId, len(shouldCommit), mp.inodeTree.Len())
	}
}

func (mp *metaPartition) syncToRaftFollowersFreeInode(hasDeleteInodes []byte) (err error) {
	_, err = mp.submit(opFSMInternalDeleteInode, hasDeleteInodes)

	return
}

func (mp *metaPartition) notifyRaftFollowerToFreeInodes(wg *sync.WaitGroup, target string, hasDeleteInodes []byte) (err error) {
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
	request := NewPacketToFreeInodeOnRaftFollower(mp.config.PartitionId, hasDeleteInodes)
	if err = request.WriteToConn(conn); err != nil {
		return
	}

	if err = request.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
		return
	}

	if request.ResultCode != proto.OpOk {
		err = fmt.Errorf("request(%v) error(%v)", request.GetUniqueLogId(), string(request.Data[:request.Size]))
	}

	return
}

func (mp *metaPartition) doDeleteMarkedInodes(ext *proto.ExtentKey) (err error) {
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
	p := NewPacketToDeleteExtent(dp, ext)
	if err = p.WriteToConn(conn); err != nil {
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

func (mp *metaPartition) persistDeletedInodes(inos []uint64) {
	for _, ino := range inos {
		if _, err := mp.delInodeFp.WriteString(fmt.Sprintf("%v\n", ino)); err != nil {
			log.LogWarnf("[persistDeletedInodes] failed store ino=%v", ino)
		}
	}
}
