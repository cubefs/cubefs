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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"net"
	"os"
	"path"
	"sync"
	"time"
)

const (
	AsyncDeleteInterval      = 10 * time.Second
	UpdateVolTicket          = 5 * time.Minute
	BatchCounts              = 500
	OpenRWAppendOpt          = os.O_CREATE | os.O_RDWR | os.O_APPEND
	TempFileValidTime        = 86400 //units: sec
	DeleteInodeFileExtension = "INODE_DEL"
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
	dataView, err := masterClient.ClientAPI().GetDataPartitions(mp.config.VolName)
	if err == nil {
		mp.vol.UpdatePartitions(convert(dataView))
	}

	for {
		select {
		case <-mp.stopC:
			t.Stop()
			return
		case <-t.C:
			volName := mp.config.VolName
			dataView, err := masterClient.ClientAPI().GetDataPartitions(volName)
			if err != nil {
				log.LogErrorf("updateVolWorker: get data partitions view fail: volume(%v) err(%v)",
					volName, err)
				break
			}
			mp.vol.UpdatePartitions(convert(dataView))
		}
	}
}

func (mp *metaPartition) deleteWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]uint64, 0, BatchCounts)
Begin:
	for {
		time.Sleep(AsyncDeleteInterval)
		log.LogInfof("Start deleteWorker: partition(%v)", mp.config.PartitionId)
		buffSlice = buffSlice[:0]
		select {
		case <-mp.stopC:
			return
		default:
		}
		if _, isLeader = mp.IsLeader(); !isLeader {
			goto Begin
		}
		for idx = 0; idx < BatchCounts; idx++ {
			// batch get free inoded from the freeList
			ino := mp.freeList.Pop()
			if ino == 0 {
				break
			}
			buffSlice = append(buffSlice, ino)
			log.LogInfof("deleteWorker: found an orphan inode: ino(%v)", ino)
		}
		mp.persistDeletedInodes(buffSlice)
		mp.deleteMarkedInodes(buffSlice)
		log.LogInfof("Finish deleteWorker: partition(%v)", mp.config.PartitionId)
	}
}

// delete Extents by Partition,and find all successDelete inode
func (mp *metaPartition) batchDeleteExtentsByPartition(partitionDeleteExtents map[uint64][]*proto.ExtentKey, allInodes []*Inode) (shouldCommit []*Inode) {
	occurErrors := make(map[uint64]error)
	shouldCommit = make([]*Inode, 0, BatchCounts)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)

	//wait all Partition do BatchDeleteExtents fininsh
	for partitionID, extents := range partitionDeleteExtents {
		wg.Add(1)
		go func(partitionID uint64, extents []*proto.ExtentKey) {
			perr := mp.doBatchDeleteExtents(partitionID, extents)
			lock.Lock()
			occurErrors[partitionID] = perr
			lock.Unlock()
			log.LogWarnf("deleteExtents on Partition(%v) error(%v)", partitionID, perr)
			wg.Done()
		}(partitionID, extents)
	}
	wg.Wait()

	//range AllNode,find all Extents delete success on inode,it must to be append shouldCommit
	for i := 0; i < len(allInodes); i++ {
		successDeleteExtentCnt := 0
		inode := allInodes[i]
		inode.Extents.Range(func(item BtreeItem) bool {
			ext := item.(*proto.ExtentKey)
			if occurErrors[ext.PartitionId] == nil {
				successDeleteExtentCnt++
				return true
			} else {
				log.LogWarnf("deleteInode Inode(%v) error(%v)", inode.Inode, occurErrors[ext.PartitionId])
				return false
			}
		})
		if successDeleteExtentCnt == inode.Extents.Len() {
			shouldCommit = append(shouldCommit, inode)
		}
	}

	return
}

// Delete the marked inodes.
func (mp *metaPartition) deleteMarkedInodes(inoSlice []uint64) {
	shouldCommit := make([]*Inode, 0, BatchCounts)
	allDeleteExtents := make(map[string]uint64)
	deleteExtentsByPartition := make(map[uint64][]*proto.ExtentKey)
	allInodes := make([]*Inode, 0)

	for _, ino := range inoSlice {
		ref := &Inode{Inode: ino}
		inode := mp.inodeTree.CopyGet(ref).(*Inode)
		inode.Extents.Range(func(item BtreeItem) bool {
			ext := item.(*proto.ExtentKey)
			_, ok := allDeleteExtents[ext.GetExtentKey()]
			if !ok {
				allDeleteExtents[ext.GetExtentKey()] = inode.Inode
			}
			exts, ok := deleteExtentsByPartition[ext.PartitionId]
			if !ok {
				exts = make([]*proto.ExtentKey, 0)
			}
			exts = append(exts, ext)
			deleteExtentsByPartition[ext.PartitionId] = exts
			return true
		})
		allInodes = append(allInodes, inode)
	}
	shouldCommit = mp.batchDeleteExtentsByPartition(deleteExtentsByPartition, allInodes)
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
		log.LogDebugf("[deleteInodeTree] inode list: %v , err(%v)", shouldCommit, err)
	}
}

func (mp *metaPartition) syncToRaftFollowersFreeInode(hasDeleteInodes []byte) (err error) {
	raftPeers := mp.GetPeers()
	raftPeersError := make([]error, len(raftPeers))
	wg := new(sync.WaitGroup)
	for index, target := range raftPeers {
		wg.Add(1)
		raftPeersError[index] = mp.notifyRaftFollowerToFreeInodes(wg, target, hasDeleteInodes)
	}
	wg.Wait()
	for index := 0; index < len(raftPeersError); index++ {
		if raftPeersError[index] != nil {
			err = raftPeersError[index]
			return
		}
	}

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

func (mp *metaPartition) doBatchDeleteExtents(partitionID uint64, exts []*proto.ExtentKey) (err error) {
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
	p := NewPacketToBatchDeleteExtent(dp, exts)
	if err = p.WriteToConn(conn); err != nil {
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
	for _, ino := range inos {
		if _, err := mp.delInodeFp.WriteString(fmt.Sprintf("%v\n", ino)); err != nil {
			log.LogWarnf("[persistDeletedInodes] failed store ino=%v", ino)
		}
	}
}
