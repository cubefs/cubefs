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
	BatchCounts              = 100
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

// Delete the marked inodes.
func (mp *metaPartition) deleteMarkedInodes(inoSlice []uint64) {
	var wg sync.WaitGroup
	var mu sync.Mutex

	shouldCommit := make([]*Inode, 0, BatchCounts)

	for _, ino := range inoSlice {
		wg.Add(1)

		ref := &Inode{Inode: ino}
		inode := mp.inodeTree.CopyGet(ref).(*Inode)

		go func(i *Inode) {
			defer wg.Done()

			var dirtyExt []*proto.ExtentKey

			i.Extents.Range(func(item BtreeItem) bool {
				ext := item.(*proto.ExtentKey)
				if err := mp.doDeleteMarkedInodes(ext); err != nil {
					dirtyExt = append(dirtyExt, ext)
					log.LogWarnf("[deleteMarkedInodes] delete failed extents: ino(%v) ext(%s), err(%s)", i.Inode, ext.String(), err.Error())
				}
				log.LogInfof("[deleteMarkedInodes] inode(%v) extent(%v)", i.Inode, ext.String())
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

func (mp *metaPartition) persistDeletedInodes(inos []uint64) {
	for _, ino := range inos {
		if _, err := mp.delInodeFp.WriteString(fmt.Sprintf("%v\n", ino)); err != nil {
			log.LogWarnf("[persistDeletedInodes] failed store ino=%v", ino)
		}
	}
}
