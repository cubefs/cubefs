// Copyright 2018 The Containerfs Authors.
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
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"os"
	"path"
	"runtime"
	"time"
)

const (
	AsyncDeleteInterval      = 10 * time.Second
	UpdateVolTicket          = 5 * time.Minute
	BatchCounts              = 100
	OpenRWAppendOpt          = os.O_CREATE | os.O_RDWR | os.O_APPEND
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
	go mp.checkFreelistWorker()
	mp.startDeleteExtents()
	return
}

func (mp *metaPartition) updateVolWorker() {
	t := time.NewTicker(UpdateVolTicket)
	reqURL := fmt.Sprintf("%s?name=%s", proto.ClientDataPartitions, mp.config.VolName)
	for {
		select {
		case <-mp.stopC:
			t.Stop()
			return
		case <-t.C:
			// GetConnect dataPartitionView
			respBody, err := masterHelper.Request("GET", reqURL, nil, nil)
			if err != nil {
				log.LogErrorf("[updateVol] %s", err.Error())
				break
			}
			dataView := new(DataPartitionsView)
			if err = json.Unmarshal(respBody, dataView); err != nil {
				log.LogErrorf("[updateVol] %s", err.Error())
				break
			}
			mp.vol.UpdatePartitions(dataView)
			log.LogDebugf("[updateVol] %v", dataView)
		}
	}
}

func (mp *metaPartition) deleteWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]*Inode, 0, BatchCounts)
Begin:
	time.Sleep(AsyncDeleteInterval)
	for {
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
			// batch get free inode from freeList
			ino := mp.freeList.Pop()
			if ino == nil {
				break
			}
			buffSlice = append(buffSlice, ino)
		}
		if len(buffSlice) == 0 {
			goto Begin
		}
		mp.storeDeletedInode(buffSlice...)
		mp.deleteDataPartitionMark(buffSlice)
		if len(buffSlice) < BatchCounts {
			goto Begin
		}
		runtime.Gosched()
	}
}

func (mp *metaPartition) checkFreelistWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]*Inode, 0, BatchCounts)
	for {
		time.Sleep(time.Second)
		buffSlice = buffSlice[:0]
		select {
		case <-mp.stopC:
			return
		default:
		}
		if _, isLeader = mp.IsLeader(); isLeader {
			continue
		}
		for idx = 0; idx < BatchCounts; idx++ {
			// get the first item of list
			ino := mp.freeList.Pop()
			if ino == nil {
				break
			}
			buffSlice = append(buffSlice, ino)
		}
		if len(buffSlice) == 0 {
			continue
		}
		for _, ino := range buffSlice {
			if mp.internalHasInode(ino) {
				mp.freeList.Push(ino)
				continue
			}
			mp.storeDeletedInode(ino)
		}

	}
}

func (mp *metaPartition) deleteDataPartitionMark(inoSlice []*Inode) {
	shouldCommit := make([]*Inode, 0, BatchCounts)
	var err error
	for _, ino := range inoSlice {
		var reExt []*proto.ExtentKey

		ino.Extents.Range(func(item BtreeItem) bool {
			ext := item.(*proto.ExtentKey)
			if err = mp.executeDeleteExtent(ext); err != nil {
				reExt = append(reExt, ext)
				log.LogWarnf("[deleteDataPartitionMark] delete failed extents: %s, err: %s", ext.String(), err.Error())
			}
			return true
		})
		if len(reExt) == 0 {
			shouldCommit = append(shouldCommit, ino)
		} else {
			newIno := NewInode(ino.Inode, ino.Type)
			for _, ext := range reExt {
				newIno.Extents.Append(ext)
			}
			mp.freeList.Push(newIno)
		}
	}
	if len(shouldCommit) > 0 {
		bufSlice := make([]byte, 0, 8*len(shouldCommit))
		for _, ino := range shouldCommit {
			bufSlice = append(bufSlice, ino.MarshalKey()...)
		}
		// raft Commit
		_, err := mp.Put(opFSMInternalDeleteInode, bufSlice)
		if err != nil {
			for _, ino := range shouldCommit {
				mp.freeList.Push(ino)
			}
			log.LogWarnf("[deleteInodeTree] raft commit inode list: %v, "+
				"response %s", shouldCommit, err.Error())
		}
		log.LogDebugf("[deleteInodeTree] inode list: %v", shouldCommit)
	}

}

func (mp *metaPartition) executeDeleteExtent(ext *proto.ExtentKey) (err error) {
	// get dataNode View
	dp := mp.vol.GetPartition(ext.PartitionId)
	if dp == nil {
		err = errors.Errorf("unknown dataPartitionID=%d in vol",
			ext.PartitionId)
		return
	}
	// delete dataNode
	conn, err := mp.config.ConnPool.GetConnect(dp.Hosts[0])
	defer mp.config.ConnPool.PutConnect(conn, ForceCloseConnect)

	if err != nil {
		err = errors.Errorf("get conn from pool %s, "+
			"extents partitionId=%d, extentId=%d",
			err.Error(), ext.PartitionId, ext.ExtentId)
		return
	}
	p := NewExtentDeletePacket(dp, ext)
	if err = p.WriteToConn(conn); err != nil {
		err = errors.Errorf("write to dataNode %s, %s", p.GetUniqueLogId(),
			err.Error())
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		err = errors.Errorf("read response from dataNode %s, %s",
			p.GetUniqueLogId(), err.Error())
		return
	}
	log.LogDebugf("[deleteDataPartitionMark] %v", p.GetUniqueLogId())
	return
}

func (mp *metaPartition) storeDeletedInode(inos ...*Inode) {
	for _, ino := range inos {
		if _, err := mp.delInodeFp.Write(ino.MarshalKey()); err != nil {
			log.LogWarnf("[storeDeletedInode] failed store inode=%d", ino.Inode)
			mp.freeList.Push(ino)
		}
	}
}
