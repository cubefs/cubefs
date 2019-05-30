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
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"net"
	"os"
	"path"
	"runtime"
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
	go mp.checkFreelistWorker()
	mp.startToDeleteExtents()
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
			respBody, err := masterHelper.Request("GET", reqURL, nil, nil)
			if err != nil {
				log.LogErrorf("[updateVol] %s", err.Error())
				break
			}

			dataView := NewDataPartitionsView()
			if err = json.Unmarshal(respBody, dataView); err != nil {
				log.LogErrorf("[updateVol] %s", err.Error())
				break
			}
			mp.vol.UpdatePartitions(dataView)
		}
	}
}

func (mp *metaPartition) deleteWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]*Inode, 0, BatchCounts)
	var tempFileSlice []*Inode
Begin:
	time.Sleep(AsyncDeleteInterval)
	for {
		buffSlice = buffSlice[:0]
		tempFileSlice = tempFileSlice[:0]
		select {
		case <-mp.stopC:
			return
		default:
		}
		if _, isLeader = mp.IsLeader(); !isLeader {
			goto Begin
		}
		curTime := Now.GetCurrentTime().Unix()
		for idx = 0; idx < BatchCounts; idx++ {
			// batch get free inoded from the freeList
			ino := mp.freeList.Pop()
			if ino == nil {
				break
			}
			// this is orphan or temp inode, we should delay delete
			if !ino.ShouldDelete() && ino.GetNLink() == 0 {
				if (curTime-ino.ModifyTime) <= TempFileValidTime || (curTime-ino.AccessTime) <= TempFileValidTime || (curTime-ino.CreateTime) <= TempFileValidTime {
					tempFileSlice = append(tempFileSlice, ino)
					continue
				}
			}
			buffSlice = append(buffSlice, ino)
		}
		for _, ino := range tempFileSlice {
			mp.freeList.Push(ino)
		}
		mp.persistDeletedInodes(buffSlice...)
		mp.deleteMarkedInodes(buffSlice)
		if len(buffSlice)+len(tempFileSlice) < BatchCounts {
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
			mp.persistDeletedInodes(ino)
		}

	}
}

// Delete the marked inodes.
func (mp *metaPartition) deleteMarkedInodes(inoSlice []*Inode) {
	shouldCommit := make([]*Inode, 0, BatchCounts)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, ino := range inoSlice {
		wg.Add(1)
		i := ino
		go func(ino *Inode) {
			defer wg.Done()
			var reExt []*proto.ExtentKey
			ino.Extents.Range(func(item BtreeItem) bool {
				ext := item.(*proto.ExtentKey)
				if err := mp.doDeleteMarkedInodes(ext); err != nil {
					reExt = append(reExt, ext)
					log.LogWarnf("[deleteMarkedInodes] delete failed extents: %s, err: %s", ext.String(), err.Error())
				}
				return true
			})
			if len(reExt) == 0 {
				mu.Lock()
				shouldCommit = append(shouldCommit, ino)
				mu.Unlock()
			} else {
				newIno := NewInode(ino.Inode, ino.Type)
				for _, ext := range reExt {
					newIno.Extents.Append(ext)
				}
				mp.freeList.Push(newIno)
			}
		}(i)
	}
	wg.Wait()
	mu.Lock()
	cnt := len(shouldCommit)
	mu.Unlock()
	if cnt > 0 {
		bufSlice := make([]byte, 0, 8*len(shouldCommit))
		for _, ino := range shouldCommit {
			bufSlice = append(bufSlice, ino.MarshalKey()...)
		}
		err := mp.syncToRaftFollowersFreeInode(bufSlice)
		if err != nil {
			for _, ino := range shouldCommit {
				mp.freeList.Push(ino)
			}
			log.LogWarnf("[deleteInodeTreeOnRaftPeers] raft commit inode list: %v, "+
				"response %s", shouldCommit, err.Error())
		}
		log.LogDebugf("[deleteInodeTree] inode list: %v", shouldCommit)
	}

}

func (mp *metaPartition) freeInodesOnRaftFollower(inodes []byte) {

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
	log.LogDebugf("[deleteMarkedInodes] %v", p.GetUniqueLogId())
	return
}

func (mp *metaPartition) persistDeletedInodes(inos ...*Inode) {
	for _, ino := range inos {
		if _, err := mp.delInodeFp.Write(ino.MarshalKey()); err != nil {
			log.LogWarnf("[persistDeletedInodes] failed store inode=%d", ino.Inode)
			mp.freeList.Push(ino)
		}
	}
}
