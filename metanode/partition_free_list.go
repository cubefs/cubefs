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
	"math/rand"
	"net"
	"os"
	"path"
	"runtime/debug"
	"sync"
	"sync/atomic"
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
	mp.renameDeleteEKRecordFile(delExtentKeyList, prefixDelExtentKeyListBackup)
	mp.renameDeleteEKRecordFile(InodeDelExtentKeyList, PrefixInodeDelExtentKeyListBackup)
	delExtentListDir := path.Join(mp.config.RootDir, delExtentKeyList)
	if mp.delEKFd, err = os.OpenFile(delExtentListDir, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err != nil {
		log.LogErrorf("[startFreeList] mp[%v] create delEKListFile(%s) failed:%v",
			mp.config.PartitionId, delExtentListDir, err)
		return
	}

	inodeDelEKListDir := path.Join(mp.config.RootDir, InodeDelExtentKeyList)
	if mp.inodeDelEkFd, err = os.OpenFile(inodeDelEKListDir, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err != nil {
		log.LogErrorf("[startFreeList] mp[%v] create inodeDelEKListFile(%s) failed:%v",
			mp.config.PartitionId, inodeDelEKListDir, err)
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
	rand.Seed(time.Now().Unix())
	time.Sleep(time.Duration(rand.Intn(int(UpdateVolTicket))))
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
				EcHosts:     view.DataPartitions[i].EcHosts,
				EcMigrateStatus: view.DataPartitions[i].EcMigrateStatus,
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

func (mp *metaPartition) GetDelInodeInterval() uint64{
	interval := mp.manager.getDelInodeInterval(mp.config.VolName)
	clusterWaitValue := atomic.LoadUint64(&deleteWorkerSleepMs)
	if interval == 0 {
		interval = clusterWaitValue
	}
	return interval
}

func (mp *metaPartition) GetBatchDelInodeCnt() uint64{
	clusterDelCnt := DeleteBatchCount()   // default 128
	batchDelCnt := mp.manager.getBatchDelInodeCnt(mp.config.VolName)
	if batchDelCnt == 0 {
		batchDelCnt = clusterDelCnt
	}

	return batchDelCnt
}

func (mp *metaPartition) deleteWorker() {
	var (
		//idx      uint64
		isLeader   bool
		sleepCnt   uint64
		leaderTerm uint64
	)
	for {
		select {
		case <-mp.stopC:
			if mp.delInodeFp != nil {
				mp.delInodeFp.Sync()
				mp.delInodeFp.Close()
			}

			if mp.inodeDelEkFd != nil {
				mp.inodeDelEkFd.Sync()
				mp.inodeDelEkFd.Close()
			}
			return
		default:
		}

		if _, isLeader = mp.IsLeader(); !isLeader {
			time.Sleep(AsyncDeleteInterval)
			continue
		}

		//DeleteWorkerSleepMs()
		interval := mp.GetDelInodeInterval()
		time.Sleep(time.Duration(interval) * time.Millisecond)

		//TODO: add sleep time value
		isForceDeleted := sleepCnt%MaxSleepCnt == 0
		if !isForceDeleted && mp.freeList.Len() < MinDeleteBatchCounts {
			time.Sleep(AsyncDeleteInterval)
			sleepCnt++
			continue
		}

		batchCount := mp.GetBatchDelInodeCnt()
		buffSlice := mp.freeList.Get(int(batchCount))
		mp.persistDeletedInodes(buffSlice)
		leaderTerm, isLeader = mp.LeaderTerm()
		if !isLeader {
			time.Sleep(AsyncDeleteInterval)
			continue
		}
		mp.deleteMarkedInodes(context.Background(), buffSlice, leaderTerm)
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
		if inode.Extents == nil || inode.Extents.Len() == 0 {
			shouldCommit = append(shouldCommit, inode)
			continue
		}
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

func (mp *metaPartition) recordInodeDeleteEkInfo(info *Inode) {
	if info == nil || info.Extents == nil ||info.Extents.Len() == 0 {
		return
	}

	log.LogDebugf("[recordInodeDeleteEkInfo] mp[%v] delEk[ino:%v] record %d eks", mp.config.PartitionId, info.Inode, info.Extents.Len())
	var (
		data []byte
		err  error
	)
	inoDelExtentListDir := path.Join(mp.config.RootDir, InodeDelExtentKeyList)
	if mp.inodeDelEkRecordCount >= defMaxDelEKRecord {
		_ = mp.inodeDelEkFd.Close()
		mp.inodeDelEkFd = nil
		mp.renameDeleteEKRecordFile(InodeDelExtentKeyList, PrefixInodeDelExtentKeyListBackup)
		mp.inodeDelEkRecordCount = 0
	}
	timeStamp := time.Now().Unix()

	info.Extents.Range(func(ek proto.ExtentKey) bool {

		delEk := ek.ConvertToMetaDelEk(info.Inode, uint64(proto.DelEkSrcTypeFromDelInode), timeStamp)
		ekBuff := make([]byte, proto.ExtentDbKeyLengthWithIno)
		delEk.MarshalDeleteEKRecord(ekBuff)
		data = append(data, ekBuff...)
		mp.inodeDelEkRecordCount++
		return true
	})

	if mp.inodeDelEkFd == nil {
		if mp.inodeDelEkFd, err = os.OpenFile(inoDelExtentListDir, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err != nil {
			log.LogErrorf("[recordInodeDeleteEkInfo] mp[%v] delEk[ino:%v, ek:%v] create delEKListFile(%s) failed:%v",
				mp.config.PartitionId, info.Inode, info.Extents.Len(), InodeDelExtentKeyList, err)
			return
		}
	}

	defer func() {
		if err != nil {
			_ = mp.inodeDelEkFd.Close()
			mp.inodeDelEkFd = nil
		}
	}()

	if _, err = mp.inodeDelEkFd.Write(data); err != nil {
		log.LogErrorf("[recordInodeDeleteEkInfo] mp[%v] delEk[ino:%v, ek:%v] write file(%s) failed:%v",
			mp.config.PartitionId, info.Inode, info.Extents.Len(), inoDelExtentListDir, err)
		return
	}
	log.LogDebugf("[recordInodeDeleteEkInfo] mp[%v] delEk[ino:%v, ek:%v] record success", mp.config.PartitionId, info.Inode, info.Extents.Len())
	return
}

// Delete the marked inodes.
func (mp *metaPartition) deleteMarkedInodes(ctx context.Context, inoSlice []uint64, term uint64) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf(fmt.Sprintf("metaPartition(%v) deleteMarkedInodes panic (%v), stack:%v", mp.config.PartitionId, r, debug.Stack()))
		}
	}()
	shouldCommit := make([]*Inode, 0, DeleteBatchCount())
	//shouldRePushToFreeList := make([]*Inode, 0)
	allDeleteExtents := make(map[string]uint64)
	deleteExtentsByPartition := make(map[uint64][]*proto.ExtentKey)
	allInodes := make([]*Inode, 0)
	for _, ino := range inoSlice {
		var inodeVal *Inode
		dio, err := mp.inodeDeletedTree.Get(ino)
		if err != nil {
			log.LogWarnf("[deleteMarkedInodes], not found the deleted inode: %v", ino)
			continue
		}
		if dio == nil || !dio.IsExpired {
			//unexpected, just for avoid mistake delete
			mp.freeList.Remove(ino)
			log.LogWarnf("[deleteMarkedInodes], unexpired deleted inode: %v", ino)
			continue
		}
		inodeVal = dio.buildInode()
		//if err == nil && dio != nil {
		//	inodeVal = dio.buildInode()
		//} else {
		//	inodeVal, err = mp.inodeTree.Get(ino)
		//	if err != nil || inodeVal == nil {
		//		log.LogWarnf("[deleteMarkedInodes], not found the deleted inode: %v", ino)
		//		continue
		//	}
		//}
		if inodeVal.Extents == nil || inodeVal.Extents.Len() == 0 {
			allInodes = append(allInodes, inodeVal)
			continue
		}

		mp.recordInodeDeleteEkInfo(inodeVal)

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
	shouldCommit, _ = mp.batchDeleteExtentsByPartition(ctx, deleteExtentsByPartition, allInodes)
	bufSlice := make([]byte, 0, 8*len(shouldCommit))
	for _, inode := range shouldCommit {
		bufSlice = append(bufSlice, inode.MarshalKey()...)
	}

	leaderTerm, isLeader := mp.LeaderTerm()
	if !isLeader || leaderTerm != term {
		log.LogErrorf("[deleteMarkedInodes] partitionID(%v) leader change", mp.config.PartitionId)
		return
	}

	err := mp.syncToRaftFollowersFreeInode(ctx, bufSlice)
	if err != nil {
		log.LogWarnf("[deleteInodeTreeOnRaftPeers] raft commit inode list: %v, "+
			"response %s", shouldCommit, err.Error())
	}
	//for _, inode := range shouldCommit {
	//	if err != nil {
	//		//mp.freeList.Push(inode.Inode)
	//	}
	//}
	log.LogInfof("metaPartition(%v) deleteInodeCnt(%v) inodeCnt(%v)", mp.config.PartitionId, len(shouldCommit), mp.inodeTree.Count())
	//for _, inode := range shouldRePushToFreeList {
	//	//mp.freeList.Push(inode.Inode)
	//}
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

	//delete the ec node
	if proto.IsEcFinished(dp.EcMigrateStatus) {
		err = mp.doDeleteEcMarkedInodes(ctx, dp, ext)
		return
	}else if dp.EcMigrateStatus == proto.Migrating {
		err = errors.NewErrorf("dp(%v) is migrate Ec, wait done", dp.PartitionID)
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

	// delete the ec node
	//delete the ec node
	if proto.IsEcFinished(dp.EcMigrateStatus) {
		err = mp.doBatchDeleteEcExtentsByPartition(ctx, dp, exts)
		return
	}else if dp.EcMigrateStatus == proto.Migrating {
		err = errors.NewErrorf("dp(%v) is migrate Ec, wait done", dp.PartitionID)
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

func (mp *metaPartition) doDeleteEcMarkedInodes(ctx context.Context, dp *DataPartition, ext *proto.ExtentKey) (err error) {
	// delete the data node
	conn, err := mp.config.ConnPool.GetConnect(dp.EcHosts[0])

	defer func() {
		if err != nil {
			log.LogError(err)
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
	p := NewPacketToDeleteEcExtent(ctx, dp, ext)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.NewErrorf("write to ecNode %s, %s", p.GetUniqueLogId(),
			err.Error())
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		err = errors.NewErrorf("read response from ecNode %s, %s",
			p.GetUniqueLogId(), err.Error())
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("[deleteEcMarkedInodes] %s response: %s", p.GetUniqueLogId(),
			p.GetResultMsg())
	}
	return
}

func (mp *metaPartition) doBatchDeleteEcExtentsByPartition(ctx context.Context, dp *DataPartition, exts []*proto.ExtentKey) (err error) {
	var (
		conn *net.TCPConn
	)
	conn, err = mp.config.ConnPool.GetConnect(dp.EcHosts[0])
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
			err.Error(), dp.PartitionID)
		return
	}
	p := NewPacketToBatchDeleteEcExtent(ctx, dp, exts)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.NewErrorf("write to ecNode %s, %s", p.GetUniqueLogId(),
			err.Error())
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		err = errors.NewErrorf("read response from ecNode %s, %s",
			p.GetUniqueLogId(), err.Error())
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("[deleteEcMarkedInodes] %s response: %s", p.GetUniqueLogId(),
			p.GetResultMsg())
	}
	return
}
