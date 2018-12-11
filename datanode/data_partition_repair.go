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

package datanode

import (
	"encoding/json"
	"net"
	"strings"
	"sync"
	"time"

	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/third_party/juju/errors"
	"github.com/tiglabs/containerfs/util/log"
	"hash/crc32"
)

/* dataPartitionRepair f

Datapartition repair process:
There are 2 types of repairs, one is normal extent repair, and the other is tinyExtent fix.
	1. Each replicate member corresponds to a DataPartitionRepairTask structure, where
       the extents member comes from all the extent information of each replicated member,
       and all the replica members' dataPartitionRepairTask are combined to be called allReplicas.

    2. Fill the DataPartitionRepairTask of all replica members, the local DataPartitionRepairTask
	   structure calls dp.extentstore.getAllWaterMark() to get the extent information of the leader member,
	  and the call to getRemoteExtentInfo on the follower gets the extent information of the follower member.

    3.The process of generating a repair task:
       a. Traverse all the members of the copy member DataPartitionRepairTask, and then for each extent,
          only find the largest extentInfo, the component maxExtentInfo structure, the key is the extendId,
           and the value is the maximum size of the extendInfo information.
       b. Traverse all the members of the copy member DataPartitionRepairTask, if there is no in the maxExtentSizeMap,
          add a task to create an extent for the AddExtentsTasks of the DataPartitionRepairTask. If the extentSize in
          the member is less than the size of the extent for the maxExtentSizeMap, then the FixExtentSizeTasks for
          the DataPartitionRepairTask Add a task to fix the extent of the extent

    4. send the DataPartitionRepairTask to all Replicates
    5. wait all dataParitionRepair Task do success
*/

type DataPartitionRepairTask struct {
	TaskType           uint8 //which type task
	addr               string
	extents            map[uint64]*storage.FileInfo //storage file on datapartion disk meta
	AddExtentsTasks    []*storage.FileInfo          //generator add extent file task
	FixExtentSizeTasks []*storage.FileInfo          //generator fixSize file task
}

func NewDataPartitionRepairTask(extentFiles []*storage.FileInfo) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		extents:            make(map[uint64]*storage.FileInfo),
		AddExtentsTasks:    make([]*storage.FileInfo, 0),
		FixExtentSizeTasks: make([]*storage.FileInfo, 0),
	}
	for _, extentFile := range extentFiles {
		task.extents[extentFile.FileId] = extentFile
	}

	return
}

//extents repair check
func (dp *dataPartition) extentFileRepair(fixExtentsType uint8) {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[extentFileRepair] partition(%v) start.",
		dp.partitionId)
	//note,if fixExtentType is TinyExtentFix,then,get unavaliTiny Extents
	unavaliTinyExtents := make([]uint64, 0)
	if fixExtentsType == proto.TinyExtentMode {
		unavaliTinyExtents = dp.getUnavaliTinyExtents()
		if len(unavaliTinyExtents) == 0 {
			return
		}
	}

	//init allReplicasDataPartitionRepairTask
	allReplicas := make([]*DataPartitionRepairTask, 0, len(dp.replicaHosts))
	err := dp.fillDataPartitionRepairTask(allReplicas, fixExtentsType, unavaliTinyExtents)
	if err != nil {
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionId, err)
		log.LogErrorf(errors.ErrorStack(err))
		dp.putTinyExtentToUnavliCh(fixExtentsType, unavaliTinyExtents)
		return
	}

	//compare allReplicasDataPartition extents,compute needFixExtents and NoNeedFixExtents
	noNeedFix, needFix := dp.generatorExtentRepairTasks(allReplicas) //generator file repair task

	//notify all followers,do DataPartitionRepairTask on followers
	err = dp.NotifyExtentRepair(allReplicas) //notify host to fix it
	if err != nil {
		dp.putAllTinyExtentsToStore(fixExtentsType, noNeedFix, needFix)
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionId, err)
		log.LogError(errors.ErrorStack(err))
		return
	}

	//Leader do dataParition repair task
	dp.LeaderDoDataPartitionRepairTask(allReplicas)
	finishTime := time.Now().UnixNano()

	//put AvalitinyExtents and UnavaliTinyExtents to store
	dp.putAllTinyExtentsToStore(fixExtentsType, noNeedFix, needFix)

	//check store avaliTinyExtents and unavaliTinyExtents sum
	if dp.extentStore.GetAvaliExtentLen()+dp.extentStore.GetUnAvaliExtentLen() > storage.TinyExtentCount {
		log.LogWarnf("action[extentFileRepair] partition(%v) AvaliTinyExtents(%v) "+
			"UnavaliTinyExtents(%v) finish cost[%vms].", dp.partitionId, dp.extentStore.GetAvaliExtentLen(),
			dp.extentStore.GetUnAvaliExtentLen(), (finishTime-startTime)/int64(time.Millisecond))
	}
	log.LogInfof("action[extentFileRepair] partition(%v) AvaliTinyExtents(%v) UnavaliTinyExtents(%v)"+
		" finish cost[%vms].", dp.partitionId, dp.extentStore.GetAvaliExtentLen(), dp.extentStore.GetUnAvaliExtentLen(),
		(finishTime-startTime)/int64(time.Millisecond))

}

// fill all replicasDataPartitionRepairTask
func (dp *dataPartition) fillDataPartitionRepairTask(replicas []*DataPartitionRepairTask, fixExtentMode uint8, needFixExtents []uint64) (err error) {
	//get leaderStore all extentInfo
	leaderExtents, err := dp.getLocalExtentInfo(fixExtentMode, needFixExtents)
	if err != nil {
		return err
	}
	//new Leader DataPartitionRepairTask
	replicas[0] = NewDataPartitionRepairTask(leaderExtents)
	replicas[0].addr = dp.replicaHosts[0]

	// new Follower DataPartitionRepair Task
	for index := 1; index < len(dp.replicaHosts); index++ {
		extents, err := dp.getRemoteExtentInfo(fixExtentMode, needFixExtents, dp.replicaHosts[index])
		if err != nil {
			return err
		}
		replicas[index] = NewDataPartitionRepairTask(extents)
		replicas[index].addr = dp.replicaHosts[index]
	}

	return
}

// Depending on the type of repair, call store GetAllWaterMark
func (dp *dataPartition) getLocalExtentInfo(fixExtentMode uint8, needFixExtents []uint64) (extentFiles []*storage.FileInfo, err error) {
	extentFiles = make([]*storage.FileInfo, 0)
	// get local extent file metas
	if fixExtentMode == proto.NormalExtentMode {
		extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter())
	} else {
		extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableTinyExtentFilter(needFixExtents))
	}
	if err != nil {
		err = errors.Annotatef(err, "getLocalExtentInfo extent dataPartition(%v) GetAllWaterMark", dp.partitionId)
		return
	}
	return
}

// call followers get extent all watner marker ,about all extents meta
func (dp *dataPartition) getRemoteExtentInfo(fixExtentMode uint8, needFixExtents []uint64, target string) (extentFiles []*storage.FileInfo, err error) {
	extentFiles = make([]*storage.FileInfo, 0)
	p := NewExtentStoreGetAllWaterMarker(dp.partitionId, fixExtentMode)
	if fixExtentMode == proto.TinyExtentMode {
		p.Data, err = json.Marshal(needFixExtents)
		if err != nil {
			err = errors.Annotatef(err, "getRemoteExtentInfo dataPartition(%v) GetAllWaterMark", dp.partitionId)
			return
		}
		p.Size = uint32(len(p.Data))
	}
	var conn *net.TCPConn
	conn, err = gConnPool.Get(target) //get remote connect
	if err != nil {
		err = errors.Annotatef(err, "getRemoteExtentInfo  dataPartition(%v) get host(%v) connect", dp.partitionId, target)
		return
	}
	err = p.WriteToConn(conn) //write command to remote host
	if err != nil {
		gConnPool.Put(conn, true)
		err = errors.Annotatef(err, "getRemoteExtentInfo dataPartition(%v) write to host(%v)", dp.partitionId, target)
		return
	}
	reply := new(Packet)
	err = reply.ReadFromConn(conn, 60) //read it response
	if err != nil {
		gConnPool.Put(conn, true)
		err = errors.Annotatef(err, "getRemoteExtentInfo dataPartition(%v) read from host(%v)", dp.partitionId, target)
		return
	}
	err = json.Unmarshal(reply.Data[:reply.Size], &extentFiles)
	if err != nil {
		gConnPool.Put(conn, true)
		err = errors.Annotatef(err, "getRemoteExtentInfo dataPartition(%v) unmarshal json(%v)", dp.partitionId, string(p.Data[:p.Size]))
		return
	}
	gConnPool.Put(conn, true)
	return
}

func (dp *dataPartition) LeaderDoDataPartitionRepairTask(allReplicas []*DataPartitionRepairTask) {
	store := dp.extentStore
	for _, addExtentFile := range allReplicas[0].AddExtentsTasks {
		store.Create(addExtentFile.FileId, addExtentFile.Inode)
	}
	for _, fixExtentFile := range allReplicas[0].FixExtentSizeTasks {
		dp.streamRepairExtent(fixExtentFile) //fix leader filesize
	}
}

func (dp *dataPartition) putTinyExtentToUnavliCh(fixExtentType uint8, extents []uint64) {
	if fixExtentType == proto.TinyExtentMode {
		dp.extentStore.PutTinyExtentsToUnAvaliCh(extents)
	}
	return
}

func (dp *dataPartition) putAllTinyExtentsToStore(fixExtentType uint8, noNeedFix, needFix []uint64) {
	if fixExtentType != proto.TinyExtentMode {
		return
	}
	for _, extentId := range noNeedFix {
		if storage.IsTinyExtent(extentId) {
			dp.extentStore.PutTinyExtentToAvaliCh(extentId)
		}
	}
	for _, extentId := range needFix {
		if storage.IsTinyExtent(extentId) {
			dp.extentStore.PutTinyExtentToUnavaliCh(extentId)
		}
	}
}

func (dp *dataPartition) getUnavaliTinyExtents() (unavaliTinyExtents []uint64) {
	unavaliTinyExtents = make([]uint64, 0)
	fixTinyExtents := MinFixTinyExtents
	if dp.isFirstFixTinyExtents {
		fixTinyExtents = storage.TinyExtentCount
		dp.isFirstFixTinyExtents = false
	}
	for i := 0; i < fixTinyExtents; i++ {
		extentId, err := dp.extentStore.GetUnavaliTinyExtent()
		if err != nil {
			return
		}
		unavaliTinyExtents = append(unavaliTinyExtents, extentId)
	}
	return
}

//generator file task
func (dp *dataPartition) generatorExtentRepairTasks(allReplicas []*DataPartitionRepairTask) (noNeedFixExtents []uint64, needFixExtents []uint64) {
	maxSizeExtentMap := dp.getSizeMaxExtentMap(allReplicas)    //map maxSize extentId to allMembers index
	dp.generatorAddExtentsTasks(allReplicas, maxSizeExtentMap) //add extentTask
	noNeedFixExtents, needFixExtents = dp.generatorFixExtentSizeTasks(allReplicas, maxSizeExtentMap)
	return
}

/* pasre all extent,select maxExtentSize to member index map
 */
func (dp *dataPartition) getSizeMaxExtentMap(allReplicas []*DataPartitionRepairTask) (maxSizeExtentMap map[uint64]*storage.FileInfo) {
	maxSizeExtentMap = make(map[uint64]*storage.FileInfo)
	for index := 0; index < len(allReplicas); index++ {
		member := allReplicas[index]
		for extentId, extentInfo := range member.extents {
			maxFileInfo, ok := maxSizeExtentMap[extentId]
			if !ok {
				maxSizeExtentMap[extentId] = extentInfo
			} else {
				orgInode := maxSizeExtentMap[extentId].Inode
				if extentInfo.Size > maxFileInfo.Size {
					if extentInfo.Inode == 0 && orgInode != 0 {
						extentInfo.Inode = orgInode
					}
					maxSizeExtentMap[extentId] = extentInfo
				}
			}
		}
	}
	return
}

/*generator add extent if follower not have this extent*/
func (dp *dataPartition) generatorAddExtentsTasks(allReplicas []*DataPartitionRepairTask, maxSizeExtentMap map[uint64]*storage.FileInfo) {
	for extentId, maxExtentInfo := range maxSizeExtentMap {
		if storage.IsTinyExtent(extentId) {
			continue
		}
		for index := 0; index < len(allReplicas); index++ {
			follower := allReplicas[index]
			if _, ok := follower.extents[extentId]; !ok && maxExtentInfo.Deleted == false {
				if maxExtentInfo.Inode == 0 {
					continue
				}
				addFile := &storage.FileInfo{Source: maxExtentInfo.Source, FileId: extentId, Size: maxExtentInfo.Size, Inode: maxExtentInfo.Inode}
				follower.AddExtentsTasks = append(follower.AddExtentsTasks, addFile)
				follower.FixExtentSizeTasks = append(follower.FixExtentSizeTasks, addFile)
				log.LogInfof("action[generatorAddExtentsTasks] partition(%v) addFile(%v) on Index(%v).", dp.partitionId, addFile, index)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorFixExtentSizeTasks(allMembers []*DataPartitionRepairTask, maxSizeExtentMap map[uint64]*storage.FileInfo) (noNeedFix []uint64, needFix []uint64) {
	noNeedFix = make([]uint64, 0)
	needFix = make([]uint64, 0)
	for fileId, maxFileInfo := range maxSizeExtentMap {
		isFix := true
		for index := 0; index < len(allMembers); index++ {
			extentInfo, ok := allMembers[index].extents[fileId]
			if !ok {
				continue
			}
			if extentInfo.Size < maxFileInfo.Size {
				fixExtent := &storage.FileInfo{Source: maxFileInfo.Source, FileId: fileId, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				allMembers[index].FixExtentSizeTasks = append(allMembers[index].FixExtentSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] partition(%v) fixExtent(%v).", dp.partitionId, fixExtent)
				isFix = false
			}
			if maxFileInfo.Inode != 0 && extentInfo.Inode == 0 {
				fixExtent := &storage.FileInfo{Source: maxFileInfo.Source, FileId: fileId, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				allMembers[index].FixExtentSizeTasks = append(allMembers[index].FixExtentSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] partition(%v) Modify Ino fixExtent(%v).", dp.partitionId, fixExtent)
			}
		}
		if storage.IsTinyExtent(fileId) {
			if isFix {
				noNeedFix = append(noNeedFix, fileId)
			} else {
				needFix = append(needFix, fileId)
			}
		}
	}
	return
}

/*notify follower to repair dataPartition extentStore*/
func (dp *dataPartition) NotifyExtentRepair(members []*DataPartitionRepairTask) (err error) {
	var wg sync.WaitGroup
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := NewNotifyExtentRepair(dp.partitionId) //notify all follower to repairt task,send opnotifyRepair command
			var conn *net.TCPConn
			target := dp.replicaHosts[index]
			conn, err = gConnPool.Get(target)
			if err != nil {
				return
			}
			p.Data, err = json.Marshal(members[index])
			p.Size = uint32(len(p.Data))
			if err = p.WriteToConn(conn); err != nil {
				gConnPool.Put(conn, true)
				return
			}
			if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
				gConnPool.Put(conn, true)
				return
			}
			gConnPool.Put(conn, true)
		}(i)
	}
	wg.Wait()

	return
}

/*notify follower to repair dataPartition extentStore*/
func (dp *dataPartition) NotifyRaftFollowerRepair(members *DataPartitionRepairTask) (err error) {
	var wg sync.WaitGroup

	for i := 0; i < len(dp.replicaHosts); i++ {
		replicaAddr := dp.replicaHosts[i]
		replicaAddrParts := strings.Split(replicaAddr, ":")
		if strings.TrimSpace(replicaAddrParts[0]) == LocalIP {
			continue //local is leader not need send notify repair
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := NewNotifyExtentRepair(dp.partitionId)
			var conn *net.TCPConn
			target := dp.replicaHosts[index]
			conn, err = gConnPool.Get(target)
			if err != nil {
				return
			}
			p.Data, err = json.Marshal(members)
			p.Size = uint32(len(p.Data))
			err = p.WriteToConn(conn)
			if err != nil {
				gConnPool.Put(conn, true)
				return
			}

			if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
				gConnPool.Put(conn, true)
				return
			}
			gConnPool.Put(conn, true)
		}(i)
	}
	wg.Wait()

	return
}

// DoStreamExtentFixRepair executed on follower node of data partition.
// It receive from leader notifyRepair command extent file repair.
func (dp *dataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.FileInfo) {
	defer wg.Done()

	err := dp.streamRepairExtent(remoteExtentInfo)

	if err != nil {
		err = errors.Annotatef(err, "doStreamExtentFixRepair %v", dp.applyRepairKey(int(remoteExtentInfo.FileId)))
		localExtentInfo, opErr := dp.GetStore().GetWatermark(uint64(remoteExtentInfo.FileId), false)
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "partition(%v) remote(%v) local(%v)",
			dp.partitionId, remoteExtentInfo, localExtentInfo)
		log.LogErrorf("action[doStreamExtentFixRepair] err(%v).", err)
	}
}

func (dp *dataPartition) applyRepairKey(fileId int) (m string) {
	return fmt.Sprintf("ApplyRepairKey(%v_%v)", dp.ID(), fileId)
}

//extent file repair function,do it on follower host
func (dp *dataPartition) streamRepairExtent(remoteExtentInfo *storage.FileInfo) (err error) {
	store := dp.GetStore()
	if !store.IsExistExtent(remoteExtentInfo.FileId) {
		return
	}

	// Get local extent file info
	localExtentInfo, err := store.GetWatermark(remoteExtentInfo.FileId, false)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}

	// Get need fix size for this extent file
	needFixSize := remoteExtentInfo.Size - localExtentInfo.Size

	// Create streamRead packet, it offset is local extentInfoSize, size is needFixSize
	request := NewExtentRepairReadPacket(dp.ID(), remoteExtentInfo.FileId, int(localExtentInfo.Size), int(needFixSize))
	var conn *net.TCPConn

	// Get a connection to leader host
	conn, err = gConnPool.Get(remoteExtentInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	defer gConnPool.Put(conn, true)

	// Write OpStreamRead command to leader
	if err = request.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "streamRepairExtent send streamRead to host[%v] error", remoteExtentInfo.Source)
		log.LogErrorf("action[streamRepairExtent] err[%v].", err)
		return
	}
	currFixOffset := localExtentInfo.Size
	for currFixOffset < remoteExtentInfo.Size {
		// If local extent size has great remoteExtent file size ,then break
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}
		localExtentInfo, err = store.GetWatermark(remoteExtentInfo.FileId, false)
		if err != nil {
			err = errors.Annotatef(err, "streamRepairExtent GetWatermark error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return err
		}
		if localExtentInfo.Size > currFixOffset {
			err = errors.Annotatef(err, "streamRepairExtent unavali fix localSize(%v) "+
				"remoteSize(%v) want fixOffset(%v) data error", localExtentInfo.Size, remoteExtentInfo.Size, currFixOffset)
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return err
		}

		reply := NewPacket()
		// Read 64k stream repair packet
		if err = reply.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent receive data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Annotatef(err, "streamRepairExtent receive opcode error(%v) ", string(reply.Data[:reply.Size]))
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID || reply.FileID != request.FileID {
			err = errors.Annotatef(err, "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}

		log.LogInfof("action[streamRepairExtent] partition(%v) extent(%v) start fix from (%v)"+
			" remoteSize(%v) localSize(%v).", dp.ID(), remoteExtentInfo.FileId,
			remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset)

		if reply.Crc != crc32.ChecksumIEEE(reply.Data[:reply.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch partition(%v) extent(%v) start fix from (%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v)", dp.ID(), remoteExtentInfo.FileId,
				remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return errors.Annotatef(err, "streamRepairExtent receive data error")
		}
		// Write it to local extent file
		if storage.IsTinyExtent(uint64(localExtentInfo.FileId)) {
			err = store.WriteTinyRecover(uint64(localExtentInfo.FileId), int64(currFixOffset), int64(reply.Size), reply.Data, reply.Crc)
		} else {
			err = store.Write(uint64(localExtentInfo.FileId), int64(currFixOffset), int64(reply.Size), reply.Data, reply.Crc)
		}
		if err != nil {
			err = errors.Annotatef(err, "streamRepairExtent repair data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}
		currFixOffset += uint64(reply.Size)

	}
	return

}
