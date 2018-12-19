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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
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
	extents            map[uint64]*storage.ExtentInfo //storage file on datapartion disk meta
	AddExtentsTasks    []*storage.ExtentInfo          //generator add extent file task
	FixExtentSizeTasks []*storage.ExtentInfo          //generator fixSize file task
}

func NewDataPartitionRepairTask(extentFiles []*storage.ExtentInfo) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		extents:            make(map[uint64]*storage.ExtentInfo),
		AddExtentsTasks:    make([]*storage.ExtentInfo, 0),
		FixExtentSizeTasks: make([]*storage.ExtentInfo, 0),
	}
	for _, extentFile := range extentFiles {
		task.extents[extentFile.FileID] = extentFile
	}

	return
}

//extents repair check
func (dp *DataPartition) extentFileRepair(fixExtentsType uint8) {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[extentFileRepair] partition(%v) start.",
		dp.partitionID)
	//note,if fixExtentType is TinyExtentFix,then,get unavaliTiny Extents
	var unavaliTinyExtents []uint64
	if fixExtentsType == proto.TinyExtentMode {
		unavaliTinyExtents = dp.getUnavaliTinyExtents()
		if len(unavaliTinyExtents) == 0 {
			return
		}
	}

	//init allReplicasDataPartitionRepairTask
	allReplicas := make([]*DataPartitionRepairTask, len(dp.replicaHosts))
	err := dp.fillDataPartitionRepairTask(allReplicas, fixExtentsType, unavaliTinyExtents)
	if err != nil {
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionID, err)
		log.LogErrorf(errors.ErrorStack(err))
		dp.putTinyExtentToUnavliCh(fixExtentsType, unavaliTinyExtents)
		return
	}

	//compare allReplicasDataPartition extents,compute needFixExtents and NoNeedFixExtents
	noNeedFix, needFix := dp.generatorExtentRepairTasks(allReplicas) //generator file repair task

	// Notify backup members to repair DataPartition extent
	err = dp.NotifyExtentRepair(allReplicas) //notify host to fix it
	if err != nil {
		dp.putAllTinyExtentsToStore(fixExtentsType, noNeedFix, needFix)
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionID, err)
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
			"UnavaliTinyExtents(%v) finish cost[%vms].", dp.partitionID, dp.extentStore.GetAvaliExtentLen(),
			dp.extentStore.GetUnAvaliExtentLen(), (finishTime-startTime)/int64(time.Millisecond))
	}
	log.LogInfof("action[extentFileRepair] partition(%v) AvaliTinyExtents(%v) UnavaliTinyExtents(%v)"+
		" finish cost[%vms].", dp.partitionID, dp.extentStore.GetAvaliExtentLen(), dp.extentStore.GetUnAvaliExtentLen(),
		(finishTime-startTime)/int64(time.Millisecond))

}

// fill all replicasDataPartitionRepairTask
func (dp *DataPartition) fillDataPartitionRepairTask(replicas []*DataPartitionRepairTask, fixExtentMode uint8, needFixExtents []uint64) (err error) {
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
func (dp *DataPartition) getLocalExtentInfo(fixExtentMode uint8, needFixExtents []uint64) (extentFiles []*storage.ExtentInfo, err error) {
	extentFiles = make([]*storage.ExtentInfo, 0)
	// get local extent file metas
	if fixExtentMode == proto.NormalExtentMode {
		extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter())
	} else {
		extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableTinyExtentFilter(needFixExtents))
	}
	if err != nil {
		err = errors.Annotatef(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
		return
	}
	return
}

// call followers get extent all watner marker ,about all extents meta
func (dp *DataPartition) getRemoteExtentInfo(fixExtentMode uint8, needFixExtents []uint64, target string) (extentFiles []*storage.ExtentInfo, err error) {
	extentFiles = make([]*storage.ExtentInfo, 0)
	p := repl.NewGetAllWaterMarker(dp.partitionID, fixExtentMode)
	if fixExtentMode == proto.TinyExtentMode {
		p.Data, err = json.Marshal(needFixExtents)
		if err != nil {
			err = errors.Annotatef(err, "getRemoteExtentInfo DataPartition(%v) GetAllWaterMark", dp.partitionID)
			return
		}
		p.Size = uint32(len(p.Data))
	}
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Annotatef(err, "getRemoteExtentInfo  DataPartition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	err = p.WriteToConn(conn) //write command to remote host
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getRemoteExtentInfo DataPartition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	reply := new(repl.Packet)
	err = reply.ReadFromConn(conn, 60) //read it response
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getRemoteExtentInfo DataPartition(%v) read from host(%v)", dp.partitionID, target)
		return
	}
	err = json.Unmarshal(reply.Data[:reply.Size], &extentFiles)
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getRemoteExtentInfo DataPartition(%v) unmarshal json(%v)", dp.partitionID, string(p.Data[:p.Size]))
		return
	}
	gConnPool.PutConnect(conn, true)
	return
}

func (dp *DataPartition) LeaderDoDataPartitionRepairTask(allReplicas []*DataPartitionRepairTask) {
	store := dp.extentStore
	for _, addExtentFile := range allReplicas[0].AddExtentsTasks {
		store.Create(addExtentFile.FileID, addExtentFile.Inode)
	}
	for _, fixExtentFile := range allReplicas[0].FixExtentSizeTasks {
		dp.streamRepairExtent(fixExtentFile) //fix leader filesize
	}
}

func (dp *DataPartition) putTinyExtentToUnavliCh(fixExtentType uint8, extents []uint64) {
	if fixExtentType == proto.TinyExtentMode {
		dp.extentStore.PutTinyExtentsToUnAvaliCh(extents)
	}
	return
}

func (dp *DataPartition) putAllTinyExtentsToStore(fixExtentType uint8, noNeedFix, needFix []uint64) {
	if fixExtentType != proto.TinyExtentMode {
		return
	}
	for _, extentID := range noNeedFix {
		if storage.IsTinyExtent(extentID) {
			dp.extentStore.PutTinyExtentToAvaliCh(extentID)
		}
	}
	for _, extentID := range needFix {
		if storage.IsTinyExtent(extentID) {
			dp.extentStore.PutTinyExtentToUnavaliCh(extentID)
		}
	}
}

func (dp *DataPartition) getUnavaliTinyExtents() (unavaliTinyExtents []uint64) {
	unavaliTinyExtents = make([]uint64, 0)
	fixTinyExtents := MinFixTinyExtents
	if dp.isFirstFixTinyExtents {
		fixTinyExtents = storage.TinyExtentCount
		dp.isFirstFixTinyExtents = false
	}
	for i := 0; i < fixTinyExtents; i++ {
		extentID, err := dp.extentStore.GetUnavaliTinyExtent()
		if err != nil {
			return
		}
		unavaliTinyExtents = append(unavaliTinyExtents, extentID)
	}
	return
}

//generator file task
func (dp *DataPartition) generatorExtentRepairTasks(allReplicas []*DataPartitionRepairTask) (noNeedFixExtents []uint64, needFixExtents []uint64) {
	maxSizeExtentMap := dp.getSizeMaxExtentMap(allReplicas)    //map maxSize extentID to allMembers index
	dp.generatorAddExtentsTasks(allReplicas, maxSizeExtentMap) //add extentTask
	noNeedFixExtents, needFixExtents = dp.generatorFixExtentSizeTasks(allReplicas, maxSizeExtentMap)
	return
}

/* pasre all extent,select maxExtentSize to member index map
 */
func (dp *DataPartition) getSizeMaxExtentMap(allReplicas []*DataPartitionRepairTask) (maxSizeExtentMap map[uint64]*storage.ExtentInfo) {
	maxSizeExtentMap = make(map[uint64]*storage.ExtentInfo)
	for index := 0; index < len(allReplicas); index++ {
		member := allReplicas[index]
		for extentID, extentInfo := range member.extents {
			maxFileInfo, ok := maxSizeExtentMap[extentID]
			if !ok {
				maxSizeExtentMap[extentID] = extentInfo
			} else {
				orgInode := maxSizeExtentMap[extentID].Inode
				if extentInfo.Size > maxFileInfo.Size {
					if extentInfo.Inode == 0 && orgInode != 0 {
						extentInfo.Inode = orgInode
					}
					maxSizeExtentMap[extentID] = extentInfo
				}
			}
		}
	}
	return
}

/*generator add extent if follower not have this extent*/
func (dp *DataPartition) generatorAddExtentsTasks(allReplicas []*DataPartitionRepairTask, maxSizeExtentMap map[uint64]*storage.ExtentInfo) {
	for extentID, maxExtentInfo := range maxSizeExtentMap {
		if storage.IsTinyExtent(extentID) {
			continue
		}
		for index := 0; index < len(allReplicas); index++ {
			follower := allReplicas[index]
			if _, ok := follower.extents[extentID]; !ok && maxExtentInfo.Deleted == false {
				if maxExtentInfo.Inode == 0 {
					continue
				}
				addFile := &storage.ExtentInfo{Source: maxExtentInfo.Source, FileID: extentID, Size: maxExtentInfo.Size, Inode: maxExtentInfo.Inode}
				follower.AddExtentsTasks = append(follower.AddExtentsTasks, addFile)
				follower.FixExtentSizeTasks = append(follower.FixExtentSizeTasks, addFile)
				log.LogInfof("action[generatorAddExtentsTasks] partition(%v) addFile(%v) on Index(%v).", dp.partitionID, addFile, index)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *DataPartition) generatorFixExtentSizeTasks(allMembers []*DataPartitionRepairTask, maxSizeExtentMap map[uint64]*storage.ExtentInfo) (noNeedFix []uint64, needFix []uint64) {
	noNeedFix = make([]uint64, 0)
	needFix = make([]uint64, 0)
	for extentID, maxFileInfo := range maxSizeExtentMap {
		isFix := true
		for index := 0; index < len(allMembers); index++ {
			extentInfo, ok := allMembers[index].extents[extentID]
			if !ok {
				continue
			}
			if extentInfo.Size < maxFileInfo.Size {
				fixExtent := &storage.ExtentInfo{Source: maxFileInfo.Source, FileID: extentID, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				allMembers[index].FixExtentSizeTasks = append(allMembers[index].FixExtentSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] partition(%v) fixExtent(%v).", dp.partitionID, fixExtent)
				isFix = false
			}
			if maxFileInfo.Inode != 0 && extentInfo.Inode == 0 {
				fixExtent := &storage.ExtentInfo{Source: maxFileInfo.Source, FileID: extentID, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				allMembers[index].FixExtentSizeTasks = append(allMembers[index].FixExtentSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] partition(%v) Modify Ino fixExtent(%v).", dp.partitionID, fixExtent)
			}
		}
		if storage.IsTinyExtent(extentID) {
			if isFix {
				noNeedFix = append(noNeedFix, extentID)
			} else {
				needFix = append(needFix, extentID)
			}
		}
	}
	return
}

// NotifyExtentRepair notify backup members to repair DataPartition extent
func (dp *DataPartition) NotifyExtentRepair(members []*DataPartitionRepairTask) (err error) {
	var wg sync.WaitGroup
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := repl.NewNotifyExtentRepair(dp.partitionID) //notify all follower to repairt task,send opnotifyRepair command
			var conn *net.TCPConn
			target := dp.replicaHosts[index]
			conn, err = gConnPool.GetConnect(target)
			if err != nil {
				return
			}
			p.Data, err = json.Marshal(members[index])
			p.Size = uint32(len(p.Data))
			if err = p.WriteToConn(conn); err != nil {
				gConnPool.PutConnect(conn, true)
				return
			}
			if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
				gConnPool.PutConnect(conn, true)
				return
			}
			gConnPool.PutConnect(conn, true)
		}(i)
	}
	wg.Wait()

	return
}

// NotifyRaftFollowerRepair notify raft follower to repair DataPartition extent*/
func (dp *DataPartition) NotifyRaftFollowerRepair(members *DataPartitionRepairTask) (err error) {
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
			p := repl.NewNotifyExtentRepair(dp.partitionID)
			var conn *net.TCPConn
			target := dp.replicaHosts[index]
			conn, err = gConnPool.GetConnect(target)
			if err != nil {
				return
			}
			p.Data, err = json.Marshal(members)
			p.Size = uint32(len(p.Data))
			err = p.WriteToConn(conn)
			if err != nil {
				gConnPool.PutConnect(conn, true)
				return
			}

			if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
				gConnPool.PutConnect(conn, true)
				return
			}
			gConnPool.PutConnect(conn, true)
		}(i)
	}
	wg.Wait()

	return
}

// DoStreamExtentFixRepair executed on follower node of data partition.
// It receive from leader notifyRepair command extent file repair.
func (dp *DataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.ExtentInfo) {
	defer wg.Done()

	err := dp.streamRepairExtent(remoteExtentInfo)

	if err != nil {
		err = errors.Annotatef(err, "doStreamExtentFixRepair %v", dp.applyRepairKey(int(remoteExtentInfo.FileID)))
		localExtentInfo, opErr := dp.GetStore().GetWatermark(uint64(remoteExtentInfo.FileID), false)
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "partition(%v) remote(%v) local(%v)",
			dp.partitionID, remoteExtentInfo, localExtentInfo)
		log.LogErrorf("action[doStreamExtentFixRepair] err(%v).", err)
	}
}

func (dp *DataPartition) applyRepairKey(extentID int) (m string) {
	return fmt.Sprintf("ApplyRepairKey(%v_%v)", dp.ID(), extentID)
}

//extent file repair function,do it on follower host
func (dp *DataPartition) streamRepairExtent(remoteExtentInfo *storage.ExtentInfo) (err error) {
	store := dp.GetStore()
	if !store.IsExistExtent(remoteExtentInfo.FileID) {
		return
	}

	defer func() {
		store.GetWatermark(remoteExtentInfo.FileID, true)
	}()

	// GetConnect local extent file info
	localExtentInfo, err := store.GetWatermark(remoteExtentInfo.FileID, true)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}

	// GetConnect need fix size for this extent file
	needFixSize := remoteExtentInfo.Size - localExtentInfo.Size

	// Create streamRead packet, it offset is local extentInfoSize, size is needFixSize
	request := repl.NewExtentRepairReadPacket(dp.ID(), remoteExtentInfo.FileID, int(localExtentInfo.Size), int(needFixSize))
	var conn *net.TCPConn

	// GetConnect a connection to leader host
	conn, err = gConnPool.GetConnect(remoteExtentInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	defer gConnPool.PutConnect(conn, true)

	// Write OpStreamRead command to leader
	if err = request.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "streamRepairExtent send streamRead to host[%v] error", remoteExtentInfo.Source)
		log.LogErrorf("action[streamRepairExtent] err[%v].", err)
		return
	}
	currFixOffset := localExtentInfo.Size
	for currFixOffset < remoteExtentInfo.Size {
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}
		reply := repl.NewPacket()
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

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentID != request.ExtentID || reply.Size == 0 || reply.ExtentOffset != int64(currFixOffset) {
			err = errors.Annotatef(err, "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}

		log.LogInfof("action[streamRepairExtent] partition(%v) extent(%v) start fix from (%v)"+
			" remoteSize(%v) localSize(%v) reply(%v).", dp.ID(), remoteExtentInfo.FileID,
			remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, reply.GetUniqueLogId())

		if reply.CRC != crc32.ChecksumIEEE(reply.Data[:reply.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch partition(%v) extent(%v) start fix from (%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v)", dp.ID(), remoteExtentInfo.FileID,
				remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return errors.Annotatef(err, "streamRepairExtent receive data error")
		}
		// Write it to local extent file
		if storage.IsTinyExtent(uint64(localExtentInfo.FileID)) {
			err = store.TinyExtentRecover(uint64(localExtentInfo.FileID), int64(currFixOffset), int64(reply.Size), reply.Data, reply.CRC)
		} else {
			err = store.Write(uint64(localExtentInfo.FileID), int64(currFixOffset), int64(reply.Size), reply.Data, reply.CRC)
		}
		if err != nil {
			err = errors.Annotatef(err, "streamRepairExtent repair data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}
		currFixOffset += uint64(reply.Size)
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}

	}
	return

}
