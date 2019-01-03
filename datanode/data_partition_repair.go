// Copyright 2018 The Container File System Authors.
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
/*
 数据修复
 数据修复任务分为大文件的normal extent和小文件的tiny extent两种类型
 1. Normal extent修复流程
    由partition成员中的主定时收集各个成员的extent信息，经过比较获得最大的extent大小。
    定时检查本地的extent大小，如果小于最大Size，加入待修复列表，生成修复任务。
 2. Tiny extent修复流程
    在新建partition时，将所有tiny extent加入到待修复extent列表中。由修复任务将所有的tiny extent创建出来；
    由partition成员中的主定时收集各个成员的extent信息，经过比较获得最大的extent大小。
    定时检查本地的extent大小，如果小于最大Size，加入待修复列表，生成修复任务。
*/

// TODO can we just call it "repairTask"?
type DataPartitionRepairTask struct {
	TaskType            uint8
	addr                string
	extents             map[uint64]*storage.ExtentInfo
	ExtentsToBeCreated  []*storage.ExtentInfo
	ExtentsToBeRepaired []*storage.ExtentInfo
}

func NewDataPartitionRepairTask(extentFiles []*storage.ExtentInfo, source string) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		extents:             make(map[uint64]*storage.ExtentInfo),
		ExtentsToBeCreated:  make([]*storage.ExtentInfo, 0),
		ExtentsToBeRepaired: make([]*storage.ExtentInfo, 0),
	}
	for _, extentFile := range extentFiles {
		extentFile.Source = source
		task.extents[extentFile.FileID] = extentFile
	}

	return
}

// Main function to perform the repair.
func (dp *DataPartition) repair(extentType uint8) {
	start := time.Now().UnixNano() // TODO is it ok to put the start here?
	log.LogInfof("action[repair] partition(%v) start.",
		dp.partitionID)

	var tinyExtents []uint64
	if extentType == proto.TinyExtentType {
		tinyExtents = dp.badTinyExtents()
		if len(tinyExtents) == 0 {
			return
		}
	}

	// TODO why not put the following two lines into a function called "createDataPartitionRepairTask"?
	repairTasks := make([]*DataPartitionRepairTask, len(dp.replicas))
	err := dp.buildDataPartitionRepairTask(repairTasks, extentType, tinyExtents)
	if err != nil {
		log.LogErrorf("action[repair] partition(%v) err(%v).",
			dp.partitionID, err)
		log.LogErrorf(errors.ErrorStack(err))
		dp.moveToBadTinyExtentC(extentType, tinyExtents)
		return
	}

	// compare all the extents in the replicas to compute the good and bad ones
	goodTinyExtents, badTinyExtents := dp.prepareRepairTasks(repairTasks)

	// notify the replicas to repair the extent
	err = dp.NotifyExtentRepair(repairTasks)
	if err != nil {
		dp.sendAllTinyExtentsToC(extentType, goodTinyExtents, badTinyExtents)
		log.LogErrorf("action[repair] partition(%v) err(%v).",
			dp.partitionID, err)
		log.LogError(errors.ErrorStack(err))
		return
	}

	// ask the leader to do the repair
	dp.DoRepair(repairTasks)
	end := time.Now().UnixNano()

	// TODO explain why we need to send all the tiny extents to the channel here
	dp.sendAllTinyExtentsToC(extentType, goodTinyExtents, badTinyExtents)

	// TODO explain what does this check mean
	if dp.extentStore.GoodTinyExtentCnt() + dp.extentStore.BadTinyExtentCnt() > storage.TinyExtentCount {
		log.LogWarnf("action[repair] partition(%v) GoodTinyExtents(%v) "+
			"BadTinyExtents(%v) finish cost[%vms].", dp.partitionID, dp.extentStore.GoodTinyExtentCnt(),
			dp.extentStore.BadTinyExtentCnt(), (end - start) / int64(time.Millisecond))
	}

	log.LogInfof("action[repair] partition(%v) GoodTinyExtents(%v) BadTinyExtents(%v)"+
		" finish cost[%vms].", dp.partitionID, dp.extentStore.GoodTinyExtentCnt(), dp.extentStore.BadTinyExtentCnt(),
		(end - start) / int64(time.Millisecond))
	log.LogInfof("action[extentFileRepair] partition(%v) end.",
		dp.partitionID)
}

func (dp *DataPartition) buildDataPartitionRepairTask(repairTasks []*DataPartitionRepairTask, extentType uint8, tinyExtents []uint64) (err error) {
	// get the local extent info
	extents, err := dp.getLocalExtentInfo(extentType, tinyExtents)
	if err != nil {
		return err
	}
	// new repair task for the leader
	repairTasks[0] = NewDataPartitionRepairTask(extents, dp.replicas[0])
	repairTasks[0].addr = dp.replicas[0]

	// new repair tasks for the followers
	for index := 1; index < len(dp.replicas); index++ {
		extents, err := dp.getRemoteExtentInfo(extentType, tinyExtents, dp.replicas[index])
		if err != nil {
			return err
		}
		repairTasks[index] = NewDataPartitionRepairTask(extents, dp.replicas[index])
		repairTasks[index].addr = dp.replicas[index]
	}

	return
}

func (dp *DataPartition) getLocalExtentInfo(extentType uint8, tinyExtents []uint64) (extents []*storage.ExtentInfo, err error) {
	extents = make([]*storage.ExtentInfo, 0)

	if extentType == proto.NormalExtentType {
		extents, err = dp.extentStore.GetAllWatermarks(storage.GetStableExtentFilter())
	} else {
		extents, err = dp.extentStore.GetAllWatermarks(storage.GetStableTinyExtentFilter(tinyExtents))
	}
	if err != nil {
		err = errors.Annotatef(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
		return
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfo(extentType uint8, tinyExtents []uint64, target string) (extentFiles []*storage.ExtentInfo, err error) {
	extentFiles = make([]*storage.ExtentInfo, 0)
	p := repl.NewPacketToGetAllWatermarks(dp.partitionID, extentType)
	if extentType == proto.TinyExtentType {
		p.Data, err = json.Marshal(tinyExtents)
		if err != nil {
			err = errors.Annotatef(err, "getRemoteExtentInfo DataPartition(%v) GetAllWatermarks", dp.partitionID)
			return
		}
		p.Size = uint32(len(p.Data))
	}
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(target) // get remote connection
	if err != nil {
		err = errors.Annotatef(err, "getRemoteExtentInfo  DataPartition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	err = p.WriteToConn(conn) // write command to the remote host
	if err != nil {
		gConnPool.PutConnect(conn, true)
		err = errors.Annotatef(err, "getRemoteExtentInfo DataPartition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	reply := new(repl.Packet)
	err = reply.ReadFromConn(conn, 60) // read the response
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

// DoRepair asks the leader to perform the repair tasks.
func (dp *DataPartition) DoRepair(repairTasks []*DataPartitionRepairTask) {
	store := dp.extentStore
	for _, extentInfo := range repairTasks[0].ExtentsToBeCreated {
		store.Create(extentInfo.FileID, extentInfo.Inode)
	}
	for _, extentInfo := range repairTasks[0].ExtentsToBeRepaired {
		dp.streamRepairExtent(extentInfo)
	}
}

func (dp *DataPartition) moveToBadTinyExtentC(extentType uint8, extents []uint64) {
	if extentType == proto.TinyExtentType {
		dp.extentStore.SendAllToBadTinyExtentC(extents)
	}
	return
}

func (dp *DataPartition) sendAllTinyExtentsToC(extentType uint8, goodTinyExtents, badTinyExtents []uint64) {
	if extentType != proto.TinyExtentType {
		return
	}
	for _, extentID := range goodTinyExtents {
		if storage.IsTinyExtent(extentID) {
			dp.extentStore.SendToGoodTinyExtentC(extentID)
		}
	}
	for _, extentID := range badTinyExtents {
		if storage.IsTinyExtent(extentID) {
			dp.extentStore.SendToBadTinyExtentC(extentID)
		}
	}
}

func (dp *DataPartition) badTinyExtents() (badTinyExtents []uint64) {
	badTinyExtents = make([]uint64, 0)
	fixTinyExtents := MinFixTinyExtents
	if dp.isFirstFixTinyExtents {
		fixTinyExtents = storage.TinyExtentCount
		dp.isFirstFixTinyExtents = false
	}
	if dp.extentStore.BadTinyExtentCnt() == 0 {
		fixTinyExtents = storage.TinyExtentCount
	}
	for i := 0; i < fixTinyExtents; i++ {
		extentID, err := dp.extentStore.GetBadTinyExtent()
		if err != nil {
			return
		}
		badTinyExtents = append(badTinyExtents, extentID)
	}
	return
}

func (dp *DataPartition) prepareRepairTasks(repairTasks []*DataPartitionRepairTask) (goodTinyExtents []uint64, badTinyExtents []uint64) {
	extentInfoMap := make(map[uint64]*storage.ExtentInfo)
	for index := 0; index < len(repairTasks); index++ {
		repairTask := repairTasks[index]
		for extentID, extentInfo := range repairTask.extents {
			extentWithMaxSize, ok := extentInfoMap[extentID]
			if !ok {
				extentInfoMap[extentID] = extentInfo
			} else {
				inode := extentInfoMap[extentID].Inode
				if extentInfo.Size > extentWithMaxSize.Size {
					if extentInfo.Inode == 0 && inode != 0 {
						extentInfo.Inode = inode
					}
					extentInfoMap[extentID] = extentInfo
				}
			}
		}
	}

	dp.buildExtentCreationTasks(repairTasks, extentInfoMap)
	goodTinyExtents, badTinyExtents = dp.buildExtentRepairTasks(repairTasks, extentInfoMap)
	return
}

//// TODO can we inline this function? it is hard to find a good name for it.
///* pasre all extent,select maxExtentSize to member index map
// */
//func (dp *DataPartition) getMaxSizedExtentMap(repairTasks []*DataPartitionRepairTask) (extentInfoMap map[uint64]*storage.ExtentInfo) {
//	extentInfoMap = make(map[uint64]*storage.ExtentInfo)
//	for index := 0; index < len(repairTasks); index++ {
//		repairTask := repairTasks[index]
//		for extentID, extentInfo := range repairTask.extents {
//			extentWithMaxSize, ok := extentInfoMap[extentID]
//			if !ok {
//				extentInfoMap[extentID] = extentInfo
//			} else {
//				inode := extentInfoMap[extentID].Inode
//				if extentInfo.Size > extentWithMaxSize.Size {
//					if extentInfo.Inode == 0 && inode != 0 {
//						extentInfo.Inode = inode
//					}
//					extentInfoMap[extentID] = extentInfo
//				}
//			}
//		}
//	}
//	return
//}

// Create a new extent if one of the replica is missing.
func (dp *DataPartition) buildExtentCreationTasks(repairTasks []*DataPartitionRepairTask, extentInfoMap map[uint64]*storage.ExtentInfo) {
	for extentID, extentInfo := range extentInfoMap {
		if storage.IsTinyExtent(extentID) {
			continue
		}
		for index := 0; index < len(repairTasks); index++ {
			repairTask := repairTasks[index]
			if _, ok := repairTask.extents[extentID]; !ok && extentInfo.Deleted == false {
				if extentInfo.Inode == 0 {
					continue
				}
				ei := &storage.ExtentInfo{Source: extentInfo.Source, FileID: extentID, Size: extentInfo.Size, Inode: extentInfo.Inode}
				repairTask.ExtentsToBeCreated = append(repairTask.ExtentsToBeCreated, ei)
				repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, ei)
				log.LogInfof("action[generatorAddExtentsTasks] addFile(%v_%v) on Index(%v).", dp.partitionID, ei, index)
			}
		}
	}
}

// Repair an extent if the replicas do not have the same length.
func (dp *DataPartition) buildExtentRepairTasks(repairTasks []*DataPartitionRepairTask, maxSizeExtentMap map[uint64]*storage.ExtentInfo) (goodTinyExtents []uint64, badTinyExtents []uint64) {
	goodTinyExtents = make([]uint64, 0)
	badTinyExtents = make([]uint64, 0)
	for extentID, maxFileInfo := range maxSizeExtentMap {

		// TODO what does "isFix" mean?
		isGoodExtent := true
		for index := 0; index < len(repairTasks); index++ {
			extentInfo, ok := repairTasks[index].extents[extentID]
			if !ok {
				continue
			}
			if extentInfo.Size < maxFileInfo.Size {
				fixExtent := &storage.ExtentInfo{Source: maxFileInfo.Source, FileID: extentID, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				repairTasks[index].ExtentsToBeRepaired = append(repairTasks[index].ExtentsToBeRepaired, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] fixExtent(%v_%v) on Index(%v).", dp.partitionID, fixExtent, index)
				isGoodExtent = false
			}

			if maxFileInfo.Inode != 0 && extentInfo.Inode == 0 {
				fixExtent := &storage.ExtentInfo{Source: maxFileInfo.Source, FileID: extentID, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				repairTasks[index].ExtentsToBeRepaired = append(repairTasks[index].ExtentsToBeRepaired, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] Modify Ino fixExtent(%v_%v).on Index(%v)", dp.partitionID, fixExtent, index)
			}
		}
		if storage.IsTinyExtent(extentID) {
			if isGoodExtent {
				goodTinyExtents = append(goodTinyExtents, extentID)
			} else {
				badTinyExtents = append(badTinyExtents, extentID)
			}
		}
	}
	return
}

func (dp *DataPartition) notifyFollower(wg *sync.WaitGroup, index int, members []*DataPartitionRepairTask) (err error) {
	p := repl.NewPacketToNotifyExtentRepair(dp.partitionID) //notify all follower to repairt task,send opnotifyRepair command
	var conn *net.TCPConn
	target := dp.replicas[index]
	p.Data, _ = json.Marshal(members[index])
	conn, err = gConnPool.GetConnect(target)
	defer func() {
		wg.Done()
		log.LogInfof(ActionNotifyFollowerRepair, fmt.Sprintf(" to (%v) task (%v) failed (%v)", target, string(p.Data), err))
	}()
	if err != nil {
		return err
	}
	p.Size = uint32(len(p.Data))
	if err = p.WriteToConn(conn); err != nil {
		gConnPool.PutConnect(conn, true)
		return err
	}
	if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
		gConnPool.PutConnect(conn, true)
		return err
	}
	gConnPool.PutConnect(conn, true)
	return err
}

// NotifyExtentRepair notify backup members to repair DataPartition extent
func (dp *DataPartition) NotifyExtentRepair(members []*DataPartitionRepairTask) (err error) {
	wg := new(sync.WaitGroup)
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go dp.notifyFollower(wg, i, members)
	}
	wg.Wait()
	return
}


// TODO it seems that there is no usage for the following function, correct?
// NotifyRaftFollowerToRepair notify raft follower to repair DataPartition extent*/
func (dp *DataPartition) NotifyRaftFollowerToRepair(repairTask *DataPartitionRepairTask) (err error) {
	var wg sync.WaitGroup

	for i := 0; i < len(dp.replicas); i++ {
		replicaAddr := strings.Split(dp.replicas[i], ":")
		if strings.TrimSpace(replicaAddr[0]) == LocalIP {
			continue // if the local one is the leader, then there is no need to send notification for repair
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := repl.NewPacketToNotifyExtentRepair(dp.partitionID)
			var conn *net.TCPConn
			target := dp.replicas[index]
			conn, err = gConnPool.GetConnect(target)
			if err != nil {
				return
			}
			p.Data, err = json.Marshal(repairTask)
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
		localExtentInfo, opErr := dp.ExtentStore().Watermark(uint64(remoteExtentInfo.FileID), false)
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

// The actual repair of an extent happens here.
func (dp *DataPartition) streamRepairExtent(remoteExtentInfo *storage.ExtentInfo) (err error) {
	store := dp.ExtentStore()
	if !store.HasExtent(remoteExtentInfo.FileID) {
		return
	}

	defer func() {
		store.Watermark(remoteExtentInfo.FileID, true)
	}()

	localExtentInfo, err := store.Watermark(remoteExtentInfo.FileID, true)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent Watermark error")
	}

	// size difference between the local extent and the remote extent
	sizeDiff := remoteExtentInfo.Size - localExtentInfo.Size

	// create a new streaming read packet
	request := repl.NewExtentRepairReadPacket(dp.ID(), remoteExtentInfo.FileID, int(localExtentInfo.Size), int(sizeDiff))
	var conn *net.TCPConn

	conn, err = gConnPool.GetConnect(remoteExtentInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	defer gConnPool.PutConnect(conn, true)

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

		// read 64k streaming repair packet
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

		log.LogInfof("action[streamRepairExtent] fix(%v_%v) start fix from (%v)"+
			" remoteSize(%v)localSize(%v) reply(%v).", dp.ID(), remoteExtentInfo.String(),
			remoteExtentInfo.Size, currFixOffset, reply.GetUniqueLogId())

		if reply.CRC != crc32.ChecksumIEEE(reply.Data[:reply.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch extent(%v_%v) start fix from (%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v)", dp.ID(), remoteExtentInfo.String(),
				remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return errors.Annotatef(err, "streamRepairExtent receive data error")
		}

		// write to the local extent file
		if storage.IsTinyExtent(uint64(localExtentInfo.FileID)) {
			err = store.TinyExtentRepairWrite(uint64(localExtentInfo.FileID), int64(currFixOffset), int64(reply.Size), reply.Data, reply.CRC)
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
