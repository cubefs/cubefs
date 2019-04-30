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

package datanode

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"hash/crc32"
)

// DataPartitionRepairTask defines the reapir task for the data partition.
type DataPartitionRepairTask struct {
	TaskType                 uint8
	addr                     string
	extents                  map[uint64]*storage.ExtentInfo
	ExtentsToBeCreated       []*storage.ExtentInfo
	ExtentsToBeRepaired      []*storage.ExtentInfo
	LeaderTinyDeleteFileSize int64
	LeaderAddr               string
}

func NewDataPartitionRepairTask(extentFiles []*storage.ExtentInfo, tinyDeleteFileSize int64, source, leaderAddr string) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		extents:                  make(map[uint64]*storage.ExtentInfo),
		ExtentsToBeCreated:       make([]*storage.ExtentInfo, 0),
		ExtentsToBeRepaired:      make([]*storage.ExtentInfo, 0),
		LeaderTinyDeleteFileSize: tinyDeleteFileSize,
		LeaderAddr:               leaderAddr,
	}
	for _, extentFile := range extentFiles {
		extentFile.Source = source
		task.extents[extentFile.FileID] = extentFile
	}

	return
}

// Main function to perform the repair.
// The repair process can be described as follows:
// There are two types of repairs.
// The first one is called the normal extent repair, and the second one is called the tiny extent repair.
// 1. normal extent repair:
// - the leader collects all the extent information from the followers.
// - for each extent, we compare all the replicas to find the one with the largest size.
// - periodically check the size of the local extent, and if it is smaller than the largest size,
//   add it to the tobeRepaired list, and generate the corresponding tasks.
// 2. tiny extent repair:
// - when creating the new partition, add all tiny extents to the toBeRepaired list,
//   and the repair task will create all the tiny extents first.
// - The leader of the replicas periodically collects the extent information of each follower
// - for each extent, we compare all the replicas to find the one with the largest size.
// - periodically check the size of the local extent, and if it is smaller than the largest size,
//   add it to the tobeRepaired list, and generate the corresponding tasks.
func (dp *DataPartition) repair(extentType uint8) {
	start := time.Now().UnixNano()
	log.LogInfof("action[repair] partition(%v) start.",
		dp.partitionID)

	var tinyExtents []uint64 // unsvailable extents 小文件写
	if extentType == proto.TinyExtentType {
		tinyExtents = dp.brokenTinyExtents()
		if len(tinyExtents) == 0 {
			return
		}
	}

	repairTasks := make([]*DataPartitionRepairTask, len(dp.replicas))
	err := dp.buildDataPartitionRepairTask(repairTasks, extentType, tinyExtents)

	if err != nil {
		log.LogErrorf(errors.Stack(err))
		log.LogErrorf("action[repair] partition(%v) err(%v).",
			dp.partitionID, err)
		dp.moveToBrokenTinyExtentC(extentType, tinyExtents)
		return
	}

	// compare all the extents in the replicas to compute the good and bad ones
	availableTinyExtents, brokenTinyExtents := dp.prepareRepairTasks(repairTasks)

	// notify the replicas to repair the extent
	err = dp.NotifyExtentRepair(repairTasks)
	if err != nil {
		dp.sendAllTinyExtentsToC(extentType, availableTinyExtents, brokenTinyExtents)
		log.LogErrorf("action[repair] partition(%v) err(%v).",
			dp.partitionID, err)
		log.LogError(errors.Stack(err))
		return
	}

	// ask the leader to do the repair
	dp.DoRepair(repairTasks)
	end := time.Now().UnixNano()

	// every time we need to figureAnnotatef out which extents need to be repaired and which ones do not.
	dp.sendAllTinyExtentsToC(extentType, availableTinyExtents, brokenTinyExtents)

	// error check
	if dp.extentStore.AvailableTinyExtentCnt()+dp.extentStore.BrokenTinyExtentCnt() > storage.TinyExtentCount {
		log.LogWarnf("action[repair] partition(%v) GoodTinyExtents(%v) "+
			"BadTinyExtents(%v) finish cost[%vms].", dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(),
			dp.extentStore.BrokenTinyExtentCnt(), (end-start)/int64(time.Millisecond))
	}

	log.LogInfof("action[repair] partition(%v) GoodTinyExtents(%v) BadTinyExtents(%v)"+
		" finish cost[%vms].", dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(), dp.extentStore.BrokenTinyExtentCnt(),
		(end-start)/int64(time.Millisecond))
}

func (dp *DataPartition) buildDataPartitionRepairTask(repairTasks []*DataPartitionRepairTask, extentType uint8, tinyExtents []uint64) (err error) {
	// get the local extent info
	extents, LeaderTinyDeleteFileSize, err := dp.getLocalExtentInfo(extentType, tinyExtents)
	if err != nil {
		return err
	}

	// new repair task for the leader
	repairTasks[0] = NewDataPartitionRepairTask(extents, LeaderTinyDeleteFileSize, dp.replicas[0], dp.replicas[0])
	repairTasks[0].addr = dp.replicas[0]

	// new repair tasks for the followers
	for index := 1; index < len(dp.replicas); index++ {
		extents, err := dp.getRemoteExtentInfo(extentType, tinyExtents, dp.replicas[index])
		if err != nil {
			return err
		}
		repairTasks[index] = NewDataPartitionRepairTask(extents, LeaderTinyDeleteFileSize, dp.replicas[index], dp.replicas[0])
		repairTasks[index].addr = dp.replicas[index]
	}

	return
}

func (dp *DataPartition) getLocalExtentInfo(extentType uint8, tinyExtents []uint64) (extents []*storage.ExtentInfo, tinyDeleteRecordSize int64, err error) {
	localExtents := make([]*storage.ExtentInfo, 0)
	extents = make([]*storage.ExtentInfo, 0)

	if extentType == proto.NormalExtentType {
		localExtents, tinyDeleteRecordSize, err = dp.extentStore.GetAllWatermarks(storage.NormalExtentFilter())
	} else {
		localExtents, tinyDeleteRecordSize, err = dp.extentStore.GetAllWatermarks(storage.TinyExtentFilter(tinyExtents))
	}
	if err != nil {
		err = errors.Trace(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
		return
	}
	data, err := json.Marshal(localExtents)
	if err != nil {
		err = errors.Trace(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
		return
	}

	if err = json.Unmarshal(data, &extents); err != nil {
		err = errors.Trace(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
	}

	return
}

func (dp *DataPartition) getRemoteExtentInfo(extentType uint8, tinyExtents []uint64,
	target string) (extentFiles []*storage.ExtentInfo, err error) {
	p := repl.NewPacketToGetAllWatermarks(dp.partitionID, extentType)
	extentFiles = make([]*storage.ExtentInfo, 0)
	if extentType == proto.TinyExtentType {
		p.Data, err = json.Marshal(tinyExtents)
		if err != nil {
			err = errors.Trace(err, "getRemoteExtentInfo DataPartition(%v) GetAllWatermarks", dp.partitionID)
			return
		}
		p.Size = uint32(len(p.Data))
	}
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(target) // get remote connection
	if err != nil {
		err = errors.Trace(err, "getRemoteExtentInfo DataPartition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn) // write command to the remote host
	if err != nil {
		err = errors.Trace(err, "getRemoteExtentInfo DataPartition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	reply := new(repl.Packet)
	err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime) // read the response
	if err != nil {
		err = errors.Trace(err, "getRemoteExtentInfo DataPartition(%v) read from host(%v)", dp.partitionID, target)
		return
	}
	err = json.Unmarshal(reply.Data[:reply.Size], &extentFiles)
	if err != nil {
		err = errors.Trace(err, "getRemoteExtentInfo DataPartition(%v) unmarshal json(%v) from host(%v)",
			dp.partitionID, string(reply.Data[:reply.Size]), target)
		return
	}

	return
}

// DoRepair asks the leader to perform the repair tasks.
func (dp *DataPartition) DoRepair(repairTasks []*DataPartitionRepairTask) {
	store := dp.extentStore
	for _, extentInfo := range repairTasks[0].ExtentsToBeCreated {
		store.Create(extentInfo.FileID)
	}
	for _, extentInfo := range repairTasks[0].ExtentsToBeRepaired {
		err := dp.streamRepairExtent(extentInfo)
		if err != nil {
			err = errors.Trace(err, "doStreamExtentFixRepair %v", dp.applyRepairKey(int(extentInfo.FileID)))
			localExtentInfo, opErr := dp.ExtentStore().Watermark(uint64(extentInfo.FileID))
			if opErr != nil {
				err = errors.Trace(err, opErr.Error())
			}
			err = errors.Trace(err, "partition(%v) remote(%v) local(%v)",
				dp.partitionID, extentInfo, localExtentInfo)
			log.LogWarnf("action[doStreamExtentFixRepair] err(%v).", err)
		}
	}
}

func (dp *DataPartition) moveToBrokenTinyExtentC(extentType uint8, extents []uint64) {
	if extentType == proto.TinyExtentType {
		dp.extentStore.SendAllToBrokenTinyExtentC(extents)
	}
	return
}

func (dp *DataPartition) sendAllTinyExtentsToC(extentType uint8, availableTinyExtents, brokenTinyExtents []uint64) {
	if extentType != proto.TinyExtentType {
		return
	}
	for _, extentID := range availableTinyExtents {
		if storage.IsTinyExtent(extentID) {
			dp.extentStore.SendToAvailableTinyExtentC(extentID)
		}
	}
	for _, extentID := range brokenTinyExtents {
		if storage.IsTinyExtent(extentID) {
			dp.extentStore.SendToBrokenTinyExtentC(extentID)
		}
	}
}

func (dp *DataPartition) brokenTinyExtents() (brokenTinyExtents []uint64) {
	brokenTinyExtents = make([]uint64, 0)
	extentsToBeRepaired := MinTinyExtentsToRepair
	if dp.extentStore.AvailableTinyExtentCnt() == 0 {
		extentsToBeRepaired = storage.TinyExtentCount
	}
	for i := 0; i < extentsToBeRepaired; i++ {
		extentID, err := dp.extentStore.GetBrokenTinyExtent()
		if err != nil {
			return
		}
		brokenTinyExtents = append(brokenTinyExtents, extentID)
	}
	return
}

func (dp *DataPartition) prepareRepairTasks(repairTasks []*DataPartitionRepairTask) (availableTinyExtents []uint64, brokenTinyExtents []uint64) {
	extentInfoMap := make(map[uint64]*storage.ExtentInfo)
	for index := 0; index < len(repairTasks); index++ {
		repairTask := repairTasks[index]
		for extentID, extentInfo := range repairTask.extents {
			if extentInfo.IsDeleted {
				continue
			}
			extentWithMaxSize, ok := extentInfoMap[extentID]
			if !ok {
				extentInfoMap[extentID] = extentInfo
			} else {
				if extentInfo.Size > extentWithMaxSize.Size {
					extentInfoMap[extentID] = extentInfo
				}
			}
		}
	}

	dp.buildExtentCreationTasks(repairTasks, extentInfoMap)
	availableTinyExtents, brokenTinyExtents = dp.buildExtentRepairTasks(repairTasks, extentInfoMap)
	return
}

// Create a new extent if one of the replica is missing.
func (dp *DataPartition) buildExtentCreationTasks(repairTasks []*DataPartitionRepairTask, extentInfoMap map[uint64]*storage.ExtentInfo) {
	for extentID, extentInfo := range extentInfoMap {
		if storage.IsTinyExtent(extentID) {
			continue
		}
		for index := 0; index < len(repairTasks); index++ {
			repairTask := repairTasks[index]
			if _, ok := repairTask.extents[extentID]; !ok && extentInfo.IsDeleted == false {
				if storage.IsTinyExtent(extentID) {
					continue
				}
				if extentInfo.IsDeleted {
					continue
				}
				ei := &storage.ExtentInfo{Source: extentInfo.Source, FileID: extentID, Size: extentInfo.Size}
				repairTask.ExtentsToBeCreated = append(repairTask.ExtentsToBeCreated, ei)
				repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, ei)
				log.LogInfof("action[generatorAddExtentsTasks] addFile(%v_%v) on Index(%v).", dp.partitionID, ei, index)
			}
		}
	}
}

// Repair an extent if the replicas do not have the same length.
func (dp *DataPartition) buildExtentRepairTasks(repairTasks []*DataPartitionRepairTask, maxSizeExtentMap map[uint64]*storage.ExtentInfo) (availableTinyExtents []uint64, brokenTinyExtents []uint64) {
	availableTinyExtents = make([]uint64, 0)
	brokenTinyExtents = make([]uint64, 0)
	for extentID, maxFileInfo := range maxSizeExtentMap {

		hasBeenRepaired := true
		for index := 0; index < len(repairTasks); index++ {
			extentInfo, ok := repairTasks[index].extents[extentID]
			if !ok {
				continue
			}
			if extentInfo.IsDeleted {
				continue
			}
			if extentInfo.Size < maxFileInfo.Size {
				fixExtent := &storage.ExtentInfo{Source: maxFileInfo.Source, FileID: extentID, Size: maxFileInfo.Size}
				repairTasks[index].ExtentsToBeRepaired = append(repairTasks[index].ExtentsToBeRepaired, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] fixExtent(%v_%v) on Index(%v) on(%v).",
					dp.partitionID, fixExtent, index, repairTasks[index].addr)
				hasBeenRepaired = false
			}

		}
		if storage.IsTinyExtent(extentID) {
			if hasBeenRepaired {
				availableTinyExtents = append(availableTinyExtents, extentID)
			} else {
				brokenTinyExtents = append(brokenTinyExtents, extentID)
			}
		}
	}
	return
}

func (dp *DataPartition) notifyFollower(wg *sync.WaitGroup, index int, members []*DataPartitionRepairTask) (err error) {
	p := repl.NewPacketToNotifyExtentRepair(dp.partitionID) // notify all the followers to repair
	var conn *net.TCPConn
	target := dp.replicas[index]
	p.Data, _ = json.Marshal(members[index])
	p.Size = uint32(len(p.Data))
	conn, err = gConnPool.GetConnect(target)
	defer func() {
		wg.Done()
		log.LogInfof(fmt.Sprintf(ActionNotifyFollowerToRepair+" to host(%v) Partition(%v) failed (%v)", target, dp.partitionID, err))
	}()
	if err != nil {
		return err
	}
	defer gConnPool.PutConnect(conn, true)
	if err = p.WriteToConn(conn); err != nil {
		return err
	}
	if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
		return err
	}
	return err
}

// NotifyExtentRepair notifies the followers to repair.
func (dp *DataPartition) NotifyExtentRepair(members []*DataPartitionRepairTask) (err error) {
	wg := new(sync.WaitGroup)
	for i := 1; i < len(members); i++ {
		wg.Add(1)

		go dp.notifyFollower(wg, i, members)
	}
	wg.Wait()
	return
}

// DoStreamExtentFixRepair executes the repair on the followers.
func (dp *DataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.ExtentInfo) {
	defer wg.Done()

	err := dp.streamRepairExtent(remoteExtentInfo)

	if err != nil {
		err = errors.Trace(err, "doStreamExtentFixRepair %v", dp.applyRepairKey(int(remoteExtentInfo.FileID)))
		localExtentInfo, opErr := dp.ExtentStore().Watermark(uint64(remoteExtentInfo.FileID))
		if opErr != nil {
			err = errors.Trace(err, opErr.Error())
		}
		err = errors.Trace(err, "partition(%v) remote(%v) local(%v)",
			dp.partitionID, remoteExtentInfo, localExtentInfo)
		log.LogWarnf("action[doStreamExtentFixRepair] err(%v).", err)
	}
}

func (dp *DataPartition) applyRepairKey(extentID int) (m string) {
	return fmt.Sprintf("ApplyRepairKey(%v_%v)", dp.partitionID, extentID)
}

// The actual repair of an extent happens here.
func (dp *DataPartition) streamRepairExtent(remoteExtentInfo *storage.ExtentInfo) (err error) {
	store := dp.ExtentStore()
	if !store.HasExtent(remoteExtentInfo.FileID) {
		return
	}
	localExtentInfo, err := store.Watermark(remoteExtentInfo.FileID)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent Watermark error")
	}

	if localExtentInfo.Size >= remoteExtentInfo.Size {
		return nil
	}
	// size difference between the local extent and the remote extent
	sizeDiff := remoteExtentInfo.Size - localExtentInfo.Size
	request := repl.NewExtentRepairReadPacket(dp.partitionID, remoteExtentInfo.FileID, int(localExtentInfo.Size), int(sizeDiff))
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(remoteExtentInfo.Source)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	defer gConnPool.PutConnect(conn, true)

	if err = request.WriteToConn(conn); err != nil {
		err = errors.Trace(err, "streamRepairExtent send streamRead to host[%v] error", remoteExtentInfo.Source)
		log.LogWarnf("action[streamRepairExtent] err[%v].", err)
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
			err = errors.Trace(err, "streamRepairExtent receive data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Trace(fmt.Errorf("unknow result code"),
				"streamRepairExtent receive opcode error(%v) ", string(reply.Data[:reply.Size]))
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentID != request.ExtentID || reply.Size == 0 || reply.ExtentOffset != int64(currFixOffset) {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId())
			return
		}

		log.LogInfof(fmt.Sprintf("action[streamRepairExtent] fix(%v_%v) start fix from (%v)"+
			" remoteSize(%v)localSize(%v) reply(%v).", dp.partitionID, localExtentInfo.FileID, remoteExtentInfo.String(),
			remoteExtentInfo.Size, currFixOffset, reply.GetUniqueLogId()))
		actualCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
		if reply.CRC != crc32.ChecksumIEEE(reply.Data[:reply.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch expectCrc(%v) actualCrc(%v) extent(%v_%v) start fix from (%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v) ", reply.CRC, actualCrc, dp.partitionID, remoteExtentInfo.String(),
				remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			return errors.Trace(err, "streamRepairExtent receive data error")
		}

		// write to the local extent file
		err = store.Write(uint64(localExtentInfo.FileID), int64(currFixOffset), int64(reply.Size), reply.Data, reply.CRC, UpdateSize, BufferWrite)
		if err != nil {
			err = errors.Trace(err, "streamRepairExtent repair data error")
			return
		}
		currFixOffset += uint64(reply.Size)
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}

	}
	return

}
