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
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util"

	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
)

// DataPartitionRepairTask defines the reapir task for the data partition.
type DataPartitionRepairTask struct {
	TaskType                       uint8
	addr                           string
	extents                        map[uint64]*storage.ExtentInfo
	ExtentsToBeCreated             []*storage.ExtentInfo
	ExtentsToBeRepaired            []*storage.ExtentInfo
	LeaderTinyDeleteRecordFileSize int64
	LeaderAddr                     string
}

func NewDataPartitionRepairTask(extentFiles []*storage.ExtentInfo, tinyDeleteRecordFileSize int64, source, leaderAddr string) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		extents:                        make(map[uint64]*storage.ExtentInfo),
		ExtentsToBeCreated:             make([]*storage.ExtentInfo, 0),
		ExtentsToBeRepaired:            make([]*storage.ExtentInfo, 0),
		LeaderTinyDeleteRecordFileSize: tinyDeleteRecordFileSize,
		LeaderAddr:                     leaderAddr,
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
func (dp *DataPartition) repair(ctx context.Context, extentType uint8) {
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
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		log.LogErrorf("action[repair] partition(%v) replicas is nil.", dp.partitionID)
		return
	}
	repairTasks := make([]*DataPartitionRepairTask, len(replicas))
	err := dp.buildDataPartitionRepairTask(ctx, repairTasks, replicas, extentType, tinyExtents)

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
	err = dp.NotifyExtentRepair(ctx, repairTasks)
	if err != nil {
		dp.sendAllTinyExtentsToC(extentType, availableTinyExtents, brokenTinyExtents)
		log.LogErrorf("action[repair] partition(%v) err(%v).",
			dp.partitionID, err)
		log.LogError(errors.Stack(err))
		return
	}

	// ask the leader to do the repair
	dp.DoRepair(ctx, repairTasks)
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
		" finish cost[%vms] masterAddr(%v).", dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(),
		dp.extentStore.BrokenTinyExtentCnt(), (end-start)/int64(time.Millisecond), MasterClient.Nodes())
}

func (dp *DataPartition) buildDataPartitionRepairTask(ctx context.Context, repairTasks []*DataPartitionRepairTask, replicas []string, extentType uint8, tinyExtents []uint64) (err error) {
	// get the local extent info
	extents, leaderTinyDeleteRecordFileSize, err := dp.getLocalExtentInfo(extentType, tinyExtents)
	if err != nil {
		return err
	}
	leaderAddr := replicas[0]
	// new repair task for the leader
	repairTasks[0] = NewDataPartitionRepairTask(extents, leaderTinyDeleteRecordFileSize, leaderAddr, leaderAddr)
	repairTasks[0].addr = leaderAddr

	// new repair tasks for the followers
	for index := 1; index < len(replicas); index++ {
		followerAddr := replicas[index]
		extents, err := dp.getRemoteExtentInfo(ctx, extentType, tinyExtents, followerAddr)
		if err != nil {
			log.LogErrorf("buildDataPartitionRepairTask PartitionID(%v) on (%v) err(%v)", dp.partitionID, followerAddr, err)
			continue
		}
		repairTasks[index] = NewDataPartitionRepairTask(extents, leaderTinyDeleteRecordFileSize, followerAddr, leaderAddr)
		repairTasks[index].addr = followerAddr
	}

	return
}

func (dp *DataPartition) getLocalExtentInfo(extentType uint8, tinyExtents []uint64) (extents []*storage.ExtentInfo, leaderTinyDeleteRecordFileSize int64, err error) {
	if extentType == proto.NormalExtentType {
		extents, leaderTinyDeleteRecordFileSize, err = dp.extentStore.GetAllWatermarks(storage.NormalExtentFilter())
	} else {
		extents, leaderTinyDeleteRecordFileSize, err = dp.extentStore.GetAllWatermarks(storage.TinyExtentFilter(tinyExtents))
	}
	if err != nil {
		err = errors.Trace(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
		return
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfo(ctx context.Context, extentType uint8, tinyExtents []uint64,
	target string) (extentFiles []*storage.ExtentInfo, err error) {

	// v1使用json序列化
	var version1Func = func() (extents []*storage.ExtentInfo, err error) {
		var packet = repl.NewPacketToGetAllWatermarks(ctx, dp.partitionID, extentType)
		if extentType == proto.TinyExtentType {
			if packet.Data, err = json.Marshal(tinyExtents); err != nil {
				err = errors.Trace(err, "marshal json failed")
				return
			}
			packet.Size = uint32(len(packet.Data))
		}
		var conn *net.TCPConn
		if conn, err = gConnPool.GetConnect(target); err != nil {
			err = errors.Trace(err, "get connection failed")
			return
		}
		defer func() {
			gConnPool.PutConnectWithErr(conn, err)
		}()
		if err = packet.WriteToConn(conn); err != nil {
			err = errors.Trace(err, "write packet to connection failed")
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			err = errors.Trace(err, "read reply from connection failed")
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.NewErrorf("reply result code: %v", reply.GetOpMsg())
			return
		}
		extents = make([]*storage.ExtentInfo, 0)
		if err = json.Unmarshal(reply.Data[:reply.Size], &extents); err != nil {
			err = errors.Trace(err, "unmarshal reply data failed")
			return
		}
		return
	}

	// v2使用二进制序列化
	var version2Func = func() (extents []*storage.ExtentInfo, err error) {
		var packet = repl.NewPacketToGetAllWatermarksV2(ctx, dp.partitionID, extentType)
		if extentType == proto.TinyExtentType {
			var buffer = bytes.NewBuffer(make([]byte, 0, len(tinyExtents)*8))
			for _, extentID := range tinyExtents {
				if err = binary.Write(buffer, binary.BigEndian, extentID); err != nil {
					err = errors.Trace(err, "binary encode failed")
					return
				}
			}
			packet.Data = buffer.Bytes()
			packet.Size = uint32(len(packet.Data))
		}
		var conn *net.TCPConn
		if conn, err = gConnPool.GetConnect(target); err != nil {
			err = errors.Trace(err, "get connection failed")
			return
		}
		defer func() {
			gConnPool.PutConnectWithErr(conn, err)
		}()
		if err = packet.WriteToConn(conn); err != nil {
			err = errors.Trace(err, "write packet to connection failed")
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			err = errors.Trace(err, "read reply from connection failed")
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.NewErrorf("reply result code: %v", reply.GetOpMsg())
			return
		}
		if reply.Size%16 != 0 {
			// 合法的data长度与16对其，每16个字节存储一个Extent信息，[0:8)为FileID，[8:16)为Size。
			err = errors.NewErrorf("illegal result data length: %v", len(reply.Data))
			return
		}
		extents = make([]*storage.ExtentInfo, 0, len(reply.Data)/16)
		for index := 0; index < int(reply.Size)/16; index++ {
			var offset = index * 16
			var extentID = binary.BigEndian.Uint64(reply.Data[offset:])
			var size = binary.BigEndian.Uint64(reply.Data[offset+8:])
			extents = append(extents, &storage.ExtentInfo{FileID: extentID, Size: size})
		}
		return
	}

	// 首先尝试使用V2版本获取远端Extent信息, 失败后则尝试使用V1版本获取信息，以保证集群灰度过程中修复功能依然可以兼容。
	if extentFiles, err = version2Func(); err != nil {
		log.LogWarnf("partition [%v] get remote [%v] extent info by version 2 method failed and will retry by using version 1 method: %v", err)
		if extentFiles, err = version1Func(); err != nil {
			log.LogErrorf("partition [%v] get remote [%v] extent info failed by both version 1 and version 2 method: %v", err)
		}
	}
	return
}

// DoRepair asks the leader to perform the repair tasks.
func (dp *DataPartition) DoRepair(ctx context.Context, repairTasks []*DataPartitionRepairTask) {
	store := dp.extentStore
	for _, extentInfo := range repairTasks[0].ExtentsToBeCreated {
		if !AutoRepairStatus {
			log.LogWarnf("AutoRepairStatus is False,so cannot Create extent(%v)", extentInfo.String())
			continue
		}
		store.Create(extentInfo.FileID, true)
	}
	for _, extentInfo := range repairTasks[0].ExtentsToBeRepaired {
		err := dp.streamRepairExtent(ctx, extentInfo)
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
	if dp.extentStore.AvailableTinyExtentCnt() <= MinAvaliTinyExtentCnt {
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
		if repairTask == nil {
			continue
		}
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
			if repairTask == nil {
				continue
			}
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
			if repairTasks[index] == nil {
				continue
			}
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

func (dp *DataPartition) notifyFollower(ctx context.Context, wg *sync.WaitGroup, index int, members []*DataPartitionRepairTask) (err error) {
	defer func() {
		wg.Done()
	}()
	p := repl.NewPacketToNotifyExtentRepair(ctx, dp.partitionID) // notify all the followers to repair
	var conn *net.TCPConn
	replicas := dp.getReplicaClone()
	if index >= len(replicas) {
		err = errors.Trace(err, " partition(%v) get FollowerHost failed ", dp.partitionID)
		return err
	}
	target := replicas[index]
	p.Data, _ = json.Marshal(members[index])
	p.Size = uint32(len(p.Data))
	conn, err = gConnPool.GetConnect(target)
	defer func() {
		log.LogInfof(fmt.Sprintf(ActionNotifyFollowerToRepair+" to host(%v) Partition(%v) failed (%v)", target, dp.partitionID, err))
	}()
	if err != nil {
		return err
	}
	defer gConnPool.PutConnect(conn, true)
	if err = p.WriteToConn(conn); err != nil {
		return err
	}
	if err = p.ReadFromConn(conn, proto.MaxWaitFollowerRepairTime); err != nil {
		return err
	}
	return err
}

// NotifyExtentRepair notifies the followers to repair.
func (dp *DataPartition) NotifyExtentRepair(ctx context.Context, members []*DataPartitionRepairTask) (err error) {
	wg := new(sync.WaitGroup)
	for i := 1; i < len(members); i++ {
		if members[i] == nil {
			continue
		}
		wg.Add(1)
		go dp.notifyFollower(ctx, wg, i, members)
	}
	wg.Wait()
	return
}

// DoStreamExtentFixRepair executes the repair on the followers.
func (dp *DataPartition) doStreamExtentFixRepair(ctx context.Context, wg *sync.WaitGroup, remoteExtentInfo *storage.ExtentInfo) {
	defer wg.Done()

	err := dp.streamRepairExtent(ctx, remoteExtentInfo)

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
func (dp *DataPartition) streamRepairExtent(ctx context.Context, remoteExtentInfo *storage.ExtentInfo) (err error) {
	store := dp.ExtentStore()
	if !store.HasExtent(remoteExtentInfo.FileID) {
		return
	}
	if !AutoRepairStatus && !storage.IsTinyExtent(remoteExtentInfo.FileID) {
		log.LogWarnf("AutoRepairStatus is False,so cannot AutoRepair extent(%v)", remoteExtentInfo.String())
		return
	}
	localExtentInfo, err := store.Watermark(remoteExtentInfo.FileID)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent Watermark error")
	}
	if !dp.Disk().canRepairOnDisk() {
		return errors.Trace(err, "limit on apply disk repair task")
	}
	defer dp.Disk().finishRepairTask()

	if localExtentInfo.Size >= remoteExtentInfo.Size {
		return nil
	}
	// size difference between the local extent and the remote extent
	sizeDiff := remoteExtentInfo.Size - localExtentInfo.Size
	request := repl.NewExtentRepairReadPacket(ctx, dp.partitionID, remoteExtentInfo.FileID, int(localExtentInfo.Size), int(sizeDiff))
	if storage.IsTinyExtent(remoteExtentInfo.FileID) {
		if sizeDiff >= math.MaxUint32 {
			sizeDiff = math.MaxUint32 - util.MB
		}
		request = repl.NewTinyExtentRepairReadPacket(ctx, dp.partitionID, remoteExtentInfo.FileID, int(localExtentInfo.Size), int(sizeDiff))
	}
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(remoteExtentInfo.Source)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent get conn from host(%v) error", remoteExtentInfo.Source)
	}
	defer gConnPool.PutConnect(conn, true)

	if err = request.WriteToConn(conn); err != nil {
		err = errors.Trace(err, "streamRepairExtent send streamRead to host(%v) error", remoteExtentInfo.Source)
		log.LogWarnf("action[streamRepairExtent] err(%v).", err)
		return
	}
	currFixOffset := localExtentInfo.Size
	var (
		hasRecoverySize uint64
	)
	for currFixOffset < remoteExtentInfo.Size {
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}
		reply := repl.NewPacket(ctx)

		// read 64k streaming repair packet
		if err = reply.ReadFromConn(conn, 60); err != nil {
			err = errors.Trace(err, "streamRepairExtent receive data error,localExtentSize(%v) remoteExtentSize(%v)", currFixOffset, remoteExtentInfo.Size)
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Trace(fmt.Errorf("unknow result code"),
				"streamRepairExtent receive opcode error(%v) ,localExtentSize(%v) remoteExtentSize(%v)", string(reply.Data[:reply.Size]), currFixOffset, remoteExtentInfo.Size)
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentID != request.ExtentID {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) ,localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteExtentInfo.Size)
			return
		}

		if !storage.IsTinyExtent(reply.ExtentID) && (reply.Size == 0 || reply.ExtentOffset != int64(currFixOffset)) {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteExtentInfo.Size)
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

		isEmptyResponse := false
		// Write it to local extent file
		if storage.IsTinyExtent(uint64(localExtentInfo.FileID)) {
			currRecoverySize := uint64(reply.Size)
			var remoteAvaliSize uint64
			if reply.ArgLen == TinyExtentRepairReadResponseArgLen {
				remoteAvaliSize = binary.BigEndian.Uint64(reply.Arg[9:TinyExtentRepairReadResponseArgLen])
			}
			if reply.Arg != nil { //compact v1.2.0 recovery
				isEmptyResponse = reply.Arg[0] == EmptyResponse
			}
			if isEmptyResponse {
				currRecoverySize = binary.BigEndian.Uint64(reply.Arg[1:9])
				reply.Size = uint32(currRecoverySize)
			}
			err = store.TinyExtentRecover(uint64(localExtentInfo.FileID), int64(currFixOffset), int64(currRecoverySize), reply.Data, reply.CRC, isEmptyResponse)
			if hasRecoverySize+currRecoverySize >= remoteAvaliSize {
				log.LogInfof("streamRepairTinyExtent(%v) recover fininsh,remoteAvaliSize(%v) "+
					"hasRecoverySize(%v) currRecoverySize(%v)", dp.applyRepairKey(int(localExtentInfo.FileID)),
					remoteAvaliSize, hasRecoverySize+currRecoverySize, currRecoverySize)
				break
			}
		} else {
			err = store.Write(ctx, uint64(localExtentInfo.FileID), int64(currFixOffset), int64(reply.Size), reply.Data, reply.CRC, storage.AppendWriteType, BufferWrite)
		}

		// write to the local extent file
		if err != nil {
			err = errors.Trace(err, "streamRepairExtent repair data error ")
			return
		}
		hasRecoverySize += uint64(reply.Size)
		currFixOffset += uint64(reply.Size)
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}

	}

	dp.monitorData[statistics.ActionRepairWrite].UpdateData(hasRecoverySize)

	return

}
