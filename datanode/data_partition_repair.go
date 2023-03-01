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

	"github.com/chubaofs/chubaofs/util/exporter"

	"github.com/chubaofs/chubaofs/util/unit"

	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

// DataPartitionRepairTask defines the reapir task for the data partition.
type DataPartitionRepairTask struct {
	TaskType                       uint8
	addr                           string
	extents                        map[uint64]storage.ExtentInfoBlock
	ExtentsToBeCreated             []storage.ExtentInfoBlock
	ExtentsToBeRepaired            []storage.ExtentInfoBlock
	ExtentsToBeRepairedSource      map[uint64]string
	LeaderTinyDeleteRecordFileSize int64
	LeaderAddr                     string
}

func NewDataPartitionRepairTask(extentFiles []storage.ExtentInfoBlock, tinyDeleteRecordFileSize int64, source, leaderAddr string) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		extents:                        make(map[uint64]storage.ExtentInfoBlock, 0),
		ExtentsToBeCreated:             make([]storage.ExtentInfoBlock, 0),
		ExtentsToBeRepaired:            make([]storage.ExtentInfoBlock, 0),
		ExtentsToBeRepairedSource:      make(map[uint64]string, 0),
		LeaderTinyDeleteRecordFileSize: tinyDeleteRecordFileSize,
		LeaderAddr:                     leaderAddr,
	}
	for _, extentFile := range extentFiles {
		task.extents[extentFile[storage.FileID]] = extentFile
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
	dp.DoRepairOnLeaderDisk(ctx, repairTasks)
	end := time.Now().UnixNano()

	// every time we need to figureAnnotatef out which extents need to be repaired and which ones do not.
	dp.sendAllTinyExtentsToC(extentType, availableTinyExtents, brokenTinyExtents)

	// error check
	if dp.extentStore.AvailableTinyExtentCnt()+dp.extentStore.BrokenTinyExtentCnt() > proto.TinyExtentCount {
		log.LogWarnf("action[repair] partition(%v) GoodTinyExtents(%v) "+
			"BadTinyExtents(%v) finish cost[%vms].", dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(),
			dp.extentStore.BrokenTinyExtentCnt(), (end-start)/int64(time.Millisecond))
	}

	log.LogInfof("action[repair] partition(%v) GoodTinyExtents(%v) BadTinyExtents(%v)"+
		" finish cost[%vms] masterAddr(%v).", dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(),
		dp.extentStore.BrokenTinyExtentCnt(), (end-start)/int64(time.Millisecond), MasterClient.Nodes())
}

func (dp *DataPartition) buildDataPartitionRepairTask(ctx context.Context, repairTasks []*DataPartitionRepairTask, replicas []string, extentType uint8, tinyExtents []uint64) (err error) {
	var leaderTinyDeleteRecordFileSize int64
	// get the local extent info
	extents, err := dp.getLocalExtentInfo(extentType, tinyExtents)
	if err != nil {
		return err
	}
	if !(dp.partitionStatus == proto.Unavailable) {
		leaderTinyDeleteRecordFileSize, err = dp.extentStore.LoadTinyDeleteFileOffset()
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
			log.LogErrorf("buildDataPartitionRepairTask PartitionID(%v) on(%v) err(%v)", dp.partitionID, followerAddr, err)
			continue
		}
		repairTasks[index] = NewDataPartitionRepairTask(extents, leaderTinyDeleteRecordFileSize, followerAddr, leaderAddr)
		repairTasks[index].addr = followerAddr
	}

	return
}

func (dp *DataPartition) getLocalExtentInfo(extentType uint8, tinyExtents []uint64) (extents []storage.ExtentInfoBlock, err error) {
	if extentType == proto.NormalExtentType {
		if !dp.ExtentStore().IsFinishLoad() {
			err = storage.PartitionIsLoaddingErr
		} else {
			extents, err = dp.extentStore.GetAllWatermarks(extentType, storage.NormalExtentFilter())
		}
	} else {
		extents, err = dp.extentStore.GetAllWatermarks(extentType, storage.TinyExtentFilter(tinyExtents))
	}
	if err != nil {
		err = errors.Trace(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
		return
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfo(ctx context.Context, extentType uint8, tinyExtents []uint64,
	target string) (extentFiles []storage.ExtentInfoBlock, err error) {

	// v1使用json序列化
	var version1Func = func() (extents []storage.ExtentInfoBlock, err error) {
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
			err = errors.Trace(err, fmt.Sprintf("get connection failed: %v", err))
			return
		}
		defer func() {
			gConnPool.PutConnectWithErr(conn, err)
		}()
		if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
			err = errors.Trace(err, fmt.Sprintf("write packet to connection failed %v", err))
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			err = errors.Trace(err, fmt.Sprintf("read reply from connection failed %v", err))
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.Trace(err, fmt.Sprintf("reply result mesg: %v", reply.GetOpMsgWithReqAndResult()))
			return
		}
		extents = make([]storage.ExtentInfoBlock, 0)
		if err = json.Unmarshal(reply.Data[:reply.Size], &extents); err != nil {
			err = errors.Trace(err, "unmarshal reply data failed")
			return
		}
		return
	}

	// v2使用二进制序列化
	var version2Func = func() (extents []storage.ExtentInfoBlock, err error) {
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
		if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
			err = errors.Trace(err, fmt.Sprintf("write packet to connection failed %v", err))
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			err = errors.Trace(err, fmt.Sprintf("read reply from connection failed %v", err))
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.Trace(err, fmt.Sprintf("reply result mesg: %v", reply.GetOpMsgWithReqAndResult()))
			return
		}
		if reply.Size%16 != 0 {
			// 合法的data长度与16对其，每16个字节存储一个Extent信息，[0:8)为FileID，[8:16)为Size。
			err = errors.NewErrorf("illegal result data length: %v", reply.Size)
			return
		}
		extents = make([]storage.ExtentInfoBlock, 0, reply.Size/16)
		for index := 0; index < int(reply.Size)/16; index++ {
			var offset = index * 16
			var extentID = binary.BigEndian.Uint64(reply.Data[offset : offset+8])
			var size = binary.BigEndian.Uint64(reply.Data[offset+8 : offset+16])
			eiBlock := storage.ExtentInfoBlock{
				storage.FileID: extentID,
				storage.Size:   size,
			}
			extents = append(extents, eiBlock)
		}
		return
	}
	// 首先尝试使用V2版本获取远端Extent信息, 失败后则尝试使用V1版本获取信息，以保证集群灰度过程中修复功能依然可以兼容。
	if extentFiles, err = version2Func(); err != nil {
		log.LogWarnf("partition(%v) get remote(%v) extent info by version 2 method failed"+
			" and will retry by using version 1 method: %v", dp.partitionID, target, err)
		if extentFiles, err = version1Func(); err != nil {
			log.LogErrorf("partition(%v) get remote(%v) extent info failed by both version 1 "+
				"and version 2 method: %v", dp.partitionID, target, err)
		}
	}
	return
}

// DoRepairOnLeaderDisk asks the leader to perform the repair tasks.
func (dp *DataPartition) DoRepairOnLeaderDisk(ctx context.Context, repairTasks []*DataPartitionRepairTask) {
	store := dp.extentStore
	for _, extentInfo := range repairTasks[0].ExtentsToBeCreated {
		if !AutoRepairStatus {
			log.LogWarnf("AutoRepairStatus is False,so cannot Create extent(%v)", extentInfo.String())
			continue
		}
		if !store.IsFinishLoad() {
			continue
		}
		if store.IsRecentDelete(extentInfo[storage.FileID]) {
			continue
		}
		store.Create(extentInfo[storage.FileID], true)
	}
	var allReplicas = dp.getReplicaClone()
	for _, extentInfo := range repairTasks[0].ExtentsToBeRepaired {
		if store.IsRecentDelete(extentInfo[storage.FileID]) {
			continue
		}
		majorSource := repairTasks[0].ExtentsToBeRepairedSource[extentInfo[storage.FileID]]
		sources := []string{
			majorSource,
		}
		if len(allReplicas) > 0 {
			for _, replica := range allReplicas[1:] {
				if replica != majorSource {
					sources = append(sources, replica)
				}
			}
		}
		for _, source := range sources {
			err := dp.streamRepairExtent(ctx, extentInfo, source, NoSkipLimit)
			if err != nil {
				err = errors.Trace(err, "DoRepairOnLeaderDisk %v", dp.applyRepairKey(int(extentInfo[storage.FileID])))
				localExtentInfo, opErr := dp.ExtentStore().Watermark(uint64(extentInfo[storage.FileID]))
				if opErr != nil {
					err = errors.Trace(err, opErr.Error())
				}
				err = errors.Trace(err, "partition(%v) remote(%v) local(%v)",
					dp.partitionID, extentInfo, localExtentInfo)
				log.LogWarnf("action[DoRepairOnLeaderDisk] err(%v).", err)
				continue
			}
			break
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
		if proto.IsTinyExtent(extentID) {
			dp.extentStore.SendToAvailableTinyExtentC(extentID)
		}
	}
	for _, extentID := range brokenTinyExtents {
		if proto.IsTinyExtent(extentID) {
			dp.extentStore.SendToBrokenTinyExtentC(extentID)
		}
	}
}

func (dp *DataPartition) brokenTinyExtents() (brokenTinyExtents []uint64) {
	brokenTinyExtents = make([]uint64, 0)
	extentsToBeRepaired := MinTinyExtentsToRepair
	if dp.extentStore.AvailableTinyExtentCnt() <= MinAvaliTinyExtentCnt {
		extentsToBeRepaired = proto.TinyExtentCount
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
	extentInfoMap := make(map[uint64]storage.ExtentInfoBlock)
	sources := make(map[uint64]string)
	for index := 0; index < len(repairTasks); index++ {
		repairTask := repairTasks[index]
		if repairTask == nil {
			continue
		}
		for extentID, extentInfo := range repairTask.extents {
			extentWithMaxSize, ok := extentInfoMap[extentID]
			if !ok {
				extentInfoMap[extentID] = extentInfo
				sources[extentID] = repairTask.addr
			} else {
				if extentInfo[storage.Size] > extentWithMaxSize[storage.Size] {
					extentInfoMap[extentID] = extentInfo
					sources[extentID] = repairTask.addr
				}
			}
		}
	}

	dp.buildExtentCreationTasks(repairTasks, extentInfoMap, sources)
	availableTinyExtents, brokenTinyExtents = dp.buildExtentRepairTasks(repairTasks, extentInfoMap, sources)
	return
}

// Create a new extent if one of the replica is missing.
func (dp *DataPartition) buildExtentCreationTasks(repairTasks []*DataPartitionRepairTask, extentInfoMap map[uint64]storage.ExtentInfoBlock, sources map[uint64]string) {
	for extentID, extentInfo := range extentInfoMap {
		if proto.IsTinyExtent(extentID) {
			continue
		}
		for index := 0; index < len(repairTasks); index++ {
			repairTask := repairTasks[index]
			if repairTask == nil {
				continue
			}
			if _, ok := repairTask.extents[extentID]; !ok {
				if proto.IsTinyExtent(extentID) {
					continue
				}
				ei := storage.ExtentInfoBlock{
					storage.FileID: extentID,
					storage.Size:   extentInfo[storage.Size],
				}
				repairTask.ExtentsToBeRepairedSource[extentID] = sources[extentID]
				repairTask.ExtentsToBeCreated = append(repairTask.ExtentsToBeCreated, ei)
				repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, ei)

				log.LogInfof("action[generatorAddExtentsTasks] addFile(%v_%v) on Index(%v).", dp.partitionID, ei, index)
			}
		}
	}
}

// Repair an extent if the replicas do not have the same length.
func (dp *DataPartition) buildExtentRepairTasks(repairTasks []*DataPartitionRepairTask, maxSizeExtentMap map[uint64]storage.ExtentInfoBlock, sources map[uint64]string) (availableTinyExtents []uint64, brokenTinyExtents []uint64) {
	availableTinyExtents = make([]uint64, 0)
	brokenTinyExtents = make([]uint64, 0)
	for extentID, maxFileInfo := range maxSizeExtentMap {
		certain := true // 状态明确
		hasBeenRepaired := true
		for index := 0; index < len(repairTasks); index++ {
			if repairTasks[index] == nil {
				certain = false // 状态设置为不明确
				continue
			}
			extentInfo, ok := repairTasks[index].extents[extentID]
			if !ok {
				continue
			}
			if extentInfo[storage.Size] < maxFileInfo[storage.Size] {
				fixExtent := storage.ExtentInfoBlock{
					storage.FileID: extentID,
					storage.Size:   maxFileInfo[storage.Size],
				}
				repairTasks[index].ExtentsToBeRepaired = append(repairTasks[index].ExtentsToBeRepaired, fixExtent)
				repairTasks[index].ExtentsToBeRepairedSource[extentID] = sources[extentID]
				log.LogInfof("action[generatorFixExtentSizeTasks] fixExtent(%v_%v) on(%v), maxExtentInfo(%v) readRepairExtentInfo(%v)",
					dp.partitionID, fixExtent, repairTasks[index].addr, maxFileInfo.String(), extentInfo.String())
				hasBeenRepaired = false
			}

		}
		if proto.IsTinyExtent(extentID) {
			if certain && hasBeenRepaired {
				// 仅状态明确且被修复完成的tiny extent可以设为available
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
		log.LogInfof(fmt.Sprintf(ActionNotifyFollowerToRepair+" to host(%v) Partition(%v) failed(%v)", target, dp.partitionID, err))
	}()
	if err != nil {
		return err
	}
	defer gConnPool.PutConnect(conn, true)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
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
func (dp *DataPartition) doStreamExtentFixRepairOnFollowerDisk(ctx context.Context, remoteExtentInfo storage.ExtentInfoBlock, sources []string) {

	var err error
	for _, source := range sources {
		err = dp.streamRepairExtent(ctx, remoteExtentInfo, source, NoSkipLimit)
		if err != nil {
			err = errors.Trace(err, "doStreamExtentFixRepairOnFollowerDisk %v", dp.applyRepairKey(int(remoteExtentInfo[storage.FileID])))
			localExtentInfo, opErr := dp.ExtentStore().Watermark(uint64(remoteExtentInfo[storage.FileID]))
			if opErr != nil {
				err = errors.Trace(err, opErr.Error())
			}
			err = errors.Trace(err, "partition(%v) remote(%v) local(%v)",
				dp.partitionID, remoteExtentInfo, localExtentInfo)
			log.LogWarnf("action[doStreamExtentFixRepairOnFollowerDisk] err(%v).", err)
			continue
		}
		break
	}
}

func (dp *DataPartition) applyRepairKey(extentID int) (m string) {
	return fmt.Sprintf("ApplyRepairKey(%v_%v)", dp.partitionID, extentID)
}

func (dp *DataPartition) tryLockExtentRepair(extentID uint64) (release func(), success bool) {
	dp.inRepairExtentMu.Lock()
	defer dp.inRepairExtentMu.Unlock()
	if _, has := dp.inRepairExtents[extentID]; has {
		success = false
		release = nil
		return
	}
	dp.inRepairExtents[extentID] = struct{}{}
	success = true
	release = func() {
		dp.inRepairExtentMu.Lock()
		defer dp.inRepairExtentMu.Unlock()
		delete(dp.inRepairExtents, extentID)
	}
	return
}

// The actual repair of an extent happens here.

func (dp *DataPartition) streamRepairExtent(ctx context.Context, remoteExtentInfo storage.ExtentInfoBlock, source string, SkipLimit bool) (err error) {
	release, success := dp.tryLockExtentRepair(remoteExtentInfo[storage.FileID])
	if !success {
		return
	}
	defer release()
	store := dp.ExtentStore()
	if !store.HasExtent(remoteExtentInfo[storage.FileID]) {
		return
	}

	var localExtentInfo *storage.ExtentInfoBlock

	if !SkipLimit {
		if !AutoRepairStatus && !proto.IsTinyExtent(remoteExtentInfo[storage.FileID]) {
			log.LogWarnf("AutoRepairStatus is False,so cannot AutoRepair extent(%v)", remoteExtentInfo.String())
			return
		}
		if !proto.IsTinyExtent(remoteExtentInfo[storage.FileID]) && !store.IsFinishLoad() {
			log.LogWarnf("partition(%v) is loading", dp.partitionID)
			return
		}
		localExtentInfo, err = store.Watermark(remoteExtentInfo[storage.FileID])
		if err != nil {
			return errors.Trace(err, "streamRepairExtent Watermark error")
		}
	} else {
		localExtentInfo, err = store.ForceWatermark(remoteExtentInfo[storage.FileID])
		if err != nil {
			return errors.Trace(err, "streamRepairExtent Watermark error")
		}
	}

	//if the data size of extentinfo struct is not equal with the data size of extent struct,use the data size of extent struct
	e, err := store.ExtentWithHeader(localExtentInfo)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent extentWithHeader error")
	}

	if localExtentInfo[storage.Size] != uint64(e.Size()) {
		localExtentInfo[storage.Size] = uint64(e.Size())
	}
	if localExtentInfo[storage.Size] >= remoteExtentInfo[storage.Size] {
		return nil
	}
	if !SkipLimit {
		if !dp.Disk().canRepairOnDisk() {
			return errors.Trace(err, "limit on apply disk repair task")
		}
		defer dp.Disk().finishRepairTask()
	}

	// size difference between the local extent and the remote extent
	sizeDiff := remoteExtentInfo[storage.Size] - localExtentInfo[storage.Size]
	request := repl.NewExtentRepairReadPacket(ctx, dp.partitionID, remoteExtentInfo[storage.FileID], int(localExtentInfo[storage.Size]), int(sizeDiff))
	if proto.IsTinyExtent(remoteExtentInfo[storage.FileID]) {
		if sizeDiff >= math.MaxUint32 {
			sizeDiff = math.MaxUint32 - unit.MB
		}
		request = repl.NewTinyExtentRepairReadPacket(ctx, dp.partitionID, remoteExtentInfo[storage.FileID], int(localExtentInfo[storage.Size]), int(sizeDiff))
	}
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(source)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent get conn from host(%v) error", source)
	}
	defer gConnPool.PutConnect(conn, true)

	if err = request.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.Trace(err, "streamRepairExtent send streamRead to host(%v) error", source)
		log.LogWarnf("action[streamRepairExtent] err(%v).", err)
		return
	}
	currFixOffset := localExtentInfo[storage.Size]
	var (
		hasRecoverySize uint64
	)
	for currFixOffset < remoteExtentInfo[storage.Size] {
		if currFixOffset >= remoteExtentInfo[storage.Size] {
			break
		}
		reply := repl.NewPacket(ctx)

		// read 64k streaming repair packet
		if err = reply.ReadFromConn(conn, 60); err != nil {
			err = errors.Trace(err, "streamRepairExtent receive data error,localExtentSize(%v) remoteExtentSize(%v)", currFixOffset, remoteExtentInfo[storage.Size])
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Trace(fmt.Errorf("unknow result code"),
				"streamRepairExtent receive opcode error(%v) ,localExtentSize(%v) remoteExtentSize(%v)", string(reply.Data[:reply.Size]), currFixOffset, remoteExtentInfo[storage.Size])
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentID != request.ExtentID {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) ,localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteExtentInfo[storage.Size])
			return
		}

		if !proto.IsTinyExtent(reply.ExtentID) && (reply.Size == 0 || reply.ExtentOffset != int64(currFixOffset)) {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteExtentInfo[storage.Size])
			return
		}
		if proto.IsTinyExtent(reply.ExtentID) && reply.ExtentOffset != int64(currFixOffset) {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteExtentInfo[storage.Size])
			return
		}

		log.LogInfof(fmt.Sprintf("action[streamRepairExtent] fix(%v_%v) start fix from(%v)"+
			" remoteSize(%v)localSize(%v) reply(%v).", dp.partitionID, localExtentInfo[storage.FileID], remoteExtentInfo.String(),
			remoteExtentInfo[storage.Size], currFixOffset, reply.GetUniqueLogId()))
		actualCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
		if reply.CRC != crc32.ChecksumIEEE(reply.Data[:reply.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch expectCrc(%v) actualCrc(%v) extent(%v_%v) start fix from(%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v) ", reply.CRC, actualCrc, dp.partitionID, remoteExtentInfo.String(),
				source, remoteExtentInfo[storage.Size], currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			return errors.Trace(err, "streamRepairExtent receive data error")
		}

		isEmptyResponse := false
		// Write it to local extent file
		currRecoverySize := uint64(reply.Size)
		if proto.IsTinyExtent(localExtentInfo[storage.FileID]) {
			originalDataSize := uint64(reply.Size)
			var remoteAvaliSize uint64
			if reply.ArgLen == TinyExtentRepairReadResponseArgLen {
				remoteAvaliSize = binary.BigEndian.Uint64(reply.Arg[9:TinyExtentRepairReadResponseArgLen])
			}
			if reply.Arg != nil { //compact v1.2.0 recovery
				isEmptyResponse = reply.Arg[0] == EmptyResponse
			}
			if isEmptyResponse {
				if reply.KernelOffset > 0 && reply.KernelOffset != uint64(crc32.ChecksumIEEE(reply.Arg)) {
					err = fmt.Errorf("streamRepairExtent arg crc mismatch expectCrc(%v) actualCrc(%v) extent(%v_%v) fix from(%v) remoteAvaliSize(%v) "+
						"hasRecoverySize(%v) currRecoverySize(%v) request(%v) reply(%v)", reply.KernelOffset, crc32.ChecksumIEEE(reply.Arg), dp.partitionID, remoteExtentInfo.String(),
						source, remoteAvaliSize, hasRecoverySize+currRecoverySize, currRecoverySize, request.GetUniqueLogId(), reply.GetUniqueLogId())
					return errors.Trace(err, "streamRepairExtent receive data error")
				}
				currRecoverySize = binary.BigEndian.Uint64(reply.Arg[1:9])
			}
			defer func() {
				if r := recover(); r != nil {
					recoverMesg := fmt.Sprintf("repairKey(%v) reply(%v) isEmptyResponse(%v) "+
						"currRecoverySize(%v) remoteAvaliSize(%v) currFixOffset(%v)", dp.applyRepairKey(int(reply.ExtentID)),
						reply, isEmptyResponse, currRecoverySize, remoteAvaliSize, currFixOffset)
					log.LogErrorf(recoverMesg)
					panic(recoverMesg)
				}
			}()
			if currFixOffset+currRecoverySize > remoteExtentInfo[storage.Size] {
				msg := fmt.Sprintf("action[streamRepairExtent] fix(%v_%v), streamRepairTinyExtent,remoteAvaliSize(%v) currFixOffset(%v) currRecoverySize(%v), remoteExtentSize(%v), isEmptyResponse(%v), needRecoverySize is too big",
					dp.partitionID, localExtentInfo[storage.FileID], remoteAvaliSize, currFixOffset, currRecoverySize, remoteExtentInfo[storage.Size], isEmptyResponse)
				exporter.WarningCritical(msg)
				return errors.Trace(err, "streamRepairExtent repair data error ")
			}
			err = store.TinyExtentRecover(uint64(localExtentInfo[storage.FileID]), int64(currFixOffset), int64(currRecoverySize), reply.Data[:originalDataSize], reply.CRC, isEmptyResponse)
			if hasRecoverySize+currRecoverySize >= remoteAvaliSize {
				log.LogInfof("streamRepairTinyExtent(%v) recover fininsh,remoteAvaliSize(%v) "+
					"hasRecoverySize(%v) currRecoverySize(%v)", dp.applyRepairKey(int(localExtentInfo[storage.FileID])),
					remoteAvaliSize, hasRecoverySize+currRecoverySize, currRecoverySize)
				break
			}
		} else {
			err = store.Write(ctx, uint64(localExtentInfo[storage.FileID]), int64(currFixOffset), int64(reply.Size), reply.Data[0:reply.Size], reply.CRC, storage.AppendWriteType, BufferWrite)
		}

		// write to the local extent file
		if err != nil {
			err = errors.Trace(err, "streamRepairExtent repair data error ")
			return
		}
		hasRecoverySize += currRecoverySize
		currFixOffset += currRecoverySize
		if currFixOffset >= remoteExtentInfo[storage.Size] {
			break
		}

	}

	dp.monitorData[proto.ActionRepairWrite].UpdateData(hasRecoverySize)

	return

}
