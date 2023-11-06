// Copyright 2018 The CubeFS Authors.
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
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/async"

	"github.com/cubefs/cubefs/util/exporter"

	"github.com/cubefs/cubefs/util/unit"

	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// DataPartitionRepairTask defines the reapir task for the data partition.
type DataPartitionRepairTask struct {
	TaskType                       uint8
	addr                           string
	extents                        map[uint64]storage.ExtentInfoBlock
	ExtentsToBeCreated             []storage.ExtentInfoBlock
	ExtentsToBeRepaired            []storage.ExtentInfoBlock
	ExtentsToBeRepairedSource      map[uint64]string
	BaseExtentID                   uint64
	LeaderTinyDeleteRecordFileSize int64
	LeaderAddr                     string
}

func NewDataPartitionRepairTask(address string, extentFiles []storage.ExtentInfoBlock, baseExtentID uint64, tinyDeleteRecordFileSize int64, source, leaderAddr string) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		addr:                           address,
		extents:                        make(map[uint64]storage.ExtentInfoBlock, 0),
		ExtentsToBeCreated:             make([]storage.ExtentInfoBlock, 0),
		ExtentsToBeRepaired:            make([]storage.ExtentInfoBlock, 0),
		ExtentsToBeRepairedSource:      make(map[uint64]string, 0),
		LeaderTinyDeleteRecordFileSize: tinyDeleteRecordFileSize,
		LeaderAddr:                     leaderAddr,
		BaseExtentID:                   baseExtentID,
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
//  1. normal extent repair:
//     - the leader collects all the extent information from the followers.
//     - for each extent, we compare all the replicas to find the one with the largest size.
//     - periodically check the size of the local extent, and if it is smaller than the largest size,
//     add it to the tobeRepaired list, and generate the corresponding tasks.
//  2. tiny extent repair:
//     - when creating the new partition, add all tiny extents to the toBeRepaired list,
//     and the repair task will create all the tiny extents first.
//     - The leader of the replicas periodically collects the extent information of each follower
//     - for each extent, we compare all the replicas to find the one with the largest size.
//     - periodically check the size of the local extent, and if it is smaller than the largest size,
//     add it to the tobeRepaired list, and generate the corresponding tasks.
func (dp *DataPartition) repair(ctx context.Context, extentType uint8) {
	start := time.Now().UnixNano()
	log.LogInfof("action[repair] partition(%v) start.",
		dp.partitionID)

	var tinyExtents []uint64 // unavailable extents 小文件写
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
	repairTasks, err := dp.buildDataPartitionRepairTask(ctx, replicas, extentType, tinyExtents)

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
	err = dp.notifyFollowersToRepair(ctx, repairTasks)
	if err != nil {
		dp.sendAllTinyExtentsToC(extentType, availableTinyExtents, brokenTinyExtents)
		log.LogErrorf("action[repair] partition(%v) err(%v).",
			dp.partitionID, err)
		log.LogError(errors.Stack(err))
		return
	}

	// ask the leader to do the repair
	dp.DoRepairOnLeaderDisk(ctx, repairTasks[0])
	end := time.Now().UnixNano()

	// every time we need to figureAnnotatef out which extents need to be repaired and which ones do not.
	dp.sendAllTinyExtentsToC(extentType, availableTinyExtents, brokenTinyExtents)

	// error check
	if dp.extentStore.AvailableTinyExtentCnt()+dp.extentStore.BrokenTinyExtentCnt() != proto.TinyExtentCount {
		log.LogWarnf("action[repair] partition(%v) GoodTinyExtents(%v) "+
			"BadTinyExtents(%v) finish cost[%vms].", dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(),
			dp.extentStore.BrokenTinyExtentCnt(), (end-start)/int64(time.Millisecond))
	}

	log.LogInfof("action[repair] partition(%v) GoodTinyExtents(%v) BadTinyExtents(%v)"+
		" finish cost[%vms] masterAddr(%v).", dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(),
		dp.extentStore.BrokenTinyExtentCnt(), (end-start)/int64(time.Millisecond), MasterClient.Nodes())
}

func (dp *DataPartition) buildDataPartitionRepairTask(ctx context.Context, replicas []string, extentType uint8, tinyExtents []uint64) ([]*DataPartitionRepairTask, error) {
	var tasks = make([]*DataPartitionRepairTask, len(replicas))
	var localTinyDeleteRecordFileSize int64
	// get the local extent info
	localExtents, localBaseExtentID, err := dp.getLocalExtentInfo(extentType, tinyExtents)
	if err != nil {
		return nil, err
	}

	if !(dp.partitionStatus == proto.Unavailable) {
		localTinyDeleteRecordFileSize, err = dp.extentStore.LoadTinyDeleteFileOffset()
	}

	leaderAddr := replicas[0]
	// new repair task for the leader
	tasks[0] = NewDataPartitionRepairTask(leaderAddr, localExtents, localBaseExtentID, localTinyDeleteRecordFileSize, leaderAddr, leaderAddr)

	// new repair tasks for the followers
	for index := 1; index < len(replicas); index++ {
		followerAddr := replicas[index]
		extents, baseExtentID, err := dp.getRemoteExtentInfo(ctx, extentType, tinyExtents, followerAddr)
		if err != nil {
			log.LogErrorf("buildDataPartitionRepairTask PartitionID(%v) on(%v) err(%v)", dp.partitionID, followerAddr, err)
			continue
		}
		tasks[index] = NewDataPartitionRepairTask(followerAddr, extents, baseExtentID, localTinyDeleteRecordFileSize, followerAddr, leaderAddr)
	}
	return tasks, nil
}

func (dp *DataPartition) getLocalExtentInfo(extentType uint8, tinyExtents []uint64) (extents []storage.ExtentInfoBlock, baseExtentID uint64, err error) {
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
	baseExtentID = dp.extentStore.GetBaseExtentID()
	return
}

func (dp *DataPartition) getRemoteExtentInfo(ctx context.Context, extentType uint8, tinyExtents []uint64,
	target string) (extentFiles []storage.ExtentInfoBlock, baseExtentID uint64, err error) {

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
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.New(string(reply.Data[:reply.Size]))
			return
		}
		extents = make([]storage.ExtentInfoBlock, 0)
		if err = json.Unmarshal(reply.Data[:reply.Size], &extents); err != nil {
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
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.New(string(reply.Data[:reply.Size]))
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

	// v3使用二进制序列化
	var version3Func = func() (extents []storage.ExtentInfoBlock, baseExtentID uint64, err error) {
		var packet = repl.NewPacketToGetAllWatermarksV3(ctx, dp.partitionID, extentType)
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
			err = errors.New(string(reply.Data[:reply.Size]))
			return
		}
		if reply.Size < 8 || (reply.Size-8)%16 != 0 {
			// 合法的data前8个字节存储baseExtentID且后续长度与16对其，每16个字节存储一个Extent信息，[0:8)为FileID，[8:16)为Size。
			err = errors.NewErrorf("illegal result data length: %v", reply.Size)
			return
		}
		baseExtentID = binary.BigEndian.Uint64(reply.Data[:8])
		extents = make([]storage.ExtentInfoBlock, 0, reply.Size/16)
		for index := 0; index < int(reply.Size-8)/16; index++ {
			var offset = 8 + index*16
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

	var isUnknownOpError = func(err error) bool {
		return err != nil && strings.Contains(err.Error(), repl.ErrorUnknownOp.Error())
	}

	// 首先尝试使用V3版本获取远端Extent信息, 失败后则尝试使用V2和V1版本获取信息，以保证集群灰度过程中修复功能依然可以兼容。
	if extentFiles, baseExtentID, err = version3Func(); isUnknownOpError(err) {
		log.LogWarnf("partition(%v) get remote(%v) extent info by version 3 method failed"+
			" and will retry by using version 2 method: %v", dp.partitionID, target, err)
		if extentFiles, err = version2Func(); isUnknownOpError(err) {
			log.LogWarnf("partition(%v) get remote(%v) extent info by version 2 method failed"+
				" and will retry by using version 1 method: %v", dp.partitionID, target, err)
			extentFiles, err = version1Func()
		}
	}
	return
}

// DoRepairOnLeaderDisk asks the leader to perform the repair tasks.
func (dp *DataPartition) DoRepairOnLeaderDisk(ctx context.Context, repairTask *DataPartitionRepairTask) {
	store := dp.extentStore
	_ = store.AdvanceBaseExtentID(repairTask.BaseExtentID)
	for _, extentInfo := range repairTask.ExtentsToBeCreated {
		if !AutoRepairStatus {
			log.LogWarnf("AutoRepairStatus is False,so cannot Create extent(%v)", extentInfo.String())
			continue
		}
		if !store.IsFinishLoad() || store.IsDeleted(extentInfo[storage.FileID]) {
			continue
		}
		_ = store.Create(extentInfo[storage.FileID], extentInfo[storage.Inode], true)
	}
	var allReplicas = dp.getReplicaClone()
	for _, extentInfo := range repairTask.ExtentsToBeRepaired {
		if store.IsDeleted(extentInfo[storage.FileID]) {
			continue
		}
		majorSource := repairTask.ExtentsToBeRepairedSource[extentInfo[storage.FileID]]
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
			err := dp.streamRepairExtent(ctx, extentInfo, source, SkipLimit)
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

// DoExtentStoreRepairOnFollowerDisk performs the repairs of the extent store.
// 1. when the extent size is smaller than the max size on the record, start to repair the missing part.
// 2. if the extent does not even exist, create the extent first, and then repair.
func (dp *DataPartition) DoExtentStoreRepairOnFollowerDisk(repairTask *DataPartitionRepairTask) {
	store := dp.extentStore
	_ = store.AdvanceBaseExtentID(repairTask.BaseExtentID)
	for _, extentInfo := range repairTask.ExtentsToBeCreated {
		if proto.IsTinyExtent(extentInfo[storage.FileID]) || !dp.ExtentStore().IsFinishLoad() {
			continue
		}

		if store.IsDeleted(extentInfo[storage.FileID]) {
			continue
		}
		if store.IsExists(extentInfo[storage.FileID]) {
			var info = storage.ExtentInfoBlock{
				storage.FileID: extentInfo[storage.FileID],
				storage.Size:   extentInfo[storage.Size],
			}
			repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
			continue
		}
		if !AutoRepairStatus {
			log.LogWarnf("AutoRepairStatus is False,so cannot Create extent(%v)", extentInfo.String())
			continue
		}
		if err := store.Create(extentInfo[storage.FileID], extentInfo[storage.Inode], true); err != nil {
			continue
		}
		var info = storage.ExtentInfoBlock{
			storage.FileID: extentInfo[storage.FileID],
			storage.Size:   extentInfo[storage.Size],
		}
		repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
	}

	localAddr := fmt.Sprintf("%v:%v", LocalIP, LocalServerPort)
	allReplicas := dp.getReplicaClone()

	// 使用生产消费模型并行修复Extent。
	var startTime = time.Now()

	// 内部数据结构，用于包裹修复Extent相关必要信息
	type __ExtentRepairTask struct {
		ExtentInfo storage.ExtentInfoBlock
		Sources    []string
	}

	var syncTinyDeleteRecordOnly = dp.DataPartitionCreateType == proto.DecommissionedCreateDataPartition
	var extentRepairTaskCh = make(chan *__ExtentRepairTask, len(repairTask.ExtentsToBeRepaired))
	var extentRepairWorkerWG = new(sync.WaitGroup)
	for i := 0; i < NumOfFilesToRecoverInParallel; i++ {
		extentRepairWorkerWG.Add(1)
		var worker = func() {
			defer extentRepairWorkerWG.Done()
			for {
				var task = <-extentRepairTaskCh
				if task == nil {
					return
				}
				dp.doStreamExtentFixRepairOnFollowerDisk(context.Background(), task.ExtentInfo, task.Sources)
			}
		}
		var handlePanic = func(r interface{}) {
			// Worker 发生panic，进行报警
			var callstack = string(debug.Stack())
			log.LogCriticalf("Occurred panic while repair extent: %v\n"+
				"Callstack: %v\n", r, callstack)
			exporter.Warning(fmt.Sprintf("PANIC ORRCURRED!\n"+
				"Fix worker occurred panic and stopped:\n"+
				"Partition: %v\n"+
				"Message  : %v\n",
				dp.partitionID, r))
			return
		}
		async.RunWorker(worker, handlePanic)
	}
	var validExtentsToBeRepaired int
	for _, extentInfo := range repairTask.ExtentsToBeRepaired {
		if store.IsDeleted(extentInfo[storage.FileID]) || !store.IsExists(extentInfo[storage.FileID]) {
			continue
		}
		majorSource := repairTask.ExtentsToBeRepairedSource[extentInfo[storage.FileID]]
		sources := make([]string, 0, len(allReplicas))
		sources = append(sources, majorSource)
		for _, replica := range allReplicas {
			if replica == majorSource || replica == localAddr {
				continue
			}
			sources = append(sources, replica)
		}
		extentRepairTaskCh <- &__ExtentRepairTask{
			ExtentInfo: extentInfo,
			Sources:    sources,
		}
		validExtentsToBeRepaired++
	}
	close(extentRepairTaskCh)
	extentRepairWorkerWG.Wait()

	dp.doStreamFixTinyDeleteRecord(context.Background(), repairTask, syncTinyDeleteRecordOnly, time.Now().Unix()-dp.FullSyncTinyDeleteTime > MaxFullSyncTinyDeleteTime)

	log.LogInfof("partition[%v] repaired %v extents, cost %v", dp.partitionID, validExtentsToBeRepaired, time.Now().Sub(startTime))
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
	var (
		extentInfoMap          = make(map[uint64]storage.ExtentInfoBlock)
		sources                = make(map[uint64]string)
		maxBaseExtentID uint64 = 0
	)

	for index := 0; index < len(repairTasks); index++ {
		repairTask := repairTasks[index]
		if repairTask == nil {
			continue
		}
		maxBaseExtentID = uint64(math.Max(float64(maxBaseExtentID), float64(repairTask.BaseExtentID)))
		for extentID, extentInfo := range repairTask.extents {
			if dp.extentStore.IsDeleted(extentID) {
				continue
			}
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
	dp.buildBaseExtentIDTasks(repairTasks, maxBaseExtentID)
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

// proposeRepair an extent if the replicas do not have the same length.
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

func (dp *DataPartition) buildBaseExtentIDTasks(repairTasks []*DataPartitionRepairTask, maxBaseExtentID uint64) {
	for index := 0; index < len(repairTasks); index++ {
		if repairTasks[index] == nil {
			continue
		}
		repairTasks[index].BaseExtentID = maxBaseExtentID
	}
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

// notifyFollowersToRepair notifies the followers to repair.
func (dp *DataPartition) notifyFollowersToRepair(ctx context.Context, members []*DataPartitionRepairTask) (err error) {
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
		err = dp.streamRepairExtent(ctx, remoteExtentInfo, source, SkipLimit)
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

func (dp *DataPartition) streamRepairExtent(ctx context.Context, remoteExtentInfo storage.ExtentInfoBlock, source string, skipLimit bool) (err error) {

	var (
		extentID   = remoteExtentInfo[storage.FileID]
		remoteSize = remoteExtentInfo[storage.Size]
	)

	// Checking store and extent state at first.
	var store = dp.ExtentStore()
	if !store.IsExists(extentID) || store.IsDeleted(extentID) {
		return
	}
	if !store.IsFinishLoad() {
		log.LogWarnf("partition(%v) is loading", dp.partitionID)
		return
	}

	if !skipLimit {
		if !dp.Disk().canRepairOnDisk() {
			return errors.Trace(err, "limit on apply disk repair task")
		}
		defer dp.Disk().finishRepairTask()
	}

	var release, success = dp.tryLockExtentRepair(extentID)
	if !success {
		return
	}
	defer release()

	var localExtentInfo *storage.ExtentInfoBlock
	if !skipLimit {
		if !AutoRepairStatus && !proto.IsTinyExtent(extentID) {
			log.LogWarnf("AutoRepairStatus is False,so cannot AutoRepair extent(%v)", remoteExtentInfo.String())
			return
		}
		if localExtentInfo, err = store.Watermark(extentID); err != nil {
			return errors.Trace(err, "streamRepairExtent Watermark error")
		}
	} else {
		if localExtentInfo, err = store.ForceWatermark(extentID); err != nil {
			return errors.Trace(err, "streamRepairExtent Watermark error")
		}
	}

	log.LogInfof("streamRepairExtent: partition[%v] start to fix extent [id: %v, modifytime: %v, localsize: %v, remotesize: %v] from [%v]",
		dp.partitionID, extentID, localExtentInfo[storage.ModifyTime], localExtentInfo[storage.Size], remoteSize, source)

	//if the data size of extentinfo struct is not equal with the data size of extent struct,use the data size of extent struct
	e, err := store.ExtentWithHeader(localExtentInfo)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent extentWithHeader error")
	}

	if localExtentInfo[storage.Size] != uint64(e.Size()) {
		localExtentInfo[storage.Size] = uint64(e.Size())
	}
	if localExtentInfo[storage.Size] >= remoteSize {
		return nil
	}

	// size difference between the local extent and the remote extent
	sizeDiff := remoteExtentInfo[storage.Size] - localExtentInfo[storage.Size]
	request := repl.NewExtentRepairReadPacket(ctx, dp.partitionID, remoteExtentInfo[storage.FileID], int(localExtentInfo[storage.Size]), int(sizeDiff), false)
	if proto.IsTinyExtent(remoteExtentInfo[storage.FileID]) {
		if sizeDiff >= math.MaxUint32 {
			sizeDiff = math.MaxUint32 - unit.MB
		}
		request = repl.NewTinyExtentRepairReadPacket(ctx, dp.partitionID, remoteExtentInfo[storage.FileID], int(localExtentInfo[storage.Size]), int(sizeDiff), false)
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
		replyDataBuffer []byte
	)
	replyDataBuffer, _ = proto.Buffers.Get(unit.ReadBlockSize)
	defer func() {
		proto.Buffers.Put(replyDataBuffer[:unit.ReadBlockSize])
	}()
	var getReplyDataBuffer = func(size uint32) []byte {
		if int(size) > cap(replyDataBuffer) {
			return make([]byte, size)
		}
		return replyDataBuffer[:size]
	}

	for currFixOffset < remoteSize {
		reply := repl.NewPacket(ctx)
		if err = reply.ReadFromConnWithSpecifiedDataBuffer(conn, 60, getReplyDataBuffer); err != nil {
			err = errors.Trace(err, "streamRepairExtent receive data error,localExtentSize(%v) remoteExtentSize(%v)", currFixOffset, remoteSize)
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Trace(fmt.Errorf("unknow result code"),
				"streamRepairExtent receive opcode error(%v) ,localExtentSize(%v) remoteExtentSize(%v)", string(reply.Data[:reply.Size]), currFixOffset, remoteSize)
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentID != request.ExtentID {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) ,localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteSize)
			return
		}

		if !proto.IsTinyExtent(reply.ExtentID) && (reply.Size == 0 || reply.ExtentOffset != int64(currFixOffset)) {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteSize)
			return
		}
		if proto.IsTinyExtent(reply.ExtentID) && reply.ExtentOffset != int64(currFixOffset) {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteSize)
			return
		}

		log.LogInfof(fmt.Sprintf("action[streamRepairExtent] fix(%v_%v) start fix from(%v)"+
			" remoteSize(%v)localSize(%v) reply(%v).", dp.partitionID, extentID, remoteExtentInfo.String(),
			remoteSize, currFixOffset, reply.GetUniqueLogId()))
		if actualCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size]); actualCrc != reply.CRC {
			err = fmt.Errorf("streamRepairExtent crc mismatch expectCrc(%v) actualCrc(%v) extent(%v_%v) start fix from(%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v) ", reply.CRC, actualCrc, dp.partitionID, remoteExtentInfo.String(),
				source, remoteSize, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			return errors.Trace(err, "streamRepairExtent receive data error")
		}

		// Write it to local extent file
		currRecoverySize := uint64(reply.Size)
		if proto.IsTinyExtent(extentID) {
			isEmptyResponse := reply.Arg != nil && reply.Arg[0] == EmptyResponse
			originalDataSize := uint64(reply.Size)
			var remoteAvaliSize uint64
			if reply.ArgLen == TinyExtentRepairReadResponseArgLen {
				remoteAvaliSize = binary.BigEndian.Uint64(reply.Arg[9:TinyExtentRepairReadResponseArgLen])
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
			if currFixOffset+currRecoverySize > remoteSize {
				msg := fmt.Sprintf("action[streamRepairExtent] fix(%v_%v), streamRepairTinyExtent,remoteAvaliSize(%v) currFixOffset(%v) currRecoverySize(%v), remoteExtentSize(%v), isEmptyResponse(%v), needRecoverySize is too big",
					dp.partitionID, localExtentInfo[storage.FileID], remoteAvaliSize, currFixOffset, currRecoverySize, remoteExtentInfo[storage.Size], isEmptyResponse)
				exporter.WarningCritical(msg)
				return errors.Trace(err, "streamRepairExtent repair data error ")
			}
			err = store.TinyExtentRecover(extentID, int64(currFixOffset), int64(currRecoverySize), reply.Data[:originalDataSize], reply.CRC, isEmptyResponse)
			if hasRecoverySize+currRecoverySize >= remoteAvaliSize {
				log.LogInfof("streamRepairTinyExtent(%v) recover fininsh,remoteAvaliSize(%v) "+
					"hasRecoverySize(%v) currRecoverySize(%v)", dp.applyRepairKey(int(localExtentInfo[storage.FileID])),
					remoteAvaliSize, hasRecoverySize+currRecoverySize, currRecoverySize)
				break
			}
		} else {
			err = store.Write(ctx, extentID, int64(currFixOffset), int64(reply.Size), reply.Data[0:reply.Size], reply.CRC, storage.AppendWriteType, BufferWrite)
		}

		// write to the local extent file
		if err != nil {
			err = errors.Trace(err, "streamRepairExtent repair data error ")
			return
		}
		hasRecoverySize += currRecoverySize
		currFixOffset += currRecoverySize
		if currFixOffset >= remoteSize {
			break
		}

	}

	dp.monitorData[proto.ActionRepairWrite].UpdateData(hasRecoverySize)

	return

}
