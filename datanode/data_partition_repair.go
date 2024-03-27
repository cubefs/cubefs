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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/exporter"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// DataPartitionRepairTask defines the repair task for the data partition.
type DataPartitionRepairTask struct {
	TaskType                       uint8
	addr                           string
	extents                        map[uint64]*storage.ExtentInfo
	ExtentsToBeCreated             []*storage.ExtentInfo
	ExtentsToBeRepaired            []*storage.ExtentInfo
	LeaderTinyDeleteRecordFileSize int64
	LeaderAddr                     string
}

func NewDataPartitionRepairTask(extentFiles []*storage.ExtentInfo, tinyDeleteRecordFileSize int64, source, leaderAddr string, extentType uint8) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		extents:                        make(map[uint64]*storage.ExtentInfo),
		ExtentsToBeCreated:             make([]*storage.ExtentInfo, 0),
		ExtentsToBeRepaired:            make([]*storage.ExtentInfo, 0),
		LeaderTinyDeleteRecordFileSize: tinyDeleteRecordFileSize,
		LeaderAddr:                     leaderAddr,
		TaskType:                       extentType,
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
func (dp *DataPartition) repair(extentType uint8) {
	start := time.Now().UnixNano()
	log.LogInfof("action[repair] partition(%v) start.", dp.partitionID)

	var tinyExtents []uint64 // unavailable extents
	if proto.IsTinyExtentType(extentType) {
		tinyExtents = dp.brokenTinyExtents()
		if len(tinyExtents) == 0 {
			return
		}
	}

	// fix dp replica index panic , using replica copy
	replica := dp.getReplicaCopy()
	repairTasks := make([]*DataPartitionRepairTask, len(replica))
	err := dp.buildDataPartitionRepairTask(repairTasks, extentType, tinyExtents, replica)
	if err != nil {
		log.LogErrorf(errors.Stack(err))
		log.LogErrorf("action[repair] partition(%v) err(%v).",
			dp.partitionID, err)
		dp.moveToBrokenTinyExtentC(extentType, tinyExtents)
		return
	}
	log.LogInfof("action[repair] partition(%v) before prepareRepairTasks", dp.partitionID)
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
	log.LogDebugf("DoRepair")
	// ask the leader to do the repair
	dp.DoRepair(repairTasks)
	end := time.Now().UnixNano()

	// every time we need to figure out which extents need to be repaired and which ones do not.
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

func (dp *DataPartition) buildDataPartitionRepairTask(repairTasks []*DataPartitionRepairTask, extentType uint8, tinyExtents []uint64, replica []string) (err error) {
	// get the local extent info
	extents, leaderTinyDeleteRecordFileSize, err := dp.getLocalExtentInfo(extentType, tinyExtents)
	if err != nil {
		return err
	}
	// new repair task for the leader
	log.LogInfof("buildDataPartitionRepairTask dp %v, extent type %v, len extent %v, replica size %v",
		dp.partitionID, extentType, len(extents), len(replica))
	repairTasks[0] = NewDataPartitionRepairTask(extents, leaderTinyDeleteRecordFileSize, replica[0], replica[0], extentType)
	repairTasks[0].addr = replica[0]

	// new repair tasks for the followers
	for index := 1; index < len(replica); index++ {
		extents, err := dp.getRemoteExtentInfo(extentType, tinyExtents, replica[index])
		if err != nil {
			log.LogErrorf("buildDataPartitionRepairTask PartitionID(%v) on (%v) err(%v)", dp.partitionID, replica[index], err)
			continue
		}
		log.LogInfof("buildDataPartitionRepairTask dp %v,add new add %v,  extent type %v", dp.partitionID, replica[index], extentType)
		repairTasks[index] = NewDataPartitionRepairTask(extents, leaderTinyDeleteRecordFileSize, replica[index], replica[0], extentType)
		repairTasks[index].addr = replica[index]
	}

	return
}

func (dp *DataPartition) getLocalExtentInfo(extentType uint8, tinyExtents []uint64) (extents []*storage.ExtentInfo, leaderTinyDeleteRecordFileSize int64, err error) {
	var localExtents []*storage.ExtentInfo

	if proto.IsNormalExtentType(extentType) {
		localExtents, leaderTinyDeleteRecordFileSize, err = dp.extentStore.GetAllWatermarks(storage.NormalExtentFilter())
	} else {
		localExtents, leaderTinyDeleteRecordFileSize, err = dp.extentStore.GetAllWatermarks(storage.TinyExtentFilter(tinyExtents))
	}
	if err != nil {
		err = errors.Trace(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
		return
	}
	if len(localExtents) <= 0 {
		extents = make([]*storage.ExtentInfo, 0)
		return
	}
	extents = make([]*storage.ExtentInfo, 0, len(localExtents))
	for _, et := range localExtents {
		newEt := *et
		extents = append(extents, &newEt)
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfo(extentType uint8, tinyExtents []uint64,
	target string) (extentFiles []*storage.ExtentInfo, err error) {
	p := repl.NewPacketToGetAllWatermarks(dp.partitionID, extentType)
	extentFiles = make([]*storage.ExtentInfo, 0)
	if proto.IsTinyExtentType(extentType) {
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
	defer func() {
		gConnPool.PutConnect(conn, err != nil)
	}()
	err = p.WriteToConn(conn) // write command to the remote host
	if err != nil {
		err = errors.Trace(err, "getRemoteExtentInfo DataPartition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	reply := new(repl.Packet)
	err = reply.ReadFromConnWithVer(conn, proto.GetAllWatermarksDeadLineTime) // read the response
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
		if !AutoRepairStatus {
			log.LogWarnf("AutoRepairStatus is False,so cannot Create extent(%v),pid=%d", extentInfo.String(), dp.partitionID)
			continue
		}
		if dp.ExtentStore().IsDeletedNormalExtent(extentInfo.FileID) {
			continue
		}

		dp.disk.allocCheckLimit(proto.IopsWriteType, 1)

		err := store.Create(extentInfo.FileID)
		if err != nil {
			log.LogWarnf("DoRepair dp %v extent %v failed, err:%v",
				dp.partitionID, extentInfo.FileID, err.Error())
			continue
		}
	}
	log.LogDebugf("action[DoRepair] leader to repair len[%v], {%v}", len(repairTasks[0].ExtentsToBeRepaired), repairTasks[0].ExtentsToBeRepaired)
	for _, extentInfo := range repairTasks[0].ExtentsToBeRepaired {
		log.LogDebugf("action[DoRepair] leader to repair len[%v], {%v}", len(repairTasks[0].ExtentsToBeRepaired), extentInfo)
	RETRY:
		err := dp.streamRepairExtent(extentInfo, repl.NewTinyExtentRepairReadPacket, repl.NewExtentRepairReadPacket, repl.NewNormalExtentWithHoleRepairReadPacket, repl.NewPacketEx)
		if err != nil {
			if strings.Contains(err.Error(), storage.NoDiskReadRepairExtentTokenError.Error()) {
				log.LogDebugf("action[DoRepair] retry dp(%v) extent(%v).", dp.partitionID, extentInfo.FileID)
				goto RETRY
			}
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
	if proto.IsTinyExtentType(extentType) {
		dp.extentStore.SendAllToBrokenTinyExtentC(extents)
	}
}

func (dp *DataPartition) sendAllTinyExtentsToC(extentType uint8, availableTinyExtents, brokenTinyExtents []uint64) {
	if !proto.IsTinyExtentType(extentType) {
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
	deleteExtents := make(map[uint64]bool)
	log.LogInfof("action[prepareRepairTasks] dp %v task len %v", dp.partitionID, len(repairTasks))
	for index := 0; index < len(repairTasks); index++ {
		repairTask := repairTasks[index]
		if repairTask == nil {
			continue
		}
		for extentID, extentInfo := range repairTask.extents {
			if extentInfo.IsDeleted {
				deleteExtents[extentID] = true
				continue
			}
			extentWithMaxSize, ok := extentInfoMap[extentID]
			if !ok {
				extentInfoMap[extentID] = extentInfo
			} else {
				if extentInfo.TotalSize() > extentWithMaxSize.TotalSize() {
					extentInfoMap[extentID] = extentInfo
				}
			}
			//			log.LogInfof("action[prepareRepairTasks] dp %v extentid %v addr[dst %v,leader %v] info %v", dp.partitionID, extentID, repairTask.addr, repairTask.LeaderAddr, extentInfoMap[extentID])
		}
	}
	for extentID := range deleteExtents {
		extentInfo := extentInfoMap[extentID]
		if extentInfo != nil {
			extentInfo.IsDeleted = true
			extentInfoMap[extentID] = extentInfo
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
			if _, ok := repairTask.extents[extentID]; !ok && !extentInfo.IsDeleted {
				if storage.IsTinyExtent(extentID) {
					continue
				}
				if extentInfo.IsDeleted {
					continue
				}
				if dp.ExtentStore().IsDeletedNormalExtent(extentID) {
					continue
				}
				ei := &storage.ExtentInfo{Source: extentInfo.Source, FileID: extentID, Size: extentInfo.Size, SnapshotDataOff: extentInfo.SnapshotDataOff}
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
			if dp.ExtentStore().IsDeletedNormalExtent(extentID) {
				continue
			}
			if extentInfo.TotalSize() < maxFileInfo.TotalSize() {
				fixExtent := &storage.ExtentInfo{Source: maxFileInfo.Source, FileID: extentID, Size: maxFileInfo.Size, SnapshotDataOff: maxFileInfo.SnapshotDataOff}
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
	// target := dp.getReplicaAddr(index)
	// fix repair case panic,may be dp's replicas is change
	target := members[index].addr

	p.Data, _ = json.Marshal(members[index])
	p.Size = uint32(len(p.Data))
	conn, err = gConnPool.GetConnect(target)
	defer func() {
		wg.Done()
		if err == nil {
			log.LogInfof(ActionNotifyFollowerToRepair+" to host(%v) Partition(%v) done", target, dp.partitionID)
		} else {
			log.LogErrorf(ActionNotifyFollowerToRepair+" to host(%v) Partition(%v) failed, err(%v)", target, dp.partitionID, err)
		}
	}()
	if err != nil {
		return err
	}
	defer func() {
		gConnPool.PutConnect(conn, err != nil)
	}()
	if err = p.WriteToConn(conn); err != nil {
		return err
	}
	if err = p.ReadFromConnWithVer(conn, proto.NoReadDeadlineTime); err != nil {
		return err
	}
	return err
}

func attachAvaliSizeOnExtentRepairRead(reply repl.PacketInterface, avaliSize uint64) {
	binary.BigEndian.PutUint64(reply.GetArg()[9:17], avaliSize)
}

func (dp *DataPartition) ExtentWithHoleRepairRead(request repl.PacketInterface, connect net.Conn, getReplyPacket func() repl.PacketInterface) {
	var (
		err                 error
		needReplySize       int64
		tinyExtentFinfoSize uint64
		crc                 uint32
	)
	defer func() {
		if err != nil {
			request.PackErrorBody(ActionStreamReadTinyExtentRepair, err.Error())
			request.WriteToConn(connect)
		}
	}()
	store := dp.ExtentStore()
	tinyExtentFinfoSize, err = store.GetExtentFinfoSize(request.GetExtentID())
	if err != nil {
		return
	}
	needReplySize = int64(request.GetSize())
	offset := request.GetExtentOffset()
	if uint64(request.GetExtentOffset())+uint64(request.GetSize()) > tinyExtentFinfoSize {
		needReplySize = int64(tinyExtentFinfoSize - uint64(request.GetExtentOffset()))
	}
	avaliReplySize := uint64(needReplySize)

	var newOffset, newEnd int64
	for {
		if needReplySize <= 0 {
			break
		}
		reply := getReplyPacket()
		reply.SetArglen(TinyExtentRepairReadResponseArgLen)
		reply.SetArg(make([]byte, TinyExtentRepairReadResponseArgLen))
		attachAvaliSizeOnExtentRepairRead(reply, avaliReplySize)
		newOffset, newEnd, err = dp.extentStore.GetExtentWithHoleAvailableOffset(request.GetExtentID(), offset)
		if err != nil {
			return
		}
		if newOffset > offset {
			var replySize int64
			if replySize, err = writeEmptyPacketOnExtentRepairRead(reply, newOffset, offset, connect); err != nil {
				return
			}
			needReplySize -= replySize
			offset += replySize
			continue
		}
		currNeedReplySize := newEnd - newOffset
		currReadSize := uint32(util.Min(int(currNeedReplySize), util.ReadBlockSize))
		if currReadSize == util.ReadBlockSize {
			data, _ := proto.Buffers.Get(util.ReadBlockSize)
			reply.SetData(data)
		} else {
			reply.SetData(make([]byte, currReadSize))
		}
		reply.SetExtentOffset(offset)
		crc, err = dp.extentStore.Read(reply.GetExtentID(), offset, int64(currReadSize), reply.GetData(), false)
		if err != nil {
			return
		}
		reply.SetCRC(crc)
		reply.SetSize(currReadSize)
		reply.SetResultCode(proto.OpOk)
		if err = reply.WriteToConn(connect); err != nil {
			connect.Close()
			return
		}
		needReplySize -= int64(currReadSize)
		offset += int64(currReadSize)
		if currReadSize == util.ReadBlockSize {
			proto.Buffers.Put(reply.GetData())
		}
		if connect.RemoteAddr() != nil { // conn in testcase may not initialize
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.GetStartT(), err))
			log.LogReadf(logContent)
		}
	}

	request.PacketOkReply()
}

func (dp *DataPartition) NormalExtentRepairRead(p repl.PacketInterface, connect net.Conn, isRepairRead bool,
	metrics *DataNodeMetrics, makeRspPacket repl.MakeStreamReadResponsePacket) (err error) {
	var (
		metricPartitionIOLabels     map[string]string
		partitionIOMetric, tpObject *exporter.TimePointCount
	)
	shallDegrade := p.ShallDegrade()
	if !shallDegrade {
		metricPartitionIOLabels = GetIoMetricLabels(dp, "read")
	}
	needReplySize := p.GetSize()
	offset := p.GetExtentOffset()
	store := dp.ExtentStore()

	log.LogDebugf("extentRepairReadPacket dp %v offset %v needSize %v", dp.partitionID, offset, needReplySize)
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		reply := makeRspPacket(p.GetReqID(), p.GetPartitionID(), p.GetExtentID())
		reply.SetStartT(p.GetStartT())
		currReadSize := uint32(util.Min(int(needReplySize), util.ReadBlockSize))
		if currReadSize == util.ReadBlockSize {
			data, _ := proto.Buffers.Get(util.ReadBlockSize)
			reply.SetData(data)
		} else {
			reply.SetData(make([]byte, currReadSize))
		}
		if !shallDegrade {
			partitionIOMetric = exporter.NewTPCnt(MetricPartitionIOName)
			tpObject = exporter.NewTPCnt(fmt.Sprintf("Repair_%s", p.GetOpMsg()))
		}
		reply.SetExtentOffset(offset)
		p.SetSize(currReadSize)
		p.SetExtentOffset(offset)

		dp.Disk().allocCheckLimit(proto.IopsReadType, 1)
		dp.Disk().allocCheckLimit(proto.FlowReadType, currReadSize)

		dp.disk.limitRead.Run(int(currReadSize), func() {
			var crc uint32
			crc, err = store.Read(reply.GetExtentID(), offset, int64(currReadSize), reply.GetData(), isRepairRead)
			reply.SetCRC(crc)
		})
		if !shallDegrade && metrics != nil {
			metrics.MetricIOBytes.AddWithLabels(int64(p.GetSize()), metricPartitionIOLabels)
			partitionIOMetric.SetWithLabels(err, metricPartitionIOLabels)
			tpObject.Set(err)
		}
		dp.checkIsDiskError(err, ReadFlag)
		p.SetCRC(reply.GetCRC())
		if err != nil {
			log.LogErrorf("action[operatePacket] err %v", err)
			return
		}
		reply.SetSize(currReadSize)
		reply.SetResultCode(proto.OpOk)
		reply.SetOpCode(p.GetOpcode())
		p.SetResultCode(proto.OpOk)
		if err = reply.WriteToConn(connect); err != nil {
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		if currReadSize == util.ReadBlockSize {
			proto.Buffers.Put(reply.GetData())
		}
		if connect.RemoteAddr() != nil {
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.GetStartT(), err))
			log.LogReadf(logContent)
		}
	}
	return
}

// NotifyExtentRepair notifies the followers to repair.
func (dp *DataPartition) NotifyExtentRepair(members []*DataPartitionRepairTask) (err error) {
	wg := new(sync.WaitGroup)
	for i := 1; i < len(members); i++ {
		if members[i] == nil || !dp.IsExistReplica(members[i].addr) {
			if members[i] != nil {
				log.LogInfof("notify extend repair is change ,index(%v),pid(%v),task_member_add(%v),IsExistReplica(%v)",
					i, dp.partitionID, members[i].addr, dp.IsExistReplica(members[i].addr))
			}
			continue
		}

		wg.Add(1)
		go dp.notifyFollower(wg, i, members)
	}
	wg.Wait()
	return
}

// DoStreamExtentFixRepair executes the repair on the followers.
func (dp *DataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.ExtentInfo) {
	defer wg.Done()
RETRY:
	err := dp.streamRepairExtent(remoteExtentInfo, repl.NewTinyExtentRepairReadPacket, repl.NewExtentRepairReadPacket, repl.NewNormalExtentWithHoleRepairReadPacket, repl.NewPacketEx)
	if err != nil {
		if strings.Contains(err.Error(), storage.NoDiskReadRepairExtentTokenError.Error()) {
			log.LogWarnf("action[DoRepair] retry dp(%v) extent(%v).", dp.partitionID, remoteExtentInfo.FileID)
			goto RETRY
		}
		// only decommission repair need to check err cnt
		if dp.isDecommissionRecovering() {
			atomic.AddUint64(&dp.recoverErrCnt, 1)
			if atomic.LoadUint64(&dp.recoverErrCnt) >= dp.dataNode.GetDpMaxRepairErrCnt() {
				dp.handleDecommissionRecoverFailed()
				return
			}
		}
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
func (dp *DataPartition) streamRepairExtent(remoteExtentInfo *storage.ExtentInfo,
	tinyPackFunc, normalPackFunc, normalWithHoleFunc repl.MakeExtentRepairReadPacket,
	newPack repl.NewPacketFunc) (err error,
) {
	defer func() {
		if err != nil {
			log.LogWarnf("streamRepairExtent: dp %v remote info %v execute repair failed, err %s",
				dp.partitionID, remoteExtentInfo, err.Error())
		}
	}()

	log.LogDebugf("streamRepairExtent dp %v remote info %v", dp.partitionID, remoteExtentInfo)

	store := dp.ExtentStore()
	if !store.HasExtent(remoteExtentInfo.FileID) {
		log.LogDebugf("streamRepairExtent dp %v extent %v not exist", dp.partitionID, remoteExtentInfo)
		return
	}
	if !AutoRepairStatus && !storage.IsTinyExtent(remoteExtentInfo.FileID) {
		log.LogWarnf("streamRepairExtent dp %v AutoRepairStatus is False,so cannot AutoRepair extent(%v)", dp.partitionID,
			remoteExtentInfo.String())
		return
	}
	localExtentInfo, err := store.Watermark(remoteExtentInfo.FileID)
	if err != nil {
		log.LogDebugf("streamRepairExtent local %v remote info %v", localExtentInfo, remoteExtentInfo)
		return errors.Trace(err, "streamRepairExtent Watermark error")
	}
	log.LogDebugf("streamRepairExtent dp %v remote info %v,local %v", dp.partitionID, remoteExtentInfo, localExtentInfo)
	if dp.ExtentStore().IsDeletedNormalExtent(remoteExtentInfo.FileID) {
		log.LogDebugf("streamRepairExtent  dp %v local %v remote info %v", dp.partitionID, localExtentInfo, remoteExtentInfo)
		return nil
	}

	if localExtentInfo.Size >= remoteExtentInfo.Size && localExtentInfo.SnapshotDataOff >= remoteExtentInfo.SnapshotDataOff {
		log.LogDebugf("streamRepairExtent  dp %v local %v remote info %v", dp.partitionID, localExtentInfo, remoteExtentInfo)
		return nil
	}

	doWork := func(wType int, currFixOffset uint64, dstOffset uint64, request repl.PacketInterface) (err error) {
		log.LogDebugf("streamRepairExtent. currFixOffset %v dstOffset %v, request %v", currFixOffset, dstOffset, request)
		var conn net.Conn
		conn, err = dp.getRepairConn(remoteExtentInfo.Source)
		if err != nil {
			return errors.Trace(err, "streamRepairExtent get conn from host(%v) error", remoteExtentInfo.Source)
		}
		defer func() {
			if dp.enableSmux() {
				dp.putRepairConn(conn, true)
			} else {
				dp.putRepairConn(conn, err != nil)
			}
		}()

		if err = request.WriteToConn(conn); err != nil {
			err = errors.Trace(err, "streamRepairExtent send streamRead to host(%v) error", remoteExtentInfo.Source)
			log.LogWarnf("action[streamRepairExtent] dp %v err(%v).", dp.partitionID, err)
			return
		}
		log.LogDebugf("streamRepairExtent dp %v extent %v currFixOffset %v dstOffset %v, request %v", dp.partitionID,
			remoteExtentInfo.FileID, currFixOffset, dstOffset, request)
		var hasRecoverySize uint64
		var loopTimes uint64
		for currFixOffset < dstOffset {
			if dp.dataNode.space.Partition(dp.partitionID) == nil {
				log.LogWarnf("streamRepairExtent dp %v is detached, quit repair",
					dp.partitionID)
				return
			}
			if currFixOffset >= dstOffset {
				break
			}
			reply := newPack() // repl.NewPacket()

			// read 64k streaming repair packet
			if err = reply.ReadFromConnWithVer(conn, 60); err != nil {
				err = errors.Trace(err, "streamRepairExtent dp %v extent %v receive data error,localExtentSize(%v) remoteExtentSize(%v)",
					dp.partitionID, remoteExtentInfo.FileID, currFixOffset, dstOffset)
				log.LogWarnf("%v", err.Error())
				return
			}

			if reply.GetResultCode() != proto.OpOk {
				if reply.GetResultCode() == proto.OpReadRepairExtentAgain {
					log.LogDebugf("streamRepairExtent dp %v extent %v wait for token", dp.partitionID, remoteExtentInfo.FileID)
					time.Sleep(time.Second * 5)
					return storage.NoDiskReadRepairExtentTokenError
				} else {
					err = errors.Trace(fmt.Errorf("unknow result code"),
						"streamRepairExtent dp %v extent %v receive opcode error(%v) ,localExtentSize(%v) remoteExtentSize(%v)",
						dp.partitionID, remoteExtentInfo.FileID, string(reply.GetData()[:intMin(len(reply.GetData()), int(reply.GetSize()))]), currFixOffset, dstOffset)
					log.LogWarnf("%v", err.Error())
					return
				}
			}

			if reply.GetReqID() != request.GetReqID() || reply.GetPartitionID() != request.GetPartitionID() ||
				reply.GetExtentID() != request.GetExtentID() {
				err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
					"request(%v) reply(%v) ,localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, dstOffset)
				return
			}

			if !storage.IsTinyExtent(reply.GetExtentID()) &&
				!(reply.GetOpcode() == proto.OpSnapshotExtentRepairRead) &&
				(reply.GetSize() == 0 || reply.GetExtentOffset() != int64(currFixOffset)) {
				err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
					"request(%v) reply(%v) localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, dstOffset)
				return
			}
			if loopTimes%100 == 0 {
				log.LogInfof(fmt.Sprintf("action[streamRepairExtent] dp %v extent %v fix(%v) start fix from (%v)"+
					" remoteSize(%v)localSize(%v) reply(%v).", dp.partitionID, remoteExtentInfo.FileID, localExtentInfo.FileID, remoteExtentInfo.String(),
					dstOffset, currFixOffset, reply.GetUniqueLogId()))
			}
			loopTimes++
			actualCrc := crc32.ChecksumIEEE(reply.GetData()[:reply.GetSize()])
			if reply.GetCRC() != actualCrc {
				err = fmt.Errorf("streamRepairExtent crc mismatch expectCrc(%v) actualCrc(%v) extent(%v_%v) start fix from (%v)"+
					" remoteSize(%v) localSize(%v) request(%v) reply(%v) ", reply.GetCRC(), actualCrc, dp.partitionID, remoteExtentInfo.String(),
					remoteExtentInfo.Source, dstOffset, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())

				return errors.Trace(err, "streamRepairExtent receive data error")
			}
			isEmptyResponse := false
			var remoteAvaliSize uint64
			currRecoverySize := uint64(reply.GetSize())
			// Write it to local extent file
			if storage.IsTinyExtent(uint64(localExtentInfo.FileID)) ||
				reply.GetOpcode() == proto.OpSnapshotExtentRepairRead {
				if reply.GetArgLen() == TinyExtentRepairReadResponseArgLen {
					remoteAvaliSize = binary.BigEndian.Uint64(reply.GetArg()[9:TinyExtentRepairReadResponseArgLen])
				} else if reply.GetArgLen() == NormalExtentWithHoleRepairReadResponseArgLen {
					remoteAvaliSize = binary.BigEndian.Uint64(reply.GetArg()[9:NormalExtentWithHoleRepairReadResponseArgLen])
				}
				if reply.GetArg() != nil { // compact v1.2.0 recovery
					isEmptyResponse = reply.GetArg()[0] == EmptyResponse
				}
				if isEmptyResponse {
					currRecoverySize = binary.BigEndian.Uint64(reply.GetArg()[1:9])
					reply.SetSize(uint32(currRecoverySize))
				}
			}
			log.LogDebugf("streamRepairExtent dp[%v] extent[%v] localExtentInfo[%v] remote info(remoteAvaliSize[%v],isEmptyResponse[%v],currRecoverySize[%v]",
				dp.partitionID, localExtentInfo, remoteExtentInfo, remoteAvaliSize, isEmptyResponse, currRecoverySize)
			if storage.IsTinyExtent(localExtentInfo.FileID) {
				err = store.TinyExtentRecover(uint64(localExtentInfo.FileID), int64(currFixOffset), int64(currRecoverySize), reply.GetData(), reply.GetCRC(), isEmptyResponse)
				if hasRecoverySize+currRecoverySize >= remoteAvaliSize {
					log.LogInfof("streamRepairTinyExtent(%v) recover fininsh,remoteAvaliSize(%v) "+
						"hasRecoverySize(%v) currRecoverySize(%v)", dp.applyRepairKey(int(localExtentInfo.FileID)),
						remoteAvaliSize, hasRecoverySize+currRecoverySize, currRecoverySize)
					break
				}
			} else {
				log.LogDebugf("streamRepairExtent reply size %v, currFixoffset %v, reply %v ", reply.GetSize(), currFixOffset, reply)
				_, err = store.Write(uint64(localExtentInfo.FileID), int64(currFixOffset), int64(reply.GetSize()), reply.GetData(), reply.GetCRC(), wType, BufferWrite, isEmptyResponse)
			}
			// log.LogDebugf("streamRepairExtent reply size %v, currFixoffset %v, reply %v err %v", reply.Size, currFixOffset, reply, err)
			// write to the local extent file
			if err != nil {
				err = errors.Trace(err, "streamRepairExtent repair data error ")
				return
			}
			hasRecoverySize += uint64(reply.GetSize())
			currFixOffset += uint64(reply.GetSize())
			if currFixOffset >= dstOffset {
				log.LogWarnf(fmt.Sprintf("action[streamRepairExtent] dp %v extent(%v) start fix from (%v)"+
					" remoteSize(%v)localSize(%v) reply(%v).", dp.partitionID, localExtentInfo.FileID, remoteExtentInfo.String(),
					dstOffset, currFixOffset, reply.GetUniqueLogId()))
				break
			}
		}
		return
	}

	// size difference between the local extent and the remote extent
	var request repl.PacketInterface
	sizeDiff := remoteExtentInfo.Size - localExtentInfo.Size

	if storage.IsTinyExtent(remoteExtentInfo.FileID) {
		if sizeDiff >= math.MaxUint32 {
			sizeDiff = math.MaxUint32 - util.MB
		}
		request = tinyPackFunc(dp.partitionID, remoteExtentInfo.FileID, int(localExtentInfo.Size), int(sizeDiff))
		currFixOffset := localExtentInfo.Size
		return doWork(0, currFixOffset, remoteExtentInfo.Size, request)
	} else if remoteExtentInfo.SnapshotDataOff == util.ExtentSize {
		request = normalPackFunc(dp.partitionID, remoteExtentInfo.FileID, int(localExtentInfo.Size), int(sizeDiff))
		currFixOffset := localExtentInfo.Size
		if err = doWork(storage.AppendWriteType, currFixOffset, remoteExtentInfo.Size, request); err != nil {
			log.LogErrorf("streamRepairExtent. local info %v, remote %v.err(%v)", localExtentInfo, remoteExtentInfo, err)
			return
		}
	} else {
		log.LogDebugf("streamRepairExtent. local info %v, remote %v", localExtentInfo, remoteExtentInfo)
		if sizeDiff > 0 {
			log.LogDebugf("streamRepairExtent. local info %v, remote %v", localExtentInfo, remoteExtentInfo)
			request = normalWithHoleFunc(dp.partitionID, remoteExtentInfo.FileID, int(localExtentInfo.Size), int(sizeDiff))
			currFixOffset := localExtentInfo.Size
			if err = doWork(storage.AppendWriteType, currFixOffset, remoteExtentInfo.Size, request); err != nil {
				log.LogErrorf("streamRepairExtent. local info %v, remote %v.err(%v)", localExtentInfo, remoteExtentInfo, err)
				return
			}
		}
		sizeDiffVerAppend := remoteExtentInfo.SnapshotDataOff - localExtentInfo.SnapshotDataOff
		request = normalWithHoleFunc(dp.partitionID, remoteExtentInfo.FileID, int(localExtentInfo.SnapshotDataOff), int(sizeDiffVerAppend))
		currFixOffset := localExtentInfo.SnapshotDataOff
		return doWork(storage.AppendRandomWriteType, currFixOffset, remoteExtentInfo.SnapshotDataOff, request)
	}

	return
}

func intMin(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
