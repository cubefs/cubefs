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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/tiglabs/raft"
	raftProto "github.com/tiglabs/raft/proto"
)

func (s *DataNode) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size

	tpObject := exporter.NewModuleTP(p.GetOpMsg())
	s.rateLimit(p, c)

	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op(%v) error(%v)", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			switch p.Opcode {
			case proto.OpStreamRead, proto.OpRead, proto.OpExtentRepairRead, proto.OpStreamFollowerRead:
			case proto.OpReadTinyDeleteRecord:
				if log.IsReadEnabled() {
					log.LogReadf("action[OperatePacket] %v.",
						p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
				}
			case proto.OpWrite, proto.OpRandomWrite, proto.OpSyncRandomWrite, proto.OpSyncWrite, proto.OpMarkDelete:
				if log.IsWriteEnabled() {
					log.LogWritef("action[OperatePacket] %v.",
						p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
				}
			default:
				if log.IsInfoEnabled() {
					log.LogInfof("action[OperatePacket] %v.",
						p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
				}
			}
		}
		p.Size = resultSize
		tpObject.Set(err)
	}()

	switch p.Opcode {
	case proto.OpCreateExtent:
		s.handlePacketToCreateExtent(p)
	case proto.OpWrite, proto.OpSyncWrite:
		s.handleWritePacket(p)
	case proto.OpStreamRead:
		s.handleStreamReadPacket(p, c, StreamRead)
	case proto.OpStreamFollowerRead:
		s.handleStreamFollowerReadPacket(p, c, StreamRead)
	case proto.OpExtentRepairRead:
		s.handleExtentRepairReadPacket(p, c, RepairRead)
	case proto.OpTinyExtentRepairRead:
		s.handleTinyExtentRepairRead(p, c)
	case proto.OpTinyExtentAvaliRead:
		s.handleTinyExtentAvaliRead(p, c)
	case proto.OpMarkDelete:
		s.handleMarkDeletePacket(p, c)
	case proto.OpBatchDeleteExtent:
		s.handleBatchMarkDeletePacket(p, c)
	case proto.OpRandomWrite, proto.OpSyncRandomWrite:
		s.handleRandomWritePacket(p)
	case proto.OpNotifyReplicasToRepair:
		s.handlePacketToNotifyExtentRepair(p)
	case proto.OpGetAllWatermarks:
		s.handlePacketToGetAllWatermarks(p)
	case proto.OpGetAllWatermarksV2:
		s.handlePacketToGetAllWatermarksV2(p)
	case proto.OpGetAllExtentInfo:
		s.handlePacketToGetAllExtentInfo(p)
	case proto.OpCreateDataPartition:
		s.handlePacketToCreateDataPartition(p)
	case proto.OpLoadDataPartition:
		s.handlePacketToLoadDataPartition(p)
	case proto.OpDeleteDataPartition:
		s.handlePacketToDeleteDataPartition(p)
	case proto.OpDataNodeHeartbeat:
		s.handleHeartbeatPacket(p)
	case proto.OpGetAppliedId:
		s.handlePacketToGetAppliedID(p)
	case proto.OpDecommissionDataPartition:
		s.handlePacketToDecommissionDataPartition(p)
	case proto.OpAddDataPartitionRaftMember:
		s.handlePacketToAddDataPartitionRaftMember(p)
	case proto.OpRemoveDataPartitionRaftMember:
		s.handlePacketToRemoveDataPartitionRaftMember(p)
	case proto.OpAddDataPartitionRaftLearner:
		s.handlePacketToAddDataPartitionRaftLearner(p)
	case proto.OpPromoteDataPartitionRaftLearner:
		s.handlePacketToPromoteDataPartitionRaftLearner(p)
	case proto.OpResetDataPartitionRaftMember:
		s.handlePacketToResetDataPartitionRaftMember(p)
	case proto.OpDataPartitionTryToLeader:
		s.handlePacketToDataPartitionTryToLeader(p)
	case proto.OpGetPartitionSize:
		s.handlePacketToGetPartitionSize(p)
	case proto.OpGetMaxExtentIDAndPartitionSize:
		s.handlePacketToGetMaxExtentIDAndPartitionSize(p)
	case proto.OpReadTinyDeleteRecord:
		s.handlePacketToReadTinyDeleteRecordFile(p, c)
	case proto.OpBroadcastMinAppliedID:
		s.handleBroadcastMinAppliedID(p)
	case proto.OpSyncDataPartitionReplicas:
		s.handlePacketToSyncDataPartitionReplicas(p)
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}

	return
}

// Handle OpCreateExtent packet.
func (s *DataNode) handlePacketToCreateExtent(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateExtent, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*DataPartition)
	if partition.Used() > partition.Size()*2 || partition.disk.Status == proto.ReadOnly || partition.IsRejectWrite() {
		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}
	err = partition.ExtentStore().Create(p.ExtentID, true)
	return
}

// Handle OpCreateDataPartition packet.
func (s *DataNode) handlePacketToCreateDataPartition(p *repl.Packet) {
	var (
		err   error
		bytes []byte
		dp    *DataPartition
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateDataPartition, err.Error())
		}
	}()
	task := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, task); err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	request := &proto.CreateDataPartitionRequest{}
	if task.OpCode != proto.OpCreateDataPartition {
		err = fmt.Errorf("from master Task(%v) failed,error unavali opcode(%v)", task.ToString(), task.OpCode)
		return
	}

	bytes, err = json.Marshal(task.Request)
	if err != nil {
		err = fmt.Errorf("from master Task(%v) cannot unmashal CreateDataPartition", task.ToString())
		return
	}
	p.AddMesgLog(string(bytes))
	if err = json.Unmarshal(bytes, request); err != nil {
		err = fmt.Errorf("from master Task(%v) cannot unmash CreateDataPartitionRequest struct", task.ToString())
		return
	}
	p.PartitionID = request.PartitionId
	if dp, err = s.space.CreatePartition(request); err != nil {
		err = fmt.Errorf("from master Task(%v) cannot create Partition err(%v)", task.ToString(), err)
		return
	}
	p.PacketOkWithBody([]byte(dp.Disk().Path))

	return
}

// Handle OpHeartbeat packet.
func (s *DataNode) handleHeartbeatPacket(p *repl.Packet) {
	var err error
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateDataPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}

	go func() {
		if task.IsHeartBeatPbRequest {
			s.responseHeartbeatPb(task)
		} else {
			s.responseHeartbeat(task)
		}
	}()

}

// Handle OpDeleteDataPartition packet.
func (s *DataNode) handlePacketToDeleteDataPartition(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDeleteDataPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}
	request := &proto.DeleteDataPartitionRequest{}
	if task.OpCode == proto.OpDeleteDataPartition {
		bytes, _ := json.Marshal(task.Request)
		p.AddMesgLog(string(bytes))
		err = json.Unmarshal(bytes, request)
		if err != nil {
			return
		} else {
			s.space.ExpiredPartition(request.PartitionId)
		}
	} else {
		err = fmt.Errorf("illegal opcode ")
	}
	if err != nil {
		err = errors.Trace(err, "delete DataPartition failed,PartitionID(%v)", request.PartitionId)
		log.LogErrorf("action[handlePacketToDeleteDataPartition] err(%v).", err)
	}
	log.LogInfof(fmt.Sprintf("action[handlePacketToDeleteDataPartition] %v error(%v)", request.PartitionId, err))

}

// Handle OpLoadDataPartition packet.
func (s *DataNode) handlePacketToLoadDataPartition(p *repl.Packet) {
	task := &proto.AdminTask{}
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionLoadDataPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	err = json.Unmarshal(p.Data, task)
	p.PacketOkReply()
	go s.asyncLoadDataPartition(task)
}

func (s *DataNode) asyncLoadDataPartition(task *proto.AdminTask) {
	var (
		err error
	)
	request := &proto.LoadDataPartitionRequest{}
	response := &proto.LoadDataPartitionResponse{}
	if task.OpCode == proto.OpLoadDataPartition {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		dp := s.space.Partition(request.PartitionId)
		if dp == nil {
			response.Status = proto.TaskFailed
			response.PartitionId = uint64(request.PartitionId)
			err = fmt.Errorf(fmt.Sprintf("DataPartition(%v) not found", request.PartitionId))
			response.Result = err.Error()
		} else {
			response = dp.Load()
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskSucceeds
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFailed
		err = fmt.Errorf("illegal opcode")
		response.Result = err.Error()
	}
	task.Response = response
	if err = MasterClient.NodeAPI().ResponseDataNodeTask(task); err != nil {
		err = errors.Trace(err, "load DataPartition failed,PartitionID(%v)", request.PartitionId)
		log.LogError(errors.Stack(err))
	}
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleMarkDeletePacket(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	remote := c.RemoteAddr().String()
	partition := p.Object.(*DataPartition)
	if p.ExtentType == proto.TinyExtentType {
		ext := new(proto.TinyExtentDeleteRecord)
		err = json.Unmarshal(p.Data, ext)
		if err == nil {
			log.LogInfof("handleMarkDeletePacket Delete PartitionID(%v)_Extent(%v)_Offset(%v)_Size(%v) from(%v)",
				p.PartitionID, p.ExtentID, ext.ExtentOffset, ext.Size, remote)
			partition.ExtentStore().MarkDelete(p.ExtentID, int64(ext.ExtentOffset), int64(ext.Size))
		}
	} else {
		log.LogInfof("handleMarkDeletePacket Delete PartitionID(%v)_Extent(%v) from(%v)",
			p.PartitionID, p.ExtentID, remote)
		partition.ExtentStore().MarkDelete(p.ExtentID, 0, 0)
	}
	_ = partition.RemoveIssueExtent(p.ExtentID)
	if err != nil {
		p.PackErrorBody(ActionMarkDelete, err.Error())
	} else {
		p.PacketOkReply()
	}

	partition.monitorData[proto.ActionMarkDelete].UpdateData(0)
	return
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleBatchMarkDeletePacket(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	remote := c.RemoteAddr().String()
	partition := p.Object.(*DataPartition)
	var exts []*proto.ExtentKey
	err = json.Unmarshal(p.Data[0:p.Size], &exts)
	store := partition.ExtentStore()
	if err == nil {
		for _, ext := range exts {
			DeleteLimiterWait()
			log.LogInfof("handleBatchMarkDeletePacket Delete PartitionID(%v)_Extent(%v)_Offset(%v)_Size(%v) from(%v)",
				p.PartitionID, ext.ExtentId, ext.ExtentOffset, ext.Size, remote)
			store.MarkDelete(ext.ExtentId, int64(ext.ExtentOffset), int64(ext.Size))
			_ = partition.RemoveIssueExtent(p.ExtentID)
		}
	}

	if err != nil {
		log.LogErrorf(fmt.Sprintf("(%v) error(%v) data(%v)", p.GetUniqueLogId(), err, string(p.Data)))
		p.PackErrorBody(ActionMarkDelete, err.Error())
	} else {
		p.PacketOkReply()
	}

	partition.monitorData[proto.ActionBatchMarkDelete].UpdateData(uint64(len(exts)))
	return
}

// Handle OpWrite packet.
func (s *DataNode) handleWritePacket(p *repl.Packet) {
	var err error
	partition := p.Object.(*DataPartition)
	defer func() {
		partition.monitorData[proto.ActionAppendWrite].UpdateData(uint64(p.Size))
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	if partition.Used() > partition.Size()*2 || partition.disk.Status == proto.ReadOnly || partition.IsRejectWrite() {
		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}

	store := partition.ExtentStore()
	if p.ExtentType == proto.TinyExtentType {
		err = store.Write(p.Ctx(), p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data[0:p.Size], p.CRC, storage.AppendWriteType, p.IsSyncWrite())
		s.incDiskErrCnt(p.PartitionID, err, WriteFlag)
		return
	}

	if p.Size <= unit.BlockSize {
		err = store.Write(p.Ctx(), p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data[0:p.Size], p.CRC, storage.AppendWriteType, p.IsSyncWrite())
		partition.checkIsDiskError(err)
	} else {
		size := p.Size
		offset := 0
		for size > 0 {
			if size <= 0 {
				break
			}
			currSize := unit.Min(int(size), unit.BlockSize)
			data := p.Data[offset : offset+currSize]
			crc := crc32.ChecksumIEEE(data)
			err = store.Write(p.Ctx(), p.ExtentID, p.ExtentOffset+int64(offset), int64(currSize), data[0:currSize], crc, storage.AppendWriteType, p.IsSyncWrite())
			partition.checkIsDiskError(err)
			if err != nil {
				break
			}
			size -= uint32(currSize)
			offset += currSize
		}
	}
	s.incDiskErrCnt(p.PartitionID, err, WriteFlag)
	return
}

func (s *DataNode) handleRandomWritePacket(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*DataPartition)
	if partition.IsRandomWriteDisabled() {
		err = proto.ErrOperationDisabled
		return
	}
	_, isLeader := partition.IsRaftLeader()
	if !isLeader {
		err = raft.ErrNotLeader
		return
	}
	err = partition.RandomWriteSubmit(p)
	if err != nil && strings.Contains(err.Error(), raft.ErrNotLeader.Error()) {
		err = raft.ErrNotLeader
		return
	}

	if err == nil && p.ResultCode != proto.OpOk {
		err = storage.TryAgainError
		return
	}
}

//func (s *DataNode) handleRandomWritePacketV3(p *repl.Packet) {
//	var err error
//	defer func() {
//		if err != nil {
//			p.PackErrorBody(ActionWrite, err.Error())
//		} else {
//			p.PacketOkReply()
//		}
//	}()
//	partition := p.Object.(*DataPartition)
//	_, isLeader := partition.IsRaftLeader()
//	if !isLeader {
//		err = raft.ErrNotLeader
//		return
//	}
//	err = partition.RandomWriteSubmitV3(p)
//	if err != nil && strings.Contains(err.Error(), raft.ErrNotLeader.Error()) {
//		err = raft.ErrNotLeader
//		return
//	}
//
//	if err == nil && p.ResultCode != proto.OpOk {
//		err = storage.TryAgainError
//		return
//	}
//}

func (s *DataNode) handleStreamReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamRead, err.Error())
			p.WriteToConn(connect, proto.WriteDeadlineTime)
		}
	}()

	// 确保Raft服务以及Partition的Raft实例已经启动，否则拒绝服务。
	if s.raftStore == nil {
		err = proto.ErrOperationDisabled
		return
	}
	var partition = p.Object.(*DataPartition)
	if !partition.IsRaftStarted() {
		err = proto.ErrOperationDisabled
		return
	}

	// 检查所请求Partition的一致性模式(Consistency Mode)， 若为标准模式(StandardMode)则仅在当前Partition实例Raft复制组内Leader角色时才提供服务。
	// 标准模式(StandardMode)下Raft采用标准的超半数复制提交机制，这种模式下仅Leader角色可以保证数据的绝对正确。
	// 严格模式(StrictMode)下Raft实例使用了特殊的复制提交机制，数据操作请求被强行要求所有成员全部复制成功才会被提交，所以不需要检查当前实例是否为Leader角色。
	if partition.GetConsistencyMode() == proto.StandardMode {
		if _, err = partition.CheckLeader(); err != nil {
			return
		}
	}

	s.handleExtentRepairReadPacket(p, connect, isRepairRead)
	return
}

func (s *DataNode) handleStreamFollowerReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	s.handleExtentRepairReadPacket(p, connect, isRepairRead)

	return
}

func (s *DataNode) handleExtentRepairReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), connect.RemoteAddr().String(), p.StartT, err))
			log.LogErrorf(logContent)
			p.PackErrorBody(ActionStreamRead, err.Error())
			p.WriteToConn(connect, proto.WriteDeadlineTime)
		}
	}()
	partition := p.Object.(*DataPartition)
	needReplySize := p.Size
	offset := p.ExtentOffset
	store := partition.ExtentStore()
	if isRepairRead {
		if !partition.Disk().canRepairOnDisk() {
			err = fmt.Errorf("disk(%v) limit on handleExtentRepairRead", partition.Disk().Path)
			return
		}
		defer func() {
			partition.Disk().finishRepairTask()
		}()
	}

	// isForceRead 函数用来检查读请求包是否有强制读表示，强制读请求会不检查请求所读数据区域是否存在风险
	var isForceRead = func(p *repl.Packet) bool {
		return len(p.Arg) > 0 && p.Arg[0] == 1
	}

	if !isForceRead(p) && partition.CheckIssue(p.ExtentID, uint64(p.ExtentOffset), uint64(p.Size)) {
		// 正常非强制读请求下，若请求所读数据区域存在风险，则拒绝响应。
		err = proto.ErrOperationDisabled
		return
	}

	action := proto.ActionRead
	if isRepairRead {
		action = proto.ActionRepairRead
	}

	var dataBuffer []byte
	if needReplySize >= unit.ReadBlockSize {
		dataBuffer, _ = proto.Buffers.Get(unit.ReadBlockSize)
		defer proto.Buffers.Put(dataBuffer)
	} else {
		dataBuffer = make([]byte, needReplySize)
	}

	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		reply := repl.NewStreamReadResponsePacket(p.Ctx(), p.ReqID, p.PartitionID, p.ExtentID)
		reply.StartT = p.StartT
		currReadSize := uint32(unit.Min(int(needReplySize), unit.ReadBlockSize))
		reply.Data = dataBuffer[:currReadSize]

		reply.ExtentOffset = offset
		p.Size = uint32(currReadSize)
		p.ExtentOffset = offset

		err = func() error {
			var storeErr error
			if !isRepairRead {
				tp := exporter.NewModuleTP("StreamRead_StoreRead")
				defer func() {
					tp.Set(storeErr)
				}()
			}
			if storeErr = partition.checkAndWaitForPendingActionApplied(reply.ExtentID, offset, int64(currReadSize)); storeErr != nil {
				return storeErr
			}
			reply.CRC, storeErr = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data[0:currReadSize], isRepairRead)
			return storeErr
		}()
		partition.checkIsDiskError(err)
		p.CRC = reply.CRC
		if err != nil {
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		reply.Opcode = p.Opcode
		p.ResultCode = proto.OpOk

		err = func() error {
			var netErr error
			if !isRepairRead {
				tp := exporter.NewModuleTP("StreamRead_WriteToConn")
				defer func() {
					tp.Set(netErr)
				}()
			}
			netErr = reply.WriteToConn(connect, proto.WriteDeadlineTime)
			return netErr
		}()
		partition.monitorData[action].UpdateData(uint64(currReadSize))
		if err != nil {
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
			log.LogErrorf(logContent)
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
		log.LogReadf(logContent)
	}
	p.PacketOkReply()

	return
}

func (s *DataNode) handlePacketToGetAllWatermarks(p *repl.Packet) {
	var (
		buf       []byte
		fInfoList []storage.ExtentInfoBlock
		err       error
	)
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetAllExtentWatermarks, err.Error())
		}
	}()
	if p.ExtentType == proto.NormalExtentType {
		if !store.IsFinishLoad() {
			err = storage.PartitionIsLoaddingErr
			return
		}
		fInfoList, err = store.GetAllWatermarks(p.ExtentType, storage.NormalExtentFilter())
	} else {
		extents := make([]uint64, 0)
		err = json.Unmarshal(p.Data, &extents)
		if err == nil {
			fInfoList, err = store.GetAllWatermarks(p.ExtentType, storage.TinyExtentFilter(extents))
		}
	}
	buf, err = json.Marshal(fInfoList)
	if err != nil {
		return
	}
	p.PacketOkWithBody(buf)
	return
}

// V2使用二进制编解码
func (s *DataNode) handlePacketToGetAllWatermarksV2(p *repl.Packet) {
	var (
		err  error
		data []byte
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetAllExtentWatermarksV2, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if p.ExtentType == proto.NormalExtentType {
		if !store.IsFinishLoad() {
			err = storage.PartitionIsLoaddingErr
			return
		}
		_, data, err = store.GetAllWatermarksWithByteArr(p.ExtentType, storage.NormalExtentFilter())
	} else {
		var extentIDs = make([]uint64, 0, len(p.Data)/8)
		var extentID uint64
		var reader = bytes.NewReader(p.Data)
		for {
			err = binary.Read(reader, binary.BigEndian, &extentID)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return
			}
			extentIDs = append(extentIDs, extentID)
		}
		_, data, err = store.GetAllWatermarksWithByteArr(p.ExtentType, storage.TinyExtentFilter(extentIDs))
	}
	if err != nil {
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (s *DataNode) handlePacketToGetAllExtentInfo(p *repl.Packet) {
	var (
		err  error
		data []byte
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetAllExtentInfo, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if !store.IsFinishLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	data, err = store.GetAllExtentInfoWithByteArr(storage.ExtentFilterForValidateCRC())
	if err != nil {
		return
	}
	p.PacketOkWithBody(data)
	return
}

func (s *DataNode) writeEmptyPacketOnTinyExtentRepairRead(reply *repl.Packet, newOffset, currentOffset int64, connect net.Conn) (replySize int64, err error) {
	replySize = newOffset - currentOffset
	reply.Data = make([]byte, 0)
	reply.Size = 0
	reply.CRC = crc32.ChecksumIEEE(reply.Data)
	reply.ResultCode = proto.OpOk
	reply.ExtentOffset = currentOffset
	reply.Arg[0] = EmptyResponse
	binary.BigEndian.PutUint64(reply.Arg[1:9], uint64(replySize))
	err = reply.WriteToConn(connect, proto.WriteDeadlineTime)
	if replySize >= math.MaxUint32 {
		reply.Size = math.MaxUint32
	} else {
		reply.Size = uint32(replySize)
	}
	//redirect kernelOffset as crc of Arg
	reply.KernelOffset = uint64(crc32.ChecksumIEEE(reply.Arg))
	logContent := fmt.Sprintf("action[write empty repair packet] %v.",
		reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
	log.LogReadf(logContent)

	return
}

func (s *DataNode) attachAvaliSizeOnTinyExtentRepairRead(reply *repl.Packet, avaliSize uint64) {
	binary.BigEndian.PutUint64(reply.Arg[9:TinyExtentRepairReadResponseArgLen], avaliSize)
}

// Handle handleTinyExtentRepairRead packet.
func (s *DataNode) handleTinyExtentRepairRead(request *repl.Packet, connect net.Conn) {
	var (
		err                 error
		needReplySize       int64
		tinyExtentFinfoSize uint64
	)

	defer func() {
		if err != nil {
			request.PackErrorBody(ActionStreamReadTinyExtentRepair, err.Error())
			request.WriteToConn(connect, proto.WriteDeadlineTime)
		}
	}()
	if !proto.IsTinyExtent(request.ExtentID) {
		err = fmt.Errorf("unavali extentID(%v)", request.ExtentID)
		return
	}

	partition := request.Object.(*DataPartition)
	if !partition.Disk().canRepairOnDisk() {
		err = fmt.Errorf("disk(%v) limit on handleExtentRepairRead", partition.Disk().Path)
		return
	}
	defer func() {
		partition.Disk().finishRepairTask()
	}()
	store := partition.ExtentStore()
	tinyExtentFinfoSize, err = store.TinyExtentGetFinfoSize(request.ExtentID)
	if err != nil {
		return
	}
	offset := request.ExtentOffset
	needReplySize = int64(tinyExtentFinfoSize - uint64(request.ExtentOffset))
	avaliReplySize := uint64(needReplySize)
	partition.monitorData[proto.ActionRepairRead].UpdateData(uint64(request.Size))

	var (
		newOffset, newEnd int64
	)
	for {
		if needReplySize <= 0 {
			break
		}
		reply := repl.NewTinyExtentStreamReadResponsePacket(request.Ctx(), request.ReqID, request.PartitionID, request.ExtentID)
		reply.ArgLen = TinyExtentRepairReadResponseArgLen
		reply.Arg = make([]byte, TinyExtentRepairReadResponseArgLen)
		s.attachAvaliSizeOnTinyExtentRepairRead(reply, avaliReplySize)
		newOffset, newEnd, err = store.TinyExtentAvaliOffset(request.ExtentID, offset)
		if err != nil {
			return
		}
		if newOffset > offset {
			var (
				replySize int64
			)
			if replySize, err = s.writeEmptyPacketOnTinyExtentRepairRead(reply, newOffset, offset, connect); err != nil {
				return
			}
			needReplySize -= replySize
			offset += replySize
			continue
		}
		currNeedReplySize := newEnd - newOffset
		currReadSize := uint32(unit.Min(int(currNeedReplySize), unit.ReadBlockSize))
		if currReadSize == unit.ReadBlockSize {
			reply.Data, _ = proto.Buffers.Get(unit.ReadBlockSize)
		} else {
			reply.Data = make([]byte, currReadSize)
		}
		reply.ExtentOffset = offset
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data, false)
		if err != nil {
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
			connect.Close()
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		needReplySize -= int64(currReadSize)
		offset += int64(currReadSize)
		if currReadSize == unit.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
		log.LogReadf(logContent)
	}

	request.PacketOkReply()
	return
}

func (s *DataNode) handleTinyExtentAvaliRead(request *repl.Packet, connect net.Conn) {
	var (
		err                 error
		needReplySize       int64
		tinyExtentFinfoSize uint64
	)

	defer func() {
		if err != nil {
			request.PackErrorBody(ActionStreamReadTinyExtentAvali, err.Error())
			request.WriteToConn(connect, proto.WriteDeadlineTime)
		}
	}()
	if !proto.IsTinyExtent(request.ExtentID) {
		err = fmt.Errorf("unavali extentID (%v)", request.ExtentID)
		return
	}

	partition := request.Object.(*DataPartition)
	store := partition.ExtentStore()
	tinyExtentFinfoSize, err = store.TinyExtentGetFinfoSize(request.ExtentID)
	if err != nil {
		return
	}
	needReplySize = int64(request.Size)
	offset := request.ExtentOffset
	if uint64(request.ExtentOffset)+uint64(request.Size) > tinyExtentFinfoSize {
		needReplySize = int64(tinyExtentFinfoSize - uint64(request.ExtentOffset))
	}
	avaliReplySize := uint64(needReplySize)

	var (
		newOffset, newEnd int64
	)
	for {
		if needReplySize <= 0 {
			break
		}
		reply := repl.NewTinyExtentStreamReadResponsePacket(request.Ctx(), request.ReqID, request.PartitionID, request.ExtentID)
		reply.Opcode = proto.OpTinyExtentAvaliRead
		reply.ArgLen = TinyExtentRepairReadResponseArgLen
		reply.Arg = make([]byte, TinyExtentRepairReadResponseArgLen)
		s.attachAvaliSizeOnTinyExtentRepairRead(reply, avaliReplySize)
		newOffset, newEnd, err = store.TinyExtentAvaliOffset(request.ExtentID, offset)
		if err != nil {
			return
		}
		if newOffset > offset {
			replySize := newOffset - offset
			offset += replySize
			continue
		}
		currNeedReplySize := newEnd - newOffset
		if currNeedReplySize <= 0 {
			err = fmt.Errorf("ExtentID(%v) offset(%v) currNeedReplySize(%v) <= 0", request.ExtentID, offset, currNeedReplySize)
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
			log.LogErrorf(logContent)
			break
		}
		currReadSize := uint32(unit.Min(int(currNeedReplySize), unit.ReadBlockSize))
		if currReadSize == unit.ReadBlockSize {
			reply.Data, _ = proto.Buffers.Get(unit.ReadBlockSize)
		} else {
			reply.Data = make([]byte, currReadSize)
		}
		reply.ExtentOffset = offset
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data, false)
		if err != nil {
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		reply.Size = currReadSize
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
			connect.Close()
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		needReplySize -= int64(currReadSize)
		offset += int64(currReadSize)
		if currReadSize == unit.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
		log.LogReadf(logContent)
	}

	request.PacketOkReply()
	return
}

func (s *DataNode) handlePacketToReadTinyDeleteRecordFile(p *repl.Packet, connect *net.TCPConn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamReadTinyDeleteRecord, err.Error())
			p.WriteToConn(connect, proto.WriteDeadlineTime)
		}
	}()
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	localTinyDeleteFileSize, err := store.LoadTinyDeleteFileOffset()
	if err != nil {
		return
	}
	needReplySize := localTinyDeleteFileSize - p.ExtentOffset
	offset := p.ExtentOffset
	reply := repl.NewReadTinyDeleteRecordResponsePacket(p.Ctx(), p.ReqID, p.PartitionID)
	reply.StartT = time.Now().UnixNano()
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		currReadSize := uint32(unit.Min(int(needReplySize), MaxSyncTinyDeleteBufferSize))
		reply.Data = make([]byte, currReadSize)
		reply.ExtentOffset = offset
		reply.CRC, err = store.ReadTinyDeleteRecords(offset, int64(currReadSize), reply.Data)
		if err != nil {
			err = fmt.Errorf(ActionStreamReadTinyDeleteRecord+" localTinyDeleteRecordSize(%v) offset(%v)"+
				" currReadSize(%v) err(%v)", localTinyDeleteFileSize, offset, currReadSize, err)
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
			return
		}
		needReplySize -= int64(currReadSize)
		offset += int64(currReadSize)
	}
	p.PacketOkReply()

	return
}

// Handle OpNotifyReplicasToRepair packet.
func (s *DataNode) handlePacketToNotifyExtentRepair(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRepair, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	mf := new(DataPartitionRepairTask)
	err = json.Unmarshal(p.Data, mf)
	if err != nil {
		return
	}
	partition.DoExtentStoreRepairOnFollowerDisk(mf)
	p.PacketOkReply()
	return
}

// Handle OpBroadcastMinAppliedID
func (s *DataNode) handleBroadcastMinAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	minAppliedID := binary.BigEndian.Uint64(p.Data[0:8])
	if minAppliedID > 0 {
		partition.SetMinAppliedID(minAppliedID)
	}
	log.LogDebugf("[handleBroadcastMinAppliedID] partition(%v) minAppliedID(%v)", partition.partitionID, minAppliedID)
	p.PacketOkReply()
	return
}

// Handle handlePacketToGetAppliedID packet.
func (s *DataNode) handlePacketToGetAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	appliedID := partition.GetAppliedID()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedID)
	p.PacketOkWithBody(buf)
	p.AddMesgLog(fmt.Sprintf("_AppliedID(%v)", appliedID))
	return
}

func (s *DataNode) handlePacketToGetPartitionSize(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionGetPartitionSize, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	if !partition.ExtentStore().IsFinishLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	usedSize := partition.extentStore.StoreSizeExtentID(p.ExtentID)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(usedSize))
	p.AddMesgLog(fmt.Sprintf("partitionSize_(%v)", usedSize))
	p.PacketOkWithBody(buf)

	return
}

func (s *DataNode) handlePacketToGetMaxExtentIDAndPartitionSize(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(GetMaxExtentIDAndPartitionSize, err.Error())
		}
	}()
	partition := p.Object.(*DataPartition)
	if !partition.ExtentStore().IsFinishLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	maxExtentID, totalPartitionSize := partition.extentStore.GetMaxExtentIDAndPartitionSize()
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(maxExtentID))
	binary.BigEndian.PutUint64(buf[8:16], totalPartitionSize)
	p.PacketOkWithBody(buf)

	return
}

func (s *DataNode) handlePacketToDecommissionDataPartition(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.DataPartitionDecommissionRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDecommissionPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = fmt.Errorf("partition %v not exsit", req.PartitionId)
		return
	}
	p.PartitionID = req.PartitionId

	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		err = raft.ErrNotLeader
		return
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.NewErrorf("[opOfflineDataPartition]: AddPeer(%v) same withRemovePeer(%v)", req.AddPeer, req.RemovePeer)
		return
	}
	if req.AddPeer.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfAddNode, raftProto.Peer{ID: req.AddPeer.ID}, reqData)
		if err != nil {
			return
		}
	}
	_, err = dp.ChangeRaftMember(raftProto.ConfRemoveNode, raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		return
	}
	return
}

func (s *DataNode) handlePacketToAddDataPartitionRaftMember(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.AddDataPartitionRaftMemberRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionAddDataPartitionRaftMember, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.PartitionID = req.PartitionId
	if dp.IsExistReplica(req.AddPeer.Addr) {
		log.LogInfof("handlePacketToAddDataPartitionRaftMember recive MasterCommand: %v "+
			"addRaftAddr(%v) has exsit", string(reqData), req.AddPeer.Addr)
		return
	}
	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}

	if req.AddPeer.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfAddNode, raftProto.Peer{ID: req.AddPeer.ID}, reqData)
		if err != nil {
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToRemoveDataPartitionRaftMember(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.RemoveDataPartitionRaftMemberRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRemoveDataPartitionRaftMember, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}
	reqData, err = json.Marshal(adminTask.Request)
	p.AddMesgLog(string(reqData))
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	req.ReserveResource = adminTask.ReserveResource
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		return
	}
	p.PartitionID = req.PartitionId

	if !req.RaftOnly && !dp.IsExistReplica(req.RemovePeer.Addr) {
		log.LogInfof("handlePacketToRemoveDataPartitionRaftMember recive MasterCommand: %v "+
			"RemoveRaftPeer(%v) has not exsit", string(reqData), req.RemovePeer.Addr)
		return
	}

	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}
	if !req.RaftOnly {
		if err = dp.CanRemoveRaftMember(req.RemovePeer); err != nil {
			return
		}
	}
	if req.RemovePeer.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfRemoveNode, raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
		if err != nil {
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToResetDataPartitionRaftMember(p *repl.Packet) {
	var (
		err       error
		reqData   []byte
		isUpdated bool
		req       = &proto.ResetDataPartitionRaftMemberRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionResetDataPartitionRaftMember, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	p.AddMesgLog(string(reqData))
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}

	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = fmt.Errorf("partition %v not exsit", req.PartitionId)
		return
	}
	p.PartitionID = req.PartitionId
	for _, peer := range req.NewPeers {
		if !dp.IsExistReplica(peer.Addr) {
			log.LogErrorf("handlePacketToResetDataPartitionRaftMember recive MasterCommand: %v "+
				"ResetRaftPeer(%v) has not exsit", string(reqData), peer.Addr)
			return
		}
		if peer.ID == 0 {
			log.LogErrorf("handlePacketToResetDataPartitionRaftMember recive MasterCommand: %v "+
				"Peer ID(%v) not valid", string(reqData), peer.ID)
			return
		}
	}
	var peers []raftProto.Peer
	for _, peer := range req.NewPeers {
		peers = append(peers, raftProto.Peer{ID: peer.ID})
	}
	if err = dp.ResetRaftMember(peers, reqData); err != nil {
		return
	}
	if isUpdated, err = dp.resetRaftNode(req); err != nil {
		return
	}
	if isUpdated {
		if err = dp.ChangeCreateType(proto.NormalCreateDataPartition); err != nil {
			log.LogErrorf("handlePacketToResetDataPartitionRaftMember dp(%v) PersistMetadata err(%v).", dp.partitionID, err)
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToAddDataPartitionRaftLearner(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.AddDataPartitionRaftLearnerRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionAddDataPartitionRaftLearner, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.PartitionID = req.PartitionId
	if dp.IsExistReplica(req.AddLearner.Addr) && dp.IsExistLearner(req.AddLearner) {
		log.LogInfof("handlePacketToAddDataPartitionRaftLearner receive MasterCommand: %v "+
			"addRaftLearnerAddr(%v) has exist", string(reqData), req.AddLearner.Addr)
		return
	}
	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}

	if req.AddLearner.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfAddLearner, raftProto.Peer{ID: req.AddLearner.ID}, reqData)
		if err != nil {
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToPromoteDataPartitionRaftLearner(p *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.PromoteDataPartitionRaftLearnerRequest{}
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionPromoteDataPartitionRaftLearner, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		return
	}

	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	p.PartitionID = req.PartitionId
	if !dp.IsExistReplica(req.PromoteLearner.Addr) || !dp.IsExistLearner(req.PromoteLearner) {
		log.LogInfof("handlePacketToPromoteDataPartitionRaftLearner receive MasterCommand: %v "+
			"promoteRaftLearnerAddr(%v) has exist", string(reqData), req.PromoteLearner.Addr)
		return
	}
	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}

	if req.PromoteLearner.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfPromoteLearner, raftProto.Peer{ID: req.PromoteLearner.ID}, reqData)
		if err != nil {
			return
		}
	}
	return
}

func (s *DataNode) handlePacketToDataPartitionTryToLeader(p *repl.Packet) {
	var (
		err error
	)

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDataPartitionTryToLeader, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	dp := s.space.Partition(p.PartitionID)
	if dp == nil {
		err = fmt.Errorf("partition %v not exsit", p.PartitionID)
		return
	}

	if dp.raftPartition.IsRaftLeader() {
		return
	}
	err = dp.raftPartition.TryToLeader(dp.partitionID)
	return
}

func (s *DataNode) handlePacketToSyncDataPartitionReplicas(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionSyncDataPartitionReplicas, err.Error())
		}
	}()
	task := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, task); err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	if task.OpCode != proto.OpSyncDataPartitionReplicas {
		err = fmt.Errorf("from master Task(%v) failed,error unavali opcode(%v)", task.ToString(), task.OpCode)
		return
	}
	request := &proto.SyncDataPartitionReplicasRequest{}
	bytes, err := json.Marshal(task.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(bytes, request); err != nil {
		return
	}
	s.space.SyncPartitionReplicas(request.PartitionId, request.PersistenceHosts)
	p.PacketOkReply()
	return

}

const (
	forwardToRaftLeaderTimeOut = 60 * 2
)

func (s *DataNode) forwardToRaftLeader(dp *DataPartition, p *repl.Packet) (ok bool, err error) {
	var (
		conn       *net.TCPConn
		leaderAddr string
	)

	if leaderAddr, ok = dp.IsRaftLeader(); ok {
		return
	}

	// return NoLeaderError if leaderAddr is nil
	if leaderAddr == "" {
		err = storage.NoLeaderError
		return
	}

	// forward the packet to the leader if local one is not the leader
	conn, err = gConnPool.GetConnect(leaderAddr)
	if err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn, proto.WriteDeadlineTime)
	if err != nil {
		return
	}
	if err = p.ReadFromConn(conn, forwardToRaftLeaderTimeOut); err != nil {
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("forwardToRaftLeader error msg: %v", p.GetOpMsgWithReqAndResult())
		return
	}
	return
}

func (s *DataNode) forwardToRaftLeaderWithTimeOut(dp *DataPartition, p *repl.Packet) (ok bool, err error) {
	var (
		conn       *net.TCPConn
		leaderAddr string
	)

	if leaderAddr, ok = dp.IsRaftLeader(); ok {
		return
	}

	// return NoLeaderError if leaderAddr is nil
	if leaderAddr == "" {
		err = storage.NoLeaderError
		return
	}

	// forward the packet to the leader if local one is not the leader
	conn, err = gConnPool.GetConnect(leaderAddr)
	if err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn, proto.WriteDeadlineTime)
	if err != nil {
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return
	}

	return
}

func (s *DataNode) rateLimit(p *repl.Packet, c *net.TCPConn) {
	if !isRateLimitOn {
		return
	}

	// ignore rate limit if request is from cluster internal nodes
	addrSlice := strings.Split(c.RemoteAddr().String(), ":")
	_, isInternal := clusterMap[addrSlice[0]]
	if isInternal {
		return
	}

	ctx := context.Background()
	// request rate limit for entire data node
	if reqRateLimit > 0 {
		reqRateLimiter.Wait(ctx)
	}

	// request rate limit for opcode
	limiter, ok := reqOpRateLimiterMap[p.Opcode]
	if ok {
		limiter.Wait(ctx)
	}

	partition, ok := p.Object.(*DataPartition)
	if !ok {
		return
	}

	// request rate limit for volume & opcode
	opRatelimiterMap, ok := reqVolOpRateLimiterMap[partition.volumeID]
	if ok {
		limiter, ok = opRatelimiterMap[p.Opcode]
		if ok {
			limiter.Wait(ctx)
		}
	}

	// request rate limit of each data partition for volume
	partRateLimiterMap, ok := reqVolPartRateLimiterMap[partition.volumeID]
	if ok {
		limiter, ok = partRateLimiterMap[partition.partitionID]
		if ok {
			limiter.Wait(ctx)
		}
	}

	// request rate limit of each data partition for volume & opcode
	opPartRateLimiterMap, ok := reqVolOpPartRateLimiterMap[partition.volumeID]
	if ok {
		partRateLimiterMap, ok = opPartRateLimiterMap[p.Opcode]
		if ok {
			limiter, ok = partRateLimiterMap[partition.partitionID]
			if ok {
				limiter.Wait(ctx)
			}
		}
	}

	return
}

func (s *DataNode) responseHeartbeatPb(task *proto.AdminTask) {
	var err error
	request := &proto.HeartBeatRequest{}
	taskPb := task.ConvertToPb()
	taskPb.DataNodeResponse = &proto.DataNodeHeartbeatResponsePb{}
	s.buildHeartBeatResponsePb(taskPb.DataNodeResponse)

	if task.OpCode == proto.OpDataNodeHeartbeat {
		marshaled, _ := json.Marshal(task.Request)
		_ = json.Unmarshal(marshaled, request)
		taskPb.Request = request.ConvertToPb()
		taskPb.DataNodeResponse.Status = proto.TaskSucceeds
	} else {
		taskPb.DataNodeResponse.Status = proto.TaskFailed
		err = fmt.Errorf("illegal opcode")
		taskPb.DataNodeResponse.Result = err.Error()
	}
	if err = MasterClient.NodeAPI().ResponseHeartBeatTaskPb(taskPb); err != nil {
		err = errors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
		log.LogErrorf(err.Error())
		return
	}
}

func (s *DataNode) responseHeartbeat(task *proto.AdminTask) {
	var err error
	request := &proto.HeartBeatRequest{}
	resp := &proto.DataNodeHeartbeatResponse{}
	s.buildHeartBeatResponse(resp)

	if task.OpCode == proto.OpDataNodeHeartbeat {
		marshaled, _ := json.Marshal(task.Request)
		_ = json.Unmarshal(marshaled, request)
		task.Request = request
		resp.Status = proto.TaskSucceeds
	} else {
		resp.Status = proto.TaskFailed
		err = fmt.Errorf("illegal opcode")
		resp.Result = err.Error()
	}
	task.Response = resp
	if err = MasterClient.NodeAPI().ResponseDataNodeTask(task); err != nil {
		err = errors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
		log.LogErrorf(err.Error())
		return
	}
}
