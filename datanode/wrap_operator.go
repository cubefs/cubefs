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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tiglabs/raft"
	raftProto "github.com/tiglabs/raft/proto"
	"hash/crc32"
	"strings"
)

func (s *DataNode) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op[%v] error[%v]", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch p.Opcode {
			case proto.OpStreamRead, proto.OpRead, proto.OpExtentRepairRead:
			case proto.OpReadTinyDelete:
				log.LogRead(logContent)
			case proto.OpWrite, proto.OpRandomWrite, proto.OpSyncRandomWrite, proto.OpSyncWrite, proto.OpMarkDelete:
				log.LogWrite(logContent)
			default:
				log.LogInfo(logContent)
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
	case proto.OpExtentRepairRead:
		s.handleExtentRepaiReadPacket(p, c, RepairRead)
	case proto.OpMarkDelete:
		s.handleMarkDeletePacket(p, c)
	case proto.OpRandomWrite, proto.OpSyncRandomWrite:
		s.handleRandomWritePacket(p)
	case proto.OpNotifyReplicasToRepair:
		s.handlePacketToNotifyExtentRepair(p)
	case proto.OpGetAllWatermarks:
		s.handlePacketToGetAllWatermarks(p)
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
	case proto.OpGetPartitionSize:
		s.handlePacketToGetPartitionSize(p)
	case proto.OpReadTinyDelete:
		s.handlePacketToReadTinyDelete(p, c)
	case proto.OpBroadcastMinAppliedID:
		s.handleBroadcastMinAppliedID(p)
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
	if partition.Available() <= 0 || partition.disk.Status == proto.ReadOnly {
		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}
	err = partition.ExtentStore().Create(p.ExtentID)

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
		err = fmt.Errorf("from master Task[%v] failed,error unavali opcode(%v)", task.ToString(), task.OpCode)
		return
	}

	bytes, err = json.Marshal(task.Request)
	if err != nil {
		err = fmt.Errorf("from master Task[%v] cannot unmashal CreateDataPartition", task.ToString())
		return
	}
	if err = json.Unmarshal(bytes, request); err != nil {
		err = fmt.Errorf("from master Task[%v] cannot unmash CreateDataPartitionRequest struct", task.ToString())
		return
	}
	if dp, err = s.space.CreatePartition(request); err != nil {
		err = fmt.Errorf("from master Task[%v] cannot create Partition err(%v)", task.ToString(), err)
		return
	}
	p.PacketOkWithBody([]byte(dp.Disk().Path))

	return
}

// Handle OpHeartbeat packet.
func (s *DataNode) handleHeartbeatPacket(p *repl.Packet) {
	var (
		data []byte
		err  error
	)
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
	request := &proto.HeartBeatRequest{}
	response := &proto.DataNodeHeartbeatResponse{}
	s.buildHeartBeatResponse(response)

	if task.OpCode == proto.OpDataNodeHeartbeat {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		response.Status = proto.TaskSucceeds
		MasterHelper.AddNode(request.MasterAddr)
	} else {
		response.Status = proto.TaskFailed
		err = fmt.Errorf("illegal opcode")
		response.Result = err.Error()
	}
	task.Response = response
	if data, err = json.Marshal(task); err != nil {
		return
	}
	_, err = MasterHelper.Request("POST", proto.GetDataNodeTaskResponse, nil, data)
	if err != nil {
		err = errors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
		return
	}
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
	response := &proto.DeleteDataPartitionResponse{}
	if task.OpCode == proto.OpDeleteDataPartition {
		bytes, _ := json.Marshal(task.Request)
		err = json.Unmarshal(bytes, request)
		if err != nil {
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskFailed
			response.Result = err.Error()
		} else {
			s.space.DeletePartition(request.PartitionId)
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskSucceeds
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFailed
		response.Result = "illegal opcode "
		err = fmt.Errorf("illegal opcode ")
	}
	task.Response = response
	data, _ := json.Marshal(task)
	_, err = MasterHelper.Request("POST", proto.GetDataNodeTaskResponse, nil, data)
	if err != nil {
		err = errors.Trace(err, "delete DataPartition failed,partitionID(%v)", request.PartitionId)
		log.LogErrorf("action[handlePacketToDeleteDataPartition] err(%v).", err)
	}
	log.LogInfof(fmt.Sprintf("action[handlePacketToDeleteDataPartition] %v error(%v)", request.PartitionId, string(data)))

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
	data, err := json.Marshal(task)
	if err != nil {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFailed
		response.Result = err.Error()
		err = fmt.Errorf("from master Task(%v) failed,error(%v)", task.ToString(), response.Result)
	}
	_, err = MasterHelper.Request("POST", proto.GetDataNodeTaskResponse, nil, data)
	if err != nil {
		err = errors.Trace(err, "load DataPartition failed,partitionID(%v)", request.PartitionId)
		log.LogError(errors.Stack(err))
	}
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleMarkDeletePacket(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	partition := p.Object.(*DataPartition)
	if p.ExtentType == proto.TinyExtentType {
		ext := new(proto.TinyExtentDeleteRecord)
		err = json.Unmarshal(p.Data, ext)
		if err == nil {
			err = partition.ExtentStore().MarkDelete(p.ExtentID, int64(ext.ExtentOffset), int64(ext.Size), ext.TinyDeleteFileOffset)
		}
	} else {
		err = partition.ExtentStore().MarkDelete(p.ExtentID, 0, 0, 0)
	}
	if err != nil {
		p.PackErrorBody(ActionMarkDelete, err.Error())
	} else {
		p.PacketOkReply()
	}

	return
}

// Handle OpWrite packet.
func (s *DataNode) handleWritePacket(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*DataPartition)
	if partition.Available() <= 0 || partition.disk.Status == proto.ReadOnly {
		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}
	store := partition.ExtentStore()
	if p.Size <= util.BlockSize {
		err = store.Write(p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data, p.CRC, UpdateSize, p.Opcode == proto.OpSyncWrite)
	} else {
		size := p.Size
		offset := 0
		for size > 0 {
			if size <= 0 {
				break
			}
			currSize := util.Min(int(size), util.BlockSize)
			data := p.Data[offset : offset+currSize]
			crc := crc32.ChecksumIEEE(data)
			err = store.Write(p.ExtentID, p.ExtentOffset+int64(offset), int64(currSize), data, crc, UpdateSize, p.Opcode == proto.OpSyncWrite)
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

	if err == nil && p.Opcode == proto.OpRandomWrite && p.Size == util.BlockSize {
		proto.Buffers.Put(p.Data)
	}
}

func (s *DataNode) handleStreamReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamRead, err.Error())
			p.WriteToConn(connect)
		}
	}()
	partition := p.Object.(*DataPartition)
	if err = partition.CheckLeader(p, connect); err != nil {
		return
	}
	s.handleExtentRepaiReadPacket(p, connect, isRepairRead)

	return
}

func (s *DataNode) handleExtentRepaiReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamRead, err.Error())
			p.WriteToConn(connect)
		}
	}()
	partition := p.Object.(*DataPartition)
	needReplySize := p.Size
	offset := p.ExtentOffset
	store := partition.ExtentStore()

	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		reply := repl.NewStreamReadResponsePacket(p.ReqID, p.PartitionID, p.ExtentID)
		reply.StartT = p.StartT
		currReadSize := uint32(util.Min(int(needReplySize), util.ReadBlockSize))
		if currReadSize == util.ReadBlockSize {
			reply.Data, _ = proto.Buffers.Get(util.ReadBlockSize)
		} else {
			reply.Data = make([]byte, currReadSize)
		}
		tpObject := exporter.NewTPCnt(p.GetOpMsg())
		reply.ExtentOffset = offset
		p.Size = uint32(currReadSize)
		p.ExtentOffset = offset
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data, isRepairRead)
		p.CRC = reply.CRC
		tpObject.Set(err)
		if err != nil {
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		p.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect); err != nil {
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		if currReadSize == util.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
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
		fInfoList []*storage.ExtentInfo
		err       error
	)
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if p.ExtentType == proto.NormalExtentType {
		fInfoList, _, err = store.GetAllWatermarks(storage.NormalExtentFilter())
	} else {
		extents := make([]uint64, 0)
		err = json.Unmarshal(p.Data, &extents)
		if err == nil {
			fInfoList, _, err = store.GetAllWatermarks(storage.TinyExtentFilter(extents))
		}
	}
	if err != nil {
		p.PackErrorBody(ActionGetAllExtentWatermarks, err.Error())
	} else {
		buf, err = json.Marshal(fInfoList)
		p.PacketOkWithBody(buf)
	}
	return
}

func (s *DataNode) handlePacketToReadTinyDelete(p *repl.Packet, connect *net.TCPConn) {
	var (
		err error
	)
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	localTinyDeleteFileSize := store.LoadTinyDeleteFileOffset()
	needReplySize := localTinyDeleteFileSize - p.ExtentOffset
	offset := p.ExtentOffset
	reply := repl.NewTinyDeleteRecordResponsePacket(p.ReqID, p.PartitionID)
	reply.StartT = time.Now().UnixNano()
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		currReadSize := uint32(util.Min(int(needReplySize), MaxSyncTinyDeleteBufferSize))
		reply.Data = make([]byte, currReadSize)
		reply.ExtentOffset = offset
		reply.CRC, err = store.ReadTinyDeleteRecords(offset, int64(currReadSize), reply.Data)
		if err != nil {
			reply.PackErrorBody(ActionStreamReadTinyDeleteRecord, err.Error())
			if err = reply.WriteToConn(connect); err != nil {
				err = fmt.Errorf(reply.LogMessage(ActionStreamReadTinyDeleteRecord, connect.RemoteAddr().String(),
					reply.StartT, err))
				log.LogErrorf(err.Error())
			}
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect); err != nil {
			err = fmt.Errorf(reply.LogMessage(ActionStreamReadTinyDeleteRecord, connect.RemoteAddr().String(),
				reply.StartT, err))
			p.PackErrorBody(ActionStreamReadTinyDeleteRecord, err.Error())
			log.LogErrorf(err.Error())
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
	partition := p.Object.(*DataPartition)
	mf := new(DataPartitionRepairTask)
	err = json.Unmarshal(p.Data, mf)
	if err != nil {
		p.PackErrorBody(ActionRepair, err.Error())
		return
	}
	partition.DoExtentStoreRepair(mf)
	p.PacketOkReply()
	return
}

// Handle OpBroadcastMinAppliedID
func (s *DataNode) handleBroadcastMinAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	minAppliedID := binary.BigEndian.Uint64(p.Data)
	if minAppliedID > 0 {
		partition.SetMinAppliedID(minAppliedID)
	}
	log.LogDebugf("[handleBroadcastMinAppliedID] partition=%v minAppliedID=%v", partition.partitionID, minAppliedID)
	p.PacketOkReply()
	return
}

// Handle handlePacketToGetAppliedID packet.
func (s *DataNode) handlePacketToGetAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	appliedID := partition.GetAppliedID() //return current appliedID
	log.LogDebugf("[handlePacketToGetAppliedID] partition=%v curAppId=%v",
		partition.partitionID, appliedID)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedID)
	p.PacketOkWithBody(buf)
	return
}

func (s *DataNode) handlePacketToGetPartitionSize(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	usedSize := partition.extentStore.StoreSize()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(usedSize))
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
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err := decode.Decode(adminTask); err != nil {
		return
	}

	log.LogDebugf("[opOfflineDataPartition] received task: %v", adminTask)
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	dp := s.space.Partition(req.PartitionId)
	if err != nil {
		return
	}

	isRaftLeader, err = s.forwardToRaftLeader(dp, p)
	if !isRaftLeader {
		return
	}

	p.PacketOkReply()

	resp := proto.DataPartitionDecommissionResponse{
		PartitionId: req.PartitionId,
		Status:      proto.TaskFailed,
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.NewErrorf("[opOfflineDataPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
		resp.Result = err.Error()
		goto end
	}

	_, err = dp.ChangeRaftMember(raftProto.ConfAddNode, raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		log.LogErrorf(fmt.Sprintf("[opOfflineDataPartition]: AddNode[%v] err[%v]", req.AddPeer, err))
		goto end
	}
	_, err = dp.ChangeRaftMember(raftProto.ConfRemoveNode, raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		log.LogErrorf(fmt.Sprintf("[opOfflineDataPartition]: RemoveNode[%v] err[%v]", req.RemovePeer, err))
		goto end
	}
	resp.Status = proto.TaskSucceeds
end:
	adminTask.Request = nil
	adminTask.Response = resp

	data, _ := json.Marshal(adminTask)
	_, err = MasterHelper.Request("POST", proto.GetDataNodeTaskResponse, nil, data)
	if err != nil {
		err = errors.Trace(err, "opOfflineDataPartition failed, partitionID(%v)", resp.PartitionId)
		log.LogError(errors.Stack(err))
		return
	}

	log.LogDebugf("[opOfflineDataPartition]: the end %v", adminTask)
	return
}

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
	err = p.WriteToConn(conn)
	if err != nil {
		return
	}
	if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
		return
	}

	return
}
