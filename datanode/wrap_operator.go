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


/* TODO should we name it "packet_handler" ? */

package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"strings"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/master"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/exporter"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft"
	raftProto "github.com/tiglabs/raft/proto"
	"hash/crc32"
)

func (s *DataNode) OperatePacket(pkg *repl.Packet, c *net.TCPConn) (err error) {
	orgSize := pkg.Size
	key := fmt.Sprintf("%s_datanode_%s", s.clusterID, pkg.GetOpMsg())
	tpObject := exporter.RegistTp(key)
	start := time.Now().UnixNano()
	defer func() {
		resultSize := pkg.Size
		pkg.Size = orgSize
		if pkg.IsErrPacket() {
			err = fmt.Errorf("op[%v] error[%v]", pkg.GetOpMsg(), string(pkg.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				pkg.LogMessage(pkg.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				pkg.LogMessage(pkg.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch pkg.Opcode {
			case proto.OpStreamRead, proto.OpRead, proto.OpExtentRepairRead:
				log.LogRead(logContent)
			case proto.OpWrite, proto.OpRandomWrite:
				log.LogWrite(logContent)
			default:
				log.LogInfo(logContent)
			}
		}
		pkg.Size = resultSize
		tpObject.CalcTp()
	}()
	switch pkg.Opcode {
	case proto.OpCreateExtent:
		s.handlePacketToCreateExtent(pkg)
	case proto.OpWrite:
		s.handleWritePacket(pkg)
	//case proto.OpRead:
	//	s.handleReadPacket(pkg)
	case proto.OpStreamRead:
		s.handleStreamReadPacket(pkg, c, StreamRead)
	case proto.OpExtentRepairRead:
		s.handleStreamReadPacket(pkg, c, RepairRead)
	case proto.OpMarkDelete:
		s.handleMarkDeletePacket(pkg)
	case proto.OpRandomWrite:
		s.handleRandomWritePacket(pkg)
	case proto.OpNotifyExtentRepair:
		s.handlePacketToNotifyExtentRepair(pkg)
	case proto.OpGetAllWatermarks:
		s.handlePacketToGetAllWatermarks(pkg)
	case proto.OpCreateDataPartition:
		s.handlePacketToCreateDataPartition(pkg)
	case proto.OpLoadDataPartition:
		s.handlePacketToLoadDataPartition(pkg)
	case proto.OpDeleteDataPartition:
		s.handlePacketToDeleteDataPartition(pkg)
	case proto.OpDataNodeHeartbeat:
		s.handleHeartbeatPacket(pkg)
	case proto.OpGetDataPartitionMetrics:
		s.handlePacketToGetDataPartitionMetrics(pkg)
	case proto.OpGetAppliedId:
		s.handlePacketToGetAppliedID(pkg)
	case proto.OpOfflineDataPartition:
		s.handlePacketToOfflineDataPartition(pkg)
	case proto.OpGetPartitionSize:
		s.handlePacketToGetPartitionSize(pkg)
	default:
		pkg.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(pkg.Opcode)))
	}

	return
}

// Handle OpCreateExtent packet.
func (s *DataNode) handlePacketToCreateExtent(pkg *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionCreateExtent, err.Error())
		} else {
			pkg.PackOkReply()
		}
	}()
	partition := pkg.Object.(*DataPartition)
	if partition.Available() <= 0 || partition.disk.Status == proto.ReadOnly {
		err = storage.NoSpaceError
		return
	}
	var ino uint64
	if len(pkg.Data) >= 8 && pkg.Size >= 8 {
		ino = binary.BigEndian.Uint64(pkg.Data)
	}
	err = partition.ExtentStore().Create(pkg.ExtentID, ino)

	return
}

// Handle OpCreateDataPartition packet.
func (s *DataNode) handlePacketToCreateDataPartition(pkg *repl.Packet) {
	var (
		err   error
		bytes []byte
	)
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionCreateDataPartition, err.Error())
		} else {
			pkg.PackOkReply()
		}
	}()
	task := &proto.AdminTask{}
	if err = json.Unmarshal(pkg.Data, task); err != nil {
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
	if _, err = s.space.CreatePartition(request); err != nil {
		err = fmt.Errorf("from master Task[%v] cannot create Partition err(%v)", task.ToString(), err)
		return
	}

	return
}

// Handle OpHeartbeat packet.
func (s *DataNode) handleHeartbeatPacket(pkg *repl.Packet) {
	var (
		data []byte
		err  error
	)
	task := &proto.AdminTask{}
	err = json.Unmarshal(pkg.Data, task)
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionCreateDataPartition, err.Error())
		} else {
			pkg.PackOkReply()
		}
	}()
	if err != nil {
		return
	}
	request := &proto.HeartBeatRequest{}

	// TODO there should be a better way to initialize a heartbeat response
	response := &proto.DataNodeHeartBeatResponse{}
	s.buildHeartBeatResponse(response)

	if task.OpCode == proto.OpDataNodeHeartbeat {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		response.Status = proto.TaskSuccess
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
	_, err = MasterHelper.Request("POST", master.GetDataNodeTaskResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "heartbeat to master(%v) failed.", request.MasterAddr)
		return
	}
}

// Handle OpDeleteDataPartition packet.
func (s *DataNode) handlePacketToDeleteDataPartition(pkg *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(pkg.Data, task)
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionDeleteDataPartition, err.Error())
		} else {
			pkg.PackOkReply()
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
			response.Status = proto.TaskSuccess
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFailed
		response.Result = "illegal opcode "
		err = fmt.Errorf("illegal opcode ")
	}
	task.Response = response
	data, _ := json.Marshal(task)
	_, err = MasterHelper.Request("POST", master.GetDataNodeTaskResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "delete DataPartition failed,partitionID(%v)", request.PartitionId)
		log.LogErrorf("action[handlePacketToDeleteDataPartition] err(%v).", err)
	}
	log.LogInfof(fmt.Sprintf("action[handlePacketToDeleteDataPartition] %v error(%v)", request.PartitionId, string(data)))

}

// Handle OpLoadDataPartition packet.
func (s *DataNode) handlePacketToLoadDataPartition(pkg *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(pkg.Data, task)
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionLoadDataPartition, err.Error())
		} else {
			pkg.PackOkReply()
		}
	}()
	if err != nil {
		return
	}
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
			response.Status = proto.TaskSuccess
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
	_, err = MasterHelper.Request("POST", master.GetDataNodeTaskResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "load DataPartition failed,partitionID(%v)", request.PartitionId)
		log.LogError(errors.ErrorStack(err))
	}
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleMarkDeletePacket(pkg *repl.Packet) {
	var (
		err error
	)
	partition := pkg.Object.(*DataPartition)
	if pkg.ExtentType == proto.TinyExtentType {
		ext := new(proto.ExtentKey)
		err = json.Unmarshal(pkg.Data, ext)
		if err == nil {
			err = partition.ExtentStore().MarkDelete(pkg.ExtentID, int64(ext.ExtentOffset), int64(ext.Size))
		}
	} else {
		err = partition.ExtentStore().MarkDelete(pkg.ExtentID, 0, 0)
	}
	if err != nil {
		pkg.PackErrorBody(ActionMarkDel, err.Error())
	} else {
		pkg.PackOkReply()
	}

	return
}

// Handle OpWrite packet.
func (s *DataNode) handleWritePacket(pkg *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionWrite, err.Error())
		} else {
			pkg.PackOkReply()
		}
	}()
	partition := pkg.Object.(*DataPartition)
	if partition.Available() <= 0 || partition.disk.Status == proto.ReadOnly {
		err = storage.NoSpaceError
		return
	}
	store := partition.ExtentStore()
	if pkg.Size <= util.BlockSize {
		err = store.Write(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data, pkg.CRC)
	} else {
		size := pkg.Size
		offset := 0
		for size > 0 {
			if size <= 0 {
				break
			}
			currSize := util.Min(int(size), util.BlockSize)
			data := pkg.Data[offset: offset+currSize]
			crc := crc32.ChecksumIEEE(data)
			err = store.Write(pkg.ExtentID, pkg.ExtentOffset+int64(offset), int64(currSize), data, crc)
			if err != nil {
				break
			}
			size -= uint32(currSize)
			offset += currSize
		}
	}

	s.addDiskErrs(pkg.PartitionID, err, WriteFlag)
	if err == nil && pkg.Opcode == proto.OpWrite && pkg.Size == util.BlockSize {
		proto.Buffers.Put(pkg.Data)
	}
	return
}

func (s *DataNode) handleRandomWritePacket(pkg *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionWrite, err.Error())
		} else {
			pkg.PackOkReply()
		}
	}()
	partition := pkg.Object.(*DataPartition)
	_, isLeader := partition.IsRaftLeader()
	if !isLeader {
		err = storage.NotALeaderError
		return
	}
	err = partition.RandomWriteSubmit(pkg)
	if err != nil && strings.Contains(err.Error(), raft.ErrNotLeader.Error()) {
		err = storage.NotALeaderError
		return
	}

	if err == nil && pkg.ResultCode != proto.OpOk {
		err = storage.TryAgainError
		return
	}

	if err == nil && pkg.Opcode == proto.OpRandomWrite && pkg.Size == util.BlockSize {
		proto.Buffers.Put(pkg.Data)
	}
}

// Handle OpStreamRead packet.
// TODO should we call it streaming read? double check
func (s *DataNode) handleStreamReadPacket(pkg *repl.Packet, connect net.Conn, isRepairRead bool) {
	var (
		err error
	)
	partition := pkg.Object.(*DataPartition)
	if !isRepairRead {
		err = partition.RandomPartitionReadCheck(pkg, connect)
		if err != nil {
			err = fmt.Errorf(pkg.LogMessage(ActionStreamRead, connect.RemoteAddr().String(),
				pkg.StartT, err))
			log.LogErrorf(err.Error())
			pkg.PackErrorBody(ActionStreamRead, err.Error())
			pkg.WriteToConn(connect)
			return
		}
	}

	needReplySize := pkg.Size
	offset := pkg.ExtentOffset
	store := partition.ExtentStore()
	exporterKey := fmt.Sprintf("%s_datanode_%s", s.clusterID, "Read")
	reply := repl.NewStreamReadResponsePacket(pkg.ReqID, pkg.PartitionID, pkg.ExtentID)
	reply.StartT = time.Now().UnixNano()
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		currReadSize := uint32(util.Min(int(needReplySize), util.ReadBlockSize))
		if currReadSize == util.ReadBlockSize {
			reply.Data, _ = proto.Buffers.Get(util.ReadBlockSize)
		} else {
			reply.Data = make([]byte, currReadSize)
		}
		tpObject := exporter.RegistTp(exporterKey)
		reply.ExtentOffset = offset
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data, isRepairRead)
		tpObject.CalcTp()
		if err != nil {
			reply.PackErrorBody(ActionStreamRead, err.Error())
			pkg.PackErrorBody(ActionStreamRead, err.Error())
			if err = reply.WriteToConn(connect); err != nil {
				err = fmt.Errorf(reply.LogMessage(ActionStreamRead, connect.RemoteAddr().String(),
					reply.StartT, err))
				log.LogErrorf(err.Error())
			}
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect); err != nil {
			err = fmt.Errorf(reply.LogMessage(ActionStreamRead, connect.RemoteAddr().String(),
				reply.StartT, err))
			pkg.PackErrorBody(ActionStreamRead, err.Error())
			log.LogErrorf(err.Error())
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		if currReadSize == util.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
	}
	pkg.PackOkReply()

	return
}

// Handle OpExtentStoreGetAllWaterMark packet.
func (s *DataNode) handlePacketToGetAllWatermarks(pkg *repl.Packet) {
	var (
		buf       []byte
		fInfoList []*storage.ExtentInfo
		err       error
	)
	partition := pkg.Object.(*DataPartition)
	store := partition.ExtentStore()
	if pkg.ExtentType == proto.NormalExtentType {
		fInfoList, err = store.GetAllWatermarks(storage.NormalExtentFilter())
	} else {
		extents := make([]uint64, 0)
		err = json.Unmarshal(pkg.Data, &extents)
		if err == nil {
			fInfoList, err = store.GetAllWatermarks(storage.TinyExtentFilter(extents))
		}
	}
	if err != nil {
		pkg.PackErrorBody(ActionGetAllExtentWaterMarker, err.Error())
	} else {
		buf, err = json.Marshal(fInfoList)
		pkg.PackOkWithBody(buf)
	}
	return
}

// Handle OpNotifyExtentRepair packet.
func (s *DataNode) handlePacketToNotifyExtentRepair(pkg *repl.Packet) {
	var (
		err error
	)
	partition := pkg.Object.(*DataPartition)
	mf := new(DataPartitionRepairTask)
	err = json.Unmarshal(pkg.Data, mf)
	if err != nil {
		pkg.PackErrorBody(ActionRepair, err.Error())
		return
	}
	partition.DoExtentStoreRepair(mf)
	pkg.PackOkReply()
	return
}

func (s *DataNode) handlePacketToGetDataPartitionMetrics(pkg *repl.Packet) {
	partition := pkg.Object.(*DataPartition)
	dp := partition
	data, err := json.Marshal(dp.runtimeMetrics)
	if err != nil {
		pkg.PackErrorBody(ActionGetDataPartitionMetrics, err.Error())
		return
	}

	pkg.PackOkWithBody(data)
	return
}

// Handle OpGetAllWatermark packet.
func (s *DataNode) handlePacketToGetAppliedID(pkg *repl.Packet) {
	partition := pkg.Object.(*DataPartition)
	minAppliedID := binary.BigEndian.Uint64(pkg.Data)
	if minAppliedID > 0 {
		partition.SetMinAppliedID(minAppliedID)
	}

	//return current appliedID
	appliedID := partition.GetAppliedID()

	log.LogDebugf("[updateMaxMinAppliedID] handlePacketToGetAppliedID partition=%v minAppId=%v curAppId=%v",
		partition.ID(), minAppliedID, appliedID)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedID)
	pkg.PackOkWithBody(buf)
	return
}

func (s *DataNode) handlePacketToGetPartitionSize(pkg *repl.Packet) {
	partition := pkg.Object.(*DataPartition)
	usedSize := partition.extentStore.StoreSize()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(usedSize))
	pkg.PackOkWithBody(buf)

	return
}

// TODO find a better name  master 让datapartition 下线
func (s *DataNode) handlePacketToOfflineDataPartition(pkg *repl.Packet) {
	var (
		err          error
		reqData      []byte
		isRaftLeader bool
		req          = &proto.DataPartitionOfflineRequest{}
	)

	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionOfflinePartition, err.Error())
		}
	}()

	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(pkg.Data))
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

	isRaftLeader, err = s.forwardToRaftLeader(dp, pkg)
	if !isRaftLeader {
		return
	}

	pkg.PackOkReply()

	resp := proto.DataPartitionOfflineResponse{
		PartitionId: req.PartitionId,
		Status:      proto.TaskFailed,
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.Errorf("[opOfflineDataPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
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
	resp.Status = proto.TaskSuccess
end:
	adminTask.Request = nil
	adminTask.Response = resp

	data, _ := json.Marshal(adminTask)
	_, err = MasterHelper.Request("POST", master.GetDataNodeTaskResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "opOfflineDataPartition failed, partitionID(%v)", resp.PartitionId)
		log.LogError(errors.ErrorStack(err))
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
		gConnPool.PutConnect(conn, true)
		return
	}

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

	return
}
