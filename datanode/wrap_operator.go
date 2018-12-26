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
			if resultSize <= uint32(len(pkg.Data)) {
				err = fmt.Errorf("op[%v] error[%v]", pkg.GetOpMsg(), string(pkg.Data[:resultSize]))
			} else {
				err = fmt.Errorf("op[%v] error[%v] resultSize=%v lenData=%v",
					pkg.GetOpMsg(), string(pkg.Data[:len(pkg.Data)]), resultSize, pkg.Data)
			}

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
		s.handleCreateExtent(pkg)
	case proto.OpWrite:
		s.handleWrite(pkg)
	case proto.OpRead:
		s.handleRead(pkg)
	case proto.OpStreamRead, proto.OpExtentRepairRead:
		s.handleStreamRead(pkg, c)
	case proto.OpMarkDelete:
		s.handleMarkDelete(pkg)
	case proto.OpRandomWrite:
		s.handleRandomWrite(pkg)
	case proto.OpNotifyExtentRepair:
		s.handleNotifyExtentRepair(pkg)
	case proto.OpGetAllWaterMark:
		s.handleGetAllWatermark(pkg)
	case proto.OpCreateDataPartition:
		s.handleCreateDataPartition(pkg)
	case proto.OpLoadDataPartition:
		s.handleLoadDataPartition(pkg)
	case proto.OpDeleteDataPartition:
		s.handleDeleteDataPartition(pkg)
	case proto.OpDataNodeHeartbeat:
		s.handleHeartbeats(pkg)
	case proto.OpGetDataPartitionMetrics:
		s.handleGetDataPartitionMetrics(pkg)
	case proto.OpGetAppliedId:
		s.handleGetAppliedID(pkg)
	case proto.OpOfflineDataPartition:
		s.handleOfflineDataPartition(pkg)
	case proto.OpGetPartitionSize:
		s.handleGetPartitionSize(pkg)
	default:
		pkg.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(pkg.Opcode)))
	}

	return
}

// Handle OpCreateExtent packet.
func (s *DataNode) handleCreateExtent(pkg *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionCreateExtent, err.Error())
		} else {
			pkg.PackOkReply()
		}
	}()
	partition := pkg.Object.(*DataPartition)
	if partition.Available() <= 0 {
		err = storage.ErrSyscallNoSpace
		return
	}
	var ino uint64
	if len(pkg.Data) >= 8 && pkg.Size >= 8 {
		ino = binary.BigEndian.Uint64(pkg.Data)
	}
	err = partition.GetStore().Create(pkg.ExtentID, ino)

	return
}

// Handle OpCreateDataPartition packet.
func (s *DataNode) handleCreateDataPartition(pkg *repl.Packet) {
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
		err = fmt.Errorf("from master Task[%v] cannot unmash CreateDataPartitionRequest struct", task.ToString())
		return
	}

	return
}

// Handle OpHeartbeat packet.
func (s *DataNode) handleHeartbeats(pkg *repl.Packet) {
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
	response := &proto.DataNodeHeartBeatResponse{}

	s.fillHeartBeatResponse(response)

	if task.OpCode == proto.OpDataNodeHeartbeat {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		response.Status = proto.TaskSuccess
		MasterHelper.AddNode(request.MasterAddr)
	} else {
		response.Status = proto.TaskFail
		err = fmt.Errorf("illegal opcode")
		response.Result = err.Error()
	}
	task.Response = response
	if data, err = json.Marshal(task); err != nil {
		return
	}
	_, err = MasterHelper.Request("POST", master.DataNodeResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "heartbeat to master(%v) failed.", request.MasterAddr)
		return
	}
}

// Handle OpDeleteDataPartition packet.
func (s *DataNode) handleDeleteDataPartition(pkg *repl.Packet) {
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
			response.Status = proto.TaskFail
			response.Result = err.Error()
		} else {
			s.space.DeletePartition(request.PartitionId)
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskSuccess
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFail
		response.Result = "illegal opcode "
		err = fmt.Errorf("illegal opcode ")
	}
	task.Response = response
	data, _ := json.Marshal(task)
	_, err = MasterHelper.Request("POST", master.DataNodeResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "delete DataPartition failed,partitionID(%v)", request.PartitionId)
		log.LogErrorf("action[handleDeleteDataPartition] err(%v).", err)
	}
	log.LogInfof(fmt.Sprintf("action[handleDeleteDataPartition] %v error(%v)", request.PartitionId, string(data)))

}

// Handle OpLoadDataPartition packet.
func (s *DataNode) handleLoadDataPartition(pkg *repl.Packet) {
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
		dp := s.space.GetPartition(request.PartitionId)
		if dp == nil {
			response.Status = proto.TaskFail
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
		response.Status = proto.TaskFail
		err = fmt.Errorf("illegal opcode")
		response.Result = err.Error()
	}
	task.Response = response
	data, err := json.Marshal(task)
	if err != nil {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFail
		response.Result = err.Error()
		err = fmt.Errorf("from master Task(%v) failed,error(%v)", task.ToString(), response.Result)
	}
	_, err = MasterHelper.Request("POST", master.DataNodeResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "load DataPartition failed,partitionID(%v)", request.PartitionId)
		log.LogError(errors.ErrorStack(err))
	}
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleMarkDelete(pkg *repl.Packet) {
	var (
		err error
	)
	partition := pkg.Object.(*DataPartition)
	if pkg.ExtentMode == proto.TinyExtentMode {
		ext := new(proto.ExtentKey)
		err = json.Unmarshal(pkg.Data, ext)
		if err == nil {
			err = partition.GetStore().MarkDelete(pkg.ExtentID, int64(ext.ExtentOffset), int64(ext.Size))
		}
	} else {
		err = partition.GetStore().MarkDelete(pkg.ExtentID, 0, 0)
	}
	if err != nil {
		pkg.PackErrorBody(ActionMarkDel, err.Error())
	} else {
		pkg.PackOkReply()
	}

	return
}

// Handle OpWrite packet.
func (s *DataNode) handleWrite(pkg *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			pkg.PackErrorBody(ActionWrite, err.Error())
		} else {
			pkg.PackOkReply()
		}
	}()
	partition := pkg.Object.(*DataPartition)
	if partition.Available() <= 0 {
		err = storage.ErrSyscallNoSpace
		return
	}
	err = partition.GetStore().Write(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data, pkg.CRC)
	s.addDiskErrs(pkg.PartitionID, err, WriteFlag)
	if err == nil && pkg.Opcode == proto.OpWrite && pkg.Size == util.BlockSize {
		proto.Buffers.Put(pkg.Data)
	}
	return
}

func (s *DataNode) handleRandomWrite(pkg *repl.Packet) {
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
		err = storage.ErrNotLeader
		return
	}
	err = partition.RandomWriteSubmit(pkg)
	if err != nil && strings.Contains(err.Error(), raft.ErrNotLeader.Error()) {
		err = storage.ErrNotLeader
		return
	}

	if err == nil && pkg.ResultCode != proto.OpOk {
	    err = storage.ErrorAgain
	    return
	}

	if err == nil && pkg.Opcode == proto.OpRandomWrite && pkg.Size == util.BlockSize {
		proto.Buffers.Put(pkg.Data)
	}
}

// Handle OpRead packet.
func (s *DataNode) handleRead(pkg *repl.Packet) {
	pkg.Data = make([]byte, pkg.Size)
	var err error
	partition := pkg.Object.(*DataPartition)
	pkg.CRC, err = partition.GetStore().Read(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data)
	s.addDiskErrs(pkg.PartitionID, err, ReadFlag)
	if err == nil {
		pkg.ResultCode = proto.OpOk
		pkg.Arglen = 0
	} else {
		pkg.PackErrorBody(ActionRead, err.Error())
	}

	return
}

// Handle OpStreamRead packet.
func (s *DataNode) handleStreamRead(pkg *repl.Packet, connect net.Conn) {
	var (
		err error
	)
	partition := pkg.Object.(*DataPartition)
	err = partition.RandomPartitionReadCheck(pkg, connect)
	if err != nil {
		err = fmt.Errorf(pkg.LogMessage(ActionStreamRead, connect.RemoteAddr().String(),
			pkg.StartT, err))
		log.LogErrorf(err.Error())
		pkg.PackErrorBody(ActionStreamRead, err.Error())
		pkg.WriteToConn(connect)
		return
	}

	needReplySize := pkg.Size
	offset := pkg.ExtentOffset
	store := partition.GetStore()
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
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data)
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
func (s *DataNode) handleGetAllWatermark(pkg *repl.Packet) {
	var (
		buf       []byte
		fInfoList []*storage.ExtentInfo
		err       error
	)
	partition := pkg.Object.(*DataPartition)
	store := partition.GetStore()
	if pkg.ExtentMode == proto.NormalExtentMode {
		fInfoList, err = store.GetAllExtentWatermark(storage.GetStableExtentFilter())
	} else {
		extents := make([]uint64, 0)
		err = json.Unmarshal(pkg.Data, &extents)
		if err == nil {
			fInfoList, err = store.GetAllExtentWatermark(storage.GetStableTinyExtentFilter(extents))
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
func (s *DataNode) handleNotifyExtentRepair(pkg *repl.Packet) {
	var (
		err error
	)
	partition := pkg.Object.(*DataPartition)
	extents := make([]*storage.ExtentInfo, 0)
	mf := NewDataPartitionRepairTask(extents)
	err = json.Unmarshal(pkg.Data, mf)
	if err != nil {
		pkg.PackErrorBody(ActionRepair, err.Error())
		return
	}
	partition.MergeExtentStoreRepair(mf)
	pkg.PackOkReply()
	return
}

func (s *DataNode) handleGetDataPartitionMetrics(pkg *repl.Packet) {
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
func (s *DataNode) handleGetAppliedID(pkg *repl.Packet) {
	partition := pkg.Object.(*DataPartition)
	minAppliedID := binary.BigEndian.Uint64(pkg.Data)
	if minAppliedID > 0 {
		partition.SetMinAppliedID(minAppliedID)
	}

	//return current appliedID
	appliedID := partition.GetAppliedID()

	log.LogDebugf("[updateMaxMinAppliedID] handleGetAppliedID partition=%v minAppId=%v curAppId=%v",
		partition.ID(), minAppliedID, appliedID)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedID)
	pkg.PackOkWithBody(buf)
	return
}

func (s *DataNode) handleGetPartitionSize(pkg *repl.Packet) {
	partition := pkg.Object.(*DataPartition)
	usedSize := partition.extentStore.GetStoreSize()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(usedSize))
	pkg.PackOkWithBody(buf)

	return
}

func (s *DataNode) handleOfflineDataPartition(pkg *repl.Packet) {
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
	dp := s.space.GetPartition(req.PartitionId)
	if err != nil {
		return
	}

	isRaftLeader, err = s.transferToRaftLeader(dp, pkg)
	if !isRaftLeader {
		return
	}

	pkg.PackOkReply()

	resp := proto.DataPartitionOfflineResponse{
		PartitionId: req.PartitionId,
		Status:      proto.TaskFail,
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
	_, err = MasterHelper.Request("POST", master.DataNodeResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "opOfflineDataPartition failed, partitionID(%v)", resp.PartitionId)
		log.LogError(errors.ErrorStack(err))
		return
	}

	log.LogDebugf("[opOfflineDataPartition]: the end %v", adminTask)
	return
}

func (s *DataNode) transferToRaftLeader(dp *DataPartition, p *repl.Packet) (ok bool, err error) {
	var (
		conn       *net.TCPConn
		leaderAddr string
	)

	// If local is leader return to continue.
	if leaderAddr, ok = dp.IsRaftLeader(); ok {
		return
	}

	// If leaderAddr is nil return ErrNoLeader
	if leaderAddr == "" {
		err = storage.ErrNoLeader
		return
	}

	// If local is not leader transfer the packet to leader
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
