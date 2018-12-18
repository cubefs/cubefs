package datanode



import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/master"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
	raftProto "github.com/tiglabs/raft/proto"
	"github.com/tiglabs/containerfs/repl"
)

var (
	ErrorUnknownOp = errors.New("unknown opcode")
)

func (s *DataNode) operatePacket(pkg *repl.Packet, c *net.TCPConn)(err error) {
	orgSize := pkg.Size
	umpKey := fmt.Sprintf("%s_datanode_%s", s.clusterId, pkg.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	start := time.Now().UnixNano()
	defer func() {
		resultSize := pkg.Size
		pkg.Size = orgSize
		if err!=nil {
			err = fmt.Errorf("op[%v] error[%v]", pkg.GetOpMsg(), err.Error())
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				pkg.LogMessage(pkg.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				pkg.LogMessage(pkg.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch pkg.Opcode {
			case proto.OpStreamRead, proto.OpRead, proto.OpExtentRepairRead:
				log.LogRead(logContent)
			case proto.OpWrite:
				log.LogWrite(logContent)
			default:
				log.LogInfo(logContent)
			}
		}
		pkg.Size = resultSize
		ump.AfterTP(tpObject, err)
	}()
	switch pkg.Opcode {
	case proto.OpCreateExtent:
		err=s.handleCreateFile(pkg)
	case proto.OpWrite:
		err=s.handleWrite(pkg)
	case proto.OpRead:
		err=s.handleRead(pkg)
	case proto.OpStreamRead, proto.OpExtentRepairRead:
		err=s.handleStreamRead(pkg, c)
	case proto.OpMarkDelete:
		err=s.handleMarkDelete(pkg)
	case proto.OpNotifyExtentRepair:
		err=s.handleNotifyExtentRepair(pkg)
	case proto.OpGetAllWaterMark:
		err=s.handleGetAllWatermark(pkg)
	case proto.OpCreateDataPartition:
		err=s.handleCreateDataPartition(pkg)
	case proto.OpLoadDataPartition:
		err=s.handleLoadDataPartition(pkg)
	case proto.OpDeleteDataPartition:
		err=s.handleDeleteDataPartition(pkg)
	case proto.OpDataNodeHeartbeat:
		err=s.handleHeartbeats(pkg)
	case proto.OpGetDataPartitionMetrics:
		err=s.handleGetDataPartitionMetrics(pkg)
	case proto.OpGetAppliedId:
		err=s.handleGetAppliedId(pkg)
	case proto.OpOfflineDataPartition:
		err=s.handleOfflineDataPartition(pkg)
	case proto.OpGetPartitionSize:
		err=s.handleGetPartitionSize(pkg)
	case proto.OpRandomWrite:
		err=s.handleRandomWrite(pkg)
	default:
		err=fmt.Errorf(ErrorUnknownOp.Error()+strconv.Itoa(int(pkg.Opcode)))
	}

	return
}

// Handle OpCreateExtent packet.
func (s *DataNode) handleCreateFile(pkg *repl.Packet)(err error) {
	partition:=pkg.Object.(DataPartition)
	if partition.Status() == proto.ReadOnly {
		log.LogInfof("createFile %v ERROR %v ", pkg.GetUniqueLogId(), err)
		return
	}
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
func (s *DataNode) handleCreateDataPartition(pkg *repl.Packet)(err error) {
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

	bytes, err := json.Marshal(task.Request)
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
func (s *DataNode) handleHeartbeats(pkg *repl.Packet)(err error) {
	task := &proto.AdminTask{}
	json.Unmarshal(pkg.Data, task)
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
		response.Result = "illegal opcode"
	}
	task.Response = response
	data, err := json.Marshal(task)
	if err != nil {
		log.LogErrorf("action[heartbeat] err(%v).", err)
		return
	}
	_, err = MasterHelper.Request("POST", master.DataNodeResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "heartbeat to master(%v) failed.", request.MasterAddr)
		log.LogErrorf("action[handleHeartbeats] err(%v).", err)
		log.LogErrorf(errors.ErrorStack(err))
		return
	}
	log.LogDebugf("action[handleHeartbeats] report data len(%v) to master success.", len(data))
}

// Handle OpDeleteDataPartition packet.
func (s *DataNode) handleDeleteDataPartition(pkg *repl.Packet)(err error) {
	task := &proto.AdminTask{}
	json.Unmarshal(pkg.Data, task)
	request := &proto.DeleteDataPartitionRequest{}
	response := &proto.DeleteDataPartitionResponse{}
	if task.OpCode == proto.OpDeleteDataPartition {
		bytes, _ := json.Marshal(task.Request)
		err := json.Unmarshal(bytes, request)
		if err != nil {
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskFail
			response.Result = err.Error()
			log.LogErrorf("action[handleDeleteDataPartition] from master Task(%v) failed, err(%v)", task.ToString(), err)
		} else {
			s.space.DeletePartition(uint32(request.PartitionId))
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskSuccess
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFail
		response.Result = "illegal opcode "
		log.LogErrorf("action[handleDeleteDataPartition] from master Task(%v) failed, err(%v).", task.ToString(), response.Result)
	}
	task.Response = response
	data, _ := json.Marshal(task)
	_, err = MasterHelper.Request("POST", master.DataNodeResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "delete dataPartition failed,partitionId(%v)", request.PartitionId)
		log.LogErrorf("action[handleDeleteDataPartition] err(%v).", err)
	}
	log.LogInfof(fmt.Sprintf("action[handleDeleteDataPartition] %v error(%v)", request.PartitionId, string(data)))

}

// Handle OpLoadDataPartition packet.
func (s *DataNode) handleLoadDataPartition(pkg *repl.Packet)(err error) {
	task := &proto.AdminTask{}
	json.Unmarshal(pkg.Data, task)
	request := &proto.LoadDataPartitionRequest{}
	response := &proto.LoadDataPartitionResponse{}
	if task.OpCode == proto.OpLoadDataPartition {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		dp := s.space.GetPartition(uint32(request.PartitionId))
		if dp == nil {
			response.Status = proto.TaskFail
			response.PartitionId = uint64(request.PartitionId)
			response.Result = fmt.Sprintf("dataPartition(%v) not found", request.PartitionId)
			log.LogErrorf("from master Task(%v) failed,error(%v)", task.ToString(), response.Result)
		} else {
			response = dp.(*dataPartition).Load()
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskSuccess
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFail
		response.Result = "illegal opcode "
		log.LogErrorf("from master Task(%v) failed,error(%v)", task.ToString(), response.Result)
	}
	task.Response = response
	data, err := json.Marshal(task)
	if err != nil {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFail
		response.Result = err.Error()
		log.LogErrorf("from master Task[%v] failed,error[%v]", task.ToString(), response.Result)
	}
	_, err = MasterHelper.Request("POST", master.DataNodeResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "load dataPartition failed,partitionId(%v)", request.PartitionId)
		log.LogError(errors.ErrorStack(err))
	}
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleMarkDelete(pkg *repl.Packet) (err error){
	partition:=pkg.Object.(DataPartition)
	if pkg.StoreMode == proto.TinyExtentMode {
		ext := new(proto.ExtentKey)
		err = json.Unmarshal(pkg.Data, ext)
		if err == nil {
			err = partition.GetStore().MarkDelete(pkg.ExtentID, int64(ext.ExtentOffset), int64(ext.Size))
		}
	} else {
		err = partition.GetStore().MarkDelete(pkg.ExtentID, 0, 0)
	}

	return
}

// Handle OpWrite packet.
func (s *DataNode) handleWrite(pkg *repl.Packet)(err error) {
	partition:=pkg.Object.(DataPartition)
	if partition.Status() == proto.ReadOnly {
		err = storage.ErrorPartitionReadOnly
		return
	}
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


func (s *DataNode)handleRandomWrite(pkg *repl.Packet){
	
}


// Handle OpRead packet.
func (s *DataNode) handleRead(pkg *repl.Packet)(err error) {
	partition:=pkg.Object.(DataPartition)
	pkg.Data = make([]byte, pkg.Size)
	pkg.CRC, err = partition.GetStore().Read(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data)
	s.addDiskErrs(pkg.PartitionID, err, ReadFlag)

	return
}

// Handle OpStreamRead packet.
func (s *DataNode) handleStreamRead(request *repl.Packet, connect net.Conn)(err error) {
	partition:=request.Object.(DataPartition)
	if err = partition.RandomPartitionReadCheck(request, connect); err != nil {
		request.PackErrorBody(ActionStreamRead, err.Error())
		if err = request.WriteToConn(connect); err != nil {
			err = fmt.Errorf(request.LogMessage(ActionWriteToCli, connect.RemoteAddr().String(), request.StartT, err))
			log.LogErrorf(err.Error())
		}
		return
	}

	needReplySize := request.Size
	offset := request.ExtentOffset
	store := partition.GetStore()
	umpKey := fmt.Sprintf("%s_datanode_%s", s.clusterId, "Read")
	reply := NewStreamReadResponsePacket(request.ReqID, request.PartitionID, request.ExtentID)
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
		tpObject := ump.BeforeTP(umpKey)
		reply.ExtentOffset = offset
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data)
		ump.AfterTP(tpObject, err)
		if err != nil {
			reply.PackErrorBody(ActionStreamRead, err.Error())
			if err = reply.WriteToConn(connect); err != nil {
				err = fmt.Errorf(reply.LogMessage(ActionWriteToCli, connect.RemoteAddr().String(),
					reply.StartT, err))
				log.LogErrorf(err.Error())
			}
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect); err != nil {
			err = fmt.Errorf(reply.LogMessage(ActionWriteToCli, connect.RemoteAddr().String(),
				reply.StartT, err))
			log.LogErrorf(err.Error())
			connect.Close()
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		if currReadSize == util.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
	}
	request.ResultCode = reply.ResultCode
	return
}

// Handle OpExtentStoreGetAllWaterMark packet.
func (s *DataNode) handleGetAllWatermark(pkg *repl.Packet) (err error){
	var (
		buf       []byte
		fInfoList []*storage.FileInfo
		err       error
	)
	partition:=pkg.Object.(DataPartition)
	store := partition.GetStore()
	if pkg.StoreMode == proto.NormalExtentMode {
		fInfoList, err = store.GetAllWatermark(storage.GetStableExtentFilter())
	} else {
		extents := make([]uint64, 0)
		err = json.Unmarshal(pkg.Data, &extents)
		if err == nil {
			fInfoList, err = store.GetAllWatermark(storage.GetStableTinyExtentFilter(extents))
		}
	}
	if err != nil {
		err = errors.Annotatef(err, "Request(%v) handleExtentStoreGetAllWatermark Error", pkg.GetUniqueLogId())
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
	extents := make([]*storage.FileInfo, 0)
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
	dp := partition.(*dataPartition)
	data, err := json.Marshal(dp.runtimeMetrics)
	if err != nil {
		err = errors.Annotatef(err, "dataPartionMetrics(%v) json mashal failed", dp.ID())
		pkg.PackErrorBody(ActionGetDataPartitionMetrics, err.Error())
		return
	} else {
		pkg.PackOkWithBody(data)
	}
}

// Handle OpGetAllWatermark packet.
func (s *DataNode) handleGetAppliedId(pkg *repl.Packet) {
	//update minAppliedId
	minAppliedId := binary.BigEndian.Uint64(pkg.Data)
	if minAppliedId > 0 {
		partition.SetMinAppliedId(minAppliedId)
	}

	//return current appliedId
	appliedId := partition.GetAppliedId()

	log.LogDebugf("[getMinAppliedId] handleGetAppliedId partition=%v minAppId=%v curAppId=%v",
		partition.ID(), minAppliedId, appliedId)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedId)
	pkg.PackOkWithBody(buf)
	return
}

func (s *DataNode) handleGetPartitionSize(pkg *repl.Packet) {
	partitionSize := partition.GetPartitionSize()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(partitionSize))
	pkg.PackOkWithBody(buf)

	log.LogDebugf("handleGetPartitionSize partition=%v partitionSize=%v",
		partition.ID(), partitionSize)
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
			err = errors.Annotatef(err, "Request(%v) OfflineDP Error", pkg.GetUniqueLogId())
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
	dp := s.space.GetPartition(uint32(req.PartitionId))
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
		goto end
	}
	_, err = dp.ChangeRaftMember(raftProto.ConfRemoveNode, raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	resp.Status = proto.TaskSuccess
end:
	adminTask.Request = nil
	adminTask.Response = resp

	data, _ := json.Marshal(adminTask)
	_, err = MasterHelper.Request("POST", master.DataNodeResponse, nil, data)
	if err != nil {
		err = errors.Annotatef(err, "offline dataPartition failed, partitionId(%v)", resp.PartitionId)
		log.LogError(errors.ErrorStack(err))
		return
	}

	log.LogDebugf("[opOfflineDataPartition]: the end %v", adminTask)
	return
}

func (d *DataNode) transferToRaftLeader(dp DataPartition, p *repl.Packet) (ok bool, err error) {
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
