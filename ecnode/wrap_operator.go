package ecnode

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/ecstorage"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

func (e *EcNode) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size
	tpObject := exporter.NewModuleTP(p.GetOpMsg())
	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op(%v) error(%v)", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch p.Opcode {
			default:
				log.LogInfo(logContent)
			}
		}
		p.Size = resultSize
		tpObject.Set(err)
	}()

	switch p.Opcode {
	case proto.OpCreateEcDataPartition:
		e.handlePacketToCreateEcPartition(p)
	case proto.OpUpdateEcDataPartition:
		e.handleUpdateEcPartition(p)
	case proto.OpEcNodeHeartbeat:
		e.handleHeartbeatPacket(p)
	case proto.OpCreateExtent:
		e.handlePacketToCreateExtent(p)
	case proto.OpEcWrite, proto.OpSyncEcWrite:
		e.handleWritePacket(p)
	case proto.OpEcRead:
		e.handleReadPacket(p, c)
	case proto.OpStreamEcRead:
		e.handleStreamReadPacket(p, c)
	case proto.OpEcTinyRepairRead:
		e.handleTinyRepairRead(p, c)
	case proto.OpEcTinyDelInfoRead:
		e.handlePacketToReadTinyDeleteRecord(p, c)
	case proto.OpEcOriginTinyDelInfoRead:
		e.handlePacketToReadOriginTinyDelRecord(p, c)
	case proto.OpNotifyReplicasToRepair:
		e.handleRepairWrite(p)
	case proto.OpNotifyEcRepairTinyDelInfo:
		e.handleRepairTinyDelInfo(p)
	case proto.OpNotifyEcRepairOriginTinyDelInfo:
		e.handleRepairOriginTinyDelInfo(p)
	case proto.OpChangeEcPartitionMembers:
		e.handleChangeMember(p)
	case proto.OpGetAllWatermarks:
		e.handleGetAllWatermarks(p)
	case proto.OpEcGetTinyDeletingInfo:
		e.handleGetTinyDeletingInfo(p)
	case proto.OpDeleteEcDataPartition:
		e.handlePacketToDeleteEcPartition(p)
	case proto.OpMarkDelete:
		e.handleMarkDeletePacket(p, c)
	case proto.OpBatchDeleteExtent:
		e.handleEcBatchMarkDeletePacket(p, c)
	case proto.OpEcTinyDelete:
		e.handleEcTinyDeletePacket(p, c)
	case proto.OpEcRecordTinyDelInfo:
		e.handleRecordEcTinyDelInfo(p, c)
	case proto.OpPersistTinyExtentDelete:
		e.handlePersistTinyExtentDelete(p)
	case proto.OpEcNodeDail: //for host0 before tinyDelete, ping ParityNode
		p.PacketOkReply()
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}
	return
}

//handle OpEcTinyDelete packet.
func (e *EcNode) handleEcTinyDeletePacket(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("handleEcMarkDeletePacket err[%v]", err)
			p.PackErrorBody(ActionMarkDelete, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	remote := c.RemoteAddr().String()
	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}
	log.LogDebugf("handleEcMarkDeletePacket PartitionID(%v)_Extent(%v) from (%v)",
		p.PartitionID, p.ExtentID, remote)
	ext := new(proto.TinyExtentDeleteRecord)
	if err = json.Unmarshal(p.Data, ext); err != nil {
		return
	}
	err = ep.extentStore.EcMarkDelete(p.ExtentID, ep.NodeIndex, int64(ext.ExtentOffset), int64(ext.Size))
	if err == ecstorage.ExtentNotFoundError {
		err = nil
	}
	return
}

func (e *EcNode) handlePacketToReadOriginTinyDelRecord(p *repl.Packet, connect *net.TCPConn) {
	var (
		err       error
		replyData []byte
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamReadOriginTinyDelRecord, err.Error())
			p.WriteToConn(connect, proto.WriteDeadlineTime)
		}
	}()
	partition := p.Object.(*EcPartition)
	log.LogDebugf("into handlePacketToReadOriginTinyDeleteRecord partition(%v) ", partition.PartitionID)

	if len(partition.originTinyExtentDeleteMap) > 0 {
		reply := repl.NewReadTinyDeleteRecordResponsePacket(p.Ctx(), p.ReqID, p.PartitionID)
		reply.StartT = time.Now().UnixNano()
		log.LogDebugf("handlePacketToReadOriginTinyDeleteFile partition(%v) originDeleteSize(%v)", partition.PartitionID, len(partition.originTinyExtentDeleteMap))
		replyData, err = json.Marshal(partition.originTinyExtentDeleteMap)
		if err != nil {
			return
		}
		reply.Size = uint32(len(replyData))
		reply.Data = make([]byte, reply.Size)
		reply.Data = replyData
		reply.CRC = crc32.ChecksumIEEE(reply.Data)
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
			return
		}
	}
	p.PacketOkReply()
	return
}

func (e *EcNode) handlePacketToReadTinyDeleteRecord(p *repl.Packet, connect *net.TCPConn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamReadTinyDelRecord, err.Error())
			p.WriteToConn(connect, proto.WriteDeadlineTime)
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*EcPartition)
	store := partition.ExtentStore()
	needReplyDeleteCount, err := store.GetEcTinyDeleteCount()
	if err != nil {
		return
	}
	if needReplyDeleteCount == 0 {
		err = errors.NewErrorf("localDeleteCount is zero")
		return
	}
	reply := repl.NewReadTinyDeleteRecordResponsePacket(p.Ctx(), p.ReqID, p.PartitionID)
	reply.StartT = time.Now().UnixNano()
	tinyDeleteRecords, err := store.GetAllEcTinyDeleteRecord()
	if len(tinyDeleteRecords) == 0 || err != nil {
		err = errors.NewErrorf("tinyDeleteRecords get failed err[%v]", err)
		return
	}
	log.LogDebugf("tinyDeleteRecords: %v", tinyDeleteRecords)
	replyData, err := json.Marshal(tinyDeleteRecords)
	if err != nil {
		return
	}
	reply.Size = uint32(len(replyData))
	reply.Data = make([]byte, reply.Size)
	reply.Data = replyData
	reply.CRC = crc32.ChecksumIEEE(reply.Data)
	reply.ResultCode = proto.OpOk
	if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
		return
	}
	return
}

// Handle handleTinyExtentRepairRead packet.
func (s *EcNode) handleTinyRepairRead(request *repl.Packet, connect net.Conn) {
	var (
		err          error
		needReadSize int64
	)

	defer func() {
		if err != nil {
			log.LogErrorf("ActionTinyExtentRepairRead err: %v", err)
			request.PackErrorBody(ActionTinyExtentRepairRead, err.Error())
			request.WriteToConn(connect, proto.WriteDeadlineTime)
		}
	}()
	if !ecstorage.IsTinyExtent(request.ExtentID) {
		err = fmt.Errorf("unavail extentID (%v)", request.ExtentID)
		return
	}

	partition := request.Object.(*EcPartition)

	//get extentSize
	originExtentSize, err := partition.GetOriginExtentSize(request.ExtentID)
	if err != nil {
		return
	}

	//calc stripeUnitSize
	stripeUnitSize := proto.CalStripeUnitSize(originExtentSize, partition.EcMaxUnitSize, uint64(partition.EcDataNum))

	ecStripe, err := NewEcStripe(partition, stripeUnitSize, request.ExtentID)
	if err != nil {
		return
	}

	if err = partition.checkDataCanRead(request.ExtentID, uint64(request.ExtentOffset), uint64(request.Size)); err != nil {
		return
	}
	store := partition.ExtentStore()

	offset := request.ExtentOffset
	needReadSize = int64(request.Size)
	reply := repl.NewTinyExtentStreamReadResponsePacket(request.Ctx(), request.ReqID, request.PartitionID, request.ExtentID)
	reply.Data = make([]byte, needReadSize)
	_, _, err = store.EcRead(reply.ExtentID, offset, needReadSize, reply.Data[:needReadSize], true)
	if !partition.checkIsEofError(err, ecStripe, originExtentSize, uint64(request.ExtentOffset), uint64(request.Size)) {
		err = nil
	}
	partition.checkIsDiskError(err)
	if err != nil {
		return
	}
	reply.ExtentOffset = request.ExtentOffset
	reply.Size = request.Size
	reply.CRC = crc32.ChecksumIEEE(reply.Data)
	reply.ResultCode = proto.OpOk
	if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
		msg := fmt.Sprintf("TinyRepairRead write to client fail. ReqID(%v) PartitionID(%v) ExtentID(%v) ExtentOffset[%v] Size[%v] err:%v",
			reply.ReqID, reply.PartitionID, reply.ExtentID, reply.ExtentOffset, reply.Size, err)
		err = errors.New(msg)
		return
	}
	request.PacketOkReply()
	return
}

func (e *EcNode) handleEcMarkDeleteTinyExtent(extentId, offset, size uint64, ep *EcPartition) (err error) {
	//get extentSize
	var (
		originExtentSize uint64
		extentOffset     uint64
	)
	//get extentSize
	originExtentSize, err = ep.GetOriginExtentSize(extentId)
	if err != nil {
		return
	}

	stripeUnitSize := proto.CalStripeUnitSize(originExtentSize, ep.EcMaxUnitSize, uint64(ep.EcDataNum))
	ecStripe, _ := NewEcStripe(ep, stripeUnitSize, extentId)
	canDelSize := size
	extentOffset = ep.getTinyExtentOffset(extentId, offset)
	for {
		if canDelSize <= 0 {
			break
		}
		nodeAddr := ecStripe.calcNode(extentOffset, stripeUnitSize)
		nodeIndex, nodeExist := proto.GetEcNodeIndex(nodeAddr, ep.Hosts)
		if !nodeExist {
			err = errors.NewErrorf("nodeAddr[%v] not exist", nodeAddr)
			return
		}
		stripeUnitFileOffset := ecStripe.calcStripeUnitFileOffset(extentOffset, stripeUnitSize)
		delSize := ecStripe.calcCanDelSize(extentOffset, canDelSize, stripeUnitSize)
		log.LogDebugf("nodeAddr(%v) localserver(%v) partition(%v) extent(%v) nodeIndex(%v) extentOffset(%v) stripeUnitFileOffset(%v) canDelSize(%v) delSize(%v)",
			nodeAddr, e.localServerAddr, ep.PartitionID, extentId, nodeIndex, extentOffset, stripeUnitFileOffset, canDelSize, delSize)

		err = ep.ExtentStore().EcRecordTinyDelete(extentId, stripeUnitFileOffset, delSize, ecstorage.TinyDeleteMark, uint32(nodeIndex))
		if err != nil {
			return
		}

		canDelSize = canDelSize - delSize
		extentOffset = extentOffset + delSize
	}
	return
}

// Handle OpMarkDelete packet.
func (e *EcNode) handleMarkDeletePacket(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("handleMarkDeletePacket err[%v]", err)
			p.PackErrorBody(ActionMarkDelete, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	remote := c.RemoteAddr().String()
	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}
	log.LogDebugf("handleMarkDeletePacket Delete PartitionID(%v)_Extent(%v) from (%v)",
		p.PartitionID, p.ExtentID, remote)

	if p.ExtentType == proto.TinyExtentType {
		ext := new(proto.TinyExtentDeleteRecord)
		if err = json.Unmarshal(p.Data, ext); err != nil {
			return
		}
		err = e.handleEcMarkDeleteTinyExtent(p.ExtentID, ext.ExtentOffset, uint64(ext.Size), ep)
	} else {
		ep.extentStore.EcMarkDelete(p.ExtentID, ep.NodeIndex, 0, 0)
		ep.deleteOriginExtentSize(p.ExtentID)
	}
	if err == ecstorage.ExtentNotFoundError {
		err = nil
	}
	return
}

func (e *EcNode) handleRecordEcTinyDelInfo(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("handleRecordEcTinyDelInfo err[%v]", err)
			p.PackErrorBody(ActionRecordTinyDelInfo, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	remote := c.RemoteAddr().String()
	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	delInfo := new(proto.TinyDelInfo)
	if err = json.Unmarshal(p.Data, delInfo); err != nil {
		return
	}
	log.LogDebugf("handleRecordEcTinyDelInfo Delete PartitionID(%v)_Extent(%v) offset(%v) size(%v) delStatus(%v) from (%v)",
		p.PartitionID, p.ExtentID, delInfo.Offset, delInfo.Size, delInfo.DeleteStatus, remote)
	err = ep.extentStore.EcRecordTinyDelete(p.ExtentID, delInfo.Offset, delInfo.Size, delInfo.DeleteStatus, delInfo.HostIndex)
	return
}

// Handle OpMarkDelete packet.
func (e *EcNode) handleEcBatchMarkDeletePacket(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			log.LogError(err)
			p.PackErrorBody(ActionMarkDelete, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	remote := c.RemoteAddr().String()
	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}
	var exts []*proto.ExtentKey
	err = json.Unmarshal(p.Data, &exts)
	if err == nil {
		for _, ext := range exts {
			log.LogDebugf("handleEcBatchMarkDeletePacket Delete partition(%v) extent(%v) offset(%v) size(%v) from (%v)",
				p.PartitionID, ext.ExtentId, ext.ExtentOffset, ext.Size, remote)
			if ecstorage.IsTinyExtent(ext.ExtentId) {
				if tmpErr := e.handleEcMarkDeleteTinyExtent(ext.ExtentId, ext.ExtentOffset, uint64(ext.Size), ep); tmpErr != nil {
					err = errors.NewErrorf("handleEcBatchMarkDeletePacket partition(%v) extent(%v) offset(%v) size(%v) err[%v]",
						ep.PartitionID, ext.ExtentId, ext.ExtentOffset, ext.Size, tmpErr)
					continue
				}
			} else {
				ep.extentStore.EcMarkDelete(ext.ExtentId, ep.NodeIndex, 0, 0)
				ep.deleteOriginExtentSize(ext.ExtentId)
			}
		}
	}
	return
}

// Handle OpGetAllWatermarks packet.
func (e *EcNode) handleGetAllWatermarks(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("%v packet:%v error: %v", ActionGetAllExtentWatermarks, p, err)
			p.PackErrorBody(ActionGetAllExtentWatermarks, err.Error())
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	extentList, holes, err := ep.listExtentsLocal()
	if err != nil {
		return
	}

	buf, err := json.Marshal(extentList)
	if err != nil {
		return
	}
	p.PacketOkWithBody(buf)
	p.Arg, err = json.Marshal(holes)
	if err != nil {
		return
	}
	p.ArgLen = uint32(len(p.Arg))

}

func (e *EcNode) handleGetTinyDeletingInfo(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("%v packet:%v error: %v", ActionGetTinyDeletingInfo, p, err)
			p.PackErrorBody(ActionGetTinyDeletingInfo, err.Error())
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	extentDeletingInfo, err := ep.getLocalTinyDeletingInfo(p.ExtentID)
	if err != nil {
		return
	}

	buf, err := json.Marshal(extentDeletingInfo)
	if err != nil {
		return
	}
	p.PacketOkWithBody(buf)
}

func (e *EcNode) handleRepairOriginTinyDelInfo(p *repl.Packet) {
	var (
		localOriginTinyDeleteSize int64
		err                       error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRepairTinyDelInfo, err.Error())
			log.LogErrorf("handleRepairOriginTinyDelInfo fail. PartitionID(%v) err: %v",
				p.PartitionID, err)
		} else {
			p.PacketOkReply()
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}
	localOriginTinyDeleteSize = int64(len(ep.originTinyExtentDeleteMap))
	task := &tinyFileRepairData{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	log.LogDebugf(ActionRepairTinyDelInfo+" start PartitionID(%v) localOriginTinyDeleteSize(%v) MaxOriginTinyDelSize(%v)",
		ep.PartitionID, localOriginTinyDeleteSize, task.MaxTinyDelCount)
	if localOriginTinyDeleteSize >= task.MaxTinyDelCount {
		log.LogDebugf("don't need repair tinyDeleteFile")
		return
	}
	err = ep.repairOriginTinyDelInfo(task.NormalHost)
}

func (e *EcNode) handleRepairTinyDelInfo(p *repl.Packet) {
	var (
		localTinyDeleteCount int64
		err                  error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRepairTinyDelInfo, err.Error())
			log.LogErrorf("handleRepairTinyDelInfo fail. PartitionID(%v) err: %v",
				p.PartitionID, err)
		} else {
			p.PacketOkReply()
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}
	if localTinyDeleteCount, err = ep.extentStore.GetEcTinyDeleteCount(); err != nil {
		return
	}
	task := &tinyFileRepairData{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	log.LogDebugf(ActionRepairTinyDelInfo+" start PartitionID(%v) localTinyDeleteCount(%v) MaxTinyDelFileSIZE(%v)",
		ep.PartitionID, localTinyDeleteCount, task.MaxTinyDelCount)
	if localTinyDeleteCount >= task.MaxTinyDelCount {
		log.LogDebugf("don't need repair tinyDeleteFile")
		return
	}
	err = ep.repairTinyDelete(task.NormalHost, task.MaxTinyDelCount)
}

// Handle OpNotifyReplicasToRepair packet.
func (e *EcNode) handleRepairWrite(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionReplicasToRepair, err.Error())
			log.LogErrorf("ActionReplicasToRepair fail. PartitionID(%v) ExtentID(%v) err: %v",
				p.PartitionID, p.ExtentID, err)
		} else {
			p.PacketOkReply()
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}
	store := ep.extentStore
	task := &extentData{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}

	if !store.HasExtent(p.ExtentID) {
		if err = ep.extentCreate(p.ExtentID, task.OriginExtentSize); err != nil {
			err = fmt.Errorf("extentCreate fail :%v", err)
			return
		}
	}

	dataCrc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if dataCrc != p.CRC {
		err = fmt.Errorf("data and crc is inconsistent")
		return
	}
	repairSize := uint32(len(task.Data))
	repairData := task.Data[:repairSize]
	repairCrc := crc32.ChecksumIEEE(repairData)
	storeType := ecstorage.AppendWriteType
	if task.RepairFlag != proto.DataRepair { //scrub repair and tiny delete update parityNode need randomWrite
		storeType = ecstorage.RandomWriteType
	}

	log.LogDebugf("RepairWrite PartitionID(%v) ExtentId(%v) offset(%v) size(%v) data len(%v), crc(%v) repairFlag(%v)",
		p.PartitionID, p.ExtentID, p.ExtentOffset, repairSize, len(p.Data), p.CRC, task.RepairFlag)
	err = ep.repairWriteToExtent(p, int64(repairSize), repairData, repairCrc, storeType)
	if err != nil {
		return
	}
	if task.RepairFlag == proto.DeleteRepair {
		err = ep.extentStore.EcRecordTinyDelete(p.ExtentID, uint64(p.ExtentOffset), uint64(repairSize), ecstorage.TinyDeleting, task.TinyDelNodeIndex)
	}
	e.incDiskErrCnt(p.PartitionID, err, WriteFlag)
}

// Handle OpUpdateEcDataPartition packet.
func (e *EcNode) handleUpdateEcPartition(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("UpdateEcPartition fail. PartitionID(%v) err:%v", p.PartitionID, err)
			p.PackErrorBody(ActionUpdateEcPartition, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	err = ep.updatePartitionLocal(p.Data)
}

// Handle OpChangeEcPartitionMembers packet.
func (e *EcNode) handleChangeMember(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("%v fail. packet:%v data:%v err:%v", ActionChangeMember, p, string(p.Data), err)
			p.PackErrorBody(ActionChangeMember, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	log.LogDebugf("change member PartitionID(%v) data:%v", p.PartitionID, string(p.Data))
	r, err := getChangeEcPartitionMemberRequest(p.Data)
	if err != nil {
		return
	}

	if len(r.Hosts) != int(ep.EcDataNum+ep.EcParityNum) {
		err = errors.NewErrorf("no enough new hosts for change member, err:%v hosts[%v] num[%v]", err, r.Hosts, ep.EcDataNum+ep.EcParityNum)
		return
	}
	err = ep.changeMember(e, r, p.Data)
}

func (e *EcNode) handlePacketToDeleteEcPartition(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDeleteEcPartition, err.Error())
			log.LogErrorf("ActionDeleteEcPartition fail. PartitionID(%v) err: %v",
				p.PartitionID, err)
		} else {
			p.PacketOkReply()
		}
	}()
	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		return
	}
	log.LogDebugf("DeleteEcPartition:%v", string(p.Data))
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	if task.OpCode != proto.OpDeleteEcDataPartition {
		err = fmt.Errorf("from master Task(%v) failed, error unavaliable opcode(%v), expected opcode(%v)",
			task.ToString(), task.OpCode, proto.OpDeleteEcDataPartition)
		return
	}
	err = e.space.ExpiredEcPartition(ep)
	if err != nil {
		err = fmt.Errorf("from master Task(%v) cannot expired Partition err(%v)", task.ToString(), err)
		return
	}
}

// Handle OpCreateEcDataPartition to create new EcPartition
func (e *EcNode) handlePacketToCreateEcPartition(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateEcPartition, err.Error())
			log.LogErrorf("ActionCreateEcPartition fail. err: %v", err)
		} else {
			p.PacketOkReply()
		}
	}()
	log.LogDebugf("CreateEcPartition:%v", string(p.Data))
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	if task.OpCode != proto.OpCreateEcDataPartition {
		err = fmt.Errorf("from master Task(%v) failed, error unavaliable opcode(%v), expected opcode(%v)",
			task.ToString(), task.OpCode, proto.OpCreateEcDataPartition)
		return
	}

	request := &proto.CreateEcPartitionRequest{}
	bytes, err := json.Marshal(task.Request)
	err = json.Unmarshal(bytes, request)
	if err != nil {
		err = fmt.Errorf("from master Task(%v) cannot convert to CreateEcPartition", task.ToString())
		return
	}

	_, err = e.space.CreatePartition(request)
	if err != nil {
		err = fmt.Errorf("from master Task(%v) cannot create Partition err(%v)", task.ToString(), err)
	}
	return
}

// Handle OpHeartbeat packet
func (e *EcNode) handleHeartbeatPacket(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionHeartbeat, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	if err != nil {
		return
	}

	go func() {
		response := &proto.EcNodeHeartbeatResponse{
			Status: proto.TaskSucceeds,
		}
		e.buildHeartbeatResponse(response)
		if task.OpCode == proto.OpEcNodeHeartbeat {
			response.Status = proto.TaskSucceeds
		} else {
			response.Status = proto.TaskFailed
			err = fmt.Errorf("illegal opcode")
			response.Result = err.Error()
		}
		task.Response = *response

		log.LogDebugf(fmt.Sprintf("%v", task))

		err = MasterClient.NodeAPI().ResponseEcNodeTask(task)
		if err != nil {
			log.LogErrorf(err.Error())
			return
		}
	}()
}

// Handle OpCreateExtent packet.
func (e *EcNode) handlePacketToCreateExtent(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateExtent, err.Error())
			log.LogErrorf("ActionCreateExtent fail. PartitionID(%v) ExtentID(%v) err: %v",
				p.PartitionID, p.ExtentID, err)
		} else {
			p.PacketOkReply()
		}
	}()

	ep := p.Object.(*EcPartition)
	if ep.disk.Status == proto.Unavailable {
		err = ecstorage.BrokenDiskError
		return
	}

	extentId := p.ExtentID

	log.LogDebugf("codecnode->ecnode CreateExtent, epid(%v) extentid(%v) reqid(%v)",
		ep.PartitionID, extentId, p.ReqID)
	// master ecnode -> follower ecnode for create extent
	if ep.ExtentStore().HasExtent(extentId) {
		ep.ExtentStore().EcMarkDelete(extentId, ep.NodeIndex, 0, 0)
		ep.deleteOriginExtentSize(extentId)
		ep.computeUsage(true)
	}
	err = ep.extentCreate(extentId, uint64(p.ExtentOffset))
	return
}

// Handle OpWrite packet.
func (e *EcNode) handleWritePacket(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
			log.LogErrorf("ActionWrite fail. PartitionID(%v) ExtentID(%v) err: %v",
				p.PartitionID, p.ExtentID, err)
		} else {
			p.PacketOkReply()
		}
	}()

	ep := p.Object.(*EcPartition)
	if ep.Available() <= 0 || ep.disk.Status == proto.ReadOnly || ep.IsRejectWrite() {
		err = fmt.Errorf("%v, partitionSize(%v) partitionUsed(%v) status(%v)",
			ecstorage.NoSpaceError, ep.PartitionSize, ep.used, ep.disk.Status)
		return
	} else if ep.disk.Status == proto.Unavailable {
		err = ecstorage.BrokenDiskError
		return
	}
	dataCrc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if dataCrc != p.CRC {
		err = fmt.Errorf("data and crc is inconsistent dataCrc(%x) p.Crc(%x) dataSize(%v) size(%v) ", dataCrc, p.CRC, len(p.Data), p.Size)
		return
	}
	err = ep.writeToExtent(p, ecstorage.AppendWriteType)
	e.incDiskErrCnt(p.PartitionID, err, WriteFlag)
}

func (e *EcNode) handleReadPacket(p *repl.Packet, c *net.TCPConn) {
	var (
		err              error
		originExtentSize uint64
		stripeUnitSize   uint64
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRead, err.Error())
			log.LogErrorf("ActionRead fail. PartitionID(%v) ExtentID(%v) err: %v",
				p.PartitionID, p.ExtentID, err)
			err = p.WriteToConn(c, proto.WriteDeadlineTime)
			if err != nil {
				log.LogErrorf("EcRead write to client fail, packet:%v err:%v", p, err)
			}
		}
	}()

	partition := p.Object.(*EcPartition)
	store := partition.extentStore
	//get extentSize
	originExtentSize, err = partition.GetOriginExtentSize(p.ExtentID)
	if err != nil {
		return
	}

	//calc stripeUnitSize
	stripeUnitSize = proto.CalStripeUnitSize(originExtentSize, partition.EcMaxUnitSize, uint64(partition.EcDataNum))

	ecStripe, _ := NewEcStripe(partition, stripeUnitSize, p.ExtentID)
	data := make([]byte, p.Size)
	tpObject := exporter.NewModuleTP(p.GetOpMsg())

	_, crc, err := store.EcRead(p.ExtentID, p.ExtentOffset, int64(p.Size), data, false)
	if !partition.checkIsEofError(err, ecStripe, originExtentSize, uint64(p.ExtentOffset), uint64(p.Size)) {
		err = nil
	}
	partition.checkIsDiskError(err)
	tpObject.Set(err)
	if err != nil {
		err = fmt.Errorf("fail to read extent. ReqID(%v) PartitionID(%v) ExtentId(%v) ExtentOffset[%v] Size[%v] err:%v",
			p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset, p.Size, err)
		data = nil
		return
	}

	p.Data = data
	p.CRC = crc
	p.ResultCode = proto.OpOk
	log.LogDebugf("read extent success, ReqID(%v) PartitionID(%v) ExtentId(%v)", p.ReqID, p.PartitionID, p.ExtentID)
	err = p.WriteToConn(c, proto.WriteDeadlineTime)
	if err != nil {
		err = fmt.Errorf("write reply packet error. ReqID(%v) PartitionID(%v) ExtentId(%v) ExtentOffset[%v] Size[%v] Crc[%v] err:%v",
			p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset, p.Size, p.CRC, err)
		return
	}
	p.PacketOkReply()
}

// Handle OpStreamFollowerRead & OpStreamRead packet.
func (e *EcNode) handleStreamReadPacket(p *repl.Packet, connect net.Conn) {
	var (
		err              error
		originExtentSize uint64
		stripeUnitSize   uint64
		extentOffset     uint64
	)
	defer func() {
		if err != nil {
			log.LogErrorf("StreamRead fail, err:%v", err)
			p.PackErrorBody(ActionStreamRead, err.Error())
			err = p.WriteToConn(connect, proto.WriteDeadlineTime)
			if err != nil {
				log.LogErrorf("StreamEcRead write to client fail, packet:%v err:%v", p, err)
			}
		}
	}()

	ep := p.Object.(*EcPartition)
	//get extentSize
	originExtentSize, err = ep.GetOriginExtentSize(p.ExtentID)
	if err != nil {
		return
	}

	//calc stripeUnitSize
	stripeUnitSize = proto.CalStripeUnitSize(originExtentSize, ep.EcMaxUnitSize, uint64(ep.EcDataNum))
	ecStripe, _ := NewEcStripe(ep, stripeUnitSize, p.ExtentID)
	readSize := uint64(p.Size)
	extentOffset = uint64(p.ExtentOffset)
	if proto.IsTinyExtent(p.ExtentID) {
		extentOffset = ep.getTinyExtentOffset(p.ExtentID, uint64(p.ExtentOffset))
	}
	log.LogDebugf("handleStreamReadPacket packet(%v) hosts(%v) extentOffset(%v) stripeUnitSize(%v) originExtentSize(%v) maxUnitSize(%v) datanum(%v)",
		p, ep.Hosts, extentOffset, stripeUnitSize, originExtentSize, ep.EcMaxUnitSize, ep.EcDataNum)
	for {
		if readSize <= 0 {
			break
		}

		nodeAddr := ecStripe.calcNode(extentOffset, stripeUnitSize)
		stripeUnitFileOffset := ecStripe.calcStripeUnitFileOffset(extentOffset, stripeUnitSize)
		curReadSize := ecStripe.calcCanReadSize(extentOffset, readSize, stripeUnitSize)
		curReadSize = uint64(unit.Min(int(curReadSize), unit.ReadBlockSize))
		data, crc, readErr := ecStripe.readStripeUnitData(nodeAddr, stripeUnitFileOffset, curReadSize, proto.NormalRead)
		log.LogDebugf("StreamRead reply packet: reqId(%v) PartitionID(%v) ExtentID(%v) nodeAddr(%v) ExtentOffset(%v) srtipeUnitFileOffset(%v) "+
			"totalReadSize(%v) curReadSize(%v) actual dataSize(%v) crc(%v) err:%v",
			p.ReqID, p.PartitionID, p.ExtentID, nodeAddr, extentOffset, stripeUnitFileOffset, p.Size, curReadSize, len(data), crc, err)
		if readErr != nil || len(data) == 0 {
			// repair read process
			data, crc, err = repairReadStripeProcess(ecStripe, stripeUnitFileOffset, originExtentSize, curReadSize, nodeAddr)
			if err != nil || len(data) == 0 {
				err = errors.NewErrorf("StreamRead repairReadStripeProcess fail, ReqID(%v) PartitionID(%v) ExtentID(%v) ExtentOffset(%v) nodeAddr(%v) extentFileOffset(%v) curReadSize(%v) dataSize(%v). err:%v",
					p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset, nodeAddr, stripeUnitFileOffset, curReadSize, len(data), err)
				exporter.Warning(err.Error())
				return
			}
			log.LogDebugf("StreamRead repairReadStripeProcess reqId(%v) nodeAddr(%v) srtipeUnitFileOffset(%v) curReadSize(%v) crc(%v) len(data)(%v) err(%v)",
				p.ReqID, nodeAddr, stripeUnitFileOffset, curReadSize, crc, len(data), err)
		}

		reply := NewReadReply(context.Background(), p)
		reply.ExtentOffset = int64(extentOffset)
		reply.Data = data
		reply.Size = uint32(curReadSize)
		reply.CRC = crc
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect, proto.WriteDeadlineTime); err != nil {
			msg := fmt.Sprintf("StreamRead write to client fail. ReqID(%v) PartitionID(%v) ExtentID(%v) ExtentOffset[%v] Size[%v] err:%v",
				reply.ReqID, reply.PartitionID, reply.ExtentID, reply.ExtentOffset, reply.Size, err)
			err = errors.New(msg)
			if reply.Size == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}

		extentOffset += curReadSize
		readSize -= curReadSize
		if reply.Size == unit.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
	}

	p.PacketOkReply()
}

func repairReadStripeProcess(ecStripe *ecStripe, stripeUnitFileOffset, originExtentSize uint64, curReadSize uint64, nodeAddr string) (data []byte, crc uint32, err error) {
	var (
		repairInfo         repairExtentInfo
		stripeUnitFileSize uint64
	)
	ep := ecStripe.ep
	stripeUnitFileSize, err = ep.getExtentStripeUnitFileSize(ecStripe.extentID, originExtentSize, ecStripe.e.localServerAddr)
	if err != nil {
		return
	}
	ep.fillRepairExtentInfo(stripeUnitFileSize, stripeUnitFileOffset, curReadSize, originExtentSize, nodeAddr, &repairInfo)
	repairData, err := ecStripe.repairStripeData(stripeUnitFileOffset, curReadSize, &repairInfo, proto.DegradeRead)
	if err != nil {
		return nil, 0, err
	}

	for i := 0; i < len(ecStripe.hosts); i++ {
		if ecStripe.hosts[i] == nodeAddr && len(repairData[i]) > 0 {
			data = repairData[i]
			crc = crc32.ChecksumIEEE(data)
			return data, crc, nil
		}
	}

	return nil, 0, errors.NewErrorf("repairStripeData is empty")
}

func (e *EcNode) handlePersistTinyExtentDelete(p *repl.Packet) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionPersistTinyExtentDelete, err.Error())
			log.LogErrorf("ActionPersistTinyExtentDelete fail. PartitionID(%v) ExtentID(%v) err: %v",
				p.PartitionID, p.ExtentID, err)
		} else {
			p.PacketOkReply()
		}
	}()

	var holes []*proto.TinyExtentHole
	if err = json.Unmarshal(p.Data, &holes); err != nil {
		return
	}

	ep := p.Object.(*EcPartition)
	ep.originTinyExtentDeleteMapLock.Lock()
	ep.originTinyExtentDeleteMap[p.ExtentID] = holes
	ep.originTinyExtentDeleteMapLock.Unlock()
	err = ep.persistOriginTinyDelInfo(p.ExtentID, holes)
}
