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
	"container/list"
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft"
	"sync/atomic"
)

// Read the packet from a packetProcessor
func (s *DataNode) readPacketFromClient(packetProcessor *PacketProcessor) (err error) {
	defer func() {
		if err != nil {
			packetProcessor.Stop()
		}
	}()
	pkg := NewPacket()
	s.statsFlow(pkg, InFlow)
	if err = pkg.ReadFromConnFromCli(packetProcessor.sourceConn, proto.NoReadDeadlineTime); err != nil {
		return
	}
	log.LogDebugf("action[readPacketFromClient] read packet(%v) from remote(%v).",
		pkg.GetUniqueLogId(), packetProcessor.sourceConn.RemoteAddr().String())
	if pkg.IsMasterCommand() {
		packetProcessor.requestCh <- pkg
		return
	}
	pkg.beforeTp(s.clusterId)

	// Check packet if valid
	if err = s.checkPacket(pkg); err != nil {
		pkg.PackErrorBody("addExtentInfo", err.Error())
		packetProcessor.replyCh <- pkg
		return
	}
	// Check data partition status
	if err = s.checkPartition(pkg); err != nil {
		pkg.PackErrorBody("checkPartition", err.Error())
		packetProcessor.replyCh <- pkg
		return
	}

	// Add extra info in packet
	if err = s.addExtentInfo(pkg); err != nil {
		pkg.PackErrorBody("addExtentInfo", err.Error())
		packetProcessor.replyCh <- pkg
		return
	}
	packetProcessor.requestCh <- pkg

	return
}

// This goroutine be used read pkg from requestCh, then send to all replicates.
// And write pkg response to client.
func (s *DataNode) InteractWithClient(packetProcessor *PacketProcessor) {
	for {
		select {
		case req := <-packetProcessor.requestCh:
			s.processPacket(req, packetProcessor)
		case reply := <-packetProcessor.replyCh:
			s.WriteResponseToClient(reply, packetProcessor)
		case <-packetProcessor.exitC:
			packetProcessor.CleanResource(s)
			return
		}
	}
}

// Receive response from all followers.
func (s *DataNode) receiveReplicatesResponse(packetProcessor *PacketProcessor) {
	for {
		select {
		case <-packetProcessor.handleCh:
			s.receiveFromAllReplicates(packetProcessor)
		case <-packetProcessor.exitC:
			return
		}
	}
}

// Process packet.
// If Op is randomWrite submit to raft, or go to primary-backup replication.
func (s *DataNode) processPacket(req *Packet, packetProcessor *PacketProcessor) {
	if req.Opcode == proto.OpRandomWrite {
		s.randomOpReq(req, packetProcessor)
		return
	}

	s.sequenceOpReq(req, packetProcessor)
	return
}

// Submit random write op to raft instance
func (s *DataNode) randomOpReq(pkg *Packet, packetProcessor *PacketProcessor) {
	var err error
	start := time.Now().UnixNano()
	defer func() {
		if err != nil {
			err = errors.Annotatef(err, "Request[%v] Write Error", pkg.GetUniqueLogId())
			pkg.PackErrorBody(ActionWrite, err.Error())
			logContent := fmt.Errorf("op[%v] error[%v]", pkg.GetOpMsg(), string(pkg.Data))
			log.LogErrorf("action[randomOp] %v", logContent)
		} else {
			logContent := fmt.Sprintf("action[randomOp] op[%v].",
				pkg.LogMessage(pkg.GetOpMsg(), packetProcessor.sourceConn.RemoteAddr().String(), start, nil))
			log.LogWrite(logContent)
			pkg.PackOkReply()
		}

		packetProcessor.replyCh <- pkg
	}()

	_, isLeader := pkg.partition.IsRaftLeader()
	if !isLeader {
		err = storage.ErrNotLeader
		return
	}
	if pkg.partition.Status() == proto.ReadOnly {
		err = storage.ErrorPartitionReadOnly
		return
	}
	if pkg.partition.Available() <= 0 {
		err = storage.ErrSyscallNoSpace
		return
	}

	err = pkg.partition.RandomWriteSubmit(pkg)
	if err != nil && strings.Contains(err.Error(), raft.ErrNotLeader.Error()) {
		err = storage.ErrNotLeader
		return
	}

	if err == nil && pkg.Opcode == proto.OpRandomWrite && pkg.Size == util.BlockSize {
		proto.Buffers.Put(pkg.Data)
	}

	return
}

// If pkg is sequence Op,then send pkg to all replicates,and do local
func (s *DataNode) sequenceOpReq(req *Packet, packetProcessor *PacketProcessor) {
	var err error
	if !req.isForwardPacket() {
		s.operatePacket(req, packetProcessor.sourceConn)
		if !(req.Opcode == proto.OpStreamRead || req.Opcode == proto.OpExtentRepairRead) {
			packetProcessor.replyCh <- req
		}

		return
	}
	if _, err = s.sendToAllReplicates(req, packetProcessor); err == nil {
		s.operatePacket(req, packetProcessor.sourceConn)
	}
	packetProcessor.handleCh <- struct{}{}

	return
}

// Write response to client and recycle the connect.
func (s *DataNode) WriteResponseToClient(reply *Packet, packetProcessor *PacketProcessor) {
	var err error
	if reply.isErrPack() {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToCli, packetProcessor.sourceConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		reply.forceDestoryAllConnect()
		log.LogErrorf("action[WriteResponseToClient] %v", err)
	}
	s.cleanupPkg(reply)

	if err = reply.WriteToConn(packetProcessor.sourceConn); err != nil {
		err = fmt.Errorf(reply.LogMessage(ActionWriteToCli, packetProcessor.sourceConn.RemoteAddr().String(),
			reply.StartT, err))
		log.LogErrorf("action[WriteResponseToClient] %v", err)
		reply.forceDestoryAllConnect()
		packetProcessor.Stop()
	}
	if !reply.IsMasterCommand() {
		s.addMetrics(reply)
		log.LogDebugf("action[WriteResponseToClient] %v", reply.LogMessage(ActionWriteToCli,
			packetProcessor.sourceConn.RemoteAddr().String(), reply.StartT, err))
		s.statsFlow(reply, OutFlow)
	}

}

// The head node release tinyExtent to store
func (s *DataNode) cleanupPkg(pkg *Packet) {
	if !pkg.isLeaderPacket() {
		return
	}
	s.leaderPutTinyExtentToStore(pkg)
	if pkg.StoreMode == proto.TinyExtentMode && pkg.isWriteOperation() {
		pkg.PutConnectsToPool()
	}
}

func (s *DataNode) addMetrics(reply *Packet) {
	reply.afterTp()
	latency := time.Since(reply.tpObject.StartTime)
	if reply.partition == nil {
		return
	}
	if reply.isWriteOperation() {
		reply.partition.AddWriteMetrics(uint64(latency))
	} else if reply.isReadOperation() {
		reply.partition.AddReadMetrics(uint64(latency))
	}
}

// Receive response from all members
func (s *DataNode) receiveFromAllReplicates(packetProcessor *PacketProcessor) (request *Packet) {
	var (
		e *list.Element
	)

	if e = packetProcessor.GetFrontPacket(); e == nil {
		return
	}
	request = e.Value.(*Packet)
	defer func() {
		packetProcessor.DelPacketFromList(request)
	}()
	for index := 0; index < len(request.replicateAddrs); index++ {
		_, err := s.receiveFromReplicate(request, index)
		if err != nil {
			request.PackErrorBody(ActionReceiveFromNext, err.Error())
			request.forceDestoryAllConnect()
			return
		}
	}
	request.PackOkReply()
	return
}

// Receive pkg response from one member*/
func (s *DataNode) receiveFromReplicate(request *Packet, index int) (reply *Packet, err error) {
	if request.replicateConns[index] == nil {
		err = errors.Annotatef(fmt.Errorf(ConnIsNullErr), "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
		return
	}

	// Check local execution result.
	if request.isErrPack() {
		err = errors.Annotatef(fmt.Errorf(request.getErrMessage()), "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
		log.LogErrorf("action[receiveFromReplicate] %v.",
			request.LogMessage(ActionReceiveFromNext, LocalProcessAddr, request.StartT, fmt.Errorf(request.getErrMessage())))
		return
	}

	reply = NewPacket()

	if err = reply.ReadFromConn(request.replicateConns[index], proto.ReadDeadlineTime); err != nil {
		err = errors.Annotatef(err, "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
		log.LogErrorf("action[receiveFromReplicate] %v.", request.LogMessage(ActionReceiveFromNext, request.replicateAddrs[index], request.StartT, err))
		return
	}

	if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
		reply.ExtentOffset != request.ExtentOffset || reply.CRC != request.CRC || reply.ExtentID != request.ExtentID {
		err = fmt.Errorf(ActionCheckReplyAvail+" request (%v) reply(%v) %v from localAddr(%v)"+
			" remoteAddr(%v) requestCrc(%v) replyCrc(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), request.replicateAddrs[index],
			request.replicateConns[index].LocalAddr().String(), request.replicateConns[index].RemoteAddr().String(), request.CRC, reply.CRC)
		log.LogErrorf("action[receiveFromReplicate] %v.", err.Error())
		return
	}

	if reply.isErrPack() {
		err = fmt.Errorf(ActionReceiveFromNext+"remote (%v) do failed(%v)",
			request.replicateAddrs[index], string(reply.Data[:reply.Size]))
		err = errors.Annotatef(err, "Request(%v) receiveFromReplicate Error", request.GetUniqueLogId())
		return
	}

	log.LogDebugf("action[receiveFromReplicate] %v.", reply.LogMessage(ActionReceiveFromNext, request.replicateAddrs[index], request.StartT, err))
	return
}

func (s *DataNode) sendToAllReplicates(pkg *Packet, packetProcessor *PacketProcessor) (index int, err error) {
	packetProcessor.PushPacketToList(pkg)
	for index = 0; index < len(pkg.replicateConns); index++ {
		err = packetProcessor.AllocateReplicatConnects(pkg, index)
		if err != nil {
			msg := fmt.Sprintf("pkg inconnect(%v) to(%v) err(%v)", packetProcessor.sourceConn.RemoteAddr().String(),
				pkg.replicateAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendToAllReplicates Error", pkg.GetUniqueLogId())
			pkg.PackErrorBody(ActionSendToNext, err.Error())
			return
		}
		nodes := pkg.RemainReplicates
		pkg.RemainReplicates = 0
		if err == nil {
			err = pkg.WriteToConn(pkg.replicateConns[index])
		}
		pkg.RemainReplicates = nodes
		if err != nil {
			msg := fmt.Sprintf("pkg inconnect(%v) to(%v) err(%v)", packetProcessor.sourceConn.RemoteAddr().String(),
				pkg.replicateAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendToAllReplicates Error", pkg.GetUniqueLogId())
			pkg.PackErrorBody(ActionSendToNext, err.Error())
			return
		}
	}

	return
}

func (s *DataNode) statsFlow(pkg *Packet, flag int) {
	stat := s.space.Stats()
	if pkg == nil {
		return
	}
	if flag == OutFlow {
		stat.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
		return
	}

	if pkg.isReadOperation() {
		stat.AddInDataSize(uint64(pkg.Arglen))
	} else {
		stat.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
	}

}

func (s *DataNode) leaderPutTinyExtentToStore(pkg *Packet) {
	if pkg == nil || !storage.IsTinyExtent(pkg.ExtentID) || pkg.ExtentID <= 0 || atomic.LoadInt32(&pkg.isRelaseTinyExtentToStore) == HasReturnToStore {
		return
	}
	if pkg.StoreMode != proto.TinyExtentMode || !pkg.isLeaderPacket() || !pkg.isWriteOperation() || !pkg.isForwardPacket() {
		return
	}
	store := pkg.partition.GetStore()
	if pkg.isErrPack() {
		store.PutTinyExtentToUnavaliCh(pkg.ExtentID)
	} else {
		store.PutTinyExtentToAvaliCh(pkg.ExtentID)
	}
	atomic.StoreInt32(&pkg.isRelaseTinyExtentToStore, HasReturnToStore)
}
