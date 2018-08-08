// Copyright 2018 The ChuBao Authors.
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
	"time"

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/storage"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
)

func (s *DataNode) readFromCliAndDeal(msgH *MessageHandler) (err error) {
	defer func() {
		if err != nil {
			msgH.Stop()
		}
	}()
	pkg := NewPacket()
	s.statsFlow(pkg, InFlow)
	if err = pkg.ReadFromConnFromCli(msgH.inConn, proto.NoReadDeadlineTime); err != nil {
		return
	}
	log.LogDebugf("action[readFromCliAndDeal] read packet[%v] from remote[%v].",
		pkg.GetUniqueLogId(), msgH.inConn.RemoteAddr().String())
	if pkg.IsMasterCommand() {
		msgH.requestCh <- pkg
		return
	}
	pkg.beforeTp(s.clusterId)

	if err = s.checkPacket(pkg); err != nil {
		pkg.PackErrorBody("checkPacket", err.Error())
		msgH.replyCh <- pkg
		return
	}
	if err = s.checkAction(pkg); err != nil {
		pkg.PackErrorBody("checkAction", err.Error())
		msgH.replyCh <- pkg
		return
	}
	if err = s.checkAndAddInfo(pkg); err != nil {
		pkg.PackErrorBody("checkAndAddInfo", err.Error())
		msgH.replyCh <- pkg
		return
	}
	msgH.requestCh <- pkg

	return
}

func (s *DataNode) checkAndAddInfo(pkg *Packet) error {
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		return s.handleChunkInfo(pkg)
	case proto.ExtentStoreMode:
		if pkg.isHeadNode() && pkg.Opcode == proto.OpCreateFile {
			pkg.FileID = pkg.DataPartition.GetExtentStore().NextExtentId()
		}
	}

	return nil
}

func (s *DataNode) handleRequest(msgH *MessageHandler) {
	for {
		select {
		case <-msgH.handleCh:
			pkg, exit := s.receiveFromNext(msgH)
			s.headNodePutChunk(pkg)
			if exit {
				msgH.Stop()
				return
			}
		case <-msgH.exitC:
			return
		}
	}
}

func (s *DataNode) doRequestCh(req *Packet, msgH *MessageHandler) {
	var err error
	if !req.IsTransitPkg() {
		s.operatePacket(req, msgH.inConn)
		if !(req.Opcode == proto.OpStreamRead) {
			msgH.replyCh <- req
		}

		return
	}

	if err = s.sendToNext(req, msgH); err == nil {
		s.operatePacket(req, msgH.inConn)
	} else {
		log.LogErrorf("action[doRequestCh] %dp.", req.ActionMsg(ActionSendToNext, req.NextAddr,
			req.StartT, fmt.Errorf("failed to send to : %v", req.NextAddr)))
		if req.IsMarkDeleteReq() {
			s.operatePacket(req, msgH.inConn)
		}
	}
	msgH.handleCh <- single

	return
}

func (s *DataNode) doReplyCh(reply *Packet, msgH *MessageHandler) {
	var err error
	if reply.IsErrPack() {
		err = fmt.Errorf(reply.ActionMsg(ActionWriteToCli, msgH.inConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		log.LogErrorf("action[doReplyCh] %v", err)
	}

	if err = reply.WriteToConn(msgH.inConn); err != nil {
		err = fmt.Errorf(reply.ActionMsg(ActionWriteToCli, msgH.inConn.RemoteAddr().String(),
			reply.StartT, err))
		log.LogErrorf("action[doReplyCh] %v", err)
		msgH.Stop()
	}
	if !reply.IsMasterCommand() {
		reply.afterTp()
		log.LogDebugf("action[doReplyCh] %v", reply.ActionMsg(ActionWriteToCli,
			msgH.inConn.RemoteAddr().String(), reply.StartT, err))
		s.statsFlow(reply, OutFlow)
	}
}

func (s *DataNode) writeToCli(msgH *MessageHandler) {
	for {
		select {
		case req := <-msgH.requestCh:
			s.doRequestCh(req, msgH)
		case reply := <-msgH.replyCh:
			s.doReplyCh(reply, msgH)
		case <-msgH.exitC:
			msgH.ClearReqs(s)
			return
		}
	}
}

func (s *DataNode) receiveFromNext(msgH *MessageHandler) (request *Packet, exit bool) {
	var (
		err   error
		e     *list.Element
		reply *Packet
	)
	if e = msgH.GetListElement(); e == nil {
		return
	}

	request = e.Value.(*Packet)

	defer func() {
		s.statsFlow(request, OutFlow)
		s.statsFlow(reply, InFlow)
		if err != nil {
			request.PackErrorBody(ActionReceiveFromNext, err.Error())
			msgH.DelListElement(request, e, true)
		} else {
			request.PackOkReply()
			msgH.DelListElement(request, e, false)
		}
	}()

	if request.NextConn == nil {
		err = errors.Annotatef(fmt.Errorf(ConnIsNullErr), "Request[%v] receiveFromNext Error", request.GetUniqueLogId())
		return
	}

	// Check local execution result.
	if request.IsErrPack() {
		err = errors.Annotatef(fmt.Errorf(request.getErr()), "Request[%v] receiveFromNext Error", request.GetUniqueLogId())
		log.LogErrorf("action[receiveFromNext] %v.",
			request.ActionMsg(ActionReceiveFromNext, LocalProcessAddr, request.StartT, fmt.Errorf(request.getErr())))
		return
	}

	reply = NewPacket()

	if err = reply.ReadFromConn(request.NextConn, proto.ReadDeadlineTime); err != nil {
		err = errors.Annotatef(err, "Request[%v] receiveFromNext Error", request.GetUniqueLogId())
		log.LogErrorf("action[receiveFromNext] %v.", request.ActionMsg(ActionReceiveFromNext, request.NextAddr, request.StartT, err))
		return
	}

	if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
		reply.Offset != request.Offset || reply.Crc != request.Crc || reply.FileID != request.FileID {
		err = fmt.Errorf(ActionCheckReplyAvail+" request [%v] reply[%v] %v from localAddr[%v]"+
			" remoteAddr[%v] requestCrc[%v] replyCrc[%v]", request.GetUniqueLogId(), reply.GetUniqueLogId(), request.NextAddr,
			request.NextConn.LocalAddr().String(), request.NextConn.RemoteAddr().String(), request.Crc, reply.Crc)
		log.LogErrorf("action[receiveFromNext] %v.", err.Error())
		return
	}

	if reply.IsErrPack() {
		err = fmt.Errorf(ActionReceiveFromNext+"remote [%v] do failed[%v]",
			request.NextAddr, string(reply.Data[:reply.Size]))
		err = errors.Annotatef(err, "Request[%v] receiveFromNext Error", request.GetUniqueLogId())
		request.CopyFrom(reply)
		return
	}

	log.LogDebugf("action[receiveFromNext] %v.", reply.ActionMsg(ActionReceiveFromNext, request.NextAddr, request.StartT, err))
	return
}

func (s *DataNode) sendToNext(pkg *Packet, msgH *MessageHandler) error {
	var (
		err error
	)
	msgH.PushListElement(pkg)
	err = msgH.AllocateNextConn(pkg)
	if err != nil {
		return err
	}
	pkg.Nodes--
	if err == nil {
		err = pkg.WriteToConn(pkg.NextConn)
	}
	pkg.Nodes++
	if err != nil {
		msg := fmt.Sprintf("pkg inconnect[%v] to[%v] err[%v]", msgH.inConn.RemoteAddr().String(), pkg.NextAddr, err.Error())
		err = errors.Annotatef(fmt.Errorf(msg), "Request[%v] sendToNext Error", pkg.GetUniqueLogId())
		pkg.PackErrorBody(ActionSendToNext, err.Error())
	}

	return err
}

func (s *DataNode) checkStoreMode(p *Packet) (err error) {
	if p.StoreMode == proto.TinyStoreMode || p.StoreMode == proto.ExtentStoreMode {
		return nil
	}
	return ErrStoreTypeMismatch
}

func (s *DataNode) checkPacket(pkg *Packet) error {
	var err error
	pkg.StartT = time.Now().UnixNano()
	if err = s.checkStoreMode(pkg); err != nil {
		return err
	}

	if err = pkg.CheckCrc(); err != nil {
		return err
	}
	var addrs []string
	if addrs, err = pkg.UnmarshalAddrs(); err == nil {
		err = pkg.GetNextAddr(addrs)
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *DataNode) checkAction(pkg *Packet) (err error) {
	dp := s.space.GetPartition(pkg.PartitionID)
	if dp == nil {
		err = errors.Errorf("partition %v is not exist", pkg.PartitionID)
		return
	}
	pkg.DataPartition = dp
	if pkg.Opcode == proto.OpWrite || pkg.Opcode == proto.OpCreateFile {
		if pkg.DataPartition.Status() == proto.ReadOnly {
			err = storage.ErrorPartitionReadOnly
			return
		}
		if pkg.DataPartition.Available() <= 0 {
			err = storage.ErrSyscallNoSpace
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

	if pkg.IsReadOperation() {
		stat.AddInDataSize(uint64(pkg.Arglen))
	} else {
		stat.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
	}

}
