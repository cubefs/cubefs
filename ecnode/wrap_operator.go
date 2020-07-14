// Copyright 2020 The Chubao Authors.
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

package ecnode

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
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
)

func (e *EcNode) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
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
	case proto.OpEcNodeHeartbeat:
		e.handleHeartbeatPacket(p)
	case proto.OpCreateExtent:
		e.handlePacketToCreateExtent(p)
	case proto.OpWrite:
		e.handleWritePacket(p)
	case proto.OpRead:
		e.handleReadPacket(p, c)
	case proto.OpStreamRead:
		e.handleStreamReadPacket(p, c)
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}
	return
}

// Handle OpCreateEcDataPartition to create new EcPartition
func (e *EcNode) handlePacketToCreateEcPartition(p *repl.Packet) {
	var (
		err error
		ep  *EcPartition
	)

	log.LogDebugf("ActionRecievePacketToCreateEcPartition")

	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		log.LogErrorf("cannnot unmashal adminTask")
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	if task.OpCode != proto.OpCreateEcDataPartition {
		log.LogErrorf("error unavaliable opcode")
		err = fmt.Errorf("from master Task(%v) failed, error unavaliable opcode(%v), expected opcode(%v)",
			task.ToString(), task.OpCode, proto.OpCreateEcDataPartition)
		return
	}

	request := &proto.CreateEcPartitionRequest{}
	bytes, err := json.Marshal(task.Request)
	err = json.Unmarshal(bytes, request)
	if err != nil {
		log.LogErrorf("cannot convert to CreateEcPartition")
		err = fmt.Errorf("from master Task(%v) cannot convert to CreateEcPartition", task.ToString())
		return
	}

	ep, err = e.space.CreatePartition(request)
	if err != nil {
		log.LogErrorf("cannot create Partition err(%v)", err)
		err = fmt.Errorf("from master Task(%v) cannot create Partition err(%v)", task.ToString(), err)
		return
	}
	p.PacketOkWithBody([]byte(ep.Disk().Path))

	return
}

// Handle OpHeartbeat packet
func (e *EcNode) handleHeartbeatPacket(p *repl.Packet) {
	log.LogDebugf("ActionRecieveEcHeartbeat")

	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)

	defer func() {
		if err != nil {
			p.PackErrorBody("ActionCreateEcPartition", err.Error())
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
	log.LogDebugf("ActionCreateExtent")

	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody("ActionCreateExtent", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*EcPartition)
	if partition.Available() <= 0 || partition.disk.Status == proto.ReadOnly || partition.IsRejectWrite() {
		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}
	err = partition.ExtentStore().Create(p.ExtentID)

	return
}

func (e *EcNode) handleWritePacket(p *repl.Packet) {
	log.LogDebugf("ActionWrite")

	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody("ActionWrite", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	partition := p.Object.(*EcPartition)
	if partition.Available() <= 0 || partition.disk.Status == proto.ReadOnly || partition.IsRejectWrite() {

		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}

	store := partition.ExtentStore()

	// we only allow write by one stripe unit
	if uint32(p.Size) != partition.stripeUnitSize {
		err = errors.New("invalid EC(erasure code) strip unit size")
		return
	}

	if p.Size <= util.BlockSize {
		err = store.Write(p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data, p.CRC, storage.AppendWriteType, p.IsSyncWrite())
		partition.checkIsDiskError(err)
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
			err = store.Write(p.ExtentID, p.ExtentOffset+int64(offset), int64(currSize), data, crc,
				storage.AppendWriteType, p.IsSyncWrite())
			partition.checkIsDiskError(err)
			if err != nil {
				break
			}
			size -= uint32(currSize)
			offset += currSize
		}
	}
	e.incDiskErrCnt(p.PartitionID, err, WriteFlag)
	return
}

func (e *EcNode) handleStreamReadPacket(p *repl.Packet, connect net.Conn) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody("ActionStreamRead", err.Error())
			p.WriteToConn(connect)
		}
	}()

	ep := p.Object.(*EcPartition)
	needReplySize := p.Size
	// stripeSize is just data size, not contains parity
	stripeIndex := p.ExtentOffset % int64(ep.stripeSize)
	curOffset := stripeIndex * int64(ep.stripeUnitSize)

	for {
		if needReplySize == 0 {
			break
		}

		currReadSize := uint32(util.Min(int(needReplySize), util.ReadBlockSize))
		p.Data, err = ep.readFromEcNode(p.PartitionID, p.ExtentID, curOffset, currReadSize, stripeIndex)
		if err != nil {
			return
		}

		p.CRC = crc32.ChecksumIEEE(p.Data)
		p.ResultCode = proto.OpOk
		if err = p.WriteToConn(connect); err != nil {
			return
		}

		needReplySize -= currReadSize
		curOffset += int64(currReadSize) * int64(ep.stripeUnitSize)
		logContent := fmt.Sprintf("action[operatePacket] %v.", p.LogMessage(p.GetOpMsg(), connect.RemoteAddr().String(), p.StartT, err))
		log.LogReadf(logContent)
	}
}

func (e *EcNode) handleReadPacket(p *repl.Packet, conn *net.TCPConn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRead, err.Error())
			p.WriteToConn(conn)
		}
	}()

	partition := p.Object.(*EcPartition)
	needReplySize := p.Size
	offset := p.ExtentOffset
	store := partition.extentStore

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
		reply.CRC, err = store.Read(reply.ExtentID, offset, int64(currReadSize), reply.Data, false)
		partition.checkIsDiskError(err)
		tpObject.Set(err)
		p.CRC = reply.CRC
		if err != nil {
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		reply.Opcode = p.Opcode
		p.ResultCode = proto.OpOk
		if err = reply.WriteToConn(conn); err != nil {
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		if currReadSize == util.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), conn.RemoteAddr().String(), reply.StartT, err))
		log.LogReadf(logContent)
	}
	p.PacketOkReply()

	return
}
