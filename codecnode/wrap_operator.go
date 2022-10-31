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

package codecnode

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	echandler "github.com/chubaofs/chubaofs/util/ec"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

func (s *CodecServer) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
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
	case proto.OpCodecNodeHeartbeat:
		s.handleHeartbeatPacket(p)
	case proto.OpIssueMigrationTask:
		s.handleEcMigrationTask(p)
	case proto.OpStopMigratingByDatapartitionTask:
		s.handleStopMigratingByDataPartitionTask(p)
	case proto.OpStopMigratingByNodeTask:
		s.handleStopMigratingByNodeTask(p)
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}

	return
}

func (s *CodecServer) handleHeartbeatPacket(p *repl.Packet) {
	var err error
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody("ActionHeartbeat", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}

	go func() {
		request := &proto.HeartBeatRequest{}
		response := &proto.CodecNodeHeartbeatResponse{}
		s.buildHeartBeatResponse(response)
		if task.OpCode == proto.OpCodecNodeHeartbeat {
			marshaled, _ := json.Marshal(task.Request)
			_ = json.Unmarshal(marshaled, request)
			response.Status = proto.TaskSucceeds
		} else {
			response.Status = proto.TaskFailed
			err = fmt.Errorf("illegal opcode")
			response.Result = err.Error()
		}
		task.Response = response
		if err = MasterClient.NodeAPI().ResponseCodecNodeTask(task); err != nil {
			err = errors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
			log.LogErrorf(err.Error())
			return
		}
	}()

}

func (s *CodecServer) buildHeartBeatResponse(response *proto.CodecNodeHeartbeatResponse) {
	response.Status = proto.TaskSucceeds
	response.Version = codecNodeLastVersion
}

func (s *CodecServer) sendStatusToMaster(partitionID uint64, currentExtentID uint64, status uint8, errMsg string) (err error) {
	task := &proto.AdminTask{
		OpCode:       proto.OpIssueMigrationTask,
		OperatorAddr: s.localServerAddr,
	}
	resp := &proto.CodecNodeMigrationResponse{
		Status:          status,
		PartitionId:     partitionID,
		CurrentExtentID: currentExtentID,
		Result:          errMsg,
	}
	task.Response = resp
	return MasterClient.NodeAPI().ResponseCodecNodeTask(task)
}

func (s *CodecServer) handleEcMigrationTask(p *repl.Packet) {
	var (
		err         error
		errMsg      = ""
		ecp         *EcPartition
		ech         *echandler.EcHandler
		req         = &proto.IssueMigrationTaskRequest{}
		adminTask   = &proto.AdminTask{Request: req}
		curExtentID = uint64(0)
	)

	err = json.Unmarshal(p.Data, adminTask)
	defer func() {
		if err != nil {
			p.PackErrorBody("ActionEcMigrationTask", err.Error())
			log.LogErrorf(err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	if err != nil {
		return
	}
	s.migrateStatus.Delete(req.PartitionId)
	log.LogDebugf("ActionEcMigrationTask req(%v)", req)
	go func() {
		defer func() {
			status := proto.TaskSucceeds
			if err != nil {
				status = proto.TaskFailed
				err = errors.Trace(err, "MigrationTask failed partition(%v).", req.PartitionId)
				log.LogErrorf(err.Error())
				errMsg = err.Error()
			}
			log.LogDebugf("send master migrate done partition(%v)", req.PartitionId)
			if err = s.sendStatusToMaster(req.PartitionId, curExtentID, uint8(status), errMsg); err != nil {
				err = errors.Trace(err, "MigrationTask to master(%v) failed.", s.masters)
				log.LogErrorf(err.Error())
			}
		}()

		if ecp, err = NewEcPartition(req, s.masters); err != nil {
			return
		}

		if len(ecp.Hosts) != int(ecp.EcParityNum+ecp.EcDataNum) {
			err = errors.NewErrorf("ecp Hosts num is not equal replicaNum")
			return
		}

		if err = ecp.GetOriginExtentInfo(); err != nil {
			return
		}

		for _, ei := range ecp.originExtentInfo {
			curExtentID = ei.FileID
			if curExtentID < req.CurrentExtentID {
				continue
			}
			if curExtentID%ReportToMasterInterval == 0 {
				if err = s.sendStatusToMaster(req.PartitionId, curExtentID, proto.TaskRunning, errMsg); err != nil {
					log.LogErrorf("migrateTask send status to master fail: %v", err.Error())
				}
			}
			holes := make([]*proto.TinyExtentHole, 0)
			if proto.IsTinyExtent(curExtentID) {
				if holes, ei.Size, err = ecp.GetTinyExtentHolesAndAvaliSize(curExtentID); err != nil {
					return
				}
				log.LogDebugf("PartitionID(%v) CurExtentID(%v) ei.Size(%v) ", ecp.PartitionId, curExtentID, ei.Size)
				if err = ecp.PersistHolesInfoToEcnode(p.Ctx(), curExtentID, holes); err != nil {
					return
				}
			}
			if ei.Size <= 0 {
				continue
			}
			if err = ecp.CreateExtentForWrite(p.Ctx(), curExtentID, ei.Size); err != nil {
				return
			}

			stripeUnitSize := proto.CalStripeUnitSize(ei.Size, ecp.EcMaxUnitSize, uint64(ecp.EcDataNum))
			stripeSize := stripeUnitSize * uint64(ecp.EcDataNum)

			if ech, err = echandler.NewEcHandler(int(stripeUnitSize), int(ecp.EcDataNum), int(ecp.EcParityNum)); err != nil {
				return
			}

			var (
				offset   uint64
				readSize int
			)
			for {
				if s.IsStopMigrate(ecp.PartitionId) {
					log.LogInfof("MigrationTask stop partitionId(%v)", ecp.PartitionId)
					err = fmt.Errorf("MigrationTask stop")
					return
				}
				if offset >= ei.Size {
					break
				}
				inbuf := make([]byte, stripeSize)
				needReadSize := stripeSize
				if offset+stripeSize >= ei.Size {
					needReadSize = ei.Size - offset
				}
				log.LogDebugf("PartitionID(%v) CurExtentID(%v) needReadSize(%v) ei.Size(%v) stripeSize(%v)", ecp.PartitionId, curExtentID, needReadSize, ei.Size, stripeSize)
				if readSize, err = ecp.Read(p.Ctx(), curExtentID, inbuf, offset, int(needReadSize)); err != nil {
					return
				}

				var shards [][]byte
				if shards, err = ech.Encode(inbuf); err != nil {
					return
				}

				if uint64(readSize) != stripeSize {
					if err = ecp.CalMisAlignMentShards(shards, uint64(readSize), stripeUnitSize); err != nil {
						return
					}
				}

				if err = ecp.Write(p.Ctx(), shards, curExtentID, (offset/stripeSize)*stripeUnitSize); err != nil {
					return
				}
				offset += uint64(readSize)
			}
			log.LogDebugf("Migration PartitionId(%v) ExtentId(%v) success", ecp.PartitionId, curExtentID)
		}

	}()
}

func (s *CodecServer) handleStopMigratingByDataPartitionTask(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody("StopMigratingByDataPartitionTask", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	task := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, task); err != nil {
		return
	}
	partitionId := task.PartitionID
	log.LogInfof("StopMigratingByDataPartition PartitionID(%v)", partitionId)
	s.migrateStatus.Store(partitionId, true)
}

func (s *CodecServer) IsStopMigrate(partitionId uint64) bool {
	if val, exist := s.migrateStatus.Load(partitionId); exist {
		isNeedStop, ok := val.(bool)
		if !ok || !isNeedStop {
			return false
		}
		return true
	}
	return false
}

func (s *CodecServer) handleStopMigratingByNodeTask(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody("handleStopMigratingByNodeTask", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partitions := &[]uint64{}
	task := &proto.AdminTask{Request: partitions}
	if err = json.Unmarshal(p.Data, task); err != nil {
		return
	}

	log.LogInfof("StopMigratingByNode partitions(%v)", partitions)
	for _, partitionId := range *partitions {
		s.migrateStatus.Store(partitionId, true)
	}
}
