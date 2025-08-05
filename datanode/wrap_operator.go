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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/strutil"
)

var ErrForbiddenDataPartition = errors.New("the data partition is forbidden")

func (s *DataNode) getPacketTpLabels(p *repl.Packet) map[string]string {
	labels := make(map[string]string)
	labels[exporter.Vol] = ""
	labels[exporter.Op] = ""
	labels[exporter.PartId] = ""
	labels[exporter.Disk] = ""

	if part, ok := p.Object.(*DataPartition); ok {
		labels[exporter.Vol] = part.volumeID
		labels[exporter.Op] = p.GetOpMsg()
		if exporter.EnablePid {
			labels[exporter.PartId] = fmt.Sprintf("%d", part.partitionID)
			labels[exporter.Disk] = part.path
		}
	}

	return labels
}

func isColdVolExtentDelErr(p *repl.Packet) bool {
	if p.Object == nil {
		return false
	}

	partition, ok := p.Object.(*DataPartition)
	if !ok {
		return false
	}

	if proto.IsNormalDp(partition.partitionType) {
		return false
	}

	if p.ResultCode == proto.OpNotExistErr {
		return true
	}

	return false
}

func (s *DataNode) OperatePacket(p *repl.Packet, c net.Conn) (err error) {
	var (
		tpLabels map[string]string
		tpObject *exporter.TimePointCount
	)
	if log.EnableDebug() {
		log.LogDebugf("action[OperatePacket] %v, pack [%v]", p.GetOpMsg(), p)
	}
	shallDegrade := p.ShallDegrade()
	sz := p.Size
	if !shallDegrade {
		tpObject = exporter.NewTPCnt(p.GetOpMsg())
		tpLabels = s.getPacketTpLabels(p)
	}
	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op(%v) error(%v)", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			if p.IsWriteOpOfPacketProtoVerForbidden() {
				log.LogWarnf(logContent)
			} else if isColdVolExtentDelErr(p) {
				log.LogInfof(logContent)
			} else {
				log.LogErrorf(logContent)
			}
		} else if log.EnableInfo() {
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch p.Opcode {
			case proto.OpStreamRead, proto.OpRead, proto.OpExtentRepairRead, proto.OpStreamFollowerRead, proto.OpBackupRead:
			case proto.OpReadTinyDeleteRecord:
				log.LogRead(logContent)
			case proto.OpWrite,
				proto.OpRandomWrite,
				proto.OpRandomWriteVer,
				proto.OpSyncRandomWriteVer,
				proto.OpRandomWriteAppend,
				proto.OpSyncRandomWriteAppend,
				proto.OpTryWriteAppend,
				proto.OpSyncTryWriteAppend,
				proto.OpSyncRandomWrite,
				proto.OpSyncWrite,
				proto.OpMarkDelete:
				log.LogWrite(logContent)
			default:
				log.LogInfo(logContent)
			}
		}
		p.Size = resultSize
		if !shallDegrade {
			tpObject.SetWithLabels(err, tpLabels)
		}

		if p.IsReadOperation() {
			now := time.Now().UnixNano()
			exporter.RecodCost("data_read_cost", (now-start)/1e3)
		}
	}()

	switch p.Opcode {
	case proto.OpCreateExtent:
		s.handlePacketToCreateExtent(p)
	case proto.OpWrite,
		proto.OpSyncWrite,
		proto.OpBackupWrite:
		s.handleWritePacket(p)
	case proto.OpStreamRead, proto.OpBackupRead:
		s.handleStreamReadPacket(p, c, StreamRead)
	case proto.OpStreamFollowerRead:
		s.extentRepairReadPacket(p, c, StreamRead)
	case proto.OpExtentRepairRead:
		s.handleExtentRepairReadPacket(p, c, RepairRead)
	case proto.OpTinyExtentRepairRead:
		s.handleTinyExtentRepairReadPacket(p, c)
	case proto.OpSnapshotExtentRepairRead:
		s.handleSnapshotExtentRepairReadPacket(p, c)
	case proto.OpMarkDelete, proto.OpSplitMarkDelete:
		s.handleMarkDeletePacket(p, c)
	case proto.OpBatchDeleteExtent, proto.OpGcBatchDeleteExtent:
		s.handleBatchMarkDeletePacket(p, c)
	case proto.OpRandomWrite,
		proto.OpSyncRandomWrite,
		proto.OpRandomWriteAppend,
		proto.OpSyncRandomWriteAppend,
		proto.OpTryWriteAppend,
		proto.OpSyncTryWriteAppend,
		proto.OpRandomWriteVer,
		proto.OpSyncRandomWriteVer:
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
	case proto.OpAddDataPartitionRaftMember:
		s.handlePacketToAddDataPartitionRaftMember(p)
	case proto.OpRemoveDataPartitionRaftMember:
		s.handlePacketToRemoveDataPartitionRaftMember(p)
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
	case proto.OpBatchLockNormalExtent:
		s.handleBatchLockNormalExtent(p, c)
	case proto.OpBatchUnlockNormalExtent:
		s.handleBatchUnlockNormalExtent(p, c)
	case proto.OpVersionOperation:
		s.handleUpdateVerPacket(p)
	case proto.OpStopDataPartitionRepair:
		s.handlePacketToStopDataPartitionRepair(p)
	case proto.OpRecoverDataReplicaMeta:
		s.handlePacketToRecoverDataReplicaMeta(p)
	case proto.OpRecoverBackupDataReplica:
		s.handlePacketToRecoverBackupDataReplica(p)
	case proto.OpRecoverBadDisk:
		s.handlePacketToRecoverBadDisk(p)
	case proto.OpQueryBadDiskRecoverProgress:
		s.handlePacketToQueryBadDiskRecoverProgress(p)
	case proto.OpDeleteBackupDirectories:
		s.handlePacketToOpDeleteBackupDirectories(p)
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

	if err = s.checkForbidWriteOpOfProtoVer0(p, partition); err != nil {
		return
	}

	if partition.Available() <= 0 || !partition.disk.CanWrite() {
		log.LogWarnf("[handlePacketToCreateExtent] dp(%v) not enough space, available(%v) canWrite(%v)", partition.partitionID, strutil.FormatSize(uint64(partition.Available())), partition.disk.CanWrite())
		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}

	partition.disk.allocCheckLimit(proto.IopsWriteType, 1)
	partition.disk.limitWrite.Run(0, true, func() {
		err = partition.ExtentStore().Create(p.ExtentID)
	})
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
		err = fmt.Errorf("from master Task(%v) cannot unmashal CreateDataPartition, err %s", task.ToString(), err.Error())
		return
	}
	p.AddMesgLog(string(bytes))
	if err = json.Unmarshal(bytes, request); err != nil {
		err = fmt.Errorf("from master Task(%v) cannot unmashal CreateDataPartitionRequest struct, err(%s)", task.ToString(), err.Error())
		return
	}
	p.PartitionID = request.PartitionId
	if dp, err = s.space.CreatePartition(request); err != nil {
		err = fmt.Errorf("from master Task(%v) cannot create Partition err(%v)", task.ToString(), err)
		return
	}
	p.PacketOkWithBody([]byte(dp.Disk().Path))
}

func (s *DataNode) commitDelVersion(volumeID string, verSeq uint64) (err error) {
	for _, partition := range s.space.partitions {
		if partition.config.VolName != volumeID {
			continue
		}
		verListMgr := partition.volVersionInfoList
		verListMgr.RWLock.Lock()
		for i, ver := range verListMgr.VerList {
			if i == len(verListMgr.VerList)-1 {
				log.LogWarnf("action[commitDelVersion] dp[%v] seq %v, seqArray size %v newest ver %v",
					partition.config.PartitionID, verSeq, len(verListMgr.VerList), ver.Ver)
				break
			}
			if ver.Ver == verSeq {
				log.LogInfof("action[commitDelVersion] updateVerList dp[%v] seq %v,seqArray size %v", partition.config.PartitionID, verSeq, len(verListMgr.VerList))
				verListMgr.VerList = append(verListMgr.VerList[:i], verListMgr.VerList[i+1:]...)
				break
			}
		}
		verListMgr.RWLock.Unlock()
	}
	return
}

func (s *DataNode) commitCreateVersion(req *proto.MultiVersionOpRequest) (err error) {
	log.LogInfof("action[commitCreateVersion] handle master version reqeust %v", req)
	var (
		value interface{}
		ok    bool
		wg    sync.WaitGroup
	)
	if value, ok = s.volUpdating.Load(req.VolumeID); !ok {
		log.LogWarnf("action[commitCreateVersion] vol %v not found seq %v", req.VolumeID, req.VerSeq)
		return
	}

	ver2Phase := value.(*verOp2Phase)
	log.LogInfof("action[commitCreateVersion] try commit volume %v ver2Phase seq %v with req seq %v",
		req.VolumeID, ver2Phase.verPrepare, req.VerSeq)
	if req.VerSeq < ver2Phase.verSeq {
		log.LogWarnf("action[commitCreateVersion] vol %v seq %v create less than loal %v", req.VolumeID, req.VerSeq, ver2Phase.verSeq)
		return
	}
	if ver2Phase.step != proto.CreateVersionPrepare {
		log.LogWarnf("action[commitCreateVersion] vol %v seq %v step not prepare", req.VolumeID, ver2Phase.step)
	}

	s.space.partitionMutex.RLock()
	partitions := make([]*DataPartition, 0)
	for _, dp := range s.space.partitions {
		partitions = append(partitions, dp)
	}
	s.space.partitionMutex.RUnlock()
	resultCh := make(chan error, len(partitions))
	for _, partition := range partitions {
		if partition.config.VolName != req.VolumeID {
			continue
		}
		if !partition.isRaftLeader {
			continue
		}
		wg.Add(1)
		go func(partition *DataPartition) {
			defer wg.Done()
			log.LogInfof("action[commitCreateVersion] volume %v dp[%v] do HandleVersionOp verSeq[%v]",
				partition.volumeID, partition.partitionID, partition.verSeq)
			if err = partition.HandleVersionOp(req); err != nil {
				log.LogErrorf("action[commitCreateVersion] volume %v dp[%v] do HandleVersionOp verSeq[%v] err %v",
					partition.volumeID, partition.partitionID, partition.verSeq, err)
				resultCh <- err
				return
			}
		}(partition)
	}

	wg.Wait()
	select {
	case err = <-resultCh:
		if err != nil {
			close(resultCh)
			return
		}
	default:
		log.LogInfof("action[commitCreateVersion] volume %v do HandleVersionOp verseq [%v] finished", req.VolumeID, req.VerSeq)
	}
	close(resultCh)
	if req.Op == proto.DeleteVersion {
		return
	}

	if req.Op == proto.CreateVersionPrepare {
		log.LogInfof("action[commitCreateVersion] commit volume %v prepare seq %v with commit seq %v",
			req.VolumeID, ver2Phase.verPrepare, req.VerSeq)
		return
	}

	ver2Phase.verSeq = req.VerSeq
	ver2Phase.step = proto.CreateVersionCommit
	ver2Phase.status = proto.VersionWorkingFinished
	log.LogInfof("action[commitCreateVersion] commit volume %v prepare seq %v with commit seq %v",
		req.VolumeID, ver2Phase.verPrepare, req.VerSeq)

	return
}

func (s *DataNode) prepareCreateVersion(req *proto.MultiVersionOpRequest) (err error, opAagin bool) {
	var ver2Phase *verOp2Phase
	if value, ok := s.volUpdating.Load(req.VolumeID); ok {
		ver2Phase = value.(*verOp2Phase)
		if req.VerSeq < ver2Phase.verSeq {
			err = fmt.Errorf("seq %v create less than loal %v", req.VerSeq, ver2Phase.verSeq)
			log.LogInfof("action[prepareCreateVersion] volume %v update to ver %v step %v", req.VolumeID, req.VerSeq, ver2Phase.step)
			return
		} else if req.VerSeq == ver2Phase.verPrepare {
			if ver2Phase.step == proto.VersionWorking {
				opAagin = true
				return
			}
		}
	}
	ver2Phase = &verOp2Phase{}
	ver2Phase.step = uint32(req.Op)
	ver2Phase.status = proto.VersionWorking
	ver2Phase.verPrepare = req.VerSeq

	s.volUpdating.Store(req.VolumeID, ver2Phase)

	log.LogInfof("action[prepareCreateVersion] volume %v update seq to %v step %v",
		req.VolumeID, req.VerSeq, ver2Phase.step)
	return
}

// Handle OpHeartbeat packet.
func (s *DataNode) handleUpdateVerPacket(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionUpdateVersion, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if !s.clusterEnableSnapshot {
		err = fmt.Errorf("cluster not enable snapshot!")
		return
	}
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		log.LogErrorf("action[handleUpdateVerPacket] handle master version reqeust err %v", err)
		return
	}
	request := &proto.MultiVersionOpRequest{}
	response := &proto.MultiVersionOpResponse{}
	response.Op = task.OpCode
	response.Status = proto.TaskSucceeds

	if task.OpCode == proto.OpVersionOperation {
		marshaled, _ := json.Marshal(task.Request)
		if err = json.Unmarshal(marshaled, request); err != nil {
			log.LogErrorf("action[handleUpdateVerPacket] handle master version reqeust err %v", err)
			response.Status = proto.TaskFailed
			goto end
		}

		if request.Op == proto.CreateVersionPrepare {
			if err, _ = s.prepareCreateVersion(request); err != nil {
				log.LogErrorf("action[handleUpdateVerPacket] handle master version reqeust err %v", err)
				goto end
			}
			if err = s.commitCreateVersion(request); err != nil {
				log.LogErrorf("action[handleUpdateVerPacket] handle master version reqeust err %v", err)
				goto end
			}
		} else if request.Op == proto.CreateVersionCommit {
			if err = s.commitCreateVersion(request); err != nil {
				log.LogErrorf("action[handleUpdateVerPacket] handle master version reqeust err %v", err)
				goto end
			}
		} else if request.Op == proto.DeleteVersion {
			if err = s.commitDelVersion(request.VolumeID, request.VerSeq); err != nil {
				log.LogErrorf("action[handleUpdateVerPacket] handle master version reqeust err %v", err)
				goto end
			}
		}

		response.VerSeq = request.VerSeq
		response.Op = request.Op
		response.Addr = request.Addr
		response.VolumeID = request.VolumeID

	} else {
		err = fmt.Errorf("illegal opcode")
		log.LogErrorf("action[handleUpdateVerPacket] handle master version reqeust err %v", err)
		goto end
	}
end:
	if err != nil {
		response.Result = err.Error()
	}
	task.Response = response
	log.LogInfof("action[handleUpdateVerPacket] rsp to client,req vol %v, verseq %v, op %v", request.VolumeID, request.VerSeq, request.Op)
	if err = MasterClient.NodeAPI().ResponseDataNodeTask(task); err != nil {
		err = errors.Trace(err, "handleUpdateVerPacket to master failed.")
		log.LogErrorf(err.Error())
		return
	}
}

func (s *DataNode) checkDecommissionDisks(decommissionDisks []string) {
	decommissionDiskSet := util.NewSet()
	for _, disk := range decommissionDisks {
		decommissionDiskSet.Add(disk)
	}
	disks := s.space.GetDisks()
	for _, disk := range disks {
		if disk.GetDecommissionStatus() && !decommissionDiskSet.Has(disk.Path) {
			log.LogDebugf("action[checkDecommissionDisks] mark %v to be undecommissioned", disk.Path)
			disk.MarkDecommissionStatus(false)
			continue
		}
		if !disk.GetDecommissionStatus() && decommissionDiskSet.Has(disk.Path) {
			log.LogDebugf("action[checkDecommissionDisks] mark %v to be decommissioned", disk.Path)
			disk.MarkDecommissionStatus(true)
			continue
		}
	}
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
		request := &proto.HeartBeatRequest{}
		response := &proto.DataNodeHeartbeatResponse{}
		begin := time.Now()
		if task.OpCode == proto.OpDataNodeHeartbeat {
			marshaled, _ := json.Marshal(task.Request)
			_ = json.Unmarshal(marshaled, request)
			response.Status = proto.TaskSucceeds
			if s.diskQosEnableFromMaster != request.EnableDiskQos {
				log.LogWarnf("action[handleHeartbeatPacket] master command disk qos enable change to [%v], local conf enable [%v]",
					request.EnableDiskQos,
					s.diskQosEnable)
			}
			dpBackupTimeout, _ := time.ParseDuration(request.DpBackupTimeout)
			if dpBackupTimeout <= proto.DefaultDataPartitionBackupTimeOut {
				dpBackupTimeout = proto.DefaultDataPartitionBackupTimeOut
			}
			s.dpBackupTimeout = dpBackupTimeout
			log.LogDebugf("handleHeartbeatPacket receive req(%v) dpBackupTimeout(%v)", task.RequestID, dpBackupTimeout)

			if s.nodeForbidWriteOpOfProtoVer0 != request.NotifyForbidWriteOpOfProtoVer0 {
				log.LogWarnf("[handleHeartbeatPacket] change nodeForbidWriteOpOfProtoVer0, old(%v) new(%v)",
					s.nodeForbidWriteOpOfProtoVer0, request.NotifyForbidWriteOpOfProtoVer0)
				s.nodeForbidWriteOpOfProtoVer0 = request.NotifyForbidWriteOpOfProtoVer0
			}

			log.LogDebugf("handleHeartbeatPacket receive req(%v)", task.RequestID)
			// NOTE: set decommission disks
			s.checkDecommissionDisks(request.DecommissionDisks)
			log.LogDebugf("handleHeartbeatPacket checkDecommissionDisks req(%v) cost %v",
				task.RequestID, time.Since(begin))

			forbiddenVols := make(map[string]struct{})
			for _, vol := range request.ForbiddenVols {
				if _, ok := forbiddenVols[vol]; !ok {
					forbiddenVols[vol] = struct{}{}
				}
			}

			volsForbidWriteOpOfProtoVer0 := make(map[string]struct{})
			for _, vol := range request.VolsForbidWriteOpOfProtoVer0 {
				if _, ok := volsForbidWriteOpOfProtoVer0[vol]; !ok {
					volsForbidWriteOpOfProtoVer0[vol] = struct{}{}
				}
			}
			s.VolsForbidWriteOpOfProtoVer0 = volsForbidWriteOpOfProtoVer0

			directReadVols := make(map[string]struct{})
			for _, vol := range request.DirectReadVols {
				if _, ok := directReadVols[vol]; !ok {
					directReadVols[vol] = struct{}{}
				}
			}
			s.DirectReadVols = directReadVols

			s.buildHeartBeatResponse(response, forbiddenVols, request.VolDpRepairBlockSize, task.RequestID)
			log.LogDebugf("handleHeartbeatPacket buildHeartBeatResponse req(%v) cost %v",
				task.RequestID, time.Since(begin))
			s.diskQosEnableFromMaster = request.EnableDiskQos

			log.LogDebugf("datanode.raftPartitionCanUsingDifferentPort from %v to %v ", s.raftPartitionCanUsingDifferentPort, request.RaftPartitionCanUsingDifferentPortEnabled)
			s.raftPartitionCanUsingDifferentPort = request.RaftPartitionCanUsingDifferentPortEnabled

			var needUpdate bool
			for _, pair := range []struct {
				replace uint64
				origin  *int
			}{
				{request.QosFlowWriteLimit, &s.diskWriteFlow},
				{request.QosFlowReadLimit, &s.diskReadFlow},
				{request.QosIopsWriteLimit, &s.diskWriteIops},
				{request.QosIopsReadLimit, &s.diskReadIops},
			} {
				if pair.replace > 0 && int(pair.replace) != *pair.origin {
					*pair.origin = int(pair.replace)
					needUpdate = true
				}
			}

			// set cpu util and io used in here
			response.CpuUtil = s.cpuUtil.Load()
			response.IoUtils = s.space.GetDiskUtils()

			if needUpdate {
				log.LogWarnf("action[handleHeartbeatPacket] master change disk qos limit to [flowWrite %v, flowRead %v, iopsWrite %v, iopsRead %v]",
					s.diskWriteFlow, s.diskReadFlow, s.diskWriteIops, s.diskReadIops)
				s.updateQosLimit()
			}
		} else {
			response.Status = proto.TaskFailed
			err = fmt.Errorf("illegal opcode")
			response.Result = err.Error()
		}
		task.Response = response
		log.LogDebugf("handleHeartbeatPacket send response req(%v) cost %v, cost from sendTime %v",
			task.RequestID, time.Since(begin), time.Since(time.Unix(task.SendTime, 0)))
		if err = MasterClient.NodeAPI().ResponseDataNodeTask(task); err != nil {
			err = errors.Trace(err, "heartbeat to master(%v) failed.", request.MasterAddr)
			log.LogErrorf("HeartbeatPacket response to master: task(%v), resp(%v) err(%v)", task, response, err.Error())
			return
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
	log.LogInfof(fmt.Sprintf("action[handlePacketToDeleteDataPartition] request %v ", request))
	if task.OpCode == proto.OpDeleteDataPartition {
		bytes, _ := json.Marshal(task.Request)
		p.AddMesgLog(string(bytes))
		err = json.Unmarshal(bytes, request)
		if err != nil {
			return
		} else {
			err = s.space.DeletePartition(request.PartitionId, request.Force)
		}
	} else {
		err = fmt.Errorf("illegal opcode ")
	}
	if err != nil {
		err = errors.Trace(err, "delete DataPartition failed,PartitionID(%v)", request.PartitionId)
		log.LogErrorf("action[handlePacketToDeleteDataPartition] err(%v).", err)
	} else {
		log.LogInfof(fmt.Sprintf("action[handlePacketToDeleteDataPartition] %v success", request.PartitionId))
	}
	auditlog.LogDataNodeOp("DeleteDataPartition", fmt.Sprintf("%v is deleted", request.PartitionId), err)
}

// Handle OpLoadDataPartition packet.
func (s *DataNode) handlePacketToLoadDataPartition(p *repl.Packet) {
	task := &proto.AdminTask{}
	var err error
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
	var err error
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
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionBatchMarkDelete, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*DataPartition)
	// NOTE: we cannot prevent mark delete
	// even the partition is forbidden, because
	// the inode already be deleted in meta partition
	// if we prevent it, we will get "orphan extents"
	if proto.IsTinyExtentType(p.ExtentType) || p.Opcode == proto.OpSplitMarkDelete {
		ext := new(proto.TinyExtentDeleteRecord)
		err = json.Unmarshal(p.Data, ext)
		if err == nil {
			log.LogInfof("handleMarkDeletePacket Delete PartitionID(%v)_Extent(%v)_Offset(%v)_Size(%v)",
				p.PartitionID, p.ExtentID, ext.ExtentOffset, ext.Size)
			partition.disk.allocCheckLimit(proto.IopsWriteType, 1)
			partition.disk.limitWrite.Run(0, true, func() {
				log.LogInfof("[handleBatchMarkDeletePacket] vol(%v) dp(%v) mark delete extent(%v)", partition.config.VolName, partition.partitionID, p.ExtentID)
				err = partition.ExtentStore().MarkDelete(p.ExtentID, int64(ext.ExtentOffset), int64(ext.Size))
				if err != nil {
					log.LogErrorf("action[handleMarkDeletePacket]: failed to mark delete extent(%v), %v", p.ExtentID, err)
				}
			})
		}
	} else {
		log.LogInfof("handleMarkDeletePacket Delete PartitionID(%v)_Extent(%v)",
			p.PartitionID, p.ExtentID)
		partition.disk.allocCheckLimit(proto.IopsWriteType, 1)
		if rs := partition.disk.limitWrite.Run(0, true, func() {
			log.LogInfof("[handleBatchMarkDeletePacket] vol(%v) dp(%v) mark delete extent(%v)", partition.config.VolName, partition.partitionID, p.ExtentID)
			err = partition.ExtentStore().MarkDelete(p.ExtentID, 0, 0)
			if err != nil {
				log.LogErrorf("action[handleMarkDeletePacket]: failed to mark delete extent(%v), %v", p.ExtentID, err)
			}
		}); err == nil && rs != nil {
			err = rs
		}
	}
}

// Handle OpMarkDelete packet.
func (s *DataNode) handleBatchMarkDeletePacket(p *repl.Packet, c net.Conn) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("(%v) error(%v).", p.GetUniqueLogId(), err))
			p.PackErrorBody(ActionBatchMarkDelete, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	partition := p.Object.(*DataPartition)
	// NOTE: we cannot prevent mark delete
	// even the partition is forbidden, because
	// the inode already be deleted in meta partition
	// if we prevent it, we will get "orphan extents"
	var exts []*proto.DelExtentParam
	err = json.Unmarshal(p.Data, &exts)
	if err != nil {
		log.LogErrorf("[handleBatchMarkDeletePacket] failed to unmarshal request, err(%v)", err)
		return
	}

	store := partition.ExtentStore()
	for _, ext := range exts {
		if p.Opcode == proto.OpGcBatchDeleteExtent && !store.CanGcDelete(ext.ExtentId) {
			log.LogWarnf("handleBatchMarkDeletePacket: ext %d is not in gc status, can't be gc delete, dp %d", ext.ExtentId, ext.PartitionId)
			err = storage.ParameterMismatchError
			return
		}

		if !deleteLimiteRater.Allow() {
			log.LogInfof("[handleBatchMarkDeletePacket] delete limiter reach(%v), remote (%v) try again.", deleteLimiteRater.Limit(), c.RemoteAddr().String())
			err = storage.LimitedIoError
			return
		}

		log.LogInfof(fmt.Sprintf("[handleBatchMarkDeletePacket] recive DeleteExtent (%v) from (%v)", ext, c.RemoteAddr().String()))
		partition.disk.allocCheckLimit(proto.IopsWriteType, 1)
		writable := partition.disk.limitWrite.TryRun(0, func() {
			if storage.IsTinyExtent(ext.ExtentId) || ext.IsSnapshotDeletion {
				log.LogInfof("[handleBatchMarkDeletePacket] vol(%v) dp(%v) mark delete extent(%v), tinyExtent or snapDeletion",
					partition.config.VolName, partition.partitionID, ext.ExtentId)
				err = store.MarkDelete(ext.ExtentId, int64(ext.ExtentOffset), int64(ext.Size))
			} else {
				log.LogInfof("[handleBatchMarkDeletePacket] vol(%v) dp(%v) mark delete extent(%v), normalExtent",
					partition.config.VolName, partition.partitionID, ext.ExtentId)
				// NOTE: it must use 0 to remove normal extent
				// Consider the following scenario:
				// data partition replica 1: size 200kb
				//                replica 2: size 100kb
				//                replica 3: size 100kb
				// meta partition: size 100kb
				// when we remove the file, the request size is 100kb
				// replica 1 will lost 100kb if we use ext.Size to remove extent
				err = partition.ExtentStore().MarkDelete(ext.ExtentId, 0, 0)
			}
			if err != nil {
				log.LogErrorf("action[handleBatchMarkDeletePacket]: failed to mark delete normalExtent extent(%v), offset(%v) err %v", ext.ExtentId, ext.FileOffset, err)
			}
		})

		if !writable {
			log.LogInfof("[handleBatchMarkDeletePacket] delete limitIo reach(%v), remote (%v) try again.", deleteLimiteRater.Limit(), c.RemoteAddr().String())
			err = storage.LimitedIoError
			return
		}

		// NOTE: reutrn if meet error
		if err != nil {
			return
		}
	}
}

func (s *DataNode) checkForbidWriteOpOfProtoVer0(p *repl.Packet, dp *DataPartition) (err error) {
	if p.Opcode == proto.OpBackupWrite {
		// this opCode only used by fsck
		return nil
	}

	if p.ProtoVersion != proto.PacketProtoVersion0 {
		return nil
	}

	if s.nodeForbidWriteOpOfProtoVer0 {
		err = fmt.Errorf("%v %v", storage.ClusterForbidWriteOpOfProtoVer, p.ProtoVersion)
		return
	}

	if dp.IsForbidWriteOpOfProtoVer0() {
		err = fmt.Errorf("%v %v", storage.VolForbidWriteOpOfProtoVer, p.ProtoVersion)
		return
	}

	return nil
}

// Handle OpWrite packet.
func (s *DataNode) handleWritePacket(p *repl.Packet) {
	var (
		err                     error
		metricPartitionIOLabels map[string]string
		partitionIOMetric       *exporter.TimePointCount
	)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	partition := p.Object.(*DataPartition)
	if partition.IsForbidden() {
		err = storage.ForbiddenDataPartitionError
		return
	}

	if err = s.checkForbidWriteOpOfProtoVer0(p, partition); err != nil {
		return
	}

	shallDegrade := p.ShallDegrade()
	if !shallDegrade {
		metricPartitionIOLabels = GetIoMetricLabels(partition, "write")
	}
	if partition.Available() <= 0 || !partition.disk.CanWrite() {
		log.LogWarnf("[handleWritePacket] dp(%v) not enough space, available(%v) canWrite(%v)", partition.partitionID, strutil.FormatSize(uint64(partition.Available())), partition.disk.CanWrite())
		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}
	store := partition.ExtentStore()
	if proto.IsTinyExtentType(p.ExtentType) {
		if !shallDegrade {
			partitionIOMetric = exporter.NewTPCnt(MetricPartitionIOName)
		}

		partition.disk.allocCheckLimit(proto.FlowWriteType, uint32(p.Size))
		partition.disk.allocCheckLimit(proto.IopsWriteType, 1)

		if writable := partition.disk.limitWrite.TryRun(int(p.Size), func() {
			param := &storage.WriteParam{
				ExtentID:      p.ExtentID,
				Offset:        p.ExtentOffset,
				Size:          int64(p.Size),
				Data:          p.Data,
				Crc:           p.CRC,
				WriteType:     storage.AppendWriteType,
				IsSync:        p.IsSyncWrite(),
				IsHole:        false,
				IsRepair:      false,
				IsBackupWrite: p.GetOpcode() == proto.OpBackupWrite,
			}
			_, err = store.Write(param)
		}); !writable {
			err = storage.LimitedIoError
			return
		}
		if !shallDegrade {
			s.metrics.MetricIOBytes.AddWithLabels(int64(p.Size), metricPartitionIOLabels)
			partitionIOMetric.SetWithLabels(err, metricPartitionIOLabels)
		}
		partition.checkIsDiskError(err, WriteFlag)
		return
	}

	if p.Size <= util.BlockSize {
		if !shallDegrade {
			partitionIOMetric = exporter.NewTPCnt(MetricPartitionIOName)
		}

		partition.disk.allocCheckLimit(proto.FlowWriteType, uint32(p.Size))
		partition.disk.allocCheckLimit(proto.IopsWriteType, 1)

		if writable := partition.disk.limitWrite.TryRun(int(p.Size), func() {
			param := &storage.WriteParam{
				ExtentID:      p.ExtentID,
				Offset:        p.ExtentOffset,
				Size:          int64(p.Size),
				Data:          p.Data,
				Crc:           p.CRC,
				WriteType:     storage.AppendWriteType,
				IsSync:        p.IsSyncWrite(),
				IsHole:        false,
				IsRepair:      false,
				IsBackupWrite: p.GetOpcode() == proto.OpBackupWrite,
			}
			_, err = store.Write(param)
		}); !writable {
			err = storage.LimitedIoError
			return
		}
		if !shallDegrade {
			s.metrics.MetricIOBytes.AddWithLabels(int64(p.Size), metricPartitionIOLabels)
			partitionIOMetric.SetWithLabels(err, metricPartitionIOLabels)
		}
		partition.checkIsDiskError(err, WriteFlag)
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
			if !shallDegrade {
				partitionIOMetric = exporter.NewTPCnt(MetricPartitionIOName)
			}

			partition.disk.allocCheckLimit(proto.FlowWriteType, uint32(currSize))
			partition.disk.allocCheckLimit(proto.IopsWriteType, 1)

			if writable := partition.disk.limitWrite.TryRun(currSize, func() {
				param := &storage.WriteParam{
					ExtentID:      p.ExtentID,
					Offset:        p.ExtentOffset + int64(offset),
					Size:          int64(currSize),
					Data:          data,
					Crc:           crc,
					WriteType:     storage.AppendWriteType,
					IsSync:        p.IsSyncWrite(),
					IsHole:        false,
					IsRepair:      false,
					IsBackupWrite: p.GetOpcode() == proto.OpBackupWrite,
				}
				_, err = store.Write(param)
			}); !writable {
				err = storage.LimitedIoError
				return
			}
			if !shallDegrade {
				s.metrics.MetricIOBytes.AddWithLabels(int64(p.Size), metricPartitionIOLabels)
				partitionIOMetric.SetWithLabels(err, metricPartitionIOLabels)
			}
			partition.checkIsDiskError(err, WriteFlag)
			if err != nil {
				break
			}
			size -= uint32(currSize)
			offset += currSize
		}
	}
}

func (s *DataNode) handleRandomWritePacket(p *repl.Packet) {
	var (
		err error

		metricPartitionIOLabels map[string]string
		partitionIOMetric       *exporter.TimePointCount
	)

	defer func() {
		log.LogDebugf("action[handleRandomWritePacket opcod %v seq %v dpid %v resultCode %v extid %v err %v",
			p.Opcode, p.VerSeq, p.PartitionID, p.ResultCode, p.ExtentID, err)
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			// avoid rsp pack ver info into package which client need do more work to read buffer
			if p.Opcode == proto.OpRandomWriteVer || p.Opcode == proto.OpSyncRandomWriteVer {
				p.Opcode = proto.OpSyncRandomWriteVerRsp
			}
			if p.Opcode == proto.OpTryWriteAppend && p.ResultCode == proto.OpTryOtherExtent {
				p.PackErrorBody(ActionWrite, storage.SnapshotNeedNewExtentError.Error())
				p.ResultCode = proto.OpTryOtherExtent
				log.LogDebugf("action[handleRandomWritePacket opcod %v seq %v dpid %v resultCode %v extid %v", p.Opcode, p.VerSeq, p.PartitionID, p.ResultCode, p.ExtentID)
				return
			}
			p.PacketOkReply()
		}
	}()

	partition := p.Object.(*DataPartition)
	if partition.IsForbidden() {
		err = storage.ForbiddenDataPartitionError
		return
	}

	if err = s.checkForbidWriteOpOfProtoVer0(p, partition); err != nil {
		return
	}

	log.LogDebugf("action[handleRandomWritePacket opcod %v seq %v dpid %v dpseq %v extid %v", p.Opcode, p.VerSeq, p.PartitionID, partition.verSeq, p.ExtentID)
	// cache or preload partition not support raft and repair.
	if !partition.isNormalType() {
		err = raft.ErrStopped
		return
	}

	_, isLeader := partition.IsRaftLeader()
	if !isLeader {
		err = raft.ErrNotLeader
		return
	}
	shallDegrade := p.ShallDegrade()
	if !shallDegrade {
		metricPartitionIOLabels = GetIoMetricLabels(partition, "randwrite")
		partitionIOMetric = exporter.NewTPCnt(MetricPartitionIOName)
	}

	err = partition.RandomWriteSubmit(p)
	if !shallDegrade {
		s.metrics.MetricIOBytes.AddWithLabels(int64(p.Size), metricPartitionIOLabels)
		partitionIOMetric.SetWithLabels(err, metricPartitionIOLabels)
	}

	if err != nil && strings.Contains(err.Error(), raft.ErrNotLeader.Error()) {
		err = raft.ErrNotLeader
		log.LogErrorf("action[handleRandomWritePacket] opcod %v seq %v dpid %v dpseq %v extid %v err %v", p.Opcode, p.VerSeq, p.PartitionID, partition.verSeq, p.ExtentID, err)
		return
	}

	if err == nil && p.ResultCode != proto.OpOk && p.ResultCode != proto.OpTryOtherExtent {
		log.LogErrorf("action[handleRandomWritePacket] opcod %v seq %v dpid %v dpseq %v extid %v ResultCode %v",
			p.Opcode, p.VerSeq, p.PartitionID, partition.verSeq, p.ExtentID, p.ResultCode)
		err = storage.TryAgainError
		return
	}
	log.LogDebugf("action[handleRandomWritePacket] opcod %v seq %v dpid %v dpseq %v after raft submit err %v resultCode %v",
		p.Opcode, p.VerSeq, p.PartitionID, partition.verSeq, err, p.ResultCode)
}

func (s *DataNode) handleStreamReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamRead, err.Error())
			p.WriteToConn(connect)
		}
	}()
	partition := p.Object.(*DataPartition)

	// cache or preload partition not support raft and repair.
	if !partition.isNormalType() {
		err = raft.ErrStopped
		return
	}

	if err = partition.CheckLeader(p, connect); err != nil {
		return
	}
	s.extentRepairReadPacket(p, connect, isRepairRead)
}

func (s *DataNode) handleExtentRepairReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var err error

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamRead, err.Error())
			p.WriteToConn(connect)
			return
		}
	}()
	err = requestDoExtentRepair()
	if err != nil {
		return
	}
	defer fininshDoExtentRepair()
	partition := p.Object.(*DataPartition)
	if !partition.disk.RequireReadExtentToken(partition.partitionID) {
		err = storage.NoDiskReadRepairExtentTokenError
		log.LogDebugf("dp(%v) disk(%v) extent(%v) wait for read extent token",
			p.PartitionID, partition.disk.Path, p.ExtentID)
		return
	}
	defer func() {
		partition.disk.ReleaseReadExtentToken()
		log.LogDebugf("dp(%v) disk(%v) extent(%v) release read extent token",
			p.PartitionID, partition.disk.Path, p.ExtentID)
	}()
	log.LogDebugf("dp(%v) disk(%v) extent(%v) get read extent token",
		p.PartitionID, partition.disk.Path, p.ExtentID)
	s.extentRepairReadPacket(p, connect, isRepairRead)
}

func (s *DataNode) handleTinyExtentRepairReadPacket(p *repl.Packet, connect net.Conn) {
	s.tinyExtentRepairRead(p, connect)
}

func (s *DataNode) handleSnapshotExtentRepairReadPacket(p *repl.Packet, connect net.Conn) {
	s.NormalSnapshotExtentRepairRead(p, connect)
}

func (s *DataNode) extentRepairReadPacket(p *repl.Packet, connect net.Conn, isRepairRead bool) {
	var err error

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamRead, err.Error())
			p.WriteToConn(connect)
		}
	}()
	partition := p.Object.(*DataPartition)
	if partition.IsForbidden() && !isRepairRead {
		err = storage.ForbiddenDataPartitionError
		return
	}
	log.LogDebugf("extentRepairReadPacket ready to repair dp(%v) disk(%v) extent(%v) offset (%v) needSize (%v)",
		p.PartitionID, partition.disk.Path, p.ExtentID, p.ExtentOffset, p.Size)

	if err = partition.NormalExtentRepairRead(p, connect, isRepairRead, s.metrics, repl.NewStreamReadResponsePacket); err != nil {
		return
	}
	p.PacketOkReply()
}

func (s *DataNode) handlePacketToGetAllWatermarks(p *repl.Packet) {
	var (
		buf       []byte
		fInfoList []*storage.ExtentInfo
		err       error
	)
	data := p.GetData()
	partition := p.Object.(*DataPartition)
	store := partition.ExtentStore()
	if proto.IsNormalExtentType(p.ExtentType) {
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
		if len(data) == 0 || data[0] != repl.ByteMarker {
			buf, err = json.Marshal(fInfoList)
		} else {
			buf, err = storage.MarshalBinarySlice(fInfoList)
		}

		if err != nil {
			p.PackErrorBody(ActionGetAllExtentWatermarks, err.Error())
		} else {
			p.PacketOkWithByte(buf)
		}
	}
}

func writeEmptyPacketOnExtentRepairRead(reply repl.PacketInterface, newOffset, currentOffset int64, connect net.Conn) (replySize int64, err error) {
	replySize = newOffset - currentOffset
	reply.SetData(make([]byte, 0))
	reply.SetSize(0)
	reply.SetCRC(crc32.ChecksumIEEE(reply.GetData()))
	reply.SetResultCode(proto.OpOk)
	reply.SetExtentOffset(currentOffset)
	reply.GetArg()[0] = EmptyResponse
	binary.BigEndian.PutUint64(reply.GetArg()[1:9], uint64(replySize))
	err = reply.WriteToConn(connect)
	reply.SetSize(uint32(replySize))
	if connect.RemoteAddr() != nil { // testcase connect not have effect fd
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.GetStartT(), err))
		log.LogReadf(logContent)
	}
	return
}

func (s *DataNode) NormalSnapshotExtentRepairRead(request *repl.Packet, connect net.Conn) {
	replyFunc := func() repl.PacketInterface {
		reply := repl.NewNormalExtentWithHoleStreamReadResponsePacket(request.ReqID, request.PartitionID, request.ExtentID)
		reply.ArgLen = NormalExtentWithHoleRepairReadResponseArgLen
		reply.Arg = make([]byte, NormalExtentWithHoleRepairReadResponseArgLen)
		return reply
	}
	s.ExtentWithHoleRepairRead(request, connect, replyFunc)
}

// Handle tinyExtentRepairRead packet.
func (s *DataNode) tinyExtentRepairRead(request *repl.Packet, connect net.Conn) {
	var err error
	defer func() {
		if err != nil {
			request.PackErrorBody(ActionStreamReadTinyExtentRepair, err.Error())
			request.WriteToConn(connect)
		}
	}()

	if !storage.IsTinyExtent(request.ExtentID) {
		err = fmt.Errorf("unavali extentID (%v)", request.ExtentID)
		return
	}
	replyFunc := func() repl.PacketInterface {
		reply := repl.NewTinyExtentStreamReadResponsePacket(request.ReqID, request.PartitionID, request.ExtentID)
		reply.ArgLen = TinyExtentRepairReadResponseArgLen
		reply.Arg = make([]byte, TinyExtentRepairReadResponseArgLen)
		return reply
	}
	s.ExtentWithHoleRepairRead(request, connect, replyFunc)
}

// Handle tinyExtentRepairRead packet.
func (s *DataNode) ExtentWithHoleRepairRead(request *repl.Packet, connect net.Conn, getReplyPacket func() repl.PacketInterface) {
	partition := request.Object.(*DataPartition)
	partition.ExtentWithHoleRepairRead(request, connect, getReplyPacket)
}

func (s *DataNode) handlePacketToReadTinyDeleteRecordFile(p *repl.Packet, connect net.Conn) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStreamReadTinyDeleteRecord, err.Error())
			p.WriteToConn(connect)
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
	reply := repl.NewReadTinyDeleteRecordResponsePacket(p.ReqID, p.PartitionID)
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
			err = fmt.Errorf(ActionStreamReadTinyDeleteRecord+" localTinyDeleteRecordSize(%v) offset(%v)"+
				" currReadSize(%v) err(%v)", localTinyDeleteFileSize, offset, currReadSize, err)
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect); err != nil {
			return
		}
		needReplySize -= int64(currReadSize)
		offset += int64(currReadSize)
	}
	p.PacketOkReply()
}

// Handle OpNotifyReplicasToRepair packet.
func (s *DataNode) handlePacketToNotifyExtentRepair(p *repl.Packet) {
	var err error
	partition := p.Object.(*DataPartition)
	mf := new(DataPartitionRepairTask)
	err = json.Unmarshal(p.Data, mf)
	if err != nil {
		p.PackErrorBody(ActionRepair, err.Error())
		return
	}
	partition.DoExtentStoreRepair(mf)
	p.PacketOkReply()
}

// Handle OpBroadcastMinAppliedID
func (s *DataNode) handleBroadcastMinAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	minAppliedID := binary.BigEndian.Uint64(p.Data)
	if minAppliedID > 0 {
		partition.SetMinAppliedID(minAppliedID)
	}
	log.LogDebugf("[handleBroadcastMinAppliedID] partition(%v) minAppliedID(%v)", partition.partitionID, minAppliedID)
	p.PacketOkReply()
}

// Handle handlePacketToGetAppliedID packet.
func (s *DataNode) handlePacketToGetAppliedID(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	appliedID := partition.GetAppliedID()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedID)
	p.PacketOkWithBody(buf)
	p.AddMesgLog(fmt.Sprintf("_AppliedID(%v)", appliedID))
}

func (s *DataNode) handlePacketToGetPartitionSize(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	logicSize := partition.extentStore.StoreSizeExtentID(p.ExtentID)
	realUsedSize := partition.Used()
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(logicSize))
	binary.BigEndian.PutUint64(buf[8:], uint64(realUsedSize))
	p.AddMesgLog(fmt.Sprintf("logicSize_(%v)_realUsed_(%d)", logicSize, realUsedSize))
	p.PacketOkWithBody(buf)
}

func (s *DataNode) handlePacketToGetMaxExtentIDAndPartitionSize(p *repl.Packet) {
	partition := p.Object.(*DataPartition)
	maxExtentID, totalPartitionSize := partition.extentStore.GetMaxExtentIDAndPartitionSize()
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(maxExtentID))
	binary.BigEndian.PutUint64(buf[8:16], totalPartitionSize)
	p.PacketOkWithBody(buf)
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

	isRaftLeader, err = s.forwardToRaftLeader(dp, p, false)
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

	log.LogInfof("action[handlePacketToAddDataPartitionRaftMember]req(%v) addPeer %v, partition id %v",
		p.GetReqID(), req.AddPeer, req.PartitionId)

	p.AddMesgLog(string(reqData))
	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		return
	}
	if !dp.setChangeMemberWaiting() {
		err = proto.ErrMemberChange
		return
	}
	defer dp.setRestoreReplicaFinish()
	p.PartitionID = req.PartitionId
	// check if peer is already added
	if dp.IsExistReplicaWithNodeId(req.AddPeer.Addr, req.AddPeer.ID) {
		if err = dp.hasNodeIDConflict(req.AddPeer.Addr, req.AddPeer.ID); err != nil {
			log.LogWarnf("action[handlePacketToAddDataPartitionRaftMember] partition %v node id conflict: %v",
				req.PartitionId, err)
		} else {
			log.LogInfof("handlePacketToAddDataPartitionRaftMember receive MasterCommand: %v "+
				"addRaftAddr(%v) is exist", string(reqData), req.AddPeer.Addr)
		}
		return
	}

	isRaftLeader, err = s.forwardToRaftLeader(dp, p, false)
	if !isRaftLeader {
		if err != nil {
			log.LogWarnf("action[handlePacketToAddDataPartitionRaftMember]dp %v  req %v  addPeer %v forward to leader failed:%v",
				dp.partitionID, p.GetReqID(), req.AddPeer, err)
		} else {
			log.LogWarnf("action[handlePacketToAddDataPartitionRaftMember]dp %v  req %v  addPeer %v forward to leader",
				dp.partitionID, p.GetReqID(), req.AddPeer)
		}
		return
	}
	log.LogInfof("action[handlePacketToAddDataPartitionRaftMember] before ChangeRaftMember %v which is sync. partition id %v", req.AddPeer, req.PartitionId)

	if req.AddPeer.ID != 0 {
		_, err = dp.ChangeRaftMember(raftProto.ConfAddNode, raftProto.Peer{ID: req.AddPeer.ID}, reqData)
		if err != nil {
			return
		}
	}
	log.LogInfof("action[handlePacketToAddDataPartitionRaftMember] after ChangeRaftMember %v, partition id %v", req.AddPeer, &req.PartitionId)
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

	dp := s.space.Partition(req.PartitionId)
	if dp == nil {
		return
	}
	if !dp.setChangeMemberWaiting() {
		err = proto.ErrMemberChange
		return
	}
	defer dp.setRestoreReplicaFinish()
	log.LogInfof("action[handlePacketToRemoveDataPartitionRaftMember], req %v (%s) RemoveRaftPeer(%s) dp %v "+
		"replicaNum %v config.Peer %v replica %v force %v auto %v",
		p.GetReqID(), string(reqData), req.RemovePeer.Addr, dp.partitionID, dp.replicaNum, dp.config.Peers, dp.replicas,
		req.Force, req.AutoRemove)

	p.PartitionID = req.PartitionId
	// do not return error to keep master decommission progress go forward
	// do not check replica existence on leader for autoRemove enable, follower may be contains redundant peers, make sure
	// leader can send remove wal logs to follower
	if !dp.IsExistReplica(req.RemovePeer.Addr) && !req.Force && !req.AutoRemove {
		log.LogWarnf("action[handlePacketToRemoveDataPartitionRaftMember]dp %v receive MasterCommand:  req %v "+
			"RemoveRaftPeer(%v) force(%v)  autoRemove(%v) has not exist", dp.partitionID, p.GetReqID(), req.RemovePeer, req.Force, req.AutoRemove)
		return
	}

	if !req.Force {
		isRaftLeader, err = s.forwardToRaftLeader(dp, p, req.Force)
		if !isRaftLeader {
			if err != nil {
				log.LogWarnf("action[handlePacketToRemoveDataPartitionRaftMember]dp %v  req %v forward to leader failed:%v",
					dp.partitionID, p.GetReqID(), err)
			} else {
				log.LogWarnf("action[handlePacketToRemoveDataPartitionRaftMember]dp %v  req %v forward to leader",
					dp.partitionID, p.GetReqID())
			}
			return
		}
	}

	removePeer := req.RemovePeer
	found := false
	// nodeId may be changed for this peer, reset nodeID to META of dp
	for _, peer := range dp.config.Peers {
		if peer.Addr != req.RemovePeer.Addr {
			continue
		} else {
			found = true
			if peer.ID != req.RemovePeer.ID {
				// update removePeer.ID  according to local config
				removePeer.ID = peer.ID
				log.LogWarnf("handlePacketToRemoveDataPartitionRaftMember dp(%v) peer(%v) nodeID(%v) is different from req(%v)",
					dp.partitionID, peer.Addr, peer.ID, req.RemovePeer.ID)
				// update reqData for
				newReq := &proto.RemoveDataPartitionRaftMemberRequest{
					PartitionId: req.PartitionId,
					Force:       req.Force,
					RemovePeer:  removePeer,
				}
				reqData, err = json.Marshal(newReq)
				if err != nil {
					log.LogWarnf("handlePacketToRemoveDataPartitionRaftMember dp(%v) marshall req(%v) failed:%v",
						dp.partitionID, newReq, err)
					return
				}
			}
		}
	}

	if !found && !req.AutoRemove && !req.Force {
		err = errors.NewErrorf("cannot found peer(%v) in dp(%v) peers", req.RemovePeer.Addr, dp.partitionID)
		log.LogWarnf("handlePacketToRemoveDataPartitionRaftMember:%v", err.Error())
		return
	}

	if !req.AutoRemove {
		if err = dp.CanRemoveRaftMember(removePeer, req.Force); err != nil {
			log.LogWarnf("action[handlePacketToRemoveDataPartitionRaftMember] CanRemoveRaftMember failed "+
				"req %v dp %v err %v",
				p.GetReqID(), dp.partitionID, err.Error())
			return
		}
	}

	if req.Force {
		cc := &raftProto.ConfChange{
			Type: raftProto.ConfRemoveNode,
			Peer: raftProto.Peer{
				ID: removePeer.ID,
			},
			Context: reqData,
		}
		s.raftStore.RaftServer().RemoveRaftForce(dp.partitionID, cc)
		dp.ApplyMemberChange(cc, 0)
		dp.PersistMetadata()
		return
	}

	if req.RemovePeer.ID != 0 {
		log.LogDebugf("action[handlePacketToRemoveDataPartitionRaftMember] ChangeRaftMember "+
			"req %v dp %v RemovePeer.ID %v", p.GetReqID(), dp.partitionID, req.RemovePeer.ID)
		_, err = dp.ChangeRaftMember(raftProto.ConfRemoveNode, raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
		if err != nil {
			return
		}
	}
	log.LogDebugf("action[handlePacketToRemoveDataPartitionRaftMember] CanRemoveRaftMember complete "+
		"req %v dp %v ", p.GetReqID(), dp.partitionID)
}

func (s *DataNode) handlePacketToDataPartitionTryToLeader(p *repl.Packet) {
	var err error

	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDataPartitionTryToLeader, err.Error())
			log.LogWarnf("handlePacketToDataPartitionTryToLeader: %v ", err.Error())
		} else {
			p.PacketOkReply()
			log.LogDebugf("handlePacketToDataPartitionTryToLeader: partition %v success ", p.PartitionID)
		}
	}()
	log.LogDebugf("handlePacketToDataPartitionTryToLeader: partition %v ", p.PartitionID)
	dp := s.space.Partition(p.PartitionID)
	if dp == nil {
		err = fmt.Errorf("partition %v not exsit", p.PartitionID)
		return
	}

	if dp.raftStatus != RaftStatusRunning {
		err = fmt.Errorf("partition %v raft not running", p.PartitionID)
		return
	}

	if dp.raftPartition.IsRaftLeader() {
		log.LogWarnf("handlePacketToDataPartitionTryToLeader: %v is already leader", p.PartitionID)
		return
	}
	err = dp.raftPartition.TryToLeader(dp.partitionID)
}

func (s *DataNode) forwardToRaftLeader(dp *DataPartition, p *repl.Packet, force bool) (ok bool, err error) {
	var (
		conn       *net.TCPConn
		leaderAddr string
	)

	if leaderAddr, ok = dp.IsRaftLeader(); ok {
		return
	}
	// return NoLeaderError if leaderAddr is nil
	if leaderAddr == "" {
		if force {
			ok = true
			log.LogInfof("action[forwardToRaftLeader] no leader but replica num %v continue", dp.replicaNum)
			return
		}
		err = storage.NoLeaderError
		return
	}

	// forward the packet to the leader if local one is not the leader
	conn, err = gConnPool.GetConnect(leaderAddr)
	if err != nil {
		return
	}
	defer func() {
		gConnPool.PutConnect(conn, err != nil)
	}()
	err = p.WriteToConn(conn)
	if err != nil {
		return
	}
	if err = p.ReadFromConnWithVer(conn, proto.NoReadDeadlineTime); err != nil {
		return
	}

	return
}

func (s *DataNode) handlePacketToStopDataPartitionRepair(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionStopDataPartitionRepair, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}
	request := &proto.StopDataPartitionRepairRequest{}
	if task.OpCode != proto.OpStopDataPartitionRepair {
		err = fmt.Errorf("action[handlePacketToStopDataPartitionRepair] illegal opcode ")
		log.LogWarnf("action[handlePacketToStopDataPartitionRepair] illegal opcode ")
		return
	}

	bytes, _ := json.Marshal(task.Request)
	p.AddMesgLog(string(bytes))
	err = json.Unmarshal(bytes, request)
	if err != nil {
		return
	}
	log.LogDebugf("action[handlePacketToStopDataPartitionRepair] try stop %v", request.PartitionId)
	dp := s.space.Partition(request.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		log.LogWarnf("action[handlePacketToStopDataPartitionRepair] cannot find dp %v", request.PartitionId)
		return
	}
	dp.StopDecommissionRecover(request.Stop)
	log.LogInfof("action[handlePacketToStopDataPartitionRepair] %v stop %v success", request.PartitionId, request.Stop)
}

func (s *DataNode) handlePacketToRecoverDataReplicaMeta(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRecoverDataReplicaMeta, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}
	request := &proto.RecoverDataReplicaMetaRequest{}
	if task.OpCode != proto.OpRecoverDataReplicaMeta {
		err = fmt.Errorf("action[handlePacketToRecoverDataReplicaMeta] illegal opcode ")
		log.LogWarnf("action[handlePacketToRecoverDataReplicaMeta] illegal opcode ")
		return
	}

	bytes, _ := json.Marshal(task.Request)
	p.AddMesgLog(string(bytes))
	err = json.Unmarshal(bytes, request)
	if err != nil {
		return
	}
	log.LogDebugf("action[handlePacketToRecoverDataReplicaMeta] try stop %v", request.PartitionId)
	dp := s.space.Partition(request.PartitionId)
	if dp == nil {
		err = proto.ErrDataPartitionNotExists
		log.LogWarnf("action[handlePacketToRecoverDataReplicaMeta] cannot find dp %v", request.PartitionId)
		return
	}
	// modify meta content
	dp.config.Hosts = request.Hosts
	dp.config.Peers = request.Peers
	if err = dp.PersistMetadata(); err != nil {
		log.LogErrorf("action[ApplyMemberChange] dp(%v) PersistMetadata err(%v).", dp.partitionID, err)
		if IsDiskErr(err.Error()) {
			panic(newRaftApplyError(err))
		}
		return
	}
	// reload data partition
	err = dp.reload(s.space)
	if err != nil {
		log.LogWarnf("action[handlePacketToRecoverDataReplicaMeta] dp %v recover replica failed %v", dp.partitionID, err)
	} else {
		log.LogInfof("action[handlePacketToRecoverDataReplicaMeta] dp %v recover replica success", dp.partitionID)
	}
}

func (s *DataNode) handleBatchLockNormalExtent(p *repl.Packet, connect net.Conn) {
	var err error

	defer func() {
		if err != nil {
			log.LogErrorf("action[handleBatchLockNormalExtent] err %v", err)
			p.PackErrorBody(ActionBatchLockNormalExtent, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	partition := p.Object.(*DataPartition)
	gcLockEks := &proto.GcLockExtents{}
	err = json.Unmarshal(p.Data, gcLockEks)
	if err != nil {
		log.LogErrorf("action[handleBatchLockNormalExtent] err %v", err)
		return
	}

	store := partition.ExtentStore()

	// In order to prevent users from writing extents, lock first and then create
	err = store.ExtentBatchLockNormalExtent(gcLockEks)
	if err != nil {
		log.LogErrorf("action[handleBatchLockNormalExtent] err %v", err)
		return
	}

	start := time.Now()
	if gcLockEks.IsCreate {
		for _, ek := range gcLockEks.Eks {
			err = store.Create(ek.ExtentId)
			if err == storage.ExtentExistsError {
				err = nil
				continue
			}
			if err != nil {
				log.LogErrorf("action[handleBatchLockNormalExtent] create path %s, id %d, extent err %v",
					partition.Path(), ek.ExtentId, err)
				return
			}
		}
	}
	log.LogInfof("action[handleBatchLockNormalExtent] dp %d, success len: %v, isCreate: %v, flag %s, cost %d ms",
		partition.partitionID, len(gcLockEks.Eks), gcLockEks.IsCreate, gcLockEks.Flag.String(), time.Since(start).Milliseconds())
}

func (s *DataNode) handleBatchUnlockNormalExtent(p *repl.Packet, connect net.Conn) {
	var err error

	defer func() {
		if err != nil {
			log.LogErrorf("action[handleBatchUnlockNormalExtent] err %v", err)
			p.PackErrorBody(ActionBatchLockNormalExtent, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	partition := p.Object.(*DataPartition)
	var exts []*proto.ExtentKey
	err = json.Unmarshal(p.Data, &exts)
	if err != nil {
		log.LogErrorf("action[handleBatchUnlockNormalExtent] err %v", err)
		return
	}

	store := partition.ExtentStore()
	store.ExtentBatchUnlockNormalExtent(exts)

	log.LogInfof("action[handleBatchUnlockNormalExtent] success len: %v", len(exts))
}

func (s *DataNode) handlePacketToRecoverBackupDataReplica(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRecoverBackupDataReplica, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}
	request := &proto.RecoverBackupDataReplicaRequest{}
	if task.OpCode != proto.OpRecoverBackupDataReplica {
		err = fmt.Errorf("action[handlePacketToRecoverBackupDataReplica] illegal opcode ")
		log.LogWarnf("action[handlePacketToRecoverBackupDataReplica] illegal opcode ")
		return
	}

	bytes, _ := json.Marshal(task.Request)
	p.AddMesgLog(string(bytes))
	err = json.Unmarshal(bytes, request)
	if err != nil {
		return
	}
	log.LogDebugf("action[handlePacketToRecoverBackupDataReplica] try recover %v", request.PartitionId)
	disk, err := s.space.GetDisk(request.Disk)
	if err != nil {
		log.LogErrorf("action[handlePacketToRecoverBackupDataReplica] disk(%v) is not found err(%v).", request.Disk, err)
		return
	}
	disk.BackupReplicaLk.Lock()
	fileInfoList, err := os.ReadDir(disk.Path)
	if err != nil {
		log.LogErrorf("action[handlePacketToRecoverBackupDataReplica] read dir(%v) err(%v).", disk.Path, err)
		disk.BackupReplicaLk.Unlock()
		return
	}
	rootDir := ""
	minDiff := math.MaxInt64
	currentTime := time.Now().Unix()
	for _, fileInfo := range fileInfoList {
		filename := fileInfo.Name()

		if !disk.isBackupPartitionDirToDelete(filename) {
			continue
		}

		if id, ts, err := unmarshalBackupPartitionDirNameAndTimestamp(filename); err != nil {
			log.LogErrorf("action[handlePacketToRecoverBackupDataReplica] unmarshal partitionName(%v) from disk(%v) err(%v) ",
				filename, disk.Path, err.Error())
			continue
		} else {
			if id == request.PartitionId {
				diff := int(math.Abs(float64(ts - currentTime)))
				if diff < minDiff {
					minDiff = diff
					rootDir = filename
				}
			}
		}
	}
	if rootDir == "" {
		err = errors.NewErrorf("dp(%v) root not found in dir(%v)", request.PartitionId, disk.Path)
		log.LogErrorf("action[handlePacketToRecoverBackupDataReplica] err %v", err.Error())
		disk.BackupReplicaLk.Unlock()
		return
	}
	log.LogDebugf("action[handlePacketToRecoverBackupDataReplica] ready to recover %v", rootDir)
	// rename root dir back to normal
	newPath := strings.Replace(rootDir, BackupPartitionPrefix, "", 1)
	parts := strings.Split(newPath, "-")
	newPath = parts[0]
	err = os.Rename(path.Join(disk.Path, rootDir), path.Join(disk.Path, newPath))
	if err != nil {
		log.LogErrorf("action[handlePacketToRecoverBackupDataReplica] rename disk %v rootDir %v to %v failed %v.",
			disk.Path, rootDir, newPath, err)
		disk.BackupReplicaLk.Unlock()
		return
	}
	disk.BackupReplicaLk.Unlock()
	_, err = LoadDataPartition(path.Join(request.Disk, newPath), disk)
	if err != nil {
		log.LogErrorf("action[handlePacketToRecoverBackupDataReplica] load disk %v rootDir %v failed err %v.",
			disk.Path, rootDir, err)
	} else {
		log.LogInfof("action[handlePacketToRecoverBackupDataReplica] load disk %v rootDir %v success .",
			disk.Path, rootDir)
	}
}

func (s *DataNode) handlePacketToRecoverBadDisk(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRecoverBadDisk, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()
	if err != nil {
		return
	}
	request := &proto.RecoverBadDiskRequest{}
	if task.OpCode != proto.OpRecoverBadDisk {
		err = fmt.Errorf("action[handlePacketToRecoverBadDisk] illegal opcode ")
		log.LogWarnf("action[handlePacketToRecoverBadDisk] illegal opcode ")
		return
	}

	bytes, _ := json.Marshal(task.Request)
	p.AddMesgLog(string(bytes))
	err = json.Unmarshal(bytes, request)
	if err != nil {
		return
	}
	log.LogDebugf("action[handlePacketToRecoverBadDisk] try recover disk %v req %v", request.DiskPath, task.RequestID)
	disk, err := s.space.GetDisk(request.DiskPath)
	if err != nil {
		log.LogErrorf("action[handlePacketToRecoverBadDisk] disk(%v) is not found err(%v).", request.DiskPath, err)
		return
	}
	if !disk.startRecover() {
		err = errors.NewErrorf("disk %v recover is still running", disk.Path)
		log.LogErrorf("action[handlePacketToRecoverBadDisk] %v.", err)
		return
	}
	log.LogInfof("action[handlePacketToRecoverBadDisk]req(%v) recover disk %v status %v disk %p",
		task.RequestID, disk.Path, disk.recoverStatus, disk)
	go func() {
		defer func() {
			disk.stopRecover()
			log.LogInfof("action[handlePacketToRecoverBadDisk]req(%v) recover disk %v async exit status %v", task.RequestID, disk.Path, disk.recoverStatus)
		}()
		badDpList := disk.GetDiskErrPartitionList()
		log.LogInfof("action[handlePacketToRecoverBadDisk]req(%v) recover disk %v async enter bad %v disk %p", task.RequestID, disk.Path, len(badDpList), disk)
		begin := time.Now()
		for _, dpId := range badDpList {
			if dpId == 0 {
				// triggered by io error without dp
				disk.DiskErrPartitionSet.Delete(dpId)
				continue
			}
			partition := s.space.Partition(dpId)
			if partition == nil {
				log.LogWarnf("action[handlePacketToRecoverBadDisk]req(%v) bad dp(%v) not found on disk (%v).", task.RequestID, dpId, request.DiskPath)
				continue
			}
			partition.resetDiskErrCnt()
			if err = partition.PersistMetadata(); err != nil {
				log.LogWarnf("action[handlePacketToRecoverBadDisk] bad dp(%v) on disk (%v) persist failed %v.",
					partition.partitionID, request.DiskPath, err)
				continue
			}
			if err = partition.reload(s.space); err != nil {
				log.LogWarnf("action[handlePacketToRecoverBadDisk] bad dp(%v) on disk (%v) reload failed %v.",
					partition.partitionID, request.DiskPath, err)
				continue
			}
			// delete bad io dp
			disk.DiskErrPartitionSet.Delete(dpId)
			log.LogInfof("action[handlePacketToRecoverBadDisk]req(%v) bad dp(%v) on disk (%v) reload success",
				task.RequestID, dpId, request.DiskPath)
		}
		diskErrPartitions := disk.GetDiskErrPartitionList()
		if len(diskErrPartitions) != 0 {
			err = errors.NewErrorf("disk(%v) has bad dp %v left", request.DiskPath, diskErrPartitions)
			return
		}
		disk.recoverDiskError()
		log.LogInfof("action[handlePacketToRecoverBadDisk]req(%v) recover disk %v success dp(%v) cost %v",
			task.RequestID, disk.Path, len(badDpList), time.Since(begin))
	}()
	log.LogInfof("action[handlePacketToRecoverBadDisk] recover bad disk (%v) run async", request.DiskPath)
}

func (s *DataNode) handlePacketToQueryBadDiskRecoverProgress(p *repl.Packet) {
	var (
		task = &proto.AdminTask{}
		buf  []byte
		err  error
	)
	err = json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionQueryBadDiskRecoverProgress, err.Error())
		} else {
			p.PacketOkWithByte(buf)
		}
	}()
	if err != nil {
		return
	}
	request := &proto.RecoverBadDiskRequest{}
	if task.OpCode != proto.OpQueryBadDiskRecoverProgress {
		err = fmt.Errorf("action[handlePacketToQueryBadDiskRecoverProgress] illegal opcode ")
		log.LogWarnf("action[handlePacketToQueryBadDiskRecoverProgress] illegal opcode ")
		return
	}

	bytes, _ := json.Marshal(task.Request)
	p.AddMesgLog(string(bytes))
	err = json.Unmarshal(bytes, request)
	if err != nil {
		return
	}
	log.LogDebugf("action[handlePacketToRecoverBadDisk] try recover disk %v req %v", request.DiskPath, task.RequestID)
	disk, err := s.space.GetDisk(request.DiskPath)
	if err != nil {
		log.LogWarnf("action[handlePacketToRecoverBadDisk] disk(%v) is not found err(%v).", request.DiskPath, err)
		return
	}
	total := disk.DataPartitionList()
	badDpList := disk.GetDiskErrPartitionList()
	resp := &proto.BadDiskRecoverProgress{
		TotalPartitionsNum:   len(total),
		BadDataPartitions:    badDpList,
		BadDataPartitionsNum: len(badDpList),
		Status:               GetStatusMessage(disk.Status),
	}
	buf, err = json.Marshal(resp)
	if err != nil {
		log.LogWarnf("action[handlePacketToRecoverBadDisk] disk %v marshal(%v) failed %v.", request.DiskPath, resp, err)
		return
	}
	log.LogInfof("action[handlePacketToQueryBadDiskRecoverProgress] bad disk (%v) resp %v", request.DiskPath, resp)
}

func GetStatusMessage(status int) string {
	switch status {
	case proto.Recovering:
		return "Recovering"
	case proto.ReadOnly:
		return "ReadOnly"
	case proto.ReadWrite:
		return "ReadWrite"
	case proto.Unavailable:
		return "Unavailable"
	default:
		return fmt.Sprintf("Unkown:%v", status)
	}
}

func (s *DataNode) handlePacketToOpDeleteBackupDirectories(p *repl.Packet) {
	var (
		task = &proto.AdminTask{}
		buf  []byte
		err  error
	)
	err = json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionDeleteBackupDirectories, err.Error())
		} else {
			p.PacketOkWithByte(buf)
		}
	}()
	if err != nil {
		return
	}
	request := &proto.RecoverBadDiskRequest{}
	if task.OpCode != proto.OpDeleteBackupDirectories {
		err = fmt.Errorf("action[handlePacketToOpDeleteBackupDirectories] illegal opcode ")
		log.LogWarnf("action[handlePacketToOpDeleteBackupDirectories] illegal opcode ")
		return
	}

	bytes, _ := json.Marshal(task.Request)
	p.AddMesgLog(string(bytes))
	err = json.Unmarshal(bytes, request)
	if err != nil {
		return
	}
	log.LogDebugf("action[handlePacketToOpDeleteBackupDirectories] try delete backup directories in disk %v req %v", request.DiskPath, task.RequestID)
	disk, err := s.space.GetDisk(request.DiskPath)
	if err != nil {
		log.LogWarnf("action[handlePacketToRecoverBadDisk] disk(%v) is not found err(%v).", request.DiskPath, err)
		return
	}
	// list root path
	fileInfoList, err := os.ReadDir(disk.Path)
	if err != nil {
		log.LogErrorf("action[handlePacketToRecoverBadDisk] read dir(%v) err(%v).", disk.Path, err)
		return
	}
	go func() {
		for _, fileInfo := range fileInfoList {
			filename := fileInfo.Name()
			if !disk.isBackupPartitionDirToDelete(filename) {
				continue
			}
			os.RemoveAll(path.Join(disk.Path, filename))
		}
	}()
	log.LogInfof("action[handlePacketToOpDeleteBackupDirectories]  delete backup directories in  disk (%v) "+
		"run async", request.DiskPath)
}
