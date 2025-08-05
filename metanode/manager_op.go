// Copyright 2018 The The CubeFS Authors.
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

package metanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/datanode/storage"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

const (
	MaxUsedMemFactor = 1.1
)

func (m *metadataManager) checkFollowerRead(volNames []string, partition MetaPartition) {
	volName := partition.GetVolName()
	for _, name := range volNames {
		if name == volName {
			partition.SetFollowerRead(true)
			return
		}
	}
	partition.SetFollowerRead(false)
}

func (m *metadataManager) checkVolForbidWriteOpOfProtoVer0(partition MetaPartition) {
	mpVolName := partition.GetVolName()
	oldVal := partition.IsForbidWriteOpOfProtoVer0()
	VolsForbidWriteOpOfProtoVer0 := m.metaNode.VolsForbidWriteOpOfProtoVer0
	if _, ok := VolsForbidWriteOpOfProtoVer0[mpVolName]; ok {
		partition.SetForbidWriteOpOfProtoVer0(true)
	} else {
		partition.SetForbidWriteOpOfProtoVer0(false)
	}
	newVal := partition.IsForbidWriteOpOfProtoVer0()
	if oldVal != newVal {
		log.LogWarnf("[checkVolForbidWriteOpOfProtoVer0] vol(%v) mpId(%v) IsForbidWriteOpOfProtoVer0 change to %v",
			mpVolName, partition.GetBaseConfig().PartitionId, newVal)
	}
}

func (m *metadataManager) isVolForbidWriteOpOfProtoVer0(volName string) (forbid bool) {
	VolsForbidWriteOpOfProtoVer0 := m.metaNode.VolsForbidWriteOpOfProtoVer0
	if _, ok := VolsForbidWriteOpOfProtoVer0[volName]; ok {
		forbid = true
	} else {
		forbid = false
	}

	return
}

func (m *metadataManager) checkForbiddenVolume(volNames []string, partition MetaPartition) {
	volName := partition.GetVolName()
	for _, name := range volNames {
		if name == volName {
			partition.SetForbidden(true)
			return
		}
	}
	partition.SetForbidden(false)
}

func (m *metadataManager) checkDisableAuditLogVolume(volNames []string, partition MetaPartition) {
	volName := partition.GetVolName()
	for _, name := range volNames {
		if name == volName {
			partition.SetEnableAuditLog(false)
			return
		}
	}
	partition.SetEnableAuditLog(true)
}

func (m *metadataManager) opMasterHeartbeat(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	// For ack to master
	data := p.Data
	m.responseAckOKToMaster(conn, p)

	var (
		req       = &proto.HeartBeatRequest{}
		resp      = &proto.MetaNodeHeartbeatResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
		volsForbidWriteOpOfProtoVer0 = make(map[string]struct{})
	)
	start := time.Now()
	go func() {
		decode := json.NewDecoder(bytes.NewBuffer(data))
		decode.UseNumber()
		if err = decode.Decode(adminTask); err != nil {
			resp.Status = proto.TaskFailed
			resp.Result = err.Error()
			goto end
		}
		m.fileStatsEnable = req.FileStatsEnable

		log.LogDebugf("metaNode.raftPartitionCanUsingDifferentPort from %v to %v", m.metaNode.raftPartitionCanUsingDifferentPort, req.RaftPartitionCanUsingDifferentPortEnabled)
		m.metaNode.raftPartitionCanUsingDifferentPort = req.RaftPartitionCanUsingDifferentPortEnabled

		if m.metaNode.nodeForbidWriteOpOfProtoVer0 != req.NotifyForbidWriteOpOfProtoVer0 {
			log.LogWarnf("[opMasterHeartbeat] change nodeForbidWriteOpOfProtoVer0, old(%v) new(%v)",
				m.metaNode.nodeForbidWriteOpOfProtoVer0, req.NotifyForbidWriteOpOfProtoVer0)
			m.metaNode.nodeForbidWriteOpOfProtoVer0 = req.NotifyForbidWriteOpOfProtoVer0
		}

		for _, vol := range req.VolsForbidWriteOpOfProtoVer0 {
			if _, ok := volsForbidWriteOpOfProtoVer0[vol]; !ok {
				volsForbidWriteOpOfProtoVer0[vol] = struct{}{}
			}
		}
		m.metaNode.VolsForbidWriteOpOfProtoVer0 = volsForbidWriteOpOfProtoVer0
		log.LogDebugf("[opMasterHeartbeat] from master, volumes forbid write operate of proto version-0: %v",
			req.VolsForbidWriteOpOfProtoVer0)

		// collect memory info
		resp.Total = configTotalMem
		resp.Used, err = util.GetProcessMemory(os.Getpid())
		if err != nil {
			adminTask.Status = proto.TaskFailed
			goto end
		}
		// set cpu util and io used in here
		resp.CpuUtil = m.cpuUtil.Load()

		m.Range(true, func(id uint64, partition MetaPartition) bool {
			m.checkFollowerRead(req.FLReadVols, partition)
			m.checkForbiddenVolume(req.ForbiddenVols, partition)
			m.checkVolForbidWriteOpOfProtoVer0(partition)
			m.checkDisableAuditLogVolume(req.DisableAuditVols, partition)
			partition.SetUidLimit(req.UidLimitInfo)
			partition.SetTxInfo(req.TxInfo)
			partition.setQuotaHbInfo(req.QuotaHbInfos)
			mConf := partition.GetBaseConfig()

			mpForbidWriteVer0 := partition.IsForbidWriteOpOfProtoVer0() || m.metaNode.nodeForbidWriteOpOfProtoVer0

			mpr := &proto.MetaPartitionReport{
				PartitionID:               mConf.PartitionId,
				Start:                     mConf.Start,
				End:                       mConf.End,
				Status:                    proto.ReadWrite,
				MaxInodeID:                mConf.Cursor,
				VolName:                   mConf.VolName,
				Size:                      partition.DataSize(),
				InodeCnt:                  uint64(partition.GetInodeTreeLen()),
				DentryCnt:                 uint64(partition.GetDentryTreeLen()),
				FreeListLen:               uint64(partition.GetFreeListLen()),
				UidInfo:                   partition.GetUidInfo(),
				QuotaReportInfos:          partition.getQuotaReportInfos(),
				StatByStorageClass:        partition.GetStatByStorageClass(),
				StatByMigrateStorageClass: partition.GetMigrateStatByStorageClass(),
				ForbidWriteOpOfProtoVer0:  mpForbidWriteVer0,
				LocalPeers:                mConf.Peers,
			}
			mpr.TxCnt, mpr.TxRbInoCnt, mpr.TxRbDenCnt = partition.TxGetCnt()

			if mConf.Cursor >= mConf.End {
				mpr.Status = proto.ReadOnly
			}
			if resp.Used > uint64(float64(resp.Total)*MaxUsedMemFactor) {
				mpr.Status = proto.ReadOnly
			}

			addr, isLeader := partition.IsLeader()
			if addr == "" {
				mpr.Status = proto.Unavailable
			}
			mpr.IsLeader = isLeader

			resp.MetaPartitionReports = append(resp.MetaPartitionReports, mpr)
			return true
		})
		resp.ZoneName = m.zoneName
		resp.ReceivedForbidWriteOpOfProtoVer0 = m.metaNode.nodeForbidWriteOpOfProtoVer0
		resp.Status = proto.TaskSucceeds
	end:
		adminTask.Request = nil
		adminTask.Response = resp
		m.respondToMaster(adminTask)
		if log.EnableInfo() {
			log.LogInfof("%s pkt %s, resp success req:%v; respAdminTask: %v, cost %s",
				remoteAddr, p.String(), req, adminTask, time.Since(start).String())
		}
	}()

	return
}

func (m *metadataManager) opCreateMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	defer func() {
		var buf []byte
		status := proto.OpOk
		if err != nil {
			status = proto.OpErr
			buf = []byte(err.Error())
		}
		p.PacketErrorWithBody(status, buf)
		m.respondToClientWithVer(conn, p)
	}()
	req := &proto.CreateMetaPartitionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		err = errors.NewErrorf("[opCreateMetaPartition]: Unmarshal AdminTask"+
			" struct: %s", err.Error())
		return
	}
	log.LogInfof("[%s] [remoteAddr=%s]accept a from"+
		" master message: %v", p.String(), remoteAddr, adminTask)
	// create a new meta partition.
	if err = m.createPartition(req); err != nil {
		err = errors.NewErrorf("[opCreateMetaPartition]->%s; request message: %v",
			err.Error(), adminTask.Request)
		return
	}
	log.LogInfof("%s [%s] create success req:%v; resp: %v", remoteAddr, p.String(),
		req, adminTask)
	return
}

// Handle OpCreate inode.
func (m *metadataManager) opCreateInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &CreateInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}

	err = mp.CreateInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	// reply the operation result to the client through TCP
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opCreateInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opQuotaCreateInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.QuotaCreateInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.QuotaCreateInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	// reply the operation result to the client through TCP
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opQuotaCreateInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxMetaLinkInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxLinkInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.TxCreateInodeLink(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opTxMetaLinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaLinkInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &LinkInodeReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.CreateInodeLink(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaLinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpCreate
func (m *metadataManager) opFreeInodeOnRaftFollower(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	mp, err := m.getPartition(p.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],err[%v]", p.GetOpMsgWithReqAndResult(), string(p.Data))
		return
	}
	mp.(*metaPartition).internalDelete(p.Data[:p.Size])
	p.PacketOkReply()
	m.respondToClientWithVer(conn, p)

	return
}

// Handle OpCreate
func (m *metadataManager) opTxCreateDentry(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxCreateDentryRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.TxCreateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)

	log.LogDebugf("%s [opTxCreateDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxCreate(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxCreateRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.TxCreate(req, p)
	m.respondToClient(conn, p)

	log.LogDebugf("%s [opTxCreate] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxGet(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxGetInfoRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.Pid)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.TxGetInfo(req, p)
	m.respondToClient(conn, p)

	if log.EnableDebug() {
		log.LogDebugf("%s [opTxGet] req: %d - %v, resp: %v, body: %s",
			remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	}
	return
}

func (m *metadataManager) opTxCommitRM(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxApplyRMRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.TxCommitRM(req, p)
	m.respondToClient(conn, p)

	log.LogDebugf("%s [opTxCommitRM] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxRollbackRM(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxApplyRMRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.TxRollbackRM(req, p)
	m.respondToClient(conn, p)

	log.LogDebugf("%s [opTxRollbackRM] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxCommit(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxApplyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.TmID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.TxCommit(req, p, remoteAddr)
	m.respondToClient(conn, p)

	log.LogDebugf("%s [opTxCommit] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxRollback(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxApplyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.TmID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.TxRollback(req, p, remoteAddr)
	m.respondToClient(conn, p)

	log.LogDebugf("%s [opTxRollback] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpCreate
func (m *metadataManager) opCreateDentry(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &CreateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.CreateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)

	log.LogDebugf("%s [opCreateDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opQuotaCreateDentry(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.QuotaCreateDentryRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.QuotaCreateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)

	log.LogDebugf("%s [opQuotaCreateDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpDelete Dentry
func (m *metadataManager) opTxDeleteDentry(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxDeleteDentryRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.TxDeleteDentry(req, p, remoteAddr)
	m.respondToClient(conn, p)
	if log.EnableDebug() {
		log.LogDebugf("%s [opTxDeleteDentry] req: %d - %v, resp: %v, body: %s",
			remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	}
	return
}

// Handle OpDelete Dentry
func (m *metadataManager) opDeleteDentry(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &DeleteDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}

	err = mp.DeleteDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle Op batch Delete Dentry
func (m *metadataManager) opBatchDeleteDentry(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &BatchDeleteDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}

	err = mp.DeleteDentryBatch(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxUpdateDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxUpdateDentryRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.TxUpdateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opTxUpdateDentry] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opUpdateDentry(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &UpdateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}

	err = mp.UpdateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opUpdateDentry] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxMetaUnlinkInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxUnlinkInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.TxUnlinkInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaUnlinkInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &UnlinkInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.UnlinkInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchUnlinkInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &BatchUnlinkInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.UnlinkInodeBatch(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opReadDirOnly(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.ReadDirOnlyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ReadDirOnly(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [%v]req: %v , resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpReadDir
func (m *metadataManager) opReadDir(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.ReadDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ReadDir(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [%v]req: %v , resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpReadDirLimit
func (m *metadataManager) opReadDirLimit(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.ReadDirLimitRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ReadDirLimit(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [%v]req: %v , resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaInodeGet(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &InodeGetReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("Unmarshal [%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	log.LogDebugf("action[opMetaInodeGet] request %v", req)
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("getPartition [%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.InodeGet(req, p); err != nil {
		err = errors.NewErrorf("InodeGet [%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		log.LogDebug(err)
	}

	if err = m.respondToClient(conn, p); err != nil {
		log.LogDebugf("%s [opMetaInodeGet] err [%v] req: %d - %v; resp: %v, body: %s",
			remoteAddr, err, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	}
	log.LogDebugf("%s [opMetaInodeGet] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)

	if value, ok := m.volUpdating.Load(req.VolName); ok {
		ver2Phase := value.(*verOp2Phase)
		if ver2Phase.verSeq > req.VerSeq {
			// reuse ExtentType to identify flag of version inconsistent between metanode and client
			// will resp to client and make client update all streamer's extent and it's verSeq
			p.ExtentType |= proto.MultiVersionFlag
			p.VerSeq = ver2Phase.verSeq
		}
	}

	return
}

func (m *metadataManager) opBatchMetaEvictInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.BatchEvictInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] request unmarshal: %v", p.GetOpMsgWithReqAndResult(), err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	if err = mp.EvictInodeBatch(req, p, remoteAddr); err != nil {
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	}
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opBatchMetaEvictInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaEvictInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.EvictInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	if err = mp.EvictInode(req, p, remoteAddr); err != nil {
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	}
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaEvictInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opSetAttr(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &SetattrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	if err = mp.SetAttr(req, p.Data, p); err != nil {
		err = errors.NewErrorf("[opSetAttr] req: %v, error: %s", req, err.Error())
	}
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opSetAttr] req: %d - %v, resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Lookup request
func (m *metadataManager) opMetaLookup(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.LookupRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	if mp.IsForbidden() {
		err = storage.ForbiddenMetaPartitionError
		p.PacketErrorWithBody(proto.OpForbidErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.Lookup(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaLookup] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsAdd(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.AppendExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.ExtentAppend(req, p)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	if err != nil {
		log.LogErrorf("%s [opMetaExtentsAdd] ExtentAppend: %s, "+
			"response to client: %s", remoteAddr, err.Error(), p.GetResultMsg())
	}
	log.LogDebugf("%s [opMetaExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Append one extent with discard check
func (m *metadataManager) opMetaExtentAddWithCheck(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.AppendExtentKeyWithCheckRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}

	if err = mp.ExtentAppendWithCheck(req, p); err != nil {
		log.LogErrorf("%s [opMetaExtentAddWithCheck] ExtentAppendWithCheck: %s", remoteAddr, err.Error())
	}
	m.updatePackRspSeq(mp, p)
	if err = m.respondToClientWithVer(conn, p); err != nil {
		log.LogErrorf("%s [opMetaExtentAddWithCheck] ExtentAppendWithCheck: %s, "+
			"response to client: %s", remoteAddr, err.Error(), p.GetResultMsg())
	}
	log.LogDebugf("%s [opMetaExtentAddWithCheck] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsList(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.GetExtentsRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	log.LogDebugf("opMetaExtentsList: id(%v) req(%v) ", p.GetReqID(), req)
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.ExtentsList(req, p)
	m.respondToClient(conn, p)
	if log.EnableDebug() {
		log.LogDebugf("%s [opMetaExtentsList] req: %d - %v; resp: %v, body: %s",
			remoteAddr, p.GetReqID(), req, p.GetResultMsg(), log.TruncMsg(string(p.Data)))
	}
	return
}

func (m *metadataManager) opMetaObjExtentsList(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.GetExtentsRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.ObjExtentsList(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaObjExtentsList] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsDel(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	panic("not implemented yet")
	// req := &proto.DelExtentKeyRequest{}
	// if err = json.Unmarshal(p.Data, req); err != nil {
	// 	p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
	// 	m.respondToClientWithVer(conn, p)
	// 	err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	// 	return
	// }
	// mp, err := m.getPartition(req.PartitionID)
	// if err != nil {
	// 	p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
	// 	m.respondToClientWithVer(conn, p)
	// 	err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	// 	return
	// }
	// if !m.serveProxy(conn, mp, p) {
	// 	return
	// }
	// mp.ExtentsDelete(req, p)
	// m.respondToClientWithVer(conn, p)
	// log.LogDebugf("%s [OpMetaTruncate] req: %d - %v, resp body: %v, "+
	// 	"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	// return
}

func (m *metadataManager) opMetaExtentsTruncate(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &ExtentsTruncateReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	if err = m.checkForbidWriteOpOfProtoVer0(p.ProtoVersion, mp.IsForbidWriteOpOfProtoVer0()); err != nil {
		log.LogWarnf("[opMetaExtentsTruncate] reqId(%v) mpId(%v) ino(%v) err: %v", p.ReqID, req.PartitionID, req.Inode, err)
		p.PacketErrorWithBody(proto.OpWriteOpOfProtoVerForbidden, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}

	if err = mp.ExtentsTruncate(req, p, remoteAddr); err != nil {
		log.LogErrorf("[opMetaExtentsTruncate] mpId(%v) ino(%v) err: %v", req.PartitionID, req.Inode, err)
	}

	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaExtentsTruncate] req: %d - %v, resp body: %v, "+
		"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaClearInodeCache(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.ClearInodeCacheRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ClearInodeCache(req, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaClearInodeCache] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Delete a meta partition.
func (m *metadataManager) opDeleteMetaPartition(conn net.Conn,
	p *Packet, remoteAddr string,
) (err error) {
	req := &proto.DeleteMetaPartitionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketOkReply()
		m.respondToClientWithVer(conn, p)
		return nil
	}
	// Ack the master request
	conf := mp.GetBaseConfig()
	mp.Stop()
	mp.DeleteRaft()
	m.deletePartition(mp.GetBaseConfig().PartitionId)
	os.RemoveAll(conf.RootDir)
	p.PacketOkReply()
	m.respondToClientWithVer(conn, p)
	runtime.GC()
	log.LogInfof("%s [opDeleteMetaPartition] req: %d - %v, resp: %v",
		remoteAddr, p.GetReqID(), req, err)
	return
}

func (m *metadataManager) opUpdateMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := new(UpdatePartitionReq)
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	m.responseAckOKToMaster(conn, p)
	resp := &UpdatePartitionResp{
		VolName:     req.VolName,
		PartitionID: req.PartitionID,
		End:         req.End,
	}
	err = mp.UpdatePartition(req, resp)
	adminTask.Response = resp
	adminTask.Request = nil
	m.respondToMaster(adminTask)
	log.LogInfof("%s [opUpdateMetaPartition] req[%v], response[%v].",
		remoteAddr, req, adminTask)
	return
}

func (m *metadataManager) opLoadMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.MetaPartitionLoadRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if err = mp.ResponseLoadMetaPartition(p); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		log.LogErrorf("%s [opLoadMetaPartition] req[%v], "+
			"response marshal[%v]", remoteAddr, req, err.Error())
		m.respondToClient(conn, p)
		return
	}
	m.respondToClient(conn, p)
	log.LogInfof("%s [opLoadMetaPartition] req[%v], response status[%s], "+
		"response body[%s], error[%v]", remoteAddr, req, p.GetResultMsg(), p.Data,
		err)
	return
}

func (m *metadataManager) opDecommissionMetaPartition(conn net.Conn,
	p *Packet, remoteAddr string,
) (err error) {
	var reqData []byte
	req := &proto.MetaPartitionDecommissionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return err
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return err
	}
	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.NewErrorf("[opDecommissionMetaPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opDecommissionMetaPartition]: partitionID= %d, "+
			"Marshal %s", req.PartitionID, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return err
	}
	_, err = mp.ChangeMember(raftProto.ConfRemoveNode,
		raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClientWithVer(conn, p)

	return
}

func (m *metadataManager) opAddMetaPartitionRaftMember(conn net.Conn,
	p *Packet, remoteAddr string,
) (err error) {
	var reqData []byte
	req := &proto.AddMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}

	defer func() {
		if err != nil {
			log.LogInfof("pkt %s remote %s reqId add raft member failed, req %v, err %s", p.String(), remoteAddr, adminTask, err.Error())
			return
		}

		log.LogInfof("pkt %s, remote %s add raft member success, req %v", p.String(), remoteAddr, adminTask)
	}()

	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return err
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpTryOtherAddr, ([]byte)(proto.ErrMetaPartitionNotExists.Error()))
		m.respondToClientWithVer(conn, p)
		return err
	}

	if mp.IsExsitPeer(req.AddPeer) {
		p.PacketOkReply()
		m.respondToClientWithVer(conn, p)
		return
	}

	log.LogInfof("[%s], remote %s start add raft member, req %v", p.String(), remoteAddr, adminTask)

	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opAddMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}
	if req.AddPeer.ID == 0 {
		err = errors.NewErrorf("[opAddMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali AddPeerID %v", req.AddPeer.ID))
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClientWithVer(conn, p)
	return
}

func (m *metadataManager) opRemoveMetaPartitionRaftMember(conn net.Conn,
	p *Packet, remoteAddr string,
) (err error) {
	var reqData []byte
	req := &proto.RemoveMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}

	defer func() {
		if err != nil {
			log.LogInfof("[%s], remote %s remove raft member failed, req %v, err %s", p.String(), remoteAddr, adminTask, err.Error())
			return
		}

		log.LogInfof("[%s], remote %s remove raft member success, req %v", p.String(), remoteAddr, adminTask)
	}()

	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}

	log.LogInfof("[%s], remote %s remove raft member success, req %v", p.String(), remoteAddr, adminTask)

	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}

	if !mp.IsExsitPeer(req.RemovePeer) {
		p.PacketOkReply()
		m.respondToClient(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if err = mp.CanRemoveRaftMember(req.RemovePeer); err != nil {
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, RemovePeerID %d, err %s",
			req.PartitionId, req.RemovePeer.ID, err.Error())
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if req.RemovePeer.ID == 0 {
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali RemovePeerID %v", req.RemovePeer.ID))
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfRemoveNode,
		raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

func (m *metadataManager) opMetaBatchInodeGet(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.BatchInodeGetRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	log.LogDebugf("action[opMetaBatchInodeGet] req %v", req)
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.InodeGetBatch(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchInodeGet] req: %d - %v, resp: %v, "+
		"body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaPartitionTryToLeader(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	mp, err := m.getPartition(p.PartitionID)
	if err != nil {
		goto errDeal
	}
	if err = mp.TryToLeader(p.PartitionID); err != nil {
		goto errDeal
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)
	return
errDeal:
	p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
	m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opMetaDeleteInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.DeleteInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteInode(req, p, remoteAddr)
	_ = m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchDeleteInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	var req *proto.DeleteInodeBatchRequest
	if err = json.Unmarshal(p.Data, &req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteInodeBatch(req, p, remoteAddr)
	log.LogDebugf("%s [opMetaDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)

	_ = m.respondToClientWithVer(conn, p)

	return
}

func (m *metadataManager) opMetaUpdateXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.UpdateXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.UpdateXAttr(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaSetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaSetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.SetXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.SetXAttr(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaSetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchSetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.BatchSetXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.BatchSetXAttr(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [OpMetaBatchSetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaGetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.GetXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.GetXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaLockDir(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.LockDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	start := time.Now()
	if mp.IsEnableAuditLog() {
		defer func() {
			err1 := fmt.Errorf("data(%s)_err(%v)_status(%s)", p.Data, err, p.GetResultMsg())
			auditlog.LogInodeOp(remoteAddr, "", p.GetOpMsg(), req.String(), err1, time.Since(start).Milliseconds(), req.Inode, 0)
		}()
	}

	err = mp.LockDir(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaLockDir] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaGetAllXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.GetAllXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.GetAllXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchGetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.BatchGetXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.BatchGetXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchGetXAttr req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaRemoveXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.RemoveXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.RemoveXAttr(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaListXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ListXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ListXAttr(req, p)
	_ = m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchExtentsAdd(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.AppendExtentKeysRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	err = mp.BatchExtentAppend(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaBatchExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchObjExtentsAdd(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.AppendObjExtentKeysRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.BatchObjExtentAppend(req, p)
	_ = m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaBatchObjExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opCreateMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.CreateMultipartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateMultipart(req, p)
	_ = m.respondToClientWithVer(conn, p)
	return
}

func (m *metadataManager) opRemoveMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.RemoveMultipartRequest{}

	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.RemoveMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opGetExpiredMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.GetExpiredMultipartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opGetExpiredMultipart] req: %v, resp: %v", req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opGetMultipart] req: %v, resp: %v", req, err.Error())
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.GetExpiredMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opGetMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.GetMultipartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opGetMultipart] req: %v, resp: %v", req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opGetMultipart] req: %v, resp: %v", req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.GetMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opAppendMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.AddMultipartPartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.AppendMultipart(req, p)
	_ = m.respondToClientWithVer(conn, p)
	return
}

func (m *metadataManager) opListMultipart(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ListMultipartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opListMultipart] req: %v, resp: %v", req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opListMultipart] req: %v, resp: %v", req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.ListMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

// Handle OpMetaTxCreateInode inode.
func (m *metadataManager) opTxCreateInode(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.TxCreateInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		m.respondToClientWithVer(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.TxCreateInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opTxCreateInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchSetInodeQuota(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.BatchSetMetaserverQuotaReuqest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaBatchSetInodeQuota] req: %v, resp: %v", req, err.Error())
		return
	}
	log.LogInfof("[opMetaBatchSetInodeQuota] req [%v] decode req.", req)
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaBatchSetInodeQuota] req: %v, resp: %v", req, err.Error())
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	resp := &proto.BatchSetMetaserverQuotaResponse{}
	err = mp.batchSetInodeQuota(req, resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	var reply []byte
	if reply, err = json.Marshal(resp); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	p.PacketOkWithBody(reply)
	_ = m.respondToClient(conn, p)
	log.LogInfof("[opMetaBatchSetInodeQuota] req [%v] resp [%v] success.", req, resp)
	return
}

func (m *metadataManager) opMetaBatchDeleteInodeQuota(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.BatchDeleteMetaserverQuotaReuqest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaBatchDeleteInodeQuota] req: %v, resp: %v", req, err.Error())
		return
	}
	log.LogInfof("[opMetaBatchDeleteInodeQuota] req [%v] decode req.", req)
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaBatchDeleteInodeQuota] req: %v, resp: %v", req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	resp := &proto.BatchDeleteMetaserverQuotaResponse{}
	err = mp.batchDeleteInodeQuota(req, resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	var reply []byte
	if reply, err = json.Marshal(resp); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	p.PacketOkWithBody(reply)
	_ = m.respondToClient(conn, p)
	log.LogInfof("[opMetaBatchDeleteInodeQuota] req [%v] resp [%v] success.", req, resp)
	return err
}

func (m *metadataManager) opMetaGetInodeQuota(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.GetInodeQuotaRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opGetMultipart] req: %v, resp: %v", req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opGetMultipart] req: %v, resp: %v", req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.getInodeQuota(req.Inode, p)
	_ = m.respondToClient(conn, p)
	log.LogInfof("[opMetaGetInodeQuota] get inode[%v] quota success.", req.Inode)
	return
}

func (m *metadataManager) opMetaGetAppliedID(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.GetAppliedIDRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaGetAppliedID] req: %v, resp: %v", req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaGetAppliedID] req: %v, resp: %v", req, err.Error())
		return
	}

	appliedID := mp.GetAppliedID()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedID)
	p.PacketOkWithBody(buf)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetAppliedID] req: %d - %v, req args: %v, resp: %v, body: %v",
		remote, p.GetReqID(), req, string(p.Arg), p.GetResultMsg(), appliedID)
	return
}

func (m *metadataManager) opMetaGetUniqID(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.GetUniqIDRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.GetUniqID(p, req.Num)
	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("%s [opMetaGetUniqID] %s, "+
			"response to client: %s", remoteAddr, err.Error(), p.GetResultMsg())
	}
	log.LogDebugf("%s [opMetaGetUniqID] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) prepareCreateVersion(req *proto.MultiVersionOpRequest) (err error, opAagin bool) {
	var ver2Phase *verOp2Phase
	if value, ok := m.volUpdating.Load(req.VolumeID); ok {
		ver2Phase = value.(*verOp2Phase)
		if req.VerSeq < ver2Phase.verSeq {
			err = fmt.Errorf("seq [%v] create less than loal %v", req.VerSeq, ver2Phase.verSeq)
			return
		} else if req.VerSeq == ver2Phase.verPrepare {
			if ver2Phase.status == proto.VersionWorking {
				opAagin = true
				return
			}
		}
	}
	ver2Phase = &verOp2Phase{}
	ver2Phase.step = uint32(req.Op)
	ver2Phase.status = proto.VersionWorking
	ver2Phase.verPrepare = req.VerSeq

	m.volUpdating.Store(req.VolumeID, ver2Phase)

	log.LogWarnf("action[prepareCreateVersion] volume %v update to ver [%v] step %v",
		req.VolumeID, req.VerSeq, ver2Phase.step)
	return
}

func (m *metadataManager) checkVolVerList() (err error) {
	volumeArr := make(map[string]bool)

	log.LogDebugf("checkVolVerList start")
	m.Range(true, func(id uint64, partition MetaPartition) bool {
		volumeArr[partition.GetVolName()] = true
		return true
	})

	for volName := range volumeArr {
		mpsVerlist := make(map[uint64]*proto.VolVersionInfoList)
		// need get first or else the mp verlist may be change in the follower process
		m.Range(true, func(id uint64, partition MetaPartition) bool {
			if partition.GetVolName() != volName {
				return true
			}
			log.LogDebugf("action[checkVolVerList] volumeName %v id[%v] dp verlist %v partition.GetBaseConfig().PartitionId %v",
				volName, id, partition.GetVerList(), partition.GetBaseConfig().PartitionId)
			mpsVerlist[id] = &proto.VolVersionInfoList{VerList: partition.GetVerList()}
			return true
		})
		var info *proto.VolVersionInfoList
		if info, err = masterClient.AdminAPI().GetVerList(volName); err != nil {
			log.LogErrorf("action[checkVolVerList] volumeName %v err %v", volName, err)
			return
		}

		log.LogDebugf("action[checkVolVerList] volumeName %v info %v", volName, info)
		m.Range(true, func(id uint64, partition MetaPartition) bool {
			if partition.GetVolName() != volName {
				return true
			}
			log.LogDebugf("action[checkVolVerList] volumeName %v info %v id[%v] ", volName, info, id)
			if _, exist := mpsVerlist[id]; exist {
				if err = partition.checkByMasterVerlist(mpsVerlist[id], info); err != nil {
					return true
				}
			}
			if _, err = partition.checkVerList(info, false); err != nil {
				log.LogErrorf("[checkVolVerList] volumeName %v err %v", volName, err)
			}
			return true
		})
	}
	return
}

func (m *metadataManager) commitCreateVersion(VolumeID string, VerSeq uint64, Op uint8, synchronize bool) (err error) {
	log.LogWarnf("action[commitCreateVersion] volume %v seq [%v]", VolumeID, VerSeq)
	var wg sync.WaitGroup
	// wg.Add(len(m.partitions))
	resultCh := make(chan error, len(m.partitions))
	m.Range(true, func(id uint64, partition MetaPartition) bool {
		if partition.GetVolName() != VolumeID {
			return true
		}

		if _, ok := partition.IsLeader(); !ok {
			return true
		}

		wg.Add(1)
		go func(mpId uint64, mp MetaPartition) {
			defer wg.Done()
			log.LogInfof("action[commitCreateVersion] volume %v mp  %v do HandleVersionOp verseq [%v]", VolumeID, mpId, VerSeq)
			if err := mp.HandleVersionOp(Op, VerSeq, nil, synchronize); err != nil {
				log.LogErrorf("action[commitCreateVersion] volume %v mp  %v do HandleVersionOp verseq [%v] err %v", VolumeID, mpId, VerSeq, err)
				resultCh <- err
				return
			}
		}(id, partition)
		return true
	})

	wg.Wait()
	select {
	case err = <-resultCh:
		if err != nil {
			close(resultCh)
			return
		}
	default:
		log.LogInfof("action[commitCreateVersion] volume %v do HandleVersionOp verseq [%v] finished", VolumeID, VerSeq)
	}
	close(resultCh)

	if Op == proto.DeleteVersion {
		return
	}
	if Op == proto.CreateVersionPrepare {
		return
	}

	if value, ok := m.volUpdating.Load(VolumeID); ok {
		ver2Phase := value.(*verOp2Phase)
		log.LogWarnf("action[commitCreateVersion] try commit volume %v prepare seq [%v] with commit seq [%v]",
			VolumeID, ver2Phase.verPrepare, VerSeq)
		if VerSeq < ver2Phase.verSeq {
			err = fmt.Errorf("volname [%v] seq [%v] create less than loal %v", VolumeID, VerSeq, ver2Phase.verSeq)
			log.LogErrorf("action[commitCreateVersion] err %v", err)
			return
		}
		if ver2Phase.step != proto.CreateVersionPrepare {
			err = fmt.Errorf("volname [%v] step not prepare", VolumeID)
			log.LogErrorf("action[commitCreateVersion] err %v", err)
			return
		}
		ver2Phase.verSeq = VerSeq
		ver2Phase.step = proto.CreateVersionCommit
		ver2Phase.status = proto.VersionWorkingFinished
		log.LogWarnf("action[commitCreateVersion] commit volume %v prepare seq [%v] with commit seq [%v]",
			VolumeID, ver2Phase.verPrepare, VerSeq)
		return
	}

	err = fmt.Errorf("volname [%v] not found", VolumeID)
	log.LogErrorf("action[commitCreateVersion] err %v", err)

	return
}

func (m *metadataManager) updatePackRspSeq(mp MetaPartition, p *Packet) {
	if !m.metaNode.clusterEnableSnapshot {
		return
	}
	if mp.GetVerSeq() > p.VerSeq {
		log.LogDebugf("action[checkmultiSnap.multiVersionstatus] mp ver [%v], packet ver [%v]", mp.GetVerSeq(), p.VerSeq)
		p.VerSeq = mp.GetVerSeq() // used to response to client and try update verSeq of client
		p.ExtentType |= proto.VersionListFlag
		p.VerList = make([]*proto.VolVersionInfo, len(mp.GetVerList()))
		copy(p.VerList, mp.GetVerList())
	}
}

func (m *metadataManager) checkMultiVersionStatus(mp MetaPartition, p *Packet) (err error) {
	if !m.metaNode.clusterEnableSnapshot {
		return
	}
	if (p.ExtentType&proto.MultiVersionFlag == 0) && mp.GetVerSeq() > 0 {
		log.LogWarnf("action[checkmultiSnap.multiVersionstatus] volname [%v] mp ver [%v], client use old ver before snapshot", mp.GetVolName(), mp.GetVerSeq())
		return fmt.Errorf("client use old ver before snapshot")
	}
	// meta node do not need to check verSeq as strictly as datanode,file append or modAppendWrite on meta node is invisible to other files.
	// only need to guarantee the verSeq wrote on meta nodes grow up linearly on client's angle
	log.LogDebugf("action[checkmultiSnap.multiVersionstatus] mp[%v] ver [%v], packet ver [%v] reqId %v", mp.GetBaseConfig().PartitionId, mp.GetVerSeq(), p.VerSeq, p.ReqID)
	if mp.GetVerSeq() >= p.VerSeq {
		if mp.GetVerSeq() > p.VerSeq {
			log.LogDebugf("action[checkmultiSnap.multiVersionstatus] mp ver [%v], packet ver [%v]", mp.GetVerSeq(), p.VerSeq)
			p.VerSeq = mp.GetVerSeq() // used to response to client and try update verSeq of client
			p.ExtentType |= proto.VersionListFlag
			p.VerList = make([]*proto.VolVersionInfo, len(mp.GetVerList()))
			copy(p.VerList, mp.GetVerList())
		}
		return
	}
	if p.IsVersionList() {
		_, err = mp.checkVerList(&proto.VolVersionInfoList{VerList: p.VerList}, true)
		return
	}
	p.ResultCode = proto.OpAgainVerionList
	// need return and tell client
	err = fmt.Errorf("volname [%v] req seq [%v] but not found commit status", mp.GetVolName(), p.VerSeq)
	if value, ok := m.volUpdating.Load(mp.GetVolName()); ok {
		ver2Phase := value.(*verOp2Phase)
		if ver2Phase.isActiveReqToMaster {
			return
		}
	}

	select {
	case m.verUpdateChan <- mp.GetVolName():
	default:
		log.LogWarnf("channel is full, volname [%v] not be queued", mp.GetVolName())
	}
	return
}

func (m *metadataManager) checkAndPromoteVersion(volName string) (err error) {
	log.LogInfof("action[checkmultiSnap.multiVersionstatus] volumeName %v", volName)
	var info *proto.VolumeVerInfo
	if value, ok := m.volUpdating.Load(volName); ok {
		ver2Phase := value.(*verOp2Phase)

		if atomic.LoadUint32(&ver2Phase.status) != proto.VersionWorkingAbnormal &&
			atomic.LoadUint32(&ver2Phase.step) == proto.CreateVersionPrepare {

			ver2Phase.Lock() // here trylock may be better after go1.18 adapted to compile
			defer ver2Phase.Unlock()

			// check again in case of sth already happened by other goroutine during be blocked by lock
			if atomic.LoadUint32(&ver2Phase.status) == proto.VersionWorkingAbnormal ||
				atomic.LoadUint32(&ver2Phase.step) != proto.CreateVersionPrepare {

				log.LogWarnf("action[checkmultiSnap.multiVersionstatus] volumeName %v status [%v] step %v",
					volName, atomic.LoadUint32(&ver2Phase.status), atomic.LoadUint32(&ver2Phase.step))
				return
			}

			if info, err = masterClient.AdminAPI().GetVerInfo(volName); err != nil {
				log.LogErrorf("action[checkmultiSnap.multiVersionstatus] volumeName %v status [%v] step %v err %v",
					volName, atomic.LoadUint32(&ver2Phase.status), atomic.LoadUint32(&ver2Phase.step), err)
				return
			}
			if info.VerSeqPrepare != ver2Phase.verPrepare {
				atomic.StoreUint32(&ver2Phase.status, proto.VersionWorkingAbnormal)
				err = fmt.Errorf("volumeName %v status [%v] step %v",
					volName, atomic.LoadUint32(&ver2Phase.status), atomic.LoadUint32(&ver2Phase.step))
				log.LogErrorf("action[checkmultiSnap.multiVersionstatus] err %v", err)
				return
			}
			if info.VerPrepareStatus == proto.CreateVersionCommit {
				if err = m.commitCreateVersion(volName, info.VerSeqPrepare, proto.CreateVersionCommit, false); err != nil {
					log.LogErrorf("action[checkmultiSnap.multiVersionstatus] err %v", err)
					return
				}
			}
		}
	} else {
		log.LogErrorf("action[checkmultiSnap.multiVersionstatus] volumeName %v not found", volName)
	}
	return
}

func (m *metadataManager) opMultiVersionOp(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	// For ack to master
	m.responseAckOKToMaster(conn, p)

	var (
		opAgain   bool
		start     = time.Now()
		data      = p.Data
		req       = &proto.MultiVersionOpRequest{}
		resp      = &proto.MultiVersionOpResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
		decode = json.NewDecoder(bytes.NewBuffer(data))
	)

	if !m.metaNode.clusterEnableSnapshot {
		err = fmt.Errorf("cluster not EnableSnapshot")
		log.LogErrorf("opMultiVersionOp volume %v", err)
		goto end
	}

	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		log.LogErrorf("action[opMultiVersionOp] %v mp  err %v do Decoder", req.VolumeID, err.Error())
		goto end
	}
	log.LogDebugf("action[opMultiVersionOp] volume %v op [%v]", req.VolumeID, req.Op)

	resp.Status = proto.TaskSucceeds
	resp.VolumeID = req.VolumeID
	resp.Addr = req.Addr
	resp.VerSeq = req.VerSeq
	resp.Op = req.Op

	if req.Op == proto.CreateVersionPrepare {
		if err, opAgain = m.prepareCreateVersion(req); err != nil || opAgain {
			log.LogErrorf("action[opMultiVersionOp] %v mp  err %v do Decoder", req.VolumeID, err)
			goto end
		}
		if err = m.commitCreateVersion(req.VolumeID, req.VerSeq, req.Op, true); err != nil {
			log.LogErrorf("action[opMultiVersionOp] %v mp  err %v do commitCreateVersion", req.VolumeID, err.Error())
			goto end
		}
	} else if req.Op == proto.CreateVersionCommit || req.Op == proto.DeleteVersion {
		if err = m.commitCreateVersion(req.VolumeID, req.VerSeq, req.Op, false); err != nil {
			log.LogErrorf("action[opMultiVersionOp] %v mp  err %v do commitCreateVersion", req.VolumeID, err.Error())
			goto end
		}
	}
end:
	if err != nil {
		resp.Result = err.Error()
	}
	adminTask.Request = nil
	adminTask.Response = resp
	if errRsp := m.respondToMaster(adminTask); errRsp != nil {
		log.LogInfof("action[opMultiVersionOp] %s pkt %s, resp success req:%v; respAdminTask: %v, resp: %v, errRsp %v err %v",
			remoteAddr, p.String(), req, adminTask, resp, errRsp, err)
	}

	if log.EnableInfo() {
		rspData, _ := json.Marshal(resp)
		log.LogInfof("action[opMultiVersionOp] %s pkt %s, resp success req:%v; respAdminTask: %v, resp: %v, cost %s",
			remoteAddr, p.String(), req, adminTask, string(rspData), time.Since(start).String())
	}

	return
}

func (m *metadataManager) opMetaInodeAccessTimeGet(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &InodeGetReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.InodeGetAccessTime(req, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaInodeAccessTimeGet] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaRenewalForbiddenMigration(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &RenewalForbiddenMigrationRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	err = mp.RenewalForbiddenMigration(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaRenewalForbiddenMigration] req: %d - %v, resp body: %v, "+
		"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaUpdateExtentKeyAfterMigration(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &UpdateExtentKeyAfterMigrationRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		err = fmt.Errorf("unmarshal req packet err: %v", err.Error())
		p.PacketErrorWithBody(proto.OpArgMismatchErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		err = fmt.Errorf("not found mpId(%v), err %s ", req.PartitionID, err.Error())
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = fmt.Errorf("mpId(%v) checkMultiVersionStatus err: %v", mp.GetBaseConfig().PartitionId, err.Error())
		p.PacketErrorWithBody(proto.OpArgMismatchErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	err = mp.UpdateExtentKeyAfterMigration(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opMetaUpdateExtentKeyAfterMigration] req: %d - %v, resp body: %v, "+
		"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opDeleteMigrationExtentKey(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &DeleteMigrationExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	err = mp.DeleteMigrationExtentKey(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	log.LogDebugf("%s [opDeleteMigrationExtentKey] req: %d - %v, resp body: %v, "+
		"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaUpdateInodeMeta(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.UpdateInodeMetaRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.UpdateInodeMeta(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [UpdateInodeMeta] err [%v] req: %d - %v; resp: %v, body: %s",
		remoteAddr, err, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opFreezeEmptyMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	req := &proto.FreezeMetaPartitionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}

	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}

	if req.Freeze && (mp.GetInodeTreeLen() != 0 || mp.GetDentryTreeLen() != 0) {
		err = errors.NewErrorf("inodeCount(%d) or dentryCount(%d) is not zero", mp.GetInodeTreeLen(), mp.GetDentryTreeLen())
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.SetFreeze(req)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}

	p.PacketOkReply()
	m.respondToClientWithVer(conn, p)
	log.LogInfof("%s [opFreezeEmptyMetaPartition] req: %d - %v, resp: %v",
		remoteAddr, p.GetReqID(), req, err)
	return
}

func (m *metadataManager) opBackupEmptyMetaPartition(conn net.Conn,
	p *Packet, remoteAddr string,
) (err error) {
	req := &proto.BackupMetaPartitionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketOkReply()
		m.respondToClientWithVer(conn, p)
		return nil
	}
	if mp.GetInodeTreeLen() != 0 || mp.GetDentryTreeLen() != 0 {
		err = errors.New("inode or dentry is not zero for delete operation")
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}

	// Ack the master request
	conf := mp.GetBaseConfig()
	mp.Stop()
	err = mp.CloseAndBackupRaft()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return
	}
	m.deletePartition(mp.GetBaseConfig().PartitionId)

	dirPath, dirName := path.Split(conf.RootDir)
	newName := dirPath + "del_" + dirName
	os.Rename(conf.RootDir, newName)

	p.PacketOkReply()
	m.respondToClientWithVer(conn, p)
	runtime.GC()
	log.LogInfof("%s [opDeleteMetaPartition] req: %d - %v, resp: %v",
		remoteAddr, p.GetReqID(), req, err)
	return
}

func (m *metadataManager) opRemoveBackupMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string,
) (err error) {
	entries, err := os.ReadDir(m.rootDir)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), DelMetaPartitionHdr) {
			idName := strings.TrimPrefix(entry.Name(), DelMetaPartitionHdr)
			id, err := strconv.ParseUint(idName, 10, 64)
			if err != nil {
				log.LogErrorf("Failed to parse meta partition(%s), error: %s", entry.Name(), err.Error())
				continue
			}
			err = m.raftStore.RemoveBackup(id)
			if err != nil {
				log.LogErrorf("Failed to remove raft backup meta partition(%s), error: %s", entry.Name(), err.Error())
				continue
			}
			removeDir := path.Join(m.rootDir, entry.Name())
			os.RemoveAll(removeDir)
		}
	}

	p.PacketOkReply()
	m.respondToClient(conn, p)
	return
}
