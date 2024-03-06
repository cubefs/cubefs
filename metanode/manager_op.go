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
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	blog "github.com/cubefs/cubefs/blobstore/util/log"
	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

const (
	MaxUsedMemFactor = 1.1
)

var errProxyFailed = errors.New("internal: op proxy failed")

func (m *metadataManager) opRequest(conn net.Conn, p *Packet, remoteAddr string,
	req interface{}, rVer bool, getpid func() uint64,
) (mp MetaPartition, err error) {
	mp, err = m.opRequestNoVer(conn, p, remoteAddr, req, rVer, getpid)
	if err != nil {
		return
	}
	if err = m.checkMultiVersionStatus(mp, p); err != nil {
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[CheckVersion],[%v],req[%v],err[%v]",
			p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	return
}

func (m *metadataManager) opRequestNoVer(conn net.Conn, p *Packet, remoteAddr string,
	req interface{}, rVer bool, getpid func() uint64,
) (mp MetaPartition, err error) {
	mp, err = m.opRequestLoad(conn, p, remoteAddr, req, rVer, getpid)
	if err != nil {
		return
	}
	if !m.serveProxy(conn, mp, p) {
		err = errProxyFailed
		return
	}
	return
}

func (m *metadataManager) opRequestNotFollower(conn net.Conn, p *Packet, remoteAddr string,
	req interface{}, rVer bool, getpid func() uint64,
) (mp MetaPartition, err error) {
	mp, err = m.opRequestLoad(conn, p, remoteAddr, req, rVer, getpid)
	if err != nil {
		return
	}
	if !mp.IsFollowerRead() && !m.serveProxy(conn, mp, p) {
		err = errProxyFailed
		return
	}
	return
}

func (m *metadataManager) opRequestLoad(conn net.Conn, p *Packet, remoteAddr string,
	req interface{}, rVer bool, getpid func() uint64,
) (mp MetaPartition, err error) {
	respond := m.respondToClient
	if rVer {
		respond = m.respondToClientWithVer
	}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorOpErr(err)
		respond(conn, p)
		err = errors.NewErrorf("[Unmarshal],[%v],req[%v],err[%v]",
			p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err = m.getPartition(getpid())
	if err != nil {
		p.PacketErrorOpErr(err)
		respond(conn, p)
		err = errors.NewErrorf("[Partition],[%v],req[%v],err[%v]",
			p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	return
}

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

func (m *metadataManager) opMasterHeartbeat(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	// For ack to master
	data := p.Data
	m.responseAckOKToMaster(conn, p)

	var (
		req       = &proto.HeartBeatRequest{}
		resp      = &proto.MetaNodeHeartbeatResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)

	ctx := p.Context()
	go func() {
		start := time.Now()
		decode := json.NewDecoder(bytes.NewBuffer(data))
		decode.UseNumber()
		if err = decode.Decode(adminTask); err != nil {
			resp.Status = proto.TaskFailed
			resp.Result = err.Error()
			goto end
		}
		m.fileStatsEnable = req.FileStatsEnable
		// collect memory info
		resp.Total = configTotalMem
		resp.MemUsed, err = util.GetProcessMemory(os.Getpid())
		if err != nil {
			adminTask.Status = proto.TaskFailed
			goto end
		}
		// set cpu util and io used in here
		resp.CpuUtil = m.cpuUtil.Load()

		m.Range(true, func(id uint64, partition MetaPartition) bool {
			m.checkFollowerRead(req.FLReadVols, partition)
			m.checkForbiddenVolume(req.ForbiddenVols, partition)
			m.checkDisableAuditLogVolume(req.DisableAuditVols, partition)
			partition.SetUidLimit(req.UidLimitInfo)
			partition.SetTxInfo(req.TxInfo)
			partition.setQuotaHbInfo(ctx, req.QuotaHbInfos)
			mConf := partition.GetBaseConfig()

			mpr := &proto.MetaPartitionReport{
				PartitionID:      mConf.PartitionId,
				Start:            mConf.Start,
				End:              mConf.End,
				Status:           proto.ReadWrite,
				MaxInodeID:       mConf.Cursor,
				VolName:          mConf.VolName,
				Size:             partition.DataSize(),
				InodeCnt:         uint64(partition.GetInodeTreeLen()),
				DentryCnt:        uint64(partition.GetDentryTreeLen()),
				FreeListLen:      uint64(partition.GetFreeListLen()),
				UidInfo:          partition.GetUidInfo(),
				QuotaReportInfos: partition.getQuotaReportInfos(ctx),
			}
			mpr.TxCnt, mpr.TxRbInoCnt, mpr.TxRbDenCnt = partition.TxGetCnt()

			if mConf.Cursor >= mConf.End {
				mpr.Status = proto.ReadOnly
			}
			if resp.MemUsed > uint64(float64(resp.Total)*MaxUsedMemFactor) {
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
		resp.Status = proto.TaskSucceeds
	end:
		adminTask.Request = nil
		adminTask.Response = resp
		m.respondToMaster(adminTask)
		p.Span().Infof("%s pkt %s, resp success req:%v; respAdminTask: %v, cost %s",
			remoteAddr, p.String(), req, adminTask, time.Since(start).String())
	}()

	return
}

func (m *metadataManager) opCreateMetaPartition(conn net.Conn, p *Packet, remoteAddr string) (err error) {
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
		err = errors.NewErrorf("[opCreateMetaPartition]: unmarshal AdminTask: %s", err.Error())
		return
	}
	span := p.Span()
	span.Infof("[%s] %s accept from master message: %v", p.String(), remoteAddr, adminTask)
	// create a new meta partition.
	if err = m.createPartition(p.Context(), req); err != nil {
		err = errors.NewErrorf("[opCreateMetaPartition]->%v; request message: %v", err, adminTask.Request)
		return
	}
	span.Infof("[%s] %s create success req: %v resp: %v", p.String(), remoteAddr, req, adminTask)
	return
}

// Handle OpCreate inode.
func (m *metadataManager) opCreateInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &CreateInoReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.CreateInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	// reply the operation result to the client through TCP
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opCreateInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opQuotaCreateInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.QuotaCreateInodeRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.QuotaCreateInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	// reply the operation result to the client through TCP
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opQuotaCreateInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxMetaLinkInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxLinkInodeRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxCreateInodeLink(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxMetaLinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaLinkInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &LinkInodeReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.CreateInodeLink(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaLinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpCreate
func (m *metadataManager) opFreeInodeOnRaftFollower(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	mp, err := m.getPartition(p.PartitionID)
	if err != nil {
		p.PacketErrorOpErr(err)
		m.respondToClientWithVer(conn, p)
		err = errors.NewErrorf("[%v],err[%v]", p.GetOpMsgWithReqAndResult(), string(p.Data))
		return
	}
	mp.(*metaPartition).internalDelete(p.Context(), p.Data[:p.Size])
	p.PacketOkReply()
	m.respondToClientWithVer(conn, p)
	return
}

// Handle OpCreate
func (m *metadataManager) opTxCreateDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxCreateDentryRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxCreateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxCreateDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxCreate(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxCreateRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxCreate(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxCreate] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxGet(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxGetInfoRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.Pid })
	if err != nil {
		return err
	}
	err = mp.TxGetInfo(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxGet] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxCommitRM(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxApplyRMRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxCommitRM(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxCommitRM] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxRollbackRM(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxApplyRMRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxRollbackRM(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxRollbackRM] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxCommit(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxApplyRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.TmID })
	if err != nil {
		return err
	}
	err = mp.TxCommit(req, p, remoteAddr)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxCommit] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxRollback(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxApplyRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.TmID })
	if err != nil {
		return err
	}
	err = mp.TxRollback(req, p, remoteAddr)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxRollback] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpCreate
func (m *metadataManager) opCreateDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &CreateDentryReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.CreateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opCreateDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opQuotaCreateDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.QuotaCreateDentryRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.QuotaCreateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opQuotaCreateDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpDelete Dentry
func (m *metadataManager) opTxDeleteDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxDeleteDentryRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxDeleteDentry(req, p, remoteAddr)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpDelete Dentry
func (m *metadataManager) opDeleteDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &DeleteDentryReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.DeleteDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle Op batch Delete Dentry
func (m *metadataManager) opBatchDeleteDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &BatchDeleteDentryReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.DeleteDentryBatch(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxUpdateDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxUpdateDentryRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxUpdateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opTxUpdateDentry] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opUpdateDentry(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &UpdateDentryReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.UpdateDentry(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opUpdateDentry] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opTxMetaUnlinkInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxUnlinkInodeRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxUnlinkInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opTxMetaUnlinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaUnlinkInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &UnlinkInoReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.UnlinkInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaUnlinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchUnlinkInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &BatchUnlinkInoReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.UnlinkInodeBatch(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaBatchUnlinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opReadDirOnly(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ReadDirOnlyRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.ReadDirOnly(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opReadDirOnly] req: %d - %v , resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpReadDir
func (m *metadataManager) opReadDir(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ReadDirRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.ReadDir(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opReadDir] req: %d - %v , resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpReadDirLimit
func (m *metadataManager) opReadDirLimit(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ReadDirLimitRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.ReadDirLimit(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opReadDirLimit] req: %d - %v , resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaInodeGet(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &InodeGetReq{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	if err = mp.InodeGet(req, p); err != nil {
		err = errors.NewErrorf("InodeGet [%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
	}

	span := p.Span()
	if err = m.respondToClient(conn, p); err != nil {
		span.Debugf("%s [opMetaInodeGet] err [%v] req: %d - %v; resp: %v, body: %s",
			remoteAddr, err, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	}
	span.Debugf("%s [opMetaInodeGet] req: %d - %v; resp: %v, body: %s",
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

func (m *metadataManager) opBatchMetaEvictInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.BatchEvictInodeRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	if err = mp.EvictInodeBatch(req, p, remoteAddr); err != nil {
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	}
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opBatchMetaEvictInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaEvictInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.EvictInodeRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	if err = mp.EvictInode(req, p, remoteAddr); err != nil {
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	}
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaEvictInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opSetAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &SetattrRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	if err = mp.SetAttr(req, p.Data, p); err != nil {
		err = errors.NewErrorf("[opSetAttr] req: %v, error: %s", req, err.Error())
	}
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opSetAttr] req: %d - %v, resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Lookup request
func (m *metadataManager) opMetaLookup(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.LookupRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.Lookup(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opMetaLookup] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsAdd(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.AppendExtentKeyRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.ExtentAppend(req, p)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	span := p.Span()
	if err != nil {
		span.Errorf("%s %s, response to client: %s", remoteAddr, err.Error(), p.GetResultMsg())
	}
	span.Debugf("%s [opMetaExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Append one extent with discard check
func (m *metadataManager) opMetaExtentAddWithCheck(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.AppendExtentKeyWithCheckRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	span := p.Span()
	if err = mp.ExtentAppendWithCheck(req, p); err != nil {
		span.Errorf("%s ExtentAppendWithCheck: %s", remoteAddr, err.Error())
	}
	m.updatePackRspSeq(mp, p)
	if err = m.respondToClientWithVer(conn, p); err != nil {
		span.Errorf("%s %s, response to client: %s", remoteAddr, err.Error(), p.GetResultMsg())
	}
	span.Debugf("%s [opMetaExtentAddWithCheck] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsList(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.GetExtentsRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.ExtentsList(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opMetaExtentsList] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), log.TruncMsg(string(p.Data)))
	return
}

func (m *metadataManager) opMetaObjExtentsList(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.GetExtentsRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.ObjExtentsList(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opMetaObjExtentsList] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsDel(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	panic("not implemented yet")
	// req := &proto.DelExtentKeyRequest{}
	// mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	// if err != nil {
	// 	return err
	// }
	// mp.ExtentsDelete(req, p)
	// m.respondToClientWithVer(conn, p)
	// p.Span().Debugf("%s [OpMetaTruncate] req: %d - %v, resp body: %v, "+
	// 	"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	// return
}

func (m *metadataManager) opMetaExtentsTruncate(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &ExtentsTruncateReq{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	mp.ExtentsTruncate(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaExtentsTruncate] req: %d - %v, resp body: %v, "+
		"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaClearInodeCache(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ClearInodeCacheRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.ClearInodeCache(req, p)
	m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaClearInodeCache] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Delete a meta partition.
func (m *metadataManager) opDeleteMetaPartition(conn net.Conn, p *Packet, remoteAddr string) (err error) {
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
		return
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
	p.Span().Debugf("%s [opDeleteMetaPartition] req: %d - %v, resp: %v",
		remoteAddr, p.GetReqID(), req, err)
	return
}

func (m *metadataManager) opUpdateMetaPartition(conn net.Conn, p *Packet, remoteAddr string) (err error) {
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
	err = mp.UpdatePartition(p.Context(), req, resp)
	adminTask.Response = resp
	adminTask.Request = nil
	m.respondToMaster(adminTask)
	p.Span().Infof("%s [opUpdateMetaPartition] req[%v], response[%v].", remoteAddr, req, adminTask)
	return
}

func (m *metadataManager) opLoadMetaPartition(conn net.Conn, p *Packet, remoteAddr string) (err error) {
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
		p.Span().Errorf("%s req[%v], response marshal[%v]", remoteAddr, req, err.Error())
		m.respondToClient(conn, p)
		return
	}
	m.respondToClient(conn, p)
	p.Span().Infof("%s [opLoadMetaPartition] req[%v], response status[%s], "+
		"response body[%s], error[%v]", remoteAddr, req, p.GetResultMsg(), p.Data, err)
	return
}

func (m *metadataManager) opDecommissionMetaPartition(conn net.Conn, p *Packet, remoteAddr string) (err error) {
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

func (m *metadataManager) opAddMetaPartitionRaftMember(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.AddMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}

	span := p.Span()
	defer func() {
		if err != nil {
			span.Infof("pkt %s remote %s reqId add raft member failed, req %v, err %s", p.String(), remoteAddr, adminTask, err.Error())
			return
		}
		span.Infof("pkt %s, remote %s add raft member success, req %v", p.String(), remoteAddr, adminTask)
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

	span.Infof("[%s], remote %s start add raft member, req %v", p.String(), remoteAddr, adminTask)
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
	_, err = mp.ChangeMember(raftProto.ConfAddNode, raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClientWithVer(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClientWithVer(conn, p)
	return
}

func (m *metadataManager) opRemoveMetaPartitionRaftMember(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.RemoveMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}

	span := p.Span()
	defer func() {
		if err != nil {
			span.Infof("[%s], remote %s remove raft member failed, req %v, err %s", p.String(), remoteAddr, adminTask, err.Error())
			return
		}
		span.Infof("[%s], remote %s remove raft member success, req %v", p.String(), remoteAddr, adminTask)
	}()

	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}

	span.Infof("[%s], remote %s remove raft member success, req %v", p.String(), remoteAddr, adminTask)

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
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali RemovePeerID %v", req.RemovePeer.ID))
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

func (m *metadataManager) opMetaBatchInodeGet(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.BatchInodeGetRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.InodeGetBatch(req, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opMetaBatchInodeGet] req: %d - %v, resp: %v, "+
		"body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaPartitionTryToLeader(conn net.Conn, p *Packet, remoteAddr string) (err error) {
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

func (m *metadataManager) opMetaDeleteInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.DeleteInodeRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.DeleteInode(req, p, remoteAddr)
	_ = m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchDeleteInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.DeleteInodeBatchRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.DeleteInodeBatch(req, p, remoteAddr)
	_ = m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaBatchDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaUpdateXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.UpdateXAttrRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.UpdateXAttr(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaUpdateXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaSetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.SetXAttrRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.SetXAttr(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaSetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchSetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.BatchSetXAttrRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.BatchSetXAttr(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClient(conn, p)
	p.Span().Debugf("%s [opMetaBatchSetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaGetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.GetXAttrRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.GetXAttr(req, p)
	_ = m.respondToClient(conn, p)
	p.Span().Debugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaGetAllXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.GetAllXAttrRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.GetAllXAttr(req, p)
	_ = m.respondToClient(conn, p)
	p.Span().Debugf("%s [opMetaGetAllXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchGetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.BatchGetXAttrRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.BatchGetXAttr(req, p)
	_ = m.respondToClient(conn, p)
	p.Span().Debugf("%s [opMetaBatchGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaRemoveXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.RemoveXAttrRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.RemoveXAttr(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaRemoveXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaListXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ListXAttrRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.ListXAttr(req, p)
	_ = m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaListXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchExtentsAdd(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.AppendExtentKeysRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.BatchExtentAppend(req, p)
	m.updatePackRspSeq(mp, p)
	_ = m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaBatchExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchObjExtentsAdd(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.AppendObjExtentKeysRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, true, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.BatchObjExtentAppend(req, p)
	_ = m.respondToClientWithVer(conn, p)
	p.Span().Debugf("%s [opMetaBatchObjExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opCreateMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.CreateMultipartRequest{}
	mp, err := m.opRequestNoVer(conn, p, remote, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.CreateMultipart(req, p)
	_ = m.respondToClientWithVer(conn, p)
	return
}

func (m *metadataManager) opRemoveMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.RemoveMultipartRequest{}
	mp, err := m.opRequestNoVer(conn, p, remote, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.RemoveMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opGetExpiredMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.GetExpiredMultipartRequest{}
	mp, err := m.opRequestNoVer(conn, p, remote, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.GetExpiredMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opGetMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.GetMultipartRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remote, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.GetMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opAppendMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.AddMultipartPartRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remote, req, true, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.AppendMultipart(req, p)
	_ = m.respondToClientWithVer(conn, p)
	return
}

func (m *metadataManager) opListMultipart(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ListMultipartRequest{}
	mp, err := m.opRequestNotFollower(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.ListMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

// Handle OpMetaTxCreateInode inode.
func (m *metadataManager) opTxCreateInode(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.TxCreateInodeRequest{}
	mp, err := m.opRequest(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.TxCreateInode(req, p, remoteAddr)
	m.updatePackRspSeq(mp, p)
	m.respondToClient(conn, p)
	p.Span().Debugf("%s [opTxCreateInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchSetInodeQuota(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.BatchSetMetaserverQuotaReuqest{}
	mp, err := m.opRequestNoVer(conn, p, remote, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
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
	p.Span().Infof("[opMetaBatchSetInodeQuota] req [%v] resp [%v] success.", req, resp)
	return
}

func (m *metadataManager) opMetaBatchDeleteInodeQuota(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.BatchDeleteMetaserverQuotaReuqest{}
	mp, err := m.opRequestNoVer(conn, p, remote, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
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
	p.Span().Infof("[opMetaBatchDeleteInodeQuota] req [%v] resp [%v] success.", req, resp)
	return err
}

func (m *metadataManager) opMetaGetInodeQuota(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.GetInodeQuotaRequest{}
	mp, err := m.opRequestNoVer(conn, p, remote, req, false, func() uint64 { return req.PartitionId })
	if err != nil {
		return err
	}
	err = mp.getInodeQuota(req.Inode, p)
	_ = m.respondToClient(conn, p)
	p.Span().Infof("[opMetaGetInodeQuota] get inode[%v] quota success.", req.Inode)
	return
}

func (m *metadataManager) opMetaGetUniqID(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.GetUniqIDRequest{}
	mp, err := m.opRequestNoVer(conn, p, remoteAddr, req, false, func() uint64 { return req.PartitionID })
	if err != nil {
		return err
	}
	err = mp.GetUniqID(p, req.Num)
	m.respondToClient(conn, p)
	if err != nil {
		p.Span().Errorf("%s [opMetaGetUniqID] %s, response to client: %s",
			remoteAddr, err.Error(), p.GetResultMsg())
	}
	p.Span().Debugf("%s [opMetaGetUniqID] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) prepareCreateVersion(ctx context.Context, req *proto.MultiVersionOpRequest) (err error, opAagin bool) {
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

	getSpan(ctx).Warnf("action[prepareCreateVersion] volume %v update to ver [%v] step %v",
		req.VolumeID, req.VerSeq, ver2Phase.step)
	return
}

func (m *metadataManager) checkVolVerList(ctx context.Context) (err error) {
	spanRoot := getSpan(ctx).WithOperation("checkVolVerList")
	volumeArr := make(map[string]bool)

	spanRoot.Debugf("start ...")
	m.Range(true, func(id uint64, partition MetaPartition) bool {
		volumeArr[partition.GetVolName()] = true
		return true
	})

	for volName := range volumeArr {
		span := spanRoot.WithOperation("volume-" + volName)
		mpsVerlist := make(map[uint64]*proto.VolVersionInfoList)
		// need get first or else the mp verlist may be change in the follower process
		m.Range(true, func(id uint64, partition MetaPartition) bool {
			if partition.GetVolName() != volName {
				return true
			}
			span.Debugf("id[%v] dp verlist %v partition.GetBaseConfig().PartitionId %v",
				id, partition.GetVerList(), partition.GetBaseConfig().PartitionId)
			mpsVerlist[id] = &proto.VolVersionInfoList{VerList: partition.GetVerList()}
			return true
		})
		var info *proto.VolVersionInfoList
		if info, err = masterClient.AdminAPI().GetVerList(volName); err != nil {
			span.Error(err)
			return
		}

		span.Debugf("info %v", info)
		m.Range(true, func(id uint64, partition MetaPartition) bool {
			if partition.GetVolName() != volName {
				return true
			}
			span.Debugf("info %v id[%v] ", info, id)
			if _, exist := mpsVerlist[id]; exist {
				if err = partition.checkByMasterVerlist(mpsVerlist[id], info); err != nil {
					return true
				}
			}
			if _, err = partition.checkVerList(info, false); err != nil {
				span.Error(err)
			}
			return true
		})
	}
	return
}

func (m *metadataManager) commitCreateVersion(ctx context.Context, VolumeID string, VerSeq uint64, Op uint8, synchronize bool) (err error) {
	span := getSpan(ctx).WithOperation("commitCreateVersion-volume." + VolumeID)
	span.Warnf("volume %v seq [%v]", VolumeID, VerSeq)
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
			span.Infof("mp %v do HandleVersionOp verseq [%v]", mpId, VerSeq)
			if err := mp.HandleVersionOp(Op, VerSeq, nil, synchronize); err != nil {
				span.Errorf("mp %v do HandleVersionOp verseq [%v] err %v", mpId, VerSeq, err)
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
		span.Infof("do HandleVersionOp verseq [%v] finished", VerSeq)
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
		span.Warnf("try commit prepare seq [%v] with commit seq [%v]", ver2Phase.verPrepare, VerSeq)
		if VerSeq < ver2Phase.verSeq {
			err = fmt.Errorf("volname [%v] seq [%v] create less than loal %v", VolumeID, VerSeq, ver2Phase.verSeq)
			span.Error(err)
			return
		}
		if ver2Phase.step != proto.CreateVersionPrepare {
			err = fmt.Errorf("volname [%v] step not prepare", VolumeID)
			span.Error(err)
			return
		}
		ver2Phase.verSeq = VerSeq
		ver2Phase.step = proto.CreateVersionCommit
		ver2Phase.status = proto.VersionWorkingFinished
		span.Warnf("commit prepare seq [%v] with commit seq [%v]", ver2Phase.verPrepare, VerSeq)
		return
	}

	err = fmt.Errorf("volname [%v] not found", VolumeID)
	span.Error(err)
	return
}

func (m *metadataManager) updatePackRspSeq(mp MetaPartition, p *Packet) {
	if mp.GetVerSeq() > p.VerSeq {
		span := p.Span().WithOperation("checkmultiSnap.multiVersionstatus")
		span.Debugf("mp ver [%v], packet ver [%v]", mp.GetVerSeq(), p.VerSeq)
		p.VerSeq = mp.GetVerSeq() // used to response to client and try update verSeq of client
		p.ExtentType |= proto.VersionListFlag
		p.VerList = make([]*proto.VolVersionInfo, len(mp.GetVerList()))
		copy(p.VerList, mp.GetVerList())
	}
}

func (m *metadataManager) checkMultiVersionStatus(mp MetaPartition, p *Packet) (err error) {
	span := p.Span().WithOperation("checkmultiSnap.multiVersionstatus")
	if (p.ExtentType&proto.MultiVersionFlag == 0) && mp.GetVerSeq() > 0 {
		span.Warnf("volname [%v] mp ver [%v], client use old ver before snapshot", mp.GetVolName(), mp.GetVerSeq())
		return fmt.Errorf("client use old ver before snapshot")
	}
	// meta node do not need to check verSeq as strictly as datanode,file append or modAppendWrite on meta node is invisible to other files.
	// only need to guarantee the verSeq wrote on meta nodes grow up linearly on client's angle
	span.Debugf("mp[%v] ver [%v], packet ver [%v] reqId %v", mp.GetBaseConfig().PartitionId, mp.GetVerSeq(), p.VerSeq, p.ReqID)
	if mp.GetVerSeq() >= p.VerSeq {
		if mp.GetVerSeq() > p.VerSeq {
			span.Debugf("mp ver [%v], packet ver [%v]", mp.GetVerSeq(), p.VerSeq)
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
		span.Warnf("channel is full, volname [%v] not be queued", mp.GetVolName())
	}
	return
}

func (m *metadataManager) checkAndPromoteVersion(ctx context.Context, volName string) (err error) {
	span := getSpan(ctx).WithOperation("checkmultiSnap.multiVersionstatus-volume." + volName)
	span.Info("check ...")
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

				span.Warnf("status [%v] step %v", atomic.LoadUint32(&ver2Phase.status), atomic.LoadUint32(&ver2Phase.step))
				return
			}

			if info, err = masterClient.AdminAPI().GetVerInfo(volName); err != nil {
				span.Errorf("status [%v] step %v %v", atomic.LoadUint32(&ver2Phase.status), atomic.LoadUint32(&ver2Phase.step), err)
				return
			}
			if info.VerSeqPrepare != ver2Phase.verPrepare {
				atomic.StoreUint32(&ver2Phase.status, proto.VersionWorkingAbnormal)
				err = fmt.Errorf("volumeName %v status [%v] step %v",
					volName, atomic.LoadUint32(&ver2Phase.status), atomic.LoadUint32(&ver2Phase.step))
				span.Error(err)
				return
			}
			if info.VerPrepareStatus == proto.CreateVersionCommit {
				if err = m.commitCreateVersion(ctx, volName, info.VerSeqPrepare, proto.CreateVersionCommit, false); err != nil {
					span.Error(err)
					return
				}
			}
		}
	} else {
		span.Errorf("volumeName %v not found", volName)
	}
	return
}

func (m *metadataManager) opMultiVersionOp(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	// For ack to master
	data := p.Data
	m.responseAckOKToMaster(conn, p)

	var (
		req       = &proto.MultiVersionOpRequest{}
		resp      = &proto.MultiVersionOpResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
		opAgain bool
	)
	ctx := p.Context()
	span := p.Span()
	span.Debugf("volume %v op [%v]", req.VolumeID, req.Op)

	start := time.Now()
	decode := json.NewDecoder(bytes.NewBuffer(data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		span.Errorf("%v mp err %v do Decoder", req.VolumeID, err.Error())
		goto end
	}

	resp.Status = proto.TaskSucceeds
	resp.VolumeID = req.VolumeID
	resp.Addr = req.Addr
	resp.VerSeq = req.VerSeq
	resp.Op = req.Op

	if req.Op == proto.CreateVersionPrepare {
		if err, opAgain = m.prepareCreateVersion(ctx, req); err != nil || opAgain {
			span.Errorf("%v mp err %v do Decoder", req.VolumeID, err)
			goto end
		}
		if err = m.commitCreateVersion(p.Context(), req.VolumeID, req.VerSeq, req.Op, true); err != nil {
			span.Errorf("%v mp err %v do commitCreateVersion", req.VolumeID, err.Error())
			goto end
		}
	} else if req.Op == proto.CreateVersionCommit || req.Op == proto.DeleteVersion {
		if err = m.commitCreateVersion(p.Context(), req.VolumeID, req.VerSeq, req.Op, false); err != nil {
			span.Errorf("%v mp err %v do commitCreateVersion", req.VolumeID, err.Error())
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
		span.Infof("%s [opMultiVersionOp] pkt %s, req: %v; respAdminTask: %v, resp: %v, errRsp %v err %v",
			remoteAddr, p.String(), req, adminTask, resp, errRsp, err)
	}

	if blog.GetOutputLevel() >= blog.Linfo {
		rspData, _ := json.Marshal(resp)
		span.Infof("%s [opMultiVersionOp] pkt %s, req: %v; respAdminTask: %v, resp: %v, cost %s",
			remoteAddr, p.String(), req, adminTask, string(rspData), time.Since(start).String())
	}
	return
}
