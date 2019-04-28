// Copyright 2018 The The Chubao Authors.
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
	"encoding/json"
	"net"
	"os"

	"bytes"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	raftProto "github.com/tiglabs/raft/proto"
)

func (m *metadataManager) opMasterHeartbeat(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	// For ack to master
	m.responseAckOKToMaster(conn, p)
	var (
		req       = &proto.HeartBeatRequest{}
		resp      = &proto.MetaNodeHeartbeatResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		goto end
	}

	// collect memory info
	resp.Total = configTotalMem
	resp.Used, err = util.GetProcessMemory(os.Getpid())
	if err != nil {
		adminTask.Status = proto.TaskFailed
		goto end
	}
	m.Range(func(id uint64, partition MetaPartition) bool {
		mConf := partition.GetBaseConfig()
		mpr := &proto.MetaPartitionReport{
			PartitionID: mConf.PartitionId,
			Start:       mConf.Start,
			End:         mConf.End,
			Status:      proto.ReadWrite,
			MaxInodeID:  mConf.Cursor,
		}
		addr, isLeader := partition.IsLeader()
		if addr == "" {
			mpr.Status = proto.Unavailable
		}
		mpr.IsLeader = isLeader
		if mConf.Cursor >= mConf.End {
			mpr.Status = proto.ReadOnly
		}
		resp.MetaPartitionReports = append(resp.MetaPartitionReports, mpr)
		return true
	})
	resp.Status = proto.TaskSucceeds
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.respondToMaster(adminTask)
	log.LogInfof("%s [opMasterHeartbeat] req:%v; respAdminTask: %v, "+
		"resp: %v", remoteAddr, req, adminTask, adminTask.Response)
	return
}

func (m *metadataManager) opCreateMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	defer func() {
		var buf []byte
		status := proto.OpOk
		if err != nil {
			status = proto.OpErr
			buf = []byte(err.Error())
		}
		p.PacketErrorWithBody(status, buf)
		m.respondToClient(conn, p)
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
	log.LogDebugf("%s [opCreateMetaPartition] [remoteAddr=%s]accept a from"+
		" master message: %v", remoteAddr, adminTask)
	// create a new meta partition.
	if err = m.createPartition(req.PartitionID, req.VolName,
		req.Start, req.End, req.Members); err != nil {
		err = errors.NewErrorf("[opCreateMetaPartition]->%s; request message: %v",
			err.Error(), adminTask.Request)
		return
	}
	log.LogInfof("%s [opCreateMetaPartition] req:%v; resp: %v", remoteAddr,
		req, adminTask)
	return
}

// Handle OpCreate inode.
func (m *metadataManager) opCreateInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &CreateInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateInode(req, p)
	// reply the operation result to the client through TCP
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opCreateInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaLinkInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &LinkInodeReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateInodeLink(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaLinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpCreate
func (m *metadataManager) opFreeInodeOnRaftFollower(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	mp, err := m.getPartition(p.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp.(*metaPartition).internalDelete(p.Data[:p.Size])
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

// Handle OpCreate
func (m *metadataManager) opCreateDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &CreateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opCreateDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpDelete Dentry
func (m *metadataManager) opDeleteDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &DeleteDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteDentry(req, p)
	m.respondToClient(conn, p)
	log.LogInfof("%s [opDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opUpdateDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &UpdateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.UpdateDentry(req, p)
	m.respondToClient(conn, p)
	log.LogInfof("%s [opUpdateDentry] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaUnlinkInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &UnlinkInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.UnlinkInode(req, p)
	m.respondToClient(conn, p)
	log.LogInfof("%s [opDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpReadDir
func (m *metadataManager) opReadDir(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.ReadDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ReadDir(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opReadDir] req: %d - %v, resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaInodeGet(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &InodeGetReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaInodeGet]: %s", err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaInodeGet] %s, req: %s", err.Error(),
			string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.InodeGet(req, p); err != nil {
		err = errors.NewErrorf("[opMetaInodeGet] %s, req: %s", err.Error(),
			string(p.Data))
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaInodeGet] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaEvictInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.EvictInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaEvictInode] request unmarshal: %v", err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaEvictInode] req: %s, resp: %v", req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = mp.EvictInode(req, p); err != nil {
		err = errors.NewErrorf("[opMetaEvictInode] req: %s, resp: %v", req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogInfof("%s [opMetaEvictInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opSetAttr(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &SetattrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opSetAttr] req: %v, error: %v", req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opSetAttr] req: %v, error: %v", req, err.Error())
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.SetAttr(p.Data, p); err != nil {
		err = errors.NewErrorf("[opSetAttr] req: %v, error: %s", req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opSetAttr] req: %d - %v, resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Lookup request
func (m *metadataManager) opMetaLookup(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.LookupRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
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
	remoteAddr string) (err error) {
	req := &proto.AppendExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.NewErrorf("%s, response to client: %s", err.Error(),
			p.GetResultMsg())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ExtentAppend(req, p)
	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("%s [opMetaExtentsAdd] ExtentAppend: %s, "+
			"response to client: %s", remoteAddr, err.Error(), p.GetResultMsg())
	}
	log.LogDebugf("%s [opMetaExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsList(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.GetExtentsRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.ExtentsList(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaExtentsList] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsDel(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	panic("not implemented yet")
}

func (m *metadataManager) opMetaExtentsTruncate(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &ExtentsTruncateReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	mp.ExtentsTruncate(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [OpMetaTruncate] req: %d - %v, resp body: %v, "+
		"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Delete a meta partition.
func (m *metadataManager) opDeleteMetaPartition(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	req := &proto.DeleteMetaPartitionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	resp := &proto.DeleteMetaPartitionResponse{
		PartitionID: req.PartitionID,
		Status:      proto.TaskSucceeds,
	}
	// Ack the master request
	m.responseAckOKToMaster(conn, p)
	conf := mp.GetBaseConfig()
	mp.Stop()
	err = mp.DeleteRaft()
	os.RemoveAll(conf.RootDir)
	if err != nil {
		resp.Status = proto.TaskFailed
	}
	adminTask.Response = resp
	adminTask.Request = nil
	m.respondToMaster(adminTask)
	log.LogInfof("%s [opDeleteMetaPartition] req: %d - %v, resp: %v",
		remoteAddr, p.GetReqID(), req, adminTask)
	return
}

func (m *metadataManager) opUpdateMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(UpdatePartitionReq)
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
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
	remoteAddr string) (err error) {
	req := &proto.MetaPartitionLoadRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		p.WriteToConn(conn)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if err = mp.LoadSnapshotSign(p); err != nil {
		log.LogErrorf("%s [opLoadMetaPartition] req[%v], "+
			"response marshal[%v]", remoteAddr, req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogInfof("%s [opLoadMetaPartition] req[%v], response status[%s], "+
		"response body[%s], error[%v]", remoteAddr, req, p.GetResultMsg(), p.Data,
		err)
	return
}

func (m *metadataManager) opDecommissionMetaPartition(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.MetaPartitionDecommissionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	m.responseAckOKToMaster(conn, p)
	resp := proto.MetaPartitionDecommissionResponse{
		PartitionID: req.PartitionID,
		VolName:     req.VolName,
		Status:      proto.TaskFailed,
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.NewErrorf("[opDecommissionMetaPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
		resp.Result = err.Error()
		goto end
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opDecommissionMetaPartition]: partitionID= %d, "+
			"Marshal %s", req.PartitionID, err)
		resp.Result = err.Error()
		goto end
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	_, err = mp.ChangeMember(raftProto.ConfRemoveNode,
		raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	resp.Status = proto.TaskSucceeds
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.respondToMaster(adminTask)
	log.LogInfof("%s [opDecommissionMetaPartition]: the end %v", remoteAddr,
		adminTask)
	return
}

func (m *metadataManager) opMetaBatchInodeGet(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.BatchInodeGetRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		return
	}
	err = mp.InodeGetBatch(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchInodeGet] req: %d - %v, resp: %v, "+
		"body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}
