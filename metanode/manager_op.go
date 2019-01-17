// Copyright 2018 The The Container File System Authors Authors.
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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	raftProto "github.com/tiglabs/raft/proto"
	"runtime"
)

func (m *metaManager) opMasterHeartbeat(conn net.Conn, p *Packet) (err error) {
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

	// collect used info
	// machine mem total and used
	resp.Total, _, err = util.GetMemInfo()
	{
		m := &runtime.MemStats{}
		runtime.ReadMemStats(m)
		resp.Used = m.Sys
	}
	if err != nil {
		adminTask.Status = proto.TaskFailed
		goto end
	}
	// every partition used
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
	log.LogDebugf("[opMasterHeartbeat] req:%v; respAdminTask: %v, resp: %v",
		req, adminTask, adminTask.Response)
	return
}

// Handle OpCreateMetaRange
func (m *metaManager) opCreateMetaPartition(conn net.Conn, p *Packet) (err error) {
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
	// GetConnect task from packet.
	req := &proto.CreateMetaPartitionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		err = errors.Errorf("[opCreateMetaPartition]: Unmarshal AdminTask"+
			" struct: %s", err.Error())
		return
	}
	log.LogDebugf("[opCreateMetaPartition] [remoteAddr=%s]accept a from"+
		" master message: %v", conn.RemoteAddr(), adminTask)
	// Create new  metaPartition.
	if err = m.createPartition(req.PartitionID, req.VolName,
		req.Start, req.End, req.Members); err != nil {
		err = errors.Errorf("[opCreateMetaPartition]->%s; request message: %v",
			err.Error(), adminTask.Request)
		return
	}
	log.LogDebugf("[opCreateMetaPartition] req:%v; resp: %v", req, adminTask)
	return
}

// Handle OpCreate Inode
func (m *metaManager) opCreateInode(conn net.Conn, p *Packet) (err error) {
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
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opCreateInode] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

func (m *metaManager) opMetaLinkInode(conn net.Conn, p *Packet) (err error) {
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
	err = mp.CreateLinkInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaLinkInode] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

// Handle OpCreate
func (m *metaManager) opCreateDentry(conn net.Conn, p *Packet) (err error) {
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
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opCreateDentry] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

// Handle OpDelete Dentry
func (m *metaManager) opDeleteDentry(conn net.Conn, p *Packet) (err error) {
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
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opDeleteDentry] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

func (m *metaManager) opUpdateDentry(conn net.Conn, p *Packet) (err error) {
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
	log.LogDebugf("[opUpdateDentry] req: %d - %v; resp: %v, body: %s",
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metaManager) opDeleteInode(conn net.Conn, p *Packet) (err error) {
	req := &DeleteInoReq{}
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
	err = mp.DeleteInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opDeleteInode] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

// Handle OpReadDir
func (m *metaManager) opReadDir(conn net.Conn, p *Packet) (err error) {
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
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opReadDir] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

// Handle OpOpen
func (m *metaManager) opOpen(conn net.Conn, p *Packet) (err error) {
	req := &proto.OpenRequest{}
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
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.Open(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opOpen] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

func (m *metaManager) opReleaseOpen(conn net.Conn, p *Packet) (err error) {
	req := &ReleaseReq{}
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
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.ReleaseOpen(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opClose] req: %d - %v, resp status: %v, resp body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

// 获取Inode请求
func (m *metaManager) opMetaInodeGet(conn net.Conn, p *Packet) (err error) {
	req := &InodeGetReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaInodeGet]: %s", err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaInodeGet] %s, req: %s", err.Error(),
			string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.InodeGet(req, p); err != nil {
		err = errors.Errorf("[opMetaInodeGet] %s, req: %s", err.Error(),
			string(p.Data))
	}
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaInodeGet] req: %d - %v; resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

func (m *metaManager) opMetaEvictInode(conn net.Conn, p *Packet) (err error) {
	req := &proto.EvictInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaEvictInode] request unmarshal: %v", err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaEvictInode] req: %s, resp: %v", req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = mp.EvictInode(req, p); err != nil {
		err = errors.Errorf("[opMetaEvictInode] req: %s, resp: %v", req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaEvictInode] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

func (m *metaManager) opSetattr(conn net.Conn, p *Packet) (err error) {
	req := &SetattrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.Errorf("[opSetattr] req: %v, error: %v", req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.Errorf("[opSetattr] req: %v, error: %v", req, err.Error())
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.SetAttr(p.Data, p); err != nil {
		err = errors.Errorf("[opSetattr] req: %v, error: %s", req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("[opSetattr] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

// Lookup request
func (m *metaManager) opMetaLookup(conn net.Conn, p *Packet) (err error) {
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
	log.LogDebugf("[opMetaLookup] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

// 更新Extents请求
func (m *metaManager) opMetaExtentsAdd(conn net.Conn, p *Packet) (err error) {
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
		err = errors.Errorf("%s, response to client: %s", err.Error(),
			p.GetResultMsg())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ExtentAppend(req, p)
	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("[opMetaExtentsAdd] ExtentAppend: %s, "+
			"response to client: %s", err.Error(), p.GetResultMsg())
	}
	log.LogDebugf("[opMetaExtentsAdd] req: %d - %v, resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

// 获取Extents列表
func (m *metaManager) opMetaExtentsList(conn net.Conn, p *Packet) (err error) {
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
	log.LogDebugf("[opMetaExtentsList] req: %d - %v; resp: %v, body: %s", p.GetReqID(), req,
		p.GetResultMsg(), p.Data)
	return
}

func (m *metaManager) opMetaExtentsDel(conn net.Conn, p *Packet) (err error) {
	// TODO: not implement yet
	panic("not implement yet")
}

// truncae请求
func (m *metaManager) opMetaExtentsTruncate(conn net.Conn, p *Packet) (err error) {
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
	log.LogDebugf("[OpMetaTruncate] req: %d - %v, resp body: %v, resp body: %s",
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// 删除某个分片请求
func (m *metaManager) opDeleteMetaPartition(conn net.Conn, p *Packet) (err error) {
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
	// Ack Master Request
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
	log.LogDebugf("[opDeleteMetaPartition] req: %d - %v, resp: %v", p.GetReqID(), req,
		adminTask)
	return
}

// 更新某个分片的最大分配的元数据ID值
func (m *metaManager) opUpdateMetaPartition(conn net.Conn, p *Packet) (err error) {
	log.LogDebugf("[opUpdateMetaPartition] request.")
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
	log.LogDebugf("[opUpdateMetaPartition] req[%v], response[%v].", req, adminTask)
	return
}

func (m *metaManager) opLoadMetaPartition(conn net.Conn, p *Packet) (err error) {
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
		log.LogErrorf("[opLoadMetaPartition] req[%v], response marshal[%v]",
			req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("[opLoadMetaPartition] req[%v], response status[%s], "+
		"response body[%s], error[%v]", req, p.GetResultMsg(), p.Data, err)
	return
}

// 变更替换分片成员请求
func (m *metaManager) opOfflineMetaPartition(conn net.Conn, p *Packet) (err error) {
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
	log.LogDebugf("[opOfflineMetaPartition] received task: %v", adminTask)
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
		err = errors.Errorf("[opOfflineMetaPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
		resp.Result = err.Error()
		goto end
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.Errorf("[opOfflineMetaPartition]: partitionID= %d, "+
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
	log.LogDebugf("[opOfflineMetaPartition]: the end %v", adminTask)
	return
}

// 批量获取Inode请求
func (m *metaManager) opMetaBatchInodeGet(conn net.Conn, p *Packet) (err error) {
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
	log.LogDebugf("[opMetaBatchInodeGet] req: %d - %v, resp: %v, body: %s",
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}
