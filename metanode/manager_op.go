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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	raftProto "github.com/tiglabs/raft/proto"
)

const (
	MaxUsedMemFactor = 1.1
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
		maxInode := partition.GetInodeTree().MaxItem()
		maxIno := mConf.Start
		if maxInode != nil {
			maxIno = maxInode.(*Inode).Inode
		}
		mpr := &proto.MetaPartitionReport{
			PartitionID:     mConf.PartitionId,
			Start:           mConf.Start,
			End:             mConf.End,
			Status:          proto.ReadWrite,
			MaxInodeID:      mConf.Cursor,
			VolName:         mConf.VolName,
			InodeCnt:        uint64(partition.GetInodeTree().Len()),
			DentryCnt:       uint64(partition.GetDentryTree().Len()),
			IsLearner:       partition.IsLearner(),
			ExistMaxInodeID: maxIno,
		}
		addr, isLeader := partition.IsLeader()
		if addr == "" {
			mpr.Status = proto.Unavailable
		}
		mpr.IsLeader = isLeader
		if mConf.Cursor >= mConf.End {
			mpr.Status = proto.ReadOnly
		}
		if resp.Used > uint64(float64(resp.Total)*MaxUsedMemFactor) {
			mpr.Status = proto.ReadOnly
		}
		resp.MetaPartitionReports = append(resp.MetaPartitionReports, mpr)
		return true
	})
	resp.ZoneName = m.zoneName
	resp.Status = proto.TaskSucceeds
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.respondToMaster(adminTask)
	data, _ := json.Marshal(resp)
	log.LogInfof("%s [opMasterHeartbeat] req:%v; respAdminTask: %v, "+
		"resp: %v", remoteAddr, req, adminTask, string(data))
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
	log.LogInfof("[opCreateMetaPartition] [remoteAddr=%s]accept a from"+
		" master message: %v", remoteAddr, adminTask)
	// create a new meta partition.
	if err = m.createPartition(req); err != nil {
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
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],err[%v]", p.GetOpMsgWithReqAndResult(), string(p.Data))
		return
	}
	mp.(*metaPartition).internalClean(p.Data[:p.Size])
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

// Handle OpCreate
func (m *metadataManager) opCreateDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
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
	err = mp.DeleteDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle Op batch Delete Dentry
func (m *metadataManager) opBatchDeleteDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &BatchDeleteDentryReq{}
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
	err = mp.DeleteDentryBatch(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opUpdateDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &UpdateDentryReq{}
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
	err = mp.UpdateDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opUpdateDentry] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaUnlinkInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &UnlinkInoReq{}
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
	err = mp.UnlinkInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchUnlinkInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &BatchUnlinkInoReq{}
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
	err = mp.UnlinkInodeBatch(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpReadDir
func (m *metadataManager) opReadDir(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
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

func (m *metadataManager) opMetaInodeGet(conn net.Conn, p *Packet,
	remoteAddr string, version uint8) (err error) {
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
	if err = mp.InodeGet(req, p, version); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaInodeGet] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opBatchMetaEvictInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.BatchEvictInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] request unmarshal: %v", p.GetOpMsgWithReqAndResult(), err.Error())
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

	if err = mp.EvictInodeBatch(req, p); err != nil {
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opBatchMetaEvictInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaEvictInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.EvictInodeRequest{}
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

	if err = mp.EvictInode(req, p); err != nil {
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaEvictInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opSetAttr(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &SetattrRequest{}
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

func (m *metadataManager) opMetaExtentsInsert(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.InsertExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ExtentInsert(req, p)
	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("%s [opMetaExtentsInsert] ExtentInsert: %s, "+
			"response to client: %s", remoteAddr, err.Error(), p.GetResultMsg())
	}
	log.LogDebugf("%s [opMetaExtentsInsert] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaExtentsList(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
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

	err = mp.ExtentsList(req, p)
	m.respondToClient(conn, p)
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
	mp.ExtentsTruncate(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [OpMetaTruncate] req: %d - %v, resp body: %v, "+
		"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Delete a meta partition.
//func (m *metadataManager) opDeleteMetaPartition(conn net.Conn,
//	p *Packet, remoteAddr string) (err error) {
//	req := &proto.DeleteMetaPartitionRequest{}
//	adminTask := &proto.AdminTask{
//		Request: req,
//	}
//	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
//	decode.UseNumber()
//	if err = decode.Decode(adminTask); err != nil {
//		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
//		m.respondToClient(conn, p)
//		return
//	}
//	mp, err := m.getPartition(req.PartitionID)
//	if err != nil {
//		p.PacketOkReply()
//		m.respondToClient(conn, p)
//		return
//	}
//	// Ack the master request
//	conf := mp.GetBaseConfig()
//	mp.Stop()
//	mp.DeleteRaft()
//	m.deletePartition(mp.GetBaseConfig().PartitionId)
//	os.RemoveAll(conf.RootDir)
//	p.PacketOkReply()
//	m.respondToClient(conn, p)
//	runtime.GC()
//	log.LogInfof("%s [opDeleteMetaPartition] req: %d - %v, resp: %v",
//		remoteAddr, p.GetReqID(), req, err)
//	return
//}

func (m *metadataManager) opExpiredMetaPartition(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	req := &proto.DeleteMetaPartitionRequest{}
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
		p.PacketOkReply()
		m.respondToClient(conn, p)
		return
	}
	// Ack the master request
	mp.ExpiredRaft()
	m.expiredPartition(mp.GetBaseConfig().PartitionId)
	p.PacketOkReply()
	m.respondToClient(conn, p)
	runtime.GC()
	log.LogInfof("%s [opDeleteMetaPartition] req: %d - %v, resp: %v",
		remoteAddr, p.GetReqID(), req, err)
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
	m.responseAckOKToMaster(conn, p)
	resp := &UpdatePartitionResp{
		VolName:     req.VolName,
		PartitionID: req.PartitionID,
		End:         req.End,
	}
	err = mp.UpdatePartition(p.Ctx(), req, resp)
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
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.MetaPartitionDecommissionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return err
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return err
	}
	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.NewErrorf("[opDecommissionMetaPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opDecommissionMetaPartition]: partitionID= %d, "+
			"Marshal %s", req.PartitionID, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
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

func (m *metadataManager) opAddMetaPartitionRaftMember(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.AddMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		log.LogErrorf("get parititon has err by id:[%d] err:[%s]", req.PartitionId, err.Error())
		p.PacketErrorWithBody(proto.OpTryOtherAddr, ([]byte)(proto.ErrMetaPartitionNotExists.Error()))
		m.respondToClient(conn, p)
		return err
	}

	if mp.IsExistPeer(req.AddPeer) {
		p.PacketOkReply()
		m.respondToClient(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opAddMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if req.AddPeer.ID == 0 {
		err = errors.NewErrorf("[opAddMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali AddPeerID %v", req.AddPeer.ID))
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

func (m *metadataManager) opAddMetaPartitionRaftLearner(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.AddMetaPartitionRaftLearnerRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpTryOtherAddr, ([]byte)(proto.ErrMetaPartitionNotExists.Error()))
		m.respondToClient(conn, p)
		return err
	}

	if mp.IsExistLearner(req.AddLearner) {
		p.PacketOkReply()
		m.respondToClient(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opAddMetaPartitionRaftLearner]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if req.AddLearner.ID == 0 {
		err = errors.NewErrorf("[opAddMetaPartitionRaftLearner]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali AddPeerID %v", req.AddLearner.ID))
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfAddLearner, raftProto.Peer{ID: req.AddLearner.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

func (m *metadataManager) opPromoteMetaPartitionRaftLearner(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.PromoteMetaPartitionRaftLearnerRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpTryOtherAddr, ([]byte)(proto.ErrMetaPartitionNotExists.Error()))
		m.respondToClient(conn, p)
		return err
	}

	if !mp.IsExistLearner(req.PromoteLearner) {
		p.PacketOkReply()
		m.respondToClient(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opAddMetaPartitionRaftLearner]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if req.PromoteLearner.ID == 0 {
		err = errors.NewErrorf("[opAddMetaPartitionRaftLearner]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali AddPeerID %v", req.PromoteLearner.ID))
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfPromoteLearner, raftProto.Peer{ID: req.PromoteLearner.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

func (m *metadataManager) opRemoveMetaPartitionRaftMember(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.RemoveMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	req.ReserveResource = adminTask.ReserveResource
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}

	if !req.RaftOnly && !mp.IsExistPeer(req.RemovePeer) {
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
	if !req.RaftOnly {
		if err = mp.CanRemoveRaftMember(req.RemovePeer); err != nil {
			err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
				"Marshal %s", req.PartitionId, fmt.Sprintf("unavali RemovePeerID %v", req.RemovePeer.ID))
			p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
			m.respondToClient(conn, p)
			return
		}
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

func (m *metadataManager) opResetMetaPartitionMember(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var (
		reqData []byte
		updated bool
	)
	req := &proto.ResetMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}

	mp, err := m.GetPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	for _, peer := range req.NewPeers {
		if !mp.IsExistPeer(peer) {
			err = errors.NewErrorf("[opResetMetaPartitionMember]: peer not exists, peer addr[%v], partitionID= %d, ", peer.Addr, req.PartitionId)
			p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
			m.respondToClient(conn, p)
			return err
		}
	}

	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opResetMetaPartitionMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	var peers []raftProto.Peer
	for _, peer := range req.NewPeers {
		peers = append(peers, raftProto.Peer{ID: peer.ID})
	}
	err = mp.ResetMember(peers, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	updated, err = mp.ApplyResetMember(req)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	if updated {
		if err = mp.PersistMetadata(); err != nil {
			log.LogErrorf("action[opResetMetaPartitionMember] err[%v].", err)
		}
	}
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		err = m.respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	err = m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opMetaBatchInodeGet(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.BatchInodeGetRequest{}
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
	err = mp.InodeGetBatch(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchInodeGet] req: %d - %v, resp: %v, "+
		"body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaPartitionTryToLeader(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
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
	remoteAddr string) (err error) {
	req := &proto.DeleteInodeRequest{}
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
	err = mp.DeleteInode(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle Inode
func (m *metadataManager) opMetaCursorReset(conn net.Conn, p *Packet, remoteAddr string) error {
	req := &proto.CursorResetRequest{}
	if err := json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
	}

	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	if _, err = mp.(*metaPartition).CursorReset(p.Ctx(), req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
	} else {
		p.PacketOkReply()
	}
	_ = m.respondToClient(conn, p)
	log.LogInfof("%s [opCursorReset] req: %d - %v, resp: %v, body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return err
}

func (m *metadataManager) opMetaBatchDeleteInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	var req *proto.DeleteInodeBatchRequest
	if err = json.Unmarshal(p.Data, &req); err != nil {
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
	err = mp.DeleteInodeBatch(req, p)
	log.LogDebugf("%s [opMetaDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)

	_ = m.respondToClient(conn, p)

	return
}

func (m *metadataManager) opMetaSetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.SetXAttrRequest{}
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
	err = mp.SetXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaSetXAttr] req: %d - %v, resp: %v, body: %s",
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
	err = mp.RemoveXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaListXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ListXAttrRequest{}
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
	err = mp.ListXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchExtentsAdd(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.AppendExtentKeysRequest{}
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
	err = mp.BatchExtentAppend(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opCreateMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.CreateMultipartRequest{}
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
	err = mp.CreateMultipart(req, p)
	_ = m.respondToClient(conn, p)
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
	defer func() {
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		}
		_ = m.respondToClient(conn, p)
	}()
	req := &proto.AddMultipartPartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.AppendMultipart(req, p)
	return
}

func (m *metadataManager) opListMultipart(conn net.Conn, p *Packet, remote string) (err error) {
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

func (m *metadataManager) opGetAppliedID(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &GetAppliedIDReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	appliedID := mp.GetAppliedID()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, appliedID)
	p.PacketOkWithBody(buf)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetAppliedID] req: %d - %v, req args: %v, resp: %v, body: %v",
		remoteAddr, p.GetReqID(), req, string(p.Arg), p.GetResultMsg(), appliedID)
	return
}

func (m *metadataManager) opGetMetaNodeVersionInfo(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	ver, err := NewMetaNodeVersion(proto.BaseVersion)
	reply, err := json.Marshal(ver)
	if err != nil {
		p.PacketErrorWithBody(proto.OpGetMetaNodeVersionInfo, []byte(err.Error()))
		return err
	}
	p.PacketOkWithBody(reply)
	m.respondToClient(conn, p)
	return
}

func (m *metadataManager) opMetaLookupDeleted(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(proto.LookupDeletedDentryRequest)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v] req: %v, resp: %v", p.GetOpMsgWithReqAndResult(), req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.LookupDeleted(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaLookupDeleted] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaGetDeletedInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &GetDeletedInodeReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.GetDeletedInode(req, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetDeletedInode] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opMetaBatchGetDeletedInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(BatchGetDeletedInodeReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.BatchGetDeletedInode(req, p); err != nil {
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchGetDeletedInode] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opRecoverDeletedDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(RecoverDeletedDentryReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.RecoverDeletedDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opRecoverDeletedDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opBatchRecoverDeletedDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(BatchRecoverDeletedDentryReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.BatchRecoverDeletedDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opBatchRecoverDeletedDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opRecoverDeletedINode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(RecoverDeletedInodeReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.RecoverDeletedInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opRecoverDeleteINode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opBatchRecoverDeletedINode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(BatchRecoverDeletedInodeReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.BatchRecoverDeletedInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opBatchRecoverDeleteINode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opReadDeletedDir(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(proto.ReadDeletedDirRequest)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ReadDeletedDir(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [%v]req: %v , resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opBatchCleanDeletedDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(BatchCleanDeletedDentryReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.BatchCleanDeletedDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opBatchCleanDeletedDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opCleanDeletedDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(CleanDeletedDentryReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CleanDeletedDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opCleanDeletedDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opBatchCleanDeletedINode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(BatchCleanDeletedInodeReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.BatchCleanDeletedInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opBatchCleanDeleteINode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opCleanDeletedINode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(CleanDeletedInodeReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}

	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CleanDeletedInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opCleanDeletedINode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *metadataManager) opStatDeletedFileInfo(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(StatDeletedFileReq)
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	var mp MetaPartition
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[%v],req[%v],err[%v]", p.GetOpMsgWithReqAndResult(), req, string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.StatDeletedFileInfo(p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opStatDeletedFileInfo] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}
