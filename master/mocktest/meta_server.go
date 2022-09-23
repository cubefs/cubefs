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

package mocktest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util"
)

type MockMetaServer struct {
	NodeID     uint64
	TcpAddr    string
	ZoneName   string
	mc         *master.MasterClient
	partitions map[uint64]*MockMetaPartition // Key: metaRangeId, Val: metaPartition
	sync.RWMutex
	stopC chan bool
}

func NewMockMetaServer(addr string, zoneName string) *MockMetaServer {
	mms := &MockMetaServer{
		TcpAddr: addr, partitions: make(map[uint64]*MockMetaPartition, 0),
		ZoneName: zoneName,
		mc:       master.NewMasterClient([]string{hostAddr}, false),
		stopC:    make(chan bool),
	}
	return mms
}

func (mms *MockMetaServer) Start() {
	mms.register()
	go mms.start()
}

func (mms *MockMetaServer) Stop() {
	close(mms.stopC)
}

func (mms *MockMetaServer) register() {
	var err error
	var nodeID uint64
	var retry int
	for retry < 3 {
		nodeID, err = mms.mc.NodeAPI().AddMetaNode(mms.TcpAddr, mms.ZoneName, "1.0.0")
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
		retry++
	}
	if err != nil {
		panic(err)
	}
	mms.NodeID = nodeID
}

func (mms *MockMetaServer) start() {
	s := strings.Split(mms.TcpAddr, ColonSeparator)
	listener, err := net.Listen("tcp", ":"+s[1])
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	go func() {
		for {
			select {
			case <-mms.stopC:
				return
			default:
			}
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("accept conn occurred error,err is [%v]", err)
		}
		go mms.serveConn(conn)
	}
}

func (mms *MockMetaServer) serveConn(rc net.Conn) {
	fmt.Printf("remote[%v],local[%v]\n", rc.RemoteAddr(), rc.LocalAddr())
	conn, ok := rc.(*net.TCPConn)
	if !ok {
		rc.Close()
		return
	}
	conn.SetKeepAlive(true)
	conn.SetNoDelay(true)
	req := proto.NewPacket(context.Background())
	err := req.ReadFromConn(conn, proto.NoReadDeadlineTime)
	if err != nil {
		fmt.Printf("remote [%v] err is [%v]\n", conn.RemoteAddr(), err)
		return
	}
	fmt.Printf("remote [%v] req [%v]\n", conn.RemoteAddr(), req.GetOpMsg())
	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(req.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		responseAckErrToMaster(conn, req, err)
		return
	}
	switch req.Opcode {
	case proto.OpCreateMetaPartition:
		err = mms.handleCreateMetaPartition(conn, req, adminTask)
		fmt.Printf("meta node [%v] create meta partition,err:%v\n", mms.TcpAddr, err)
	case proto.OpMetaNodeHeartbeat:
		err = mms.handleHeartbeats(conn, req, adminTask)
		fmt.Printf("meta node [%v] heartbeat,err:%v\n", mms.TcpAddr, err)
	case proto.OpDeleteMetaPartition:
		err = mms.handleDeleteMetaPartition(conn, req, adminTask)
		fmt.Printf("meta node [%v] delete meta partition,err:%v\n", mms.TcpAddr, err)
	case proto.OpUpdateMetaPartition:
		err = mms.handleUpdateMetaPartition(conn, req, adminTask)
		fmt.Printf("meta node [%v] update meta partition,err:%v\n", mms.TcpAddr, err)
	case proto.OpLoadMetaPartition:
		err = mms.handleLoadMetaPartition(conn, req, adminTask)
		fmt.Printf("meta node [%v] load meta partition,err:%v\n", mms.TcpAddr, err)
	case proto.OpDecommissionMetaPartition:
		err = mms.handleDecommissionMetaPartition(conn, req, adminTask)
		fmt.Printf("meta node [%v] offline meta partition,err:%v\n", mms.TcpAddr, err)
	case proto.OpAddMetaPartitionRaftMember:
		err = mms.handleAddMetaPartitionRaftMember(conn, req, adminTask)
		fmt.Printf("meta node [%v] add meta partition raft member,id[%v],err:%v\n", mms.TcpAddr, adminTask.ID, err)
	case proto.OpRemoveMetaPartitionRaftMember:
		err = mms.handleRemoveMetaPartitionRaftMember(conn, req, adminTask)
		fmt.Printf("meta node [%v] remove meta partition raft member,id[%v],err:%v\n", mms.TcpAddr, adminTask.ID, err)
	case proto.OpAddMetaPartitionRaftLearner:
		err = mms.handleAddMetaPartitionRaftLearner(conn, req, adminTask)
		fmt.Printf("meta node [%v] add meta partition raft learner,id[%v],err:%v\n", mms.TcpAddr, adminTask.ID, err)
	case proto.OpPromoteMetaPartitionRaftLearner:
		err = mms.handlePromoteMetaPartitionRaftLearner(conn, req, adminTask)
		fmt.Printf("meta node [%v] promote meta partition raft learner,id[%v],err:%v\n", mms.TcpAddr, adminTask.ID, err)
	case proto.OpResetMetaPartitionRaftMember:
		err = mms.handleResetMetaPartitionRaftMember(conn, req, adminTask)
		fmt.Printf("meta node [%v] reset data partition raft member,id[%v],err:%v\n", mms.TcpAddr, adminTask.ID, err)
	case proto.OpMetaPartitionTryToLeader:
		err = mms.handleTryToLeader(conn, req, adminTask)
		fmt.Printf("meta node [%v] try to leader,id[%v],err:%v\n", mms.TcpAddr, adminTask.ID, err)
	default:
		fmt.Printf("unknown code [%v]\n", req.Opcode)
	}
}

func (mms *MockMetaServer) handleAddMetaPartitionRaftMember(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mms *MockMetaServer) handleRemoveMetaPartitionRaftMember(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mms *MockMetaServer) handleResetMetaPartitionRaftMember(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mms *MockMetaServer) handleAddMetaPartitionRaftLearner(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mms *MockMetaServer) handlePromoteMetaPartitionRaftLearner(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mms *MockMetaServer) handleTryToLeader(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mms *MockMetaServer) handleCreateMetaPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	defer func() {
		if err != nil {
			responseAckErrToMaster(conn, p, err)
		} else {
			responseAckOKToMaster(conn, p, nil)
		}
	}()
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	req := &proto.CreateMetaPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	// Create new  metaPartition.
	partition := &MockMetaPartition{
		PartitionID: req.PartitionID,
		VolName:     req.VolName,
		Start:       req.Start,
		End:         req.End,
		Cursor:      req.Start,
		Members:     req.Members,
		Learners:    req.Learners,
	}
	mms.Lock()
	mms.partitions[req.PartitionID] = partition
	mms.Unlock()
	return
}

// Handle OpHeartbeat packet.
func (mms *MockMetaServer) handleHeartbeats(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	// For ack to master
	responseAckOKToMaster(conn, p, nil)
	var (
		req     = &proto.HeartBeatRequest{}
		resp    = &proto.MetaNodeHeartbeatResponse{}
		reqData []byte
	)
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		goto end
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		goto end
	}
	resp.Total = 10 * util.GB
	resp.Used = 1 * util.GB
	// every partition used
	mms.RLock()
	for id, partition := range mms.partitions {
		mpr := &proto.MetaPartitionReport{
			PartitionID: id,
			Start:       partition.Start,
			End:         partition.End,
			Status:      proto.ReadWrite,
			MaxInodeID:  1,
			VolName:     partition.VolName,
		}
		mpr.Status = proto.ReadWrite
		mpr.IsLeader = true
		resp.MetaPartitionReports = append(resp.MetaPartitionReports, mpr)
	}
	mms.RUnlock()
	resp.ZoneName = mms.ZoneName
	resp.Status = proto.TaskSucceeds
	resp.RocksDBDiskInfo = []*proto.MetaNodeDiskInfo{
		{Total: 1 * util.TB, Used: 12 * util.GB, Path: "/data0", UsageRatio: float64(12) / float64(1024)},
	}
end:
	return mms.postResponseToMaster(adminTask, resp)
}

func (mms *MockMetaServer) postResponseToMaster(adminTask *proto.AdminTask, resp interface{}) (err error) {
	adminTask.Request = nil
	adminTask.Response = resp
	if err = mms.mc.NodeAPI().ResponseMetaNodeTask(adminTask); err != nil {
		return
	}
	return
}

func (mms *MockMetaServer) handleDeleteMetaPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	req := &proto.DeleteMetaPartitionRequest{}
	reqData, err := json.Marshal(adminTask.Request)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		responseAckErrToMaster(conn, p, err)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		responseAckErrToMaster(conn, p, err)
		return
	}
	resp := &proto.DeleteMetaPartitionResponse{
		PartitionID: req.PartitionID,
		Status:      proto.TaskSucceeds,
	}
	return mms.postResponseToMaster(adminTask, resp)
}

func (mms *MockMetaServer) handleUpdateMetaPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	req := &proto.UpdateMetaPartitionRequest{}
	reqData, err := json.Marshal(adminTask.Request)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		responseAckErrToMaster(conn, p, err)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		responseAckErrToMaster(conn, p, err)
		return
	}
	resp := &proto.UpdateMetaPartitionResponse{
		VolName:     req.VolName,
		PartitionID: req.PartitionID,
		End:         req.End,
	}
	mms.Lock()
	partition := mms.partitions[req.PartitionID]
	partition.End = req.End
	mms.Unlock()
	return mms.postResponseToMaster(adminTask, resp)
}

func (mms *MockMetaServer) handleLoadMetaPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	var data []byte
	defer func() {
		if err != nil {
			responseAckErrToMaster(conn, p, err)
		} else {
			responseAckOKToMaster(conn, p, data)
		}
	}()
	req := &proto.MetaPartitionLoadRequest{}
	reqData, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	resp := &proto.MetaPartitionLoadResponse{
		PartitionID: req.PartitionID,
		DoCompare:   true,
		ApplyID:     100,
		MaxInode:    123456,
		DentryCount: 123456,
	}
	data, err = json.Marshal(resp)
	if err != nil {
		return
	}
	return
}

func (mms *MockMetaServer) handleDecommissionMetaPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	req := &proto.MetaPartitionDecommissionRequest{}
	reqData, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		return
	}
	resp := &proto.MetaPartitionDecommissionResponse{
		PartitionID: req.PartitionID,
		VolName:     req.VolName,
		Status:      proto.TaskSucceeds,
	}
	return mms.postResponseToMaster(adminTask, resp)
}
