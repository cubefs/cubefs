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

package mocktest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/unit"
)

const (
	defaultUsedSize = 20 * unit.GB
)

type MockDataServer struct {
	nodeID                          uint64
	TcpAddr                         string
	Zone                            string
	ClusterID                       string
	Total                           uint64
	Used                            uint64
	Available                       uint64
	CreatedPartitionWeights         uint64 //dataPartitionCnt*dataPartitionSize
	RemainWeightsForCreatePartition uint64 //all-useddataPartitionsWieghts
	CreatedPartitionCnt             uint64
	MaxWeightsForCreatePartition    uint64
	partitions                      []*MockDataPartition
	zoneName                        string
	mc                              *master.MasterClient
	stopC                           chan bool
}

func NewMockDataServer(addr string, zoneName string) *MockDataServer {
	mds := &MockDataServer{
		TcpAddr:    addr,
		zoneName:   zoneName,
		partitions: make([]*MockDataPartition, 0),
		mc:         master.NewMasterClient([]string{hostAddr}, false),
		stopC:      make(chan bool),
	}

	return mds
}

func (mds *MockDataServer) Start() {
	mds.register()
	go mds.start()
}

func (mds *MockDataServer) Stop() {
	close(mds.stopC)
}

func (mds *MockDataServer) register() {
	var err error
	var nodeID uint64
	var retry int
	for retry < 20 {
		nodeID, err = mds.mc.NodeAPI().AddDataNode(mds.TcpAddr, mds.zoneName, "1.0.0")
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
		retry++
	}
	if err != nil {
		panic(err)
	}
	mds.nodeID = nodeID
}

func (mds *MockDataServer) start() {
	listener, err := net.Listen("tcp", mds.TcpAddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	go func() {
		for {
			select {
			case <-mds.stopC:
				return
			default:
			}
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		go mds.serveConn(conn)
	}
}

func (mds *MockDataServer) serveConn(rc net.Conn) {
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
		return
	}
	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(req.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		responseAckErrToMaster(conn, req, err)
		return
	}
	switch req.Opcode {
	case proto.OpCreateDataPartition:
		err = mds.handleCreateDataPartition(conn, req, adminTask)
		fmt.Printf("data node [%v] create data partition,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpDeleteDataPartition:
		err = mds.handleDeleteDataPartition(conn, req)
		fmt.Printf("data node [%v] delete data partition,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpDataNodeHeartbeat:
		err = mds.handleHeartbeats(conn, req, adminTask)
		fmt.Printf("data node [%v] report heartbeat to master,err:%v\n", mds.TcpAddr, err)
	case proto.OpLoadDataPartition:
		err = mds.handleLoadDataPartition(conn, req, adminTask)
		fmt.Printf("data node [%v] load data partition,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpDecommissionDataPartition:
		err = mds.handleDecommissionDataPartition(conn, req, adminTask)
		fmt.Printf("data node [%v] decommission data partition,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpAddDataPartitionRaftMember:
		err = mds.handleAddDataPartitionRaftMember(conn, req, adminTask)
		fmt.Printf("data node [%v] add data partition raft member,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpRemoveDataPartitionRaftMember:
		err = mds.handleRemoveDataPartitionRaftMember(conn, req, adminTask)
		fmt.Printf("data node [%v] remove data partition raft member,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpResetDataPartitionRaftMember:
		err = mds.handleResetDataPartitionRaftMember(conn, req, adminTask)
		fmt.Printf("data node [%v] reset data partition raft member,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpDataPartitionTryToLeader:
		err = mds.handleTryToLeader(conn, req, adminTask)
		fmt.Printf("data node [%v] try to leader,id[%v],err:%v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpAddDataPartitionRaftLearner:
		err = mds.handleAddDataPartitionRaftLearner(conn, req, adminTask)
		fmt.Printf("data node [%v] add data partition raft learner, id[%v], err: %v\n", mds.TcpAddr, adminTask.ID, err)
	case proto.OpPromoteDataPartitionRaftLearner:
		err = mds.handlePromoteDataPartitionRaftLearner(conn, req, adminTask)
		fmt.Printf("data node [%v] promote data partition raft learner, id[%v], err: %v\n", mds.TcpAddr, adminTask.ID, err)
	default:
		fmt.Printf("unknown code [%v]\n", req.Opcode)
	}
}

func (mds *MockDataServer) handleAddDataPartitionRaftMember(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mds *MockDataServer) handleRemoveDataPartitionRaftMember(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mds *MockDataServer) handleAddDataPartitionRaftLearner(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mds *MockDataServer) handlePromoteDataPartitionRaftLearner(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mds *MockDataServer) handleResetDataPartitionRaftMember(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mds *MockDataServer) handleTryToLeader(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (mds *MockDataServer) handleDecommissionDataPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	defer func() {
		if err != nil {
			responseAckErrToMaster(conn, p, err)
		} else {
			p.PacketOkWithBody([]byte("/cfs"))
			p.WriteToConn(conn, proto.WriteDeadlineTime)
		}
	}()
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	req := &proto.DataPartitionDecommissionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	partitions := make([]*MockDataPartition, 0)
	for index, dp := range mds.partitions {
		if dp.PartitionID == req.PartitionId {
			partitions = append(partitions, mds.partitions[:index]...)
			partitions = append(partitions, mds.partitions[index+1:]...)
		}
	}
	if len(partitions) != 0 {
		mds.partitions = partitions
	}
	return
}

func (mds *MockDataServer) handleCreateDataPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
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
	req := &proto.CreateDataPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	// Create new  partition.
	partition := &MockDataPartition{
		PartitionID: req.PartitionId,
		VolName:     req.VolumeId,
		total:       req.PartitionSize,
		used:        defaultUsedSize,
	}
	mds.partitions = append(mds.partitions, partition)
	return
}

// Handle OpHeartbeat packet.
func (mds *MockDataServer) handleHeartbeats(conn net.Conn, pkg *proto.Packet, task *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, pkg, nil)
	response := &proto.DataNodeHeartbeatResponse{}
	response.Status = proto.TaskSucceeds
	response.Used = 5 * unit.GB
	response.Total = 1024 * unit.GB
	response.Available = 1024 * unit.GB
	response.CreatedPartitionCnt = 3
	response.TotalPartitionSize = 120 * unit.GB
	response.MaxCapacity = 800 * unit.GB
	response.RemainingCapacity = 800 * unit.GB

	response.ZoneName = mds.zoneName
	response.PartitionReports = make([]*proto.PartitionReport, 0)
	response.DiskInfos = make(map[string]*proto.DiskInfo, 0)

	for _, partition := range mds.partitions {
		vr := &proto.PartitionReport{
			PartitionID:     partition.PartitionID,
			PartitionStatus: proto.ReadWrite,
			Total:           120 * unit.GB,
			Used:            defaultUsedSize,
			DiskPath:        "/cfs",
			ExtentCount:     10,
			NeedCompare:     true,
			IsLeader:        true, //todo
			VolName:         partition.VolName,
		}
		response.PartitionReports = append(response.PartitionReports, vr)
	}
	response.DiskInfos["/cfs"] = &proto.DiskInfo{
		Total:         10 * unit.TB,
		Used:          8 * unit.TB,
		ReservedSpace: 0,
		Status:        0,
		Path:          "/cfs",
	}

	task.Response = response
	if err = mds.mc.NodeAPI().ResponseDataNodeTask(task); err != nil {
		return
	}
	return
}

func (mds *MockDataServer) handleDeleteDataPartition(conn net.Conn, pkg *proto.Packet) (err error) {
	err = responseAckOKToMaster(conn, pkg, nil)
	return
}

func (mds *MockDataServer) handleLoadDataPartition(conn net.Conn, pkg *proto.Packet, task *proto.AdminTask) (err error) {
	if err = responseAckOKToMaster(conn, pkg, nil); err != nil {
		return
	}
	// Marshal request body.
	requestJson, err := json.Marshal(task.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	req := &proto.LoadDataPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	partitionID := uint64(req.PartitionId)
	response := &proto.LoadDataPartitionResponse{}
	response.PartitionId = partitionID
	response.Used = defaultUsedSize
	response.PartitionSnapshot = buildSnapshot()
	response.Status = proto.TaskSucceeds
	var partition *MockDataPartition
	for _, partition = range mds.partitions {
		if partition.PartitionID == partitionID {
			break
		}
	}
	if partition == nil {
		return
	}
	//response.VolName = partition.VolName
	task.Response = response
	if err = mds.mc.NodeAPI().ResponseDataNodeTask(task); err != nil {
		return
	}
	return
}

func buildSnapshot() (files []*proto.File) {
	files = make([]*proto.File, 0)
	f1 := &proto.File{
		Name:     "1",
		Crc:      4045512210,
		Size:     2 * unit.MB,
		Modified: 1562507765,
	}
	files = append(files, f1)

	f2 := &proto.File{
		Name:     "2",
		Crc:      4045512210,
		Size:     2 * unit.MB,
		Modified: 1562507765,
	}
	files = append(files, f2)

	f3 := &proto.File{
		Name:     "50000010",
		Crc:      4045512210,
		Size:     2 * unit.MB,
		Modified: 1562507765,
	}
	files = append(files, f3)
	return
}
