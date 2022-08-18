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
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util"
)

type MockEcPartition struct {
	PartitionID      uint64
	PersistenceHosts []string
	total            uint64
	used             uint64
	VolName          string
	NodeIndex        uint32
}

type MockEcServer struct {
	nodeID                          uint64
	TcpAddr                         string
	HttpPort                        string
	Zone                            string
	ClusterID                       string
	Total                           uint64
	Used                            uint64
	Available                       uint64
	CreatedPartitionWeights         uint64 //dataPartitionCnt*dataPartitionSize
	RemainWeightsForCreatePartition uint64 //all-used ec Partitions Wieghts
	CreatedPartitionCnt             uint64
	MaxWeightsForCreatePartition    uint64
	partitions                      []*MockEcPartition
	zoneName                        string
	mc                              *master.MasterClient
}

func NewMockEcServer(addr string, httpPort string, zoneName string) *MockEcServer {
	ecs := &MockEcServer{
		TcpAddr:    addr,
		HttpPort:   httpPort,
		zoneName:   zoneName,
		partitions: make([]*MockEcPartition, 0),
		mc:         master.NewMasterClient([]string{hostAddr}, false),
	}

	return ecs
}

func (ecs *MockEcServer) Start() {
	ecs.register()
	go ecs.start()
}

func (ecs *MockEcServer) register() {
	var err error
	var nodeID uint64
	var retry int
	for retry < 3 {
		nodeID, err = ecs.mc.NodeAPI().AddEcNode(ecs.TcpAddr, ecs.HttpPort, ecs.zoneName, proto.Version)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
		retry++
		newPort := fmt.Sprintf("1031%d", retry)
		ecs.HttpPort = newPort
	}
	ecs.nodeID = nodeID
}

func (ecs *MockEcServer) start() {
	listener, err := net.Listen("tcp", ecs.TcpAddr)
	if err != nil {
		panic(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		go ecs.serveConn(conn)
	}
}

func (ecs *MockEcServer) serveConn(rc net.Conn) {
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
	case proto.OpCreateEcDataPartition:
		err = ecs.handleCreateEcPartition(conn, req, adminTask)
		fmt.Printf("data node [%v] create data partition,id[%v],err:%v\n", ecs.TcpAddr, adminTask.ID, err)
	case proto.OpDeleteEcDataPartition:
		err = ecs.handleDeleteEcPartition(conn, req)
		fmt.Printf("data node [%v] delete data partition,id[%v],err:%v\n", ecs.TcpAddr, adminTask.ID, err)
	case proto.OpEcNodeHeartbeat:
		err = ecs.handleHeartbeats(conn, req, adminTask)
		fmt.Printf("data node [%v] report heartbeat to master,err:%v\n", ecs.TcpAddr, err)
	case proto.OpChangeEcPartitionMembers:
		err = ecs.handleChangeEcPartitionMembers(conn, req, adminTask)
		fmt.Printf("data node [%v] remove data partition raft member,id[%v],err:%v\n", ecs.TcpAddr, adminTask.ID, err)
	default:
		fmt.Printf("unknown code [%v]\n", req.Opcode)
	}
}

func (ecs *MockEcServer) handleChangeEcPartitionMembers(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, p, nil)
	return
}

func (ecs *MockEcServer) handleCreateEcPartition(conn net.Conn, p *proto.Packet, adminTask *proto.AdminTask) (err error) {
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
	req := &proto.CreateEcPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	// Create new  ecPartition.
	partition := &MockEcPartition{
		PartitionID: req.PartitionID,
		VolName:     req.VolumeID,
		total:       req.PartitionSize,
		used:        10 * util.GB,
		NodeIndex:   req.NodeIndex,
	}
	ecs.partitions = append(ecs.partitions, partition)
	return
}

// Handle OpHeartbeat packet.
func (ecs *MockEcServer) handleHeartbeats(conn net.Conn, pkg *proto.Packet, task *proto.AdminTask) (err error) {
	responseAckOKToMaster(conn, pkg, nil)
	response := &proto.EcNodeHeartbeatResponse{}
	response.Status = proto.TaskSucceeds
	response.Used = 5 * util.GB
	response.Total = 1024 * util.GB
	response.Available = 1024 * util.GB
	response.CreatedPartitionCnt = 3
	response.TotalPartitionSize = 120 * util.GB
	response.MaxCapacity = 800 * util.GB
	response.CellName = ecs.zoneName

	response.PartitionReports = make([]*proto.EcPartitionReport, 0)

	for _, partition := range ecs.partitions {
		vr := &proto.EcPartitionReport{
			PartitionID:     partition.PartitionID,
			PartitionStatus: proto.ReadWrite,
			Total:           120 * util.GB,
			Used:            20 * util.GB,
			DiskPath:        "/cfs",
			ExtentCount:     10,
			NeedCompare:     true,
			IsLeader:        true, //todo
			VolName:         partition.VolName,
			NodeIndex:       partition.NodeIndex,
		}
		response.PartitionReports = append(response.PartitionReports, vr)
	}

	task.Response = response
	if err = ecs.mc.NodeAPI().ResponseEcNodeTask(task); err != nil {
		return
	}
	return
}

func (ecs *MockEcServer) handleDeleteEcPartition(conn net.Conn, pkg *proto.Packet) (err error) {
	err = responseAckOKToMaster(conn, pkg, nil)
	return
}
