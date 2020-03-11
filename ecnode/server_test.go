// Copyright 2020 The Chubao Authors.
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

package ecnode

import (
	"testing"

	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
)

func TestLoadEcPartition(t *testing.T) {
	startMasterSimulate()

	startEcNode(t)
}

func TestEcPartitionCreate(t *testing.T) {
	startEcNode(t)

	time.Sleep(5 * time.Second)

	addr := "127.0.0.1:27310"

	req := &proto.CreateEcPartitionRequest{
		PartitionID:   1,
		PartitionSize: 1024000,
		VolumeID:      "ltptest",
		DataNodeNum:   4,
		ParityNodeNum: 2,
		NodeIndex:     3,
		DataNodes:     []string{"192.168.0.35:17310", "192.168.0.34:17310", "192.168.0.33:17310", "192.168.0.32:17310"},
		ParityNodes:   []string{"192.168.0.37:17310", "192.168.0.36:17310"},
	}

	task := proto.NewAdminTask(proto.OpCreateEcPartition, addr, req)

	packet := proto.NewPacket()
	packet.Opcode = proto.OpCreateEcPartition
	packet.ReqID = 1
	packet.PartitionID = 1
	body, err := json.Marshal(task)
	if err != nil {
		fmt.Println("Marshal task failed")
		os.Exit(1)
	}
	packet.Size = uint32(len(body))
	packet.Data = body

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Connect ecnode failed")
		os.Exit(1)
	}

	if err = packet.WriteToConn(conn); err != nil {
		fmt.Println(err, "action[syncSendAdminTask], WriteToConn failed, task:%v, reqID[%v]", task.ID, packet.ReqID)
		os.Exit(1)
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		fmt.Println(err, "action[syncSendAdminTask], ReadFromConn failed, task:%v, reqID[%v]", task.ID, packet.ReqID)
		os.Exit(1)
	}
	if packet.ResultCode != proto.OpOk {
		fmt.Println("EcPartitionCreate response success failed")
		os.Exit(1)
	}

	fmt.Println("EcPartitionCreate response success, wait 300s")
	time.Sleep(300 * time.Second)
}

func startEcNode(t *testing.T) (e *EcNode) {
	ecNodeCfg := `
{
    "role": "ecnode",
    "listen": "27310",
    "prof": "27320",
    "raftDir":   "/tmp/cfs/log",
    "exporterPort": 9500,
    "logDir": "/tmp/cfs/log",
    "logLevel": "debug",
    "disks": [
        "/tmp/cfs/disk:10737418240"
    ],
    "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.1:17010",
        "127.0.0.1:17010"
    ]
}
`
	cfg := config.LoadConfigString(ecNodeCfg)

	_, err := log.InitLog(cfg.GetString("logDir"), "ecnode", log.DebugLevel, nil)
	if err != nil {
		fmt.Println("Fatal: failed to init log, ", err)
		os.Exit(1)
		return
	}

	ecNode := NewServer()
	go func() {
		err = ecNode.Start(cfg)
		if err != nil {
			t.Fatalf("Start ecnode server caused by: %v", err)
		}
	}()
	fmt.Println("Start Ecnode")
	return ecNode
}

func sendOKReply(w http.ResponseWriter, r *http.Request, data interface{}) {
	httpReply := &proto.HTTPReply{
		Code: proto.ErrCodeSuccess,
		Msg:  proto.ErrSuc.Error(),
		Data: data,
	}

	reply, err := json.Marshal(httpReply)
	if err != nil {
		fmt.Println("Marshal httpReply failed, ", err)
		os.Exit(1)
	}

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		fmt.Println("Write reply failed, ", err)
		os.Exit(1)
	}
}

func handleGetCluster(w http.ResponseWriter, r *http.Request) {
	cv := &proto.ClusterView{
		Name:                "ErasureCodeAutoTest",
		LeaderAddr:          "127.0.0.1:17010",
		DisableAutoAlloc:    false,
		MetaNodeThreshold:   0.75,
		Applied:             65,
		MaxDataPartitionID:  1,
		MaxMetaNodeID:       1,
		MaxMetaPartitionID:  1,
		MetaNodes:           make([]proto.NodeView, 0),
		DataNodes:           make([]proto.NodeView, 0),
		VolStatInfo:         make([]*proto.VolStatInfo, 0),
		BadPartitionIDs:     make([]proto.BadPartitionView, 0),
		BadMetaPartitionIDs: make([]proto.BadPartitionView, 0),
	}
	fmt.Println("Simulate Master HandleGetCluster Response")

	sendOKReply(w, r, cv)
	return
}

func handleGetIPAddr(w http.ResponseWriter, r *http.Request) {
	cInfo := &proto.ClusterInfo{
		Cluster: "ErasureCodeAutoTest",
		Ip:      strings.Split(r.RemoteAddr, ":")[0],
	}
	fmt.Println("Simulate Master HandleGetIPAddr Response")

	sendOKReply(w, r, cInfo)
	return
}

func handleAddEcNode(w http.ResponseWriter, r *http.Request) {
	nodeID := 10
	fmt.Println("Simulate Master HandleAddEcNode Response")

	sendOKReply(w, r, nodeID)
	return
}

func startMasterSimulate() {
	go func() {
		server := http.Server{
			Addr: "127.0.0.1:17010",
		}

		http.HandleFunc(proto.AdminGetIP, handleGetIPAddr)
		http.HandleFunc(proto.AdminGetCluster, handleGetCluster)
		http.HandleFunc(proto.AddEcNode, handleAddEcNode)
		err := server.ListenAndServe()
		if err != nil {
			fmt.Println("Start Master Simulate failed, ", err)
			os.Exit(1)
		}
	}()
	fmt.Println("Start Master Simulate Success")
}
