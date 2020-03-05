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
	"os"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
)

func TestLoadEcPartition(t *testing.T) {
	startEcNode(t)
	time.Sleep(5 * time.Second)

	fmt.Println("Load EcPartition success, wait 300s")
	time.Sleep(300 * time.Second)
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
        "192.168.0.11:17010",
        "192.168.0.12:17010",
        "192.168.0.13:17010"
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
	fmt.Println("Start ecnode daemon")
	return ecNode
}
