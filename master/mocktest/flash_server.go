// Copyright 2023 The CubeFS Authors.
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
	"encoding/json"
	"net"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

type MockFlashServer struct {
	nodeID    uint64
	TCPAddr   string
	ClusterID string
	Available uint64
	zoneName  string

	stopCh chan struct{}
	mc     *master.MasterClient
}

func NewMockFlashServer(addr, zoneName string) *MockFlashServer {
	return &MockFlashServer{
		TCPAddr:  addr,
		zoneName: zoneName,
		mc:       master.NewMasterClient([]string{hostAddr}, false),
		stopCh:   make(chan struct{}),
	}
}

func (mfs *MockFlashServer) Start() {
	mfs.register()
	go mfs.start()
}

func (mfs *MockFlashServer) Stop() {
	close(mfs.stopCh)
}

func (mfs *MockFlashServer) register() {
	var nodeID uint64
	var err error
	for range [100]struct{}{} {
		nodeID, err = mfs.mc.NodeAPI().AddFlashNode(mfs.TCPAddr, mfs.zoneName, "")
		if err == nil {
			mfs.nodeID = nodeID
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		panic(err)
	}
}

func (mfs *MockFlashServer) start() {
	listener, err := net.Listen("tcp", mfs.TCPAddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	go func() {
		<-mfs.stopCh
		listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		select {
		case <-mfs.stopCh:
			return
		default:
		}
		if err != nil {
			panic(err)
		}
		go mfs.serveConn(conn)
	}
}

func (mfs *MockFlashServer) serveConn(rc net.Conn) {
	conn, ok := rc.(*net.TCPConn)
	if !ok {
		rc.Close()
		return
	}
	conn.SetKeepAlive(true)
	conn.SetNoDelay(true)

	req := proto.NewPacket()
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
	case proto.OpFlashNodeHeartbeat:
		err = responseAckOKToMaster(conn, req, nil)
		Printf("[mocktest] flashnode [%s] report heartbeat to master err:%v\n", mfs.TCPAddr, err)
	default:
		Printf("[mocktest] flashnode unknown code [%d]\n", req.Opcode)
	}
}
