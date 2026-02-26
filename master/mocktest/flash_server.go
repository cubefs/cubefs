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
	syslog "log"
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
		nodeID, err = mfs.mc.NodeAPI().AddFlashNode(mfs.TCPAddr, mfs.zoneName, "", proto.DefaultRegion, 0)
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

	p := proto.NewPacket()
	err := p.ReadFromConn(conn, proto.NoReadDeadlineTime)
	if err != nil {
		return
	}
	switch p.Opcode {
	case proto.OpFlashNodeHeartbeat:
		mfs.handleHeartbeats(conn, p)
	default:
		syslog.Printf("[mocktest] flashnode unknown code [%d]", p.Opcode)
	}
}

func (mfs *MockFlashServer) handleHeartbeats(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data
	if err = responseAckOKToMaster(conn, p, nil); err != nil {
		return
	}
	req := &proto.HeartBeatRequest{}
	resp := &proto.FlashNodeHeartbeatResponse{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		syslog.Printf("handleHeartbeats, flashNode:%v, p.reqID %v, topo:%v failed:len %v %v", conn.LocalAddr().String(),
			p.ReqID, req.TopoName, len(data), err.Error())
		return
	}
	resp.Stat = make([]*proto.FlashNodeDiskCacheStat, 0)
	resp.LimiterStatus = &proto.FlashNodeLimiterStatusInfo{}
	resp.FlashNodeTaskCountLimit = 8
	resp.ManualScanningTasks = make(map[string]*proto.FlashNodeManualTaskResponse)
	resp.Status = proto.TaskSucceeds
	resp.TopoName = req.TopoName
	resp.ZoneName = mfs.zoneName
	adminTask.Response = resp
	adminTask.TopoName = req.TopoName
	if err = mfs.mc.NodeAPI().ResponseFlashNodeTask(adminTask); err != nil {
		return
	}
	return
}
