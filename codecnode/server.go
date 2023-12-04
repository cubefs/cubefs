// Copyright 2020 The CubeFS Authors.
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

package codecnode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"net"
	"regexp"
	"strings"
	"sync"
)

var (
	LocalIP      string
	MasterClient = masterSDK.NewMasterClient(nil, false)
)

const (
	ModuleName           = "codecNode"
	codecNodeLastVersion = proto.BaseVersion
)

// Network protocol
const (
	NetworkProtocol = "tcp"
)

type CodecServer struct {
	clusterID       string
	port            string
	nodeID          uint64
	localServerAddr string

	masters []string

	tcpListener   net.Listener
	stopC         chan bool
	migrateStatus sync.Map //key:partitionID value:needStop(bool)
	wg            sync.WaitGroup
}

func NewServer() *CodecServer {
	return &CodecServer{}
}

func (s *CodecServer) Start(cfg *config.Config) (err error) {
	if err = s.parseConfig(cfg); err != nil {
		return
	}

	s.stopC = make(chan bool, 0)
	if err = s.register(cfg); err != nil {
		return
	}

	s.wg.Add(1)

	// start tcp listening
	err = s.startTCPService()
	if err != nil {
		return
	}

	return
}

func (s *CodecServer) Shutdown() {
	s.wg.Done()
}

func (s *CodecServer) Sync() {
	s.wg.Wait()
}

func (s *CodecServer) parseConfig(cfg *config.Config) error {
	port := cfg.GetString(proto.ListenPort)
	regexpPort, err := regexp.Compile("^(\\d)+$")
	if err != nil {
		return fmt.Errorf("Err: lack of port, err(%v)", err)
	}
	if !regexpPort.MatchString(port) {
		return fmt.Errorf("Err: invalid port, port(%v)", port)
	}
	s.port = port

	s.masters = cfg.GetStringSlice(proto.MasterAddr)

	if len(cfg.GetSlice(proto.MasterAddr)) == 0 {
		return fmt.Errorf("Err: no masterAddr specified")
	}
	for _, ip := range cfg.GetSlice(proto.MasterAddr) {
		MasterClient.AddNode(ip.(string))
	}

	return nil
}

func (s *CodecServer) startTCPService() (err error) {
	log.LogInfo("Start: startTCPService")
	addr := fmt.Sprintf(":%v", s.port)
	l, err := net.Listen(NetworkProtocol, addr)
	log.LogDebugf("action[startTCPService] listen %v address(%v).", NetworkProtocol, addr)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	s.tcpListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[startTCPService] failed to accept, err:%s", err.Error())
				break
			}
			log.LogDebugf("action[startTCPService] accept connection from %s.", conn.RemoteAddr().String())
			go s.serverConn(conn)
		}
	}(l)
	return
}

func (s *CodecServer) stopTCPService() (err error) {
	if s.tcpListener != nil {

		s.tcpListener.Close()
		log.LogDebugf("action[stopTCPService] stop tcp service.")
	}
	return
}

func (s *CodecServer) serverConn(conn net.Conn) {
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	packetProcessor := repl.NewReplProtocol(c, func(p *repl.Packet, remote string) (err error) { return }, s.OperatePacket, func(p *repl.Packet) error { return nil })
	packetProcessor.ServerConn()
}

func (s *CodecServer) register(cfg *config.Config) (err error){
	var (
		regInfo = &masterSDK.RegNodeInfoReq{
			Role:     proto.RoleCodec,
			Version:  codecNodeLastVersion,
			SrvPort:  s.port,
		}
		regRsp *proto.RegNodeRsp
	)


	if regRsp, err = MasterClient.RegNodeInfo(proto.AuthFilePath, regInfo); err != nil {
		return
	}
	ipAddr := strings.Split(regRsp.Addr, ":")[0]
	if !unit.IsIPV4(ipAddr) {
		err = fmt.Errorf("got invalid local IP %v fetched from Master", ipAddr)
		return
	}
	s.clusterID = regRsp.Cluster
	if LocalIP == "" {
		LocalIP = ipAddr
	}
	s.localServerAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)
	s.nodeID = regRsp.Id

	return
}
