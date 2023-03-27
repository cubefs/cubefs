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
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
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
	s.register(cfg)

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

func (s *CodecServer) register(cfg *config.Config) {
	timer := time.NewTimer(0)

	for {
		select {
		case <-timer.C:
			// Get cluster info
			ci, err := MasterClient.AdminAPI().GetClusterInfo()
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot get ip from master(%v) err(%v).", MasterClient.Leader(), err)
				timer.Reset(2 * time.Second)
				continue
			}

			masterAddr := MasterClient.Leader()
			s.clusterID = ci.Cluster

			// Get local ip address
			if LocalIP == "" {
				LocalIP = string(ci.Ip)
			}
			if !unit.IsIPV4(LocalIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip(%v) from master(%v).", LocalIP, masterAddr)
				timer.Reset(2 * time.Second)
				continue
			}
			s.localServerAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)

			// Get node id allocated by master
			nodeID, err := MasterClient.NodeAPI().AddCodecNode(fmt.Sprintf("%s:%v", LocalIP, s.port), codecNodeLastVersion)
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot register this node to master(%v) err(%v).", masterAddr, err)
				timer.Reset(2 * time.Second)
				continue
			}
			s.nodeID = nodeID

			// Register monitor module
			exporter.RegistConsul(cfg)

			// Success
			log.LogDebugf("register: register CodecNode: nodeID(%v)", s.nodeID)
			return
		case <-s.stopC:
			timer.Stop()
			return
		}
	}
}
