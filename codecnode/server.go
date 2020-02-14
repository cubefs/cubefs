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

package codecnode

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

var (
	LocalIP      string
	MasterClient = masterSDK.NewMasterClient(nil, false)
)

const (
	ModuleName = "codecNode"
)

type CodecServer struct {
	clusterID       string
	port            string
	nodeID          uint64
	localServerAddr string

	stopC chan bool

	wg sync.WaitGroup
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

	addrs := cfg.GetArray(proto.MasterAddr)
	if len(addrs) == 0 {
		return fmt.Errorf("Err: no masterAddr specified")
	}
	for _, ip := range addrs {
		MasterClient.AddNode(ip.(string))
	}

	return nil
}

func (s *CodecServer) register(cfg *config.Config) {
	timer := time.NewTimer(0)

	for {
		select {
		case <-timer.C:
			masterAddr := MasterClient.Leader()
			// Get cluster info
			ci, err := MasterClient.AdminAPI().GetClusterInfo()
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot get ip from master(%v) err(%v).", masterAddr, err)
				timer.Reset(2 * time.Second)
				continue
			}
			s.clusterID = ci.Cluster

			// Get local ip address
			if LocalIP == "" {
				LocalIP = string(ci.Ip)
			}
			if !util.IsIPV4(LocalIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip(%v) from master(%v).", LocalIP, masterAddr)
				timer.Reset(2 * time.Second)
				continue
			}
			s.localServerAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)

			// Get node id allocated by master
			nodeID, err := MasterClient.NodeAPI().AddCodecNode(fmt.Sprintf("%s:%v", LocalIP, s.port))
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot register this node to master(%v) err(%v).", masterAddr, err)
				timer.Reset(2 * time.Second)
				continue
			}
			s.nodeID = nodeID

			// Register monitor module
			exporter.RegistConsul(s.clusterID, ModuleName, cfg)

			// Success
			log.LogDebugf("register: register CodecNode: nodeID(%v)", s.nodeID)
			return
		case <-s.stopC:
			timer.Stop()
			return
		}
	}
}
