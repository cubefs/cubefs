// Copyright 2018 The Containerfs Authors.
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

package master

import (
	"fmt"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

const (
	colonSplit                  = ":"
	commaSplit                  = ","
	cfgPeers                    = "peers"
	dataPartitionMissSec        = "dataPartitionMissSec"
	dataPartitionTimeOutSec     = "dataPartitionTimeOutSec"
	everyLoadDataPartitionCount = "everyLoadDataPartitionCount"
	fileDelayCheckCrc           = "fileDelayCheckCrc"
	nodeSetCapacity             = "nodeSetCap"
)

const (
	defaultEveryReleaseDataPartitionCount       = 1000
	defaultReleaseDataPartitionAfterLoadSeconds = 5 * 60
	defaultReleaseDataPartitionInternalSeconds  = 10
	defaultCheckHeartbeatIntervalSeconds        = 60
	defaultCheckDataPartitionIntervalSeconds    = 60
	defaultFileDelayCheckLackSec                = 5 * defaultCheckHeartbeatIntervalSeconds
	defaultFileDelayCheckCrcSec                 = 20 * defaultCheckHeartbeatIntervalSeconds
	noHeartBeatTimes                            = 3
	defaultNodeTimeOutSec                       = noHeartBeatTimes * defaultCheckHeartbeatIntervalSeconds
	defaultDataPartitionTimeOutSec              = 10 * defaultCheckHeartbeatIntervalSeconds
	defaultDataPartitionMissSec                 = 24 * 3600
	defaultDataPartitionWarnInterval            = 60 * 60
	loadDataPartitionWaitTime                   = 120
	defaultLoadDataPartitionFrequencyTime       = 60 * 60 * 4
	defaultEveryLoadDataPartitionCount          = 50
	defaultMetaPartitionTimeOutSec              = 10 * defaultCheckHeartbeatIntervalSeconds
	//DefaultMetaPartitionMissSec                         = 3600
	defaultMetaPartitionWarnInterval            = 10 * 60
	defaultMetaPartitionThreshold       float32 = 0.75
	defaultMetaPartitionCountOnEachNode         = 100
)

//AddrDatabase ...
var AddrDatabase = make(map[uint64]string)

type clusterConfig struct {
	FileDelayCheckCrcSec                 int64
	FileDelayCheckLackSec                int64
	releaseDataPartitionAfterLoadSeconds int64
	NodeTimeOutSec                       int64
	DataPartitionMissSec                 int64
	DataPartitionTimeOutSec              int64
	DataPartitionWarnInterval            int64
	LoadDataPartitionFrequencyTime       int64
	CheckDataPartitionIntervalSeconds    int
	everyReleaseDataPartitionCount       int
	everyLoadDataPartitionCount          int
	nodeSetCapacity                      int
	MetaNodeThreshold                    float32

	peers     []raftstore.PeerAddress
	peerAddrs []string
}

func newClusterConfig() (cfg *clusterConfig) {
	cfg = new(clusterConfig)
	cfg.FileDelayCheckCrcSec = defaultFileDelayCheckCrcSec
	cfg.FileDelayCheckLackSec = defaultFileDelayCheckLackSec
	cfg.everyReleaseDataPartitionCount = defaultEveryReleaseDataPartitionCount
	cfg.releaseDataPartitionAfterLoadSeconds = defaultReleaseDataPartitionAfterLoadSeconds
	cfg.NodeTimeOutSec = defaultNodeTimeOutSec
	cfg.DataPartitionMissSec = defaultDataPartitionMissSec
	cfg.DataPartitionTimeOutSec = defaultDataPartitionTimeOutSec
	cfg.CheckDataPartitionIntervalSeconds = defaultCheckDataPartitionIntervalSeconds
	cfg.DataPartitionWarnInterval = defaultDataPartitionWarnInterval
	cfg.everyLoadDataPartitionCount = defaultEveryLoadDataPartitionCount
	cfg.LoadDataPartitionFrequencyTime = defaultLoadDataPartitionFrequencyTime
	cfg.MetaNodeThreshold = defaultMetaPartitionThreshold
	return
}

func parsePeerAddr(peerAddr string) (id uint64, ip string, port uint64, err error) {
	peerStr := strings.Split(peerAddr, colonSplit)
	id, err = strconv.ParseUint(peerStr[0], 10, 64)
	if err != nil {
		return
	}
	port, err = strconv.ParseUint(peerStr[2], 10, 64)
	if err != nil {
		return
	}
	ip = peerStr[1]
	return
}

func (cfg *clusterConfig) parsePeers(peerStr string) error {
	peerArr := strings.Split(peerStr, commaSplit)
	cfg.peerAddrs = peerArr
	for _, peerAddr := range peerArr {
		id, ip, port, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		cfg.peers = append(cfg.peers, raftstore.PeerAddress{Peer: proto.Peer{ID: id}, Address: ip})
		address := fmt.Sprintf("%v:%v", ip, port)
		fmt.Println(address)
		AddrDatabase[id] = address
	}
	return nil
}
