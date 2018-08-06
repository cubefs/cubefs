// Copyright 2018 The ChuBao Authors.
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
	"github.com/chubaoio/cbfs/raftstore"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

const (
	ColonSplit                  = ":"
	CommaSplit                  = ","
	CfgPeers                    = "peers"
	DataPartitionMissSec        = "dataPartitionMissSec"
	DataPartitionTimeOutSec     = "dataPartitionTimeOutSec"
	EveryLoadDataPartitionCount = "everyLoadDataPartitionCount"
	FileDelayCheckCrc           = "fileDelayCheckCrc"
	ReplicaNum                  = "replicaNum"
)

const (
	DefaultEveryReleaseDataPartitionCount       = 50
	DefaultReleaseDataPartitionAfterLoadSeconds = 5 * 60
	DefaultReleaseDataPartitionInternalSeconds  = 10
	DefaultCheckHeartbeatIntervalSeconds        = 60
	DefaultCheckDataPartitionIntervalSeconds    = 60
	DefaultFileDelayCheckLackSec                = 5 * DefaultCheckHeartbeatIntervalSeconds
	DefaultFileDelayCheckCrcSec                 = 20 * DefaultCheckHeartbeatIntervalSeconds
	NoHeartBeatTimes                            = 3
	DefaultNodeTimeOutSec                       = NoHeartBeatTimes * DefaultCheckHeartbeatIntervalSeconds
	DefaultDataPartitionTimeOutSec              = 10 * DefaultCheckHeartbeatIntervalSeconds
	DefaultDataPartitionMissSec                 = 24 * 3600
	DefaultDataPartitionWarnInterval            = 60 * 60
	LoadDataPartitionWaitTime                   = 100
	DefaultLoadDataPartitionFrequencyTime       = 60 * 60
	DefaultEveryLoadDataPartitionCount          = 50
	DefaultMetaPartitionTimeOutSec              = 10 * DefaultCheckHeartbeatIntervalSeconds
	//DefaultMetaPartitionMissSec                         = 3600
	DefaultMetaPartitionWarnInterval            = 10 * 60
	DefaultMetaPartitionThreshold       float32 = 0.75
	DefaultMetaPartitionCountOnEachNode         = 100
)

//AddrDatabase ...
var AddrDatabase = make(map[uint64]string)

type ClusterConfig struct {
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
	replicaNum                           int
	MetaNodeThreshold                    float32

	peers     []raftstore.PeerAddress
	peerAddrs []string
}

func NewClusterConfig() (cfg *ClusterConfig) {
	cfg = new(ClusterConfig)
	cfg.FileDelayCheckCrcSec = DefaultFileDelayCheckCrcSec
	cfg.FileDelayCheckLackSec = DefaultFileDelayCheckLackSec
	cfg.everyReleaseDataPartitionCount = DefaultEveryReleaseDataPartitionCount
	cfg.releaseDataPartitionAfterLoadSeconds = DefaultReleaseDataPartitionAfterLoadSeconds
	cfg.NodeTimeOutSec = DefaultNodeTimeOutSec
	cfg.DataPartitionMissSec = DefaultDataPartitionMissSec
	cfg.DataPartitionTimeOutSec = DefaultDataPartitionTimeOutSec
	cfg.CheckDataPartitionIntervalSeconds = DefaultCheckDataPartitionIntervalSeconds
	cfg.DataPartitionWarnInterval = DefaultDataPartitionWarnInterval
	cfg.everyLoadDataPartitionCount = DefaultEveryLoadDataPartitionCount
	cfg.LoadDataPartitionFrequencyTime = DefaultLoadDataPartitionFrequencyTime
	cfg.MetaNodeThreshold = DefaultMetaPartitionThreshold
	return
}

func parsePeerAddr(peerAddr string) (id uint64, ip string, port uint64, err error) {
	peerStr := strings.Split(peerAddr, ColonSplit)
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

func (cfg *ClusterConfig) parsePeers(peerStr string) error {
	peerArr := strings.Split(peerStr, CommaSplit)
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
