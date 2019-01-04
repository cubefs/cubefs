// Copyright 2018 The Container File System Authors.
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
	colonSplit                           = ":"
	commaSplit                           = ","
	cfgPeers                             = "peers"
	dataPartitionMissSec                 = "dataPartitionMissSec" // TODO what is dataPartitionMissSec
	dataPartitionTimeOutSec              = "dataPartitionTimeOutSec"
	everyLoadDataPartitionCount          = "everyLoadDataPartitionCount" // TODO what is everyLoadDataPartitionCount ?
	releaseDataPartitionAfterLoadSeconds = "releaseDataPartitionAfterLoadSeconds"
	nodeSetCapacity                      = "nodeSetCap"
)

// TODO the followings seem to be related to intervals
// TODO Suggested naming: defaultIntervalToXXX
const (
	defaultEveryReleaseDataPartitionCount       = 1000 // TODO explain
	defaultReleaseDataPartitionAfterLoadSeconds = 5 * 60 // TODO explain
	defaultReleaseDataPartitionInternalSeconds  = 10
	defaultCheckHeartbeatIntervalSeconds        = 60
	defaultIntervalToCheckDataPartition   = 60
	defaultFileDelayCheckLackSec          = 5 * defaultCheckHeartbeatIntervalSeconds // TODO explain
	defaultFileDelayCheckCrcSec           = 20 * defaultCheckHeartbeatIntervalSeconds // TODO explain
	noHeartBeatTimes                      = 3 // TODO explain
	defaultNodeTimeOutSec                 = noHeartBeatTimes * defaultCheckHeartbeatIntervalSeconds
	defaultDataPartitionTimeOutSec        = 10 * defaultCheckHeartbeatIntervalSeconds
	defaultDataPartitionMissSec           = 24 * 3600 // TODO explain

	// TODO change to defaultIntervalToCheckMissingDP ?
	defaultDataPartitionWarningInterval   = 60 * 60
	loadDataPartitionWaitTime             = 120 // TODO explain
	defaultLoadDataPartitionFrequencyTime = 60 * 60 * 4 // TODO explain
	defaultEveryLoadDataPartitionCount    = 50  // TODO what is defaultEveryLoadDataPartitionCount?
	defaultMetaPartitionTimeOutSec        = 10 * defaultCheckHeartbeatIntervalSeconds
	//DefaultMetaPartitionMissSec                         = 3600

	// TODO change to defaultIntervalToCheckMissingMP?
	defaultMetaPartitionWarningInterval            = 10 * 60 // interval of checking if a replica is missing
	defaultMetaPartitionMemUsageThreshold  float32 = 0.75 // memory usage threshold on a meta partition
	defaultMaxMetaPartitionCountOnEachNode         = 100
)

// AddrDatabase is a map that stores the address of a given host (e.g., the leader)
var AddrDatabase = make(map[uint64]string)

type clusterConfig struct {
	releaseDataPartitionAfterLoadSeconds int64 // TODO explain
	NodeTimeOutSec                       int64
	DataPartitionMissSec                 int64 // TODO explain
	DataPartitionTimeOutSec              int64
	DataPartitionWarningInterval         int64 // TODO rename? warning interval?
	LoadDataPartitionFrequencyTime       int64 // TODO explain
	IntervalToCheckDataPartition         int // seconds
	everyReleaseDataPartitionCount       int /// TODO explain
	everyLoadDataPartitionCount          int // TODO explain
	nodeSetCapacity                      int
	MetaNodeThreshold                    float32
	peers                                []raftstore.PeerAddress
	peerAddrs                            []string
}

func newClusterConfig() (cfg *clusterConfig) {
	cfg = new(clusterConfig)
	cfg.everyReleaseDataPartitionCount = defaultEveryReleaseDataPartitionCount
	cfg.releaseDataPartitionAfterLoadSeconds = defaultReleaseDataPartitionAfterLoadSeconds
	cfg.NodeTimeOutSec = defaultNodeTimeOutSec
	cfg.DataPartitionMissSec = defaultDataPartitionMissSec
	cfg.DataPartitionTimeOutSec = defaultDataPartitionTimeOutSec
	cfg.IntervalToCheckDataPartition = defaultIntervalToCheckDataPartition
	cfg.DataPartitionWarningInterval = defaultDataPartitionWarningInterval
	cfg.everyLoadDataPartitionCount = defaultEveryLoadDataPartitionCount
	cfg.LoadDataPartitionFrequencyTime = defaultLoadDataPartitionFrequencyTime
	cfg.MetaNodeThreshold = defaultMetaPartitionMemUsageThreshold
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
