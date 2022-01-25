// Copyright 2018 The Chubao Authors.
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

package authnode

import (
	"fmt"
	syslog "log"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/raftstore"
	"github.com/tiglabs/raft/proto"
)

//config key
const (
	colonSplit = ":"
	commaSplit = ","
	cfgPeers   = "peers"
	// if the data partition has not been reported within this interval  (in terms of seconds), it will be considered as missing.
	heartbeatPortKey = "heartbeatPort"
	replicaPortKey   = "replicaPort"
)

//default value
const (
	defaultIntervalToCheckHeartbeat = 60
)

type clusterConfig struct {
	peers         []raftstore.PeerAddress
	peerAddrs     []string
	heartbeatPort int64
	replicaPort   int64
}

// AddrDatabase is a map that stores the address of a given host (e.g., the leader)
var AddrDatabase = make(map[uint64]string)

func newClusterConfig() (cfg *clusterConfig) {
	cfg = new(clusterConfig)
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
		cfg.peers = append(cfg.peers, raftstore.PeerAddress{Peer: proto.Peer{ID: id}, Address: ip, HeartbeatPort: int(cfg.heartbeatPort), ReplicaPort: int(cfg.replicaPort)})
		address := fmt.Sprintf("%v:%v", ip, port)
		syslog.Println(address)
		AddrDatabase[id] = address
	}
	return nil
}
