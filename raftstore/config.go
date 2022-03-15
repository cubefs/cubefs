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

package raftstore

import (
	"fmt"

	"github.com/tiglabs/raft/proto"
)

// Constants for network port definition.
const (
	DefaultHeartbeatPort     = 5901
	DefaultReplicaPort       = 5902
	DefaultNumOfLogsToRetain = 20000
	DefaultTickInterval      = 300
	DefaultElectionTick      = 3
)

// Config defines the configuration properties for the raft store.
type Config struct {
	NodeID            uint64 // Identity of raft server instance.
	RaftPath          string // Path of raft logs
	IPAddr            string // IP address
	HeartbeatPort     int
	ReplicaPort       int
	NumOfLogsToRetain uint64 // number of logs to be kept after truncation. The default value is 20000.

	// TickInterval is the interval of timer which check heartbeat and election timeout.
	// The default value is 300,unit is millisecond.
	TickInterval int

	// ElectionTick is the election timeout. If a follower does not receive any message
	// from the leader of current term during ElectionTick, it will become candidate and start an election.
	// ElectionTick must be greater than HeartbeatTick.
	// We suggest to use ElectionTick = 10 * HeartbeatTick to avoid unnecessary leader switching.
	// The default value is 1s.
	ElectionTick int

	MaxSnapConcurrency int
}

// PeerAddress defines the set of addresses that will be used by the peers.
type PeerAddress struct {
	proto.Peer
	Address       string
	HeartbeatPort int
	ReplicaPort   int
}

// PartitionConfig defines the configuration properties for the partitions.
type PartitionConfig struct {
	ID      uint64
	Applied uint64
	Leader  uint64
	Term    uint64
	Peers   []PeerAddress
	SM      PartitionFsm
	WalPath string
}

func (p PeerAddress) String() string {
	return fmt.Sprintf(`"nodeID":"%v","peerID":"%v","priority":"%v","type":"%v","heartbeatPort":"%v","ReplicaPort":"%v"`,
		p.ID, p.PeerID, p.Priority, p.Type.String(), p.HeartbeatPort, p.ReplicaPort)
}
