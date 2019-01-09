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

package raftstore

import (
	"github.com/tiglabs/raft/proto"
)

// Constants for network port definition.
const (
	DefaultHeartbeatPort = 5901
	DefaultReplicatePort = 5902
	DefaultRetainLogs    = 20000
)

// Config defined necessary configuration properties for raft store.
type Config struct {
	NodeID        uint64 // Identity of raft server instance.
	RaftPath      string // Path of raft logs
	IPAddr        string // IP address of node
	HeartbeatPort int
	ReplicaPort   int
	RetainLogs    uint64 // // RetainLogs controls how many logs we leave after truncate. The default value is 20000.
}

type PeerAddress struct {
	proto.Peer
	Address       string
	HeartbeatPort int
	ReplicaPort   int
}

// PartitionConfig defined necessary configuration properties for raft store partition.
type PartitionConfig struct {
	ID      uint64
	Applied uint64
	Leader  uint64
	Term    uint64
	Peers   []PeerAddress
	SM      PartitionFsm
	WalPath string
}
