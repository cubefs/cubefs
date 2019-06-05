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
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	raftlog "github.com/tiglabs/raft/util/log"
	"os"
	"path"
	"strconv"
	"time"
)

// RaftStore defines the interface for the raft store.
type RaftStore interface {
	CreatePartition(cfg *PartitionConfig) (Partition, error)
	Stop()
	RaftConfig() *raft.Config
	RaftStatus(raftID uint64) (raftStatus *raft.Status)
	NodeManager
}

type raftStore struct {
	nodeID     uint64
	resolver   NodeResolver
	raftConfig *raft.Config
	raftServer *raft.RaftServer
	raftPath   string
}

// RaftConfig returns the raft configuration.
func (s *raftStore) RaftConfig() *raft.Config {
	return s.raftConfig
}

func (s *raftStore) RaftStatus(raftID uint64) (raftStatus *raft.Status) {
	return s.raftServer.Status(raftID)
}

// AddNode adds a new node to the raft store.
func (s *raftStore) AddNode(nodeID uint64, addr string) {
	if s.resolver != nil {
		s.resolver.AddNode(nodeID, addr)
	}
}

// AddNodeWithPort add a new node with the given port.
func (s *raftStore) AddNodeWithPort(nodeID uint64, addr string, heartbeat int, replicate int) {
	if s.resolver != nil {
		s.resolver.AddNodeWithPort(nodeID, addr, heartbeat, replicate)
	}
}

// DeleteNode deletes the node with the given ID in the raft store.
func (s *raftStore) DeleteNode(nodeID uint64) {
	DeleteNode(s.resolver, nodeID)
}

// Stop stops the raft store server.
func (s *raftStore) Stop() {
	if s.raftServer != nil {
		s.raftServer.Stop()
	}
}

func newRaftLogger(dir string) {

	raftLogPath := path.Join(dir, "logs")
	_, err := os.Stat(raftLogPath)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			if os.IsNotExist(pathErr) {
				os.MkdirAll(raftLogPath, 0755)
			}
		}
	}

	raftLog, err := raftlog.NewLog(raftLogPath, "raft", "debug")
	if err != nil {
		fmt.Println("Fatal: failed to start the baud storage daemon - ", err)
		return
	}
	logger.SetLogger(raftLog)
	return
}

// NewRaftStore returns a new raft store instance.
func NewRaftStore(cfg *Config) (mr RaftStore, err error) {
	resolver := NewNodeResolver()

	newRaftLogger(cfg.RaftPath)

	rc := raft.DefaultConfig()
	rc.NodeID = cfg.NodeID
	rc.LeaseCheck = true
	if cfg.HeartbeatPort <= 0 {
		cfg.HeartbeatPort = DefaultHeartbeatPort
	}
	if cfg.ReplicaPort <= 0 {
		cfg.ReplicaPort = DefaultReplicaPort
	}
	if cfg.NumOfLogsToRetain == 0 {
		cfg.NumOfLogsToRetain = DefaultNumOfLogsToRetain
	}
	rc.HeartbeatAddr = fmt.Sprintf("%s:%d", cfg.IPAddr, cfg.HeartbeatPort)
	rc.ReplicateAddr = fmt.Sprintf("%s:%d", cfg.IPAddr, cfg.ReplicaPort)
	rc.Resolver = resolver
	rc.RetainLogs = cfg.NumOfLogsToRetain
	rc.TickInterval = 300 * time.Millisecond
	rc.ElectionTick = 3
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		return
	}
	mr = &raftStore{
		nodeID:     cfg.NodeID,
		resolver:   resolver,
		raftConfig: rc,
		raftServer: rs,
		raftPath:   cfg.RaftPath,
	}
	return
}

// CreatePartition creates a new partition in the raft store.
func (s *raftStore) CreatePartition(cfg *PartitionConfig) (p Partition, err error) {
	// Init WaL Storage for this partition.
	// Variables:
	// wc: WaL Configuration.
	// wp: WaL Path.
	// ws: WaL Storage.
	var walPath string
	if cfg.WalPath == "" {
		walPath = path.Join(s.raftPath, strconv.FormatUint(cfg.ID, 10))
	} else {
		walPath = path.Join(cfg.WalPath, "wal_"+strconv.FormatUint(cfg.ID, 10))
	}

	wc := &wal.Config{}
	ws, err := wal.NewStorage(walPath, wc)
	if err != nil {
		return
	}
	peers := make([]proto.Peer, 0)
	for _, peerAddress := range cfg.Peers {
		peers = append(peers, peerAddress.Peer)
		s.AddNodeWithPort(
			peerAddress.ID,
			peerAddress.Address,
			peerAddress.HeartbeatPort,
			peerAddress.ReplicaPort,
		)
	}
	rc := &raft.RaftConfig{
		ID:           cfg.ID,
		Peers:        peers,
		Leader:       cfg.Leader,
		Term:         cfg.Term,
		Storage:      ws,
		StateMachine: cfg.SM,
		Applied:      cfg.Applied,
	}
	if err = s.raftServer.CreateRaft(rc); err != nil {
		return
	}
	p = newPartition(cfg, s.raftServer, walPath)
	return
}
