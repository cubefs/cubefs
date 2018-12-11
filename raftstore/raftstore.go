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

type RaftStore interface {
	CreatePartition(cfg *PartitionConfig) (Partition, error)
	Stop()
	RaftConfig() *raft.Config
	NodeManager
}

type raftStore struct {
	nodeId     uint64
	resolver   NodeResolver
	raftConfig *raft.Config
	raftServer *raft.RaftServer
	walPath    string
}

func (s *raftStore) RaftConfig() *raft.Config {
	return s.raftConfig
}

func (s *raftStore) AddNode(nodeId uint64, addr string) {
	if s.resolver != nil {
		s.resolver.AddNode(nodeId, addr)
	}
}

func (s *raftStore) AddNodeWithPort(nodeId uint64, addr string, heartbeat int, replicate int) {
	if s.resolver != nil {
		s.resolver.AddNodeWithPort(nodeId, addr, heartbeat, replicate)
	}
}

func (s *raftStore) DeleteNode(nodeId uint64) {
	DeleteNode(s.resolver, nodeId)
}

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

func NewRaftStore(cfg *Config) (mr RaftStore, err error) {
	resolver := NewNodeResolver()

	newRaftLogger(cfg.WalPath)

	rc := raft.DefaultConfig()
	rc.NodeID = cfg.NodeID
	rc.LeaseCheck = true
	if cfg.HeartbeatPort <= 0 {
		cfg.HeartbeatPort = DefaultHeartbeatPort
	}
	if cfg.ReplicatePort <= 0 {
		cfg.ReplicatePort = DefaultReplicatePort
	}
	if cfg.RetainLogs == 0 {
		cfg.RetainLogs = DefaultRetainLogs
	}
	rc.HeartbeatAddr = fmt.Sprintf("%s:%d", cfg.IpAddr, cfg.HeartbeatPort)
	rc.ReplicateAddr = fmt.Sprintf("%s:%d", cfg.IpAddr, cfg.ReplicatePort)
	rc.Resolver = resolver
	rc.RetainLogs = cfg.RetainLogs
	rc.TickInterval = 300 * time.Millisecond
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		return
	}
	mr = &raftStore{
		nodeId:     cfg.NodeID,
		resolver:   resolver,
		raftConfig: rc,
		raftServer: rs,
		walPath:    cfg.WalPath,
	}
	return
}

func (s *raftStore) CreatePartition(cfg *PartitionConfig) (p Partition, err error) {
	// Init WaL Storage for this partition.
	// Variables:
	// wc: WaL Configuration.
	// wp: WaL Path.
	// ws: WaL Storage.
	var walPath string
	if cfg.WalPath == "" {
		walPath = path.Join(s.walPath, strconv.FormatUint(cfg.ID, 10))
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
			peerAddress.ReplicatePort,
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
