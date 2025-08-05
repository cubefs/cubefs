// Copyright 2018 The CubeFS Authors.
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
	syslog "log"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/storage/wal"
	raftlog "github.com/cubefs/cubefs/depends/tiglabs/raft/util/log"
	utilConfig "github.com/cubefs/cubefs/util/config"
)

// RaftStore defines the interface for the raft store.
type RaftStore interface {
	CreatePartition(cfg *PartitionConfig) (Partition, error)
	Stop()
	RaftConfig() *raft.Config
	RaftStatus(raftID uint64) (raftStatus *raft.Status)
	NodeManager
	RaftServer() *raft.RaftServer
	RemoveBackup(id uint64) error
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

// AddNodeWithPort add a new node with the given port.
func (s *raftStore) AddNodeWithPort(nodeID uint64, addr string, heartbeat int, replicate int) {
	s.resolver.AddNodeWithPort(nodeID, addr, heartbeat, replicate)
}

// DeleteNode deletes the node with the given ID in the raft store.
func (s *raftStore) DeleteNode(nodeID uint64) {
	s.resolver.DeleteNode(nodeID)
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
				os.MkdirAll(raftLogPath, 0o755)
			}
		}
	}

	raftLog, err := raftlog.NewLog(raftLogPath, "raft", "debug")
	if err != nil {
		syslog.Println("Fatal: failed to start the baud storage daemon - ", err)
		return
	}
	logger.SetLogger(raftLog)
}

// NewRaftStore returns a new raft store instance.
func NewRaftStore(cfg *Config, extendCfg *utilConfig.Config) (mr RaftStore, err error) {
	resolver := NewNodeResolver()

	newRaftLogger(cfg.RaftPath)
	setMonitorConf(extendCfg)

	rc := raft.DefaultConfig()
	rc.NodeID = cfg.NodeID
	rc.LeaseCheck = true
	rc.PreVote = true
	if cfg.HeartbeatPort <= 0 {
		cfg.HeartbeatPort = DefaultHeartbeatPort
	}
	if cfg.ReplicaPort <= 0 {
		cfg.ReplicaPort = DefaultReplicaPort
	}
	if cfg.NumOfLogsToRetain == 0 {
		cfg.NumOfLogsToRetain = DefaultNumOfLogsToRetain
	}
	if cfg.ElectionTick < DefaultElectionTick {
		cfg.ElectionTick = DefaultElectionTick
	}
	if cfg.TickInterval < DefaultTickInterval {
		cfg.TickInterval = DefaultTickInterval
	}
	// if cfg's RecvBufSize bigger than the default 2048,
	// use the bigger one.
	if cfg.RecvBufSize > rc.ReqBufferSize {
		rc.ReqBufferSize = cfg.RecvBufSize
	}
	rc.HeartbeatAddr = fmt.Sprintf("%s:%d", cfg.IPAddr, cfg.HeartbeatPort)
	rc.ReplicateAddr = fmt.Sprintf("%s:%d", cfg.IPAddr, cfg.ReplicaPort)
	rc.Resolver = resolver
	rc.RetainLogs = cfg.NumOfLogsToRetain
	rc.TickInterval = time.Duration(cfg.TickInterval) * time.Millisecond
	rc.ElectionTick = cfg.ElectionTick
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

func (s *raftStore) RaftServer() *raft.RaftServer {
	return s.raftServer
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
	logger.Info("action[raftstore:CreatePartition] raft config applied [%v] id:%d", cfg.Applied, cfg.ID)
	rc := &raft.RaftConfig{
		ID:           cfg.ID,
		Peers:        peers,
		Leader:       cfg.Leader,
		Term:         cfg.Term,
		Storage:      ws,
		StateMachine: cfg.SM,
		Applied:      cfg.Applied,
		Monitor:      newMonitor(),
	}
	if err = s.raftServer.CreateRaft(rc); err != nil {
		return
	}
	p = newPartition(cfg, s.raftServer, walPath)
	return
}

func (s *raftStore) RemoveBackup(id uint64) error {
	dirName := "del_" + strconv.FormatUint(id, 10)
	dirPath := path.Join(s.raftPath, dirName)
	return os.RemoveAll(dirPath)
}
