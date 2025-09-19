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

package metanode

import (
	"fmt"
	"os"
	"strconv"

	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
)

// StartRaftServer initializes the address resolver and the raftStore server instance.
func (m *MetaNode) startRaftServer(cfg *config.Config) (err error) {
	// Create raft directory if it doesn't exist
	if err = m.ensureRaftDirectory(); err != nil {
		return fmt.Errorf("failed to ensure raft directory: %w", err)
	}

	// Handle cluster UUID if enabled
	if m.clusterUuidEnable {
		if err = m.handleClusterUuid(); err != nil {
			return fmt.Errorf("cluster UUID handling failed: %w", err)
		}
	}

	heartbeatPort, _ := strconv.Atoi(m.raftHeartbeatPort)
	replicaPort, _ := strconv.Atoi(m.raftReplicatePort)

	raftConf := &raftstore.Config{
		NodeID:            m.nodeId,
		RaftPath:          m.raftDir,
		IPAddr:            m.localAddr,
		HeartbeatPort:     heartbeatPort,
		ReplicaPort:       replicaPort,
		TickInterval:      m.tickInterval,
		RecvBufSize:       m.raftRecvBufSize,
		NumOfLogsToRetain: m.raftRetainLogs,
	}

	// Initialize raft store
	m.raftStore, err = raftstore.NewRaftStore(raftConf, cfg)
	if err != nil {
		return fmt.Errorf("failed to create raft store: %w", err)
	}

	log.LogInfof("Raft server started successfully on node %d", m.nodeId)
	return nil
}

// ensureRaftDirectory creates the raft directory if it doesn't exist
func (m *MetaNode) ensureRaftDirectory() error {
	_, err := os.Stat(m.raftDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to check raft directory %s: %w", m.raftDir, err)
		}
		if err = os.MkdirAll(m.raftDir, 0o755); err != nil {
			return fmt.Errorf("failed to create raft directory %s: %w", m.raftDir, err)
		}
		log.LogInfof("Created raft directory: %s", m.raftDir)
	}
	return nil
}

// handleClusterUuid handles cluster UUID validation and storage
func (m *MetaNode) handleClusterUuid() error {
	if err := config.CheckOrStoreClusterUuid(m.raftDir, m.clusterUuid, false); err != nil {
		log.LogErrorf("CheckOrStoreClusterUuid failed: %v", err)
		return fmt.Errorf("cluster UUID validation failed: %w", err)
	}
	return nil
}

// stopRaftServer stops the raft server gracefully
func (m *MetaNode) stopRaftServer() {
	if m.raftStore != nil {
		log.LogInfof("Stopping raft server on node %d", m.nodeId)
		m.raftStore.Stop()
		log.LogInfof("Raft server stopped successfully")
	}
}
