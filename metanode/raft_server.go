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
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// StartRaftServer initializes the address resolver and the raftStore server instance.
func (m *MetaNode) startRaftServer(cfg *config.Config) (err error) {
	_, err = os.Stat(m.raftDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
		if err = os.MkdirAll(m.raftDir, 0755); err != nil {
			err = errors.NewErrorf("create raft server dir: %s", err.Error())
			return
		}
	}

	if m.clusterUuidEnable {
		if err = config.CheckOrStoreClusterUuid(m.raftDir, m.clusterUuid, false); err != nil {
			log.LogErrorf("CheckOrStoreClusterUuid failed: %v", err)
			return fmt.Errorf("CheckOrStoreClusterUuid failed: %v", err)
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
		NumOfLogsToRetain: raftstore.DefaultNumOfLogsToRetain * 2,
	}
	m.raftStore, err = raftstore.NewRaftStore(raftConf, cfg)
	if err != nil {
		err = errors.NewErrorf("new raftStore: %s", err.Error())
	}
	return
}

func (m *MetaNode) stopRaftServer() {
	if m.raftStore != nil {
		m.raftStore.Stop()
	}
}
