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
	"os"
	"strconv"

	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util/errors"
)

// StartRaftServer initializes the address resolver and the raftStore server instance.
func (m *MetaNode) startRaftServer() (err error) {
	if _, err = os.Stat(m.raftDir); err != nil {
		if err = os.MkdirAll(m.raftDir, 0755); err != nil {
			err = errors.NewErrorf("create raft server dir: %s", err.Error())
			return
		}
	}

	heartbeatPort, _ := strconv.Atoi(m.raftHeartbeatPort)
	replicaPort, _ := strconv.Atoi(m.raftReplicatePort)

	raftConf := &raftstore.Config{
		NodeID:            m.nodeId,
		RaftPath:          m.raftDir,
		TickInterval:      m.tickInterval,
		IPAddr:            m.localAddr,
		HeartbeatPort:     heartbeatPort,
		ReplicaPort:       replicaPort,
		NumOfLogsToRetain: raftstore.DefaultNumOfLogsToRetain * 2,
	}
	m.raftStore, err = raftstore.NewRaftStore(raftConf)
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
