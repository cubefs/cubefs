// Copyright 2018 The CFS Authors.
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
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft/proto"
	"strings"
)

// LeaderInfo represents the leader's information
type LeaderInfo struct {
	addr string //host:port
}

func (m *Server) handleLeaderChange(leader uint64) {
	if leader == 0 {
		log.LogWarnf("action[handleLeaderChange] but no leader")
		return
	}
	m.leaderInfo.addr = AddrDatabase[leader]
	log.LogWarnf("action[handleLeaderChange] change leader to [%v] ", m.leaderInfo.addr)
	m.reverseProxy = m.newReverseProxy()

	// once switching to the master, check the Heartbeat
	if m.id == leader {
		Warn(m.clusterName, fmt.Sprintf("clusterID[%v] leader is changed to %v",
			m.clusterName, m.leaderInfo.addr))
		m.cluster.checkMetaNodeHeartbeat()
		m.cluster.checkDataNodeHeartbeat()
	}
}

func (m *Server) handlePeerChange(confChange *proto.ConfChange) (err error) {
	var msg string
	addr := string(confChange.Context)
	switch confChange.Type {
	case proto.ConfAddNode:
		var arr []string
		if arr = strings.Split(addr, colonSplit); len(arr) < 2 {
			msg = fmt.Sprintf("action[handlePeerChange] clusterID[%v] nodeAddr[%v] is invalid", m.clusterName, addr)
			break
		}
		m.raftStore.AddNode(confChange.Peer.ID, arr[0])
		AddrDatabase[confChange.Peer.ID] = string(confChange.Context)
		msg = fmt.Sprintf("clusterID[%v] peerID:%v,nodeAddr[%v] has been add", m.clusterName, confChange.Peer.ID, addr)
	case proto.ConfRemoveNode:
		m.raftStore.DeleteNode(confChange.Peer.ID)
		msg = fmt.Sprintf("clusterID[%v] peerID:%v,nodeAddr[%v] has been removed", m.clusterName, confChange.Peer.ID, addr)
	}
	Warn(m.clusterName, msg)
	return
}

func (m *Server) handleApply(cmd *RaftCmdData) (err error) {
	return m.cluster.handleApply(cmd)
}

func (m *Server) handleApplySnapshot() {
	m.fsm.restore()
	m.restoreIDAlloc()
	return
}

func (m *Server) restoreIDAlloc() {
	m.cluster.idAlloc.restore()
}

// load stored metadata into the memory
func (m *Server) loadMetadata() {
	log.LogInfo("action[loadMetadata] begin")
	m.restoreIDAlloc()
	var err error
	if err = m.cluster.loadCompactStatus(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadNodeSets(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadDataNodes(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadMetaNodes(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadVols(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadMetaPartitions(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadDataPartitions(); err != nil {
		panic(err)
	}
	log.LogInfo("action[loadMetadata] end")

}
