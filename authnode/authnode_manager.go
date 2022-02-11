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
	"strings"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/util/log"
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
	m.authProxy = m.newAuthProxy() // TODO no lock?

	if m.metaReady == false {
		if err := m.cluster.loadKeystore(); err != nil {
			panic(err)
		}
		if err := m.cluster.loadAKstore(); err != nil {
			panic(err)
		}
		m.metaReady = true
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
		m.raftStore.AddNodeWithPort(confChange.Peer.ID, arr[0], int(m.config.heartbeatPort), int(m.config.replicaPort))
		AddrDatabase[confChange.Peer.ID] = string(confChange.Context)
		msg = fmt.Sprintf("clusterID[%v] peerID:%v,nodeAddr[%v] has been add", m.clusterName, confChange.Peer.ID, addr)
	case proto.ConfRemoveNode:
		m.raftStore.DeleteNode(confChange.Peer.ID)
		msg = fmt.Sprintf("clusterID[%v] peerID:%v,nodeAddr[%v] has been removed", m.clusterName, confChange.Peer.ID, addr)
	}
	log.LogWarnf(msg)
	return
}

func (m *Server) handleApplySnapshot() {
	log.LogInfof("clusterID[%v] peerID:%v action[handleApplySnapshot]", m.clusterName, m.id)
	m.fsm.restore()
	return
}
