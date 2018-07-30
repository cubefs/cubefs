package master

import (
	"fmt"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/tiglabs/raft/proto"
	"strings"
)

type LeaderInfo struct {
	addr string //host:port
}

func (m *Master) handleLeaderChange(leader uint64) {
	if leader == 0 {
		log.LogWarnf("action[handleLeaderChange] but no leader")
	}
	m.leaderInfo.addr = AddrDatabase[leader]
	//Once switched to the master, the checkHeartbeat is executed
	if m.id == leader {
		Warn(m.clusterName, fmt.Sprintf("clusterID[%v] leader is changed to %v",
			m.clusterName, m.leaderInfo.addr))
		//m.loadMetadata()
		m.cluster.checkDataNodeHeartbeat()
		m.cluster.checkMetaNodeHeartbeat()
	}
}

func (m *Master) handlePeerChange(confChange *proto.ConfChange) (err error) {
	var msg string
	addr := string(confChange.Context)
	switch confChange.Type {
	case proto.ConfAddNode:
		var arr []string
		if arr = strings.Split(addr, ColonSplit); len(arr) < 2 {
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

func (m *Master) handleApply(cmd *Metadata) (err error) {
	return m.cluster.handleApply(cmd)
}

func (m *Master) handleApplySnapshot() {
	m.fsm.restore()
	m.restoreIDAlloc()
	return
}

func (m *Master) restoreIDAlloc() {
	m.cluster.idAlloc.restore()
}

// load stored meta data to memory
func (m *Master) loadMetadata() {

	m.restoreIDAlloc()
	var err error
	if err = m.cluster.loadCompactStatus(); err != nil {
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

}
