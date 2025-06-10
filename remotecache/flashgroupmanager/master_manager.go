package flashgroupmanager

import (
	"fmt"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	syslog "log"
	"strings"

	"github.com/cubefs/cubefs/util/log"
)

type LeaderInfo struct {
	addr string //host:port
	id   uint64
}

func (m *FlashGroupManager) handleLeaderChange(leader uint64) {
	if leader == 0 {
		log.LogWarnf("action[handleLeaderChange] but no leader")
		m.leaderInfo.id = 0
		m.leaderInfo.addr = ""
		return
	}

	m.leaderInfo.addr = AddrDatabase[leader]
	m.leaderInfo.id = leader

	log.LogWarnf("action[handleLeaderChange] current id [%v] new leader addr [%v] leader id [%v]", m.id, m.leaderInfo.addr, leader)
	m.reverseProxy = m.newReverseProxy()

	if m.id == leader {
		Warn(m.clusterName, fmt.Sprintf("clusterID[%v] current is leader, leader is changed to %v",
			m.clusterName, m.leaderInfo.addr))
		m.loadMetadata()
		m.metaReady = true
	} else {
		Warn(m.clusterName, fmt.Sprintf("clusterID[%v] leader is changed to %v",
			m.clusterName, m.leaderInfo.addr))
		m.clearMetadata()
		m.metaReady = false
	}
}

func (m *FlashGroupManager) clearMetadata() {
	m.cluster.flashNodeTopo.clear()
	m.cluster.flashNodeTopo = NewFlashNodeTopology()
}

func (m *FlashGroupManager) loadMetadata() {
	var err error
	log.LogInfo("action[loadMetadata] begin")
	syslog.Println("action[loadMetadata] begin")
	m.clearMetadata()
	m.restoreIDAlloc()
	m.cluster.fsm.restore()

	if err = m.cluster.loadClusterValue(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadFlashNodes(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadFlashGroups(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadFlashTopology(); err != nil {
		panic(err)
	}

	// TODO
	//log.LogInfo("action[loadApiLimiterInfo] begin")
	//if err = m.cluster.loadApiLimiterInfo(); err != nil {
	//	panic(err)
	//}
	//log.LogInfo("action[loadApiLimiterInfo] end")

	log.LogInfo("action[loadMetadata] end")
	syslog.Println("action[loadMetadata] end")
}

func (m *FlashGroupManager) restoreIDAlloc() {
	m.cluster.idAlloc.restore()
}

func (m *FlashGroupManager) handlePeerChange(confChange *proto.ConfChange) (err error) {
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
	default:
		// do nothing
	}
	Warn(m.clusterName, msg)
	return
}

func (m *FlashGroupManager) handleApplySnapshot() {
	m.fsm.restore()
	m.restoreIDAlloc()
}

func (m *FlashGroupManager) handleRaftUserCmd(opt uint32, key string, cmdMap map[string][]byte) (err error) {
	log.LogInfof("action[handleRaftUserCmd] opt %v, key %v, map len %v", opt, key, len(cmdMap))
	switch opt {
	// TODO
	//case opSyncPutFollowerApiLimiterInfo, opSyncPutApiLimiterInfo:
	//	if m.cluster != nil && !m.partition.IsRaftLeader() {
	//		m.cluster.apiLimiter.updateLimiterInfoFromLeader(cmdMap[key])
	//	}
	default:
		log.LogErrorf("action[handleRaftUserCmd] opt %v not supported,key %v, map len %v", opt, key, len(cmdMap))
	}
	return nil
}
