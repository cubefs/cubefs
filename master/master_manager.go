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

package master

import (
	"fmt"
	syslog "log"
	"strings"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	cfsProto "github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// LeaderInfo represents the leader's information
type LeaderInfo struct {
	addr string //host:port
}

func (m *Server) handleLeaderChange(leader uint64) {
	if leader == 0 {
		log.LogWarnf("action[handleLeaderChange] but no leader")
		if WarnMetrics != nil {
			WarnMetrics.reset()
		}
		return
	}

	oldLeaderAddr := m.leaderInfo.addr
	m.leaderInfo.addr = AddrDatabase[leader]
	log.LogWarnf("action[handleLeaderChange]  [%v] ", m.leaderInfo.addr)
	m.reverseProxy = m.newReverseProxy()

	if m.id == leader {
		Warn(m.clusterName, fmt.Sprintf("clusterID[%v] leader is changed to %v",
			m.clusterName, m.leaderInfo.addr))
		if oldLeaderAddr != m.leaderInfo.addr {
			m.cluster.checkPersistClusterValue()

			m.loadMetadata()
			m.cluster.metaReady = true
			m.metaReady = true
		}
		m.cluster.checkDataNodeHeartbeat()
		m.cluster.checkMetaNodeHeartbeat()
		m.cluster.checkLcNodeHeartbeat()
		m.cluster.followerReadManager.reSet()

	} else {
		Warn(m.clusterName, fmt.Sprintf("clusterID[%v] leader is changed to %v",
			m.clusterName, m.leaderInfo.addr))
		m.clearMetadata()
		m.metaReady = false
		m.cluster.metaReady = false
		m.cluster.masterClient.AddNode(m.leaderInfo.addr)
		m.cluster.masterClient.SetLeader(m.leaderInfo.addr)
		if WarnMetrics != nil {
			WarnMetrics.reset()
		}
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
	default:
		// do nothing
	}
	Warn(m.clusterName, msg)
	return
}

func (m *Server) handleApplySnapshot() {
	m.fsm.restore()
	m.restoreIDAlloc()
	return
}

func (m *Server) handleRaftUserCmd(opt uint32, key string, cmdMap map[string][]byte) (err error) {
	log.LogInfof("action[handleRaftUserCmd] opt %v, key %v, map len %v", opt, key, len(cmdMap))
	switch opt {
	case opSyncPutFollowerApiLimiterInfo, opSyncPutApiLimiterInfo:
		if m.cluster != nil && !m.partition.IsRaftLeader() {
			m.cluster.apiLimiter.updateLimiterInfoFromLeader(cmdMap[key])
		}
	default:
		log.LogErrorf("action[handleRaftUserCmd] opt %v not supported,key %v, map len %v", opt, key, len(cmdMap))
	}
	return nil
}

func (m *Server) restoreIDAlloc() {
	m.cluster.idAlloc.restore()
}

// Load stored metadata into the memory
func (m *Server) loadMetadata() {
	log.LogInfo("action[loadMetadata] begin")
	syslog.Println("action[loadMetadata] begin")
	m.clearMetadata()
	m.restoreIDAlloc()
	m.cluster.fsm.restore()
	var err error
	if err = m.cluster.loadClusterValue(); err != nil {
		panic(err)
	}
	var loadDomain bool
	if m.cluster.FaultDomain { // try load exclude
		if loadDomain, err = m.cluster.loadZoneDomain(); err != nil {
			log.LogInfof("action[putZoneDomain] err[%v]", err)
			panic(err)
		}
		if err = m.cluster.loadNodeSetGrps(); err != nil {
			panic(err)
		}
		if loadDomain {
			// if load success the domain already init before this startup,
			// start grp manager ,load nodeset can trigger build ns grps
			m.cluster.domainManager.start()
		}
	}

	if err = m.cluster.loadNodeSets(); err != nil {
		panic(err)
	}

	if m.cluster.FaultDomain {
		log.LogInfof("action[FaultDomain] set")
		if !loadDomain { // first restart after domain item be added
			if err = m.cluster.putZoneDomain(true); err != nil {
				log.LogInfof("action[putZoneDomain] err[%v]", err)
				panic(err)
			}
			m.cluster.domainManager.start()
		}
	}

	func(funcs ...func() error) {
		for _, f := range funcs {
			if err = f(); err != nil {
				panic(err)
			}
		}
	}(
		m.cluster.loadDataNodes,
		m.cluster.loadMetaNodes,
		m.cluster.loadFlashNodes,
		m.cluster.loadFlashGroups,
		m.cluster.loadFlashTopology,
		m.cluster.loadZoneValue,
		m.cluster.loadVols,
		m.cluster.loadMetaPartitions,
		m.cluster.loadDataPartitions,
		m.cluster.loadDecommissionDiskList,
		m.cluster.startDecommissionListTraverse,
	)

	log.LogInfo("action[loadMetadata] end")

	log.LogInfo("action[loadUserInfo] begin")
	if err = m.user.loadUserStore(); err != nil {
		panic(err)
	}
	if err = m.user.loadAKStore(); err != nil {
		panic(err)
	}
	if err = m.user.loadVolUsers(); err != nil {
		panic(err)
	}
	log.LogInfo("action[loadUserInfo] end")

	log.LogInfo("action[refreshUser] begin")
	if err = m.refreshUser(); err != nil {
		panic(err)
	}
	log.LogInfo("action[refreshUser] end")

	log.LogInfo("action[loadApiLimiterInfo] begin")
	if err = m.cluster.loadApiLimiterInfo(); err != nil {
		panic(err)
	}
	log.LogInfo("action[loadApiLimiterInfo] end")

	log.LogInfo("action[loadQuota] begin")
	if err = m.cluster.loadQuota(); err != nil {
		panic(err)
	}
	log.LogInfo("action[loadQuota] end")

	log.LogInfo("action[loadLcConfs] begin")
	if err = m.cluster.loadLcConfs(); err != nil {
		panic(err)
	}
	log.LogInfo("action[loadLcConfs] end")

	log.LogInfo("action[loadLcNodes] begin")
	if err = m.cluster.loadLcNodes(); err != nil {
		panic(err)
	}
	log.LogInfo("action[loadLcNodes] end")
	syslog.Println("action[loadMetadata] end")

	log.LogInfo("action[loadS3QoSInfo] begin")
	if err = m.cluster.loadS3ApiQosInfo(); err != nil {
		panic(err)
	}
	log.LogInfo("action[loadS3QoSInfo] end")
}

func (m *Server) clearMetadata() {
	m.cluster.clearTopology()
	m.cluster.clearDataNodes()
	m.cluster.clearMetaNodes()
	m.cluster.clearLcNodes()
	m.cluster.clearVols()

	if m.user != nil {
		// leader change event may be before m.user initialization
		m.user.clearUserStore()
		m.user.clearAKStore()
		m.user.clearVolUsers()
	}

	m.cluster.t = newTopology()
	// m.cluster.apiLimiter.Clear()

	m.cluster.flashNodeTopo.clear()
	m.cluster.flashNodeTopo = newFlashNodeTopology()
}

func (m *Server) refreshUser() (err error) {
	/* todo create user automatically
	var userInfo *cfsProto.UserInfo
	for volName, vol := range m.cluster.allVols() {
		if _, err = m.user.getUserInfo(vol.Owner); err == cfsProto.ErrUserNotExists {
			if len(vol.OSSAccessKey) > 0 && len(vol.OSSSecretKey) > 0 {
				var param = cfsProto.UserCreateParam{
					ID:        vol.Owner,
					Password:  DefaultUserPassword,
					AccessKey: vol.OSSAccessKey,
					SecretKey: vol.OSSSecretKey,
					Type:      cfsProto.UserTypeNormal,
				}
				userInfo, err = m.user.createKey(&param)
				if err != nil && err != cfsProto.ErrDuplicateUserID && err != cfsProto.ErrDuplicateAccessKey {
					return err
				}
			} else {
				var param = cfsProto.UserCreateParam{
					ID:       vol.Owner,
					Password: DefaultUserPassword,
					Type:     cfsProto.UserTypeNormal,
				}
				userInfo, err = m.user.createKey(&param)
				if err != nil && err != cfsProto.ErrDuplicateUserID {
					return err
				}
			}
			if err == nil && userInfo != nil {
				if _, err = m.user.addOwnVol(userInfo.UserID, volName); err != nil {
					return err
				}
			}
		}
	}*/
	if _, err = m.user.getUserInfo(RootUserID); err != nil {
		param := cfsProto.UserCreateParam{
			ID:       RootUserID,
			Password: DefaultRootPasswd,
			Type:     cfsProto.UserTypeRoot,
		}
		if _, err = m.user.createKey(&param); err != nil {
			return err
		}
	}
	return nil
}
