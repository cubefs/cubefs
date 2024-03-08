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
	"context"
	"fmt"
	syslog "log"
	"strings"

	"github.com/cubefs/cubefs/blobstore/common/trace"
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
		log.Warnf("action[handleLeaderChange] but no leader")
		if WarnMetrics != nil {
			WarnMetrics.reset()
		}
		return
	}

	oldLeaderAddr := m.leaderInfo.addr
	m.leaderInfo.addr = AddrDatabase[leader]
	log.Warnf("action[handleLeaderChange]  [%v] ", m.leaderInfo.addr)
	m.reverseProxy = m.newReverseProxy()
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "leader-change")
	if m.id == leader {
		Warn(ctx, m.clusterName, fmt.Sprintf("clusterID[%v] leader is changed to %v",
			m.clusterName, m.leaderInfo.addr))
		if oldLeaderAddr != m.leaderInfo.addr {
			m.cluster.checkPersistClusterValue(ctx)

			m.loadMetadata(ctx)
			m.cluster.metaReady = true
			m.metaReady = true
		}
		m.cluster.checkDataNodeHeartbeat(ctx)
		m.cluster.checkMetaNodeHeartbeat(ctx)
		m.cluster.checkLcNodeHeartbeat(ctx)
		m.cluster.followerReadManager.reSet()

	} else {
		Warn(ctx, m.clusterName, fmt.Sprintf("clusterID[%v] leader is changed to %v",
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
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "peer-change")
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
	Warn(ctx, m.clusterName, msg)
	return
}

func (m *Server) handleApplySnapshot() {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "snapshot-apply")
	m.fsm.restore()
	m.restoreIDAlloc(ctx)
	return
}

func (m *Server) handleRaftUserCmd(opt uint32, key string, cmdMap map[string][]byte) (err error) {
	log.Infof("action[handleRaftUserCmd] opt %v, key %v, map len %v", opt, key, len(cmdMap))
	switch opt {
	case opSyncPutFollowerApiLimiterInfo, opSyncPutApiLimiterInfo:
		if m.cluster != nil && !m.partition.IsRaftLeader() {
			m.cluster.apiLimiter.updateLimiterInfoFromLeader(cmdMap[key])
		}
	default:
		log.Errorf("action[handleRaftUserCmd] opt %v not supported,key %v, map len %v", opt, key, len(cmdMap))
	}
	return nil
}

func (m *Server) restoreIDAlloc(ctx context.Context) {
	m.cluster.idAlloc.restore(ctx)
}

// Load stored metadata into the memory
func (m *Server) loadMetadata(ctx context.Context) {
	span := cfsProto.SpanFromContext(ctx)
	span.Info("action[loadMetadata] begin")
	syslog.Println("action[loadMetadata] begin")
	m.clearMetadata()
	m.restoreIDAlloc(ctx)
	m.cluster.fsm.restore()
	var err error
	if err = m.cluster.loadClusterValue(ctx); err != nil {
		panic(err)
	}
	var loadDomain bool
	if m.cluster.FaultDomain { // try load exclude
		if loadDomain, err = m.cluster.loadZoneDomain(ctx); err != nil {
			span.Infof("action[putZoneDomain] err[%v]", err)
			panic(err)
		}
		if err = m.cluster.loadNodeSetGrps(ctx); err != nil {
			panic(err)
		}
		if loadDomain {
			// if load success the domain already init before this startup,
			// start grp manager ,load nodeset can trigger build ns grps
			m.cluster.domainManager.start()
		}
	}

	if err = m.cluster.loadNodeSets(ctx); err != nil {
		panic(err)
	}

	if m.cluster.FaultDomain {
		span.Infof("action[FaultDomain] set")
		if !loadDomain { // first restart after domain item be added
			if err = m.cluster.putZoneDomain(ctx, true); err != nil {
				span.Infof("action[putZoneDomain] err[%v]", err)
				panic(err)
			}
			m.cluster.domainManager.start()
		}
	}

	if err = m.cluster.loadDataNodes(ctx); err != nil {
		panic(err)
	}

	if err = m.cluster.loadMetaNodes(ctx); err != nil {
		panic(err)
	}

	if err = m.cluster.loadZoneValue(ctx); err != nil {
		panic(err)
	}

	if err = m.cluster.loadVols(ctx); err != nil {
		panic(err)
	}

	if err = m.cluster.loadMetaPartitions(ctx); err != nil {
		panic(err)
	}
	if err = m.cluster.loadDataPartitions(ctx); err != nil {
		panic(err)
	}
	if err = m.cluster.loadDecommissionDiskList(ctx); err != nil {
		panic(err)
	}
	if err = m.cluster.startDecommissionListTraverse(ctx); err != nil {
		panic(err)
	}
	span.Info("action[loadMetadata] end")

	span.Info("action[loadUserInfo] begin")
	if err = m.user.loadUserStore(ctx); err != nil {
		panic(err)
	}
	if err = m.user.loadAKStore(ctx); err != nil {
		panic(err)
	}
	if err = m.user.loadVolUsers(ctx); err != nil {
		panic(err)
	}
	span.Info("action[loadUserInfo] end")

	span.Info("action[refreshUser] begin")
	if err = m.refreshUser(ctx); err != nil {
		panic(err)
	}
	span.Info("action[refreshUser] end")

	span.Info("action[loadApiLimiterInfo] begin")
	if err = m.cluster.loadApiLimiterInfo(ctx); err != nil {
		panic(err)
	}
	span.Info("action[loadApiLimiterInfo] end")

	span.Info("action[loadQuota] begin")
	if err = m.cluster.loadQuota(ctx); err != nil {
		panic(err)
	}
	span.Info("action[loadQuota] end")

	span.Info("action[loadLcConfs] begin")
	if err = m.cluster.loadLcConfs(ctx); err != nil {
		panic(err)
	}
	span.Info("action[loadLcConfs] end")

	span.Info("action[loadLcNodes] begin")
	if err = m.cluster.loadLcNodes(ctx); err != nil {
		panic(err)
	}
	span.Info("action[loadLcNodes] end")
	syslog.Println("action[loadMetadata] end")

	span.Info("action[loadS3QoSInfo] begin")
	if err = m.cluster.loadS3ApiQosInfo(ctx); err != nil {
		panic(err)
	}
	span.Info("action[loadS3QoSInfo] end")
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
}

func (m *Server) refreshUser(ctx context.Context) (err error) {
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
	if _, err = m.user.getUserInfo(ctx, RootUserID); err != nil {
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
