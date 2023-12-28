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
	"encoding/json"
	"fmt"
	cfsProto "github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util"
	"strings"
	"sync/atomic"
	"time"
)

func (m *Server) scheduleProcessLeaderChange() {
	go func() {
		for {
			select {
			case lt := <-m.leaderChangeChan:
				log.LogWarnf("action[scheduleProcessLeaderChange] change leader to [%v],at term [%v], last leader version:%v ", lt.id, lt.term, m.cluster.getLeaderVersion())
				m.doLeaderChange(lt.id, lt.term, lt.lastIndex)
			}
		}
	}()
}

func (m *Server) doLeaderChange(leader, term, li uint64) {
	m.preProcessOnLeaderChange()
	// wait old task exit
	m.cluster.taskWg.Wait()
	defer func() {
		m.cluster.taskExitC = make(chan int, 16)
		// schedule new task
		if term == 0 {
			_, term = m.cluster.partition.LeaderTerm()
		}
		if m.id == leader {
			atomic.StoreUint64(&m.cluster.raftTermOnStartTask, term)
			m.cluster.startPeriodicBackgroundSchedulingTasks()
		}
	}()
	if leader == 0 {
		log.LogWarnf("action[doLeaderChange] but no leader")
		m.loadMetadataOnFollower(term, leader, m.cluster.getLeaderVersion())
		return
	}
	oldLeaderAddr := m.leaderInfo.addr
	m.leaderInfo.addr = AddrDatabase[leader]
	log.LogWarnf("action[doLeaderChange] change leader from %v to [%v] ", oldLeaderAddr, m.leaderInfo.addr)
	m.reverseProxy = m.newReverseProxy()
	if m.id == leader {
		msg := fmt.Sprintf("clusterID[%v] leader is changed to %v",
			m.clusterName, m.leaderInfo.addr)
		WarnBySpecialKey(gAlarmKeyMap[alarmKeyLeaderChanged], msg)
		m.loadMetadata(term, leader, m.cluster.getLeaderVersion(), li)
		log.LogInfo("action[refreshUser] begin")
		if err := m.refreshUser(); err != nil {
			log.LogErrorf("action[refreshUser] failed,err:%v", err)
		}
		log.LogInfo("action[refreshUser] end")
		m.cluster.updateMetaLoadedTime()
		msg = fmt.Sprintf("clusterID[%v] leader[%v] load metadata finished.",
			m.clusterName, m.leaderInfo.addr)
		WarnBySpecialKey(gAlarmKeyMap[alarmKeyLeaderChanged], msg)
		m.cluster.doCheckDataNodeHeartbeat()
		m.cluster.doCheckMetaNodeHeartbeat()
		m.postProcessOnLeaderChange()
		//new node, snapshot success, but set cluster name after snapshot apply;
		if m.cluster.cfg.ClusterName != "" && m.cluster.cfg.ClusterName != m.clusterName {
			msg = fmt.Sprintf("clusterID[%v] leader[%v] check conf cluster name failed; conf[%s], expect[%s].",
				m.clusterName, m.leaderInfo.addr, m.clusterName, m.cluster.cfg.ClusterName)
			WarnBySpecialKey(gAlarmKeyMap[alarmKeyLoadClusterMetadata], msg)
		}

	} else {
		msg := fmt.Sprintf("clusterID[%v] leader is changed to %v",
			m.clusterName, m.leaderInfo.addr)
		WarnBySpecialKey(gAlarmKeyMap[alarmKeyLeaderChanged], msg)
		m.loadMetadataOnFollower(term, leader, m.cluster.getLeaderVersion())
		m.cluster.resetMetaLoadedTime()
		msg = fmt.Sprintf("clusterID[%v] follower[%v] clear metadata has finished.",
			m.clusterName, m.ip)
		WarnBySpecialKey(gAlarmKeyMap[alarmKeyLeaderChanged], msg)
	}
}

func (m *Server) handleLeaderChange(leader uint64) {

	_, term := m.cluster.partition.LeaderTerm()
	li, err := m.cluster.partition.GetLastIndex()
	if err != nil {
		panic(err)
	}
	lt := &LeaderTermInfo{id: leader, term: term, lastIndex: li}
	log.LogWarnf("action[handleLeaderChange] to %v,at term %v", leader, term)
	m.leaderChangeChan <- lt
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
		msg = fmt.Sprintf("action[handlePeerChange] clusterID[%v] peerID:%v,nodeAddr[%v] has been add", m.clusterName, confChange.Peer.ID, addr)
	case proto.ConfRemoveNode:
		m.raftStore.DeleteNode(confChange.Peer.ID)
		msg = fmt.Sprintf("action[handlePeerChange] clusterID[%v] peerID:%v,nodeAddr[%v] has been removed", m.clusterName, confChange.Peer.ID, addr)
	}
	WarnBySpecialKey(gAlarmKeyMap[alarmKeyPeerChanged], msg)
	return
}

func (m *Server) handleApplySnapshot() {
	if err := m.checkClusterName(); err != nil {
		log.LogErrorf(errors.Stack(err))
		log.LogFlush()
		panic("cfg cluster name failed")
		return
	}
	m.fsm.restore()
	m.restoreIDAlloc()
	return
}

func (m *Server) handleApply(cmdMap map[string]*RaftCmd) (err error) {

	for _, cmd := range cmdMap {
		if cmd.Op != opSyncAddMetaPartition {
			continue
		}
		mpv := new(metaPartitionValue)
		if err = json.Unmarshal(cmd.V, mpv); err == nil {
			vol, err1 := m.cluster.getVol(mpv.VolName)
			if err1 != nil {
				log.LogErrorf("action[handleApply] err:%v", err1.Error())
				continue
			}
			if vol.ID != mpv.VolID {
				Warn(m.cluster.Name, fmt.Sprintf("action[handleApply] has duplicate vol[%v],vol.ID[%v],mpv.VolID[%v],mp[%v]", mpv.VolName, vol.ID, mpv.VolID, mpv.PartitionID))
				continue
			}
			mp := m.cluster.buildMetaPartition(mpv, vol)
			if err = vol.addMetaPartition(mp, m.cluster.Name); err != nil {
				return
			}
			id, err1 := m.cluster.getPreMetaPartitionIDFromRocksDB(vol.ID, mp.PrePartitionID)
			if err1 != nil {
				log.LogErrorf("action[handleApply] vol[%v],mp[%v],err:%v", vol.Name, mp.PartitionID, err1)
				return
			}

			if id != 0 && id != mp.PartitionID {
				log.LogErrorf("action[handleApply] mp[%v,%v] has common preMpID[%v],err:%v", id, mp.PartitionID, mpv.PrePartitionID, err1)
				return cfsProto.ErrHasCommonPreMetaPartition
			}

			log.LogInfof("action[handleApply] vol[%v] add new mp[%v],start[%v],end[%v]", vol.Name, mp.PartitionID, mp.Start, mp.End)
		}
	}
	return
}

func (m *Server) restoreIDAlloc() {
	m.cluster.idAlloc.restore()
}

func (m *Server) IsAllEmptyMsg(start, end uint64) bool {
	future := m.fsm.rs.GetEntries(GroupID, start, 64*util.MB)
	resp, err := future.Response()
	if err != nil {
		return false
	}
	entries, ok := resp.([]*proto.Entry)
	if !ok {
		return false
	}
	if len(entries) == 0 {
		return true
	}
	if entries[len(entries)-1].Index < end {
		return false
	}
	ok = true
	for _, entry := range entries {
		if entry.Index > end {
			break
		}
		if !(entry.Data == nil || len(entry.Data) == 0) {
			ok = false
			break
		}
	}
	return ok
}

func (m *Server) loadMetadataOnFollower(term, leader, version uint64) {
	log.LogInfof("action[loadMetadataOnFollower] begin at term:%v,leader:%v,version:%v", term, leader, version)
	m.clearMetadata(false)
	m.restoreIDAlloc()
	m.cluster.fsm.restore()
	var err error

	if err = m.cluster.loadClusterValue(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadVols(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadMetaPartitions(); err != nil {
		panic(err)
	}
	log.LogInfof("action[loadMetadataOnFollower] end at term:%v,leader:%v,version:%v", term, leader, version)
}

// Load stored metadata into the memory
func (m *Server) loadMetadata(term, leader, version, raftLogLastIndex uint64) {
	log.LogInfof("action[loadMetadata] begin at term:%v,version:%v,leader:%v", term, version, leader)
	m.clearMetadata(true)
	m.restoreIDAlloc()
	m.cluster.fsm.restore()
	var (
		raftInternalApplied uint64
	)
	for {
		raftInternalApplied = m.cluster.partition.AppliedIndex()
		log.LogWarnf("action[loadMetadata] applied:%v,lastIndex:%v,raftInternalApplied:%v,raftCommitId:%v,term:%v,leader:%v",
			m.cluster.fsm.applied, raftLogLastIndex, raftInternalApplied, m.cluster.partition.CommittedIndex(), term, leader)
		if raftInternalApplied >= raftLogLastIndex {
			break
		}

		//ok, _ := m.cluster.partition.IsAllEmptyMsg(raftLogLastIndex)
		ok := m.IsAllEmptyMsg(raftInternalApplied, raftLogLastIndex)
		if ok {
			log.LogWarnf("action[loadMetadata] all unapplied raft logs are empty messages;applied:%v,lastIndex:%v,raftInternalApplied:%v,raftCommitId:%v,term:%v,leader:%v",
				m.cluster.fsm.applied, raftLogLastIndex, raftInternalApplied, m.cluster.partition.CommittedIndex(), term, leader)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	var err error

	if err = m.cluster.loadClusterValue(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadNodeSets(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadRegions(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadIDCs(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadDataNodes(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadMetaNodes(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadEcNodes(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadCodecNodes(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadVols(); err != nil {
		panic(err)
	}

	if err = m.cluster.loadTokens(); err != nil {
		panic(err)
	}

	//rand.Seed(time.Now().UnixNano())
	//v := 15 + rand.Intn(10)
	//time.Sleep(time.Second * time.Duration(v))
	if err = m.cluster.loadMetaPartitions(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadDataPartitions(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadEcPartitions(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadMigrateTask(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadFlashGroups(); err != nil {
		panic(err)
	}
	if err = m.cluster.loadFlashNodes(); err != nil {
		panic(err)
	}
	log.LogInfof("action[loadMetadata] end at term:%v,leader:%v,version:%v", term, leader, version)

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
	m.resetBandwidthLimiter(int(m.cluster.cfg.BandwidthRateLimit))
	log.LogInfo("action[loadUserInfo] end")
}

func (m *Server) clearMetadata(isLeader bool) {
	m.cluster.clearTopology()
	m.cluster.clearDataNodes()
	m.cluster.clearMetaNodes()
	m.cluster.clearCodecNodes()
	m.cluster.clearEcNodes()
	m.cluster.clearMigrateTask()
	if isLeader {
		m.cluster.clearVols()
	}
	m.cluster.clearVolsResponseCache()
	m.cluster.clearClusterViewResponseCache()
	m.user.clearUserStore()
	m.user.clearAKStore()
	m.user.clearVolUsers()
	m.cluster.flashNodeTopo.clear()
	m.cluster.clearFlashGroupResponseCache()

	m.cluster.t = newTopology()
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
		var param = cfsProto.UserCreateParam{
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
