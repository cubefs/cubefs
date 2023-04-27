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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) initInode(ino *Inode) {
	for {

		time.Sleep(10 * time.Nanosecond)
		select {
		case <-mp.stopC:
			return
		default:
			// check first root inode
			if ok, _ := mp.hasInode(ino); ok {
				return
			}
			if !mp.raftPartition.IsRaftLeader() {
				continue
			}
			data, err := ino.Marshal()
			if err != nil {
				log.LogFatalf("[initInode] marshal: %s", err.Error())
			}

			ctx := context.Background()
			// put first root inode
			resp, err := mp.submit(ctx, opFSMCreateInode, "", data)
			if err != nil {
				log.LogFatalf("[initInode] raft sync: %s", err.Error())
			}
			p := NewPacket(ctx)
			p.ResultCode = resp.(uint8)
			log.LogDebugf("[initInode] raft sync: response status = %v.",
				p.GetResultMsg())
			return
		}
	}
}

// Not implemented.
func (mp *metaPartition) decommissionPartition() (err error) {
	return
}

func (mp *metaPartition) fsmConfigHasChanged() bool{
	needStore := false

	if mp.CreationType != mp.config.CreationType {
		needStore = true
		mp.config.CreationType = mp.CreationType
	}

	globalConfInfo := getGlobalConfNodeInfo()

	if globalConfInfo.rocksWalFileSize != 0 && globalConfInfo.rocksWalFileSize != mp.config.RocksWalFileSize {
		needStore = true
		mp.config.RocksWalFileSize = globalConfInfo.rocksWalFileSize
	}

	if globalConfInfo.rocksWalMemSize != 0 && globalConfInfo.rocksWalMemSize != mp.config.RocksWalMemSize {
		needStore = true
		mp.config.RocksWalMemSize = globalConfInfo.rocksWalMemSize
	}

	if globalConfInfo.rocksLogSize != 0 && globalConfInfo.rocksLogSize != mp.config.RocksLogFileSize {
		needStore = true
		mp.config.RocksLogFileSize = globalConfInfo.rocksLogSize
	}

	if globalConfInfo.rocksLogReservedTime != 0 && globalConfInfo.rocksLogReservedTime != mp.config.RocksLogReversedTime {
		needStore = true
		mp.config.RocksLogReversedTime = globalConfInfo.rocksLogReservedTime
	}

	if globalConfInfo.rocksLogReservedCnt != 0 && globalConfInfo.rocksLogReservedCnt != mp.config.RocksLogReVersedCnt {
		needStore = true
		mp.config.RocksLogReVersedCnt = globalConfInfo.rocksLogReservedCnt
	}

	if globalConfInfo.rocksWalTTL != 0 && globalConfInfo.rocksWalTTL != mp.config.RocksWalTTL {
		needStore = true
		mp.config.RocksWalTTL = globalConfInfo.rocksWalTTL
	}
	return needStore
}

func (mp *metaPartition) fsmStoreConfig() {
	if mp.fsmConfigHasChanged() {
		_ = mp.PersistMetadata()
	}
}

func (mp *metaPartition) fsmUpdatePartition(end uint64) (status uint8, err error) {
	status = proto.OpOk
	oldEnd := atomic.LoadUint64(&mp.config.End)
	atomic.StoreUint64(&mp.config.End, end)
	defer func() {
		if err != nil {
			log.LogErrorf("fsmUpdatePartition failed:%v", err.Error())
			atomic.StoreUint64(&mp.config.End, oldEnd)
			status = proto.OpDiskErr
		}
	}()
	if err = mp.config.checkMeta(); err != nil {
		return
	}

	if err = mp.config.persist(); err != nil {
		err = errors.NewErrorf("[persistConf] config persist->%s", err.Error())
		return
	}
	return
}

func (mp *metaPartition) confAddNode(req *proto.
AddMetaPartitionRaftMemberRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}

	addPeer := false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			addPeer = true
			break
		}
	}
	updated = !addPeer
	if !updated {
		return
	}
	mp.config.Peers = append(mp.config.Peers, req.AddPeer)
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	mp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicaPort)
	return
}

func (mp *metaPartition) confRemoveNode(req *proto.RemoveMetaPartitionRaftMemberRequest,
	index uint64) (updated bool, err error) {
	canRemoveSelf := true
	if mp.config.NodeId == req.RemovePeer.ID {
		if canRemoveSelf, err = mp.canRemoveSelf(); err != nil {
			return
		}
	}
	peerIndex := -1
	data, _ := json.Marshal(req)
	log.LogInfof("Start RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	for i, peer := range mp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			updated = true
			peerIndex = i
			break
		}
	}
	if !updated {
		log.LogInfof("NoUpdate RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
			req.PartitionId, mp.config.NodeId, string(data))
		return
	}
	mp.config.Peers = append(mp.config.Peers[:peerIndex], mp.config.Peers[peerIndex+1:]...)
	learnerIndex := -1
	for i, learner := range mp.config.Learners {
		if learner.ID == req.RemovePeer.ID {
			learnerIndex = i
			break
		}
	}
	if learnerIndex != -1 {
		mp.config.Learners = append(mp.config.Learners[:learnerIndex], mp.config.Learners[learnerIndex+1:]...)
	}
	if mp.config.NodeId == req.RemovePeer.ID && canRemoveSelf {
		//if req.ReserveResource {
		//	mp.raftPartition.Stop()
		//} else {
		//	mp.ExpiredRaft()
		//}
		_ = mp.ExpiredRaft()
		_ = mp.manager.expiredPartition(mp.GetBaseConfig().PartitionId)
		updated = false
	}
	log.LogInfof("Finish RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	return
}

func (mp *metaPartition) ApplyResetMember(req *proto.ResetMetaPartitionRaftMemberRequest) (updated bool, err error) {
	var (
		newPeerIndexes    []int
		newLearnerIndexes []int
		newPeers          []proto.Peer
		newLearners       []proto.Learner
	)
	data, _ := json.Marshal(req)
	updated = true
	log.LogInfof("Start ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	if len(req.NewPeers) >= len(mp.config.Peers) {
		log.LogErrorf("NoUpdate ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
			req.PartitionId, mp.config.NodeId, string(data))
		return
	}
	for _, peer := range req.NewPeers {
		flag := false
		for index, p := range mp.config.Peers {
			if peer.ID == p.ID {
				flag = true
				newPeerIndexes = append(newPeerIndexes, index)
				break
			}
		}
		if !flag {
			updated = false
			log.LogErrorf("ResetRaftNode must be old node, PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
				req.PartitionId, mp.config.NodeId, string(data))
			return
		}
	}
	for _, peer := range req.NewPeers {
		for index, l := range mp.config.Learners {
			if peer.ID == l.ID {
				newLearnerIndexes = append(newLearnerIndexes, index)
				break
			}
		}
	}
	if !updated {
		log.LogInfof("NoUpdate ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
			req.PartitionId, mp.config.NodeId, string(data))
		return
	}

	newPeers = make([]proto.Peer, len(newPeerIndexes))
	newLearners = make([]proto.Learner, len(newLearnerIndexes))
	sort.Ints(newPeerIndexes)
	for i, index := range newPeerIndexes {
		newPeers[i] = mp.config.Peers[index]
	}
	mp.config.Peers = newPeers

	sort.Ints(newLearnerIndexes)
	for i, index := range newLearnerIndexes {
		newLearners[i] = mp.config.Learners[index]
	}
	mp.config.Learners = newLearners

	log.LogInfof("Finish ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	return
}

func (mp *metaPartition) confAddLearner(req *proto.AddMetaPartitionRaftLearnerRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}

	addPeer := false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddLearner.ID {
			addPeer = true
			break
		}
	}
	if !addPeer {
		peer := proto.Peer{ID: req.AddLearner.ID, Addr: req.AddLearner.Addr}
		mp.config.Peers = append(mp.config.Peers, peer)
	}

	addLearner := false
	for _, learner := range mp.config.Learners {
		if learner.ID == req.AddLearner.ID {
			addLearner = true
			break
		}
	}
	if !addLearner {
		mp.config.Learners = append(mp.config.Learners, req.AddLearner)
	}
	updated = !addPeer || !addLearner
	if !updated {
		return
	}
	addr := strings.Split(req.AddLearner.Addr, ":")[0]
	mp.config.RaftStore.AddNodeWithPort(req.AddLearner.ID, addr, heartbeatPort, replicaPort)
	return
}

func (mp *metaPartition) confPromoteLearner(req *proto.PromoteMetaPartitionRaftLearnerRequest, index uint64) (updated bool, err error) {
	var promoteIndex int
	for i, learner := range mp.config.Learners {
		if learner.ID == req.PromoteLearner.ID {
			updated = true
			promoteIndex = i
			break
		}
	}
	if updated {
		mp.config.Learners = append(mp.config.Learners[:promoteIndex], mp.config.Learners[promoteIndex+1:]...)
	}
	return
}

func (mp *metaPartition) confUpdateNode(req *proto.MetaPartitionDecommissionRequest,
	index uint64) (updated bool, err error) {
	return
}

func (mp *metaPartition) delOldExtentFile(buf []byte) (err error) {
	fileName := string(buf)
	id, e := delExtNameID(fileName)
	if e != nil {
		err = fmt.Errorf("load file:[%s] format err:[%v] so skip", fileName, e)
		log.LogErrorf(err.Error())
		return
	}

	infos, err := ioutil.ReadDir(mp.config.RootDir)
	if err != nil {
		return
	}

	for _, f := range infos {
		if f.IsDir() {
			continue
		}
		if !strings.HasPrefix(f.Name(), prefixDelExtent) {
			continue
		}
		fid, e := delExtNameID(f.Name())
		if e != nil {
			log.LogErrorf("load file:[%s] format err:[%v] so skip", fileName, e)
			continue
		}
		if fid <= id {
			_ = os.Remove(path.Join(mp.config.RootDir, f.Name()))
		}
	}
	return
}

func (mp *metaPartition) setExtentDeleteFileCursor(buf []byte) (err error) {
	str := string(buf)
	var (
		fileName string
		cursor   int64
	)
	_, err = fmt.Sscanf(str, "%s %d", &fileName, &cursor)
	if err != nil {
		return
	}
	fp, err := os.OpenFile(path.Join(mp.config.RootDir, fileName), os.O_CREATE|os.O_RDWR,
		0644)
	if err != nil {
		log.LogErrorf("[setExtentDeleteFileCursor] openFile %s failed: %s",
			fileName, err.Error())
		return
	}
	if err = binary.Write(fp, binary.BigEndian, cursor); err != nil {
		log.LogErrorf("[setExtentDeleteFileCursor] write file %s cursor"+
			" failed: %s", fileName, err.Error())
	}
	// TODO Unhandled errors
	fp.Close()
	return
}

func (mp *metaPartition) CanRemoveRaftMember(peer proto.Peer) error {
	for _, learner := range mp.config.Learners {
		if peer.ID == learner.ID && peer.Addr == learner.Addr {
			return nil
		}
	}
	downReplicas := mp.config.RaftStore.RaftServer().GetDownReplicas(mp.config.PartitionId)
	hasExsit := false
	for _, p := range mp.config.Peers {
		if p.ID == peer.ID {
			hasExsit = true
			break
		}
	}
	if !hasExsit {
		return nil
	}

	hasDownReplicasExcludePeer := make([]uint64, 0)
	for _, nodeID := range downReplicas {
		if nodeID.NodeID == peer.ID {
			continue
		}
		hasDownReplicasExcludePeer = append(hasDownReplicasExcludePeer, nodeID.NodeID)
	}

	sumReplicas := len(mp.config.Peers) - len(mp.config.Learners)
	if sumReplicas%2 == 1 {
		if sumReplicas-len(hasDownReplicasExcludePeer) > (sumReplicas/2 + 1) {
			return nil
		}
	} else {
		if sumReplicas-len(hasDownReplicasExcludePeer) >= (sumReplicas/2 + 1) {
			return nil
		}
	}

	return fmt.Errorf("downReplicas(%v) too much,so donnot offline (%v)", downReplicas, peer)
}

func (mp *metaPartition) IsEquareCreateMetaPartitionRequst(request *proto.CreateMetaPartitionRequest) (err error) {
	if len(mp.config.Peers) != len(request.Members) {
		return fmt.Errorf("exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", mp.config.PartitionId, mp.config.Peers, request.Members)
	}
	if mp.config.Start != request.Start || mp.config.End != request.End {
		return fmt.Errorf("exsit unavali Partition(%v) range(%v-%v) requestRange(%v-%v)", mp.config.PartitionId, mp.config.Start, mp.config.End, request.Start, request.End)
	}
	for index, peer := range mp.config.Peers {
		requestPeer := request.Members[index]
		if requestPeer.ID != peer.ID || requestPeer.Addr != peer.Addr {
			return fmt.Errorf("exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", mp.config.PartitionId, mp.config.Peers, request.Members)
		}
	}
	if mp.config.VolName != request.VolName {
		return fmt.Errorf("exsit unavali Partition(%v) VolName(%v) requestVolName(%v)", mp.config.PartitionId, mp.config.VolName, request.VolName)
	}
	if len(mp.config.Learners) != len(request.Learners) {
		return fmt.Errorf("exsit unavali Partition(%v) partitionLearners(%v) requestLearners(%v)", mp.config.PartitionId, mp.config.Learners, request.Learners)
	}

	return
}
