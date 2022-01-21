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

package metanode

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"encoding/binary"
	"fmt"
	"path"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func (mp *metaPartition) initInode(ino *Inode) {
	for {
		time.Sleep(10 * time.Nanosecond)
		select {
		case <-mp.stopC:
			return
		default:
			// check first root inode
			if mp.hasInode(ino) {
				return
			}
			if !mp.raftPartition.IsRaftLeader() {
				continue
			}
			data, err := ino.Marshal()
			if err != nil {
				log.LogFatalf("[initInode] marshal: %s", err.Error())
			}
			// put first root inode
			resp, err := mp.submit(opFSMCreateInode, data)
			if err != nil {
				log.LogFatalf("[initInode] raft sync: %s", err.Error())
			}
			p := &Packet{}
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

func (mp *metaPartition) fsmUpdatePartition(end uint64) (status uint8,
	err error) {
	status = proto.OpOk
	oldEnd := mp.config.End
	mp.config.End = end
	defer func() {
		if err != nil {
			mp.config.End = oldEnd
			status = proto.OpDiskErr
		}
	}()
	err = mp.PersistMetadata()
	return
}

func (mp *metaPartition) confAddNode(req *proto.AddMetaPartitionRaftMemberRequest, index uint64) (updated bool, err error) {
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

func (mp *metaPartition) confRemoveNode(req *proto.RemoveMetaPartitionRaftMemberRequest, index uint64) (updated bool, err error) {
	var canRemoveSelf bool
	if canRemoveSelf, err = mp.canRemoveSelf(); err != nil {
		return
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
	if mp.config.NodeId == req.RemovePeer.ID && !mp.isLoadingMetaPartition && canRemoveSelf {
		mp.Stop()
		mp.DeleteRaft()
		mp.manager.deletePartition(mp.GetBaseConfig().PartitionId)
		os.RemoveAll(mp.config.RootDir)
		updated = false
	}
	log.LogInfof("Fininsh RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	return
}

func (mp *metaPartition) delOldExtentFile(buf []byte) (err error) {
	fileName := string(buf)
	infos, err := os.ReadDir(mp.config.RootDir)
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
		if f.Name() <= fileName {
			// TODO Unhandled errors
			os.Remove(path.Join(mp.config.RootDir, f.Name()))
			continue
		}
		break
	}
	return
}

//
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

	sumReplicas := len(mp.config.Peers)
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
		return fmt.Errorf("Exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", mp.config.PartitionId, mp.config.Peers, request.Members)
	}
	if mp.config.Start != request.Start || mp.config.End != request.End {
		return fmt.Errorf("Exsit unavali Partition(%v) range(%v-%v) requestRange(%v-%v)", mp.config.PartitionId, mp.config.Start, mp.config.End, request.Start, request.End)
	}
	for index, peer := range mp.config.Peers {
		requestPeer := request.Members[index]
		if requestPeer.ID != peer.ID || requestPeer.Addr != peer.Addr {
			return fmt.Errorf("Exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", mp.config.PartitionId, mp.config.Peers, request.Members)
		}
	}
	if mp.config.VolName != request.VolName {
		return fmt.Errorf("Exsit unavali Partition(%v) VolName(%v) requestVolName(%v)", mp.config.PartitionId, mp.config.VolName, request.VolName)
	}

	return
}
