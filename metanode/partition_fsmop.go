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
	"os"
	"path"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func (mp *metaPartition) initInode(ctx context.Context, ino *Inode) {
	for {
		time.Sleep(10 * time.Nanosecond)
		select {
		case <-mp.stopC:
			return
		default:
			span := getSpan(ctx)
			// check first root inode
			if mp.hasInode(ctx, ino) {
				return
			}
			if !mp.raftPartition.IsRaftLeader() {
				continue
			}
			// qinode := &MetaQuotaInode{
			// 	inode:    ino,
			// 	quotaIds: make([]uint32, 0, 0),
			// }
			// data, err := qinode.Marshal()
			// if err != nil {
			// 	span.Fatalf("[initInode] marshal: %s", err.Error())
			// }

			data, err := ino.Marshal()
			if err != nil {
				span.Fatalf("[initInode] marshal: %s", err.Error())
			}
			// put first root inode
			resp, err := mp.submit(ctx, opFSMCreateInode, data)
			if err != nil {
				span.Fatalf("[initInode] raft sync: %s", err.Error())
			}
			p := &Packet{}
			p.ResultCode = resp.(uint8)
			span.Debugf("[initInode] raft sync: response status = %v.", p.GetResultMsg())
			return
		}
	}
}

// Not implemented.
func (mp *metaPartition) decommissionPartition() (err error) {
	return
}

func (mp *metaPartition) fsmUpdatePartition(ctx context.Context, end uint64) (status uint8, err error) {
	status = proto.OpOk
	oldEnd := mp.config.End
	mp.config.End = end

	if end < mp.config.Cursor {
		status = proto.OpAgain
		mp.config.End = oldEnd
		return
	}
	if err = mp.PersistMetadata(ctx); err != nil {
		status = proto.OpDiskErr
		mp.config.End = oldEnd
	}
	return
}

func (mp *metaPartition) confAddNode(ctx context.Context, req *proto.AddMetaPartitionRaftMemberRequest, index uint64) (updated bool, err error) {
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

func (mp *metaPartition) confRemoveNode(ctx context.Context, req *proto.RemoveMetaPartitionRaftMemberRequest, index uint64) (updated bool, err error) {
	var canRemoveSelf bool
	if canRemoveSelf, err = mp.canRemoveSelf(ctx); err != nil {
		return
	}
	peerIndex := -1
	data, _ := json.Marshal(req)
	span := getSpan(ctx)
	span.Infof("Start RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	for i, peer := range mp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			updated = true
			peerIndex = i
			break
		}
	}
	if !updated {
		span.Infof("NoUpdate RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
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
	span.Infof("Fininsh RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	return
}

func (mp *metaPartition) delOldExtentFile(ctx context.Context, buf []byte) (err error) {
	fileName := string(buf)
	span := getSpan(ctx)
	span.Warnf("[delOldExtentFile] del extent file(%s), mp[%v]", fileName, mp.config.PartitionId)

	entries, err := os.ReadDir(mp.config.RootDir)
	if err != nil {
		return
	}
	infos := sortDelExtFileInfo(entries)
	tgtIdx := getDelExtFileIdx(fileName)

	for _, f := range infos {
		idx := getDelExtFileIdx(f.Name())
		if idx > tgtIdx {
			break
		}

		span.Warnf("[delOldExtentFile] del extent file(%s), mp[%v]", f.Name(), mp.config.PartitionId)
		os.Remove(path.Join(mp.config.RootDir, f.Name()))
	}
	return
}

func (mp *metaPartition) setExtentDeleteFileCursor(ctx context.Context, buf []byte) (err error) {
	span := getSpan(ctx)
	str := string(buf)
	var (
		fileName string
		cursor   int64
	)
	_, err = fmt.Sscanf(str, "%s %d", &fileName, &cursor)
	span.Infof("[setExtentDeleteFileCursor] &fileName_&cursor(%s), mp[%v]", str, mp.config.PartitionId)
	if err != nil {
		return
	}
	fp, err := os.OpenFile(path.Join(mp.config.RootDir, fileName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		span.Errorf("[setExtentDeleteFileCursor] openFile %s failed: %s", fileName, err.Error())
		return
	}
	if err = binary.Write(fp, binary.BigEndian, cursor); err != nil {
		span.Errorf("[setExtentDeleteFileCursor] write file %s cursor failed: %s", fileName, err.Error())
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
