// Copyright 2025 The CubeFS Authors.
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
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestCheckPeerDiffWithRaft_PendingPeersMarkedAbnormal(t *testing.T) {
	c := &Cluster{ClusterTopoSubItem: ClusterTopoSubItem{AbnormalRaftMP: &sync.Map{}}}
	mp := &MetaPartition{
		PartitionID: 1,
		Peers: []proto.Peer{
			{ID: 1, Addr: "m1"},
			{ID: 2, Addr: "m2"},
		},
		LoadResponse: []*proto.MetaPartitionLoadResponse{
			{
				Addr:      "m1",
				DoCompare: true,
				RaftInfo: proto.RaftInfo{
					RaftStatus:   raft.Status{Leader: 1, NodeID: 1},
					PendingPeers: []uint64{3},
				},
			},
		},
	}

	mp.checkPeerDiffWithRaft(c)
	_, ok := c.AbnormalRaftMP.Load(mp.PartitionID)
	require.True(t, ok, "pending peers should mark abnormal")
}

func TestCheckPeerDiffWithRaft_PeerMismatchAndRecovery(t *testing.T) {
	c := &Cluster{ClusterTopoSubItem: ClusterTopoSubItem{AbnormalRaftMP: &sync.Map{}}}
	mp := &MetaPartition{
		PartitionID: 2,
		Peers: []proto.Peer{
			{ID: 1, Addr: "m1"},
			{ID: 2, Addr: "m2"},
		},
		LoadResponse: []*proto.MetaPartitionLoadResponse{
			{
				Addr:      "m1",
				DoCompare: true,
				RaftInfo: proto.RaftInfo{
					RaftStatus: raft.Status{Leader: 1, NodeID: 1},
					Hosts:      []proto.Peer{{ID: 1, Addr: "m1"}, {ID: 99, Addr: "ghost"}},
				},
			},
		},
	}

	// extra peer in raft -> abnormal
	mp.checkPeerDiffWithRaft(c)
	_, ok := c.AbnormalRaftMP.Load(mp.PartitionID)
	require.True(t, ok)

	// fix raft hosts to match master, abnormal should be cleared
	mp.LoadResponse[0].RaftInfo.Hosts = []proto.Peer{{ID: 1, Addr: "m1"}, {ID: 2, Addr: "m2"}}
	mp.checkPeerDiffWithRaft(c)
	_, ok = c.AbnormalRaftMP.Load(mp.PartitionID)
	require.False(t, ok)
}

func TestCheckMetaReplicaMeta(t *testing.T) {
	server.cluster.checkMetaReplicaMeta()
}

func newMetaRecoveryTestCluster(vol *Vol, mp *MetaPartition) *Cluster {
	volName := vol.Name
	if vol.MetaPartitions == nil {
		vol.MetaPartitions = make(map[uint64]*MetaPartition)
	}
	if vol.mpsLock == nil {
		vol.mpsLock = newMpsLockManager(vol)
	}
	mp.volName = volName
	vol.MetaPartitions[mp.PartitionID] = mp
	return &Cluster{
		Name: "test-cluster",
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{volName: vol},
		},
		ClusterDecommission: ClusterDecommission{
			BadMetaPartitionIds: new(sync.Map),
		},
		partition: &mockPartition{isLeader: true},
	}
}

func TestCheckMetaPartitionRecoveryProgressSkipsInitializingVol(t *testing.T) {
	const volName = "vol-mp-init"
	vol := createTestVolForDecommission(volName)
	vol.Status = proto.VolStatusInitializing
	mp := newMetaPartition(10, 0, defaultMaxMetaPartitionInodeID, 3, volName, 1, 0)
	mp.IsRecover.Store(true)
	mp.Replicas = []*MetaReplica{
		{MaxInodeID: 100},
		{MaxInodeID: 2000},
	}
	cluster := newMetaRecoveryTestCluster(vol, mp)
	cluster.BadMetaPartitionIds.Store("node1", []uint64{mp.PartitionID})

	cluster.checkMetaPartitionRecoveryProgress()

	require.True(t, mp.IsRecover.Load())
}

func TestCheckMetaPartitionRecoveryProgressDelayDeleteVolCompletes(t *testing.T) {
	const volName = "vol-mp-delay-delete"
	vol := createTestVolForDecommission(volName)
	vol.Status = proto.VolStatusMarkDelete
	vol.Forbidden = true
	vol.DeleteExecTime = time.Now().Add(time.Hour)
	require.True(t, vol.isUnavailable())
	mp := newMetaPartition(11, 0, defaultMaxMetaPartitionInodeID, 3, volName, 1, 0)
	mp.IsRecover.Store(true)
	mp.Replicas = []*MetaReplica{
		{MaxInodeID: 1000},
		{MaxInodeID: 1001},
	}
	cluster := newMetaRecoveryTestCluster(vol, mp)
	cluster.BadMetaPartitionIds.Store("node1", []uint64{mp.PartitionID})

	cluster.checkMetaPartitionRecoveryProgress()

	require.False(t, mp.IsRecover.Load())
	_, ok := cluster.BadMetaPartitionIds.Load("node1")
	require.False(t, ok)
}
