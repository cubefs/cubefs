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
	"testing"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestIsLackReplicaMetaPartition(t *testing.T) {
	mp := &MetaPartition{
		ReplicaNum: 1,
		Peers: []proto.Peer{
			{Addr: "learner-1", Type: raftProto.PeerLearner},
		},
	}
	require.True(t, isLackReplicaMetaPartition(mp))

	mp.Peers = []proto.Peer{
		{Addr: "peer-1", Type: raftProto.PeerNormal},
	}
	require.False(t, isLackReplicaMetaPartition(mp))
}

func TestIsExcessiveReplicaMetaPartition(t *testing.T) {
	mp := &MetaPartition{
		ReplicaNum: 1,
		Peers: []proto.Peer{
			{Addr: "p1", Type: raftProto.PeerNormal},
			{Addr: "p2", Type: raftProto.PeerNormal},
		},
	}
	require.True(t, IsExcessiveReplicaMetaPartition(nil, mp))

	// Auto learner with ReplicaNum met for voters still needs cleanup (same as needReplicaMetaRestore stage1).
	mp.Peers = []proto.Peer{
		{Addr: "p1", Type: raftProto.PeerNormal},
		{Addr: "l1", Type: raftProto.PeerLearner},
	}
	require.True(t, IsExcessiveReplicaMetaPartition(nil, mp))
}

func TestIsExcessiveReplicaMetaPartition_ManualLearnerDoesNotCountAgainstReplicaNum(t *testing.T) {
	mp := &MetaPartition{
		ReplicaNum: 3,
		Peers: []proto.Peer{
			{Addr: "m1", Type: raftProto.PeerNormal},
			{Addr: "m2", Type: raftProto.PeerNormal},
			{Addr: "m3", Type: raftProto.PeerNormal},
			{Addr: "learner", Type: raftProto.PeerLearner, ManualPromote: true},
		},
	}
	require.False(t, IsExcessiveReplicaMetaPartition(nil, mp))
}

func TestIsExcessiveReplicaMetaPartition_RegionPolicy(t *testing.T) {
	learnerAddr := "10.0.1.10:17210"
	vol := &Vol{Name: "v1", mpPolicy: nil}
	c := &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{"v1": vol},
		},
	}
	c.metaNodes.Store(learnerAddr, &MetaNode{Addr: learnerAddr, Region: "west"})

	// Two voters + one manual learner: count == ReplicaNum, but nil mpPolicy violates region policy.
	mp := &MetaPartition{
		volName:    "v1",
		Region:     "east",
		ReplicaNum: 3,
		Peers: []proto.Peer{
			{Addr: "m1", Type: raftProto.PeerNormal},
			{Addr: "m2", Type: raftProto.PeerNormal},
			{Addr: learnerAddr, Type: raftProto.PeerLearner, ManualPromote: true},
		},
	}
	require.False(t, IsExcessiveReplicaMetaPartition(nil, mp))
	require.True(t, IsExcessiveReplicaMetaPartition(c, mp))

	vol.mpPolicy = map[string]*proto.VolMpPolicy{
		"east": {Learner: map[string]*proto.LearnerPolicy{"west": {Mode: proto.StoreModeMem}}},
	}
	require.False(t, IsExcessiveReplicaMetaPartition(c, mp))
}

func TestHasLearnerFlagMismatch(t *testing.T) {
	mp := &MetaPartition{
		Peers: []proto.Peer{
			{Addr: "m1", Type: raftProto.PeerLearner},
			{Addr: "m2", Type: raftProto.PeerNormal},
		},
		Replicas: []*MetaReplica{
			{Addr: "m1", IsLearner: false},
			{Addr: "m2", IsLearner: false},
		},
	}
	require.True(t, hasLearnerFlagMismatch(mp))

	mp.Replicas[0].IsLearner = true
	require.False(t, hasLearnerFlagMismatch(mp))
}

func TestShouldUseTagForMetaPartitionSelection(t *testing.T) {
	cluster := &Cluster{
		cfg: &clusterConfig{},
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{
				"vol-1": {
					Name:  "vol-1",
					MpTag: "tag-a",
				},
			},
		},
	}

	mp := &MetaPartition{
		PartitionID: 1,
		volName:     "vol-1",
		Peers: []proto.Peer{
			{Addr: "manual-learner", Type: raftProto.PeerLearner, ManualPromote: true},
			{Addr: "auto-learner", Type: raftProto.PeerLearner, ManualPromote: false},
			{Addr: "normal", Type: raftProto.PeerNormal},
		},
	}

	useTag, err := cluster.shouldUseTagForMetaPartitionSelection(mp, "manual-learner")
	require.NoError(t, err)
	require.False(t, useTag)

	useTag, err = cluster.shouldUseTagForMetaPartitionSelection(mp, "auto-learner")
	require.NoError(t, err)
	require.True(t, useTag)

	useTag, err = cluster.shouldUseTagForMetaPartitionSelection(mp, "manual-learner")
	require.NoError(t, err)
	require.False(t, useTag)
}
