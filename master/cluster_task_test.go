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
	require.True(t, IsExcessiveReplicaMetaPartition(mp))

	mp.Peers = []proto.Peer{
		{Addr: "p1", Type: raftProto.PeerNormal},
		{Addr: "l1", Type: raftProto.PeerLearner},
	}
	require.False(t, IsExcessiveReplicaMetaPartition(mp))
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
