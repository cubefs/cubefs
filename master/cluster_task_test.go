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

func TestAddDataNodeTasks_and_addDataNodeTask(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	c.addDataNodeTasks(nil)
	c.addDataNodeTask(nil)
	c.addDataNodeTasks([]*proto.AdminTask{})
	task := &proto.AdminTask{OperatorAddr: "no-such-dn:17320", ID: "t1"}
	c.addDataNodeTask(task)
	c.addDataNodeTasks([]*proto.AdminTask{nil, task})
}

func TestAddMetaNodeTasks(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	c.addMetaNodeTasks(nil)
	c.addMetaNodeTasks([]*proto.AdminTask{})
	c.addMetaNodeTasks([]*proto.AdminTask{nil, {OperatorAddr: "no-such-mn:17210", ID: "m1"}})
}

func TestAddLcNodeTasks(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	c.addLcNodeTasks(nil)
	c.addLcNodeTasks([]*proto.AdminTask{})
	c.addLcNodeTasks([]*proto.AdminTask{nil, {OperatorAddr: "no-lc:19000", ID: "l1"}})
}

func TestCheckInactiveMetaNodes(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	out, err := c.checkInactiveMetaNodes()
	require.NoError(t, err)
	require.Empty(t, out)

	c.metaNodes.Store("a1", &MetaNode{Addr: "a1", IsActive: false})
	c.metaNodes.Store("a2", &MetaNode{Addr: "a2", IsActive: true})
	out, err = c.checkInactiveMetaNodes()
	require.NoError(t, err)
	require.Equal(t, []string{"a1"}, out)
}

func TestCheckMultipleReplicasOnSameMachineForMigration(t *testing.T) {
	t.Parallel()
	c := &Cluster{cfg: &clusterConfig{AllowMultipleReplicasOnSameMachine: false}}
	err := c.checkMultipleReplicasOnSameMachineForMigration([]string{"10.0.0.1:17320"}, "10.0.0.1:17321")
	require.Error(t, err)

	c2 := &Cluster{cfg: &clusterConfig{AllowMultipleReplicasOnSameMachine: true}}
	require.NoError(t, c2.checkMultipleReplicasOnSameMachineForMigration([]string{"10.0.0.1:17320"}, "10.0.0.2:17320"))
}

func TestDecommissionMetaPartition_forbidden(t *testing.T) {
	t.Parallel()
	c := &Cluster{
		Name: "c",
		ClusterDecommission: ClusterDecommission{
			ForbidMpDecommission: true,
		},
	}
	mp := &MetaPartition{PartitionID: 1, volName: "v"}
	err := c.decommissionMetaPartition("addr", mp, proto.StoreModeMem)
	require.Error(t, err)
	require.Contains(t, err.Error(), "disabled")
}

func TestPrepareMetaPartitionMigration_errors(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c", cfg: newClusterConfig(), ClusterVolSubItem: ClusterVolSubItem{vols: map[string]*Vol{}}}
	mp := &MetaPartition{
		PartitionID: 42,
		volName:     "volx",
		Hosts:       []string{"10.0.0.1:17210", "10.0.0.2:17210"},
	}
	_, _, err := c.prepareMetaPartitionMigration("not-in-hosts", "", mp, proto.StoreModeMem)
	require.Error(t, err)

	_, _, err = c.prepareMetaPartitionMigration("10.0.0.1:17210", "10.0.0.2:17210", mp, proto.StoreModeMem)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exist")
}

func TestCheckReplicaMetaPartitions_emptyCluster(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c", cfg: newClusterConfig()}
	// Match fully-initialized cluster: these paths use *sync.Map without nil checks.
	c.inodeCountNotEqualMP = new(sync.Map)
	c.maxInodeNotEqualMP = new(sync.Map)
	c.dentryCountNotEqualMP = new(sync.Map)
	c.AbnormalRaftMP = new(sync.Map)
	c.BadMetaPartitionIds = new(sync.Map)
	c.RecoverMetaPartitionIds = new(sync.Map)
	d, err := c.checkReplicaMetaPartitions()
	require.NoError(t, err)
	require.NotNil(t, d)
	require.Empty(t, d.LackReplicaMps)

	d1, err := c.checkReplicaMetaPartitionsV1()
	require.NoError(t, err)
	require.NotNil(t, d1)
}

func TestDealUpdateMetaPartitionResp(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	require.NoError(t, c.dealUpdateMetaPartitionResp("n", &proto.UpdateMetaPartitionResponse{Status: proto.TaskSucceeds}))
	require.NoError(t, c.dealUpdateMetaPartitionResp("n", &proto.UpdateMetaPartitionResponse{Status: proto.TaskFailed, Result: "x"}))
}

func TestDealOpMetaNodeMultiVerResp_and_dataNode(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c", ClusterVolSubItem: ClusterVolSubItem{vols: map[string]*Vol{}}}
	resp := &proto.MultiVersionOpResponse{VolumeID: "missing", Status: proto.TaskFailed, Result: "e"}
	require.Error(t, c.dealOpMetaNodeMultiVerResp("n", resp))
	require.Error(t, c.dealOpDataNodeMultiVerResp("n", resp))
}

func TestDealDeleteMetaPartitionResp_taskFailed(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	err := c.dealDeleteMetaPartitionResp("addr", &proto.DeleteMetaPartitionResponse{Status: proto.TaskFailed, Result: "bad"})
	require.NoError(t, err)
}

func TestDealDeleteMetaPartitionResp_missingPartition(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	err := c.dealDeleteMetaPartitionResp("addr", &proto.DeleteMetaPartitionResponse{
		Status:      proto.TaskSucceeds,
		PartitionID: 999999999999,
	})
	require.Error(t, err)
}

func TestDealDeleteDataPartitionResponse(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	require.NoError(t, c.dealDeleteDataPartitionResponse("addr", &proto.DeleteDataPartitionResponse{Status: proto.TaskFailed, Result: "x"}))
	err := c.dealDeleteDataPartitionResponse("addr", &proto.DeleteDataPartitionResponse{Status: proto.TaskSucceeds, PartitionId: 123456789})
	require.Error(t, err)
}

func TestHandleResponseToLoadDataPartition_earlyReturn(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	require.NoError(t, c.handleResponseToLoadDataPartition("1.2.3.4:17320", &proto.LoadDataPartitionResponse{Status: proto.TaskFailed}))
	require.NoError(t, c.handleResponseToLoadDataPartition("1.2.3.4:17320", &proto.LoadDataPartitionResponse{
		Status:            proto.TaskSucceeds,
		PartitionSnapshot: nil,
	}))
}

func TestHandleDataNodeHeartbeatResp_statusAndMissingNode(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	err := c.handleDataNodeHeartbeatResp("9.9.9.9:17320", &proto.DataNodeHeartbeatResponse{Status: proto.TaskFailed}, "r1")
	require.NoError(t, err)

	err = c.handleDataNodeHeartbeatResp("9.9.9.9:17320", &proto.DataNodeHeartbeatResponse{Status: proto.TaskSucceeds}, "r2")
	require.Error(t, err)
}

func TestDealMetaNodeHeartbeatResp_failedAndMissingNode(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	require.NoError(t, c.dealMetaNodeHeartbeatResp("m", &proto.MetaNodeHeartbeatResponse{Status: proto.TaskFailed, Result: "x"}))
	err := c.dealMetaNodeHeartbeatResp("8.8.8.8:17210", &proto.MetaNodeHeartbeatResponse{Status: proto.TaskSucceeds})
	require.Error(t, err)
}

func TestHandleMetaNodeTaskResponse_nilAndMissingNode(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	require.NoError(t, c.handleMetaNodeTaskResponse("any", nil))

	task := &proto.AdminTask{OperatorAddr: "no-meta:17210", ID: "id1", OpCode: proto.OpMetaNodeHeartbeat}
	err := c.handleMetaNodeTaskResponse("no-meta:17210", task)
	require.Error(t, err)
}

func TestHandleDataNodeTaskResponse_nilAndMissingNode(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c"}
	c.handleDataNodeTaskResponse("any", nil)

	task := &proto.AdminTask{OperatorAddr: "no-dn:17320", ID: "id2", OpCode: proto.OpDataNodeHeartbeat}
	c.handleDataNodeTaskResponse("no-dn:17320", task)
}

func TestUpdateDataNode_nilAndUnknownVol(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c", cfg: newClusterConfig(), ClusterVolSubItem: ClusterVolSubItem{vols: map[string]*Vol{}}}
	dn := &DataNode{Addr: "dn1"}
	c.updateDataNode(dn, []*proto.DataPartitionReport{
		nil,
		{VolName: "no-vol", PartitionID: 1},
	})
}

func TestUpdateInodeIDUpperBound_shortCircuits(t *testing.T) {
	t.Parallel()
	vol := &Vol{Name: "novol", MetaPartitions: map[uint64]*MetaPartition{10: {PartitionID: 10}}}
	vol.mpsLock = newMpsLockManager(vol)
	c := &Cluster{Name: "c", cfg: newClusterConfig(), ClusterVolSubItem: ClusterVolSubItem{vols: map[string]*Vol{"novol": vol}}}
	mp := &MetaPartition{volName: "novol", PartitionID: 7}
	mr := &proto.MetaPartitionReport{PartitionID: 7}
	mn := &MetaNode{Addr: "mn"}
	require.NoError(t, c.updateInodeIDUpperBound(mp, mr, false, mn))

	// hasArriveThreshold true loads vol; PartitionID 7 is below max (10) so split is skipped without error
	require.NoError(t, c.updateInodeIDUpperBound(mp, mr, true, mn))
}

func TestGetMetaReplicaLearnerInfo(t *testing.T) {
	t.Parallel()
	mp := &MetaPartition{
		PartitionID: 3,
		Peers: []proto.Peer{
			{Addr: "voter", Type: raftProto.PeerNormal},
			{Addr: "learner-auto", Type: raftProto.PeerLearner, ManualPromote: false},
			{Addr: "learner-manual", Type: raftProto.PeerLearner, ManualPromote: true},
		},
	}
	isL, man, err := getMetaReplicaLearnerInfo(mp, "voter")
	require.NoError(t, err)
	require.False(t, isL)
	require.False(t, man)

	isL, man, err = getMetaReplicaLearnerInfo(mp, "learner-manual")
	require.NoError(t, err)
	require.True(t, isL)
	require.True(t, man)

	_, _, err = getMetaReplicaLearnerInfo(mp, "nope")
	require.Error(t, err)
}

func TestIsLackReplicaMetaPartition_edges(t *testing.T) {
	t.Parallel()
	mp := &MetaPartition{ReplicaNum: 2, Peers: []proto.Peer{{Addr: "a", Type: raftProto.PeerNormal}}}
	require.True(t, isLackReplicaMetaPartition(mp))
	mp.Peers = append(mp.Peers, proto.Peer{Addr: "b", Type: raftProto.PeerNormal})
	require.False(t, isLackReplicaMetaPartition(mp))
}

func TestHasLearnerFlagMismatch_unknownReplicaIgnored(t *testing.T) {
	t.Parallel()
	mp := &MetaPartition{
		Peers: []proto.Peer{{Addr: "p1", Type: raftProto.PeerNormal}},
		Replicas: []*MetaReplica{
			{Addr: "p1", IsLearner: false},
			{Addr: "orphan", IsLearner: true},
		},
	}
	require.False(t, hasLearnerFlagMismatch(mp))
}

func TestIsExcessiveReplicaMetaPartition_nilCluster_edges(t *testing.T) {
	t.Parallel()
	mp := &MetaPartition{ReplicaNum: 2, Peers: []proto.Peer{
		{Addr: "a", Type: raftProto.PeerNormal},
		{Addr: "b", Type: raftProto.PeerNormal},
	}}
	require.False(t, IsExcessiveReplicaMetaPartition(nil, mp))
	mp.Peers = append(mp.Peers, proto.Peer{Addr: "c", Type: raftProto.PeerNormal})
	require.True(t, IsExcessiveReplicaMetaPartition(nil, mp))
}

func TestShouldUseTagForMetaPartitionSelection_emptySrc(t *testing.T) {
	t.Parallel()
	c := &Cluster{cfg: &clusterConfig{}, ClusterVolSubItem: ClusterVolSubItem{vols: map[string]*Vol{
		"v": {Name: "v", MpTag: "t"},
	}}}
	mp := &MetaPartition{volName: "v", Peers: []proto.Peer{{Addr: "x", Type: raftProto.PeerNormal}}}
	use, err := c.shouldUseTagForMetaPartitionSelection(mp, "")
	require.NoError(t, err)
	require.False(t, use) // srcAddr=="" short-circuits before tag logic
}

func TestApplyMetaPartitionSelectionTag(t *testing.T) {
	t.Parallel()

	const srcAddr = "10.0.0.1:17210"
	mp := &MetaPartition{
		volName: "v",
		Peers:   []proto.Peer{{Addr: srcAddr, Type: raftProto.PeerNormal}},
	}

	t.Run("default source tag skips tag filter", func(t *testing.T) {
		c := &Cluster{cfg: &clusterConfig{DefaultMpTag: "old->new"}}
		c.metaNodes.Store(srcAddr, &MetaNode{Addr: srcAddr, Tag: DefaultTag})
		param := &selectParam{}

		require.NoError(t, c.applyMetaPartitionSelectionTag(param, mp, srcAddr))
		require.Equal(t, int32(0), param.selectType)
		require.Equal(t, DefaultTag, param.tag)
	})

	t.Run("non-default source tag keeps tag filter", func(t *testing.T) {
		c := &Cluster{cfg: &clusterConfig{DefaultMpTag: "old->new"}}
		c.metaNodes.Store(srcAddr, &MetaNode{Addr: srcAddr, Tag: "old"})
		param := &selectParam{}

		require.NoError(t, c.applyMetaPartitionSelectionTag(param, mp, srcAddr))
		require.Equal(t, int32(proto.SelectTypeTag), param.selectType)
		require.Equal(t, "old", param.tag)
	})
}

func TestSelectTargetMetaPeer_noHosts(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "c", cfg: newClusterConfig(), ClusterTopoSubItem: ClusterTopoSubItem{t: newTopology()}}
	mp := &MetaPartition{PartitionID: 99, volName: "vv", Hosts: []string{}}
	_, _, err := c.selectTargetMetaPeer(mp, "", "", proto.StoreModeMem, proto.DefaultRegion)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no hosts")
}

func TestMigrateMetaPartitionByLearner_fallbackWhenDisabled(t *testing.T) {
	t.Parallel()
	c := &Cluster{
		Name: "cl",
		cfg:  newClusterConfig(),
		ClusterVolSubItem: ClusterVolSubItem{vols: map[string]*Vol{
			"vv": {Name: "vv"},
		}},
		ClusterDecommission: ClusterDecommission{
			EnableMpDecommissionByLearner: false,
			BadMetaPartitionIds:           new(sync.Map),
			RecoverMetaPartitionIds:       new(sync.Map),
		},
	}
	mp := &MetaPartition{PartitionID: 9, volName: "vv", Hosts: []string{"10.0.0.1:17210"}}
	err := c.migrateMetaPartitionByLearner("10.0.0.1:17210", "", mp, proto.StoreModeMem, proto.ManualDecommission)
	require.Error(t, err)
}
