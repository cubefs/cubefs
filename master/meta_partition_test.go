package master

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetaPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkMetaPartitions()
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	createMetaPartition(commonVol, t)
	maxPartitionID := commonVol.maxMetaPartitionID()
	getMetaPartition(commonVol.Name, maxPartitionID, t)
	loadMetaPartitionTest(commonVol, maxPartitionID, t)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	decommissionMetaPartition(commonVol, maxPartitionID, t)
}

func createMetaPartition(vol *Vol, t *testing.T) {
	count := 3
	vol.mpsLock.RLock()
	oldPartitionCount := len(vol.MetaPartitions)
	vol.mpsLock.RUnlock()

	reqURL := fmt.Sprintf("%v%v?name=%v&count=%v",
		hostAddr, proto.AdminCreateMetaPartition, vol.Name, count)
	process(reqURL, t)

	vol, err := server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}

	vol.mpsLock.RLock()
	newPartitionCount := len(vol.MetaPartitions)
	newMaxPartitionID := vol.maxMetaPartitionID()
	newMaxMetaPartition, err := vol.metaPartition(newMaxPartitionID)
	if err != nil {
		vol.mpsLock.RUnlock()
		t.Errorf("createMetaPartition,err [%v]", err)
		return
	}

	assert.Equal(t, oldPartitionCount+count, newPartitionCount)

	if defaultMaxMetaPartitionInodeID != newMaxMetaPartition.End {
		t.Errorf("createMetaPartition,err expected MaxMetaPartitionEnd [%v] , actual MaxMetaPartitionEnd [%v]", defaultMaxMetaPartitionInodeID, newMaxMetaPartition.End)
	}
	vol.mpsLock.RUnlock()

	server.cluster.checkMetaNodeHeartbeat()
}

func getMetaPartition(volName string, id uint64, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v",
		hostAddr, proto.ClientMetaPartition, volName, id)
	process(reqURL, t)
}

func loadMetaPartitionTest(vol *Vol, id uint64, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v", hostAddr, proto.AdminLoadMetaPartition, vol.Name, id)
	process(reqURL, t)
}

func decommissionMetaPartition(vol *Vol, id uint64, t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetCluster)
	process(reqURL, t)
	vol, err := server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}
	mp, err := vol.metaPartition(id)
	if err != nil {
		t.Errorf("decommissionMetaPartition,err [%v]", err)
		return
	}
	offlineAddr := mp.Hosts[0]
	reqURL = fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, proto.AdminDecommissionMetaPartition, vol.Name, id, offlineAddr)
	process(reqURL, t)
	mp, err = server.cluster.getMetaPartitionByID(id)
	if err != nil {
		t.Errorf("decommissionMetaPartition,err [%v]", err)
		return
	}
	if contains(mp.Hosts, offlineAddr) {
		t.Errorf("decommissionMetaPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, mp.Hosts)
		return
	}
}

func TestIsMissingReplica(t *testing.T) {
	mp := &MetaPartition{
		Replicas: []*MetaReplica{
			{Addr: "m1", ReportTime: time.Now().Unix()},
		},
	}

	require.False(t, mp.isMissingReplica("m1", 5))

	mp.Replicas[0].ReportTime = time.Now().Add(-10 * time.Second).Unix()
	require.True(t, mp.isMissingReplica("m1", 5))

	require.True(t, mp.isMissingReplica("m2", 5))
}

func TestCreateTaskToRemoveRaftMember(t *testing.T) {
	mp := &MetaPartition{PartitionID: 10}
	peer := proto.Peer{ID: 1, Addr: "m1"}

	_, err := mp.createTaskToRemoveRaftMember(peer, false, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no leader")

	mp.Replicas = []*MetaReplica{{Addr: "m1", IsLeader: true}}
	task, err := mp.createTaskToRemoveRaftMember(peer, false, true)
	require.NoError(t, err)
	require.Equal(t, proto.OpRemoveMetaPartitionRaftMember, task.OpCode)
	require.Equal(t, mp.PartitionID, task.PartitionID)
}

func TestCreateTaskToDeleteReplica(t *testing.T) {
	mr := &MetaReplica{Addr: "m1"}
	task := mr.createTaskToDeleteReplica(5, true)
	require.Equal(t, proto.OpDeleteMetaPartition, task.OpCode)
	require.Equal(t, uint64(5), task.PartitionID)
}

func TestCheckIntersection(t *testing.T) {
	c := &Cluster{}

	// empty intersection -> error and mark
	mp := &MetaPartition{
		PartitionID: 20,
		Peers:       []proto.Peer{{Addr: "m1"}},
		Replicas: []*MetaReplica{
			{Addr: "r1", LocalPeers: []proto.Peer{{Addr: "m2"}}},
		},
	}
	err := mp.checkIntersection(c)
	require.ErrorIs(t, err, proto.ErrDpNoSamePeer)
	_, ok := c.NoSamePeerMps.Load(mp.PartitionID)
	require.True(t, ok)

	// has intersection -> success and cleaned
	mp.Peers = []proto.Peer{{Addr: "m1"}, {Addr: "m2"}}
	mp.Replicas[0].LocalPeers = []proto.Peer{{Addr: "m1"}}
	err = mp.checkIntersection(c)
	require.NoError(t, err)
}

func TestNeedReplicaMetaRestore(t *testing.T) {
	mp := &MetaPartition{
		PartitionID: 30,
		ReplicaNum:  2,
		Peers: []proto.Peer{
			{Addr: "m1", Type: raftProto.PeerNormal},
			{Addr: "m2", Type: raftProto.PeerNormal},
			{Addr: "l1", Type: raftProto.PeerLearner, ManualPromote: false},
		},
		Replicas: []*MetaReplica{
			{Addr: "m1", LocalPeers: []proto.Peer{{Addr: "m1", Type: raftProto.PeerNormal}}},
			{Addr: "m2", LocalPeers: []proto.Peer{{Addr: "m2", Type: raftProto.PeerNormal}, {Addr: "x", Type: raftProto.PeerNormal}}},
		},
	}
	c := &Cluster{cfg: newClusterConfig()}

	// excessive non-learner + auto learner -> need restore
	require.True(t, mp.needReplicaMetaRestore(c))

	// remove auto learner, keep non-learners equal to ReplicaNum
	mp.Peers = []proto.Peer{
		{Addr: "m1", Type: raftProto.PeerNormal},
		{Addr: "m2", Type: raftProto.PeerNormal},
	}
	require.True(t, mp.needReplicaMetaRestore(c)) // replica m2 has extra peer "x" vs master peers

	// align master peers with leader LocalPeers, ensure non-learner count equals ReplicaNum
	mp.Replicas[1].LocalPeers = []proto.Peer{{Addr: "m2", Type: raftProto.PeerNormal}}
	mp.Replicas[0].LocalPeers = []proto.Peer{
		{Addr: "m1", Type: raftProto.PeerNormal},
		{Addr: "m2", Type: raftProto.PeerNormal},
	}
	mp.Replicas[0].IsLeader = true
	require.False(t, mp.needReplicaMetaRestore(c))

	// drop one non-learner -> need add replica
	mp.Peers = []proto.Peer{
		{Addr: "m1", Type: raftProto.PeerNormal},
	}
	require.True(t, mp.needReplicaMetaRestore(c))
}

func TestSetRestoreReplica(t *testing.T) {
	mp := &MetaPartition{}
	require.Equal(t, RestoreReplicaMetaStop, atomic.LoadUint32(&mp.RestoreReplicaMeta))

	ok := mp.setRestoreReplicaRunning()
	require.True(t, ok)
	require.Equal(t, RestoreReplicaMetaRunning, atomic.LoadUint32(&mp.RestoreReplicaMeta))

	ok = mp.setRestoreReplicaForbidden()
	require.False(t, ok, "should not switch from running to forbidden directly")

	// reset to stop then forbid
	mp.setRestoreReplicaStatus(RestoreReplicaMetaStop)
	ok = mp.setRestoreReplicaForbidden()
	require.True(t, ok)
	require.Equal(t, RestoreReplicaMetaForbidden, atomic.LoadUint32(&mp.RestoreReplicaMeta))
}
