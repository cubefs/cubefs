// Copyright 2024 The CubeFS Authors.
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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	utilConfig "github.com/cubefs/cubefs/util/config"
)

const (
	RocksdbFsmTestDir     = "/tmp/cfs/fsm_test"
	RocksdbFsmRootTestDir = "/tmp/cfs/fsm_test_root"
)

func getMpConfigForFsmTest(storeMode proto.StoreMode) (config *MetaPartitionConfig) {
	config = &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	if config.StoreMode == proto.StoreModeRocksDb {
		config.RocksDBDir = fmt.Sprintf("%v/%v_%v", RocksdbFsmTestDir, partitionId, time.Now().UnixMilli())
	}
	config.RootDir = fmt.Sprintf("%v/%v_%v", RocksdbFsmRootTestDir, partitionId, time.Now().UnixMilli())
	os.RemoveAll(config.RootDir)
	os.MkdirAll(config.RootDir, 0o755)
	return
}

func newMpForFsmTest(t *testing.T, storeMode proto.StoreMode) (mp *metaPartition) {
	var _ interface{} = t
	config := getMpConfigForFsmTest(storeMode)
	mp = newPartition(config, newManager())
	mp.manager.metaNode = &MetaNode{
		raftSyncSnapFormatVersion: SnapFormatVersion_1,
	}
	mp.uniqChecker = newUniqChecker()
	mp.multiVersionList = &proto.VolVersionInfoList{}
	// mp.storeChan = make(chan *storeMsg, 10000)
	return
}

func prepareDataForMpFsmTest(t *testing.T, mp *metaPartition) {
	prepareDataForMpTest(t, mp)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.inodeTree.SetApplyID(10)
	mp.applyID = 10
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, true)
	require.NoError(t, err)
}

func checkEmptyMpForMpFsmTest(t *testing.T, mp *metaPartition) {
	snap, err := mp.GetSnapShot()
	if err != nil {
		return
	}
	defer snap.Close()

	count := 0
	err = snap.Range(InodeType, func(item interface{}) bool {
		count++
		return true
	})
	require.NoError(t, err)

	count = 0
	err = snap.Range(DentryType, func(item interface{}) bool {
		count++
		return true
	})
	require.NoError(t, err)

	count = 0
	err = snap.Range(ExtendType, func(item interface{}) bool {
		count++
		return true
	})
	require.NoError(t, err)

	count = 0
	err = snap.Range(MultipartType, func(item interface{}) bool {
		count++
		return true
	})
	require.NoError(t, err)

	count = 0
	err = snap.Range(TransactionType, func(item interface{}) bool {
		count++
		return true
	})
	require.NoError(t, err)

	count = 0
	err = snap.Range(TransactionRollbackInodeType, func(item interface{}) bool {
		count++
		return true
	})
	require.NoError(t, err)

	count = 0
	err = snap.Range(TransactionRollbackDentryType, func(item interface{}) bool {
		count++
		return true
	})
	require.NoError(t, err)
}

func testApplySnapshot(t *testing.T, storeMode proto.StoreMode) {
	leaderMp := newMpForFsmTest(t, storeMode)
	followerMp := newMpForFsmTest(t, storeMode)
	prepareDataForMpFsmTest(t, leaderMp)
	checkTreeCntForMpTest(t, leaderMp)

	iter, err := leaderMp.Snapshot()
	require.NoError(t, err)

	require.EqualValues(t, 10, iter.ApplyIndex())

	go func() {
		sm := <-followerMp.storeChan
		err = followerMp.store(sm)
		require.NoError(t, err)
	}()
	err = followerMp.ApplySnapshot(nil, iter)
	require.NoError(t, err)

	iter.Close()

	require.EqualValues(t, 10, followerMp.getApplyID())

	checkTreeCntForMpTest(t, followerMp)

	err = leaderMp.Clear()
	require.NoError(t, err)

	handle, err := leaderMp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	leaderMp.inodeTree.SetApplyID(20)
	leaderMp.applyID = 20
	err = leaderMp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, true)
	require.NoError(t, err)

	checkEmptyMpForMpFsmTest(t, leaderMp)

	iter, err = leaderMp.Snapshot()
	require.NoError(t, err)

	require.EqualValues(t, 20, iter.ApplyIndex())

	go func() {
		sm := <-followerMp.storeChan
		err = followerMp.store(sm)
		require.NoError(t, err)
	}()
	err = followerMp.ApplySnapshot(nil, iter)
	require.NoError(t, err)

	iter.Close()

	require.EqualValues(t, 20, followerMp.getApplyID())

	checkEmptyMpForMpFsmTest(t, followerMp)
}

/*
	func TestApplySnapshot(t *testing.T) {
		testApplySnapshot(t, proto.StoreModeMem)
	}
*/
func TestApplySnapshot_Rocksdb(t *testing.T) {
	testApplySnapshot(t, proto.StoreModeRocksDb)
}

func TestCleanRocksdbMpFsmTestDir(t *testing.T) {
	os.RemoveAll(RocksdbFsmTestDir)
	os.RemoveAll(RocksdbFsmRootTestDir)
}

// testSnapIterator feeds prebuilt MetaItem records for ApplySnapshot tests.
type testSnapIterator struct {
	records [][]byte
	pos     int
}

func (it *testSnapIterator) Next() ([]byte, error) {
	if it.pos >= len(it.records) {
		return nil, io.EOF
	}
	data := it.records[it.pos]
	it.pos++
	return data, nil
}

func (it *testSnapIterator) ApplyIndex() uint64 { return 1 }

func (it *testSnapIterator) Close() {}

func marshalMetaItemForSnapTest(t *testing.T, op uint32, k, v []byte) []byte {
	t.Helper()
	item := NewMetaItem(op, k, v)
	data, err := item.MarshalBinary()
	require.NoError(t, err)
	return data
}

// corruptMultipartSnapValue mimics production snapshot blobs that panic in MultipartFromBytes.
func corruptMultipartSnapValue() []byte {
	raw := make([]byte, 247)
	for i := range raw {
		raw[i] = 0xff
	}
	return raw
}

func TestApplySnapshot_recoverMultipartDecodePanic_returnsError(t *testing.T) {
	require.Panics(t, func() {
		_ = MultipartFromBytes(corruptMultipartSnapValue())
	}, "sanity: corrupt multipart bytes must panic without recovery")

	mp := newMpForFsmTest(t, proto.StoreModeMem)
	mpID := mp.config.PartitionId
	corruptV := corruptMultipartSnapValue()

	verBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(verBytes, SnapFormatVersion_1)
	applyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(applyBytes, 1)

	iter := &testSnapIterator{records: [][]byte{
		marshalMetaItemForSnapTest(t, opFSMSnapFormatVersion, nil, verBytes),
		marshalMetaItemForSnapTest(t, opFSMApplyId, nil, applyBytes),
		marshalMetaItemForSnapTest(t, opFSMCreateMultipart, nil, corruptV),
	}}

	var applyErr error
	require.NotPanics(t, func() {
		applyErr = mp.ApplySnapshot(nil, iter)
	})
	require.Error(t, applyErr)
	require.Contains(t, applyErr.Error(), "ApplySnapshot panic")
	require.Contains(t, applyErr.Error(), fmt.Sprintf("mpId(%v)", mpID))
	require.Contains(t, applyErr.Error(), fmt.Sprintf("lastSnapOp(%v)", opFSMCreateMultipart))
	require.Contains(t, applyErr.Error(), fmt.Sprintf("lastSnapValueLen(%v)", len(corruptV)))
	require.True(t, strings.Contains(applyErr.Error(), "bounds") ||
		strings.Contains(applyErr.Error(), "out of range"),
		"panic value should mention slice bounds, got: %v", applyErr)
}

func TestApplySnapshot_recoverMultipartDecodePanic_rocksdb(t *testing.T) {
	root := t.TempDir()
	config := &MetaPartitionConfig{
		PartitionId:   10002,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     proto.StoreModeRocksDb,
		RootDir:       root,
		RocksDBDir:    root + "/rocksdb",
	}
	mp := newPartition(config, newManager())
	mp.manager.metaNode = &MetaNode{raftSyncSnapFormatVersion: SnapFormatVersion_1}
	mp.uniqChecker = newUniqChecker()
	mp.multiVersionList = &proto.VolVersionInfoList{}

	corruptV := corruptMultipartSnapValue()

	verBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(verBytes, SnapFormatVersion_1)
	applyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(applyBytes, 1)

	iter := &testSnapIterator{records: [][]byte{
		marshalMetaItemForSnapTest(t, opFSMSnapFormatVersion, nil, verBytes),
		marshalMetaItemForSnapTest(t, opFSMApplyId, nil, applyBytes),
		marshalMetaItemForSnapTest(t, opFSMCreateMultipart, nil, corruptV),
	}}

	err := mp.ApplySnapshot(nil, iter)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ApplySnapshot panic")
	require.Contains(t, err.Error(), fmt.Sprintf("lastSnapOp(%v)", opFSMCreateMultipart))
}

// Test cases for ApplyMemberChange function
func createTestMetaPartitionForApplyMember(t *testing.T) *metaPartition {
	testPath := fmt.Sprintf("/tmp/test_applymember_%d", os.Getpid())
	os.RemoveAll(testPath)
	os.MkdirAll(testPath, 0o755)

	raftPath := fmt.Sprintf("%s/raft", testPath)
	os.MkdirAll(raftPath, 0o755)

	config := &MetaPartitionConfig{
		PartitionId:   1,
		VolName:       "test_vol",
		Start:         0,
		End:           100,
		PartitionType: proto.VolumeTypeHot,
		Peers: []proto.Peer{
			{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
		},
		RootDir:   testPath,
		StoreMode: proto.StoreModeMem,
	}

	manager := &metadataManager{
		partitions: make(map[uint64]MetaPartition),
		metaNode: &MetaNode{
			raftPartitionCanUsingDifferentPort: false,
		},
	}

	mp := newPartition(config, manager)
	mp.config.NodeId = 1

	// Create a real RaftStore instance using NewRaftStore
	raftConf := &raftstore.Config{
		NodeID:        1,
		RaftPath:      raftPath,
		IPAddr:        "127.0.0.1",
		HeartbeatPort: 17210,
		ReplicaPort:   17211,
	}
	extendCfg := utilConfig.NewConfig()
	raftStore, err := raftstore.NewRaftStore(raftConf, extendCfg)
	require.NoError(t, err)
	mp.config.RaftStore = raftStore

	// Initialize required fields
	err = mp.initObjects(true)
	require.NoError(t, err)

	// Persist initial metadata to ensure file exists for verification
	err = mp.persistMetadata()
	require.NoError(t, err)

	// Cleanup: stop raftStore when test is done
	t.Cleanup(func() {
		if raftStore != nil {
			raftStore.Stop()
		}
		os.RemoveAll(testPath)
	})

	return mp
}

// verifyPersistedMetadata verifies that the persisted metadata file contains the expected peers
func verifyPersistedMetadata(t *testing.T, mp *metaPartition, expectedPeers []proto.Peer) {
	metaFile := fmt.Sprintf("%s/meta", mp.config.RootDir)
	data, err := os.ReadFile(metaFile)
	require.NoError(t, err, "Should be able to read metadata file")
	require.NotEmpty(t, data, "Metadata file should not be empty")

	var config MetaPartitionConfig
	err = json.Unmarshal(data, &config)
	require.NoError(t, err, "Should be able to unmarshal metadata")

	require.Equal(t, len(expectedPeers), len(config.Peers), "Peer count should match")

	// Create a map for easier lookup (peers may be sorted)
	expectedPeerMap := make(map[uint64]proto.Peer)
	for _, peer := range expectedPeers {
		expectedPeerMap[peer.ID] = peer
	}

	// Verify each peer exists and has correct properties
	for _, peer := range config.Peers {
		expectedPeer, exists := expectedPeerMap[peer.ID]
		require.True(t, exists, "Peer %v should exist in persisted metadata", peer.ID)
		require.Equal(t, expectedPeer.Addr, peer.Addr, "Peer %v address should match", peer.ID)
		require.Equal(t, expectedPeer.Type, peer.Type, "Peer %v type should match", peer.ID)
	}

	// Verify all expected peers are present
	require.Equal(t, len(expectedPeers), len(config.Peers), "All expected peers should be present")
}

func TestApplyMemberChange_AddNode_Success(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
			Type: raftProto.PeerNormal,
		},
		OpType: proto.OpTypeAddRaftMember,
	}
	context, err := json.Marshal(req)
	require.NoError(t, err)

	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfAddNode,
		Peer:    raftProto.Peer{ID: 2},
		Context: context,
	}

	initialPeerCount := len(mp.config.Peers)
	resp, err := mp.ApplyMemberChange(confChange, 10)

	require.NoError(t, err)
	require.Nil(t, resp)
	require.Equal(t, initialPeerCount+1, len(mp.config.Peers))

	// Check if new peer was added
	found := false
	for _, peer := range mp.config.Peers {
		if peer.ID == 2 {
			found = true
			require.Equal(t, "127.0.0.1:17211", peer.Addr)
			break
		}
	}
	require.True(t, found, "New peer should be added")

	// Verify persisted metadata
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
		{ID: 2, Addr: "127.0.0.1:17211", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_AddNode_PeerAlreadyExists(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	// Add peer that already exists
	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   1, // Already exists
			Addr: "127.0.0.1:17210",
			Type: raftProto.PeerNormal,
		},
		OpType: proto.OpTypeAddRaftMember,
	}
	context, err := json.Marshal(req)
	require.NoError(t, err)

	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfAddNode,
		Peer:    raftProto.Peer{ID: 1},
		Context: context,
	}

	initialPeerCount := len(mp.config.Peers)
	resp, err := mp.ApplyMemberChange(confChange, 10)

	require.NoError(t, err)
	require.Nil(t, resp)
	// Peer count should not change
	require.Equal(t, initialPeerCount, len(mp.config.Peers))

	// Verify persisted metadata - should remain unchanged
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_AddLearner_Success(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
		},
		OpType: proto.OpTypeAddLearner,
	}
	context, err := json.Marshal(req)
	require.NoError(t, err)

	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfAddLearner,
		Peer:    raftProto.Peer{ID: 2},
		Context: context,
	}

	initialPeerCount := len(mp.config.Peers)
	resp, err := mp.ApplyMemberChange(confChange, 10)

	require.NoError(t, err)
	require.Nil(t, resp)
	require.Equal(t, initialPeerCount+1, len(mp.config.Peers))

	// Check if learner was added with correct type
	found := false
	for _, peer := range mp.config.Peers {
		if peer.ID == 2 {
			found = true
			require.Equal(t, raftProto.PeerLearner, peer.Type)
			require.Equal(t, "127.0.0.1:17211", peer.Addr)
			break
		}
	}
	require.True(t, found, "Learner peer should be added")

	// Verify persisted metadata
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
		{ID: 2, Addr: "127.0.0.1:17211", Type: raftProto.PeerLearner},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_AddLearner_PeerAlreadyExists(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	// First add a learner
	req1 := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
		},
		OpType: proto.OpTypeAddLearner,
	}
	context1, _ := json.Marshal(req1)
	confChange1 := &raftProto.ConfChange{
		Type:    raftProto.ConfAddLearner,
		Peer:    raftProto.Peer{ID: 2},
		Context: context1,
	}
	_, err := mp.ApplyMemberChange(confChange1, 10)
	require.NoError(t, err)

	// Try to add the same learner again
	req2 := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
		},
		OpType: proto.OpTypeAddLearner,
	}
	context2, err := json.Marshal(req2)
	require.NoError(t, err)

	confChange2 := &raftProto.ConfChange{
		Type:    raftProto.ConfAddLearner,
		Peer:    raftProto.Peer{ID: 2},
		Context: context2,
	}

	peerCountBefore := len(mp.config.Peers)
	resp, err := mp.ApplyMemberChange(confChange2, 11)

	require.NoError(t, err)
	require.Nil(t, resp)
	// Peer count should not change
	require.Equal(t, peerCountBefore, len(mp.config.Peers))

	// Verify persisted metadata - should have one learner
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
		{ID: 2, Addr: "127.0.0.1:17211", Type: raftProto.PeerLearner},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_PromoteLearner_Success(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	// First add a learner
	req1 := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
		},
		OpType: proto.OpTypeAddLearner,
	}
	context1, _ := json.Marshal(req1)
	confChange1 := &raftProto.ConfChange{
		Type:    raftProto.ConfAddLearner,
		Peer:    raftProto.Peer{ID: 2},
		Context: context1,
	}
	_, err := mp.ApplyMemberChange(confChange1, 10)
	require.NoError(t, err)

	// Verify learner was added
	var learnerPeer *proto.Peer
	for i, peer := range mp.config.Peers {
		if peer.ID == 2 {
			learnerPeer = &mp.config.Peers[i]
			require.Equal(t, raftProto.PeerLearner, peer.Type)
			break
		}
	}
	require.NotNil(t, learnerPeer, "Learner should exist")

	// Now promote the learner
	req2 := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
		},
		OpType: proto.OpTypePromoteLearner,
	}
	context2, err := json.Marshal(req2)
	require.NoError(t, err)

	confChange2 := &raftProto.ConfChange{
		Type:    raftProto.ConfPromoteLearner,
		Peer:    raftProto.Peer{ID: 2},
		Context: context2,
	}

	resp, err := mp.ApplyMemberChange(confChange2, 11)

	require.NoError(t, err)
	require.Nil(t, resp)

	// Check if learner was promoted to normal
	for _, peer := range mp.config.Peers {
		if peer.ID == 2 {
			require.Equal(t, raftProto.PeerNormal, peer.Type, "Learner should be promoted to normal")
			break
		}
	}

	// Verify persisted metadata - learner should be promoted to normal
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
		{ID: 2, Addr: "127.0.0.1:17211", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_PromoteLearner_NotFound(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	// Try to promote a non-existent learner
	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   999, // Non-existent
			Addr: "127.0.0.1:17211",
		},
		OpType: proto.OpTypePromoteLearner,
	}
	context, err := json.Marshal(req)
	require.NoError(t, err)

	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfPromoteLearner,
		Peer:    raftProto.Peer{ID: 999},
		Context: context,
	}

	resp, err := mp.ApplyMemberChange(confChange, 10)

	require.NoError(t, err)
	require.Nil(t, resp)

	// Verify persisted metadata - should remain unchanged (operation failed)
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_InvalidContext(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	invalidContext := []byte("{invalid json}")
	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfAddNode,
		Peer:    raftProto.Peer{ID: 2},
		Context: invalidContext,
	}

	resp, err := mp.ApplyMemberChange(confChange, 10)

	require.Error(t, err)
	require.Nil(t, resp)

	// Verify persisted metadata - should remain unchanged (operation failed)
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_UnknownType(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
		},
	}
	context, err := json.Marshal(req)
	require.NoError(t, err)

	// Use an unknown ConfChange type (using a value that won't overflow)
	// Note: ConfChangeType is uint8, so we use a value within range but not defined
	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfChangeType(255), // Max uint8 value, unknown type
		Peer:    raftProto.Peer{ID: 2},
		Context: context,
	}

	initialPeerCount := len(mp.config.Peers)
	resp, err := mp.ApplyMemberChange(confChange, 10)

	require.NoError(t, err) // Unknown type is handled gracefully
	require.Nil(t, resp)
	require.Equal(t, initialPeerCount, len(mp.config.Peers))

	// Verify persisted metadata - should remain unchanged
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_ConfUpdateNode(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
		},
	}
	context, err := json.Marshal(req)
	require.NoError(t, err)

	// ConfUpdateNode is not implemented, should do nothing
	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfUpdateNode,
		Peer:    raftProto.Peer{ID: 2},
		Context: context,
	}

	initialPeerCount := len(mp.config.Peers)
	resp, err := mp.ApplyMemberChange(confChange, 10)

	require.NoError(t, err)
	require.Nil(t, resp)
	require.Equal(t, initialPeerCount, len(mp.config.Peers))

	// Verify persisted metadata - should remain unchanged (ConfUpdateNode not implemented)
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_PersistMetadataOnUpdate(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
			Type: raftProto.PeerNormal,
		},
		OpType: proto.OpTypeAddRaftMember,
	}
	context, err := json.Marshal(req)
	require.NoError(t, err)

	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfAddNode,
		Peer:    raftProto.Peer{ID: 2},
		Context: context,
	}

	initialPeerCount := len(mp.config.Peers)
	resp, err := mp.ApplyMemberChange(confChange, 10)

	require.NoError(t, err)
	require.Nil(t, resp)
	// When peer is added (updated=true), peer count should increase
	require.Equal(t, initialPeerCount+1, len(mp.config.Peers), "Peer should be added")

	// Verify persisted metadata
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
		{ID: 2, Addr: "127.0.0.1:17211", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}

func TestApplyMemberChange_UploadApplyID(t *testing.T) {
	mp := createTestMetaPartitionForApplyMember(t)

	req := &proto.AddMetaPartitionRaftMemberRequest{
		PartitionId: 1,
		AddPeer: proto.Peer{
			ID:   2,
			Addr: "127.0.0.1:17211",
			Type: raftProto.PeerNormal,
		},
		OpType: proto.OpTypeAddRaftMember,
	}
	context, err := json.Marshal(req)
	require.NoError(t, err)

	confChange := &raftProto.ConfChange{
		Type:    raftProto.ConfAddNode,
		Peer:    raftProto.Peer{ID: 2},
		Context: context,
	}

	initialApplyID := mp.getApplyID()
	testIndex := uint64(100)

	resp, err := mp.ApplyMemberChange(confChange, testIndex)

	require.NoError(t, err)
	require.Nil(t, resp)
	// ApplyID should be updated to the index
	require.Equal(t, testIndex, mp.getApplyID(), "ApplyID should be updated to the index")
	require.Greater(t, mp.getApplyID(), initialApplyID)

	// Verify persisted metadata
	expectedPeers := []proto.Peer{
		{ID: 1, Addr: "127.0.0.1:17210", Type: raftProto.PeerNormal},
		{ID: 2, Addr: "127.0.0.1:17211", Type: raftProto.PeerNormal},
	}
	verifyPersistedMetadata(t, mp, expectedPeers)
}
