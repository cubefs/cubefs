// Copyright 2022 The CubeFS Authors.
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

package raft

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"

	"github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var (
	testRaftCF         = kvstore.CF("test-raft")
	testStateMachineCF = kvstore.CF("test-sm")

	testNodes = []Member{
		{NodeID: 1, Host: "127.0.0.1"},
		{NodeID: 2, Host: "127.0.0.1"},
		{NodeID: 3, Host: "127.0.0.1"},
		{NodeID: 4, Host: "127.0.0.1", Learner: true},
		{NodeID: 5, Host: "127.0.0.1", Learner: true},
	}
)

func init() {
	ports := make(map[int]bool)

	for i := range testNodes {
	RETRY:
		port := util.GenUnusedPort()
		if ports[port] {
			goto RETRY
		}

		testNodes[i].Host += ":" + strconv.Itoa(port)
		ports[port] = true
	}
}

func TestManager_CreateGroup(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestManager_CreateGroup")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storagePath, _ := util.GenTmpPath()
	m, done := initManager(t, ctrl, testNodes[0], storagePath)
	require.NotNil(t, m)
	defer done()

	storage := &testStorage{
		cf:      testStateMachineCF,
		kvStore: m.cfg.Storage.(*testStorage).kvStore,
	}
	sm := newTestStateMachine(storage)

	groupConfig := &GroupConfig{
		ID:      1,
		Applied: 0,
		Members: testNodes[:1],
		SM:      sm,
	}
	_, err := m.CreateRaftGroup(ctx, groupConfig)
	require.NoError(t, err)

	defer m.RemoveRaftGroup(ctx, groupConfig.ID, true)

	group, err := m.GetRaftGroup(groupConfig.ID)
	require.NoError(t, err)
	require.NotNil(t, group)
}

func TestManager_GroupInOneServer(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestManager_GroupInOneServer")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storagePath, _ := util.GenTmpPath()
	m, done := initManager(t, ctrl, testNodes[0], storagePath)
	require.NotNil(t, m)
	defer done()

	storage := &testStorage{
		cf:      testStateMachineCF,
		kvStore: m.cfg.Storage.(*testStorage).kvStore,
	}
	sm := newTestStateMachine(storage)

	groupConfig := &GroupConfig{
		ID:      1,
		Applied: 0,
		Members: testNodes[:1],
		SM:      sm,
	}
	t.Log(groupConfig.Members)
	_, err := m.CreateRaftGroup(ctx, groupConfig)
	require.NoError(t, err)

	defer m.RemoveRaftGroup(ctx, groupConfig.ID, true)

	rawGroup, err := m.GetRaftGroup(groupConfig.ID)
	require.NoError(t, err)
	group := rawGroup.(*group)

	err = group.Campaign(ctx)
	require.NoError(t, err)

	sm.WaitLeaderChange()
	err = group.ReadIndex(ctx)
	require.NoError(t, err)

	kvs := []*testKV{
		{key: "k1", value: "v1"},
		{key: "k2", value: "v2"},
		{key: "k3", value: "v3"},
	}
	// test Propose
	{
		for i := range kvs {
			resp, err := group.Propose(ctx, &ProposalData{
				Module: nil,
				Op:     0,
				Data:   kvs[i].Marshal(),
			})
			require.NoError(t, err)
			require.Equal(t, kvs[i].key, resp.Data.(string))
		}
	}
	// test truncate
	{
		stat, err := group.Stat()
		require.NoError(t, err)
		require.Equal(t, uint64(4), stat.Applied)
		require.Equal(t, uint64(4), stat.Commit)

		err = group.Truncate(ctx, 2)
		require.NoError(t, err)
		firstIndex, err := group.storage.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(2), firstIndex)
	}
}

func TestManager_GroupInMultiServer(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestManager_GroupInOneServer")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Logf("test nodes: %+v", testNodes)
	allNodes := testNodes[:4]

	// initial multi state machine and manager
	managers := make([]*manager, len(allNodes))
	closeManagers := make([]func(), len(allNodes))
	sms := make([]*testStateMachine, len(allNodes))
	groups := make([]*group, len(allNodes))

	for i := 0; i < len(allNodes); i++ {
		storagePath, err := util.GenTmpPath()
		require.NoError(t, err)

		m, done := initManager(t, ctrl, allNodes[i], storagePath)
		require.NotNil(t, m)
		managers[i] = m
		closeManagers[i] = done

		storage := &testStorage{
			cf:      testStateMachineCF,
			kvStore: m.cfg.Storage.(*testStorage).kvStore,
		}
		sms[i] = newTestStateMachine(storage)
	}

	defer func() {
		for i := range closeManagers {
			closeManagers[i]()
		}
	}()

	groupConfig := &GroupConfig{
		ID:      1,
		Applied: 0,
		Members: allNodes,
	}

	for i, m := range managers {
		groupConfig.SM = sms[i]
		_, err := m.CreateRaftGroup(ctx, groupConfig)
		require.NoError(t, err)

		rawGroup, err := m.GetRaftGroup(groupConfig.ID)
		require.NoError(t, err)
		group := rawGroup.(*group)

		if i == 0 {
			err = group.Campaign(ctx)
			require.NoError(t, err)
		}

		groups[i] = group
	}

	t.Log("start to wait for leader change")

	// wait for all node receive leader change and read index done
	for _, sm := range sms {
		sm.WaitLeaderChange()
	}
	t.Log("all leader change done")

	defer func() {
		for _, m := range managers {
			m.RemoveRaftGroup(ctx, groupConfig.ID, true)
		}
	}()

	// get leader index
	stat, _ := groups[0].Stat()
	t.Logf("stat leader: %d", stat.Leader)
	leaderIndex := int(stat.Leader - 1)
	followerIndex := 0
	for i := range managers {
		if i != leaderIndex {
			followerIndex = i
			break
		}
	}

	kvs := []*testKV{
		{key: "k1", value: "v1"},
		{key: "k2", value: "v2"},
		{key: "k3", value: "v3"},
		{key: "k4", value: "v4"},
		{key: "k5", value: "v5"},
		{key: "k6", value: "v6"},
	}

	// test leader and follower Propose
	{
		for index := range []int{leaderIndex, followerIndex} {
			for i := range kvs {
				resp, err := groups[index].Propose(ctx, &ProposalData{
					Module: nil,
					Op:     0,
					Data:   kvs[i].Marshal(),
				})
				require.NoError(t, err)
				require.Equal(t, kvs[i].key, resp.Data.(string))
			}
		}
	}

	// test leader and follower ReadIndex
	{
		t.Log("start to test ReadIndex")
		for index := range []int{leaderIndex, followerIndex} {
			err := groups[index].ReadIndex(ctx)
			require.NoError(t, err)
		}
	}
	// test MemberChange
	{
		t.Log("start to test MemberChange")
		err := groups[leaderIndex].MemberChange(ctx, &allNodes[followerIndex])
		require.NoError(t, err)

		removeNode := allNodes[followerIndex]
		removeNode.Type = MemberChangeType_RemoveMember
		err = groups[leaderIndex].MemberChange(ctx, &removeNode)
		require.NoError(t, err)

		stat, _ := groups[leaderIndex].Stat()
		require.Equal(t, len(allNodes)-1, len(stat.Peers))

		err = groups[leaderIndex].MemberChange(ctx, &allNodes[followerIndex])
		require.NoError(t, err)

	}
	// test snapshot, add new node into group
	{
		t.Log("start to test snapshot")
		// truncate leader raft log
		err := groups[leaderIndex].Truncate(ctx, 3)
		require.NoError(t, err)
		_, err = groups[leaderIndex].storage.Term(2)
		require.ErrorIs(t, raft.ErrCompacted, err)
		term, err := groups[leaderIndex].storage.Term(3)
		require.NoError(t, err)
		require.Equal(t, uint64(1), term)

		// create new node
		storagePath, err := util.GenTmpPath()
		require.NoError(t, err)
		newNode := testNodes[4]
		m, done := initManager(t, ctrl, newNode, storagePath)
		require.NotNil(t, m)
		defer done()

		storage := &testStorage{
			cf:      testStateMachineCF,
			kvStore: m.cfg.Storage.(*testStorage).kvStore,
		}
		sm := newTestStateMachine(storage)
		groupConfig.Members = testNodes
		groupConfig.SM = sm
		_, err = m.CreateRaftGroup(ctx, groupConfig)
		require.NoError(t, err)

		err = groups[leaderIndex].MemberChange(ctx, &newNode)
		require.NoError(t, err)
		// wait for apply snapshot
		time.Sleep(1 * time.Second)
	}

	// test LeaderTransfer
	{
		t.Log("start to test LeaderTransfer")
		// reset wait leader change channel and once
		for _, sm := range sms {
			sm.once = sync.Once{}
			sm.waitLeaderCh = make(chan struct{})
		}
		err := groups[leaderIndex].LeaderTransfer(ctx, groups[followerIndex].cfg.nodeID)
		require.NoError(t, err)

		for _, sm := range sms {
			sm.WaitLeaderChange()
		}
		stat, _ := groups[leaderIndex].Stat()
		require.Equal(t, groups[followerIndex].cfg.nodeID, stat.Leader)
	}

	time.Sleep(1 * time.Second)
}

func initManager(t *testing.T, ctrl *gomock.Controller, member Member, storagePath string) (*manager, func()) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	mockResolver := NewMockAddressResolver(ctrl)
	mockResolver.EXPECT().Resolve(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, nodeID uint64) (Addr, error) {
		span.Infof("get node id[%d] address", nodeID)
		mockAddr := NewMockAddr(ctrl)
		for i := range testNodes {
			if testNodes[i].NodeID == nodeID {
				mockAddr.EXPECT().String().AnyTimes().Return(testNodes[i].Host)
			}
		}
		return mockAddr, nil
	})

	kvStore, err := kvstore.NewKVStore(ctx, storagePath, kvstore.RocksdbLsmKVType, &kvstore.Option{
		CreateIfMissing: true,
		ColumnFamily:    []kvstore.CF{testRaftCF, testStateMachineCF},
	})
	require.NoError(t, err)
	storage := &testStorage{kvStore: kvStore, cf: testRaftCF}

	m, err := NewManager(&Config{
		NodeID:               member.NodeID,
		TickIntervalMS:       200,
		HeartbeatTick:        5,
		ElectionTick:         20,
		MaxWorkerNum:         6,
		MaxSnapshotWorkerNum: 2,
		TransportConfig: TransportConfig{
			Addr: member.Host,
		},
		Logger:   log.DefaultLogger,
		Storage:  storage,
		Resolver: mockResolver,
	})

	require.NoError(t, err)
	require.NotNil(t, m)

	return m.(*manager), func() {
		m.Close()
		os.RemoveAll(storagePath)
	}
}
