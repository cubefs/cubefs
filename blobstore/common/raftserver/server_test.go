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

package raftserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type srvStore struct {
	kv      sync.Map
	applied uint64
}

func (s *srvStore) Put(key, val []byte) error {
	s.kv.Store(string(key), val)
	return nil
}

func (s *srvStore) Get(key []byte) ([]byte, error) {
	val, hit := s.kv.Load(string(key))
	if hit {
		return val.([]byte), nil
	}
	return nil, nil
}

func encode(key []byte, value []byte) []byte {
	data := make([]byte, 8+len(key)+len(value))
	binary.BigEndian.PutUint32(data, uint32(len(key)))
	binary.BigEndian.PutUint32(data[4:], uint32(len(value)))
	copy(data[8:], key)
	copy(data[8+len(key):], value)
	return data
}

func decode(data []byte) ([]byte, []byte) {
	keyLen := binary.BigEndian.Uint32(data)
	valLen := binary.BigEndian.Uint32(data[4:])

	return data[8 : 8+keyLen], data[8+keyLen : 8+keyLen+valLen]
}

type srvSnapshot struct {
	store *srvStore
	name  string
	index uint64
	read  bool
}

func (st *srvSnapshot) Name() string {
	return st.name
}

func (st *srvSnapshot) Index() uint64 {
	return st.index
}

func (st *srvSnapshot) Read() ([]byte, error) {
	if st.read {
		return nil, io.EOF
	}
	var data []byte
	st.store.kv.Range(func(key, value interface{}) bool {
		data = append(data, encode([]byte(key.(string)), value.([]byte))...)
		return true
	})
	if len(data) == 0 {
		return nil, io.EOF
	}
	st.read = true
	return data, nil
}

func (st *srvSnapshot) Close() {
	st.read = true
}

type srvStatemachine struct {
	id            uint64
	store         *srvStore
	leader        uint64
	leaderChangec chan struct{}
	applySnapC    chan struct{}
}

func newSrvStatemachine(id uint64, store *srvStore) *srvStatemachine {
	return &srvStatemachine{
		id:            id,
		store:         store,
		leaderChangec: make(chan struct{}, 1),
		applySnapC:    make(chan struct{}, 1),
	}
}

func (sm *srvStatemachine) Apply(data [][]byte, index uint64) error {
	for _, d := range data {
		key, val := decode(d)
		// log.Infof("[node=%d] Apply key: %s val: %s", sm.id, string(key), string(val))
		if err := sm.store.Put(key, val); err != nil {
			return err
		}
	}
	sm.store.applied = index
	return nil
}

func (sm *srvStatemachine) ApplyMemberChange(cc ConfChange, index uint64) error {
	log.Infof("[node=%d] ApplyMemberChange [NodeID: %d NodeHost: %s Type: %s]", sm.id, cc.NodeID, string(cc.Context), cc.Type.String())
	sm.store.applied = index
	return nil
}

func (sm *srvStatemachine) Snapshot() (Snapshot, error) {
	return &srvSnapshot{
		store: sm.store,
		index: sm.store.applied,
		name:  uuid.New().String(),
	}, nil
}

func (sm *srvStatemachine) ApplySnapshot(meta SnapshotMeta, st Snapshot) error {
	defer func() {
		sm.applySnapC <- struct{}{}
		log.Info("end apply snapshot")
	}()
	log.Info("begin apply snapshot")
	for {
		data, err := st.Read()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		p := data
		for len(p) > 0 {
			keyLen := binary.BigEndian.Uint32(p)
			valLen := binary.BigEndian.Uint32(p[4:])
			sm.store.Put(p[8:8+keyLen], p[8+keyLen:8+keyLen+valLen])
			p = p[8+keyLen+valLen:]
		}
	}
	sm.store.applied = st.Index()
	return nil
}

func (sm *srvStatemachine) LeaderChange(leader uint64, host string) {
	sm.leader = leader
	if leader != 0 {
		select {
		case sm.leaderChangec <- struct{}{}:
		default:
		}
	}
	log.Infof("[node=%d] leader change leader=%d host=%s", sm.id, leader, host)
}

var members = []Member{
	{
		NodeID:  1,
		Host:    "127.0.0.1:9090",
		Learner: false,
	},
	{
		NodeID:  2,
		Host:    "127.0.0.1:9091",
		Learner: false,
	},
	{
		NodeID:  3,
		Host:    "127.0.0.1:9092",
		Learner: false,
	},
}

var cfgs = [3]*Config{
	{
		NodeId:       1,
		ListenPort:   9090,
		WalDir:       "/tmp/raftserver/wal1",
		TickInterval: 1,
		ElectionTick: 3,
		Members:      members,

		TickIntervalMs: 50,
	},
	{
		NodeId:       2,
		ListenPort:   9091,
		WalDir:       "/tmp/raftserver/wal2",
		TickInterval: 1,
		ElectionTick: 3,
		Members:      members,

		TickIntervalMs: 50,
	},
	{
		NodeId:       3,
		ListenPort:   9092,
		WalDir:       "/tmp/raftserver/wal3",
		TickInterval: 1,
		ElectionTick: 3,
		Members:      members,

		TickIntervalMs: 50,
	},
}

func submit(rs RaftServer, ctx context.Context, key string, value []byte) error {
	return rs.Propose(ctx, encode([]byte(key), value))
}

func TestRaftServer(t *testing.T) {
	log.SetOutputLevel(0)
	os.RemoveAll("/tmp/raftserver")
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != http.ErrServerClosed {
			return
		}
	}()
	var stores [3]*srvStore
	var sms [3]*srvStatemachine
	var rss [3]RaftServer
	var err error

	for i := 0; i < 3; i++ {
		stores[i] = &srvStore{}
		sms[i] = newSrvStatemachine(cfgs[i].NodeId, stores[i])
		cfgs[i].SM = sms[i]
		rss[i], err = NewRaftServer(cfgs[i])
		require.Nil(t, err)
	}

	err = rss[0].(*raftServer).campaign(context.TODO())
	require.Nil(t, err)
	var rs *raftServer
	for i := 0; i < 3; i++ {
		<-sms[i].leaderChangec
		if rs == nil {
			rs = rss[i].(*raftServer)
		}
		for {
			// make sure the data has already been loaded
			if err = rss[i].ReadIndex(context.TODO()); err == nil {
				break
			}
		}
	}

	err = submit(rs, context.TODO(), "12345", []byte("dfakliuerooioitirw"))
	require.Nil(t, err)
	var cnt int
	for i := 0; i < 3; i++ {
		err = rss[i].ReadIndex(context.TODO())
		require.Nil(t, err)
		val, _ := stores[i].Get([]byte("12345"))
		if bytes.Equal(val, []byte("dfakliuerooioitirw")) {
			cnt++
		}
	}
	require.GreaterOrEqual(t, cnt, 2)
	for i := 0; i < 3; i++ {
		rss[i].Stop()
	}
	log.Info("stop all raft server")

	// test raft restart and recover data from wal log
	for i := 0; i < 3; i++ {
		stores[i].kv.Delete("12345")
		sms[i].leader = 0
		cfgs[i].Applied = 0
		rss[i], err = NewRaftServer(cfgs[i])
		require.Nil(t, err)
	}

	err = rss[0].(*raftServer).campaign(context.TODO())
	require.Nil(t, err)
	for i := 0; i < 3; i++ {
		<-sms[i].leaderChangec
	}
	cnt = 0
	for i := 0; i < 3; i++ {
		for {
			if err = rss[i].ReadIndex(context.TODO()); err == nil {
				break
			}
		}
		require.Nil(t, err)
		val, _ := stores[i].Get([]byte("12345"))
		if bytes.Equal(val, []byte("dfakliuerooioitirw")) {
			cnt++
		}
	}
	require.GreaterOrEqual(t, cnt, 2)

	// test remove member
	leader := sms[0].leader
	idx := leader - 1
	require.NotZero(t, leader)
	log.Infof("remove node=%d", leader)
	err = rss[idx].RemoveMember(context.TODO(), leader)
	require.Nil(t, err)
	rss[idx].Stop()
	err = submit(rss[idx], context.TODO(), "abcdefg", []byte("hudfaijeriuhuoio"))
	require.NotNil(t, err)

	err = rss[(idx+1)%3].(*raftServer).campaign(context.TODO())
	require.Nil(t, err)
	for i := 0; i < 3; i++ {
		if i == int(idx) {
			continue
		}
		<-sms[i].leaderChangec // wait leader change
	}
	prevIdx := idx
	idx = (idx + 1) % 3
	log.Infof("[node=%d] propose key(%s) val(%s)", cfgs[idx].NodeId, "abcdefg", "hudfaijeriuhuoio")
	err = submit(rss[idx], context.TODO(), "abcdefg", []byte("hudfaijeriuhuoio"))
	require.Nil(t, err)
	for i := 0; i < 3; i++ {
		if i != int(prevIdx) {
			err = rss[i].ReadIndex(context.TODO())
			require.Nil(t, err)
			val, _ := stores[i].Get([]byte("abcdefg"))
			require.Equal(t, "hudfaijeriuhuoio", string(val))
		} else {
			val, _ := stores[i].Get([]byte("abcdefg"))
			require.Nil(t, val)
		}
	}

	// test add member
	os.RemoveAll(cfgs[prevIdx].WalDir)   // clear raft wal
	stores[prevIdx].kv.Delete("abcdefg") // clear data in kv storage
	mbs := cfgs[prevIdx].Members
	mbs[prevIdx].Host = "127.0.0.1:9096"
	require.Equal(t, 3, len(mbs))
	cfgs[prevIdx].ListenPort = 9096
	rss[prevIdx], err = NewRaftServer(cfgs[prevIdx])
	require.Nil(t, err)

	m := cfgs[int(prevIdx)].Members[prevIdx]
	log.Infof("add member %+v", m)
	err = rss[idx].AddMember(context.TODO(), m)
	require.Nil(t, err)
	<-sms[prevIdx].leaderChangec
	for {
		if err = rss[prevIdx].ReadIndex(context.TODO()); err == nil {
			break
		}
	}
	require.Nil(t, err)
	val, _ := stores[prevIdx].Get([]byte("abcdefg"))
	require.True(t, bytes.Equal(val, []byte("hudfaijeriuhuoio")))

	// test transfer leadership and status
	leader = sms[0].leader
	var transferee uint64
	for i := 0; i < 3; i++ {
		if i+1 == int(leader) {
			require.True(t, rss[i].IsLeader())
		} else {
			transferee = uint64(i + 1)
			require.False(t, rss[i].IsLeader())
		}
	}
	status := rss[leader-1].Status()
	require.NotZero(t, len(status.Peers))
	log.Infof("transfer leader from %d to %d", leader, transferee)
	rss[leader-1].TransferLeadership(context.TODO(), leader, transferee)
	for i := 0; i < 3; i++ {
		<-sms[i].leaderChangec
		require.Equal(t, int(transferee), int(sms[i].leader))
	}

	// test add learner
	learnerCfg := Config{
		NodeId:       4,
		ListenPort:   9093,
		WalDir:       "/tmp/raftserver/wal4",
		TickInterval: 1,
		ElectionTick: 3,
		Members:      members,
	}
	learnerCfg.Members = append(learnerCfg.Members, Member{
		NodeID:  4,
		Host:    "127.0.0.1:9093",
		Learner: true,
	})
	learnerStore := &srvStore{}
	learnerSM := newSrvStatemachine(learnerCfg.NodeId, learnerStore)
	learnerCfg.SM = learnerSM
	log.Infof("add learner %d=%s", 4, "127.0.0.1:9093")
	err = rss[0].AddMember(context.TODO(), learnerCfg.Members[3])
	require.Nil(t, err)
	learnerRaft, err := NewRaftServer(&learnerCfg)
	require.Nil(t, err)
	<-learnerSM.leaderChangec
	for {
		if err = learnerRaft.ReadIndex(context.TODO()); err == nil {
			break
		}
	}
	require.Equal(t, int(sms[0].leader), int(learnerSM.leader))
	status = rss[int(learnerSM.leader)-1].Status()
	require.Equal(t, 4, len(status.Peers))
	for _, p := range status.Peers {
		if p.Id == 4 {
			require.True(t, p.IsLearner)
		}
	}

	// change learner to member
	log.Infof("change learner %d=%s to member", 4, "127.0.0.1:9093")
	err = rss[0].AddMember(context.TODO(), Member{
		NodeID: 4,
		Host:   "127.0.0.1:9093",
	})
	require.Nil(t, err)
	status = rss[int(learnerSM.leader)-1].Status()
	require.Equal(t, 4, len(status.Peers))
	require.False(t, status.Peers[3].IsLearner)

	for i := 0; i < 3; i++ {
		rss[i].Stop()
	}
	learnerRaft.Stop()
}

func TestRaftServerSnapshot(t *testing.T) {
	os.RemoveAll("/tmp/raftserver")
	var stores [3]*srvStore
	var sms [3]*srvStatemachine
	var rss [3]RaftServer
	var err error

	for i := 0; i < 2; i++ {
		stores[i] = &srvStore{}
		sms[i] = newSrvStatemachine(cfgs[i].NodeId, stores[i])
		cfgs[i].SM = sms[i]
		rss[i], err = NewRaftServer(cfgs[i])
		require.Nil(t, err)
	}

	rss[0].(*raftServer).campaign(context.TODO())

	alphas := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	randBytes := func(n int) []byte {
		data := make([]byte, n)
		for i := 0; i < n; i++ {
			data[i] = alphas[rand.Intn(len(alphas))]
		}
		return data
	}
	<-sms[0].leaderChangec
	var wg sync.WaitGroup
	log.Info("propose begin")
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				key := randBytes(16)
				val := randBytes(64)
				rss[0].Propose(context.TODO(), encode(key, val))
			}
		}()
	}
	wg.Wait()
	log.Info("propose finish")
	for i := 0; i < 2; i++ {
		err = rss[i].Truncate(stores[i].applied)
		require.Nil(t, err)
	}
	stores[2] = &srvStore{}
	sms[2] = newSrvStatemachine(cfgs[2].NodeId, stores[2])
	cfgs[2].SM = sms[2]
	rss[2], err = NewRaftServer(cfgs[2])
	require.Nil(t, err)
	log.Infof("[node %d] wait apply snapshot ......", cfgs[2].NodeId)
	<-sms[2].applySnapC
	log.Infof("[node %d] apply snapshot success", cfgs[2].NodeId)
	err = rss[2].ReadIndex(context.TODO())
	require.Nil(t, err)
	require.Equal(t, stores[0].applied, stores[2].applied)
	log.Infof("[node %d] read index success, start stop raft......", cfgs[2].NodeId)
	for i := 0; i < 3; i++ {
		rss[i].Stop()
	}
}
