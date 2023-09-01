// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"encoding/binary"
	"fmt"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/storage/wal"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type serverStorage struct {
	kv      sync.Map
	applied uint64
}

func (s *serverStorage) Put(key, val []byte) error {
	s.kv.Store(string(key), val)
	return nil
}

func (s *serverStorage) Get(key string) ([]byte, error) {
	val, hit := s.kv.Load(key)
	if hit {
		return val.([]byte), nil
	}
	return nil, nil
}

type testStateMachine struct {
	store         *serverStorage
	id            uint64
	leader        uint64
	changeLeaderC chan struct{}
	applySnapC    chan struct{}
	applyC        chan struct{}
}

func newTestStateMachine(id uint64, store *serverStorage) *testStateMachine {
	return &testStateMachine{
		id:            id,
		store:         store,
		changeLeaderC: make(chan struct{}, 1),
		applySnapC:    make(chan struct{}, 1),
		applyC:        make(chan struct{}, 1),
	}
}

func (sm *testStateMachine) Apply(command []byte, index uint64) (interface{}, error) {

	key, val := decode(command)
	if err := sm.store.Put(key, val); err != nil {
		return nil, err
	}

	sm.store.applied = index
	sm.applyC <- struct{}{}

	return nil, nil
}

func (sm *testStateMachine) ApplyMemberChange(cc *proto.ConfChange, index uint64) (interface{}, error) {
	log.LogInfof("[node=%d] ApplyMemberChange [context: %s Type: %s]", sm.id, string(cc.Context), cc.Type.String())

	sm.store.applied = index
	return nil, nil
}

func (sm *testStateMachine) Snapshot() (proto.Snapshot, error) {
	return nil, nil
}

func (sm *testStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {

	return nil
}

func (sm *testStateMachine) HandleLeaderChange(leader uint64) {
	sm.leader = leader
	if leader != 0 {
		select {
		case sm.changeLeaderC <- struct{}{}:

		default:
		}
	}
}

func (sm *testStateMachine) HandleFatalEvent(err *FatalError) {

}

type testResolver struct {
	nodeMap sync.Map
}

type testNodeAddress struct {
	Heartbeat string
	Replicate string
}

func newTestResolver() *testResolver {
	return &testResolver{}
}

func (r *testResolver) NodeAddress(nodeID uint64, stype SocketType) (addr string, err error) {
	val, ok := r.nodeMap.Load(nodeID)
	if !ok {
		return
	}
	address, ok := val.(*testNodeAddress)
	if !ok {
		return
	}
	switch stype {
	case HeartBeat:
		addr = address.Heartbeat
	case Replicate:
		addr = address.Replicate
	default:

	}
	return
}

func (r *testResolver) AddNodeWithPort(nodeID uint64, addr string, heartbeat int, replicate int) {
	if heartbeat == 0 {
		heartbeat = 5901
	}
	if replicate == 0 {
		replicate = 5902
	}
	if len(strings.TrimSpace(addr)) != 0 {
		r.nodeMap.Store(nodeID, &testNodeAddress{
			Heartbeat: fmt.Sprintf("%s:%d", addr, heartbeat),
			Replicate: fmt.Sprintf("%s:%d", addr, replicate),
		})
	}
}

const (
	raftId1 = iota + 11
	raftId2
	raftId3
)

var cleanWG sync.WaitGroup

var peers = [3][]proto.Peer{
	{{ID: 1, PeerID: raftId1}, {ID: 2, PeerID: raftId1}, {ID: 3, PeerID: raftId1}},
	{{ID: 1, PeerID: raftId2}, {ID: 2, PeerID: raftId2}, {ID: 3, PeerID: raftId2}},
	{{ID: 1, PeerID: raftId3}, {ID: 2, PeerID: raftId3}, {ID: 3, PeerID: raftId3}},
}

var walPaths = map[string]struct{}{}

var rCfgs = [3][3]*RaftConfig{
	{
		{ID: raftId1, Term: 0, Applied: 0, Peers: peers[0]},
		{ID: raftId2, Term: 0, Applied: 0, Peers: peers[1]},
		{ID: raftId3, Term: 0, Applied: 0, Peers: peers[2]},
	},
	{
		{ID: raftId1, Term: 0, Applied: 0, Peers: peers[0]},
		{ID: raftId2, Term: 0, Applied: 0, Peers: peers[1]},
		{ID: raftId3, Term: 0, Applied: 0, Peers: peers[2]},
	},
	{
		{ID: raftId1, Term: 0, Applied: 0, Peers: peers[0]},
		{ID: raftId2, Term: 0, Applied: 0, Peers: peers[1]},
		{ID: raftId3, Term: 0, Applied: 0, Peers: peers[2]},
	},
}

var cfgs = [3]*Config{
	{NodeID: 1, TransportConfig: TransportConfig{HeartbeatAddr: ":9026", ReplicateAddr: ":9995"}},
	{NodeID: 2, TransportConfig: TransportConfig{HeartbeatAddr: ":9027", ReplicateAddr: ":9996"}},
	{NodeID: 3, TransportConfig: TransportConfig{HeartbeatAddr: ":9028", ReplicateAddr: ":9997"}},
}

func randID() string {
	return fmt.Sprintf("%010d", rand.Intn(100000000))
}

func cleanTestServer(servers [3]*RaftServer) {
	for i := range servers {
		servers[i].Stop()
	}
	for p := range walPaths {
		os.RemoveAll(p)
	}

}

func initConfig() {
	wc := &wal.Config{}
	for j := range rCfgs {
		nodePath := "/tmp/raft-" + fmt.Sprintf("%d/", j+1)
		walPaths[nodePath] = struct{}{}
		for _, rcf := range rCfgs[j] {
			path := nodePath + fmt.Sprintf("%d-", rcf.ID) + randID()
			rcf.Storage, _ = wal.NewStorage(path, wc)
		}
	}

	resolver := newTestResolver()
	for i := 0; i < len(cfgs); i++ {
		cfgs[i].Resolver = resolver
		hPorts := strings.Split(cfgs[i].HeartbeatAddr, ":")
		h, _ := strconv.Atoi(hPorts[1])
		rPorts := strings.Split(cfgs[i].ReplicateAddr, ":")
		r, _ := strconv.Atoi(rPorts[1])
		resolver.AddNodeWithPort(cfgs[i].NodeID, "127.0.0.1", h, r)
	}
}

func newTestRaftServer() ([3]*RaftServer, func()) {
	initConfig()

	var (
		servers [3]*RaftServer
		sms     [3][3]*testStateMachine
	)

	// new raft server
	for i := 0; i < len(rCfgs); i++ {
		servers[i], _ = NewRaftServer(cfgs[i])
		for j := 0; j < len(rCfgs); j++ {
			sms[i][j] = newTestStateMachine(rCfgs[i][j].ID, &serverStorage{})
			rCfgs[i][j].StateMachine = sms[i][j]
			servers[i].CreateRaft(rCfgs[i][j])
		}
	}
	cleanWG.Add(1)

	return servers, func() {
		go func() {
			cleanTestServer(servers)
			cleanWG.Done()
		}()
		cleanWG.Wait()
	}
}

func TestRaftServer(t *testing.T) {
	servers, clean := newTestRaftServer()
	defer clean()

	// campaign and change leader
	for _, raft := range servers[0].rafts {
		raft.raftFsm.campaign(true, campaignPreElection)
	}

	for i := range servers {
		for _, raft := range servers[i].rafts {
			// wait leader change finished
			sm := raft.raftConfig.StateMachine.(*testStateMachine)
			<-sm.changeLeaderC
		}
	}

	// is leader
	for i := range servers[0].rafts {
		isLeader := servers[0].IsLeader(i)
		require.True(t, isLeader)
	}

	//submit some data
	k1 := "key1"
	v1 := "some data"
	servers[0].Submit(raftId1, encode([]byte(k1), []byte(v1)))
	sm1 := servers[0].rafts[raftId1].raftConfig.StateMachine.(*testStateMachine)
	<-sm1.applyC
	val, _ := sm1.store.Get(k1)
	require.Equal(t, val, []byte(v1))

	// get committed and applied
	committed := servers[0].CommittedIndex(raftId1)
	require.Equal(t, committed, uint64(2))
	applied := servers[0].AppliedIndex(raftId1)
	require.Equal(t, applied, uint64(2))

	// remove raft
	{
		err := servers[0].RemoveRaft(raftId1)
		require.NoError(t, err)

		// remove raft will not exist
		k2 := "key2"
		v2 := "value data2"
		future := servers[0].Submit(raftId1, encode([]byte(k2), []byte(v2)))
		_, err = future.Response()
		require.Equal(t, err, ErrRaftNotExists)

		t.Logf("----------------test -------------------")
		// node1's raftId1 campaign
		servers[1].rafts[raftId1].raftFsm.campaign(true, campaignElection)

		//for i := 0; i < len(servers); i++ {
		//	if i == 0 {
		//		continue
		//	}
		//	raft := servers[i].rafts[raftId1]
		//	// wait leader change finished
		//	sm := raft.raftConfig.StateMachine.(*testStateMachine)
		//	<-sm.changeLeaderC
		//}
		//
		//require.True(t, servers[1].IsLeader(raftId1))
		//future = servers[1].Submit(raftId1, encode([]byte(k2), []byte(v2)))
		//_, err = future.Response()
		//require.NoError(t, err)

	}

}

func TestSnapshot(t *testing.T) {
	//servers, clean := newTestRaftServer()
	//defer clean()

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
