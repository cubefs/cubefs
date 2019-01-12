// Copyright 2018 The TigLabs raft Authors.
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

package test

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage"
)

var (
	testSnap     = true
	storageType  = 0
	elcTick      = 5
	htbTick      = 1
	tickInterval = 100 * time.Millisecond
	resolver     = newNodeManager()

	temp        = "0123456789abcdefghijklmnopqrstuvwxyz"
	format_time = "2006-01-02 15:04:05.000"

	peers = []proto.Peer{proto.Peer{ID: 1}, proto.Peer{ID: 2}, proto.Peer{ID: 3}}
)

func init() {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)
	fmt.Printf("[System], Cpu Num = [%d]\r\n", numCpu)
}

type replAddr struct {
	heart string
	repl  string
}

type nodeManager struct {
	sync.Mutex
	nodes    map[uint64]int
	allAddrs map[uint64]replAddr
}

func newNodeManager() *nodeManager {
	nm := new(nodeManager)
	nm.nodes = map[uint64]int{1: 1, 2: 1, 3: 1}
	nm.allAddrs = map[uint64]replAddr{1: {heart: "127.0.0.1:8000", repl: "127.0.0.1:9000"}, 2: {heart: "127.0.0.1:8001", repl: "127.0.0.1:9001"}, 3: {heart: "127.0.0.1:8002", repl: "127.0.0.1:9002"}, 4: {heart: "127.0.0.1:8003", repl: "127.0.0.1:9003"}}
	return nm
}

func (nm *nodeManager) addNode(nodeId uint64, pri int) {
	nm.Lock()
	defer nm.Unlock()

	nm.nodes[nodeId] = pri
}

func (nm *nodeManager) delNode(nodeId uint64) {
	nm.Lock()
	defer nm.Unlock()

	delete(nm.nodes, nodeId)
}

func (nm *nodeManager) AllNodes() []uint64 {
	nm.Lock()
	defer nm.Unlock()

	nodes := make([]uint64, 0)
	for k, _ := range nm.nodes {
		nodes = append(nodes, k)
	}
	return nodes
}

func (nm *nodeManager) NodeAddress(nodeID uint64, stype raft.SocketType) (string, error) {
	addr := nm.allAddrs[nodeID]
	if stype == raft.HeartBeat {
		return addr.heart, nil
	}
	return addr.repl, nil
}

func randomStr(size int) string {
	rand.Seed(time.Now().UnixNano())
	curr := make([]byte, size)
	for i := 0; i < size; i++ {
		curr[i] = temp[rand.Int()%36]
	}
	return string(curr)
}

type testServer struct {
	isLease   bool
	nodeID    uint64
	sm        *memoryStatemachine
	store     storage.Storage
	raft      *raft.RaftServer
	peers     []proto.Peer
	hardState proto.HardState
}

func initTestServer(peers []proto.Peer, isLease, clear bool) []*testServer {
	rs := make([]*testServer, 0)
	for _, p := range peers {
		rs = append(rs, createRaftServer(p.ID, 0, 0, peers, isLease, clear))
	}
	return rs
}

func createRaftServer(nodeId, leader, term uint64, peers []proto.Peer, isLease, clear bool) *testServer {
	config := raft.DefaultConfig()
	config.NodeID = nodeId
	config.TickInterval = tickInterval
	config.HeartbeatTick = htbTick
	config.ElectionTick = elcTick
	config.LeaseCheck = isLease
	config.HeartbeatAddr = resolver.allAddrs[nodeId].heart
	config.ReplicateAddr = resolver.allAddrs[nodeId].repl
	config.Resolver = resolver
	config.RetainLogs = 0

	rs, err := raft.NewRaftServer(config)
	if err != nil {
		panic(err)
	}

	sm := newMemoryStatemachine(1, rs)
	st := getStorage(rs)
	if clear {
		st.ApplySnapshot(proto.SnapshotMeta{})
	}
	raftConfig := &raft.RaftConfig{
		ID:           1,
		Peers:        peers,
		Term:         term,
		Leader:       leader,
		Storage:      st,
		StateMachine: sm,
	}
	err = rs.CreateRaft(raftConfig)
	if err != nil {
		panic(err)
	}
	return &testServer{
		nodeID:  nodeId,
		peers:   peers,
		isLease: isLease,
		raft:    rs,
		sm:      sm,
		store:   st,
	}
}

func getStorage(raft *raft.RaftServer) storage.Storage {
	switch storageType {
	case 0:
		return storage.NewMemoryStorage(raft, 1, 8192)
	}
	return nil
}

func getTestPath() string {
	path := os.TempDir() + string(filepath.Separator) + "rafttest"
	os.MkdirAll(path, os.ModePerm)
	return path
}

func getLogFile(name string) (*os.File, *bufio.Writer) {
	filename := getTestPath() + string(filepath.Separator) + name
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(f)
	return f, w
}

func getCurrentNanoTime() int64 {
	return time.Now().UnixNano()
}
