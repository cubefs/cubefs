// Copyright 2018 The tiglabs raft Authors.
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

package main

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage"
	"github.com/tiglabs/raft/storage/wal"
	"github.com/tiglabs/raft/util/log"
)

type storeType uint8

const (
	memoryStore storeType = iota
	singleStore
	separateStore
	specifyStore
)

type RaftMode int

const (
	StandardMode RaftMode = iota
	StrictMode
	MixMode
)

var (
	testSnap     = true
	storageType  = 1
	elcTick      = 5
	htbTick      = 1
	tickInterval = 100 * time.Millisecond
	logLevel     = "debug"
	walDir       = ""
	diskNum      = 5
	dataType     = 0
	resolver     = newNodeManager()

	temp        = "0123456789abcdefghijklmnopqrstuvwxyz"
	format_time = "2006-01-02 15:04:05.000"

	peers      = []proto.Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	subTimeMap map[uint64]*subTime

	outputToStdout bool
)

func init() {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)
	initRaftLog(getTestPath())
	output("[System], Cpu Num = [%d], Test Path = [%v]\r", numCpu, getTestPath())
	subTimeMap = make(map[uint64]*subTime)

	outputToStdout = os.Getenv("DEBUG") == "1"
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

func initNodeManager() *nodeManager {
	nm := new(nodeManager)
	nm.nodes = make(map[uint64]int)
	nm.allAddrs = make(map[uint64]replAddr)
	return nm
}

func newNodeManager() *nodeManager {
	nm := new(nodeManager)
	nm.nodes = map[uint64]int{1: 1, 2: 1, 3: 1}
	nm.allAddrs = map[uint64]replAddr{
		1: {heart: "127.0.0.1:8000", repl: "127.0.0.1:9000"},
		2: {heart: "127.0.0.1:8001", repl: "127.0.0.1:9001"},
		3: {heart: "127.0.0.1:8002", repl: "127.0.0.1:9002"},
		4: {heart: "127.0.0.1:8003", repl: "127.0.0.1:9003"},
		5: {heart: "127.0.0.1:8004", repl: "127.0.0.1:9004"},
	}
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
	for k := range nm.nodes {
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

func (nm *nodeManager) addNodeAddr(p peer, replicaPort string, heartPort string) {
	nm.nodes[p.ID] = 1
	nm.allAddrs[p.ID] = replAddr{
		heart: p.Addr + ":" + replicaPort,
		repl:  p.Addr + ":" + heartPort,
	}
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
	sm        map[uint64]*memoryStatemachine
	store     map[uint64]storage.Storage
	raft      *raft.RaftServer
	peers     []proto.Peer
	hardState proto.HardState
	conf      *raftServerConfig
	mode      raft.ConsistencyMode
}

func initTestServer(peers []proto.Peer, isLease, clear bool, groupNum int, testMode RaftMode) []*testServer {
	rs := make([]*testServer, 0)
	for _, p := range peers {
		if clear {
			os.RemoveAll(path.Join(getTestPath(), strconv.FormatUint(p.ID, 10)))
		}
		mode := getConsistencyMode(testMode, p.ID)
		raftConfig := &raft.RaftConfig{Peers: peers, Leader: 0, Term: 0, Mode: mode}
		rs = append(rs, createRaftServer(p.ID, isLease, clear, groupNum, raftConfig))
	}
	return rs
}

func getConsistencyMode(testMode RaftMode, peerID uint64) raft.ConsistencyMode {
	switch testMode {
	case StandardMode:
		return raft.DefaultMode
	case StrictMode:
		return raft.StrictMode
	case MixMode:
		if peerID == 1 {
			return raft.StrictMode
		} else {
			return raft.DefaultMode
		}
	}
	return raft.DefaultMode
}

func initTestServerWithMsgFilter(peers []proto.Peer, isLease, clear bool, groupNum int, mode raft.ConsistencyMode, msgFilter raft.MsgFilterFunc) []*testServer {
	rs := make([]*testServer, 0)
	for _, p := range peers {
		if clear {
			os.RemoveAll(path.Join(getTestPath(), strconv.FormatUint(p.ID, 10)))
		}
		raftConfig := &raft.RaftConfig{Peers: peers, Leader: 0, Term: 0, Mode: mode, MsgFilter: msgFilter}
		rs = append(rs, createRaftServer(p.ID, isLease, clear, groupNum, raftConfig))
	}
	return rs
}

func createRaftServer(nodeId uint64, isLease, clear bool, groupNum int, raftConfig *raft.RaftConfig) *testServer {
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

	smMap := make(map[uint64]*memoryStatemachine)
	stMap := make(map[uint64]storage.Storage)
	for i := 1; i <= groupNum; i++ {
		sm := newMemoryStatemachine(uint64(i), rs)
		st := getMemoryStorage(rs, nodeId, uint64(i))
		if clear {
			st.ApplySnapshot(proto.SnapshotMeta{})
		}
		rc := &raft.RaftConfig{
			ID:           uint64(i),
			Peers:        raftConfig.Peers,
			Term:         raftConfig.Term,
			Leader:       raftConfig.Leader,
			Storage:      st,
			StateMachine: sm,
			Mode:         raftConfig.Mode,
			MsgFilter:    raftConfig.MsgFilter,
		}
		err = rs.CreateRaft(rc)
		if err != nil {
			panic(err)
		}
		smMap[uint64(i)] = sm
		stMap[uint64(i)] = st
		subTimeMap[uint64(i)] = &subTime{minSubTime: math.MaxInt64, maxSubTime: 0, totalSubTime: 0, subCount: 0}
	}
	return &testServer{
		nodeID:  nodeId,
		peers:   raftConfig.Peers,
		isLease: isLease,
		raft:    rs,
		sm:      smMap,
		store:   stMap,
		mode:    raftConfig.Mode,
	}
}

func getMemoryStorage(raft *raft.RaftServer, nodeId, raftId uint64) storage.Storage {
	switch storageType {
	case 0:
		return storage.NewMemoryStorage(raft, raftId, 200000)
	case 1:
		return getStorage(nodeId, raftId)
	case 2:
		return getSeparateStorage(nodeId, raftId)
	case 3:
		return getSpecifyStorage(nodeId, raftId)
	}
	return nil
}

func getSpecifyStorage(nodeId uint64, raftId uint64) storage.Storage {
	walPath := path.Join("/data"+strconv.FormatUint(nodeId, 10), "rafttest", strconv.FormatUint(nodeId, 10), strconv.FormatUint(raftId, 10))
	os.RemoveAll(walPath)
	os.MkdirAll(walPath, 0777)
	output(fmt.Sprintf("raft: %v, walPath: %v", raftId, walPath))
	wc := &wal.Config{}
	st, err := wal.NewStorage(walPath, wc)
	if err != nil {
		panic(err)
	}
	return st
}

func getStorage(nodeId, raftId uint64) storage.Storage {
	walPath := path.Join(getTestPath(), strconv.FormatUint(nodeId, 10), strconv.FormatUint(raftId, 10))
	wc := &wal.Config{}
	st, err := wal.NewStorage(walPath, wc)
	if err != nil {
		panic(err)
	}
	return st
}

func getSeparateStorage(nodeId, raftId uint64) storage.Storage {
	diskIndex := strconv.FormatUint(raftId%uint64(diskNum), 10)
	walPath := path.Join("/data"+diskIndex, "rafttest", strconv.FormatUint(nodeId, 10), strconv.FormatUint(raftId, 10))
	output(fmt.Sprintf("raft: %v, walPath: %v", raftId, walPath))
	wc := &wal.Config{}
	st, err := wal.NewStorage(walPath, wc)
	if err != nil {
		panic(err)
	}
	return st
}

func getTestPath() (path string) {
	if walDir == "" {
		path = os.TempDir() + string(filepath.Separator) + "rafttest"
	} else {
		path = walDir
	}
	return
}

func getLogFile(dir, name string) (*os.File, *bufio.Writer) {
	if dir == "" {
		dir = getTestPath()
	}
	filename := path.Join(dir, name)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(f)
	return f, w
}

func getCurrentNanoTime() int64 {
	return time.Now().UnixNano()
}

func initRaftLog(logDir string) {
	raftLogPath := path.Join("/tmp/rafttest/", "logs")
	os.RemoveAll(raftLogPath)
	_, err := os.Stat(raftLogPath)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			if os.IsNotExist(pathErr) {
				os.MkdirAll(raftLogPath, 0755)
			}
		}
	}

	raftLog, err := log.NewLog(raftLogPath, "raft", logLevel)
	if err != nil {
		panic(err)
		return
	}
	logger.SetLogger(raftLog)
	return
}

func output(format string, a ...interface{}) {
	if outputToStdout {
		const timeFmt = "2006-01-02 15:04:05.000000"
		var pc, file, line, _ = runtime.Caller(1)
		var funcName = runtime.FuncForPC(pc).Name()
		var prefix = fmt.Sprintf("%v: %v:%v: %v: ", time.Now().Format(timeFmt), path.Base(file), line, path.Base(funcName))
		fmt.Printf(prefix+format+"\n", a...)
	}
}
