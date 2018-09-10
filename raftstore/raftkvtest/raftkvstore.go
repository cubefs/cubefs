// Copyright 2018 The Containerfs Authors.
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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/juju/errors"
	. "github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/gorocksdb"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	raftproto "github.com/tiglabs/raft/proto"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

const (
	RaftApplyId = "RaftApplyId"
)

type RaftKvData struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

type RaftKvFsm struct {
	store               *RocksDBStore
	applied             uint64
	leaderChangeHandler RaftLeaderChangeHandler
	peerChangeHandler   RaftPeerChangeHandler
	applyHandler        RaftKvApplyHandler
}

type RaftLeaderChangeHandler func(leader uint64)
type RaftPeerChangeHandler func(confChange *proto.ConfChange) (err error)
type RaftKvApplyHandler func(cmd *RaftKvData) (err error)

//need application to implement -- begin
type MasterConfig struct {
	peers     []PeerAddress
	peerAddrs []string
}

type Master struct {
	nodeId        uint64
	groupId       uint64
	ip            string
	heartbeatPort int
	replicatePort int
	walDir        string
	storeDir      string
	config        *MasterConfig
	raftStore     RaftStore
	fsm           *RaftKvFsm
	raftPartition map[uint64]Partition
	wg            sync.WaitGroup
}

func (m *Master) handleLeaderChange(leader uint64) {
	fmt.Println("leader change leader ", leader)
	return
}

func (m *Master) handlePeerChange(confChange *proto.ConfChange) (err error) {
	addr := string(confChange.Context)
	switch confChange.Type {
	case proto.ConfAddNode:
		var arr []string
		if arr = strings.Split(addr, ":"); len(arr) < 2 {
			fmt.Println(fmt.Sprintf("action[handlePeerChange] nodeAddr[%v] is invalid", addr))
			break
		}
		m.raftStore.AddNodeWithPort(confChange.Peer.ID, arr[0], 8801, 8802)
		fmt.Println(fmt.Sprintf("peerID:%v,nodeAddr[%v] has been add", confChange.Peer.ID, addr))
	case proto.ConfRemoveNode:
		m.raftStore.DeleteNode(confChange.Peer.ID)
		fmt.Println(fmt.Sprintf("peerID:%v,nodeAddr[%v] has been removed", confChange.Peer.ID, addr))
	case proto.ConfAddLearner:
		var arr []string
		if arr = strings.Split(addr, ":"); len(arr) < 2 {
			fmt.Println(fmt.Sprintf("action[handlePeerChange] add learner nodeAddr[%v] is invalid", addr))
			break
		}
		m.raftStore.AddNodeWithPort(confChange.Peer.ID, arr[0], 8801, 8802)
		fmt.Println(fmt.Sprintf("peerID:%v has add learner nodeAddr[%v] addr[%v]", confChange.Peer.ID, addr, arr[0]))
	}

	return nil
}

func (m *Master) handleApply(cmd *RaftKvData) (err error) {
	fmt.Println("apply cmd ", cmd)
	return nil
}

//need application to implement -- end

//example begin ----------------------------------

//just for test
type testSM struct {
	stopc chan struct{}
}

func (m *Master) handleFunctions() {
	http.Handle("/raftKvTest/add", m.handlerWithInterceptor())
	http.Handle("/raftKvTest/addlearner", m.handlerWithInterceptor())
	http.Handle("/raftKvTest/remove", m.handlerWithInterceptor())
	http.Handle("/raftKvTest/submit", m.handlerWithInterceptor())

	http.Handle("/raftKvTest/getStatus", m.handlerWithInterceptor())
	return
}

func (m *Master) handlerWithInterceptor() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if m.raftPartition[1].IsLeader() {
				m.ServeHTTP(w, r)
			} else {
				http.Error(w, "not leader", http.StatusForbidden)
			}
		})
}

func (m *Master) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/raftKvTest/add":
		m.handleAddRaftNode(w, r)
	case "/raftKvTest/addlearner":
		m.handleAddRaftLearner(w, r)
	case "/raftKvTest/remove":
		m.handleRemoveRaftNode(w, r)
	case "/raftKvTest/submit":
		m.handleRaftKvSubmit(w, r)
	case "/raftKvTest/getStatus":
		m.handleGetStatus(w, r)
	default:
	}
}

func parseRaftNodePara(r *http.Request) (id uint64, host string, err error) {
	r.ParseForm()
	var idStr string
	if idStr = r.FormValue("id"); idStr == "" {
		err = errors.Errorf("parameter id not found %v", r)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
		return
	}
	if host = r.FormValue("addr"); host == "" {
		err = errors.Errorf("parameter addr not found", r)
		return
	}

	if arr := strings.Split(host, ":"); len(arr) < 2 {
		err = errors.Errorf("parameter %v not match", arr)
		return
	}
	return
}

func (m *Master) handleAddRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRaftNodePara(r)
	if err != nil {
		fmt.Println(fmt.Sprintf("parse parameter error: %v", err))
		return
	}

	peer := proto.Peer{ID: id}
	if _, err = m.raftPartition[1].ChangeMember(proto.ConfAddNode, peer, []byte(addr)); err != nil {
		fmt.Println(fmt.Sprintf("add raft error: %v", err))
		return
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successed \n", id, addr)
	io.WriteString(w, msg)
	return
}

func (m *Master) handleAddRaftLearner(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRaftNodePara(r)
	if err != nil {
		fmt.Println(fmt.Sprintf("parse parameter error: %v", err))
		return
	}

	peer := proto.Peer{ID: id}
	if _, err = m.raftPartition[1].ChangeMember(proto.ConfAddLearner, peer, []byte(addr)); err != nil {
		fmt.Println(fmt.Sprintf("add raft error: %v", err))
		return
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successed \n", id, addr)
	io.WriteString(w, msg)
	return
}

func (m *Master) handleRemoveRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRaftNodePara(r)
	if err != nil {
		fmt.Println(fmt.Sprintf("parse para error: %v", err))
		return
	}
	peer := proto.Peer{ID: id}
	if _, err = m.raftPartition[1].ChangeMember(proto.ConfRemoveNode, peer, []byte(addr)); err != nil {
		fmt.Println(fmt.Sprintf("add raft error: %v", err))
		return
	}
	msg = fmt.Sprintf("remove  raft node id :%v,adr:%v successed\n", id, addr)
	io.WriteString(w, msg)
	return
}

func (m *Master) handleRaftKvSubmit(w http.ResponseWriter, r *http.Request) {
	var (
		cmd []byte
		err error
	)
	raftKvData := new(RaftKvData)
	raftKvData.Op = uint32(0x01)
	raftKvData.K = "raft_kv_test01"
	value := strconv.FormatUint(uint64(1234), 10)
	raftKvData.V = []byte(value)
	cmd, err = json.Marshal(raftKvData)
	if err != nil {
		goto errDeal
	}
	if _, err = m.raftPartition[1].Submit(cmd); err != nil {
		goto errDeal
	}
	fmt.Println("raft kv submit", raftKvData, cmd)
	return
errDeal:
	log.LogError("action[submit] err:%v", err.Error())
	return
}

func (m *Master) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	var idStr, msg string
	r.ParseForm()

	if idStr = r.FormValue("groupid"); idStr == "" {
		fmt.Println(errors.Errorf("parameter id not found %v", r))
		return
	}

	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		fmt.Println(errors.Errorf("id parse err %v", r))
		return
	}

	status := m.raftPartition[id].Status()

	msg = fmt.Sprintf("get raftid[%v] status :%v \n", id, status)
	io.WriteString(w, msg)
	return
}

func main() {
	fmt.Println("Hello raft kv store")
	var (
		confFile  = flag.String("c", "", "config file path")
		testParam testSM
	)

	flag.Parse()
	cfg := config.LoadConfigFile(*confFile)
	nodeId := cfg.GetString("nodeid")
	heartbeatPort := cfg.GetString("heartbeat")
	replicatePort := cfg.GetString("replicate")

	m := new(Master)
	m.config = new(MasterConfig)
	m.raftPartition = make(map[uint64]Partition)

	m.nodeId, _ = strconv.ParseUint(nodeId, 10, 10)
	m.heartbeatPort, _ = strconv.Atoi(heartbeatPort)
	m.replicatePort, _ = strconv.Atoi(replicatePort)
	m.groupId = 1
	m.walDir = "raft_log"
	m.storeDir = "store_log"

	peerAddrs := cfg.GetString("peers")
	if err := m.parsePeers(peerAddrs); err != nil {
		log.LogFatal("parse peers fail", err)
		return
	}

	err := m.CreateKvRaft()
	if err != nil {
		fmt.Println("creade kv raft err ", err)
		return
	}

	go func() {
		m.handleFunctions()
		err := http.ListenAndServe(":8800", nil)
		if err != nil {
			fmt.Println("listenAndServe", err)
		}
	}()

	for {
		select {
		case <-testParam.stopc:
			return
		default:
		}
	}

	return
}

func (m *Master) parsePeers(peerStr string) error {
	fmt.Printf("peerStr %s", peerStr)
	peerArr := strings.Split(peerStr, ",")

	m.config.peerAddrs = peerArr
	for _, peerAddr := range peerArr {
		peer := strings.Split(peerAddr, ":")
		id, err := strconv.ParseUint(peer[0], 10, 64)
		if err != nil {
			return err
		}
		ip := peer[1]

		raftPeer := PeerAddress{
			Peer: raftproto.Peer{
				ID: id,
			},
			Address:       ip,
			HeartbeatPort: m.heartbeatPort,
			ReplicatePort: m.replicatePort,
		}

		m.config.peers = append(m.config.peers, raftPeer)
	}

	return nil
}

func (m *Master) CreateKvRaft() (err error) {
	raftCfg := &Config{
		NodeID:        m.nodeId,
		WalPath:       m.walDir,
		IpAddr:        m.ip,
		HeartbeatPort: m.heartbeatPort,
		ReplicatePort: m.replicatePort,
	}

	fmt.Println("create kv raft ", raftCfg.HeartbeatPort, raftCfg.ReplicatePort)

	if m.raftStore, err = NewRaftStore(raftCfg); err != nil {
		return errors.Annotatef(err, "NewRaftStore failed! id[%v] walPath[%v]", m.nodeId, m.walDir)
	}

	fsm := new(RaftKvFsm)
	fsm.store = NewRocksDBStore(m.walDir)
	fsm.RegisterLeaderChangeHandler(m.handleLeaderChange)
	fsm.RegisterPeerChangeHandler(m.handlePeerChange)
	fsm.RegisterApplyHandler(m.handleApply)
	fsm.restore()

	m.fsm = fsm
	fmt.Println(m.config.peers)

	for i := 0; i < 3; i++ {
		partitionCfg := &PartitionConfig{
			ID:      m.groupId + uint64(i),
			Peers:   m.config.peers,
			Applied: fsm.applied,
			SM:      fsm,
		}

		if m.raftPartition[partitionCfg.ID], err = m.raftStore.CreatePartition(partitionCfg); err != nil {
			return errors.Annotate(err, "CreatePartition failed")
		}
	}

	return
}

//example end -------------------------------------------

//Handler
func (rkf *RaftKvFsm) RegisterLeaderChangeHandler(handler RaftLeaderChangeHandler) {
	rkf.leaderChangeHandler = handler
}

func (rkf *RaftKvFsm) RegisterPeerChangeHandler(handler RaftPeerChangeHandler) {
	rkf.peerChangeHandler = handler
}

func (rkf *RaftKvFsm) RegisterApplyHandler(handler RaftKvApplyHandler) {
	rkf.applyHandler = handler
}

//restore apply id
func (rkf *RaftKvFsm) restore() {
	rkf.restoreApplied()
}

func (rkf *RaftKvFsm) restoreApplied() {
	value, err := rkf.Get(RaftApplyId)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied err:%v", err.Error()))
	}
	byteValues := value.([]byte)
	if len(byteValues) == 0 {
		rkf.applied = 0
		return
	}
	applied, err := strconv.ParseUint(string(byteValues), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied,err:%v ", err.Error()))
	}
	rkf.applied = applied
}

//raft StateMachine
func (rkf *RaftKvFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	if rkf.applyHandler != nil {
		cmd := new(RaftKvData)
		if err = json.Unmarshal(command, cmd); err != nil {
			return nil, fmt.Errorf("action[fsmApply],unmarshal data:%v, err:%v", command, err.Error())
		}
		err = rkf.applyHandler(cmd)
		rkf.applied = index
	}
	return
}

func (rkf *RaftKvFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (resp interface{}, err error) {
	if rkf.peerChangeHandler != nil {
		err = rkf.peerChangeHandler(confChange)
	}
	return
}

func (rkf *RaftKvFsm) Snapshot() (proto.Snapshot, error) {
	snapshot := rkf.store.RocksDBSnapshot()

	iterator := rkf.store.Iterator(snapshot)
	iterator.SeekToFirst()
	return &RaftKvSnapshot{
		applied:  rkf.applied,
		snapshot: snapshot,
		fsm:      rkf,
		iterator: iterator,
	}, nil
}

func (rkf *RaftKvFsm) ApplySnapshot(peers []proto.Peer, iterator proto.SnapIterator) (err error) {
	var data []byte
	for err == nil {
		if data, err = iterator.Next(); err != nil {
			goto errDeal
		}
		cmd := &RaftKvData{}
		if err = json.Unmarshal(data, cmd); err != nil {
			goto errDeal
		}
		if _, err = rkf.store.Put(cmd.K, cmd.V); err != nil {
			goto errDeal
		}

		if err = rkf.applyHandler(cmd); err != nil {
			goto errDeal
		}
	}
	return
errDeal:
	if err == io.EOF {
		return
	}
	log.LogError(fmt.Sprintf("action[ApplySnapshot] failed,err:%v", err.Error()))
	return err
}

func (rkf *RaftKvFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

func (rkf *RaftKvFsm) HandleLeaderChange(leader uint64) {
	if rkf.leaderChangeHandler != nil {
		go rkf.leaderChangeHandler(leader)
	}
}

//snapshot interface
type RaftKvSnapshot struct {
	fsm      *RaftKvFsm
	applied  uint64
	snapshot *gorocksdb.Snapshot
	iterator *gorocksdb.Iterator
}

func (rks *RaftKvSnapshot) ApplyIndex() uint64 {
	return rks.applied
}

func (rks *RaftKvSnapshot) Close() {
	rks.fsm.store.ReleaseSnapshot(rks.snapshot)
}

func (rks *RaftKvSnapshot) Next() (data []byte, err error) {

	if rks.iterator.Valid() {
		rks.iterator.Next()
		return data, nil
	}
	return nil, io.EOF
}

//store interface
func (rkf *RaftKvFsm) Put(key, val interface{}) (interface{}, error) {
	return rkf.store.Put(key, val)
}

func (rkf *RaftKvFsm) BatchPut(cmdMap map[string][]byte) (err error) {
	return rkf.store.BatchPut(cmdMap)
}

func (rkf *RaftKvFsm) Get(key interface{}) (interface{}, error) {
	return rkf.store.Get(key)
}

func (rkf *RaftKvFsm) Del(key interface{}) (interface{}, error) {
	return rkf.store.Del(key)
}
