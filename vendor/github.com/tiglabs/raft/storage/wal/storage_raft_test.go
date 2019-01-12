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

package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

type raftAddr struct {
	heartbeat string
	replicate string
}

var raftAddresses = make(map[uint64]*raftAddr)

// 三个本地节点
func init() {
	for i := 1; i <= 3; i++ {
		raftAddresses[uint64(i)] = &raftAddr{
			heartbeat: fmt.Sprintf(":99%d1", i),
			replicate: fmt.Sprintf(":99%d2", i),
		}
	}
}

type testResolver struct {
}

func (r *testResolver) AllNodes() (all []uint64) {
	for k := range raftAddresses {
		all = append(all, k)
	}
	return
}

func (r *testResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	raddr, ok := raftAddresses[nodeID]
	if !ok {
		return "", errors.New("no such node")
	}
	switch stype {
	case raft.HeartBeat:
		return "127.0.0.1" + raddr.heartbeat, nil
	case raft.Replicate:
		return "127.0.0.1" + raddr.replicate, nil
	default:
		return "", errors.New("unknown socket type")
	}
}

// 状态机为一个数字，命令为一个数字i，收到命令后sum+=i
type testStateMachine struct {
	t      *testing.T
	nodeID uint64
	sum    uint64
}

func (m *testStateMachine) Apply(command []byte, index uint64) (interface{}, error) {
	u := binary.BigEndian.Uint64(command)
	m.sum += u
	m.t.Logf("[NODE: %d] sum increased %d, sum=%d", m.nodeID, u, m.sum)
	return m.sum, nil
}

func (m *testStateMachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

func (m *testStateMachine) Snapshot() (proto.Snapshot, error) {
	return nil, nil
}

func (m *testStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	return nil
}

func (m *testStateMachine) HandleFatalEvent(err *raft.FatalError) {
}

func (m *testStateMachine) HandleLeaderChange(leader uint64) {
}

func TestRaft(t *testing.T) {
	var err error
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_raft_wal_raft_")
	if err != nil {
		t.Fatal(err)
	}
	// dir := "/tmp/fbase_test_raft_wal_raft_549378644"
	t.Logf("TestPath: %v", dir)
	defer os.RemoveAll(dir)

	var wg sync.WaitGroup
	wg.Add(3)
	exitC := make(chan struct{})
	for i := 1; i <= 3; i++ {
		go runNode(t, uint64(i), dir, exitC, &wg)
	}
	wg.Wait()
}

// 启动一个节点，提交命令，当状态机里的sum到达一定值退出
func runNode(t *testing.T, nodeID uint64, dir string, exitC chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	listenAddr, ok := raftAddresses[nodeID]
	if !ok {
		t.Fatal("no such node")
	}

	rc := raft.DefaultConfig()
	rc.TickInterval = time.Millisecond * 100
	rc.NodeID = nodeID
	rc.Resolver = &testResolver{}
	rc.HeartbeatAddr = listenAddr.heartbeat
	rc.ReplicateAddr = listenAddr.replicate
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		t.Fatal(err)
	}

	// path: {dir}/{nodeID}
	wal, err := NewStorage(path.Join(dir, fmt.Sprintf("%d", nodeID)), nil)
	if err != nil {
		t.Fatal(err)
	}

	var peers []proto.Peer
	for k := range raftAddresses {
		peers = append(peers, proto.Peer{ID: k})
	}
	statemachine := &testStateMachine{
		t:      t,
		nodeID: nodeID,
	}
	raftConfig := &raft.RaftConfig{
		ID:           1,
		Applied:      0,
		Peers:        peers,
		Storage:      wal,
		StateMachine: statemachine,
	}
	err = rs.CreateRaft(raftConfig)
	if err != nil {
		t.Fatal(err)
	}

	ticker := time.NewTicker(time.Duration(rc.HeartbeatTick) * rc.TickInterval)
	var i uint64
	for {
		select {
		case <-ticker.C:
			if rs.IsLeader(1) {
				b := make([]byte, 8)
				i++
				binary.BigEndian.PutUint64(b, i)
				f := rs.Submit(1, b)
				resp, err := f.Response()
				if err != nil {
					t.Error(err)
				}
				sum := resp.(uint64)
				if sum >= 50 {
					close(exitC)
					return
				}
			}

		case <-exitC:
			return
		}
	}
}
