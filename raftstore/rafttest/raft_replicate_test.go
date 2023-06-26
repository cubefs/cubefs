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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/tiglabs/raft"

	"github.com/tiglabs/raft/proto"
)

func TestReplicate(t *testing.T) {
	tests := []RaftTestConfig{
		{
			name:     "leaderReplWithoutLease_default",
			isLease:  false,
			mode:     StandardMode,
			testFunc: leaderRepl,
		},
		{
			name:     "leaderReplWithLease_default",
			isLease:  true,
			mode:     StandardMode,
			testFunc: leaderRepl,
		},
		{
			name:     "followerRepl_default",
			mode:     StandardMode,
			testFunc: followerRepl,
		},
		{
			name:     "followerRepl_strict",
			mode:     StrictMode,
			testFunc: followerRepl,
		},
		{
			name:     "followerRepl_mix",
			mode:     MixMode,
			testFunc: followerRepl,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t, tt.name, tt.isLease, tt.mode)
		})
	}

}

func leaderRepl(t *testing.T, testName string, isLease bool, mode RaftMode) {
	var servers []*testServer
	servers = initTestServer(peers, isLease, true, 1, mode)

	f, w := getLogFile("", testName+".log")
	defer func() {
		w.Flush()
		f.Close()
	}()

	defer func() {
		for _, server := range servers {
			server.raft.Stop()
		}
	}()
	w.WriteString("waiting electing leader....\n")
	leadServer := waitElect(servers, 1, w)
	w.WriteString(fmt.Sprintf("leader is %v\n", leadServer.nodeID))
	printStatus(servers, w)
	time.Sleep(time.Second)

	// verify load
	for _, s := range servers {
		li, _ := s.store[1].LastIndex()
		w.WriteString(fmt.Sprintf("node(%v) raft(%v) synced raftlog keysize(%v) lastIndex(%v).\r\n", s.nodeID, 1, len(s.sm[1].data), li))
	}

	w.WriteString(fmt.Sprintf("Starting put data at(%v).\r\n", time.Now().Format(format_time)))
	lastKey, lastVal := NoCheckLinear, NoCheckLinear
	for i := 0; i < defaultKeySize; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(i) + "_val"
		if err := leadServer.sm[1].Put(k, v, lastKey, lastVal); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm[1].Get(k); err != nil || vget != v {
			t.Fatal("get Value is wrong err is:", err)
		}
		lastKey, lastVal = k, v
	}
	w.WriteString(fmt.Sprintf("End put data at(%v).\r\n", time.Now().Format(format_time)))

	// add node
	w.WriteString(fmt.Sprintf("[%s] Add new node \r\n", time.Now().Format(format_time)))
	leader, term := leadServer.raft.LeaderTerm(1)
	raftConfig := &raft.RaftConfig{Peers: peers, Leader: leader, Term: term, Mode: getConsistencyMode(mode, 4)}
	newServer := createRaftServer(4, isLease, true, 1, raftConfig)
	// add node
	resolver.addNode(4, 0)
	leadServer.sm[1].AddNode(proto.Peer{ID: 4})
	output("added node")
	time.Sleep(time.Second)
	servers = append(servers, newServer)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("Waiting repl at(%v).\r\n", time.Now().Format(format_time)))
	for {
		a := leadServer.raft.AppliedIndex(1)
		b := newServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}
	w.WriteString(fmt.Sprintf("End repl at(%v).\r\n", time.Now().Format(format_time)))

	w.WriteString(fmt.Sprintf("Starting verify repl data at(%v).\r\n", time.Now().Format(format_time)))
	for k, v := range leadServer.sm[1].data {
		if vget, err := newServer.sm[1].Get(k); err != nil || vget != v {
			t.Fatalf("verify repl error:put and get not match [%v %v %v].\r\n", k, vget, v)
		}
	}
	w.WriteString(fmt.Sprintf("End verify repl data at(%v).\r\n", time.Now().Format(format_time)))

	// restart node
	w.WriteString(fmt.Sprintf("[%s] Restart new node \r\n", time.Now().Format(format_time)))
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if s.nodeID == newServer.nodeID {
			s.raft.Stop()
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	leadServer = waitElect(servers, 1, w)
	w.WriteString(fmt.Sprintf("leader is %v\n", leadServer.nodeID))
	if err := leadServer.sm[1].Put(strconv.Itoa(defaultKeySize+1), strconv.Itoa(defaultKeySize+1)+"_val", NoCheckLinear, NoCheckLinear); err != nil {
		t.Fatal(err)
	}
	if vget, err := leadServer.sm[1].Get(strconv.Itoa(defaultKeySize + 1)); err != nil || vget != strconv.Itoa(defaultKeySize+1)+"_val" {
		t.Fatal("get Value is wrong err is:", err)
	}

	time.Sleep(100 * time.Millisecond)
	raftConfig = &raft.RaftConfig{Peers: append(peers, proto.Peer{ID: 4}), Leader: 0, Term: 10, Mode: getConsistencyMode(mode, 4)}
	newServer = createRaftServer(4, isLease, false, 1, raftConfig)
	servers = append(servers, newServer)
	w.WriteString(fmt.Sprintf("Waiting repl at(%v).\r\n", time.Now().Format(format_time)))
	for {
		a := leadServer.raft.AppliedIndex(1)
		b := newServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}
	w.WriteString(fmt.Sprintf("End repl at(%v).\r\n", time.Now().Format(format_time)))

	w.WriteString(fmt.Sprintf("Starting verify repl data at(%v).\r\n", time.Now().Format(format_time)))
	for k, v := range leadServer.sm[1].data {
		if vget, err := newServer.sm[1].Get(k); err != nil || vget != v {
			t.Fatalf("verify repl error:put and get not match [%v %v %v].\r\n", k, vget, v)
		}
	}
	w.WriteString(fmt.Sprintf("End verify repl data at(%v).\r\n", time.Now().Format(format_time)))
	printStatus(servers, w)
	resolver.delNode(4)
}

func followerRepl(t *testing.T, testName string, isLease bool, mode RaftMode) {
	servers := initTestServer(peers, isLease, true, 1, mode)
	f, w := getLogFile("", testName+".log")
	defer func() {
		w.Flush()
		f.Close()
		for _, s := range servers {
			s.raft.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}()
	fmt.Sprintln("waiting electing leader....")
	waitElect(servers, 1, w)
	printStatus(servers, w)
	time.Sleep(time.Second)

	var followServer *testServer
	for _, s := range servers {
		if !s.raft.IsLeader(1) {
			followServer = s
			break
		}
	}

	if err := followServer.sm[1].Put("err_test", "err_test", NoCheckLinear, NoCheckLinear); err == nil {
		t.Fatal("follow submit data not return error.")
	}
}
