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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

const (
	keysize = 5000
)

func verifyRedoValue(nid uint64, ms *memoryStatemachine) {
	if len(ms.data) >= keysize {
		for i := 0; i < keysize; i++ {
			if ms.data[strconv.Itoa(i)] != strconv.Itoa(i)+"_val" {
				panic(fmt.Errorf("node(%v) raft(%v) load raftlog key(%v) error.\r\n", nid, 1, i))
			}
		}
	}
}

func TestLeaderReplWithoutLease(t *testing.T) {
	f, w := getLogFile("leaderReplWithoutLease.log")
	defer func() {
		w.Flush()
		f.Close()
	}()

	servers := initTestServer(peers, false, false)
	w.WriteString("waiting electing leader....")
	leadServer := waitElect(servers, w)
	printStatus(servers, w)
	time.Sleep(time.Second)

	// verify load
	for _, s := range servers {
		li, _ := s.store.LastIndex()
		w.WriteString(fmt.Sprintf("node(%v) raft(%v) synced raftlog keysize(%v) lastIndex(%v).\r\n", s.nodeID, 1, len(s.sm.data), li))
		verifyRedoValue(s.nodeID, s.sm)
	}

	w.WriteString(fmt.Sprintf("Starting put data at(%v).\r\n", time.Now().Format(format_time)))
	for i := 0; i < keysize; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(i) + "_val"
		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("get value is wrong err is:", err)
		}
	}
	w.WriteString(fmt.Sprintf("End put data at(%v).\r\n", time.Now().Format(format_time)))

	// add node
	w.WriteString(fmt.Sprintf("[%s] Add new node \r\n", time.Now().Format(format_time)))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer := createRaftServer(4, leader, term, peers, false, true)
	// add node
	resolver.addNode(4, 0)
	leadServer.sm.AddNode(proto.Peer{ID: 4})
	fmt.Println("added node")
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
	for k, v := range leadServer.sm.data {
		if vget, err := newServer.sm.Get(k); err != nil || vget != v {
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
	if err := leadServer.sm.Put(strconv.Itoa(keysize+1), strconv.Itoa(keysize+1)+"_val"); err != nil {
		t.Fatal(err)
	}
	if vget, err := leadServer.sm.Get(strconv.Itoa(keysize + 1)); err != nil || vget != strconv.Itoa(keysize+1)+"_val" {
		t.Fatal("get value is wrong err is:", err)
	}

	time.Sleep(100 * time.Millisecond)
	newServer = createRaftServer(4, 0, 10, append(peers, proto.Peer{ID: 4}), false, false)
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
	for k, v := range leadServer.sm.data {
		if vget, err := newServer.sm.Get(k); err != nil || vget != v {
			t.Fatalf("verify repl error:put and get not match [%v %v %v].\r\n", k, vget, v)
		}
	}
	w.WriteString(fmt.Sprintf("End verify repl data at(%v).\r\n", time.Now().Format(format_time)))
	printStatus(servers, w)
	resolver.delNode(4)

	for _, s := range servers {
		s.raft.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}

func TestLeaderReplWithLease(t *testing.T) {
	f, w := getLogFile("leaderReplWithLease.log")
	defer func() {
		w.Flush()
		f.Close()
	}()

	servers := initTestServer(peers, true, false)
	w.WriteString("waiting electing leader....")
	leadServer := waitElect(servers, w)
	printStatus(servers, w)
	time.Sleep(time.Second)

	// verify load
	for _, s := range servers {
		li, _ := s.store.LastIndex()
		w.WriteString(fmt.Sprintf("node(%v) raft(%v) synced raftlog keysize(%v) lastIndex(%v).\r\n", s.nodeID, 1, len(s.sm.data), li))
		verifyRedoValue(s.nodeID, s.sm)
	}

	w.WriteString(fmt.Sprintf("Starting put data at(%v).\r\n", time.Now().Format(format_time)))
	for i := 0; i < keysize; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(i) + "_val"
		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("get value is wrong err is:", err)
		}
	}
	w.WriteString(fmt.Sprintf("End put data at(%v).\r\n", time.Now().Format(format_time)))

	// add node
	w.WriteString(fmt.Sprintf("[%s] Add new node \r\n", time.Now().Format(format_time)))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer := createRaftServer(4, leader, term, peers, true, true)
	// add node
	resolver.addNode(4, 0)
	leadServer.sm.AddNode(proto.Peer{ID: 4})
	w.WriteString("added node")
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

	time.Sleep(time.Second)
	w.WriteString(fmt.Sprintf("Starting verify repl data at(%v).\r\n", time.Now().Format(format_time)))
	for k, v := range leadServer.sm.data {
		if vget, err := newServer.sm.Get(k); err != nil || vget != v {
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
	if err := leadServer.sm.Put(strconv.Itoa(keysize+1), strconv.Itoa(keysize+1)+"_val"); err != nil {
		t.Fatal(err)
	}
	if vget, err := leadServer.sm.Get(strconv.Itoa(keysize + 1)); err != nil || vget != strconv.Itoa(keysize+1)+"_val" {
		t.Fatal("get value is wrong err is:", err)
	}

	time.Sleep(100 * time.Millisecond)
	newServer = createRaftServer(4, 0, 10, append(peers, proto.Peer{ID: 4}), true, false)
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
	for k, v := range leadServer.sm.data {
		if vget, err := newServer.sm.Get(k); err != nil || vget != v {
			t.Fatalf("verify repl error:put and get not match [%v %v %v].\r\n", k, vget, v)
		}
	}
	w.WriteString(fmt.Sprintf("End verify repl data at(%v).\r\n", time.Now().Format(format_time)))
	printStatus(servers, w)
	resolver.delNode(4)

	for _, s := range servers {
		s.raft.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}

func TestFollowerRepl(t *testing.T) {
	w := bufio.NewWriter(os.Stdout)
	servers := initTestServer(peers, false, false)
	fmt.Sprintln("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)
	time.Sleep(time.Second)

	var followServer *testServer
	for _, s := range servers {
		if !s.raft.IsLeader(1) {
			followServer = s
			break
		}
	}

	if err := followServer.sm.Put("err_test", "err_test"); err == nil {
		t.Fatal("follow submit data not return error.")
	}

	for _, s := range servers {
		s.raft.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}
