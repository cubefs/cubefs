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
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

func TestMemberWithNoLease(t *testing.T) {
	f, w := getLogFile("", "changemember_nolease.log")
	servers := initTestServer(peers, false, true, 1)
	defer func() {
		w.Flush()
		f.Close()
		for _, s := range servers {
			s.raft.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}()

	fmt.Println("waiting electing leader....")
	leadServer := waitElect(servers, 1, w)
	printStatus(servers, w)

	// test add node
	w.WriteString(fmt.Sprintf("[%s] Add new node \r\n", time.Now().Format(format_time)))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer := createRaftServer(4, leader, term, peers, false, true, 1)
	// add node
	resolver.addNode(4, 0)
	fmt.Println("starting add node")
	leadServer.sm[1].AddNode(proto.Peer{ID: 4})
	fmt.Println("added node")
	time.Sleep(time.Second)
	servers = append(servers, newServer)
	printStatus(servers, w)

	fmt.Println("starting put data")
	if err := leadServer.sm[1].Put("test2", "test2_val", NoCheckLinear, NoCheckLinear); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if vget, err := newServer.sm[1].Get("test2"); err != nil || vget != "test2_val" {
		t.Fatal("new add node not get the data")
	}
	fmt.Println("success put data")

	// test remove node
	w.WriteString(fmt.Sprintf("[%s] Remove node \r\n", time.Now().Format(format_time)))
	fmt.Println("starting remove node")
	leadServer.sm[1].RemoveNode(proto.Peer{ID: 4})
	fmt.Println("removed node")
	fmt.Println("starting put data")
	if err := leadServer.sm[1].Put("test3", "test3_val", NoCheckLinear, NoCheckLinear); err != nil {
		t.Fatal(err)
	}
	fmt.Println("success put data")
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if s.nodeID == newServer.nodeID {
			s.raft.Stop()
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	time.Sleep(100 * time.Millisecond)
	newServer = createRaftServer(4, 0, 10, append(peers, proto.Peer{ID: 4}), false, false, 1)
	servers = append(servers, newServer)
	time.Sleep(10 * time.Second)
	printStatus(servers, w)
	resolver.delNode(4)

}

func TestMemberWithLease(t *testing.T) {
	f, w := getLogFile("", "changemember_lease.log")
	servers := initTestServer(peers, true, true, 1)
	defer func() {
		w.Flush()
		f.Close()
		for _, s := range servers {
			s.raft.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}()

	fmt.Println("waiting electing leader....")
	leadServer := waitElect(servers, 1, w)
	printStatus(servers, w)

	// test add node
	w.WriteString(fmt.Sprintf("[%s] Add new node \r\n", time.Now().Format(format_time)))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer := createRaftServer(4, leader, term, peers, true, true, 1)
	// add node
	resolver.addNode(4, 0)
	fmt.Println("starting add node")
	leadServer.sm[1].AddNode(proto.Peer{ID: 4})
	fmt.Println("added node")
	time.Sleep(time.Second)
	servers = append(servers, newServer)
	printStatus(servers, w)

	fmt.Println("starting put data")
	if err := leadServer.sm[1].Put("test2", "test2_val", NoCheckLinear, NoCheckLinear); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if vget, err := newServer.sm[1].Get("test2"); err != nil || vget != "test2_val" {
		t.Fatal("new add node not get the data")
	}
	fmt.Println("success put data")

	// test remove node
	w.WriteString(fmt.Sprintf("[%s] Remove node \r\n", time.Now().Format(format_time)))
	fmt.Println("starting remove node")
	leadServer.sm[1].RemoveNode(proto.Peer{ID: 4})
	fmt.Println("removed node")
	fmt.Println("starting put data")
	if err := leadServer.sm[1].Put("test3", "test3_val", NoCheckLinear, NoCheckLinear); err != nil {
		t.Fatal(err)
	}
	fmt.Println("success put data")
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if s.nodeID == newServer.nodeID {
			s.raft.Stop()
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	time.Sleep(100 * time.Millisecond)
	newServer = createRaftServer(4, 0, 10, append(peers, proto.Peer{ID: 4}), false, false, 1)
	servers = append(servers, newServer)
	time.Sleep(10 * time.Second)
	printStatus(servers, w)
	resolver.delNode(4)

}
