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
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

func TestSnapFullNewServer(t *testing.T) {
	if !testSnap {
		return
	}

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	servers := initTestServer(peers, false, true)
	fmt.Println("waiting electing leader....")
	leadServer := waitElect(servers, w)
	printStatus(servers, w)

	valCache := make(map[string]string)
	fmt.Println("initing data")
	for i := 0; i <= 5000; i++ {
		k := randomStr(10 + i)
		v := randomStr(5 + i)

		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("init data error:put and get not match")
		}
		valCache[k] = v
	}
	fmt.Println("inited data")

	// truncate
	leadServer.raft.Truncate(1, leadServer.raft.AppliedIndex(1))
	leadServer.sm.setApplied(leadServer.raft.AppliedIndex(1))
	time.Sleep(time.Second)

	// add node
	fmt.Printf("[%s] Add new node \r\n", time.Now().Format(format_time))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer := createRaftServer(4, leader, term, peers, false, true)
	// add node
	resolver.addNode(4, 0)
	leadServer.sm.AddNode(proto.Peer{ID: 4})
	fmt.Println("added node")
	time.Sleep(time.Second)
	servers = append(servers, newServer)

	fmt.Printf("Waiting repl at(%v).\r\n", time.Now().Format(format_time))
	for {
		a := leadServer.raft.AppliedIndex(1)
		b := newServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}
	fmt.Printf("End repl at(%v).\r\n", time.Now().Format(format_time))

	fmt.Println("verify snapshot data...")
	printStatus(servers, w)
	for k, v := range valCache {
		if vget, err := newServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("verify snapshot error:put and get not match")
		}
	}
	fmt.Println("verify snapshot data success.")

	fmt.Println("staring put data")
	for i := 0; i < 1000; i++ {
		k := randomStr(20 + i)
		v := randomStr(15 + i)
		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("put data error:put and get not match")
		}
		valCache[k] = v
	}
	fmt.Println("end put data")

	for {
		a := leadServer.raft.AppliedIndex(1)
		b := newServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}

	fmt.Println("verify repl data...")
	printStatus(servers, w)
	for k, v := range valCache {
		if vget, err := newServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("verify repl data error:put and get not match")
		}
	}
	fmt.Println("verify repl data success.")

	resolver.delNode(4)
	for _, s := range servers {
		s.raft.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}

func TestSnapPartialNewServer(t *testing.T) {
	if !testSnap {
		return
	}

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	servers := initTestServer(peers, false, true)
	fmt.Println("waiting electing leader....")
	leadServer := waitElect(servers, w)
	printStatus(servers, w)

	valCache := make(map[string]string)
	fmt.Println("initing data")
	for i := 0; i <= 5000; i++ {
		k := randomStr(10 + i)
		v := randomStr(5 + i)

		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("init data error:put and get not match")
		}
		valCache[k] = v
	}
	fmt.Println("inited data")

	// truncate
	leadServer.raft.Truncate(1, 3000)
	leadServer.sm.setApplied(3000)
	time.Sleep(time.Second)

	// add node
	fmt.Printf("[%s] Add new node \r\n", time.Now().Format(format_time))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer := createRaftServer(4, leader, term, peers, false, true)
	// add node
	resolver.addNode(4, 0)
	leadServer.sm.AddNode(proto.Peer{ID: 4})
	fmt.Println("added node")
	time.Sleep(time.Second)
	servers = append(servers, newServer)

	fmt.Printf("Waiting repl at(%v).\r\n", time.Now().Format(format_time))
	for {
		a := leadServer.raft.AppliedIndex(1)
		b := newServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}
	fmt.Printf("End repl at(%v).\r\n", time.Now().Format(format_time))

	fmt.Println("verify snapshot data...")
	printStatus(servers, w)
	for k, v := range valCache {
		if vget, err := newServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("verify snapshot error:put and get not match")
		}
	}
	fmt.Println("verify snapshot data success.")

	fmt.Println("staring put data")
	for i := 0; i < 1000; i++ {
		k := randomStr(20 + i)
		v := randomStr(15 + i)
		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("put data error:put and get not match")
		}
		valCache[k] = v
	}
	fmt.Println("end put data")

	for {
		a := leadServer.raft.AppliedIndex(1)
		b := newServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}

	fmt.Println("verify repl data...")
	printStatus(servers, w)
	for k, v := range valCache {
		if vget, err := newServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("verify repl data error:put and get not match")
		}
	}
	fmt.Println("verify repl data success.")

	resolver.delNode(4)
	for _, s := range servers {
		s.raft.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}

func TestSnapFullRestartServer(t *testing.T) {
	if !testSnap {
		return
	}

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	servers := initTestServer(peers, false, true)
	fmt.Println("waiting electing leader....")
	leadServer := waitElect(servers, w)
	printStatus(servers, w)

	valCache := make(map[string]string)
	fmt.Println("initing data")
	for i := 0; i <= 5000; i++ {
		k := randomStr(10 + i)
		v := randomStr(5 + i)

		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("init data error:put and get not match")
		}
		valCache[k] = v
	}
	fmt.Println("inited data")

	// shutdown follower
	var followServer *testServer
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if !s.raft.IsLeader(1) && followServer == nil {
			followServer = s
			followServer.raft.Stop()
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	k := randomStr(10 + 5001)
	v := randomStr(5 + 5001)
	if err := leadServer.sm.Put(k, v); err != nil {
		t.Fatal(err)
	}
	if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
		t.Fatal("init data error:put and get not match")
	}
	valCache[k] = v

	// truncate
	leadServer.raft.Truncate(1, leadServer.raft.AppliedIndex(1))
	leadServer.sm.setApplied(leadServer.raft.AppliedIndex(1))
	time.Sleep(time.Second)

	// restart follower
	lead, term := leadServer.raft.LeaderTerm(1)
	followServer = restartServer([]*testServer{followServer}, lead, term, false)[0]
	servers = append(servers, followServer)
	fmt.Printf("Waiting repl at(%v).\r\n", time.Now().Format(format_time))
	for {
		a := leadServer.raft.AppliedIndex(1)
		b := followServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}
	fmt.Printf("End repl at(%v).\r\n", time.Now().Format(format_time))

	fmt.Println("verify snapshot data...")
	printStatus(servers, w)
	for k, v := range valCache {
		if vget, err := followServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("verify snapshot error:put and get not match")
		}
	}
	fmt.Println("verify snapshot data success.")

	fmt.Println("staring put data")
	for i := 0; i < 1000; i++ {
		k := randomStr(20 + i)
		v := randomStr(15 + i)
		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("put data error:put and get not match")
		}
		valCache[k] = v
	}
	fmt.Println("end put data")

	for {
		a := leadServer.raft.AppliedIndex(1)
		b := followServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}

	fmt.Println("verify repl data...")
	printStatus(servers, w)
	for k, v := range valCache {
		if vget, err := followServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("verify repl data error:put and get not match")
		}
	}
	fmt.Println("verify repl data success.")

	resolver.delNode(4)
	for _, s := range servers {
		s.raft.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}

func TestSnapPartialRestartServer(t *testing.T) {
	if !testSnap {
		return
	}

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	servers := initTestServer(peers, false, true)
	fmt.Println("waiting electing leader....")
	leadServer := waitElect(servers, w)
	printStatus(servers, w)

	valCache := make(map[string]string)
	fmt.Println("initing data")
	for i := 0; i <= 5000; i++ {
		k := randomStr(10 + i)
		v := randomStr(5 + i)

		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("init data error:put and get not match")
		}
		valCache[k] = v
	}
	fmt.Println("inited data")

	// shutdown follower
	var followServer *testServer
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if !s.raft.IsLeader(1) && followServer == nil {
			followServer = s
			followServer.raft.Stop()
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	k := randomStr(10 + 5001)
	v := randomStr(5 + 5001)
	if err := leadServer.sm.Put(k, v); err != nil {
		t.Fatal(err)
	}
	if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
		t.Fatal("init data error:put and get not match")
	}
	valCache[k] = v

	// truncate
	leadServer.raft.Truncate(1, 2300)
	leadServer.sm.setApplied(2300)
	time.Sleep(time.Second)

	// restart follower
	lead, term := leadServer.raft.LeaderTerm(1)
	followServer = restartServer([]*testServer{followServer}, lead, term, false)[0]
	servers = append(servers, followServer)
	fmt.Printf("Waiting repl at(%v).\r\n", time.Now().Format(format_time))
	for {
		a := leadServer.raft.AppliedIndex(1)
		b := followServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}
	fmt.Printf("End repl at(%v).\r\n", time.Now().Format(format_time))

	fmt.Println("verify snapshot data...")
	printStatus(servers, w)
	for k, v := range valCache {
		if vget, err := followServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("verify snapshot error:put and get not match")
		}
	}
	fmt.Println("verify snapshot data success.")

	fmt.Println("staring put data")
	for i := 0; i < 1000; i++ {
		k := randomStr(20 + i)
		v := randomStr(15 + i)
		if err := leadServer.sm.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if vget, err := leadServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("put data error:put and get not match")
		}
		valCache[k] = v
	}
	fmt.Println("end put data")

	for {
		a := leadServer.raft.AppliedIndex(1)
		b := followServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}

	fmt.Println("verify repl data...")
	printStatus(servers, w)
	for k, v := range valCache {
		if vget, err := followServer.sm.Get(k); err != nil || vget != v {
			t.Fatal("verify repl data error:put and get not match")
		}
	}
	fmt.Println("verify repl data success.")

	resolver.delNode(4)
	for _, s := range servers {
		s.raft.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}
