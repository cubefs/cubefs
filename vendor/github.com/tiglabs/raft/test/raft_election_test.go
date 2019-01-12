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
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

func TestWithoutLeaseAndDown(t *testing.T) {
	f, w := getLogFile("withoutLeaseAndDown.log")
	defer func() {
		w.Flush()
		f.Close()
	}()

	servers := initTestServer(peers, false, true)
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] shutdown one Follower \r\n", time.Now().Format(format_time)))
	shutServer := make([]*testServer, 0)
	newServers := make([]*testServer, 0)

	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID != lead && len(shutServer) == 0 {
			s.raft.Stop()
			shutServer = append(shutServer, s)
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	fmt.Println("waiting electing leader....")
	newLeader := waitElect(servers, w)
	printStatus(servers, w)

	time.Sleep(time.Duration(int64(htbTick) * tickInterval.Nanoseconds()))
	w.WriteString(fmt.Sprintf("[%s] restart shutdown Follower \r\n", time.Now().Format(format_time)))
	leader, term := newLeader.raft.LeaderTerm(1)
	newServers = restartServer(shutServer, leader, term, false)
	servers = append(servers, newServers...)
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] shutdown all Follower \r\n", time.Now().Format(format_time)))
	shutServer = make([]*testServer, 0)
	newServers = make([]*testServer, 0)
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID != lead {
			s.raft.Stop()
			shutServer = append(shutServer, s)
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	fmt.Println("waiting electing leader....")
	newLeader = waitElect(servers, w)
	printStatus(servers, w)

	time.Sleep(time.Duration(int64(htbTick) * tickInterval.Nanoseconds()))
	w.WriteString(fmt.Sprintf("[%s] restart all shutdown Follower \r\n", time.Now().Format(format_time)))
	leader, term = newLeader.raft.LeaderTerm(1)
	newServers = restartServer(shutServer, leader, term, false)
	servers = append(servers, newServers...)
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] shutdown Leader \r\n", time.Now().Format(format_time)))
	shutServer = make([]*testServer, 0)
	newServers = make([]*testServer, 0)
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID == lead && len(shutServer) == 0 {
			s.raft.Stop()
			shutServer = append(shutServer, s)
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	fmt.Println("waiting electing leader....")
	newLeader = waitElect(servers, w)
	printStatus(servers, w)

	time.Sleep(time.Duration(int64(htbTick) * tickInterval.Nanoseconds()))
	w.WriteString(fmt.Sprintf("[%s] restart shutdown Leader \r\n", time.Now().Format(format_time)))
	leader, term = newLeader.raft.LeaderTerm(1)
	newServers = restartServer(shutServer, leader, term, false)
	servers = append(servers, newServers...)
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] let leader to leader \r\n", time.Now().Format(format_time)))
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID == lead {
			s.raft.TryToLeader(1)
			break
		}
	}
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] let follower to leader \r\n", time.Now().Format(format_time)))
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID != lead {
			s.raft.TryToLeader(1)
			break
		}
	}
	fmt.Println("waiting electing leader....")
	time.Sleep(2000 * time.Millisecond)
	waitElect(servers, w)
	printStatus(servers, w)

	for _, s := range servers {
		s.raft.Stop()
	}
	time.Sleep(100 * time.Millisecond)
}

func TestWithLeaseAndDown(t *testing.T) {
	f, w := getLogFile("withLeaseAndDown.log")
	defer func() {
		w.Flush()
		f.Close()
	}()

	servers := initTestServer(peers, true, true)
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] shutdown one Follower \r\n", time.Now().Format(format_time)))
	shutServer := make([]*testServer, 0)
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID != lead && len(shutServer) == 0 {
			s.raft.Stop()
			shutServer = append(shutServer, s)
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	fmt.Println("waiting electing leader....")
	newLeader := waitElect(servers, w)
	printStatus(servers, w)

	time.Sleep(time.Duration(int64(htbTick) * tickInterval.Nanoseconds()))
	w.WriteString(fmt.Sprintf("[%s] restart shutdown Follower \r\n", time.Now().Format(format_time)))
	leader, term := newLeader.raft.LeaderTerm(1)
	newServers = restartServer(shutServer, leader, term, false)
	servers = append(servers, newServers...)
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] shutdown all Follower \r\n", time.Now().Format(format_time)))
	shutServer = make([]*testServer, 0)
	newServers = make([]*testServer, 0)
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID != lead {
			s.raft.Stop()
			shutServer = append(shutServer, s)
		} else {
			newServers = append(newServers, s)
		}
	}
	stopT := time.Now()
	w.WriteString(fmt.Sprintf("[%s] shutdown all Follower \r\n", stopT.Format(format_time)))
	servers = newServers
	var end time.Time
	for {
		flag := false
		for _, s := range servers {
			if lead, _ := s.raft.LeaderTerm(1); lead == 0 {
				flag = true
				end = time.Now()
			}
		}
		if flag {
			break
		}
	}
	w.WriteString(fmt.Sprintf("[%s] Leader step down.\r\n", end.Format(format_time)))
	if (end.Sub(stopT).Nanoseconds() + int64(htbTick)*tickInterval.Nanoseconds()) < int64(elcTick)*tickInterval.Nanoseconds() {
		w.WriteString("Leader step down not lost lease.")
		t.Fatal("Leader step down not lost lease.")
	}
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] restart all shutdown Follower \r\n", time.Now().Format(format_time)))
	newServers = restartServer(shutServer, 0, 10, false)
	servers = append(servers, newServers...)
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] shutdown Leader \r\n", time.Now().Format(format_time)))
	shutServer = make([]*testServer, 0)
	newServers = make([]*testServer, 0)
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID == lead && len(shutServer) == 0 {
			s.raft.Stop()
			stopT = time.Now()
			w.WriteString(fmt.Sprintf("[%s] stop leader.\r\n", stopT.Format(format_time)))
			shutServer = append(shutServer, s)
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	if ns := waitAndValidElect(servers, w, stopT); ns == nil {
		t.Fatal("Lease Election error")
	} else {
		leader, term = ns.raft.LeaderTerm(1)
	}
	printStatus(servers, w)

	time.Sleep(time.Duration(int64(htbTick) * tickInterval.Nanoseconds()))
	w.WriteString(fmt.Sprintf("[%s] restart shutdown Leader \r\n", time.Now().Format(format_time)))
	newServers = restartServer(shutServer, leader, term, false)
	servers = append(servers, newServers...)
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] let leader to leader \r\n", time.Now().Format(format_time)))
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID == lead {
			s.raft.TryToLeader(1)
			break
		}
	}
	fmt.Println("waiting electing leader....")
	waitElect(servers, w)
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] let follower to leader \r\n", time.Now().Format(format_time)))
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID != lead {
			s.raft.TryToLeader(1)
			break
		}
	}
	fmt.Println("waiting electing leader....")
	time.Sleep(2000 * time.Millisecond)
	waitElect(servers, w)
	printStatus(servers, w)

	for _, s := range servers {
		s.raft.Stop()
	}
	time.Sleep(100 * time.Millisecond)
}

func TestWithPriorityAndDown(t *testing.T) {
	f, w := getLogFile("withPriorityAndDown.log")
	defer func() {
		w.Flush()
		f.Close()
	}()

	time.Sleep(100 * time.Millisecond)
	peers := []proto.Peer{proto.Peer{ID: 1, Priority: 1}, proto.Peer{ID: 2, Priority: 3}, proto.Peer{ID: 3, Priority: 2}}
	servers := initTestServer(peers, false, true)
	fmt.Println("waiting electing leader....")
	if ns := waitElect(servers, w); ns.nodeID != 2 && ns.nodeID != 3 {
		t.Fatal("Priority Election error")
	}
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] shutdown one Follower \r\n", time.Now().Format(format_time)))
	shutServer := make([]*testServer, 0)
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID != lead && len(shutServer) == 0 {
			s.raft.Stop()
			shutServer = append(shutServer, s)
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	printStatus(servers, w)

	w.WriteString(fmt.Sprintf("[%s] shutdown Leader \r\n", time.Now().Format(format_time)))
	shutServer = make([]*testServer, 0)
	newServers = make([]*testServer, 0)
	for _, s := range servers {
		if lead, _ := s.raft.LeaderTerm(1); s.nodeID == lead && len(shutServer) == 0 {
			s.raft.Stop()
			shutServer = append(shutServer, s)
		} else {
			newServers = append(newServers, s)
		}
	}
	servers = newServers
	printStatus(servers, w)

	time.Sleep(100 * time.Millisecond)
	w.WriteString(fmt.Sprintf("[%s] restart shutdown Leader \r\n", time.Now().Format(format_time)))
	newServers = restartServer(shutServer, 0, 10, true)
	servers = append(servers, newServers...)
	fmt.Println("waiting electing leader....")
	if ns := waitElect(servers, w); ns.nodeID == shutServer[0].nodeID {
		t.Fatal("Priority Election error")
	}
	printStatus(servers, w)

	for _, s := range servers {
		s.raft.Stop()
	}
	time.Sleep(100 * time.Millisecond)
}

func waitElect(ts []*testServer, w *bufio.Writer) *testServer {
	defer w.Flush()

	var term uint64
	var leader *testServer
	for {
	redo:
		term = 0
		leader = nil
		for _, s := range ts {
			l, t := s.raft.LeaderTerm(1)
			if (term > 0 && term != t) || l == 0 || (leader != nil && leader.nodeID != l) {
				goto redo
			}
			term = t
			if s.nodeID == l {
				if leader != nil {
					goto redo
				}
				leader = s
			}
		}
		if leader != nil {
			w.WriteString(fmt.Sprintf("[%s] elected leader: %v\r\n", time.Now().Format(format_time), leader.nodeID))
			return leader
		}
	}
}

func waitAndValidElect(ts []*testServer, w *bufio.Writer, start time.Time) *testServer {
	defer w.Flush()

	flag := false
	var ret *testServer
	for {
		for _, s := range ts {
			if !flag {
				if s.raft.IsLeader(1) {
					flag = true
					end := time.Now()
					w.WriteString(fmt.Sprintf("[%s] Follower begin election.\r\n", end.Format(format_time)))
					if (end.Sub(start).Nanoseconds()) < (2*int64(elcTick)-1)*tickInterval.Nanoseconds() {
						w.WriteString("Follow begin election not lose lease.\r\n")
						return nil
					}
				}
			}
			if s.raft.IsLeader(1) {
				if ret != nil {
					w.WriteString("ERR: There is more than one leader.\r\n")
					return nil
				}
				ret = s
				w.WriteString(fmt.Sprintf("[%s] elected leader: %v\r\n", time.Now().Format(format_time), s.nodeID))
			}
		}
		if ret != nil {
			break
		}
	}

	return ret
}

func printStatus(ts []*testServer, w *bufio.Writer) {
	defer w.Flush()

	time.Sleep(time.Second)
	for _, s := range ts {
		w.WriteString(s.raft.Status(1).String())
		w.WriteString("\r\n")
	}
	w.WriteString("\r\n")
}

func restartServer(ts []*testServer, leader, term uint64, clear bool) []*testServer {
	ret := make([]*testServer, 0)
	for _, s := range ts {
		ns := createRaftServer(s.nodeID, leader, term, s.peers, s.isLease, clear)
		ret = append(ret, ns)
	}
	return ret
}
