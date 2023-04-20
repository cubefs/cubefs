package main

import (
	"bufio"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tiglabs/raft"
)

func TestLinear(t *testing.T) {
	tests := []RaftTestConfig{
		{
			name:     "linearWithLeaderChange_default",
			mode:     StandardMode,
			testFunc: linearWithLeaderChange,
		},
		{
			name:     "linearWithLeaderChange_strict",
			mode:     StrictMode,
			testFunc: linearWithLeaderChange,
		},
		{
			name:     "linearWithLeaderChange_mix",
			mode:     MixMode,
			testFunc: linearWithLeaderChange,
		},
		{
			name:     "linearWithDelLeader_default",
			mode:     StandardMode,
			testFunc: linearWithDelLeader,
		},
		{
			name:     "linearWithDelLeader_strict",
			mode:     StrictMode,
			testFunc: linearWithDelLeader,
		},
		{
			name:     "linearWithDelLeader_mix",
			mode:     MixMode,
			testFunc: linearWithDelLeader,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t, tt.name, tt.isLease, tt.mode)
		})
	}
}

func TestLinearWithMemberChange(t *testing.T) {
	servers := initTestServer(peers, true, true, 1, StandardMode)
	f, w := getLogFile("", "TestLinearWithMemberChange.log")
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}()

	leadServer := waitElect(servers, 1, w)
	printStatus(servers, w)
	time.Sleep(time.Second)
	dataLen := verifyRestoreValue(servers, leadServer, w)

	// put data
	var wg sync.WaitGroup
	wg.Add(1)
	go func(len int) {
		defer wg.Done()
		if _, err := leadServer.putData(1, len, PutDataStep, w); err != nil {
			t.Fatal(err)
		}
	}(dataLen)
	dataLen += PutDataStep
	// test add member
	newServer := leadServer.addMember(4, raft.DefaultMode, w, t)
	servers = append(servers, newServer)
	printStatus(servers, w)
	wg.Wait()
	leadServer = waitElect(servers, 1, w)
	compareTwoServers(leadServer, newServer, w, t)
	printStatus(servers, w)

	// test delete member
	wg.Add(1)
	go func(len int) {
		defer wg.Done()
		if _, err := leadServer.putData(1, len, PutDataStep, w); err != nil {
			t.Fatal(err)
		}
	}(dataLen)
	// delete node
	newServers := make([]*testServer, 0)
	leadServer.deleteMember(newServer, w, t)
	for _, s := range servers {
		if s.nodeID == newServer.nodeID {
			continue
		}
		newServers = append(newServers, s)
	}
	servers = newServers
	printStatus(servers, w)
	wg.Wait()
	compareServersWithLeader(servers, w, t)
	printStatus(servers, w)
}

//func TestLinearWithQuorumChange(t *testing.T) {
//	var servers []*testServer
//
//	f, w := getLogFile("", "linearWithQuorumChange.log")
//	servers = initTestServer(peers, true, false, 1)
//	defer func() {
//		for _, server := range servers {
//			server.raft.Stop()
//		}
//		w.Flush()
//		f.Close()
//	}()
//
//	leadServer := waitElect(servers, 1, w)
//	printStatus(servers, w)
//	time.Sleep(time.Second)
//	dataLen := verifyRestoreValue(servers, leadServer, w)
//
//	// put data
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go func(len int) {
//		if _, err := leadServer.putData(1, len, PutDataStep, w); err != nil {
//			t.Fatal(err)
//		}
//		wg.Done()
//	}(dataLen)
//	dataLen += PutDataStep
//	// test add 2 members to change quorum
//	newServer1 := leadServer.addMember(4, w, t)
//	servers = append(servers, newServer1)
//	newServer2 := leadServer.addMember(5, w, t)
//	servers = append(servers, newServer2)
//	printStatus(servers, w)
//	wg.Wait()
//	compareTwoServers(leadServer, newServer1, w, t)
//	compareTwoServers(leadServer, newServer2, w, t)
//	printStatus(servers, w)
//
//	// test delete member
//	wg.Add(1)
//	go func(len int) {
//		if _, err := leadServer.putData(1, len, PutDataStep, w); err != nil {
//			t.Fatal(err)
//		}
//		wg.Done()
//	}(dataLen)
//	// delete node
//	newServers := make([]*testServer, 0)
//	leadServer.deleteMember(newServer1, w, t)
//	leadServer.deleteMember(newServer2, w, t)
//	for _, s := range servers {
//		if s.nodeID == newServer1.nodeID || s.nodeID == newServer2.nodeID {
//			continue
//		}
//		newServers = append(newServers, s)
//	}
//	servers = newServers
//	printStatus(servers, w)
//	wg.Wait()
//	compareServersWithLeader(servers, w, t)
//	printStatus(servers, w)
//
//	time.Sleep(100 * time.Millisecond)
//}

func linearWithLeaderChange(t *testing.T, testName string, isLease bool, mode RaftMode) {
	servers := initTestServer(peers, true, true, 1, mode)
	f, w := getLogFile("", testName+".log")
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}()

	leadServer := waitElect(servers, 1, w)
	printStatus(servers, w)
	time.Sleep(time.Second)
	dataLen := verifyRestoreValue(servers, leadServer, w)

	// test downtime
	var wg sync.WaitGroup
	wg.Add(1)
	go func(len int) {
		defer wg.Done()
		putDataWithRetry(servers, len, PutDataStep, w, t)
	}(dataLen)
	dataLen += PutDataStep

	// try follower to leader
	output("let follower to leader")
	var tryLeaderServer *testServer
	for _, s := range servers {
		l, _ := s.raft.LeaderTerm(1)
		if l != s.nodeID {
			tryLeaderServer = s
			break
		}
	}
	w.WriteString(fmt.Sprintf("let follower server[%v] to leader at(%v).\r\n", tryLeaderServer.nodeID, time.Now().Format(format_time)))
	tryLeaderServer.raft.TryToLeader(1)
	waitElect(servers, 1, w)
	printStatus(servers, w)
	wg.Wait()

	compareServersWithLeader(servers, w, t)
	printStatus(servers, w)
}

func linearWithDelLeader(t *testing.T, testName string, isLease bool, mode RaftMode) {
	servers := initTestServer(peers, true, true, 1, mode)
	f, w := getLogFile("", testName+".log")
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
	}()

	leadServer := waitElect(servers, 1, w)
	printStatus(servers, w)
	time.Sleep(time.Second)
	dataLen := verifyRestoreValue(servers, leadServer, w)
	leadServer = waitElect(servers, 1, w)

	go func(startIndex int) {
		if _, err := leadServer.putData(1, startIndex, PutDataStep, w); err != nil {
			output("put data err: %v", err)
			w.WriteString(fmt.Sprintf("put data err[%v] at(%v).\r\n", err, time.Now().Format(format_time)))
		}
	}(dataLen)

	// delete raft leader server and add
	leadServer, servers = delAndAddLeader(servers, w, t)
	startIndex := verifyRestoreValue(servers, leadServer, w)
	output("start put data")
	if _, err := leadServer.putData(1, startIndex, PutDataStep/5, w); err != nil {
		t.Fatal(err)
	}
	compareServersWithLeader(servers, w, t)
	printStatus(servers, w)
}

func TestLinearWithFollowerDown(t *testing.T) {
	servers := initTestServer(peers, true, false, 1, StandardMode)
	f, w := getLogFile("", "linearWithFollowerDown.log")
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}()

	leadServer := waitElect(servers, 1, w)
	printStatus(servers, w)
	time.Sleep(time.Second)
	dataLen := verifyRestoreValue(servers, leadServer, w)

	// test downtime
	var wg sync.WaitGroup
	wg.Add(1)
	go func(len int) {
		defer wg.Done()
		if _, err := leadServer.putData(1, len, PutDataStep, w); err != nil {
			t.Fatal(err)
		}
	}(dataLen)
	dataLen += PutDataStep

	// stop and restart raft server
	time.Sleep(1 * time.Second)
	output("stop and restart raft server")
	var downServer *testServer
	newServers := make([]*testServer, 0)
	for _, s := range servers {
		l, _ := s.raft.LeaderTerm(1)
		if l != s.nodeID && downServer == nil {
			downServer = s
			continue
		}
		newServers = append(newServers, s)
	}
	w.WriteString(fmt.Sprintf("stop and restart raft server[%v] at(%v).\r\n", downServer.nodeID, time.Now().Format(format_time)))
	downServer.raft.Stop()
	raftConfig := &raft.RaftConfig{Peers: peers, Leader: 0, Term: 0, Mode: downServer.mode}
	downServer = createRaftServer(downServer.nodeID, true, false, 1, raftConfig)
	newServers = append(newServers, downServer)
	servers = newServers
	printStatus(servers, w)
	wg.Wait()

	compareServersWithLeader(servers, w, t)
	printStatus(servers, w)
}

func TestLinearWithLeaderDown(t *testing.T) {
	servers := initTestServer(peers, true, false, 1, StandardMode)
	f, w := getLogFile("", "linearWithLeaderDown.log")
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
		time.Sleep(100 * time.Millisecond)
	}()

	leadServer := waitElect(servers, 1, w)
	printStatus(servers, w)
	time.Sleep(time.Second)
	dataLen := verifyRestoreValue(servers, leadServer, w)
	leadServer = waitElect(servers, 1, w)

	go func(startIndex int) {
		if _, err := leadServer.putData(1, startIndex, PutDataStep, w); err != nil {
			output("put data err: %v", err)
			w.WriteString(fmt.Sprintf("put data err[%v] at(%v).\r\n", err, time.Now().Format(format_time)))
		}
	}(dataLen)

	time.Sleep(1 * time.Second)

	// stop and restart raft leader server
	leadServer, servers = restartLeader(servers, w)
	waitForApply(servers, 1, w)

	startIndex := verifyRestoreValue(servers, leadServer, w)
	output("start put data")
	if _, err := leadServer.putData(1, startIndex, PutDataStep/5, w); err != nil {
		t.Fatal(err)
	}

	compareServersWithLeader(servers, w, t)
	printStatus(servers, w)
}

func delAndAddLeader(servers []*testServer, w *bufio.Writer, t *testing.T) (leadServer *testServer, newServers []*testServer) {
	time.Sleep(1 * time.Second)
	output("delete raft leader server and add a member")
	var delServer *testServer
	newServers = make([]*testServer, 0)
	for _, s := range servers {
		l, _ := s.raft.LeaderTerm(1)
		if l == s.nodeID && delServer == nil {
			delServer = s
			continue
		}
		newServers = append(newServers, s)
	}
	w.WriteString(fmt.Sprintf("delete member of raft leader server[%v] at(%v).\r\n", delServer.nodeID, time.Now().Format(format_time)))
	delServer.deleteMember(delServer, w, t)
	leadServer = waitElect(newServers, 1, w)
	delServer = leadServer.addMember(delServer.nodeID, delServer.mode, w, t)
	newServers = append(newServers, delServer)
	leadServer = waitElect(newServers, 1, w)
	return
}
