package main

import (
	"bufio"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLinearWithMemberChange(t *testing.T) {
	servers := initTestServer(peers, true, true, 1)
	f, w := getLogFile("", "linearWithMemberChange.log")
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
	newServer := leadServer.addMember(4, w, t)
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

func TestLinearWithLeaderChange(t *testing.T) {
	servers := initTestServer(peers, true, false, 1)
	f, w := getLogFile("", "linearWithLeaderChange.log")
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

	// stop and restart raft leader server
	fmt.Println("let follower to leader")
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

func TestLinearWithFollowerDown(t *testing.T) {
	servers := initTestServer(peers, true, false, 1)
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
	time.Sleep(5 * time.Second)
	fmt.Println("stop and restart raft server")
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
	downServer = createRaftServer(downServer.nodeID, 0, 0, peers, true, false, 1)
	newServers = append(newServers, downServer)
	servers = newServers
	printStatus(servers, w)
	wg.Wait()

	compareServersWithLeader(servers, w, t)
	printStatus(servers, w)
}

func TestLinearWithLeaderDown(t *testing.T) {
	servers := initTestServer(peers, true, false, 1)
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
			fmt.Println("put data err: ", err)
			w.WriteString(fmt.Sprintf("put data err[%v] at(%v).\r\n", err, time.Now().Format(format_time)))
		}
	}(dataLen)

	// stop and restart raft leader server
	leadServer, servers = restartLeader(servers, w)

	startIndex := verifyRestoreValue(servers, leadServer, w)
	fmt.Println("start put data")
	if _, err := leadServer.putData(1, startIndex, PutDataStep/5, w); err != nil {
		t.Fatal(err)
	}

	compareServersWithLeader(servers, w, t)
	printStatus(servers, w)
}

func restartLeader(servers []*testServer, w *bufio.Writer) (leaderServer *testServer, newServers []*testServer) {
	time.Sleep(5 * time.Second)
	fmt.Println("stop and restart raft leader server")
	var downServer *testServer
	newServers = make([]*testServer, 0)
	for _, s := range servers {
		l, _ := s.raft.LeaderTerm(1)
		if l == s.nodeID && downServer == nil {
			downServer = s
			continue
		}
		newServers = append(newServers, s)
	}
	w.WriteString(fmt.Sprintf("stop and restart raft leader server[%v] at(%v).\r\n", downServer.nodeID, time.Now().Format(format_time)))
	downServer.raft.Stop()
	waitElect(newServers, 1, w)
	downServer = createRaftServer(downServer.nodeID, 0, 0, peers, true, false, 1)
	newServers = append(newServers, downServer)
	leaderServer = waitElect(newServers, 1, w)
	return
}

func TestLinearWithDelLeader(t *testing.T) {
	servers := initTestServer(peers, true, false, 1)
	f, w := getLogFile("", "linearWithDelLeader.log")
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
			fmt.Println("put data err: ", err)
			w.WriteString(fmt.Sprintf("put data err[%v] at(%v).\r\n", err, time.Now().Format(format_time)))
		}
	}(dataLen)

	// delete raft leader server and add
	leadServer, servers = delAndAddLeader(servers, w, t)
	startIndex := verifyRestoreValue(servers, leadServer, w)
	fmt.Println("start put data")
	if _, err := leadServer.putData(1, startIndex, PutDataStep/5, w); err != nil {
		t.Fatal(err)
	}
	compareServersWithLeader(servers, w, t)
	printStatus(servers, w)
}

func delAndAddLeader(servers []*testServer, w *bufio.Writer, t *testing.T) (leadServer *testServer, newServers []*testServer) {
	time.Sleep(5 * time.Second)
	fmt.Println("delete raft leader server and add a member")
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
	delServer = leadServer.addMember(delServer.nodeID, w, t)
	newServers = append(newServers, delServer)
	leadServer = waitElect(newServers, 1, w)
	return
}

func TestDelLeaderWithTryLeader(t *testing.T) {
	//todo try to leader, and delete pre leader
}

func TestReadIndex(t *testing.T) {
	//todo
}

func TestParalPutData(t *testing.T) {
	// todo
}
