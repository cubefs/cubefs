package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tiglabs/raft"

	"github.com/tiglabs/raft/proto"
)

const (
	defaultKeySize = 10000
)

func printLog(w *bufio.Writer, str string) {
	w.WriteString(fmt.Sprintf("[%v] %s \r\n", time.Now().Format(format_time), str))
}

// return existed length of data
func (leadServer *testServer) putData(raftId uint64, startIndex, keysize int, w *bufio.Writer) (nextIndex int, err error) {
	output("putData: startIndex(%v) keysize(%v)", startIndex, keysize)
	printLog(w, fmt.Sprintf("Starting put data startIndex(%v) keysize(%v).", startIndex, keysize))
	defer w.Flush()
	lastKey, lastVal := NoCheckLinear, NoCheckLinear
	for i := startIndex; i < startIndex+keysize; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(i) + "_val"
		if err := leadServer.sm[raftId].Put(k, v, lastKey, lastVal); err != nil {
			return i, fmt.Errorf("put data err: %v", err)
		}
		if vget, err := leadServer.sm[raftId].Get(k); err != nil || vget != v {
			return i, fmt.Errorf("get Value err: %v", err)
		}
		lastKey, lastVal = k, v
	}
	printLog(w, "End put data.")
	return startIndex + keysize, nil
}

func (leadServer *testServer) putDataOnly(raftId uint64, startIndex, keysize int) error {
	lastKey, lastVal := NoCheckLinear, NoCheckLinear
	for i := startIndex; i < startIndex+keysize; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(i) + "_val"
		if err := leadServer.sm[raftId].Put(k, v, lastKey, lastVal); err != nil {
			return fmt.Errorf("put data err: %v", err)
		}
		lastKey, lastVal = k, v
	}
	return nil
}

func (leadServer *testServer) putOneBigData(raftId uint64, bitSize int) error {
	if err := leadServer.sm[raftId].constructBigData(bitSize); err != nil {
		return fmt.Errorf("put data err: %v", err)
	}
	return nil
}

func (leadServer *testServer) putDataAsync(raftId uint64, startIndex, keysize int, w *bufio.Writer) (nextIndex int, futs map[int]*raft.Future, err error) {
	output("putDataAsync: startIndex(%v) keysize(%v)", startIndex, keysize)
	printLog(w, fmt.Sprintf("Starting put data async startIndex(%v) keysize(%v).", startIndex, keysize))
	defer w.Flush()
	lastKey, lastVal := NoCheckLinear, NoCheckLinear
	futs = make(map[int]*raft.Future)
	for i := startIndex; i < startIndex+keysize; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(i) + "_val"
		future, err := leadServer.sm[raftId].PutAsync(k, v, lastKey, lastVal)
		if err != nil {
			return i, futs, fmt.Errorf("put data err: %v", err)
		}
		futs[i] = future
		lastKey, lastVal = k, v
	}
	printLog(w, "End put data async.")
	return startIndex + keysize, futs, nil
}

type resultTest struct {
	sucOp      int64
	failOp     int64
	totalCount int64
	err        error
}

func (leadServer *testServer) localPutBigData(raftId uint64, bitSize int, exeMin int, goroutingNumber int) (error, string) {
	var rst string
	//var err error
	var totalCount, totalSucOp, totalFailOp int64
	results := make([]resultTest, goroutingNumber)
	var wg sync.WaitGroup
	sec := exeMin * 60
	start := time.Now()
	for i := 0; i < goroutingNumber; i++ {
		wg.Add(1)
		go func(rest *resultTest) {
			leadServer.sm[raftId].localConstructBigData(bitSize, exeMin, rest)
			wg.Done()
		}(&results[i])
	}
	wg.Wait()
	end := time.Now()
	for i := 0; i < goroutingNumber; i++ {
		/*
			err = resuts[i].err
			if err != nil {
				return fmt.Errorf("put data err: %v", err), nil

			}
		*/
		totalCount += results[i].totalCount
		totalSucOp += results[i].sucOp
		totalFailOp += results[i].failOp
	}
	rst = fmt.Sprintf("local put bigsubmit: start-%v, end-%v; size-%d, executeTime-%dmin, success-%d, fail-%d, tps-%d",
		start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"), bitSize, exeMin, totalSucOp, totalFailOp, totalSucOp/int64(sec))

	return nil, rst
}

func putDataWithRetry(servers []*testServer, startIndex int, keysize int, w *bufio.Writer, t *testing.T) {
	printLog(w, fmt.Sprintf("Starting put data with retry: startIndex(%v) keysize(%v)", startIndex, keysize))
	lastKey, lastVal := NoCheckLinear, NoCheckLinear
	for i := startIndex; i < startIndex+keysize; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(i) + "_val"
		for {
			done := false
			for _, server := range servers {
				if err := server.sm[1].Put(k, v, lastKey, lastVal); err != nil && !strings.Contains(err.Error(), "is exist") {
					continue
				}
				if vget, err := server.sm[1].Get(k); err != nil || vget != v {
					t.Errorf("get Value err: %v", err)
				}
				done = true
				break
			}
			if done {
				break
			}
		}
		lastKey, lastVal = k, v
	}
	printLog(w, "End put data with retry.")
}

func (leadServer *testServer) putDataParallel(raftId uint64, startIndex, keysize, paral int, w *bufio.Writer) error {
	printLog(w, fmt.Sprintf("Starting parallel put data: startIndex(%v) keysize(%v)", startIndex, keysize))
	lastKey, lastVal := NoCheckLinear, NoCheckLinear
	eachSize := keysize / paral
	var wg sync.WaitGroup
	wg.Add(paral)
	for i := startIndex; i < startIndex+keysize; i += eachSize {
		go func(start int) {
			for j := start; j < start+eachSize; j++ {
				k := strconv.Itoa(j)
				v := strconv.Itoa(j) + "_val"
				if err := leadServer.sm[raftId].Put(k, v, lastKey, lastVal); err != nil {
					printLog(w, fmt.Sprintf("put data err: (%v)", err))
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	printLog(w, "End parallel put data.")
	return nil
}

func (leadServer *testServer) addMember(nodeId uint64, mode raft.ConsistencyMode, w *bufio.Writer, t *testing.T) (newServer *testServer) {
	printLog(w, fmt.Sprintf("Add new node[%v]", nodeId))
	leader, term := leadServer.raft.LeaderTerm(1)
	raftConfig := &raft.RaftConfig{Peers: peers, Leader: leader, Term: term, Mode: mode}
	newServer = createRaftServer(nodeId, true, true, 1, raftConfig)
	// add node
	resolver.addNode(nodeId, 0)
	err := leadServer.sm[1].AddNode(proto.Peer{ID: nodeId})
	if err != nil {
		t.Fatalf("add member[%v] err: %v", nodeId, err)
	}
	output("added node")
	printLog(w, fmt.Sprintf("Add new node[%v] done", nodeId))
	time.Sleep(time.Second)
	return
}

func (leadServer *testServer) deleteMember(server *testServer, w *bufio.Writer, t *testing.T) {
	printLog(w, fmt.Sprintf("Delete node[%v]", server.nodeID))
	// delete node
	err := leadServer.sm[1].RemoveNode(proto.Peer{ID: server.nodeID})
	if err != nil {
		t.Fatalf("delete member[%v] err: %v", server.nodeID, err)
	}
	server.raft.Stop()
	resolver.delNode(server.nodeID)
	printLog(w, fmt.Sprintf("Delete node[%v] done", server.nodeID))
	output("deleted node")
	time.Sleep(time.Second)
	return
}

func compareServersWithLeader(servers []*testServer, w *bufio.Writer, t *testing.T) {
	output("compare servers with leader")
	leadServer := waitElect(servers, 1, w)
	for _, s := range servers {
		if s == leadServer {
			continue
		}
		compareTwoServers(leadServer, s, w, t)
	}
}

func compareTwoServers(leadServer *testServer, followServer *testServer, w *bufio.Writer, t *testing.T) {
	compareIndex(leadServer, followServer, w)

	printLog(w, fmt.Sprintf("Starting verify data between follower(%v) and leader(%v).", followServer.nodeID, leadServer.nodeID))
	defer w.Flush()
	for k, v := range leadServer.sm[1].data {
		if vget, err := followServer.sm[1].Get(k); err != nil || vget != v {
			printLog(w, fmt.Sprintf("inconsistent value: key(%v) leader value(%v) follower value(%v).", k, v, vget))
			t.Fatalf("verify repl error:put and get not match [%v %v %v].\r\n", k, vget, v)
		}
	}
	printLog(w, fmt.Sprintf("End verify data between follower(%v) and leader(%v).", followServer.nodeID, leadServer.nodeID))
}

func compareIndex(leadServer *testServer, followServer *testServer, w *bufio.Writer) {
	printLog(w, fmt.Sprintf("Starting wait apply between follower(%v) and leader(%v).", followServer.nodeID, leadServer.nodeID))
	for {
		time.Sleep(tickInterval)
		if leadServer.raft.AppliedIndex(1) != followServer.raft.AppliedIndex(1) {
			continue
		}
		if leadServer.raft.CommittedIndex(1) != followServer.raft.CommittedIndex(1) {
			continue
		}
		li1, _ := leadServer.store[1].LastIndex()
		li2, _ := followServer.store[1].LastIndex()
		if li1 == 0 || li2 == 0 || li1 != li2 {
			continue
		}
		break
	}
	printLog(w, fmt.Sprintf("End wait apply between follower(%v) and leader(%v).", followServer.nodeID, leadServer.nodeID))
	w.Flush()
}

func verifyRestoreValue(servers []*testServer, leader *testServer, w *bufio.Writer) int {
	// verify load
	printLog(w, "Starting verify value.")

	leader.sm[1].RLock()
	leadLen := len(leader.sm[1].data)
	leader.sm[1].RUnlock()
	for _, s := range servers {
		li, _ := s.store[1].LastIndex()
		s.sm[1].Lock()
		printLog(w, fmt.Sprintf("node(%v) raft(%v) synced raftlog keysize(%v) lastIndex(%v) commitID(%v) appliedID(%v).",
			s.nodeID, 1, len(s.sm[1].data), li, s.raft.CommittedIndex(1), s.raft.AppliedIndex(1)))
		for i := 0; i < len(s.sm[1].data); i++ {
			if s.sm[1].data[strconv.Itoa(i)] != strconv.Itoa(i)+"_val" {
				printLog(w, fmt.Sprintf("node(%v) raft(%v) load raftlog Key(%v) error: value(%v), expected(%v).", s.nodeID, 1, i, s.sm[1].data[strconv.Itoa(i)], strconv.Itoa(i)+"_val"))
				panic(fmt.Errorf("node(%v) raft(%v) load raftlog Key(%v) error: value(%v), expected(%v).\r\n",
					s.nodeID, 1, i, s.sm[1].data[strconv.Itoa(i)], strconv.Itoa(i)+"_val"))
			}
		}
		if len(s.sm[1].data) != leadLen {
			errStr := fmt.Sprintf("[WARN] node[%v] data length[%v] is different from leader[%v] data length[%v]", s.nodeID, len(s.sm[1].data), leader.nodeID, leadLen)
			printLog(w, errStr)
			printStatus(servers, w)
			panic(errStr)
		}
		s.sm[1].Unlock()
	}
	printLog(w, "End verify value.")
	w.Flush()
	return leadLen
}

func verifyStrictRestoreValue(dataLen int, servers []*testServer, w *bufio.Writer) error {
	printLog(w, "Starting strictly verify value.")
	output("verifyStrictRestoreValue start: max(%v)", dataLen)
	for _, s := range servers {
		li, _ := s.store[1].LastIndex()
		s.sm[1].Lock()
		printLog(w, fmt.Sprintf("node(%v) raft(%v) synced raftlog keysize(%v) lastIndex(%v) commitID(%v) appliedID(%v).",
			s.nodeID, 1, len(s.sm[1].data), li, s.raft.CommittedIndex(1), s.raft.AppliedIndex(1)))
		for i := 0; i < len(s.sm[1].data); i++ {
			if s.sm[1].data[strconv.Itoa(i)] != strconv.Itoa(i)+"_val" {
				s.sm[1].Unlock()
				errStr := fmt.Sprintf("node(%v) raft(%v) load raftlog Key(%v) error: value(%v), expected(%v)", s.nodeID, 1, i, s.sm[1].data[strconv.Itoa(i)], strconv.Itoa(i)+"_val")
				printLog(w, errStr)
				return fmt.Errorf(errStr)
			}
		}
		if len(s.sm[1].data) != dataLen {
			s.sm[1].Unlock()
			errStr := fmt.Sprintf("node(%v) data length(%v) is different from length(%v)", s.nodeID, len(s.sm[1].data), dataLen)
			printLog(w, errStr)
			return fmt.Errorf(errStr)
		}
		s.sm[1].Unlock()
	}
	printLog(w, "End strictly verify value.")
	w.Flush()
	return nil
}

func verifyRollbackValue(rollbackStart, rollbackEnd int, servers []*testServer, w *bufio.Writer) error {
	printLog(w, "Starting verify rollback value.")
	output("verifyRollbackValue start: (%v)~(%v)", rollbackStart, rollbackEnd)
	for _, s := range servers {
		for i := rollbackStart; i < rollbackEnd; i++ {
			value, err := s.sm[1].GetRollback(strconv.Itoa(i))
			if err != nil || value != strconv.Itoa(i)+"_val" {
				errStr := fmt.Sprintf("node(%v) raft(%v) load rollback Key(%v) error: value(%v), expected(%v)", s.nodeID, 1, i, s.sm[1].data[strconv.Itoa(i)], strconv.Itoa(i)+"_val")
				printLog(w, errStr)
				return fmt.Errorf(errStr)
			}
		}
		_, rollbackLen := s.sm[1].GetLen()
		if rollbackLen != (rollbackEnd - rollbackStart) {
			errStr := fmt.Sprintf("node(%v) rollback data length(%v) is different from length(%v)", s.nodeID, rollbackLen, rollbackEnd-rollbackStart)
			printLog(w, errStr)
			return fmt.Errorf(errStr)
		}
	}
	printLog(w, "End verify rollback value.")
	w.Flush()
	return nil
}

func waitForApply(ts []*testServer, raftId uint64, w *bufio.Writer) {
	printLog(w, "waiting apply....")
	defer w.Flush()

	var appliedIndex, commitIndex, lastIndex uint64

redo:
	printStatus(ts, w)
	appliedIndex = 0
	commitIndex = 0
	lastIndex = 0
	for _, s := range ts {
		ai := s.raft.AppliedIndex(raftId)
		ci := s.raft.CommittedIndex(raftId)
		li, _ := s.store[raftId].LastIndex()
		if li != ci || li != ai || (appliedIndex != 0 && appliedIndex != ai) || (commitIndex != 0 && commitIndex != ci) || (lastIndex != 0 && lastIndex != li) {
			goto redo
		}
		appliedIndex = ai
		commitIndex = ci
		lastIndex = li
	}

	printLog(w, fmt.Sprintf("waiting apply done: applyIndex(%v) commitIndex(%v) lastIndex(%v)", appliedIndex, commitIndex, lastIndex))
}

func waitElect(ts []*testServer, raftId uint64, w *bufio.Writer) *testServer {
	printLog(w, "waiting electing leader....")
	time.Sleep(time.Duration(int64(htbTick) * tickInterval.Nanoseconds()))

	defer w.Flush()

	var term uint64
	var leader *testServer
	for {
	redo:
		term = 0
		leader = nil
		// 保证三副本的leader和term一致，并且返回当前的leader
		for _, s := range ts {
			l, t := s.raft.LeaderTerm(raftId)
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
			printLog(w, fmt.Sprintf("elected leader: %v", leader.nodeID))
			return leader
		}
	}
}

func restartAllServers(ts []*testServer, leader, term uint64, clear bool) []*testServer {
	output("restart all raft servers")
	ret := make([]*testServer, 0)
	for _, s := range ts {
		raftConfig := &raft.RaftConfig{Peers: s.peers, Leader: leader, Term: term, Mode: s.mode}
		ns := createRaftServer(s.nodeID, s.isLease, clear, 1, raftConfig)
		ret = append(ret, ns)
	}
	return ret
}

func restartLeader(servers []*testServer, w *bufio.Writer) (leaderServer *testServer, newServers []*testServer) {
	output("stop and restart raft leader server")
	printLog(w, "stop and restart raft leader server")
	var downServer *testServer
	downServer, _, newServers = stopLeader(servers, w, false)
	time.Sleep(1 * time.Second)
	leaderServer, newServers = startServer(newServers, downServer, w)
	return
}

func startServer(servers []*testServer, newServer *testServer, w *bufio.Writer) (leaderServer *testServer, newServers []*testServer) {
	output("start raft server: %v", newServer.nodeID)
	raftConfig := &raft.RaftConfig{Peers: peers, Leader: 0, Term: 0, Mode: newServer.mode}
	newServer = createRaftServer(newServer.nodeID, true, false, 1, raftConfig)
	printLog(w, fmt.Sprintf("start raft server(%v)", newServer.nodeID))
	newServers = append(servers, newServer)
	leaderServer = waitElect(newServers, 1, w)
	return
}

func stopLeader(servers []*testServer, w *bufio.Writer, elect bool) (downServer, newLeader *testServer, newServers []*testServer) {
	output("stop raft leader server")
	newServers = make([]*testServer, 0)
	for _, s := range servers {
		l, _ := s.raft.LeaderTerm(1)
		if l == s.nodeID && downServer == nil {
			downServer = s
			continue
		}
		newServers = append(newServers, s)
	}
	downServer.raft.Stop()
	printLog(w, fmt.Sprintf("stop raft leader server(%v)", downServer.nodeID))
	if elect {
		newLeader = waitElect(newServers, 1, w)
	}
	return
}

func stopNFollowers(nFollowerDown int, servers []*testServer, w *bufio.Writer) (downServers, newServers []*testServer) {
	output("stop raft follower server")
	newServers = make([]*testServer, 0)
	downServers = make([]*testServer, 0)
	for _, s := range servers {
		l, _ := s.raft.LeaderTerm(1)
		if l != s.nodeID && len(downServers) < nFollowerDown {
			downServers = append(downServers, s)
			continue
		}
		newServers = append(newServers, s)
	}
	for _, downServer := range downServers {
		downServer.raft.Stop()
		printLog(w, fmt.Sprintf("stop raft server(%v)", downServer.nodeID))
	}
	return
}

func printStatus(ts []*testServer, w *bufio.Writer) {
	defer w.Flush()

	time.Sleep(time.Second)
	for _, s := range ts {
		printLog(w, s.raft.Status(1).String())
	}
}

func tryToLeader(nodeID uint64, servers []*testServer, w *bufio.Writer) (leadServer *testServer) {
	output("start try to leader: %v", nodeID)
	printLog(w, fmt.Sprintf("try leader(%v) start", nodeID))
	for {
		servers[nodeID-1].raft.TryToLeader(1)
		leadServer = waitElect(servers, 1, w)
		if leadServer.nodeID == nodeID {
			break
		}
	}
	printLog(w, fmt.Sprintf("try leader(%v) done", nodeID))
	return leadServer
}

func getRaftIndex(s *testServer, raftId uint64) (ai, ci, li uint64) {
	ai = s.raft.AppliedIndex(raftId)
	ci = s.raft.CommittedIndex(raftId)
	li, _ = s.store[raftId].LastIndex()
	return
}
