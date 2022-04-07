package main

import (
	"bufio"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

const (
	defaultKeysize = 10000
)

func verifyElect(ts []*testServer, w *bufio.Writer) *testServer {
	w.WriteString("waiting electing leader....")
	time.Sleep(time.Duration(int64(htbTick) * tickInterval.Nanoseconds()))

	defer w.Flush()

	var term uint64
	var leader *testServer
	for {
	redo:
		term = 0
		leader = nil
		var leaderID uint64
		for _, s := range ts {
			if s.raft.IsLeader(1) {
				leaderID, term = s.raft.LeaderTerm(1)
				leader = s
				break
			}
		}
		if leader == nil {
			w.WriteString("waiting electing....")
			goto redo
		}
		for _, s := range ts {
			l, t := s.raft.LeaderTerm(1)
			if l != leaderID || t != term {
				w.WriteString("waiting electing....")
				goto redo
			}
		}
		w.WriteString(fmt.Sprintf("[%s] elected leader: %v\r\n", time.Now().Format(format_time), leader.nodeID))
		return leader
	}
}

// return existed length of data
func (leadServer *testServer) putData(raftId uint64, startIndex, keysize int, w *bufio.Writer) (int, error) {
	w.WriteString(fmt.Sprintf("Starting put data at(%v).\r\n", time.Now().Format(format_time)))
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
	w.WriteString(fmt.Sprintf("End put data at(%v).\r\n", time.Now().Format(format_time)))
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
	w.WriteString(fmt.Sprintf("Starting put data with retry at(%v).\r\n", time.Now().Format(format_time)))
	lastKey, lastVal := NoCheckLinear, NoCheckLinear
	for i := startIndex; i < startIndex+keysize; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(i) + "_val"
		for {
			done := false
			for _, server := range servers {
				if err := server.sm[1].Put(k, v, lastKey, lastVal); err != nil {
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
	w.WriteString(fmt.Sprintf("End put data at(%v).\r\n", time.Now().Format(format_time)))
}

func (leadServer *testServer) putDataParallel(raftId uint64, startIndex, keysize, paral int, w *bufio.Writer) error {
	w.WriteString(fmt.Sprintf("Starting parallel put data at(%v).\r\n", time.Now().Format(format_time)))
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
					w.WriteString(fmt.Sprintf("put data err: %v", err))
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	w.WriteString(fmt.Sprintf("End parallel put data at(%v).\r\n", time.Now().Format(format_time)))
	return nil
}

func (leadServer *testServer) addMember(nodeId uint64, w *bufio.Writer, t *testing.T) (newServer *testServer) {
	w.WriteString(fmt.Sprintf("[%s] Add new node[%v] \r\n", time.Now().Format(format_time), nodeId))
	leader, term := leadServer.raft.LeaderTerm(1)
	newServer = createRaftServer(nodeId, leader, term, peers, true, true, 1)
	// add node
	resolver.addNode(nodeId, 0)
	err := leadServer.sm[1].AddNode(proto.Peer{ID: nodeId})
	if err != nil {
		t.Fatalf("add member[%v] err: %v", nodeId, err)
	}
	fmt.Println("added node")
	time.Sleep(time.Second)
	return
}

func (leadServer *testServer) deleteMember(server *testServer, w *bufio.Writer, t *testing.T) {
	w.WriteString(fmt.Sprintf("[%s] delete node[%v] \r\n", time.Now().Format(format_time), server.nodeID))
	// delete node
	err := leadServer.sm[1].RemoveNode(proto.Peer{ID: server.nodeID})
	if err != nil {
		t.Fatalf("delete member[%v] err: %v", server.nodeID, err)
	}
	server.raft.Stop()
	resolver.delNode(server.nodeID)
	fmt.Println("deleted node")
	time.Sleep(time.Second)
	return
}

func compareServersWithLeader(servers []*testServer, w *bufio.Writer, t *testing.T) {
	leadServer := waitElect(servers, 1, w)
	for _, s := range servers {
		if s == leadServer {
			continue
		}
		compareTwoServers(leadServer, s, w, t)
	}
}

func compareTwoServers(leadServer *testServer, followServer *testServer, w *bufio.Writer, t *testing.T) {
	compareApply(leadServer, followServer, w, t)

	w.WriteString(fmt.Sprintf("Starting verify repl data at(%v).\r\n", time.Now().Format(format_time)))
	defer w.Flush()
	for k, v := range leadServer.sm[1].data {
		if vget, err := followServer.sm[1].Get(k); err != nil || vget != v {
			t.Fatalf("verify repl error:put and get not match [%v %v %v].\r\n", k, vget, v)
		}
	}
	w.WriteString(fmt.Sprintf("End verify repl data at(%v).\r\n", time.Now().Format(format_time)))
}

func compareApply(leadServer *testServer, followServer *testServer, w *bufio.Writer, t *testing.T) {
	w.WriteString(fmt.Sprintf("Waiting repl at(%v).\r\n", time.Now().Format(format_time)))
	for {
		a := leadServer.raft.AppliedIndex(1)
		b := followServer.raft.AppliedIndex(1)
		if a == b {
			break
		}
	}
	w.WriteString(fmt.Sprintf("End repl at(%v).\r\n", time.Now().Format(format_time)))
	w.Flush()
}

func verifyRestoreValue(servers []*testServer, leader *testServer, w *bufio.Writer) int {
	// verify load
	leader.sm[1].RLock()
	leadLen := len(leader.sm[1].data)
	leader.sm[1].RUnlock()
	for _, s := range servers {
		li, _ := s.store[1].LastIndex()
		w.WriteString(fmt.Sprintf("node(%v) raft(%v) synced raftlog keysize(%v) lastIndex(%v).\r\n", s.nodeID, 1, len(s.sm[1].data), li))
		s.sm[1].Lock()
		for i := 0; i < len(s.sm[1].data); i++ {
			if s.sm[1].data[strconv.Itoa(i)] != strconv.Itoa(i)+"_val" {
				panic(fmt.Errorf("node(%v) raft(%v) load raftlog Key(%v) error: value(%v), expected(%v).\r\n",
					s.nodeID, 1, i, s.sm[1].data[strconv.Itoa(i)], strconv.Itoa(i)+"_val"))
			}
		}
		if len(s.sm[1].data) != leadLen {
			w.WriteString(fmt.Sprintf("[WARN] node[%v] data length[%v] is different from leader[%v] data length[%v]", s.nodeID, len(s.sm[1].data), leader.nodeID, leadLen))
		}
		s.sm[1].Unlock()
	}
	w.Flush()
	return leadLen
}

func waitElect(ts []*testServer, raftId uint64, w *bufio.Writer) *testServer {
	w.WriteString("waiting electing leader....")
	time.Sleep(time.Duration(int64(htbTick) * tickInterval.Nanoseconds()))

	defer w.Flush()

	var term uint64
	var leader *testServer
	for {
	redo:
		term = 0
		leader = nil
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
			w.WriteString(fmt.Sprintf("[%s] elected leader: %v\r\n", time.Now().Format(format_time), leader.nodeID))
			return leader
		}
	}
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

func printSubmitTime(raftId uint64, step, paral int, w *bufio.Writer) {
	subtime := subTimeMap[raftId]
	defer w.Flush()
	w.WriteString(fmt.Sprintf("submit time: raftId [%v], step [%v], paral [%v], min [%v]us, max [%v]us, avg [%v]us, subCount[%v]\r\n",
		raftId, step, paral, subtime.minSubTime, subtime.maxSubTime, subtime.totalSubTime/subtime.subCount, subtime.subCount))
	subtime.minSubTime = math.MaxInt64
	subtime.maxSubTime = 0
	subtime.subCount = 0
	subtime.totalSubTime = 0
}
