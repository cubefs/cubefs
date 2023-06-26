package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util"
)

const (
	F1_No_entry      = "1F_no_entry"
	F2_No_entry      = "2F_no_entry_1"
	F3_No_entry      = "2F_no_entry_2"
	F1_Match_Less_Li = "1F_match_less_li"
	F2_Match_Less_Li = "2F_match_less_li"
	F1_Ci_Less_Li    = "1F_ci_less_li"
	F2_Ci_Less_Li    = "2F_ci_less_li"
	Mix_Li_Ci        = "mix_li_ci"
)

type raftTestRules struct {
	name      string
	msgFilter raft.MsgFilterFunc
}

var raftTestCases = map[string]raftTestRules{
	F1_No_entry: {
		/* e.g.
			 L    F    F
		li  200  200  50
		ci   50   50  50
		ai	 50   50  50
		*/
		name: F1_No_entry,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && msg.To == 3 && msg.Index > 50 {
				return true
			}
			return false
		},
	},

	F2_No_entry: {
		/* e.g.
			 L    F    F
		li  200  140  50
		ci   50   50  50
		ai	 50   50  50
		*/
		name: F2_No_entry,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && msg.To == 2 && msg.Index > 140 {
				return true
			}
			if msg.Type == proto.ReqMsgAppend && msg.To == 3 && msg.Index > 50 {
				return true
			}
			return false
		},
	},

	F3_No_entry: {
		/* e.g.
			 L    F    F
		li  200   50   50
		ci   50   50   50
		ai	 50   50   50
		*/
		name: F3_No_entry,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && msg.Index > 50 {
				return true
			}
			return false
		},
	},

	F1_Match_Less_Li: {
		/* e.g.
			 L     F    F
		li  200  200  200
		ci   90   90   90
		ai	 90   90   90
		*/
		name: F1_Match_Less_Li,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.RespMsgAppend && msg.From == 3 && msg.Index > 90 {
				msg.Index = 90
			}
			return false
		},
	},

	F2_Match_Less_Li: {
		/* e.g.
			 L     F    F
		li  200  200  200
		ci   90   90   90
		ai	 90   90   90
		*/
		name: F2_Match_Less_Li,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.RespMsgAppend && msg.From == 2 && msg.Index > 140 {
				msg.Index = 140
			}
			if msg.Type == proto.RespMsgAppend && msg.From == 3 && msg.Index > 90 {
				msg.Index = 90
			}
			return false
		},
	},

	F1_Ci_Less_Li: {
		/* e.g.
			 L    F    F
		li  200  200  200
		ci  200  200   90
		ai	200  200   90
		*/
		name: F1_Ci_Less_Li,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && msg.To == 3 && msg.Commit > 90 {
				msg.Commit = 90
			}
			return false
		},
	},

	F2_Ci_Less_Li: {
		/* e.g.
			 L    F    F
		li  200  200  200
		ci  200  140   90
		ai	200  140   90
		*/
		name: F2_Ci_Less_Li,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && msg.To == 2 && msg.Commit > 140 {
				msg.Commit = 140
			}
			if msg.Type == proto.ReqMsgAppend && msg.To == 3 && msg.Commit > 90 {
				msg.Commit = 90
			}
			return false
		},
	},

	Mix_Li_Ci: {
		/* e.g.
			 L    F    F
		li  200  140   50
		ci   40   20   30
		ai	 40   20   30
		*/
		name: Mix_Li_Ci,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && msg.To == 2 && msg.Index > 140 {
				return true
			}
			if msg.Type == proto.ReqMsgAppend && msg.To == 3 && msg.Index > 50 {
				return true
			}
			if msg.Type == proto.RespMsgAppend && msg.From == 3 && msg.Index > 40 {
				msg.Index = 40
			}
			if msg.Type == proto.ReqMsgAppend && msg.To == 2 && msg.Commit > 20 {
				msg.Commit = 20
			}
			if msg.Type == proto.ReqMsgAppend && msg.To == 3 && msg.Commit > 30 {
				msg.Commit = 30
			}
			return false
		},
	},
}

func TestStrictMode_FilterMsg_FollowerDown(t *testing.T) {
	followerDownTests := []string{
		F1_No_entry,
		F2_No_entry,
		F3_No_entry,
		F1_Match_Less_Li,
		F2_Match_Less_Li,
		Mix_Li_Ci,
	}
	for _, name := range followerDownTests {
		tt := raftTestCases[name]
		t.Run(tt.name, func(t *testing.T) {
			strictMode_filterMsgs_rollback_followerDown(t, tt.name, tt.msgFilter, 1)
			strictMode_filterMsgs_rollback_followerDown(t, tt.name, tt.msgFilter, 2)
		})
	}
}

func TestStrictMode_FilterMsg_LeaderDown_Rollback(t *testing.T) {
	leaderDownRollbackTests := []string{
		F1_No_entry,
		F2_No_entry,
		Mix_Li_Ci,
	}
	for _, name := range leaderDownRollbackTests {
		tt := raftTestCases[name]
		t.Run(tt.name, func(t *testing.T) {
			strictMode_filterMsgs_rollback_leaderDown(t, tt.name, tt.msgFilter, true)
		})
	}
}

func TestStrictMode_FilterMsg_LeaderDown_NoRollback(t *testing.T) {
	leaderDownNoRollbackTests := []string{
		F3_No_entry,
		F1_Match_Less_Li,
		F2_Match_Less_Li,
		F1_Ci_Less_Li,
		F2_Ci_Less_Li,
	}
	for _, name := range leaderDownNoRollbackTests {
		tt := raftTestCases[name]
		t.Run(tt.name, func(t *testing.T) {
			strictMode_filterMsgs_rollback_leaderDown(t, tt.name, tt.msgFilter, false)
		})
	}
}

func strictMode_filterMsgs_rollback_followerDown(t *testing.T, name string, msgFilter raft.MsgFilterFunc, downFollowerNum int) {
	putDataStep := 200

	servers := initTestServerWithMsgFilter(peers, true, true, 1, raft.StrictMode, msgFilter)
	f, w := getLogFile("", fmt.Sprintf("TestStrictMode_FilterMsgs_%v_%vFollowerDown.log", name, downFollowerNum))
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
	}()

	leadServer := tryToLeader(1, servers, w)
	dataLen := verifyRestoreValue(servers, leadServer, w)
	printStatus(servers, w)

	var (
		futs        map[int]*raft.Future
		err         error
		wg          sync.WaitGroup
		downServers []*testServer
	)

	wg.Add(1)
	go func(startIndex int) {
		defer wg.Done()
		_, futs, err = leadServer.putDataAsync(1, startIndex, putDataStep, w)
	}(dataLen)
	wg.Wait()

	startIndex := dataLen
	dataLen += putDataStep
	if err != nil {
		output("put data err: %v", err)
		msg := fmt.Sprintf("put data maxKey(%v) err(%v) future len(%v).", dataLen-1, err, len(futs))
		printLog(w, msg)
		t.Fatalf(msg)
	}
	time.Sleep(5 * time.Second)
	printStatus(servers, w)

	// check index
	lai, _, lli := getRaftIndex(leadServer, 1)
	failedCount := int(lli - lai)
	assert.Greater(t, failedCount, 0, "failed count must greater than 0")

	// stop servers
	downServers, servers = stopNFollowers(downFollowerNum, servers, w)
	time.Sleep(1 * time.Second)

	for _, s := range servers {
		s.raft.SetMsgFilterFunc(1, raft.DefaultNoMsgFilter)
	}
	//waitForApply(servers, 1, w)

	// check response
	failedIndex := 0
	for key := startIndex; key < dataLen; key++ {
		future, exist := futs[key]
		assert.Equal(t, true, exist, fmt.Sprintf("key(%v) is not exist", key))
		_, err := future.Response()
		//output("time(%v) key(%v) err(%v)", time.Now().Format(format_time), key, err)
		if failedIndex > 0 {
			assert.NotEqual(t, nil, err, fmt.Sprintf("key(%v) should fail", key))
		} else if err != nil && failedIndex == 0 {
			failedIndex = key
		} else {
			assert.Equal(t, nil, err, fmt.Sprintf("key(%v) should success", key))
		}
	}
	assert.Greater(t, failedIndex, 0, "failedIndex must greater than 0")
	assert.Equal(t, failedCount, dataLen-failedIndex, "failed count")

	if downFollowerNum < 2 {
		waitForApply(servers, 1, w)

		// check data and roll back
		err = verifyStrictRestoreValue(failedIndex, servers, w)
		assert.Equal(t, nil, err, "verify data is inconsistent")

		err = verifyRollbackValue(failedIndex, dataLen, servers, w)
		assert.Equal(t, nil, err, "verify roll back data is inconsistent")
	}

	// restart down servers
	for _, downServer := range downServers {
		_, servers = startServer(servers, downServer, w)
	}
	waitForApply(servers, 1, w)

	// check data and roll back
	err = verifyStrictRestoreValue(failedIndex, servers, w)
	assert.Equal(t, nil, err, "verify data is inconsistent")

	err = verifyRollbackValue(failedIndex, dataLen, servers, w)
	assert.Equal(t, nil, err, "verify roll back data is inconsistent")
}

func strictMode_filterMsgs_rollback_leaderDown(t *testing.T, name string, msgFilter raft.MsgFilterFunc, rollback bool) {
	putDataStep := 200

	servers := initTestServerWithMsgFilter(peers, true, true, 1, raft.StrictMode, msgFilter)
	f, w := getLogFile("", fmt.Sprintf("TestStrictMode_FilterMsgs_%v_LeaderDown_rb_%v.log", name, rollback))
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
	}()

	leadServer := tryToLeader(1, servers, w)
	dataLen := verifyRestoreValue(servers, leadServer, w)
	printStatus(servers, w)

	var (
		futs       map[int]*raft.Future
		err        error
		wg         sync.WaitGroup
		downServer *testServer
	)

	wg.Add(1)
	go func(startIndex int) {
		defer wg.Done()
		_, futs, err = leadServer.putDataAsync(1, startIndex, putDataStep, w)
	}(dataLen)
	wg.Wait()

	dataLen += putDataStep
	if err != nil {
		output("put data err: %v", err)
		msg := fmt.Sprintf("put data maxKey(%v) err(%v) future len(%v).", dataLen-1, err, len(futs))
		printLog(w, msg)
		t.Fatalf(msg)
	}
	time.Sleep(5 * time.Second)
	printStatus(servers, w)

	// get rollback index
	_, _, minLi := getRaftIndex(leadServer, 1)
	maxLi := uint64(0)
	for _, s := range servers {
		if s.nodeID == leadServer.nodeID {
			continue
		}
		_, _, lli := getRaftIndex(s, 1)
		maxLi = util.Max(lli, maxLi)
		minLi = util.Min(lli, minLi)
	}
	rollbackCount := int(maxLi - minLi)
	if rollback {
		assert.Greater(t, rollbackCount, 0, "rollback count must greater than 0")
	}

	// stop leader
	downServer, leadServer, servers = stopLeader(servers, w, true)
	time.Sleep(1 * time.Second)
	printStatus(servers, w)

	// restart leader
	leadServer, servers = restartLeader(servers, w)
	time.Sleep(1 * time.Second)

	for _, s := range servers {
		s.raft.SetMsgFilterFunc(1, raft.DefaultNoMsgFilter)
	}

	waitForApply(servers, 1, w)
	// check roll back
	failedIndex := verifyRestoreValue(servers, leadServer, w)
	err = verifyRollbackValue(failedIndex, failedIndex+rollbackCount, servers, w)
	assert.Equal(t, nil, err, "verify roll back data is inconsistent")

	// start down servers
	_, servers = startServer(servers, downServer, w)
	waitForApply(servers, 1, w)

	// check data and roll back
	err = verifyStrictRestoreValue(failedIndex, servers, w)
	assert.Equal(t, nil, err, "verify data is inconsistent")

	err = verifyRollbackValue(failedIndex, failedIndex+rollbackCount, servers, w)
	assert.Equal(t, nil, err, "verify roll back data is inconsistent")
}

func TestStrictMode_LeaderDown(t *testing.T) {
	servers := initTestServer(peers, true, true, 1, StrictMode)
	f, w := getLogFile("", "TestStrictMode_LeaderDown.log")
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
	waitForApply(servers, 1, w)
	dataLen := verifyRestoreValue(servers, leadServer, w)

	var (
		failedIndex int
		err         error
	)
	go func(startIndex int) {
		var err error
		failedIndex, err = leadServer.putData(1, startIndex, PutDataStep, w)
		if err != nil {
			output("put data err: %v", err)
			printLog(w, fmt.Sprintf("put data maxKey(%v) err(%v) failed index(%v).", startIndex+PutDataStep-1, err, failedIndex))
		}
	}(dataLen)

	time.Sleep(1 * time.Second)

	// stop raft leader server
	var downServer *testServer
	downServer, leadServer, servers = stopLeader(servers, w, true)
	waitForApply(servers, 1, w)
	startIndex := verifyRestoreValue(servers, leadServer, w)
	printStatus(servers, w)

	// unstable shouldn't submit new data
	output("start put data")
	_, err = leadServer.putData(1, startIndex, 1, w)
	assert.NotEqual(t, nil, err, "raft submit should be error")

	// stable should submit new data
	leadServer, servers = startServer(servers, downServer, w)
	_, err = leadServer.putData(1, startIndex, PutDataStep, w)
	assert.Equal(t, nil, err, "raft submit failed")

	// check data
	compareServersWithLeader(servers, w, t)
	waitForApply(servers, 1, w)
	err = verifyStrictRestoreValue(startIndex+PutDataStep, servers, w)
	assert.Equal(t, nil, err, "verifyStrictRestoreValue failed")
	printStatus(servers, w)
}

func TestStrictMode_MemberChange(t *testing.T) {
	servers := initTestServer(peers, true, false, 1, StrictMode)
	f, w := getLogFile("", "TestStrictMode_MemberChange.log")
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
	dataLen := verifyRestoreValue(servers, leadServer, w)

	var (
		failedIndex int
		err         error
	)
	go func(leaderServer *testServer, startIndex int) {
		var err error
		failedIndex, err = leaderServer.putData(1, startIndex, PutDataStep, w)
		if err != nil {
			output("put data err: %v", err)
			printLog(w, fmt.Sprintf("put data maxKey(%v) err(%v) failed index(%v).", startIndex+PutDataStep-1, err, failedIndex))
		}
	}(leadServer, dataLen)

	// add member
	newServer := leadServer.addMember(4, raft.StrictMode, w, t)
	servers = append(servers, newServer)
	waitForApply(servers, 1, w)
	printStatus(servers, w)

	go func(leaderServer *testServer, startIndex int) {
		var err error
		failedIndex, err = leaderServer.putData(1, startIndex, PutDataStep, w)
		if err != nil {
			output("put data err: %v", err)
			printLog(w, fmt.Sprintf("put data maxKey(%v) err(%v) failed index(%v).", startIndex+PutDataStep-1, err, failedIndex))
		}
	}(leadServer, failedIndex)

	// delete member
	newServers := make([]*testServer, 0)
	leadServer.deleteMember(newServer, w, t)
	for _, s := range servers {
		if s.nodeID == newServer.nodeID {
			continue
		}
		newServers = append(newServers, s)
	}
	servers = newServers
	waitForApply(servers, 1, w)
	printStatus(servers, w)

	_, err = leadServer.putData(1, failedIndex, PutDataStep, w)
	assert.Equal(t, nil, err, "raft submit failed")

	// check data
	compareServersWithLeader(servers, w, t)
	err = verifyStrictRestoreValue(failedIndex+PutDataStep, servers, w)
	assert.Equal(t, nil, err, "verifyStrictRestoreValue failed")
	printStatus(servers, w)
}
