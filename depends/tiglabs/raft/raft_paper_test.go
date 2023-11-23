// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
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

package raft

import (
	"fmt"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"sort"
	"testing"
)

func TestFollowerUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, stateFollower)
}
func TestCandidateUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, stateCandidate)
}
func TestLeaderUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, stateLeader)
}

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
func testUpdateTermFromMessage(t *testing.T, state fsmState) {
	r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))

	switch state {
	case stateFollower:
		r.becomeFollower(1, 2)
	case stateCandidate:
		r.becomeCandidate()
	case stateLeader:
		r.becomeCandidate()
		r.becomeLeader()
	}

	r.Step(&proto.Message{Type: proto.ReqMsgAppend, Term: 2})

	if r.term != 2 {
		t.Errorf("term = %d, want %d", r.term, 2)
	}
	if r.state != stateFollower {
		t.Errorf("state = %v, want %v", r.state, stateFollower)
	}

}

// TestRejectStaleTermMessage tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
func TestRejectStaleTermMessage(t *testing.T) {
	called := false
	fakeStep := func(r *raftFsm, m *proto.Message) {
		called = true
	}
	r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
	r.step = fakeStep
	r.loadState(proto.HardState{Term: 2})

	r.Step(&proto.Message{Type: proto.ReqMsgAppend, Term: r.term - 1})

	if called {
		t.Errorf("stepFunc called = %v, want %v", called, false)
	}
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
func TestStartAsFollower(t *testing.T) {
	r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
	if r.state != stateFollower {
		t.Errorf("state = %s, want %s", r.state, stateFollower)
	}
}

//	TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
//	it will send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
//	as heartbeat to all followers.
//	Reference: section 5.2
//	tiglab/raft use ReqCheckQuorum while not ReqMsgHeartBeat, and if readIndex queue not nil direct return
func TestLeaderBcastBeat(t *testing.T) {
	// heartbeat interval
	hi := 1
	r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
	r.becomeCandidate()
	r.becomeLeader()
	for i := 0; i < 10; i++ {
		r.appendEntry(&proto.Entry{Index: uint64(i) + 1})
	}

	for i := 0; i < hi; i++ {
		r.tick()
	}

	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []proto.Message{
		{ID: r.id, From: 1, To: 2, Term: 1, Type: proto.ReqCheckQuorum},
		{ID: r.id, From: 1, To: 3, Term: 1, Type: proto.ReqCheckQuorum},
	}
	for i := range msgs {
		msgs[i].Entries = nil
		require.Equal(t, msgs[i], wmsgs[i])
	}
}

func TestFollowerStartElection(t *testing.T) {
	testNonleaderStartElection(t, stateFollower)
}
func TestCandidateStartNewElection(t *testing.T) {
	testNonleaderStartElection(t, stateCandidate)
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
func testNonleaderStartElection(t *testing.T, state fsmState) {
	// election timeout
	et := 10
	r := newTestRaftFsm(et, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
	switch state {
	case stateFollower:
		r.becomeFollower(1, 2)
	case stateCandidate:
		r.becomeCandidate()
	}

	for i := 1; i <= 2*et; i++ {
		r.tick()
	}

	if r.term != 2 {
		t.Errorf("term = %d, want 2", r.term)
	}
	if r.state != stateCandidate {
		t.Errorf("state = %s, want %d", r.state, stateCandidate)
	}
	if !r.votes[r.id] {
		t.Errorf("vote for self = false, want true")
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []*proto.Message{
		{ID: r.id, From: 1, To: 2, Term: 2, Type: proto.ReqMsgVote},
		{ID: r.id, From: 1, To: 3, Term: 2, Type: proto.ReqMsgVote},
	}
	for i := range wmsgs {
		require.Equal(t, msgs[i].Type, wmsgs[i].Type)
		require.Equal(t, msgs[i].Term, wmsgs[i].Term)
		require.Equal(t, msgs[i].To, wmsgs[i].To)
		require.Equal(t, msgs[i].From, wmsgs[i].From)
	}

}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
func TestLeaderElectionInOneRoundRPC(t *testing.T) {
	tests := []struct {
		size  int
		votes map[uint64]bool
		state fsmState
	}{
		// win the election when receiving votes from a majority of the servers
		{1, map[uint64]bool{}, stateLeader},
		{3, map[uint64]bool{2: true, 3: true}, stateLeader},
		{3, map[uint64]bool{2: true}, stateLeader},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, stateLeader},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, stateLeader},
		{5, map[uint64]bool{2: true, 3: true}, stateLeader},

		// return to follower state if it receives vote denial from a majority
		{3, map[uint64]bool{2: false, 3: false}, stateFollower},
		{5, map[uint64]bool{2: false, 3: false, 4: false, 5: false}, stateFollower},
		{5, map[uint64]bool{2: true, 3: false, 4: false, 5: false}, stateFollower},

		// stay in candidate if it does not obtain the majority
		{3, map[uint64]bool{}, stateCandidate},
		{5, map[uint64]bool{2: true}, stateCandidate},
		{5, map[uint64]bool{2: false, 3: false}, stateCandidate},
		{5, map[uint64]bool{}, stateCandidate},
	}
	for i, tt := range tests {
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(idsBySize(tt.size)...)))
		r.Step(&proto.Message{ID: r.id, From: 1, To: 1, Type: proto.LocalMsgHup})
		for id, vote := range tt.votes {
			r.Step(&proto.Message{From: id, To: 1, Term: r.term, Type: proto.RespMsgVote, Reject: !vote})
		}

		if r.state != tt.state {
			t.Errorf("#%d: state = %s, want %d", i, r.state, tt.state)
		}
		if g := r.term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
func TestFollowerVote(t *testing.T) {
	tests := []struct {
		vote    uint64
		nvote   uint64
		wreject bool
	}{
		{NoLeader, 2, false},
		{NoLeader, 3, false},
		{2, 2, false},
		{3, 3, false},
		{2, 3, true},
		{3, 2, true},
	}
	for _, tt := range tests {
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
		r.loadState(proto.HardState{Term: 1, Vote: tt.vote})

		r.Step(&proto.Message{From: tt.nvote, To: 1, Term: 1, Type: proto.ReqMsgVote})

		msgs := r.readMessages()
		wmsgs := []proto.Message{
			{ID: r.id, From: 1, To: tt.nvote, Term: 1, Type: proto.RespMsgVote, Reject: tt.wreject},
		}

		//todo
		msgs[0].Entries = nil
		require.Equal(t, msgs, wmsgs)
	}
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
func TestCandidateFallback(t *testing.T) {
	tests := []*proto.Message{
		{From: 2, To: 1, Term: 1, Type: proto.ReqMsgAppend},
		{From: 2, To: 1, Term: 2, Type: proto.ReqMsgAppend},
	}
	for i, tt := range tests {
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
		r.Step(&proto.Message{ID: r.id, From: 1, To: 1, Type: proto.LocalMsgHup})
		if r.state != stateCandidate {
			t.Fatalf("unexpected state = %s, want %d", r.state, stateCandidate)
		}

		r.Step(tt)

		if g := r.state; g != stateFollower {
			t.Errorf("#%d: state = %s, want %d", i, g, stateFollower)
		}
		if g := r.term; g != tt.Term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.Term)
		}
	}
}

func TestFollowerElectionTimeoutRandomized(t *testing.T) {
	testNonleaderElectionTimeoutRandomized(t, stateFollower)
}
func TestCandidateElectionTimeoutRandomized(t *testing.T) {
	testNonleaderElectionTimeoutRandomized(t, stateCandidate)
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
func testNonleaderElectionTimeoutRandomized(t *testing.T, state fsmState) {
	et := 10
	r := newTestRaftFsm(et, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
	timeouts := make(map[int]bool)
	for round := 0; round < 50*et; round++ {
		switch state {
		case stateFollower:
			r.becomeFollower(r.term+1, 2)
		case stateCandidate:
			r.becomeCandidate()
		}

		time := 0
		for len(r.readMessages()) == 0 {
			r.tick()
			time++
		}
		timeouts[time] = true
	}

	for d := et; d < 2*et; d++ {
		if !timeouts[d] {
			t.Errorf("timeout in %d ticks should happen", d)
		}
	}
}

func TestFollowersElectionTimeoutNonconflict(t *testing.T) {
	testNonleadersElectionTimeoutNonconflict(t, stateFollower)
}
func TestCandidatesElectionTimeoutNonconflict(t *testing.T) {
	testNonleadersElectionTimeoutNonconflict(t, stateCandidate)
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
func testNonleadersElectionTimeoutNonconflict(t *testing.T, state fsmState) {
	et := 10
	size := 5
	rs := make([]*raftFsm, size)
	ids := idsBySize(size)
	for k := range rs {
		rs[k] = newTestRaftFsm(et, 1, newTestRaftConfig(ids[k], withPeers(ids...)))
	}
	conflicts := 0
	for round := 0; round < 1000; round++ {
		for _, r := range rs {
			switch state {
			case stateFollower:
				r.becomeFollower(r.term+1, NoLeader)
			case stateCandidate:
				r.becomeCandidate()
			}
		}

		timeoutNum := 0
		for timeoutNum == 0 {
			for _, r := range rs {
				r.tick()
				if len(r.readMessages()) > 0 {
					timeoutNum++
				}
			}
		}
		// several rafts time out at the same tick
		if timeoutNum > 1 {
			conflicts++
		}
	}

	if g := float64(conflicts) / 1000; g > 0.3 {
		t.Errorf("probability of conflicts = %v, want <= 0.3", g)
	}
}

// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
func TestLeaderStartReplication(t *testing.T) {
	s := storage.DefaultMemoryStorage()
	r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(s), withPeers(1, 2, 3)))
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.raftLog.lastIndex()

	ents := []*proto.Entry{{Index: li + 1, Term: r.term, Data: []byte("some data")}}
	r.Step(&proto.Message{ID: r.id, From: 1, To: 1, Type: proto.LocalMsgProp, Entries: ents})

	if g := r.raftLog.lastIndex(); g != li+1 {
		t.Errorf("lastIndex = %d, want %d", g, li+1)
	}
	if g := r.raftLog.committed; g != li {
		t.Errorf("committed = %d, want %d", g, li)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wents := []*proto.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	wmsgs := []proto.Message{
		{ID: r.id, From: 1, To: 2, Term: 1, Type: proto.ReqMsgAppend, Index: li, LogTerm: 1, Entries: wents, Commit: li},
		{ID: r.id, From: 1, To: 3, Term: 1, Type: proto.ReqMsgAppend, Index: li, LogTerm: 1, Entries: wents, Commit: li},
	}
	require.Equal(t, msgs, wmsgs)
	g := r.raftLog.unstableEntries()
	require.Equal(t, g, wents)

}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
func TestLeaderCommitEntry(t *testing.T) {
	s := storage.DefaultMemoryStorage()
	r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(s), withPeers(1, 2, 3)))
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.raftLog.lastIndex()
	r.Step(&proto.Message{ID: r.id, From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Index: li + 1, Term: r.term, Data: []byte("some data")}}})

	for _, m := range r.readMessages() {
		r.Step(acceptAndReply(m))
	}

	if g := r.raftLog.committed; g != li+1 {
		t.Errorf("committed = %d, want %d", g, li+1)
	}
	wents := []*proto.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	if g := r.raftLog.nextEnts(noLimit); !reflect.DeepEqual(g, wents) {
		t.Errorf("nextEnts = %+v, want %+v", g, wents)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	for i, m := range msgs {
		if w := uint64(i + 2); m.To != w {
			t.Errorf("to = %x, want %x", m.To, w)
		}
		if m.Type != proto.ReqMsgAppend {
			t.Errorf("type = %v, want %v", m.Type, proto.ReqMsgAppend)
		}
		if m.Commit != li+1 {
			t.Errorf("commit = %d, want %d", m.Commit, li+1)
		}
	}
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
func TestLeaderAcknowledgeCommit(t *testing.T) {
	tests := []struct {
		size               int
		nonLeaderAcceptors map[uint64]bool
		wack               bool
	}{
		{1, nil, true},
		{3, nil, false},
		{3, map[uint64]bool{2: true}, true},
		{3, map[uint64]bool{2: true, 3: true}, true},
		{5, nil, false},
		{5, map[uint64]bool{2: true}, false},
		{5, map[uint64]bool{2: true, 3: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, true},
	}
	for i, tt := range tests {
		s := storage.DefaultMemoryStorage()
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(s), withPeers(idsBySize(tt.size)...)))
		r.becomeCandidate()
		r.becomeLeader()
		commitNoopEntry(r, s)
		li := r.raftLog.lastIndex()
		r.Step(&proto.Message{ID: r.id, From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Index: li + 1, Term: r.term, Data: []byte("some data")}}})
		for _, m := range r.msgs {
			if tt.nonLeaderAcceptors[m.To] {
				r.Step(acceptAndReply(*m))
			}
		}

		if g := r.raftLog.committed > li; g != tt.wack {
			t.Errorf("#%d: ack commit = %v, want %v", i, g, tt.wack)
		}
	}
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestLeaderCommitPrecedingEntries(t *testing.T) {
	tests := [][]*proto.Entry{
		{},
		{{Term: 2, Index: 1}},
		{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
		{{Term: 1, Index: 1}},
	}
	for i, tt := range tests {
		s := storage.DefaultMemoryStorage()
		s.StoreEntries(tt)
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(s), withPeers(1, 2, 3)))
		r.loadState(proto.HardState{Term: 2})
		r.becomeCandidate()
		r.becomeLeader()
		r.Step(&proto.Message{ID: r.id, From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Index: r.raftLog.lastIndex() + 1, Term: r.term, Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			r.Step(acceptAndReply(m))
		}

		li := uint64(len(tt))
		wents := append(tt, &proto.Entry{Term: 3, Index: li + 1}, &proto.Entry{Term: 3, Index: li + 2, Data: []byte("some data")})
		if g := r.raftLog.nextEnts(noLimit); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, wents)
		}
	}
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestFollowerCommitEntry(t *testing.T) {
	tests := []struct {
		ents   []*proto.Entry
		commit uint64
	}{
		{
			[]*proto.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
			},
			1,
		},
		{
			[]*proto.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			2,
		},
		{
			[]*proto.Entry{
				{Term: 1, Index: 1, Data: []byte("some data2")},
				{Term: 1, Index: 2, Data: []byte("some data")},
			},
			2,
		},
		{
			[]*proto.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			1,
		},
	}
	for i, tt := range tests {
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
		r.becomeFollower(1, 2)

		r.Step(&proto.Message{From: 2, To: 1, Type: proto.ReqMsgAppend, Term: 1, Entries: tt.ents, Commit: tt.commit})

		if g := r.raftLog.committed; g != tt.commit {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.commit)
		}
		wents := tt.ents[:int(tt.commit)]
		//if g := r.raftLog.nextCommittedEnts(true); !reflect.DeepEqual(g, wents) {
		if g := r.raftLog.nextEnts(noLimit); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: nextCommittedEnts = %v, want %v", i, g, wents)
		}
	}
}

// TestFollowerCheckMsgApp tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
// todo tiglab raft msg append rejiect not same with raft
func TestFollowerCheckMsgApp(t *testing.T) {
	ents := []*proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		term        uint64
		index       uint64
		windex      uint64
		wreject     bool
		wrejectHint uint64
		wlogterm    uint64
	}{
		// match with committed entries
		{0, 0, 1, false, 0, 0},
		{ents[0].Term, ents[0].Index, 1, false, 0, 0},
		// match with uncommitted entries
		{ents[1].Term, ents[1].Index, 2, false, 0, 0},

		// unmatch with existing entry
		{ents[0].Term, ents[1].Index, ents[1].Index, true, 2, 1},
		// unexisting entry
		{ents[1].Term + 1, ents[1].Index + 1, ents[1].Index + 1, true, 2, 2},
	}
	for i, tt := range tests {
		s := storage.DefaultMemoryStorage()
		s.StoreEntries(ents)
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(s), withPeers(1, 2, 3)))
		r.loadState(proto.HardState{Commit: 1})
		r.becomeFollower(2, 2)

		r.Step(&proto.Message{From: 2, To: 1, Type: proto.ReqMsgAppend, Term: 2, LogTerm: tt.term, Index: tt.index})

		msgs := r.readMessages()
		// todo  this is diff from etcd raft, because tiglab raft has commited in (replica) which not in etcd raft (Progress)
		// so this test case in tiglab raft, not use findConflictByTerm func, which wrejectHint should 1 and logTerm=0
		wmsgs := []proto.Message{
			{ID: r.id, From: 1, To: 2, Type: proto.RespMsgAppend, Term: 2, Index: tt.windex, Reject: tt.wreject, RejectHint: tt.wrejectHint, LogTerm: 0, Commit: 1},
		}
		msgs[0].Entries = nil
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %+v, want %+v", i, msgs, wmsgs)
		}

		// in test case the entries is empty, not need to check and manual set nil

		fmt.Println("i=", i)
		require.Equal(t, msgs, wmsgs)
	}
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
func TestFollowerAppendEntries(t *testing.T) {
	tests := []struct {
		index, term uint64
		ents        []*proto.Entry
		wents       []*proto.Entry
		wunstable   []*proto.Entry
	}{
		{
			2, 2,
			[]*proto.Entry{{Term: 3, Index: 3}},
			[]*proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
			[]*proto.Entry{{Term: 3, Index: 3}},
		},
		{
			1, 1,
			[]*proto.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]*proto.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]*proto.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
		},
		{
			0, 0,
			[]*proto.Entry{{Term: 1, Index: 1}},
			[]*proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
			nil,
		},
		{
			0, 0,
			[]*proto.Entry{{Term: 3, Index: 1}},
			[]*proto.Entry{{Term: 3, Index: 1}},
			[]*proto.Entry{{Term: 3, Index: 1}},
		},
	}
	for i, tt := range tests {
		s := storage.DefaultMemoryStorage()
		s.StoreEntries([]*proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(s), withPeers(1, 2, 3)))
		r.becomeFollower(2, 2)

		r.Step(&proto.Message{From: 2, To: 1, Type: proto.ReqMsgAppend, Term: 2, LogTerm: tt.term, Index: tt.index, Entries: tt.ents})

		if g := r.raftLog.allEntries(); !reflect.DeepEqual(g, tt.wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, tt.wents)
		}
		if g := r.raftLog.unstableEntries(); !reflect.DeepEqual(g, tt.wunstable) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, g, tt.wunstable)
		}
	}
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
// todo remove
//func TestLeaderSyncFollowerLog(t *testing.T) {
//	ents := []*proto.Entry{
//		{},
//		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
//		{Term: 4, Index: 4}, {Term: 4, Index: 5},
//		{Term: 5, Index: 6}, {Term: 5, Index: 7},
//		{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
//	}
//	term := uint64(8)
//	tests := [][]*proto.Entry{
//		{
//			{},
//			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
//			{Term: 4, Index: 4}, {Term: 4, Index: 5},
//			{Term: 5, Index: 6}, {Term: 5, Index: 7},
//			{Term: 6, Index: 8}, {Term: 6, Index: 9},
//		},
//		{
//			{},
//			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
//			{Term: 4, Index: 4},
//		},
//		{
//			{},
//			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
//			{Term: 4, Index: 4}, {Term: 4, Index: 5},
//			{Term: 5, Index: 6}, {Term: 5, Index: 7},
//			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10}, {Term: 6, Index: 11},
//		},
//		{
//			{},
//			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
//			{Term: 4, Index: 4}, {Term: 4, Index: 5},
//			{Term: 5, Index: 6}, {Term: 5, Index: 7},
//			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
//			{Term: 7, Index: 11}, {Term: 7, Index: 12},
//		},
//		{
//			{},
//			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
//			{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 4, Index: 6}, {Term: 4, Index: 7},
//		},
//		{
//			{},
//			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
//			{Term: 2, Index: 4}, {Term: 2, Index: 5}, {Term: 2, Index: 6},
//			{Term: 3, Index: 7}, {Term: 3, Index: 8}, {Term: 3, Index: 9}, {Term: 3, Index: 10}, {Term: 3, Index: 11},
//		},
//	}
//	for i, tt := range tests {
//
//		leadStorage := storage.DefaultMemoryStorage()
//		leadStorage.StoreEntries(ents)
//		lead := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(leadStorage), withPeers(1, 2, 3)))
//
//		lead.loadState(proto.HardState{Commit: lead.raftLog.lastIndex(), Term: term})
//		followerStorage := storage.DefaultMemoryStorage()
//		followerStorage.StoreEntries(tt)
//		follower := newTestRaftFsm(10, 1, newTestRaftConfig(2, withStorage(followerStorage), withPeers(1, 2, 3)))
//
//		follower.loadState(proto.HardState{Term: term - 1})
//		// It is necessary to have a three-node cluster.
//		// The second may have more up-to-date log than the first one, so the
//		// first node needs the vote from the third node to become the leader.
//		n := newNetwork(lead, follower, nopStepper)
//		n.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
//		// The election occurs in the term after the one we loaded with
//		// lead.loadState above.
//		n.send(proto.Message{From: 3, To: 1, Type: proto.RespMsgVote, Term: term + 1})
//
//		n.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{}}})
//
//		if g := diffu(ltoa(lead.raftLog), ltoa(follower.raftLog)); g != "" {
//			t.Errorf("#%d: log diff:\n%s", i, g)
//		}
//	}
//}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
func TestVoteRequest(t *testing.T) {
	tests := []struct {
		ents  []*proto.Entry
		wterm uint64
	}{
		{[]*proto.Entry{{Term: 1, Index: 1}}, 2},
		{[]*proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}, 3},
	}
	for j, tt := range tests {
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withPeers(1, 2, 3)))
		r.Step(&proto.Message{
			From: 2, To: 1, Type: proto.ReqMsgAppend, Term: tt.wterm - 1, LogTerm: 0, Index: 0, Entries: tt.ents,
		})
		r.readMessages()

		for i := 1; i < r.config.ElectionTick*2; i++ {
			r.tickElection()
		}

		msgs := r.readMessages()
		sort.Sort(messageSlice(msgs))
		if len(msgs) != 2 {
			t.Fatalf("#%d: len(msg) = %d, want %d", j, len(msgs), 2)
		}
		for i, m := range msgs {
			if m.Type != proto.ReqMsgVote {
				t.Errorf("#%d: msgType = %d, want %d", i, m.Type, proto.ReqMsgVote)
			}
			if m.To != uint64(i+2) {
				t.Errorf("#%d: to = %d, want %d", i, m.To, i+2)
			}
			if m.Term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, m.Term, tt.wterm)
			}
			windex, wlogterm := tt.ents[len(tt.ents)-1].Index, tt.ents[len(tt.ents)-1].Term
			if m.Index != windex {
				t.Errorf("#%d: index = %d, want %d", i, m.Index, windex)
			}
			if m.LogTerm != wlogterm {
				t.Errorf("#%d: logterm = %d, want %d", i, m.LogTerm, wlogterm)
			}
		}
	}
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
func TestVoter(t *testing.T) {
	tests := []struct {
		ents    []*proto.Entry
		logterm uint64
		index   uint64

		wreject bool
	}{
		// same logterm
		{[]*proto.Entry{{Term: 1, Index: 1}}, 1, 1, false},
		{[]*proto.Entry{{Term: 1, Index: 1}}, 1, 2, false},
		{[]*proto.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
		// candidate higher logterm
		{[]*proto.Entry{{Term: 1, Index: 1}}, 2, 1, false},
		{[]*proto.Entry{{Term: 1, Index: 1}}, 2, 2, false},
		{[]*proto.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 2, 1, false},
		// voter higher logterm
		{[]*proto.Entry{{Term: 2, Index: 1}}, 1, 1, true},
		{[]*proto.Entry{{Term: 2, Index: 1}}, 1, 2, true},
		{[]*proto.Entry{{Term: 2, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
	}
	for i, tt := range tests {
		s := storage.DefaultMemoryStorage()
		s.StoreEntries(tt.ents)
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(s), withPeers(1, 2)))

		r.Step(&proto.Message{From: 2, To: 1, Type: proto.ReqMsgVote, Term: 3, LogTerm: tt.logterm, Index: tt.index})

		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Fatalf("#%d: len(msg) = %d, want %d", i, len(msgs), 1)
		}
		m := msgs[0]
		if m.Type != proto.RespMsgVote {
			t.Errorf("#%d: msgType = %d, want %d", i, m.Type, proto.RespMsgVote)
		}
		if m.Reject != tt.wreject {
			t.Errorf("#%d: reject = %t, want %t", i, m.Reject, tt.wreject)
		}
	}
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
func TestLeaderOnlyCommitsLogFromCurrentTerm(t *testing.T) {
	ents := []*proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		index   uint64
		wcommit uint64
	}{
		// do not commit log entries in previous terms
		{1, 0},
		{2, 0},
		// commit log in current term
		{3, 3},
	}
	for i, tt := range tests {
		s := storage.DefaultMemoryStorage()
		s.StoreEntries(ents)
		r := newTestRaftFsm(10, 1, newTestRaftConfig(1, withStorage(s), withPeers(1, 2)))

		r.loadState(proto.HardState{Term: 2})
		// become leader at term 3
		r.becomeCandidate()
		r.becomeLeader()
		r.readMessages()
		// propose a entry to current term
		r.Step(&proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Index: r.raftLog.lastIndex() + 1, Term: r.term}}})

		r.Step(&proto.Message{From: 2, To: 1, Type: proto.RespMsgAppend, Term: r.term, Index: tt.index})
		if r.raftLog.committed != tt.wcommit {
			t.Errorf("#%d: commit = %d, want %d", i, r.raftLog.committed, tt.wcommit)
		}
	}
}

// nodeId is nodeID which must equal to one of peers ID
func newTestRaftFsm(election, heartbeat int, rc *RaftConfig) *raftFsm {
	conf := DefaultConfig()
	conf.HeartbeatTick = heartbeat
	conf.ElectionTick = election
	conf.NodeID = rc.ID

	r, _ := newRaftFsm(conf, rc)
	return r
}

// lease check is true while use stateAck
func newTestRaftFsmWithLeaseCheck(election, heartbeat int, rc *RaftConfig) *raftFsm {
	conf := DefaultConfig()
	conf.HeartbeatTick = heartbeat
	conf.ElectionTick = election
	conf.NodeID = rc.ID
	conf.LeaseCheck = true

	r, _ := newRaftFsm(conf, rc)
	return r
}

type testRaftConfigOptions func(*RaftConfig)

func withPeers(peersID ...uint64) testRaftConfigOptions {
	peers := make([]proto.Peer, 0)
	for _, p := range peersID {
		peers = append(peers, proto.Peer{Type: 0, Priority: 0, ID: p, PeerID: p})
	}
	return func(config *RaftConfig) {
		config.Peers = peers
	}
}

func withStorage(s storage.Storage) testRaftConfigOptions {
	return func(config *RaftConfig) {
		config.Storage = s
	}
}

func newTestRaftConfig(nodeId uint64, opts ...testRaftConfigOptions) *RaftConfig {
	rc := &RaftConfig{
		ID: nodeId,
	}
	for _, o := range opts {
		o(rc)
	}

	if rc.Storage == nil {
		rc.Storage = storage.DefaultMemoryStorage()
	}
	return rc
}

func (r *raftFsm) readMessages() []proto.Message {
	msgs := make([]proto.Message, 0)
	for _, m := range r.msgs {
		msgs = append(msgs, *m)
	}
	r.msgs = make([]*proto.Message, 0)
	return msgs
}

type messageSlice []proto.Message

func (s messageSlice) Len() int           { return len(s) }
func (s messageSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s messageSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

func commitNoopEntry(r *raftFsm, s *storage.MemoryStorage) {
	if r.state != stateLeader {
		panic("it should only be used when it is the leader")
	}
	r.bcastAppend()
	// simulate the response of MsgApp
	msgs := r.readMessages()
	for _, m := range msgs {
		//if m.Type != proto.RespMsgAppend || len(m.Entries) != 1 || m.Entries[0].Data != nil {
		if m.Type != proto.ReqMsgAppend || len(m.Entries) != 1 || m.Entries[0].Data != nil {
			panic("not a message to append noop entry")
		}
		r.Step(acceptAndReply(m))
	}
	// ignore further messages to refresh followers' commit index
	r.readMessages()
	s.StoreEntries(r.raftLog.unstableEntries())
	r.raftLog.appliedTo(r.raftLog.committed)
	r.raftLog.stableTo(r.raftLog.lastIndex(), r.raftLog.lastTerm())
}

func acceptAndReply(m proto.Message) *proto.Message {
	if m.Type != proto.ReqMsgAppend {
		panic("type should be MsgApp")
	}
	return &proto.Message{
		From:  m.To,
		To:    m.From,
		Term:  m.Term,
		Type:  proto.RespMsgAppend,
		Index: m.Index + uint64(len(m.Entries)),
	}
}
