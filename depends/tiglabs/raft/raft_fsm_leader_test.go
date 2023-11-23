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
	"encoding/json"
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	stor "github.com/cubefs/cubefs/depends/tiglabs/raft/storage"
)

func TestLeaderElection(t *testing.T) {
	testLeaderElection(t, false)
}

func TestLeaderElectionPreVote(t *testing.T) {
	testLeaderElection(t, true)
}

var nopStepper = &blackHole{}

func testLeaderElection(t *testing.T, preVote bool) {
	var rf func(*raftFsm)
	candState := stateCandidate
	candTerm := uint64(1)
	if preVote {
		rf = preVoteConfig
		// In pre-vote mode, an election that fails to complete
		// leaves the node in pre-candidate state without advancing
		// the term.
		candState = statePreCandidate
		candTerm = 0
	}
	tests := []struct {
		*network
		state   fsmState
		expTerm uint64
	}{
		{newNetworkWithConfig(rf, nil, nil, nil), stateLeader, 1},
		{newNetworkWithConfig(rf, nil, nil, nopStepper), stateLeader, 1},
		{newNetworkWithConfig(rf, nil, nopStepper, nopStepper), fsmState(candState), candTerm},
		{newNetworkWithConfig(rf, nil, nopStepper, nopStepper, nil), fsmState(candState), candTerm},
		{newNetworkWithConfig(rf, nil, nopStepper, nopStepper, nil, nil), stateLeader, 1},

		// three logs further along than 0, but in the same term so rejections
		// are returned instead of the votes being ignored.
		{newNetworkWithConfig(rf,
			nil, entsWithConfig(rf, 1), entsWithConfig(rf, 1), entsWithConfig(rf, 1, 1), nil),
			stateFollower, 1},
	}

	for i, tt := range tests {
		tt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
		sm := tt.network.peers[1].(*raftFsm)
		if sm.state != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, sm.state, tt.state)
		}
		if g := sm.term; g != tt.expTerm {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.expTerm)
		}
	}
}

func entsWithConfig(configFunc func(fsm *raftFsm), terms ...uint64) *raftFsm {
	storage := stor.DefaultMemoryStorage()

	for i, term := range terms {
		storage.StoreEntries([]*proto.Entry{{Index: uint64(i + 1), Term: term}})
	}
	sm := newTestRaftFsm(5, 1,
		newTestRaftConfig(1, withStorage(storage)))

	if configFunc != nil {
		configFunc(sm)
	}

	sm.reset(terms[len(terms)-1], uint64(1), false)

	return sm
}

// votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
func votedWithConfig(configFunc func(fsm *raftFsm), vote, term uint64) *raftFsm {
	storage := stor.DefaultMemoryStorage()
	storage.StoreHardState(proto.HardState{Vote: vote, Term: term})
	sm := newTestRaftFsm(5, 1,
		newTestRaftConfig(1, withStorage(storage)))

	if configFunc != nil {
		configFunc(sm)
	}
	return sm
}

func TestLeaderCycle(t *testing.T) {
	testLeaderCycle(t, false)
}

func TestLeaderCyclePreVote(t *testing.T) {
	testLeaderCycle(t, true)
}

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean slate (as they do in
// TestLeaderElection)
func testLeaderCycle(t *testing.T, preVote bool) {
	n := newNetworkWithConfig(preVoteConfig, nil, nil, nil)
	for campaignerID := uint64(1); campaignerID <= 3; campaignerID++ {
		n.send(proto.Message{From: campaignerID, To: campaignerID, Type: proto.LocalMsgHup})

		for _, peer := range n.peers {
			sm := peer.(*raftFsm)
			if sm.id == campaignerID && sm.state != stateLeader {
				t.Errorf("preVote=%v: campaigning node %d state = %v, want stateLeader",
					preVote, sm.id, sm.state)
			} else if sm.id != campaignerID && sm.state != stateFollower {
				t.Errorf("preVote=%v: after campaign of node %d, "+
					"node %d had state = %v, want stateFollower",
					preVote, campaignerID, sm.id, sm.state)
			}
		}
	}
}

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
func TestLeaderElectionOverwriteNewerLogs(t *testing.T) {
	testLeaderElectionOverwriteNewerLogs(t, false)
}

func TestLeaderElectionOverwriteNewerLogsPreVote(t *testing.T) {
	testLeaderElectionOverwriteNewerLogs(t, true)
}

func testLeaderElectionOverwriteNewerLogs(t *testing.T, preVote bool) {
	var rf func(*raftFsm)
	if preVote {
		rf = preVoteConfig
	}
	// This network represents the results of the following sequence of
	// events:
	// - Node 1 won the election in term 1.
	// - Node 1 replicated a log entry to node 2 but died before sending
	//   it to other nodes.
	// - Node 3 won the second election in term 2.
	// - Node 3 wrote an entry to its logs but died without sending it
	//   to any other nodes.
	//
	// At this point, nodes 1, 2, and 3 all have uncommitted entries in
	// their logs and could win an election at term 3. The winner's log
	// entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
	// the case where older log entries are overwritten, so this test
	// focuses on the case where the newer entries are lost).
	n := newNetworkWithConfig(rf,
		entsWithConfig(rf, 1),     // Node 1: Won first election
		entsWithConfig(rf, 1),     // Node 2: Got logs from node 1
		entsWithConfig(rf, 2),     // Node 3: Won second election
		votedWithConfig(rf, 3, 2), // Node 4: Voted but didn't get logs
		votedWithConfig(rf, 3, 2)) // Node 5: Voted but didn't get logs

	// Node 1 campaigns. The election fails because a quorum of nodes
	// know about the election that already happened at term 2. Node 1's
	// term is pushed ahead to 2.
	n.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	sm1 := n.peers[1].(*raftFsm)
	if sm1.state != stateFollower {
		t.Errorf("state = %s, want stateFollower", sm1.state)
	}
	if sm1.term != 2 {
		t.Errorf("term = %d, want 2", sm1.term)
	}

	// Node 1 campaigns again with a higher term. This time it succeeds.
	n.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	if sm1.state != stateLeader {
		t.Errorf("state = %s, want stateLeader", sm1.state)
	}
	if sm1.term != 3 {
		t.Errorf("term = %d, want 3", sm1.term)
	}

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
	for i := range n.peers {
		sm := n.peers[i].(*raftFsm)
		entries := sm.raftLog.allEntries()
		if len(entries) != 2 {
			t.Fatalf("node %d: len(entries) == %d, want 2", i, len(entries))
		}
		if entries[0].Term != 1 {
			t.Errorf("node %d: term at index 1 == %d, want 1", i, entries[0].Term)
		}
		if entries[1].Term != 3 {
			t.Errorf("node %d: term at index 2 == %d, want 3", i, entries[1].Term)
		}
	}
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
func TestCannotCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Index: 2, Term: 1, Data: []byte("some data")}}})
	tt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Index: 3, Term: 1, Data: []byte("some data")}}})

	sm := tt.peers[1].(*raftFsm)
	if sm.raftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 1)
	}

	// network recovery
	tt.recover()
	// avoid committing ChangeTerm proposal
	tt.ignore(proto.ReqMsgAppend)

	// elect 2 as the new leader with term 2
	tt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup})

	// no log entries from previous term should be committed
	sm = tt.peers[2].(*raftFsm)
	if sm.raftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 1)
	}

	tt.recover()

	// tell other nodes, network has recover
	tt.send(proto.Message{From: 2, To: 1, Type: proto.ReqMsgAppend, Term: 2, LogTerm: 2, Index: 0, Entries: []*proto.Entry{{Data: nil, Index: 4, Term: 2}}})
	tt.send(proto.Message{From: 2, To: 3, Type: proto.ReqMsgAppend, Term: 2, LogTerm: 2, Index: 0, Entries: []*proto.Entry{{Data: nil, Index: 4, Term: 2}}})
	tt.send(proto.Message{From: 2, To: 4, Type: proto.ReqMsgAppend, Term: 2, LogTerm: 2, Index: 0, Entries: []*proto.Entry{{Data: nil, Index: 4, Term: 2}}})
	tt.send(proto.Message{From: 2, To: 5, Type: proto.ReqMsgAppend, Term: 2, LogTerm: 2, Index: 0, Entries: []*proto.Entry{{Data: nil, Index: 4, Term: 2}}})

	// append an entry at current term
	tt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Index: 5, Term: 2, Data: []byte("some data")}}})
	// expect the committed to be advanced

	if sm.raftLog.committed != 5 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 5)
	}
}

func TestCommit(t *testing.T) {
	tests := []struct {
		matches []uint64
		logs    []*proto.Entry
		smTerm  uint64
		w       uint64
	}{
		// single
		{[]uint64{1}, []*proto.Entry{{Index: 1, Term: 1}}, 1, 1},
		{[]uint64{1}, []*proto.Entry{{Index: 1, Term: 1}}, 2, 0},
		{[]uint64{2}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{1}, []*proto.Entry{{Index: 1, Term: 2}}, 2, 1},

		// odd
		{[]uint64{2, 1, 1}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 2}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{2, 1, 2}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},

		// even
		{[]uint64{2, 1, 1, 1}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1, 1}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 1, 2}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1, 2}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 2, 2}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{2, 1, 2, 2}, []*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
	}

	for i, tt := range tests {
		s := stor.DefaultMemoryStorage()
		s.StoreEntries(tt.logs)
		s.StoreHardState(proto.HardState{Term: tt.smTerm})
		cfg := newTestRaftConfig(1, withStorage(s), withPeers(1))
		sm := newTestRaftFsm(10, 1, cfg)

		for j := 0; j < len(tt.matches); j++ {
			id := uint64(j) + 1
			if id > 1 {
				sm.applyConfChange(&proto.ConfChange{Type: proto.ConfAddNode, Peer: proto.Peer{PeerID: id, ID: id, Type: proto.PeerNormal}})
			}
			pr := sm.replicas[id]
			pr.match, pr.next = tt.matches[j], tt.matches[j]+1
		}
		sm.maybeCommit()
		if g := sm.raftLog.committed; g != tt.w {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.w)
		}
	}
}

// TestHandleMsgApp ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
func TestHandleMsgApp(t *testing.T) {
	tests := []struct {
		m       *proto.Message
		wIndex  uint64
		wCommit uint64
		wReject bool
	}{
		// Ensure 1
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 3, Index: 2, Commit: 3}, 2, 0, true}, // previous log mismatch
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 3, Index: 3, Commit: 3}, 2, 0, true}, // previous log non-exist

		// Ensure 2
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 1, Index: 1, Commit: 1}, 2, 1, false},
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 0, Index: 0, Commit: 1, Entries: []*proto.Entry{{Index: 1, Term: 2}}}, 1, 1, false},
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 3, Entries: []*proto.Entry{{Index: 3, Term: 2}, {Index: 4, Term: 2}}}, 4, 3, false},
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 4, Entries: []*proto.Entry{{Index: 3, Term: 2}}}, 3, 3, false},
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 1, Index: 1, Commit: 4, Entries: []*proto.Entry{{Index: 2, Term: 2}}}, 2, 2, false},

		// Ensure 3
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 1, LogTerm: 1, Index: 1, Commit: 3}, 2, 1, false},                                               // match entry 1, commit up to last new entry 1
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 1, LogTerm: 1, Index: 1, Commit: 3, Entries: []*proto.Entry{{Index: 2, Term: 2}}}, 2, 2, false}, // match entry 1, commit up to last new entry 2
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 3}, 2, 2, false},                                               // match entry 2, commit up to last new entry 2
		{&proto.Message{Type: proto.ReqMsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 4}, 2, 2, false},                                               // commit up to log.last()
	}

	for i, tt := range tests {
		s := stor.DefaultMemoryStorage()
		s.StoreEntries([]*proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
		cfg := newTestRaftConfig(1, withStorage(s), withPeers(1))
		sm := newTestRaftFsm(10, 1, cfg)

		sm.handleAppendEntries(tt.m)
		if sm.raftLog.lastIndex() != tt.wIndex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, sm.raftLog.lastIndex(), tt.wIndex)
		}
		if sm.raftLog.committed != tt.wCommit {
			t.Errorf("#%d: committed = %d, want %d", i, sm.raftLog.committed, tt.wCommit)
		}
		m := sm.readMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: msg = nil, want 1", i)
		}
		if m[0].Reject != tt.wReject {
			t.Errorf("#%d: reject = %v, want %v", i, m[0].Reject, tt.wReject)
		}
	}
}

// TestMsgAppRespWaitReset verifies the resume behavior of a leader
// MsgAppResp.
func TestMsgAppRespWaitReset(t *testing.T) {
	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1, 2, 3))
	sm := newTestRaftFsm(5, 1, cfg)
	sm.becomeCandidate()
	sm.becomeLeader()

	// The new leader has just emitted a new Term 4 entry; consume those messages
	// from the outgoing queue.
	sm.bcastAppend()
	sm.readMessages()

	// Node 2 acks the first entry, making it committed.
	sm.Step(&proto.Message{
		From:  2,
		Type:  proto.RespMsgAppend,
		Index: 1,
	})
	if sm.raftLog.committed != 1 {
		t.Fatalf("expected committed to be 1, got %d", sm.raftLog.committed)
	}
	// Also consume the MsgApp messages that update Commit on the followers.
	sm.readMessages()

	// A new command is now proposed on node 1.
	sm.Step(&proto.Message{
		From:    1,
		To:      1,
		Type:    proto.LocalMsgProp,
		Entries: []*proto.Entry{{Term: 1, Index: 2, Data: nil}},
	})

	// The command is broadcast to all nodes not in the wait state.
	// Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != proto.ReqMsgAppend || msgs[0].To != 2 {
		t.Errorf("expected MsgApp to node 2, got %v to %d", msgs[0].Type, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].Index != 2 {
		t.Errorf("expected to send entry 2, but got %v", msgs[0].Entries)
	}

	// Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
	sm.Step(&proto.Message{
		From:  3,
		Type:  proto.RespMsgAppend,
		Index: 1,
	})
	msgs = sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != proto.ReqMsgAppend || msgs[0].To != 3 {
		t.Errorf("expected MsgApp to node 3, got %v to %d", msgs[0].Type, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].Index != 2 {
		t.Errorf("expected to send entry 2, but got %v", msgs[0].Entries)
	}
}

func TestLeaderAppResp(t *testing.T) {
	// initial progress: match = 0; next = 3
	tests := []struct {
		index  uint64
		reject bool
		// progress
		wmatch uint64
		wnext  uint64
		// message
		wmsgNum    int
		windex     uint64
		wcommitted uint64
	}{
		{3, true, 0, 3, 0, 0, 0},  // stale resp; no replies
		{2, true, 0, 2, 1, 1, 0},  // denied resp; leader does not commit; decrease next and send probing msg
		{2, false, 2, 4, 2, 2, 2}, // accept resp; leader commits; broadcast with commit index
		{0, false, 0, 3, 0, 0, 0}, // ignore heartbeat replies
	}

	for i, tt := range tests {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
		s := stor.DefaultMemoryStorage()
		s.StoreEntries([]*proto.Entry{{}, {Index: 1, Term: 0}, {Index: 2, Term: 1}})
		cfg := newTestRaftConfig(1, withStorage(s), withPeers(1, 2, 3))
		sm := newTestRaftFsm(5, 1, cfg)
		sm.raftLog.unstable = unstable{offset: 3}
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.readMessages()
		sm.Step(&proto.Message{
			From:       2,
			Type:       proto.RespMsgAppend,
			Index:      tt.index,
			Term:       sm.term,
			Reject:     tt.reject,
			RejectHint: tt.index},
		)

		p := sm.replicas[2]
		if p.match != tt.wmatch {
			t.Errorf("#%d match = %d, want %d", i, p.match, tt.wmatch)
		}
		if p.next != tt.wnext {
			t.Errorf("#%d next = %d, want %d", i, p.next, tt.wnext)
		}

		msgs := sm.readMessages()

		if len(msgs) != tt.wmsgNum {
			t.Errorf("#%d msgNum = %d, want %d", i, len(msgs), tt.wmsgNum)
		}
		for j, msg := range msgs {
			if msg.Index != tt.windex {
				t.Errorf("#%d.%d index = %d, want %d", i, j, msg.Index, tt.windex)
			}
			if msg.Commit != tt.wcommitted {
				t.Errorf("#%d.%d commit = %d, want %d", i, j, msg.Commit, tt.wcommitted)
			}
		}
	}
}

func TestLeaderIncreaseNext(t *testing.T) {
	previousEnts := []*proto.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	tests := []struct {
		// progress
		state replicaState
		next  uint64

		wnext uint64
	}{
		// state replicate, optimistically increase next
		// previous entries + noop entry + propose + 1
		{replicaStateReplicate, 2, uint64(len(previousEnts) + 1 + 1 + 1)},
		// state probe, not optimistically increase next
		{replicaStateProbe, 2, 2},
	}

	for i, tt := range tests {
		s := stor.DefaultMemoryStorage()
		s.StoreEntries(previousEnts)
		cfg := newTestRaftConfig(1, withStorage(s), withPeers(1, 2))
		sm := newTestRaftFsm(5, 1, cfg)
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.replicas[2].state = tt.state
		sm.replicas[2].next = tt.next
		// Index = 3(old data) + 1(leader send nil data to other) + 1(new index) = 5
		sm.Step(&proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Term: 1, Index: 5, Data: []byte("somedata")}}})

		p := sm.replicas[2]
		if p.next != tt.wnext {
			t.Errorf("#%d next = %d, want %d", i, p.next, tt.wnext)
		}
	}
}

func mustAppendEntry(r *raftFsm, ents ...*proto.Entry) {
	for _, entry := range ents {
		r.appendEntry(entry)
	}
}

func TestSendAppendForProgressReplicate(t *testing.T) {
	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1, 2))
	r := newTestRaftFsm(5, 1, cfg)
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.replicas[2].becomeReplicate()

	for i := 0; i < 10; i++ {
		mustAppendEntry(r, &proto.Entry{Data: []byte("somedata")})
		r.sendAppend(2)
		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Errorf("len(msg) = %d, want %d", len(msgs), 1)
		}
	}
}

func TestRestore(t *testing.T) {
	peers := make([]proto.Peer, 0)
	for _, p := range []uint64{1, 2} {
		peers = append(peers, proto.Peer{Type: 0, Priority: 0, ID: p, PeerID: p})
	}
	snap := proto.SnapshotMeta{
		Index: 11, // magic number
		Term:  11, // magic number
		Peers: peers}

	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1, 2))
	sm := newTestRaftFsm(5, 1, cfg)
	sm.restore(snap)
	if err := sm.raftLog.storage.ApplySnapshot(snap); err != nil {
		t.Fatal("restore fail, want succeed")
	}

	if sm.raftLog.lastIndex() != snap.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.raftLog.lastIndex(), snap.Index)
	}
	if mustTerm(sm.raftLog.term(snap.Index)) != snap.Term {
		t.Errorf("log.lastTerm = %d, want %d", mustTerm(sm.raftLog.term(snap.Index)), snap.Term)
	}
	sg := sm.peers()
	if !reflect.DeepEqual(sg, snap.Peers) {
		t.Errorf("sm.Voters = %+v, want %+v", sg, snap.Peers)
	}
	// mark node 1 as snapshot state ?
	sm.replicas[1].becomeSnapshot(12)

	//todo  It should not campaign before actually applying data.
	for i := 0; i < sm.randElectionTick; i++ {
		sm.tick()
	}
	if sm.state != stateFollower {
		t.Errorf("state = %d, want %d", sm.state, stateFollower)
	}
}

func TestRestoreIgnoreSnapshot(t *testing.T) {
	peers := make([]proto.Peer, 0)
	for _, p := range []uint64{1, 2} {
		peers = append(peers, proto.Peer{Type: 0, Priority: 0, ID: p, PeerID: p})
	}
	previousEnts := []*proto.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	commit := uint64(1)
	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1, 2))
	sm := newTestRaftFsm(5, 1, cfg)
	sm.raftLog.append(previousEnts...)
	sm.raftLog.commitTo(commit)

	meta := proto.SnapshotMeta{
		Index: commit,
		Term:  1,
		Peers: peers,
	}

	// ignore snapshot
	sm.restore(meta)
	if sm.raftLog.committed != commit {
		t.Errorf("commit = %d, want %d", sm.raftLog.committed, commit)
	}

	// ignore snapshot and fast forward commit
	meta.Index = commit + 1
	sm.restore(meta)
	if sm.raftLog.committed != commit+1 {
		t.Errorf("commit = %d, want %d", sm.raftLog.committed, commit+1)
	}
}

type testFsmStateMachine struct {
	applyIndex uint64
}

func (sm *testFsmStateMachine) Apply(command []byte, index uint64) (interface{}, error) {
	return nil, nil
}
func (sm *testFsmStateMachine) ApplyMemberChange(cc *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}
func (sm *testFsmStateMachine) Snapshot() (proto.Snapshot, error) {
	return &testSnapshot{applyIndex: sm.applyIndex}, nil
}
func (sm *testFsmStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	return nil
}
func (sm *testFsmStateMachine) HandleLeaderChange(leader uint64) {}
func (sm *testFsmStateMachine) HandleFatalEvent(err *FatalError) {}

type testSnapshot struct {
	applyIndex uint64
}

func (ts *testSnapshot) ApplyIndex() uint64    { return ts.applyIndex }
func (ts *testSnapshot) Close()                {}
func (ts *testSnapshot) Next() ([]byte, error) { return nil, nil }

func TestProvideSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	peers := make([]proto.Peer, 0)
	for _, p := range []uint64{1, 2} {
		peers = append(peers, proto.Peer{Type: 0, Priority: 0, ID: p, PeerID: p})
	}
	meta := proto.SnapshotMeta{
		Index: 11, // magic number
		Term:  11, // magic number
		Peers: peers,
	}
	s := stor.DefaultMemoryStorage()
	s.ApplySnapshot(meta)
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1))
	sm := newTestRaftFsm(5, 1, cfg)
	sm.restore(meta)

	sm.sm = &testFsmStateMachine{applyIndex: 13}

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 2, so that node 2 needs a snapshot
	sm.replicas[2].next = sm.raftLog.firstIndex()
	sm.Step(&proto.Message{From: 2, To: 1, Type: proto.RespMsgAppend, Index: sm.replicas[2].next - 1, Reject: true})

	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Type != proto.ReqMsgSnapShot {
		t.Errorf("m.Type = %v, want %v", m.Type, proto.ReqMsgSnapShot)
	}
}

func TestIgnoreProvidingSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	peers := make([]proto.Peer, 0)
	for _, p := range []uint64{1, 2} {
		peers = append(peers, proto.Peer{Type: 0, Priority: 0, ID: p, PeerID: p})
	}
	meta := proto.SnapshotMeta{
		Index: 11, // magic number
		Term:  11, // magic number
		Peers: peers,
	}
	s := stor.DefaultMemoryStorage()
	s.ApplySnapshot(meta)
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1))
	sm := newTestRaftFsm(5, 1, cfg)
	sm.restore(meta)

	sm.sm = &testFsmStateMachine{applyIndex: 13}

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 2, so that node 2 needs a snapshot
	// change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
	sm.replicas[2].next = sm.raftLog.firstIndex() - 1
	sm.replicas[2].active = false

	sm.Step(&proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Term: 12, Index: 13, Data: []byte("somedata")}}})

	msgs := sm.readMessages()
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

// TestAddNode tests that addNode could update nodes correctly.
func TestAddNode(t *testing.T) {
	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1))
	r := newTestRaftFsm(5, 1, cfg)
	r.applyConfChange(&proto.ConfChange{Peer: proto.Peer{Type: 0, ID: 2, PeerID: 2, Priority: 0}, Type: proto.ConfAddNode})
	nodes := r.replicas
	if len(nodes) != 2 {
		t.Errorf("nodes = %v, want %v", len(nodes), 2)
	}
}

// TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
// immediately when checkQuorum is set.
func TestAddNodeCheckQuorum(t *testing.T) {
	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1))
	r := newTestRaftFsm(5, 1, cfg)
	r.config.LeaseCheck = true
	r.becomeCandidate()
	r.becomeLeader()

	for i := 0; i < r.electionElapsed-1; i++ {
		r.tick()
	}

	r.applyConfChange(&proto.ConfChange{Peer: proto.Peer{Type: 0, ID: 2, PeerID: 2, Priority: 0}, Type: proto.ConfAddNode})

	// This tick will reach electionTimeout, which triggers a quorum check.
	r.tick()

	// Node 1 should still be the leader after a single tick.
	if r.state != stateLeader {
		t.Errorf("state = %v, want %v", r.state, stateLeader)
	}

	// After another electionTimeout ticks without hearing from node 2,
	// node 1 should step down.
	for i := 0; i < r.electionElapsed; i++ {
		r.tick()
	}

	if r.state != stateFollower {
		t.Errorf("state = %v, want %v", r.state, stateFollower)
	}
}

// TestRemoveNode tests that removeNode could update nodes and
// and removed list correctly.
func TestRemoveNode(t *testing.T) {
	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1, 2))
	r := newTestRaftFsm(5, 1, cfg)
	r.applyConfChange(&proto.ConfChange{Peer: proto.Peer{Type: 0, ID: 2, PeerID: 2, Priority: 0}, Type: proto.ConfRemoveNode})
	w := []uint64{1}
	if g := r.peers(); len(g) != len(w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}

	//todo Removing the remaining voter will panic?
	//defer func() {
	//	if r := recover(); r == nil {
	//		t.Error("did not panic")
	//	}
	//}()
	//r.applyConfChange(&proto.ConfChange{Peer: proto.Peer{Type: 0, ID: 1, PeerID: 1, Priority: 0}, Type: proto.ConfRemoveNode})
}

func TestPromotable(t *testing.T) {
	id := uint64(1)
	tests := []struct {
		peers []uint64
		wp    bool
	}{
		{[]uint64{1}, true},
		{[]uint64{1, 2, 3}, true},
		{[]uint64{}, false},
		{[]uint64{2, 3}, false},
	}
	for i, tt := range tests {
		s := stor.DefaultMemoryStorage()
		cfg := newTestRaftConfig(id, withStorage(s), withPeers(tt.peers...))
		r := newTestRaftFsm(5, 1, cfg)
		if g := r.promotable(); g != tt.wp {
			t.Errorf("#%d: promotable = %v, want %v", i, g, tt.wp)
		}
	}
}

func TestCampaignWhileLeader(t *testing.T) {
	testCampaignWhileLeader(t, false)
}

func TestPreCampaignWhileLeader(t *testing.T) {
	testCampaignWhileLeader(t, true)
}

func testCampaignWhileLeader(t *testing.T, preVote bool) {
	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1))
	r := newTestRaftFsm(5, 1, cfg)
	r.config.PreVote = preVote
	if r.state != stateFollower {
		t.Errorf("expected new node to be follower but got %s", r.state)
	}
	// We don't call campaign() directly because it comes after the check
	// for our current state.
	r.Step(&proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	if r.state != stateLeader {
		t.Errorf("expected single-node election to become leader but got %s", r.state)
	}
	term := r.term
	r.Step(&proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	if r.state != stateLeader {
		t.Errorf("expected to remain leader but got %s", r.state)
	}
	if r.term != term {
		t.Errorf("expected to remain in term %v but got %v", term, r.term)
	}
}

// nextEnts returns the appliable entries and updates the applied index
func nextEnts(r *raftFsm, s *stor.MemoryStorage) (ents []*proto.Entry) {
	// Transfer all unstable entries to "stable" storage.
	s.StoreEntries(r.raftLog.unstableEntries())
	r.raftLog.stableTo(r.raftLog.lastIndex(), r.raftLog.lastTerm())

	ents = r.raftLog.nextEnts(1024)
	r.raftLog.appliedTo(r.raftLog.committed)
	return ents
}

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
func TestCommitAfterRemoveNode(t *testing.T) {
	// Create a cluster with two nodes.
	s := stor.DefaultMemoryStorage()
	cfg := newTestRaftConfig(1, withStorage(s), withPeers(1, 2))
	r := newTestRaftFsm(5, 1, cfg)
	r.becomeCandidate()
	r.becomeLeader()

	// Begin to remove the second node.
	cc := proto.ConfChange{
		Type: proto.ConfRemoveNode,
		Peer: proto.Peer{PeerID: 2, ID: 2},
	}
	ccData, err := json.Marshal(cc)
	if err != nil {
		t.Fatal(err)
	}
	r.Step(&proto.Message{
		Type: proto.LocalMsgProp,
		Entries: []*proto.Entry{
			{Type: proto.EntryConfChange, Data: ccData, Index: 2, Term: 1},
		},
	})
	// Stabilize the log and make sure nothing is committed yet.
	if ents := nextEnts(r, s); len(ents) > 0 {
		t.Fatalf("unexpected committed entries: %v", ents)
	}
	ccIndex := r.raftLog.lastIndex()

	// While the config change is pending, make another proposal.
	r.Step(&proto.Message{
		Type: proto.LocalMsgProp,
		Entries: []*proto.Entry{
			{Type: proto.EntryNormal, Data: []byte("hello"), Index: 3, Term: 1},
		},
	})

	// Node 2 acknowledges the config change, committing it.
	r.Step(&proto.Message{
		Type:  proto.RespMsgAppend,
		From:  2,
		To:    1,
		Index: ccIndex,
	})
	ents := nextEnts(r, s)
	if len(ents) != 2 {
		t.Fatalf("expected two committed entries, got %v", ents)
	}
	if ents[0].Type != proto.EntryNormal || ents[0].Data != nil {
		t.Fatalf("expected ents[0] to be empty, but got %v", ents[0])
	}
	if ents[1].Type != proto.EntryConfChange {
		t.Fatalf("expected ents[1] to be EntryConfChange, got %v", ents[1])
	}

	// Apply the config change. This reduces quorum requirements so the
	// pending command can now commit.
	r.applyConfChange(&cc)
	ents = nextEnts(r, s)
	if len(ents) != 1 || ents[0].Type != proto.EntryNormal ||
		string(ents[0].Data) != "hello" {
		t.Fatalf("expected one committed EntryNormal, got %v", ents)
	}
}

func checkLeaderTransferState(t *testing.T, r *raftFsm, state fsmState, lead uint64) {
	if r.state != state || r.leader != lead {
		t.Fatalf("after transferring, node has state %v lead %v, want state %v lead %v", r.state, r.leader, state, lead)
	}
	//if r.leadTransferee != None {
	//	t.Fatalf("after transferring, node has leadTransferee %v, want leadTransferee %v", r.leadTransferee, None)
	//}
}

// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
func TestLeaderTransferToUpToDateNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	lead := nt.peers[1].(*raftFsm)

	if lead.leader != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.leader)
	}

	// Transfer leadership to 2.
	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup, ForceVote: true})

	checkLeaderTransferState(t, lead, stateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, ForceVote: true})

	nt.send(proto.Message{From: 1, To: 2, Type: proto.LeaseMsgOffline})

	checkLeaderTransferState(t, lead, stateLeader, 1)
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
func TestLeaderTransferToUpToDateNodeFromFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	lead := nt.peers[1].(*raftFsm)

	if lead.leader != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.leader)
	}

	// Transfer leadership to 2.
	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup, ForceVote: true})

	checkLeaderTransferState(t, lead, stateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{}}})

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup, ForceVote: true})

	checkLeaderTransferState(t, lead, stateLeader, 1)
}

// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
func setRandomizedElectionTimeout(r *raftFsm, v int) {
	r.randElectionTick = v
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
func TestLeaderTransferWithCheckQuorum(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	for i := 1; i < 4; i++ {
		r := nt.peers[uint64(i)].(*raftFsm)
		r.config.LeaseCheck = true
		setRandomizedElectionTimeout(r, r.electionElapsed+i)
	}

	// Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
	f := nt.peers[2].(*raftFsm)
	for i := 0; i < f.electionElapsed; i++ {
		f.tick()
	}

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	lead := nt.peers[1].(*raftFsm)

	if lead.leader != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.leader)
	}

	// Transfer leadership to 2.
	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup, ForceVote: true})

	checkLeaderTransferState(t, lead, stateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{}}})

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup, ForceVote: true})

	checkLeaderTransferState(t, lead, stateLeader, 1)
}

func TestLeaderTransferToSlowFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	nt.isolate(3)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{}}})

	nt.recover()
	lead := nt.peers[1].(*raftFsm)
	if lead.replicas[3].match != 1 {
		t.Fatalf("node 1 has match %x for node 3, want %x", lead.replicas[3].match, 1)
	}

	// Transfer leadership to 3 when node 3 is lack of log.
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup, ForceVote: true})

	checkLeaderTransferState(t, lead, stateFollower, 3)
}

//func TestLeaderTransferAfterSnapshot(t *testing.T) {
//	nt := newNetwork(nil, nil, nil)
//	nt.send(proto.Message{From: 1, To: 1, Type: proto.MsgHup})
//
//	nt.isolate(3)
//
//	nt.send(proto.Message{From: 1, To: 1, Type: proto.MsgProp, Entries: []proto.Entry{{}}})
//	lead := nt.peers[1].(*raft)
//	nextEnts(lead, nt.storage[1])
//	nt.storage[1].CreateSnapshot(lead.raftLog.applied, &proto.ConfState{Voters: lead.prs.VoterNodes()}, nil)
//	nt.storage[1].Compact(lead.raftLog.applied)
//
//	nt.recover()
//	if lead.prs.Progress[3].Match != 1 {
//		t.Fatalf("node 1 has match %x for node 3, want %x", lead.prs.Progress[3].Match, 1)
//	}
//
//	filtered := proto.Message{}
//	// Snapshot needs to be applied before sending MsgAppResp
//	nt.msgHook = func(m proto.Message) bool {
//		if m.Type != proto.MsgAppResp || m.From != 3 || m.Reject {
//			return true
//		}
//		filtered = m
//		return false
//	}
//	// Transfer leadership to 3 when node 3 is lack of snapshot.
//	nt.send(proto.Message{From: 3, To: 1, Type: proto.MsgTransferLeader})
//	if lead.state != StateLeader {
//		t.Fatalf("node 1 should still be leader as snapshot is not applied, got %x", lead.state)
//	}
//	if reflect.DeepEqual(filtered, proto.Message{}) {
//		t.Fatalf("Follower should report snapshot progress automatically.")
//	}
//
//	// Apply snapshot and resume progress
//	follower := nt.peers[3].(*raft)
//	ready := newReady(follower, &SoftState{}, proto.HardState{})
//	nt.storage[3].ApplySnapshot(ready.Snapshot)
//	follower.advance(ready)
//	nt.msgHook = nil
//	nt.send(filtered)
//
//	checkLeaderTransferState(t, lead, StateFollower, 3)
//}

func TestLeaderTransferToSelf(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	lead := nt.peers[1].(*raftFsm)

	// Transfer leadership to self, there will be noop.
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup, ForceVote: true})
	checkLeaderTransferState(t, lead, stateLeader, 1)
}

func TestLeaderTransferToNonExistingNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	lead := nt.peers[1].(*raftFsm)
	// Transfer leadership to non-existing node, there will be noop.
	nt.send(proto.Message{From: 4, To: 1, Type: proto.LocalMsgProp, ForceVote: true})
	checkLeaderTransferState(t, lead, stateLeader, 1)
}

func TestLeaderTransferTimeout(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raftFsm)

	// Transfer leadership to isolated node, wait for timeout.
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup, ForceVote: true})

	for i := 0; i < lead.heartbeatElapsed; i++ {
		lead.tick()
	}

	for i := 0; i < lead.electionElapsed-lead.heartbeatElapsed; i++ {
		lead.tick()
	}

	checkLeaderTransferState(t, lead, stateLeader, 1)
}

func TestLeaderTransferIgnoreProposal(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raftFsm)

	// Transfer leadership to isolated node to let transfer pending, then send proposal.
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{}}})
	lead.Step(&proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{}}})

	if lead.replicas[1].match != 1 {
		t.Fatalf("node 1 has match %x, want %x", lead.replicas[1].match, 1)
	}
}

func TestLeaderTransferReceiveHigherTermVote(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raftFsm)

	// Transfer leadership to isolated node to let transfer pending.
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup, Index: 1, Term: 2})

	checkLeaderTransferState(t, lead, stateFollower, 2)
}
