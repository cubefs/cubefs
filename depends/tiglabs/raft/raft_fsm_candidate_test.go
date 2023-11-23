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
	"testing"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	stor "github.com/cubefs/cubefs/depends/tiglabs/raft/storage"
)

func TestDuelingCandidates(t *testing.T) {
	a := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	b := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	c := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*raftFsm)
	if sm.state != stateLeader {
		t.Errorf("state = %s, want %v", sm.state, stateLeader)
	}

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
	sm = nt.peers[3].(*raftFsm)
	if sm.state != stateCandidate {
		t.Errorf("state = %s, want %v", sm.state, stateCandidate)
	}

	nt.recover()

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	wlog := &raftLog{
		storage:   &stor.MemoryStorage{},
		committed: 1,
		unstable:  unstable{offset: 2},
	}
	wlog.storage.StoreEntries([]*proto.Entry{{}, {Data: nil, Term: 1, Index: 1}})

	dlog, _ := newRaftLog(stor.DefaultMemoryStorage())
	tests := []struct {
		sm      *raftFsm
		state   fsmState
		term    uint64
		raftLog *raftLog
	}{
		{a, stateFollower, 2, wlog},
		{b, stateFollower, 2, wlog},
		{c, stateFollower, 2, dlog},
	}

	for i, tt := range tests {
		if g := tt.sm.state; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
		base := ltoa(tt.raftLog)
		if sm, ok := nt.peers[1+uint64(i)].(*raftFsm); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestDuelingPreCandidates(t *testing.T) {

	a := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	b := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	c := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	a.config.PreVote = true
	b.config.PreVote = true
	c.config.PreVote = true

	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*raftFsm)
	if sm.state != stateLeader {
		t.Errorf("state = %s, want %v", sm.state, stateLeader)
	}

	// 3 campaigns then reverts to follower when its PreVote is rejected
	sm = nt.peers[3].(*raftFsm)
	if sm.state != stateFollower {
		t.Errorf("state = %s, want %s", sm.state, stateFollower)
	}

	nt.recover()

	// Candidate 3 now increases its term and tries to vote again.
	// With PreVote, it does not disrupt the leader.
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	wlog := &raftLog{
		storage:   &stor.MemoryStorage{},
		committed: 1,
		unstable:  unstable{offset: 2},
	}
	wlog.storage.StoreEntries([]*proto.Entry{{}, {Data: nil, Term: 1, Index: 1}})
	dlog, _ := newRaftLog(stor.DefaultMemoryStorage())
	tests := []struct {
		sm      *raftFsm
		state   fsmState
		term    uint64
		raftLog *raftLog
	}{
		{a, stateLeader, 1, wlog},
		{b, stateFollower, 1, wlog},
		{c, stateFollower, 1, dlog},
	}

	for i, tt := range tests {
		if g := tt.sm.state; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
		base := ltoa(tt.raftLog)
		if sm, ok := nt.peers[1+uint64(i)].(*raftFsm); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestCandidateConcede(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	tt.isolate(1)

	tt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	tt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	// heal the partition
	tt.recover()
	// send ReqMsgAppend; flush nil message to node 1
	tt.send(proto.Message{From: 3, To: 1, Type: proto.ReqMsgAppend, Term: 1, LogTerm: 1, Index: 0, Entries: []*proto.Entry{{Data: nil, Index: 1, Term: 1}}})
	data := []byte("force follower")
	// send a proposal to 3 to flush out a MsgApp to 1
	tt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Data: data, Index: 2, Term: 1}}})

	a := tt.peers[1].(*raftFsm)
	if g := a.state; g != stateFollower {
		t.Errorf("state = %s, want %s", g, stateFollower)
	}
	if g := a.term; g != 1 {
		t.Errorf("term = %d, want %d", g, 1)
	}
	storage := stor.DefaultMemoryStorage()
	storage.StoreEntries([]*proto.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}})
	wantLog := ltoa(&raftLog{
		storage:   storage,
		unstable:  unstable{offset: 3},
		committed: 2,
	})

	for i, p := range tt.peers {
		if sm, ok := p.(*raftFsm); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(wantLog, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestSingleNodeCandidate(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	sm := tt.peers[1].(*raftFsm)
	if sm.state != stateLeader {
		t.Errorf("state = %d, want %d", sm.state, stateLeader)
	}
}

func TestSingleNodePreCandidate(t *testing.T) {
	tt := newNetworkWithConfig(preVoteConfig, nil)
	tt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	sm := tt.peers[1].(*raftFsm)
	if sm.state != stateLeader {
		t.Errorf("state = %d, want %d", sm.state, stateLeader)
	}
}
