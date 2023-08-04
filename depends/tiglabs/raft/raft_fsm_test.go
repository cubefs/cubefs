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
	"math"
	"math/rand"
	"testing"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	stor "github.com/cubefs/cubefs/depends/tiglabs/raft/storage"
)

type connem struct {
	from, to uint64
}

type network struct {
	peers   map[uint64]stateMachine
	storage map[uint64]*stor.MemoryStorage
	dropm   map[connem]float64
	ignorem map[proto.MsgType]bool

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	msgHook func(proto.Message) bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [1, n].
func newNetwork(peers ...stateMachine) *network {
	return newNetworkWithConfig(nil, peers...)
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates.
func newNetworkWithConfig(configFunc func(fsm *raftFsm), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]*stor.MemoryStorage, size)

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			s := stor.DefaultMemoryStorage()
			cfg := newTestRaftConfig(id, withStorage(s), withPeers(peerAddrs...))
			r := newTestRaftFsm(10, 1, cfg)

			for i := 0; i < size; i++ {
				r.replicas[peerAddrs[i]] = newReplica(proto.Peer{PeerID: peerAddrs[i], Priority: 0, ID: peerAddrs[i]}, 256)
			}
			if configFunc != nil {
				configFunc(r)
			}
			r.id = id
			r.config.NodeID = id
			npeers[id] = r
		case *raftFsm:
			v.id = id
			for i := 0; i < size; i++ {
				v.replicas[peerAddrs[i]] = newReplica(proto.Peer{PeerID: peerAddrs[i], Priority: 0, ID: peerAddrs[i]}, 256)
			}
			v.config.NodeID = id
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic(fmt.Sprintf("unexpected state machine type: %T", p))
		}
	}
	return &network{
		peers:   npeers,
		storage: nstorage,
		dropm:   make(map[connem]float64),
		ignorem: make(map[proto.MsgType]bool),
	}
}

func (nw *network) send(msgs ...proto.Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		p.Step(&m)
		msgs = append(msgs[1:], nw.filter(p.readMessages())...)
	}
}

func (nw *network) drop(from, to uint64, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other uint64) {
	nw.drop(one, other, 2.0) // always drop
	nw.drop(other, one, 2.0) // always drop
}

func (nw *network) isolate(id uint64) {
	for i := 0; i < len(nw.peers); i++ {
		nid := uint64(i) + 1
		if nid != id {
			nw.drop(id, nid, 1.0) // always drop
			nw.drop(nid, id, 1.0) // always drop
		}
	}
}

func (nw *network) ignore(t proto.MsgType) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[proto.MsgType]bool)
}

func (nw *network) filter(msgs []proto.Message) []proto.Message {
	mm := []proto.Message{}
	for _, m := range msgs {
		if nw.ignorem[m.Type] {
			continue
		}
		switch m.Type {
		case proto.LocalMsgHup:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected msgHup")
		default:
			perc := nw.dropm[connem{m.From, m.To}]
			if n := rand.Float64(); n < perc {
				continue
			}
		}
		if nw.msgHook != nil {
			if !nw.msgHook(m) {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

// voteResponseType maps vote and prevote message types to their corresponding responses.
func voteRespMsgType(msgt proto.MsgType) proto.MsgType {
	switch msgt {
	case proto.ReqMsgVote:
		return proto.RespMsgVote
	case proto.ReqMsgPreVote:
		return proto.RespMsgPreVote
	default:
		panic(fmt.Sprintf("not a vote message: %s", msgt))
	}
}

func preVoteConfig(r *raftFsm) {
	r.config.PreVote = true
}

type stateMachine interface {
	Step(m *proto.Message)
	readMessages() []proto.Message
}

type blackHole struct{}

func (blackHole) Step(*proto.Message)           {}
func (blackHole) readMessages() []proto.Message { return nil }

func TestVoteFromAnyState(t *testing.T) {
	testVoteFromAnyState(t, proto.ReqMsgVote)
}

func TestPreVoteFromAnyState(t *testing.T) {
	testVoteFromAnyState(t, proto.ReqMsgPreVote)
}

func testVoteFromAnyState(t *testing.T, vt proto.MsgType) {
	for st := fsmState(0); st < 4; st++ {
		r := newTestRaftFsm(10, 1,
			newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
		r.term = 1

		switch st {
		case stateFollower:
			r.becomeFollower(r.term, 3)
		case statePreCandidate:
			r.becomePreCandidate()
		case stateCandidate:
			r.becomeCandidate()
		case stateLeader:
			r.becomeCandidate()
			r.becomeLeader()
		}

		// Note that setting our state above may have advanced r.term
		// past its initial value.
		origTerm := r.term
		newTerm := r.term + 1

		msg := &proto.Message{
			From:    2,
			To:      1,
			Type:    vt,
			Term:    newTerm,
			LogTerm: newTerm,
			Index:   42,
		}
		r.Step(msg)
		if len(r.msgs) != 1 {
			t.Errorf("%s,%s: %d response messages, want 1: %+v", vt, st, len(r.msgs), r.msgs)
		} else {
			resp := r.msgs[0]
			if resp.Type != voteRespMsgType(vt) {
				t.Errorf("%s,%s: response message is %s, want %s",
					vt, st, resp.Type, voteRespMsgType(vt))
			}
			if resp.Reject {
				t.Errorf("%s,%s: unexpected rejection", vt, st)
			}
		}

		// If this was a real vote, we reset our state and term.
		if vt == proto.ReqMsgVote {
			if r.state != stateFollower {
				t.Errorf("%s,%s: state %s, want %s", vt, st, r.state, stateFollower)
			}
			if r.term != newTerm {
				t.Errorf("%s,%s: term %d, want %d", vt, st, r.term, newTerm)
			}
			if r.vote != 2 {
				t.Errorf("%s,%s: vote %d, want 2", vt, st, r.vote)
			}
		} else {
			// In a prevote, nothing changes.
			if r.state != st {
				t.Errorf("%s,%s: state %s, want %s", vt, st, r.state, st)
			}
			if r.term != origTerm {
				t.Errorf("%s,%s: term %d, want %d", vt, st, r.term, origTerm)
			}
			// if st == stateFollower or statePreCandidate, r hasn't voted yet.
			// In stateCandidate or stateLeader, it's voted for itself.
			if r.vote != NoLeader && r.vote != 1 {
				t.Errorf("%s,%s: vote %d, want %d or 1", vt, st, r.vote, NoLeader)
			}
		}
	}
}

func TestPastElectionTimeout(t *testing.T) {
	tests := []struct {
		elapse       int
		wprobability float64
		round        bool
	}{
		{5, 0, false},
		{10, 0.1, true},
		{13, 0.4, true},
		{15, 0.6, true},
		{18, 0.9, true},
		{20, 1, false},
	}

	for i, tt := range tests {
		sm := newTestRaftFsm(10, 1,
			newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
		sm.electionElapsed = tt.elapse
		c := 0
		for j := 0; j < 10000; j++ {
			sm.resetRandomizedElectionTimeout()
			if sm.pastElectionTimeout() {
				c++
			}
		}
		got := float64(c) / 10000.0
		if tt.round {
			got = math.Floor(got*10+0.5) / 10.0
		}
		if got != tt.wprobability {
			t.Errorf("#%d: probability = %v, want %v", i, got, tt.wprobability)
		}
	}
}

// TestStepIgnoreOldTermMsg to ensure that the Step function ignores the message
// from old term and does not pass it to the actual stepX function.
func TestStepIgnoreOldTermMsg(t *testing.T) {
	called := false
	fakeStep := func(r *raftFsm, m *proto.Message) {
		called = true
	}
	sm := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1)))
	sm.step = fakeStep
	sm.term = 2
	sm.Step(&proto.Message{Type: proto.ReqMsgAppend, Term: sm.term - 1})
	if called {
		t.Errorf("stepFunc called = %v , want %v", called, false)
	}
}

// TestTransferNonMember verifies that when a MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to stateLeader)
func TestTransferNonMember(t *testing.T) {
	r := newTestRaftFsm(5, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(2, 3, 4)))
	// todo
	//r.Step(proto.Message{From: 2, To: 1, Type: proto.MsgTimeoutNow})

	r.Step(&proto.Message{From: 2, To: 1, Type: proto.RespMsgPreVote})
	r.Step(&proto.Message{From: 3, To: 1, Type: proto.RespMsgPreVote})
	if r.state != stateFollower {
		t.Fatalf("state is %s, want stateFollower", r.state)
	}
}

// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
func TestNodeWithSmallerTermCanCompleteElection(t *testing.T) {
	n1 := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n2 := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n3 := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	n1.becomeFollower(1, NoLeader)
	n2.becomeFollower(1, NoLeader)
	n3.becomeFollower(1, NoLeader)

	n1.config.PreVote = true
	n2.config.PreVote = true
	n3.config.PreVote = true

	// cause a network partition to isolate node 3
	nt := newNetwork(n1, n2, n3)
	nt.cut(1, 3)
	nt.cut(2, 3)

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	sm := nt.peers[1].(*raftFsm)
	if sm.state != stateLeader {
		t.Errorf("peer 1 state: %s, want %v", sm.state, stateLeader)
	}

	sm = nt.peers[2].(*raftFsm)
	if sm.state != stateFollower {
		t.Errorf("peer 2 state: %s, want %v", sm.state, stateFollower)
	}

	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})
	sm = nt.peers[3].(*raftFsm)
	if sm.state != statePreCandidate {
		t.Errorf("peer 3 state: %s, want %v", sm.state, statePreCandidate)
	}

	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup})

	// check whether the term values are expected
	// n1.term == 3
	// n2.term == 3
	// n3.term == 1
	sm = nt.peers[1].(*raftFsm)
	if sm.term != 3 {
		t.Errorf("peer 1 term: %d, want %d", sm.term, 3)
	}

	sm = nt.peers[2].(*raftFsm)
	if sm.term != 3 {
		t.Errorf("peer 2 term: %d, want %d", sm.term, 3)
	}

	sm = nt.peers[3].(*raftFsm)
	if sm.term != 1 {
		t.Errorf("peer 3 term: %d, want %d", sm.term, 1)
	}

	// check state
	// n1 == follower
	// n2 == leader
	// n3 == pre-candidate
	sm = nt.peers[1].(*raftFsm)
	if sm.state != stateFollower {
		t.Errorf("peer 1 state: %s, want %v", sm.state, stateFollower)
	}
	sm = nt.peers[2].(*raftFsm)
	if sm.state != stateLeader {
		t.Errorf("peer 2 state: %s, want %v", sm.state, stateLeader)
	}
	sm = nt.peers[3].(*raftFsm)
	if sm.state != statePreCandidate {
		t.Errorf("peer 3 state: %s, want %v", sm.state, statePreCandidate)
	}

	// recover the network then immediately isolate b which is currently
	// the leader, this is to emulate the crash of b.
	nt.recover()
	nt.cut(2, 1)
	nt.cut(2, 3)

	// call for election
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	// do we have a leader?
	sma := nt.peers[1].(*raftFsm)
	smb := nt.peers[3].(*raftFsm)
	if sma.state != stateLeader && smb.state != stateLeader {
		t.Errorf("no leader")
	}
}

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
func TestPreVoteWithSplitVote(t *testing.T) {
	n1 := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n2 := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n3 := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	n1.becomeFollower(1, NoLeader)
	n2.becomeFollower(1, NoLeader)
	n3.becomeFollower(1, NoLeader)

	n1.config.PreVote = true
	n2.config.PreVote = true
	n3.config.PreVote = true

	nt := newNetwork(n1, n2, n3)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	// simulate leader down. followers start split vote.
	nt.isolate(1)
	nt.send([]proto.Message{
		{From: 2, To: 2, Type: proto.LocalMsgHup},
		{From: 3, To: 3, Type: proto.LocalMsgHup},
	}...)

	// check whether the term values are expected
	// n2.term == 3
	// n3.term == 3
	sm := nt.peers[2].(*raftFsm)
	if sm.term != 3 {
		t.Errorf("peer 2 term: %d, want %d", sm.term, 3)
	}
	sm = nt.peers[3].(*raftFsm)
	if sm.term != 3 {
		t.Errorf("peer 3 term: %d, want %d", sm.term, 3)
	}

	// check state
	// n2 == candidate
	// n3 == candidate
	sm = nt.peers[2].(*raftFsm)
	if sm.state != stateCandidate {
		t.Errorf("peer 2 state: %s, want %v", sm.state, stateCandidate)
	}
	sm = nt.peers[3].(*raftFsm)
	if sm.state != stateCandidate {
		t.Errorf("peer 3 state: %s, want %v", sm.state, stateCandidate)
	}

	// node 2 election timeout first
	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup})

	// check whether the term values are expected
	// n2.term == 4
	// n3.term == 4
	sm = nt.peers[2].(*raftFsm)
	if sm.term != 4 {
		t.Errorf("peer 2 term: %d, want %d", sm.term, 4)
	}
	sm = nt.peers[3].(*raftFsm)
	if sm.term != 4 {
		t.Errorf("peer 3 term: %d, want %d", sm.term, 4)
	}

	// check state
	// n2 == leader
	// n3 == follower
	sm = nt.peers[2].(*raftFsm)
	if sm.state != stateLeader {
		t.Errorf("peer 2 state: %s, want %v", sm.state, stateLeader)
	}
	sm = nt.peers[3].(*raftFsm)
	if sm.state != stateFollower {
		t.Errorf("peer 3 state: %s, want %v", sm.state, stateFollower)
	}
}

// TestPreVoteWithCheckQuorum ensures that after a node become pre-candidate,
// it will checkQuorum correctly.
func TestPreVoteWithCheckQuorum(t *testing.T) {
	n1 := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n2 := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n3 := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	n1.becomeFollower(1, NoLeader)
	n2.becomeFollower(1, NoLeader)
	n3.becomeFollower(1, NoLeader)

	n1.config.PreVote = true
	n2.config.PreVote = true
	n3.config.PreVote = true

	//n1.config.LeaseCheck  = true
	//n2.config.LeaseCheck  = true
	//n3.config.LeaseCheck  = true

	nt := newNetwork(n1, n2, n3)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	// isolate node 1. node 2 and node 3 have leader info
	nt.isolate(1)

	// check state
	sm := nt.peers[1].(*raftFsm)
	if sm.state != stateLeader {
		t.Fatalf("peer 1 state: %s, want %v", sm.state, stateLeader)
	}
	sm = nt.peers[2].(*raftFsm)
	if sm.state != stateFollower {
		t.Fatalf("peer 2 state: %s, want %v", sm.state, stateFollower)
	}
	sm = nt.peers[3].(*raftFsm)
	if sm.state != stateFollower {
		t.Fatalf("peer 3 state: %s, want %v", sm.state, stateFollower)
	}

	// node 2 will ignore node 3's PreVote
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})
	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup})

	// Do we have a leader?
	if n2.state != stateLeader && n3.state != stateFollower {
		t.Errorf("no leader")
	}
}

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
func newPreVoteMigrationCluster(t *testing.T) *network {
	n1 := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n2 := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n3 := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n1.becomeFollower(1, NoLeader)
	n2.becomeFollower(1, NoLeader)
	n3.becomeFollower(1, NoLeader)

	n1.config.PreVote = true
	n2.config.PreVote = true
	// We intentionally do not enable PreVote for n3, this is done so in order
	// to simulate a rolling restart process where it's possible to have a mixed
	// version cluster with replicas with PreVote enabled, and replicas without.

	nt := newNetwork(n1, n2, n3)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	// Cause a network partition to isolate n3.
	nt.isolate(3)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Data: []byte("some data")}}})
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	// check state
	// n1.state == stateLeader
	// n2.state == stateFollower
	// n3.state == stateCandidate
	if n1.state != stateLeader {
		t.Fatalf("node 1 state: %s, want %d", n1.state, stateLeader)
	}
	if n2.state != stateFollower {
		t.Fatalf("node 2 state: %s, want %d", n2.state, stateFollower)
	}
	if n3.state != stateCandidate {
		t.Fatalf("node 3 state: %s, want %d", n3.state, statePreCandidate)
	}

	// check term
	// n1.term == 2
	// n2.term == 2
	// n3.term == 4
	if n1.term != 2 {
		t.Fatalf("node 1 term: %d, want %d", n1.term, 2)
	}
	if n2.term != 2 {
		t.Fatalf("node 2 term: %d, want %d", n2.term, 2)
	}
	if n3.term != 4 {
		t.Fatalf("node 3 term: %d, want %d", n3.term, 4)
	}

	// recover the network
	n3.config.PreVote = true
	nt.recover()

	return nt
}

func TestPreVoteMigrationCanCompleteElection(t *testing.T) {
	nt := newPreVoteMigrationCluster(t)

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
	n2 := nt.peers[2].(*raftFsm)
	n3 := nt.peers[3].(*raftFsm)

	// simulate leader down
	nt.isolate(1)

	// Call for elections from both n2 and n3.
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})
	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup})

	// check state
	// n2.state == Follower
	// n3.state == PreCandidate
	if n2.state != stateFollower {
		t.Errorf("node 2 state: %s, want %v", n2.state, stateFollower)
	}
	if n3.state != statePreCandidate {
		t.Errorf("node 3 state: %s, want %v", n3.state, statePreCandidate)
	}

	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})
	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup})

	// Do we have a leader?
	if n2.state != stateLeader && n3.state != stateFollower {
		t.Errorf("no leader")
	}

}

func TestPreVoteMigrationWithFreeStuckPreCandidate(t *testing.T) {
	nt := newPreVoteMigrationCluster(t)

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
	n1 := nt.peers[1].(*raftFsm)
	n2 := nt.peers[2].(*raftFsm)
	n3 := nt.peers[3].(*raftFsm)

	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	if n1.state != stateLeader {
		t.Errorf("node 1 state: %s, want %v", n1.state, stateLeader)
	}
	if n2.state != stateFollower {
		t.Errorf("node 2 state: %s, want %v", n2.state, stateFollower)
	}
	if n3.state != statePreCandidate {
		t.Errorf("node 3 state: %s, want %v", n3.state, statePreCandidate)
	}

	// Pre-Vote again for safety
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	if n1.state != stateLeader {
		t.Errorf("node 1 state: %s, want %v", n1.state, stateLeader)
	}
	if n2.state != stateFollower {
		t.Errorf("node 2 state: %s, want %v", n2.state, stateFollower)
	}
	if n3.state != statePreCandidate {
		t.Errorf("node 3 state: %s, want %v", n3.state, statePreCandidate)
	}

	nt.send(proto.Message{From: 1, To: 3, Type: proto.ReqMsgHeartBeat, Term: n1.term})

	// Disrupt the leader so that the stuck peer is freed
	if n1.state != stateFollower {
		t.Errorf("state = %s, want %v", n1.state, stateFollower)
	}
	if n3.term != n1.term {
		t.Errorf("term = %d, want %d", n3.term, n1.term)
	}
}

// TestMsgAppRespWaitReset verifies the resume behavior of a leader
// MsgAppResp.
//func TestMsgAppRespWaitReset(t *testing.T) {
//	sm := newTestRaftFsm(5, 1,
//		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
//	sm.becomeCandidate()
//	sm.becomeLeader()
//
//	// The new leader has just emitted a new Term 4 entry; consume those messages
//	// from the outgoing queue.
//	sm.bcastAppend()
//	sm.readMessages()
//
//	// Node 2 acks the first entry, making it committed.
//	sm.Step(&proto.Message{
//		From:  2,
//		Type:  proto.RespMsgAppend,
//		Index: 1,
//	})
//	if sm.raftLog.committed != 1 {
//		t.Fatalf("expected committed to be 1, got %d", sm.raftLog.committed)
//	}
//	// Also consume the MsgApp messages that update Commit on the followers.
//	sm.readMessages()
//
//	// A new command is now proposed on node 1.
//	sm.Step(&proto.Message{
//		From:    1,
//		Type:    proto.LocalMsgProp,
//		Entries: []*proto.Entry{{}},
//	})
//
//	// The command is broadcast to all nodes not in the wait state.
//	// Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
//	msgs := sm.readMessages()
//	if len(msgs) != 1 {
//		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
//	}
//	if msgs[0].Type != proto.ReqMsgAppend || msgs[0].To != 2 {
//		t.Errorf("expected MsgApp to node 2, got %v to %d", msgs[0].Type, msgs[0].To)
//	}
//	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].Index != 2 {
//		t.Errorf("expected to send entry 2, but got %v", msgs[0].Entries)
//	}
//
//	// Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
//	sm.Step(&proto.Message{
//		From:  3,
//		Type:  proto.RespMsgAppend,
//		Index: 1,
//	})
//	msgs = sm.readMessages()
//	if len(msgs) != 1 {
//		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
//	}
//	if msgs[0].Type != proto.ReqMsgAppend || msgs[0].To != 3 {
//		t.Errorf("expected MsgApp to node 3, got %v to %d", msgs[0].Type, msgs[0].To)
//	}
//	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].Index != 2 {
//		t.Errorf("expected to send entry 2, but got %v", msgs[0].Entries)
//	}
//}

func TestRecvReqMsgVote(t *testing.T) {
	testRecvReqMsgVote(t, proto.ReqMsgVote)
}

func TestRecvMsgPreVote(t *testing.T) {
	testRecvReqMsgVote(t, proto.ReqMsgPreVote)
}

func testRecvReqMsgVote(t *testing.T, msgType proto.MsgType) {
	tests := []struct {
		state          fsmState
		index, logTerm uint64
		voteFor        uint64
		wreject        bool
	}{
		{stateFollower, 0, 0, NoLeader, true},
		{stateFollower, 0, 1, NoLeader, true},
		{stateFollower, 0, 2, NoLeader, true},
		{stateFollower, 0, 3, NoLeader, false},

		{stateFollower, 1, 0, NoLeader, true},
		{stateFollower, 1, 1, NoLeader, true},
		{stateFollower, 1, 2, NoLeader, true},
		{stateFollower, 1, 3, NoLeader, false},

		{stateFollower, 2, 0, NoLeader, true},
		{stateFollower, 2, 1, NoLeader, true},
		{stateFollower, 2, 2, NoLeader, false},
		{stateFollower, 2, 3, NoLeader, false},

		{stateFollower, 3, 0, NoLeader, true},
		{stateFollower, 3, 1, NoLeader, true},
		{stateFollower, 3, 2, NoLeader, false},
		{stateFollower, 3, 3, NoLeader, false},

		{stateFollower, 3, 2, 2, false},
		{stateFollower, 3, 2, 1, true},

		{stateLeader, 3, 3, 1, true},
		{statePreCandidate, 3, 3, 1, true},
		{stateCandidate, 3, 3, 1, true},
	}

	max := func(a, b uint64) uint64 {
		if a > b {
			return a
		}
		return b
	}

	for i, tt := range tests {
		sm := newTestRaftFsm(1, 10, newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1)))
		sm.state = tt.state
		switch tt.state {
		case stateFollower:
			sm.step = stepFollower
		case stateCandidate, statePreCandidate:
			sm.step = stepCandidate
		case stateLeader:
			sm.step = stepLeader
		}
		sm.vote = tt.voteFor
		sm.raftLog = &raftLog{
			storage:  &stor.MemoryStorage{},
			unstable: unstable{offset: 3},
		}
		sm.raftLog.append([]*proto.Entry{{}, {Index: 1, Term: 2}, {Index: 2, Term: 2}}...)

		// raft.term is greater than or equal to raft.raftLog.lastTerm. In this
		// test we're only testing ReqMsgVote responses when the campaigning node
		// has a different raft log compared to the recipient node.
		// Additionally we're verifying behaviour when the recipient node has
		// already given out its vote for its current term. We're not testing
		// what the recipient node does when receiving a message with a
		// different term number, so we simply initialize both term numbers to
		// be the same.
		term := max(sm.raftLog.lastTerm(), tt.logTerm)
		sm.term = term
		sm.Step(&proto.Message{Type: msgType, Term: term, From: 2, Index: tt.index, LogTerm: tt.logTerm})

		msgs := sm.readMessages()
		if g := len(msgs); g != 1 {
			t.Fatalf("#%d: len(msgs) = %d, want 1", i, g)
			continue
		}
		if g := msgs[0].Type; g != voteRespMsgType(msgType) {
			t.Errorf("#%d, m.Type = %v, want %v", i, g, voteRespMsgType(msgType))
		}
		if g := msgs[0].Reject; g != tt.wreject {
			t.Errorf("#%d, m.Reject = %v, want %v", i, g, tt.wreject)
		}
	}
}

func TestStateTransition(t *testing.T) {
	tests := []struct {
		from   fsmState
		to     fsmState
		wallow bool
		wterm  uint64
		wlead  uint64
	}{
		{stateFollower, stateFollower, true, 1, NoLeader},
		{stateFollower, statePreCandidate, true, 0, NoLeader},
		{stateFollower, stateCandidate, true, 1, NoLeader},
		{stateFollower, stateLeader, false, 0, NoLeader},

		{statePreCandidate, stateFollower, true, 0, NoLeader},
		{statePreCandidate, statePreCandidate, true, 0, NoLeader},
		{statePreCandidate, stateCandidate, true, 1, NoLeader},
		{statePreCandidate, stateLeader, true, 0, 1},

		{stateCandidate, stateFollower, true, 0, NoLeader},
		{stateCandidate, statePreCandidate, true, 0, NoLeader},
		{stateCandidate, stateCandidate, true, 1, NoLeader},
		{stateCandidate, stateLeader, true, 0, 1},

		{stateLeader, stateFollower, true, 1, NoLeader},
		{stateLeader, statePreCandidate, false, 0, NoLeader},
		{stateLeader, stateCandidate, false, 1, NoLeader},
		{stateLeader, stateLeader, true, 0, 1},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wallow {
						t.Errorf("%d: allow = %v, want %v", i, false, true)
					}
				}
			}()

			sm := newTestRaftFsm(1, 10, newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1)))
			sm.state = tt.from

			switch tt.to {
			case stateFollower:
				sm.becomeFollower(tt.wterm, tt.wlead)
			case statePreCandidate:
				sm.becomePreCandidate()
			case stateCandidate:
				sm.becomeCandidate()
			case stateLeader:
				sm.becomeLeader()
			}

			if sm.term != tt.wterm {
				t.Errorf("%d: term = %d, want %d", i, sm.term, tt.wterm)
			}
			if sm.leader != tt.wlead {
				t.Errorf("%d: lead = %d, want %d", i, sm.leader, tt.wlead)
			}
		}()
	}
}

func TestAllServerStepdown(t *testing.T) {
	tests := []struct {
		state fsmState

		wstate fsmState
		wterm  uint64
		windex uint64
	}{
		{stateFollower, stateFollower, 3, 0},
		{statePreCandidate, stateFollower, 3, 0},
		{stateCandidate, stateFollower, 3, 0},
		{stateLeader, stateFollower, 3, 1},
	}

	tmsgTypes := [...]proto.MsgType{proto.ReqMsgVote, proto.ReqMsgAppend}
	tterm := uint64(3)

	for i, tt := range tests {
		sm := newTestRaftFsm(1, 10, newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1)))
		switch tt.state {
		case stateFollower:
			sm.becomeFollower(1, NoLeader)
		case statePreCandidate:
			sm.becomePreCandidate()
		case stateCandidate:
			sm.becomeCandidate()
		case stateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		for j, msgType := range tmsgTypes {
			sm.Step(&proto.Message{From: 2, Type: msgType, Term: tterm, LogTerm: tterm})

			if sm.state != tt.wstate {
				t.Errorf("#%d.%d state = %v , want %v", i, j, sm.state, tt.wstate)
			}
			if sm.term != tt.wterm {
				t.Errorf("#%d.%d term = %v , want %v", i, j, sm.term, tt.wterm)
			}
			if sm.raftLog.lastIndex() != tt.windex {
				t.Errorf("#%d.%d index = %v , want %v", i, j, sm.raftLog.lastIndex(), tt.windex)
			}
			if uint64(len(sm.raftLog.allEntries())) != tt.windex {
				t.Errorf("#%d.%d len(ents) = %v , want %v", i, j, len(sm.raftLog.allEntries()), tt.windex)
			}
			wlead := uint64(2)
			if msgType == proto.ReqMsgVote {
				wlead = NoLeader
			}
			if sm.leader != wlead {
				t.Errorf("#%d, sm.leader = %d, want %d", i, sm.leader, NoLeader)
			}
		}
	}
}

func TestCandidateResetTermMsgHeartbeat(t *testing.T) {
	testCandidateResetTerm(t, proto.ReqMsgHeartBeat)
}

func TestCandidateResetTermMsgApp(t *testing.T) {
	testCandidateResetTerm(t, proto.ReqMsgAppend)
}

// testCandidateResetTerm tests when a candidate receives a
// MsgHeartbeat or proto.ReqMsgAppend from leader, "Step" resets the term
// with leader's and reverts back to follower.
func testCandidateResetTerm(t *testing.T, mt proto.MsgType) {
	a := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	b := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	c := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	nt := newNetwork(a, b, c)

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	if a.state != stateLeader {
		t.Errorf("state = %s, want %v", a.state, stateLeader)
	}
	if b.state != stateFollower {
		t.Errorf("state = %s, want %v", b.state, stateFollower)
	}
	if c.state != stateFollower {
		t.Errorf("state = %s, want %v", c.state, stateFollower)
	}

	// isolate 3 and increase term in rest
	nt.isolate(3)

	nt.send(proto.Message{From: 2, To: 2, Type: proto.LocalMsgHup})
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	if a.state != stateLeader {
		t.Errorf("state = %s, want %v", a.state, stateLeader)
	}
	if b.state != stateFollower {
		t.Errorf("state = %s, want %v", b.state, stateFollower)
	}

	// trigger campaign in isolated c
	c.resetRandomizedElectionTimeout()
	for i := 0; i < c.randElectionTick; i++ {
		c.tick()
	}

	if c.state != stateCandidate {
		t.Errorf("state = %s, want %v", c.state, stateCandidate)
	}

	nt.recover()

	// leader sends to isolated candidate
	// and expects candidate to revert to follower
	nt.send(proto.Message{From: 1, To: 3, Term: a.term, Type: mt})

	if c.state != stateFollower {
		t.Errorf("state = %s, want %v", c.state, stateFollower)
	}

	// follower c term is reset with leader's
	if a.term != c.term {
		t.Errorf("follower term expected same term as leader's %d, got %d", a.term, c.term)
	}
}

func TestLeaderStepdownWhenQuorumActive(t *testing.T) {
	sm := newTestRaftFsm(5, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	sm.config.LeaseCheck = true

	sm.becomeCandidate()
	sm.becomeLeader()

	for i := 0; i < sm.randElectionTick+1; i++ {
		sm.Step(&proto.Message{From: 2, Type: proto.RespMsgHeartBeat, Term: sm.term})
		sm.tick()
	}

	if sm.state != stateLeader {
		t.Errorf("state = %v, want %v", sm.state, stateLeader)
	}
}

func TestLeaderStepdownWhenQuorumLost(t *testing.T) {
	sm := newTestRaftFsm(5, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	sm.config.LeaseCheck = true

	sm.becomeCandidate()
	sm.becomeLeader()

	for i := 0; i < sm.randElectionTick+1; i++ {
		sm.tick()
	}

	if sm.state != stateFollower {
		t.Errorf("state = %v, want %v", sm.state, stateFollower)
	}
}

func TestLeaderSupersedingWithCheckQuorum(t *testing.T) {
	a := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	b := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	c := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	a.config.LeaseCheck = true
	b.config.LeaseCheck = true
	c.config.LeaseCheck = true

	nt := newNetwork(a, b, c)
	b.randElectionTick = b.randElectionTick + 1

	for i := 0; i < b.randElectionTick; i++ {
		b.tick()
	}
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	if a.state != stateLeader {
		t.Errorf("state = %s, want %v", a.state, stateLeader)
	}

	if c.state != stateFollower {
		t.Errorf("state = %s, want %v", c.state, stateFollower)
	}

	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	// Peer b rejected c's vote since its electionElapsed had not reached to randElectionTick
	if c.state != stateCandidate {
		t.Errorf("state = %s, want %v", c.state, stateCandidate)
	}

	// Letting b's electionElapsed reach to randElectionTick
	for i := 0; i < b.randElectionTick; i++ {
		b.tick()
	}
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	if c.state != stateLeader {
		t.Errorf("state = %s, want %v", c.state, stateLeader)
	}
}

func TestLeaderElectionWithCheckQuorum(t *testing.T) {
	a := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	b := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	c := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	a.config.LeaseCheck = true
	b.config.LeaseCheck = true
	c.config.LeaseCheck = true

	nt := newNetwork(a, b, c)
	a.randElectionTick += 1
	b.randElectionTick += 2

	// Immediately after creation, votes are cast regardless of the
	// election timeout.
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	if a.state != stateLeader {
		t.Errorf("state = %s, want %v", a.state, stateLeader)
	}

	if c.state != stateFollower {
		t.Errorf("state = %s, want %v", c.state, stateFollower)
	}

	// need to reset randomizedElectionTimeout larger than randElectionTick again,
	// because the value might be reset to randElectionTick since the last state changes
	a.randElectionTick += 1
	b.randElectionTick += 2
	for i := 0; i < a.randElectionTick; i++ {
		a.tick()
	}
	for i := 0; i < b.randElectionTick; i++ {
		b.tick()
	}
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	if a.state != stateFollower {
		t.Errorf("state = %s, want %v", a.state, stateFollower)
	}

	if c.state != stateLeader {
		t.Errorf("state = %s, want %v", c.state, stateLeader)
	}
}

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
func TestFreeStuckCandidateWithCheckQuorum(t *testing.T) {
	a := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	b := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	c := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	a.config.LeaseCheck = true
	b.config.LeaseCheck = true
	c.config.LeaseCheck = true

	nt := newNetwork(a, b, c)
	b.randElectionTick += 1

	for i := 0; i < b.randElectionTick; i++ {
		b.tick()
	}
	// first a selected leader
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
	if a.state != stateLeader {
		t.Errorf("state = %s, want %v", b.state, stateFollower)
	}

	nt.isolate(1)
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	if b.state != stateFollower {
		t.Errorf("state = %s, want %v", b.state, stateFollower)
	}

	if c.state != stateCandidate {
		t.Errorf("state = %s, want %v", c.state, stateCandidate)
	}

	if c.term != b.term+1 {
		t.Errorf("term = %d, want %d", c.term, b.term+1)
	}

	// Vote again for safety
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	if b.state != stateFollower {
		t.Errorf("state = %s, want %v", b.state, stateFollower)
	}

	if c.state != stateCandidate {
		t.Errorf("state = %s, want %v", c.state, stateCandidate)
	}

	if c.term != b.term+2 {
		t.Errorf("term = %d, want %d", c.term, b.term+2)
	}

	// c term is bigger than a, a should change to follower
	nt.recover()
	nt.send(proto.Message{From: 1, To: 3, Type: proto.ReqMsgHeartBeat, Term: a.term})

	// Disrupt the leader so that the stuck peer is freed
	if a.state != stateFollower {
		t.Errorf("state = %s, want %v", a.state, stateFollower)
	}

	if c.term != a.term {
		t.Errorf("term = %d, want %d", c.term, a.term)
	}

	// Vote again, should become leader this time
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	if c.state != stateLeader {
		t.Errorf("peer 3 state: %s, want %v", c.state, stateLeader)
	}
}

func TestNonPromotableVoterWithCheckQuorum(t *testing.T) {
	a := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2)))
	b := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1)))

	a.config.LeaseCheck = true
	b.config.LeaseCheck = true

	nt := newNetwork(a, b)
	b.randElectionTick += 1
	// Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
	b.applyConfChange(&proto.ConfChange{Type: proto.ConfRemoveNode, Peer: proto.Peer{PeerID: 2, ID: 2}})

	if b.promotable() {
		t.Fatalf("promotable = %v, want false", b.promotable())
	}

	for i := 0; i < b.randElectionTick; i++ {
		b.tick()
	}
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	if a.state != stateLeader {
		t.Errorf("state = %s, want %v", a.state, stateLeader)
	}

	if b.state != stateFollower {
		t.Errorf("state = %s, want %v", b.state, stateFollower)
	}

	if b.leader != 1 {
		t.Errorf("lead = %d, want 1", b.leader)
	}
}

//TestDisruptiveFollower tests isolated follower,
//with slow network incoming from leader, election times out
//to become a candidate with an increased term. Then, the
//candiate's response to late leader heartbeat forces the leader
//to step down.
func TestDisruptiveFollower(t *testing.T) {
	n1 := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n2 := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n3 := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	n1.config.LeaseCheck = true
	n2.config.LeaseCheck = true
	n3.config.LeaseCheck = true

	n1.becomeFollower(1, NoLeader)
	n2.becomeFollower(1, NoLeader)
	n3.becomeFollower(1, NoLeader)

	nt := newNetwork(n1, n2, n3)

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	// check state
	// n1.state == stateLeader
	// n2.state == stateFollower
	// n3.state == stateFollower
	if n1.state != stateLeader {
		t.Fatalf("node 1 state: %s, want %v", n1.state, stateLeader)
	}
	if n2.state != stateFollower {
		t.Fatalf("node 2 state: %s, want %v", n2.state, stateFollower)
	}
	if n3.state != stateFollower {
		t.Fatalf("node 3 state: %s, want %v", n3.state, stateFollower)
	}

	// etcd server "advanceTicksForElection" on restart;
	// this is to expedite campaign trigger when given larger
	// election timeouts (e.g. multi-datacenter deploy)
	// Or leader messages are being delayed while ticks elapse
	n3.randElectionTick += 2
	for i := 0; i < n3.randElectionTick-1; i++ {
		n3.tick()
	}

	// ideally, before last election tick elapses,
	// the follower n3 receives "proto.MsgApp" or "proto.ReqMsgHeartBeat"
	// from leader n1, and then resets its "electionElapsed"
	// however, last tick may elapse before receiving any
	// messages from leader, thus triggering campaign
	n3.tick()

	// n1 is still leader yet
	// while its heartbeat to candidate n3 is being delayed

	// check state
	// n1.state == stateLeader
	// n2.state == stateFollower
	// n3.state == stateCandidate
	if n1.state != stateLeader {
		t.Fatalf("node 1 state: %s, want %v", n1.state, stateLeader)
	}
	if n2.state != stateFollower {
		t.Fatalf("node 2 state: %s, want %v", n2.state, stateFollower)
	}
	if n3.state != stateCandidate {
		t.Fatalf("node 3 state: %s, want %v", n3.state, stateCandidate)
	}
	// check term
	// n1.term == 2
	// n2.term == 2
	// n3.term == 3
	if n1.term != 2 {
		t.Fatalf("node 1 term: %d, want %d", n1.term, 2)
	}
	if n2.term != 2 {
		t.Fatalf("node 2 term: %d, want %d", n2.term, 2)
	}
	if n3.term != 3 {
		t.Fatalf("node 3 term: %d, want %d", n3.term, 3)
	}

	// while outgoing vote requests are still queued in n3,
	// leader heartbeat finally arrives at candidate n3
	// however, due to delayed network from leader, leader
	// heartbeat was sent with lower term than candidate's
	nt.send(proto.Message{From: 1, To: 3, Term: n1.term, Type: proto.ReqMsgHeartBeat})

	// then candidate n3 responds with "proto.RespMsgAppend" of higher term
	// and leader steps down from a message with higher term
	// this is to disrupt the current leader, so that candidate
	// with higher term can be freed with following election

	// check state
	// n1.state == stateFollower
	// n2.state == stateFollower
	// n3.state == stateCandidate
	if n1.state != stateFollower {
		t.Fatalf("node 1 state: %s, want %v", n1.state, stateFollower)
	}
	if n2.state != stateFollower {
		t.Fatalf("node 2 state: %s, want %v", n2.state, stateFollower)
	}
	if n3.state != stateCandidate {
		t.Fatalf("node 3 state: %s, want %v", n3.state, stateCandidate)
	}
	// check term
	// n1.term == 3
	// n2.term == 2
	// n3.term == 3
	if n1.term != 3 {
		t.Fatalf("node 1 term: %d, want %d", n1.term, 3)
	}
	if n2.term != 2 {
		t.Fatalf("node 2 term: %d, want %d", n2.term, 2)
	}
	if n3.term != 3 {
		t.Fatalf("node 3 term: %d, want %d", n3.term, 3)
	}
}

// TestDisruptiveFollowerPreVote tests isolated follower,
// with slow network incoming from leader, election times out
// to become a pre-candidate with less log than current leader.
// Then pre-vote phase prevents this isolated node from forcing
// current leader to step down, thus less disruptions.
func TestDisruptiveFollowerPreVote(t *testing.T) {
	n1 := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n2 := newTestRaftFsm(10, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n3 := newTestRaftFsm(10, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	n1.config.LeaseCheck = true
	n2.config.LeaseCheck = true
	n3.config.LeaseCheck = true

	n1.becomeFollower(1, NoLeader)
	n2.becomeFollower(1, NoLeader)
	n3.becomeFollower(1, NoLeader)

	nt := newNetwork(n1, n2, n3)

	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})

	// check state
	// n1.state == stateLeader
	// n2.state == stateFollower
	// n3.state == stateFollower
	if n1.state != stateLeader {
		t.Fatalf("node 1 state: %s, want %v", n1.state, stateLeader)
	}
	if n2.state != stateFollower {
		t.Fatalf("node 2 state: %s, want %v", n2.state, stateFollower)
	}
	if n3.state != stateFollower {
		t.Fatalf("node 3 state: %s, want %v", n3.state, stateFollower)
	}

	nt.isolate(3)
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Data: []byte("somedata")}}})
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Data: []byte("somedata")}}})
	nt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgProp, Entries: []*proto.Entry{{Data: []byte("somedata")}}})
	n1.config.PreVote = true
	n2.config.PreVote = true
	n3.config.PreVote = true
	nt.recover()
	nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})

	// check state
	// n1.state == stateLeader
	// n2.state == stateFollower
	// n3.state == statePreCandidate
	if n1.state != stateLeader {
		t.Fatalf("node 1 state: %s, want %v", n1.state, stateLeader)
	}
	if n2.state != stateFollower {
		t.Fatalf("node 2 state: %s, want %v", n2.state, stateFollower)
	}
	if n3.state != statePreCandidate {
		t.Fatalf("node 3 state: %s, want %v", n3.state, statePreCandidate)
	}
	// check term
	// n1.term == 2
	// n2.term == 2
	// n3.term == 2
	if n1.term != 2 {
		t.Fatalf("node 1 term: %d, want %d", n1.term, 2)
	}
	if n2.term != 2 {
		t.Fatalf("node 2 term: %d, want %d", n2.term, 2)
	}
	if n3.term != 2 {
		t.Fatalf("node 2 term: %d, want %d", n3.term, 2)
	}

	// delayed leader heartbeat does not force current leader to step down
	nt.send(proto.Message{From: 1, To: 3, Term: n1.term, Type: proto.ReqMsgHeartBeat})
	if n1.state != stateLeader {
		t.Fatalf("node 1 state: %s, want %v", n1.state, stateLeader)
	}
}

func TestFastLogRejection(t *testing.T) {
	tests := []struct {
		leaderLog       []*proto.Entry // Logs on the leader
		followerLog     []*proto.Entry // Logs on the follower
		rejectHintTerm  uint64         // Expected term included in rejected MsgAppResp.
		rejectHintIndex uint64         // Expected index included in rejected MsgAppResp.
		nextAppendTerm  uint64         // Expected term when leader appends after rejected.
		nextAppendIndex uint64         // Expected index when leader appends after rejected.
	}{
		// This case tests that leader can find the conflict index quickly.
		// Firstly leader appends (type=MsgApp,index=7,logTerm=4, entries=...);
		// After rejected leader appends (type=MsgApp,index=3,logTerm=2).
		{
			leaderLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 2},
				{Term: 2, Index: 3},
				{Term: 4, Index: 4},
				{Term: 4, Index: 5},
				{Term: 4, Index: 6},
				{Term: 4, Index: 7},
			},
			followerLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 2},
				{Term: 2, Index: 3},
				{Term: 3, Index: 4},
				{Term: 3, Index: 5},
				{Term: 3, Index: 6},
				{Term: 3, Index: 7},
				{Term: 3, Index: 8},
				{Term: 3, Index: 9},
				{Term: 3, Index: 10},
				{Term: 3, Index: 11},
			},
			rejectHintTerm:  3,
			rejectHintIndex: 7,
			nextAppendTerm:  2,
			nextAppendIndex: 3,
		},
		// This case tests that leader can find the conflict index quickly.
		// Firstly leader appends (type=MsgApp,index=8,logTerm=5, entries=...);
		// After rejected leader appends (type=MsgApp,index=4,logTerm=3).
		{
			leaderLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 2},
				{Term: 2, Index: 3},
				{Term: 3, Index: 4},
				{Term: 4, Index: 5},
				{Term: 4, Index: 6},
				{Term: 4, Index: 7},
				{Term: 5, Index: 8},
			},
			followerLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 2},
				{Term: 2, Index: 3},
				{Term: 3, Index: 4},
				{Term: 3, Index: 5},
				{Term: 3, Index: 6},
				{Term: 3, Index: 7},
				{Term: 3, Index: 8},
				{Term: 3, Index: 9},
				{Term: 3, Index: 10},
				{Term: 3, Index: 11},
			},
			rejectHintTerm:  3,
			rejectHintIndex: 8,
			nextAppendTerm:  3,
			nextAppendIndex: 4,
		},
		// This case tests that follower can find the conflict index quickly.
		// Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
		// After rejected leader appends (type=MsgApp,index=1,logTerm=1).
		{
			leaderLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
				{Term: 1, Index: 4},
			},
			followerLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 2},
				{Term: 2, Index: 3},
				{Term: 4, Index: 4},
			},
			rejectHintTerm:  1,
			rejectHintIndex: 1,
			nextAppendTerm:  1,
			nextAppendIndex: 1,
		},
		// This case is similar to the previous case. However, this time, the
		// leader has a longer uncommitted log tail than the follower.
		// Firstly leader appends (type=MsgApp,index=6,logTerm=1, entries=...);
		// After rejected leader appends (type=MsgApp,index=1,logTerm=1).
		{
			leaderLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
				{Term: 1, Index: 4},
				{Term: 1, Index: 5},
				{Term: 1, Index: 6},
			},
			followerLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 2},
				{Term: 2, Index: 3},
				{Term: 4, Index: 4},
			},
			rejectHintTerm:  1,
			rejectHintIndex: 1,
			nextAppendTerm:  1,
			nextAppendIndex: 1,
		},
		// This case is similar to the previous case. However, this time, the
		// follower has a longer uncommitted log tail than the leader.
		// Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
		// After rejected leader appends (type=MsgApp,index=1,logTerm=1).
		{
			leaderLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
				{Term: 1, Index: 4},
			},
			followerLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 2},
				{Term: 2, Index: 3},
				{Term: 4, Index: 4},
				{Term: 4, Index: 5},
				{Term: 4, Index: 6},
			},
			rejectHintTerm:  1,
			rejectHintIndex: 1,
			nextAppendTerm:  1,
			nextAppendIndex: 1,
		},
		// An normal case that there are no log conflicts.
		// Firstly leader appends (type=MsgApp,index=5,logTerm=5, entries=...);
		// After rejected leader appends (type=MsgApp,index=4,logTerm=4).
		{
			leaderLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
				{Term: 4, Index: 4},
				{Term: 5, Index: 5},
			},
			followerLog: []*proto.Entry{
				{Term: 1, Index: 1},
				{Term: 1, Index: 2},
				{Term: 1, Index: 3},
				{Term: 4, Index: 4},
			},
			rejectHintTerm:  4,
			rejectHintIndex: 4,
			nextAppendTerm:  4,
			nextAppendIndex: 4,
		},
		// Test case from example comment in stepLeader (on leader).
		{
			leaderLog: []*proto.Entry{
				{Term: 2, Index: 1},
				{Term: 5, Index: 2},
				{Term: 5, Index: 3},
				{Term: 5, Index: 4},
				{Term: 5, Index: 5},
				{Term: 5, Index: 6},
				{Term: 5, Index: 7},
				{Term: 5, Index: 8},
				{Term: 5, Index: 9},
			},
			followerLog: []*proto.Entry{
				{Term: 2, Index: 1},
				{Term: 4, Index: 2},
				{Term: 4, Index: 3},
				{Term: 4, Index: 4},
				{Term: 4, Index: 5},
				{Term: 4, Index: 6},
			},
			rejectHintTerm:  4,
			rejectHintIndex: 6,
			nextAppendTerm:  2,
			nextAppendIndex: 1,
		},
		// Test case from example comment in handleAppendEntries (on follower).
		{
			leaderLog: []*proto.Entry{
				{Term: 2, Index: 1},
				{Term: 2, Index: 2},
				{Term: 2, Index: 3},
				{Term: 2, Index: 4},
				{Term: 2, Index: 5},
			},
			followerLog: []*proto.Entry{
				{Term: 2, Index: 1},
				{Term: 4, Index: 2},
				{Term: 4, Index: 3},
				{Term: 4, Index: 4},
				{Term: 4, Index: 5},
				{Term: 4, Index: 6},
				{Term: 4, Index: 7},
				{Term: 4, Index: 8},
			},
			nextAppendTerm:  2,
			nextAppendIndex: 1,
			rejectHintTerm:  2,
			rejectHintIndex: 1,
		},
	}

	for i, test := range tests {
		t.Run("", func(t *testing.T) {
			s1 := stor.DefaultMemoryStorage()
			s1.StoreEntries(test.leaderLog)
			n1 := newTestRaftFsm(10, 1,
				newTestRaftConfig(1, withStorage(s1), withPeers(1, 2)))
			s2 := stor.DefaultMemoryStorage()
			s2.StoreEntries(test.followerLog)
			n2 := newTestRaftFsm(10, 1,
				newTestRaftConfig(2, withStorage(s2), withPeers(1, 2)))

			n1.becomeCandidate()
			n1.becomeLeader()

			n2.Step(&proto.Message{From: 1, To: 2, Term: n1.term, Type: proto.ReqMsgHeartBeat})
			// CubeFS not response heartbeat in raft node, so need let leader step RespMsgHeartBeat
			n1.Step(&proto.Message{From: 2, To: 1, Term: n2.term, Type: proto.RespMsgHeartBeat})
			msgs := n1.readMessages()
			if len(msgs) != 1 {
				t.Errorf("can't read 1 message from peer 1")
			}
			if msgs[0].Type != proto.ReqMsgAppend {
				t.Errorf("can't read append from peer 1")
			}
			n2.Step(&msgs[0])
			msgs = n2.readMessages()
			if len(msgs) != 1 {
				t.Errorf("can't read 1 message from peer 2")
			}
			if msgs[0].Type != proto.RespMsgAppend {
				t.Errorf("can't read append response from peer 2")
			}
			if !msgs[0].Reject {
				t.Errorf("expected rejected append response from peer 2")
			}
			if msgs[0].LogTerm != test.rejectHintTerm {
				t.Fatalf("#%d expected hint log term = %d, but got %d", i, test.rejectHintTerm, msgs[0].LogTerm)
			}
			if msgs[0].RejectHint != test.rejectHintIndex {
				t.Fatalf("#%d expected hint index = %d, but got %d", i, test.rejectHintIndex, msgs[0].RejectHint)
			}

			n1.Step(&msgs[0])
			msgs = n1.readMessages()
			if msgs[0].LogTerm != test.nextAppendTerm {
				t.Fatalf("#%d expected log term = %d, but got %d", i, test.nextAppendTerm, msgs[0].LogTerm)
			}
			if msgs[0].Index != test.nextAppendIndex {
				t.Fatalf("#%d expected index = %d, but got %d", i, test.nextAppendIndex, msgs[0].Index)
			}
		})
	}
}
