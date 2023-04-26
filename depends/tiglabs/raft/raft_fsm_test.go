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
	stor "github.com/cubefs/cubefs/depends/tiglabs/raft/storage"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
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
func newNetworkWithConfig(configFunc func(*Config), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]*stor.MemoryStorage, size)

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			s := stor.DefaultMemoryStorage()
			r := newTestRaftFsm(10, 1, newTestRaftConfig(id, withStorage(s), withPeers(peerAddrs...)))
			npeers[id] = r
		case *raftFsm:
			//learners := make(map[uint64]bool, len(v.prs.Learners))
			//for i := range v.prs.Learners {
			//	learners[i] = true
			//}
			//v.id = id
			//v.prs = tracker.MakeProgressTracker(v.prs.MaxInflight)
			//if len(learners) > 0 {
			//	v.prs.Learners = map[uint64]struct{}{}
			//}
			//for i := 0; i < size; i++ {
			//	pr := &tracker.Progress{}
			//	if _, ok := learners[peerAddrs[i]]; ok {
			//		pr.IsLearner = true
			//		v.prs.Learners[peerAddrs[i]] = struct{}{}
			//	} else {
			//		v.prs.Voters[0][peerAddrs[i]] = struct{}{}
			//	}
			//	v.prs.Progress[peerAddrs[i]] = pr
			//}
			//v.reset(v.term)
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

type stateMachine interface {
	Step(m *proto.Message)
	readMessages() []proto.Message
}

type blackHole struct{}

func (blackHole) Step(*proto.Message)           {}
func (blackHole) readMessages() []proto.Message { return nil }

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
func newPreVoteMigrationCluster(t *testing.T) *network {
	n1 := newTestRaftFsmWithLeaseCheck(2, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n2 := newTestRaftFsmWithLeaseCheck(2, 1,
		newTestRaftConfig(2, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	n3 := newTestRaftFsmWithLeaseCheck(2, 1,
		newTestRaftConfig(3, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))

	n1.becomeFollower(1, NoLeader)
	n2.becomeFollower(1, NoLeader)
	n3.becomeFollower(1, NoLeader)

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
	// n3.state == StateCandidate
	if n1.state != stateLeader {
		t.Fatalf("node 1 state: %s, want %d", n1.state, stateLeader)
	}
	if n2.state != stateFollower {
		t.Fatalf("node 2 state: %s, want %d", n2.state, stateFollower)
	}
	if n3.state != stateCandidate {
		t.Fatalf("node 3 state: %s, want %d", n3.state, stateCandidate)
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
	nt.recover()

	return nt
}

func TestPreVoteMigrationCanCompleteElection(t *testing.T) {
	nt := newPreVoteMigrationCluster(t)

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is candidate with term 4, and less log
	n1 := nt.peers[1].(*raftFsm)
	n3 := nt.peers[3].(*raftFsm)

	require.Equal(t, int(n1.state), stateLeader)
	// todo n3 less log will not be leader
	//nt.send(proto.Message{From: 3, To: 3, Type: proto.LocalMsgHup})
	//require.Equal(t, int(n3.state), stateCandidate)
	require.GreaterOrEqual(t, int(n3.term), 4)

}
