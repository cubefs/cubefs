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
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"testing"
)

//func TestLeaderElection(t *testing.T) {
//	testLeaderElection(t, false)
//}
//
//func TestLeaderElectionPreVote(t *testing.T) {
//	testLeaderElection(t, true)
//}
//
//func testLeaderElection(t *testing.T, preVote bool) {
//	var cfg func(*Config)
//	candState := stateCandidate
//	candTerm := uint64(1)
//	if preVote {
//		cfg = preVoteConfig
//		// In pre-vote mode, an election that fails to complete
//		// leaves the node in pre-candidate state without advancing
//		// the term.
//		candState = statePreCandidate
//		candTerm = 0
//	}
//	tests := []struct {
//		*network
//		state   fsmState
//		expTerm uint64
//	}{
//		{newNetworkWithConfig(cfg, nil, nil, nil), stateLeader, 1},
//		{newNetworkWithConfig(cfg, nil, nil, nopStepper), stateLeader, 1},
//		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper), candState, candTerm},
//		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil), candState, candTerm},
//		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil, nil), stateLeader, 1},
//
//		// three logs further along than 0, but in the same term so rejections
//		// are returned instead of the votes being ignored.
//		{newNetworkWithConfig(cfg,
//			nil, entsWithConfig(cfg, 1), entsWithConfig(cfg, 1), entsWithConfig(cfg, 1, 1), nil),
//			stateFollower, 1},
//	}
//
//	for i, tt := range tests {
//		tt.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
//		sm := tt.network.peers[1].(*raftFsm)
//		if sm.state != tt.state {
//			t.Errorf("#%d: state = %s, want %s", i, sm.state, tt.state)
//		}
//		if g := sm.term; g != tt.expTerm {
//			t.Errorf("#%d: term = %d, want %d", i, g, tt.expTerm)
//		}
//	}
//}

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
//func TestLeaderElectionOverwriteNewerLogs(t *testing.T) {
//	testLeaderElectionOverwriteNewerLogs(t, false)
//}
//
//func TestLeaderElectionOverwriteNewerLogsPreVote(t *testing.T) {
//	testLeaderElectionOverwriteNewerLogs(t, true)
//}
//
//func testLeaderElectionOverwriteNewerLogs(t *testing.T, preVote bool) {
//	var cfg func(*Config)
//	if preVote {
//		cfg = preVoteConfig
//	}
//	// This network represents the results of the following sequence of
//	// events:
//	// - Node 1 won the election in term 1.
//	// - Node 1 replicated a log entry to node 2 but died before sending
//	//   it to other nodes.
//	// - Node 3 won the second election in term 2.
//	// - Node 3 wrote an entry to its logs but died without sending it
//	//   to any other nodes.
//	//
//	// At this point, nodes 1, 2, and 3 all have uncommitted entries in
//	// their logs and could win an election at term 3. The winner's log
//	// entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
//	// the case where older log entries are overwritten, so this test
//	// focuses on the case where the newer entries are lost).
//	n := newNetworkWithConfig(cfg,
//		entsWithConfig(cfg, 1),     // Node 1: Won first election
//		entsWithConfig(cfg, 1),     // Node 2: Got logs from node 1
//		entsWithConfig(cfg, 2),     // Node 3: Won second election
//		votedWithConfig(cfg, 3, 2), // Node 4: Voted but didn't get logs
//		votedWithConfig(cfg, 3, 2)) // Node 5: Voted but didn't get logs
//
//	// Node 1 campaigns. The election fails because a quorum of nodes
//	// know about the election that already happened at term 2. Node 1's
//	// term is pushed ahead to 2.
//	n.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
//	sm1 := n.peers[1].(*raftFsm)
//	if sm1.state != stateFollower {
//		t.Errorf("state = %s, want stateFollower", sm1.state)
//	}
//	if sm1.term != 2 {
//		t.Errorf("term = %d, want 2", sm1.term)
//	}
//
//	// Node 1 campaigns again with a higher term. This time it succeeds.
//	n.send(proto.Message{From: 1, To: 1, Type: proto.LocalMsgHup})
//	if sm1.state != stateLeader {
//		t.Errorf("state = %s, want stateLeader", sm1.state)
//	}
//	if sm1.term != 3 {
//		t.Errorf("term = %d, want 3", sm1.term)
//	}
//
//	// Now all nodes agree on a log entry with term 1 at index 1 (and
//	// term 3 at index 2).
//	for i := range n.peers {
//		sm := n.peers[i].(*raftFsm)
//		entries := sm.raftLog.allEntries()
//		if len(entries) != 2 {
//			t.Fatalf("node %d: len(entries) == %d, want 2", i, len(entries))
//		}
//		if entries[0].term != 1 {
//			t.Errorf("node %d: term at index 1 == %d, want 1", i, entries[0].term)
//		}
//		if entries[1].term != 3 {
//			t.Errorf("node %d: term at index 2 == %d, want 3", i, entries[1].term)
//		}
//	}
//}
