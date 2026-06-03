// Copyright 2024 The CubeFS Authors.
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

// TestStepLowerTermHeartBeatAlwaysReplies verifies that a follower/leader at a
// higher term always sends back RespMsgAppend when it receives a ReqMsgHeartBeat
// from a stale leader, regardless of LeaseCheck or PreVote settings.
//
// This is the core fix in the commit: in multi-raft the merged heartbeat from the
// new leader continuously resets electionElapsed, so the node will never time out
// and never send MsgVote to the stale leader. We must unconditionally reply so
// the stale leader is forced to step down.
func TestStepLowerTermHeartBeatAlwaysReplies(t *testing.T) {
	tests := []struct {
		name       string
		leaseCheck bool
		preVote    bool
	}{
		{"no_leaseCheck_no_preVote", false, false},
		{"leaseCheck_only", true, false},
		{"preVote_only", false, true},
		{"both_enabled", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRaftFsm(10, 1,
				newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
			r.config.LeaseCheck = tt.leaseCheck
			r.config.PreVote = tt.preVote

			// Advance to term 2 so node is at a higher term than the incoming message.
			r.term = 2

			msgs := r.readMessages()
			if len(msgs) != 0 {
				t.Fatalf("expected no pending messages before step, got %d", len(msgs))
			}

			// Deliver a ReqMsgHeartBeat from a stale leader at term 1.
			r.Step(&proto.Message{From: 2, To: 1, Term: 1, Type: proto.ReqMsgHeartBeat})

			msgs = r.readMessages()
			if len(msgs) != 1 {
				t.Fatalf("[%s] expected exactly 1 response message, got %d", tt.name, len(msgs))
			}
			m := msgs[0]
			if m.Type != proto.RespMsgAppend {
				t.Errorf("[%s] expected RespMsgAppend, got %v", tt.name, m.Type)
			}
			if m.To != 2 {
				t.Errorf("[%s] expected response To=2, got %d", tt.name, m.To)
			}
			if m.Term != r.term {
				t.Errorf("[%s] expected response Term=%d, got %d", tt.name, r.term, m.Term)
			}
		})
	}
}

// TestStepLowerTermAppendAlwaysReplies is the same as the heartbeat test but
// for ReqMsgAppend, which is the other message type that triggers the fix.
func TestStepLowerTermAppendAlwaysReplies(t *testing.T) {
	tests := []struct {
		name       string
		leaseCheck bool
		preVote    bool
	}{
		{"no_leaseCheck_no_preVote", false, false},
		{"leaseCheck_only", true, false},
		{"preVote_only", false, true},
		{"both_enabled", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRaftFsm(10, 1,
				newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
			r.config.LeaseCheck = tt.leaseCheck
			r.config.PreVote = tt.preVote
			r.term = 3

			r.Step(&proto.Message{From: 2, To: 1, Term: 1, Type: proto.ReqMsgAppend})

			msgs := r.readMessages()
			if len(msgs) != 1 {
				t.Fatalf("[%s] expected exactly 1 response message for ReqMsgAppend, got %d", tt.name, len(msgs))
			}
			m := msgs[0]
			if m.Type != proto.RespMsgAppend {
				t.Errorf("[%s] expected RespMsgAppend, got %v", tt.name, m.Type)
			}
			if m.To != 2 {
				t.Errorf("[%s] expected To=2, got %d", tt.name, m.To)
			}
			if m.Term != r.term {
				t.Errorf("[%s] expected Term=%d, got %d", tt.name, r.term, m.Term)
			}
		})
	}
}

// TestStepLowerTermReplyUsesGetMessage verifies the returned message is properly
// initialised via proto.GetMessage() — specifically that Term is set to current
// node term (not zero) so the stale leader can detect the term mismatch.
func TestStepLowerTermReplyCarriesCurrentTerm(t *testing.T) {
	r := newTestRaftFsm(10, 1,
		newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
	r.term = 5

	r.Step(&proto.Message{From: 3, To: 1, Term: 2, Type: proto.ReqMsgHeartBeat})

	msgs := r.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Term != 5 {
		t.Errorf("expected Term=5 (current term), got %d", msgs[0].Term)
	}
}

// TestStepLowerTermOtherMsgTypesIgnored ensures that message types OTHER than
// ReqMsgHeartBeat and ReqMsgAppend at a lower term are silently dropped (no
// reply is sent).
func TestStepLowerTermOtherMsgTypesIgnored(t *testing.T) {
	ignoredTypes := []proto.MsgType{
		proto.ReqMsgVote,
		proto.RespMsgAppend,
		proto.RespMsgHeartBeat,
	}

	for _, mt := range ignoredTypes {
		r := newTestRaftFsm(10, 1,
			newTestRaftConfig(1, withStorage(stor.DefaultMemoryStorage()), withPeers(1, 2, 3)))
		r.term = 3

		r.Step(&proto.Message{From: 2, To: 1, Term: 1, Type: mt})

		msgs := r.readMessages()
		// ReqMsgPreVote at lower term sends a rejection, but other types should be dropped.
		for _, m := range msgs {
			if m.Type == proto.RespMsgAppend && mt != proto.ReqMsgHeartBeat && mt != proto.ReqMsgAppend {
				// unexpected RespMsgAppend for a non-heartbeat/append type
				t.Errorf("msgType %v at lower term unexpectedly produced RespMsgAppend", mt)
			}
		}
	}
}
