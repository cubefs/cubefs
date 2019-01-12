// Copyright 2015 The etcd Authors
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
	"math/rand"
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

func TestRemovePeer(t *testing.T) {
	peer := proto.Peer{
		ID:     1,
		PeerID: 10,
	}

	r := &raftFsm{
		config: &Config{
			NodeID:       1,
			ElectionTick: 10,
		},
		rand:     rand.New(rand.NewSource(1)),
		replicas: map[uint64]*replica{peer.ID: newReplica(peer, 100)},
	}

	removedPeer := proto.Peer{
		ID:     1,
		PeerID: 2,
	}

	r.removePeer(removedPeer)
	if len(r.replicas) != 1 {
		t.Errorf("expected replicas size = 1")
	}

	removedPeer.PeerID = peer.PeerID
	r.removePeer(removedPeer)
	if len(r.replicas) != 0 {
		t.Error("expected replicas size = 0")
	}

	time.Sleep(time.Second)
}
