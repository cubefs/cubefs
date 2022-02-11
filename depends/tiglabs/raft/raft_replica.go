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
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
)

// replication represents a follower’s progress of replicate in the view of the leader.
// Leader maintains progresses of all followers, and sends entries to the follower based on its progress.
type replica struct {
	inflight
	peer                                proto.Peer
	state                               replicaState
	paused, active, pending             bool
	match, next, committed, pendingSnap uint64

	lastActive time.Time
	lastZombie time.Time
}

func newReplica(peer proto.Peer, maxInflight int) *replica {
	repl := &replica{
		peer:       peer,
		state:      replicaStateProbe,
		lastActive: time.Now(),
	}
	if maxInflight > 0 {
		repl.inflight.size = maxInflight
		repl.inflight.buffer = make([]uint64, maxInflight)
	}

	return repl
}

func (r *replica) resetState(state replicaState) {
	r.paused = false
	r.pendingSnap = 0
	r.state = state
	r.reset()
}

func (r *replica) becomeProbe() {
	if r.state == replicaStateSnapshot {
		pendingSnap := r.pendingSnap
		r.resetState(replicaStateProbe)
		r.next = util.Max(r.match+1, pendingSnap+1)
	} else {
		r.resetState(replicaStateProbe)
		r.next = r.match + 1
	}
}

func (r *replica) becomeReplicate() {
	r.resetState(replicaStateReplicate)
	r.next = r.match + 1
}

func (r *replica) becomeSnapshot(index uint64) {
	r.resetState(replicaStateSnapshot)
	r.pendingSnap = index
}

func (r *replica) update(index uint64) {
	r.next = index + 1
}

func (r *replica) maybeUpdate(index, commit uint64) bool {
	updated := false
	if r.committed < commit {
		r.committed = commit
	}
	if r.match < index {
		r.match = index
		updated = true
		r.resume()
	}
	next := index + 1
	if r.next < next {
		r.next = next
	}
	return updated
}

func (r *replica) maybeDecrTo(rejected, last, commit uint64) bool {
	if r.state == replicaStateReplicate {
		if r.committed < commit {
			r.committed = commit
		}
		if rejected <= r.match {
			return false
		}
		r.next = r.match + 1
		return true
	}
	//Probe State
	if r.next-1 != rejected {
		return false
	}
	if r.next = util.Min(rejected, last+1); r.next < 1 {
		r.next = 1
	}
	r.committed = commit
	r.resume()
	return true
}

func (r *replica) snapshotFailure() { r.pendingSnap = 0 }

func (r *replica) needSnapshotAbort() bool {
	return r.state == replicaStateSnapshot && r.match >= r.pendingSnap
}

func (r *replica) pause() { r.paused = true }

func (r *replica) resume() { r.paused = false }

func (r *replica) isPaused() bool {
	switch r.state {
	case replicaStateProbe:
		return r.paused
	case replicaStateSnapshot:
		return true
	default:
		return r.full()
	}
}

func (r *replica) String() string {
	return fmt.Sprintf("next = %d, match = %d, commit = %d, state = %s, waiting = %v, pendingSnapshot = %d", r.next, r.match, r.committed, r.state, r.isPaused(), r.pendingSnap)
}

// inflight is the replication sliding window,avoid overflowing that sending buffer.
type inflight struct {
	start  int
	count  int
	size   int
	buffer []uint64
}

func (in *inflight) add(index uint64) {
	if in.full() {
		panic(AppPanicError(fmt.Sprint("inflight.add cannot add into a full inflights.")))
	}
	next := in.start + in.count
	if next >= in.size {
		next = next - in.size
	}
	in.buffer[next] = index
	in.count = in.count + 1
}

func (in *inflight) freeTo(index uint64) {
	if in.count == 0 || index < in.buffer[in.start] {
		return
	}
	i, idx := 0, in.start
	for ; i < in.count; i++ {
		if index < in.buffer[idx] {
			break
		}
		if idx = idx + 1; idx >= in.size {
			idx = idx - in.size
		}
	}
	in.count = in.count - i
	in.start = idx
}

func (in *inflight) freeFirstOne() {
	in.freeTo(in.buffer[in.start])
}

func (in *inflight) full() bool {
	return in.count == in.size
}

func (in *inflight) reset() {
	in.count = 0
	in.start = 0
}
