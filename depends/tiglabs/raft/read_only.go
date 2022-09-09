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

	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
)

// ReadOnlyOption read only option
type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

type readIndexStatus struct {
	index   uint64
	futures []*Future
	acks    map[uint64]struct{}
}

type readIndexReady struct {
	index   uint64
	futures []*Future
}

type readOnly struct {
	id     uint64 // raft id
	option ReadOnlyOption

	// wait leader to commit an entry in current term
	committed bool
	// ReadIndex requests before leader commit entry in current term
	scratch []*Future

	// wait quorum ack
	pendings     map[uint64]*readIndexStatus
	pendingQueue []uint64

	// quorum acked, wait apply
	readys     map[uint64]*readIndexReady
	readyQueue []uint64
}

func newReadOnly(id uint64, option ReadOnlyOption) *readOnly {
	return &readOnly{
		id:       id,
		option:   option,
		pendings: make(map[uint64]*readIndexStatus),
		readys:   make(map[uint64]*readIndexReady),
	}
}

func (r *readOnly) addPending(index uint64, futures []*Future) {
	if status, ok := r.pendings[index]; ok {
		status.futures = append(status.futures, futures...)
		return
	}

	// check index valid
	if index <= r.lastPending() {
		panic(AppPanicError(fmt.Sprintf("[raft->addReadOnly][%v] invalid index[%d]: less than last[%d]", r.id, index, r.lastPending())))
	}
	r.pendingQueue = append(r.pendingQueue, index)
	r.pendings[index] = &readIndexStatus{
		index:   index,
		futures: futures,
		acks:    make(map[uint64]struct{}),
	}
}

func (r *readOnly) addReady(index uint64, futures []*Future) {
	if status, ok := r.readys[index]; ok {
		status.futures = append(status.futures, futures...)
		return
	}
	r.readyQueue = append(r.readyQueue, index)
	r.readys[index] = &readIndexReady{
		index:   index,
		futures: futures,
	}
}

func (r *readOnly) add(index uint64, futures []*Future) {
	if !r.committed {
		r.scratch = append(r.scratch, futures...)
		return
	}

	if r.option == ReadOnlyLeaseBased {
		r.addReady(index, futures)
	} else {
		r.addPending(index, futures)
	}
}

func (r *readOnly) commit(index uint64) {
	if !r.committed {
		r.committed = true
		if len(r.scratch) > 0 {
			r.add(index, r.scratch)
			r.scratch = nil
		}
	}
}

func (r *readOnly) lastPending() uint64 {
	if len(r.pendingQueue) > 0 {
		return r.pendingQueue[len(r.pendingQueue)-1]
	}
	return 0
}

func (r *readOnly) recvAck(index uint64, from uint64, quorum int) {
	status, ok := r.pendings[index]
	if !ok {
		return
	}
	status.acks[from] = struct{}{}
	// add one to include an ack from local node
	if len(status.acks)+1 >= quorum {
		r.advance(index)
	}
}

func (r *readOnly) advance(index uint64) {
	var i int
	for _, idx := range r.pendingQueue {
		if idx > index {
			break
		}
		if rs, ok := r.pendings[idx]; ok {
			r.addReady(idx, rs.futures)
			delete(r.pendings, idx)
		}
		i++
	}
	r.pendingQueue = r.pendingQueue[i:]
}

func (r *readOnly) getReady(applied uint64) (futures []*Future) {
	if len(r.readyQueue) == 0 {
		return nil
	}

	var i int
	for _, idx := range r.readyQueue {
		if idx > applied {
			break
		}
		if rs, ok := r.readys[idx]; ok {
			futures = append(futures, rs.futures...)
			delete(r.readys, idx)
		}
		i++
	}
	r.readyQueue = r.readyQueue[i:]
	// TODO: remove this when stable
	if logger.IsEnableDebug() {
		logger.Debug("raft[%d] get ready index %d, futures len: %d", r.id, applied, len(futures))
	}
	return
}

func (r *readOnly) containsUpdate(applied uint64) bool {
	return len(r.readyQueue) > 0 && applied >= r.readyQueue[0]
}

func (r *readOnly) reset(err error) {
	respondReadIndex(r.scratch, err)
	for _, status := range r.pendings {
		respondReadIndex(status.futures, err)
	}
	for _, ready := range r.readys {
		respondReadIndex(ready.futures, err)
	}

	r.committed = false
	r.scratch = nil
	r.pendings = make(map[uint64]*readIndexStatus)
	r.pendingQueue = nil
	r.readys = make(map[uint64]*readIndexReady)
}

func respondReadIndex(future []*Future, err error) {
	for _, f := range future {
		f.respond(nil, err)
	}
}
