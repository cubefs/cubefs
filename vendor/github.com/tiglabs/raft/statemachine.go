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
	"github.com/tiglabs/raft/proto"
)

// The StateMachine interface is supplied by the application to persist/snapshot data of application.
type StateMachine interface {
	Apply(command []byte, index uint64) (interface{}, error)
	ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error)
	Snapshot(recoverNode uint64) (proto.Snapshot, error)
	ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator, snapV uint32) error
	HandleFatalEvent(err *FatalError)
	HandleLeaderChange(leader uint64)
}

type SocketType byte

const (
	HeartBeat SocketType = 0
	Replicate SocketType = 1
)

func (t SocketType) String() string {
	switch t {
	case 0:
		return "HeartBeat"
	case 1:
		return "Replicate"
	}
	return "unkown"
}

// The SocketResolver interface is supplied by the application to resolve NodeID to net.Addr addresses.
type SocketResolver interface {
	NodeAddress(nodeID uint64, stype SocketType) (addr string, err error)
}
