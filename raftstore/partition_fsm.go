// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package raftstore

import (
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

// PartitionFsm wraps necessary methods include both FSM implementation
// and data storage operation for raft store partition.
// It extends from raft StateMachine and Store.
type PartitionFsm = raft.StateMachine

type FunctionalPartitionFsm struct {
	ApplyFunc              func(command []byte, index uint64) (interface{}, error)
	ApplyMemberChangeFunc  func(confChange *proto.ConfChange, index uint64) (interface{}, error)
	SnapshotFunc           func(recoverNode uint64) (proto.Snapshot, error)
	AskRollbackFunc        func(original []byte) (rollback []byte, err error)
	ApplySnapshotFunc      func(peers []proto.Peer, iter proto.SnapIterator, snapV uint32) error
	HandleFatalEventFunc   func(err *raft.FatalError)
	HandleLeaderChangeFunc func(leader uint64)
}

func (f *FunctionalPartitionFsm) Apply(command []byte, index uint64) (interface{}, error) {
	if f != nil && f.ApplyFunc != nil {
		return f.ApplyFunc(command, index)
	}
	return nil, nil
}

func (f *FunctionalPartitionFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	if f != nil && f.ApplyMemberChangeFunc != nil {
		return f.ApplyMemberChangeFunc(confChange, index)
	}
	return nil, nil
}

func (f *FunctionalPartitionFsm) Snapshot(recoverNode uint64) (proto.Snapshot, error) {
	if f != nil && f.SnapshotFunc != nil {
		return f.SnapshotFunc(recoverNode)
	}
	return nil, nil
}

func (f *FunctionalPartitionFsm) AskRollback(original []byte) (rollback []byte, err error) {
	if f != nil && f.AskRollbackFunc != nil {
		return f.AskRollbackFunc(original)
	}
	return nil, nil
}

func (f *FunctionalPartitionFsm) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator, snapV uint32) error {
	if f != nil && f.ApplySnapshotFunc != nil {
		return f.ApplySnapshotFunc(peers, iter, snapV)
	}
	return nil
}

func (f *FunctionalPartitionFsm) HandleFatalEvent(err *raft.FatalError) {
	if f != nil && f.HandleFatalEventFunc != nil {
		f.HandleFatalEventFunc(err)
	}
}

func (f *FunctionalPartitionFsm) HandleLeaderChange(leader uint64) {
	if f != nil && f.HandleLeaderChangeFunc != nil {
		f.HandleLeaderChangeFunc(leader)
	}
}
