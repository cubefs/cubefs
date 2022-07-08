// Copyright 2022 The CubeFS Authors.
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

package raftserver

import (
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type Snapshot interface {
	Read() ([]byte, error)
	Name() string
	Index() uint64
	Close()
}

type (
	ConfChange pb.ConfChange
)

// The StateMachine interface is supplied by the application to persist/snapshot data of application.
type StateMachine interface {
	Apply(data [][]byte, index uint64) error
	ApplyMemberChange(cc ConfChange, index uint64) error
	Snapshot() (Snapshot, error)
	ApplySnapshot(meta SnapshotMeta, st Snapshot) error
	LeaderChange(leader uint64, host string)
}
