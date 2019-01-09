// Copyright 2018 The Container File System Authors.
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
	"github.com/juju/errors"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"os"
)

// Error definitions for raft store partition.
var (
	ErrNotLeader = errors.New("not raft leader")
)

// PartitionStatus is a type alias of raft.Status
type PartitionStatus = raft.Status

// PartitionFsm wraps necessary methods include both FSM implementation
// and data storage operation for raft store partition.
// It extends from raft StateMachine and Store.
type PartitionFsm interface {
	raft.StateMachine
	Store
}

// Partition wraps necessary methods for raft store partition operation.
// Partition is a shard for multi-raft in RaftSore. RaftStore based on multi-raft which
// managing multiple raft replication group at same time through single
// raft server instance and system resource.
type Partition interface {
	// Submit submits command data to raft log.
	Submit(cmd []byte) (resp interface{}, err error)

	// ChaneMember submits member change event and information to raft log.
	ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (resp interface{}, err error)

	// Stop removes this raft partition from raft server and make this partition shutdown.
	Stop() error

	// Delete stop and delete this partition forever.
	Delete() error

	// Status returns current raft status.
	Status() (status *PartitionStatus)

	// LeaderTerm returns current term of leader in raft group.
	LeaderTerm() (leaderID, term uint64)

	// IsRaftLeader returns true if this node current is the leader in the raft group it belong to.
	IsLeader() bool

	// AppliedIndex returns current index value of applied raft log in this raft store partition.
	AppliedIndex() uint64

	// CommittedIndex returns current index value of applied raft log in this raft store partition.
	CommittedIndex() uint64

	// Truncate raft log
	Truncate(index uint64)
}

// This is the default implementation of Partition interface.
type partition struct {
	id      uint64
	raft    *raft.RaftServer
	walPath string
	config  *PartitionConfig
}

func (p *partition) ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (
	resp interface{}, err error) {
	if !p.IsLeader() {
		err = ErrNotLeader
		return
	}
	future := p.raft.ChangeMember(p.id, changeType, peer, context)
	resp, err = future.Response()
	return
}

func (p *partition) Stop() (err error) {
	err = p.raft.RemoveRaft(p.id)
	return
}

func (p *partition) Delete() (err error) {
	if err = p.Stop(); err != nil {
		return
	}
	err = os.RemoveAll(p.walPath)
	return
}

func (p *partition) Status() (status *PartitionStatus) {
	status = p.raft.Status(p.id)
	return
}

func (p *partition) LeaderTerm() (leaderID, term uint64) {
	leaderID, term = p.raft.LeaderTerm(p.id)
	return
}

func (p *partition) IsLeader() (isLeader bool) {
	isLeader = p.raft != nil && p.raft.IsLeader(p.id)
	return
}

func (p *partition) AppliedIndex() (applied uint64) {
	applied = p.raft.AppliedIndex(p.id)
	return
}

func (p *partition) CommittedIndex() (applied uint64) {
	applied = p.raft.CommittedIndex(p.id)
	return
}

func (p *partition) Submit(cmd []byte) (resp interface{}, err error) {
	if !p.IsLeader() {
		err = ErrNotLeader
		return
	}
	future := p.raft.Submit(p.id, cmd)
	resp, err = future.Response()
	return
}

func (p *partition) Truncate(index uint64) {
	if p.raft != nil {
		p.raft.Truncate(p.id, index)
	}
}

func newPartition(cfg *PartitionConfig, raft *raft.RaftServer, walPath string) Partition {
	return &partition{
		id:      cfg.ID,
		raft:    raft,
		walPath: walPath,
		config:  cfg,
	}
}
