// Copyright 2018 The Chubao Authors.
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
	"context"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/util/log"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

const (
	ExpiredPartitionPrefix = "expired_"
)

// PartitionStatus is a type alias of raft.Status
type PartitionStatus = raft.Status

// PartitionFsm wraps necessary methods include both FSM implementation
// and data storage operation for raft store partition.
// It extends from raft StateMachine and Store.
type PartitionFsm = raft.StateMachine

// Partition wraps necessary methods for raft store partition operation.
// Partition is a shard for multi-raft in RaftSore. RaftStore is based on multi-raft which
// manages multiple raft replication groups at same time through a single
// raft server instance and system resource.
type Partition interface {
	// Submit submits command data to raft log.
	Submit(cmd []byte) (resp interface{}, err error)

	SubmitWithCtx(ctx context.Context, cmd []byte) (resp interface{}, err error)

	// ChaneMember submits member change event and information to raft log.
	ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (resp interface{}, err error)

	// ResetMember reset members directly with no submit, be carefully calling this method. It is used only when dead replicas > live ones and can no longer be alive
	ResetMember(peers []proto.Peer, context []byte) (err error)
	// Stop removes the raft partition from raft server and shuts down this partition.
	Stop() error

	// Delete stops and deletes the partition.
	Delete() error

	// Expired stops and marks specified partition as expired.
	Expired() error

	// Status returns the current raft status.
	Status() (status *PartitionStatus)

	// LeaderTerm returns the current term of leader in the raft group. TODO what is term?
	LeaderTerm() (leaderID, term uint64)

	// IsRaftLeader returns true if this node is the leader of the raft group it belongs to.
	IsRaftLeader() bool

	// AppliedIndex returns the current index of the applied raft log in the raft store partition.
	AppliedIndex() uint64

	// CommittedIndex returns the current index of the applied raft log in the raft store partition.
	CommittedIndex() uint64

	// Truncate raft log
	Truncate(index uint64)

	TryToLeader(nodeID uint64) error

	IsOfflinePeer() bool
}

// Default implementation of the Partition interface.
type partition struct {
	id      uint64
	raft    *raft.RaftServer
	walPath string
	config  *PartitionConfig
}

// ChaneMember submits member change event and information to raft log.
func (p *partition) ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (
	resp interface{}, err error) {
	if !p.IsRaftLeader() {
		err = raft.ErrNotLeader
		return
	}
	future := p.raft.ChangeMember(p.id, changeType, peer, context)
	resp, err = future.Response()
	return
}

func (p *partition) ResetMember(peers []proto.Peer, context []byte) (err error) {
	err = p.raft.ResetMember(p.id, peers, context)
	return
}

// Stop removes the raft partition from raft server and shuts down this partition.
func (p *partition) Stop() (err error) {
	err = p.raft.RemoveRaft(p.id)
	return
}

func (p *partition) TryToLeader(nodeID uint64) (err error) {
	future := p.raft.TryToLeader(nodeID)
	_, err = future.Response()
	return
}

// Delete stops and deletes the partition.
func (p *partition) Delete() (err error) {
	if err = p.Stop(); err != nil {
		return
	}
	err = os.RemoveAll(p.walPath)
	return
}

// Expired stops and marks specified partition as expired.
// It renames data path to a new name which add 'expired_' as prefix and operation timestamp as suffix.
// (e.g. '/path/1' to '/path/expired_1_1600054521')
func (p *partition) Expired() (err error) {
	if err = p.Stop(); err != nil {
		return
	}
	var currentPath = path.Clean(p.walPath)
	var newPath = path.Join(path.Dir(currentPath),
		ExpiredPartitionPrefix+path.Base(currentPath)+"_"+strconv.FormatInt(time.Now().Unix(), 10))
	if err = os.Rename(currentPath, newPath); err != nil {
		log.LogErrorf("Expired: mark expired partition fail: partitionID(%v) path(%v) newPath(%v) err(%v)",
			p.id, p.walPath, newPath, err)
		return
	}
	log.LogInfof("ExpiredPartition: mark expired partition: partitionID(%v) path(%v) newPath(%v)",
		p.id, p.walPath, newPath)
	return
}

// Status returns the current raft status.
func (p *partition) Status() (status *PartitionStatus) {
	if p == nil || p.raft == nil {
		return nil
	}
	status = p.raft.Status(p.id)
	return
}

// LeaderTerm returns the current term of leader in the raft group.
func (p *partition) LeaderTerm() (leaderID, term uint64) {
	leaderID, term = p.raft.LeaderTerm(p.id)
	return
}

func (p *partition) IsOfflinePeer() bool {
	status := p.Status()
	active := 0
	sumPeers := 0
	for _, peer := range status.Replicas {
		if peer.Active == true {
			active++
		}
		sumPeers++
	}

	return active >= (int(sumPeers)/2 + 1)
}

// IsRaftLeader returns true if this node is the leader of the raft group it belongs to.
func (p *partition) IsRaftLeader() (isLeader bool) {
	isLeader = p.raft != nil && p.raft.IsLeader(p.id)
	return
}

// AppliedIndex returns the current index of the applied raft log in the raft store partition.
func (p *partition) AppliedIndex() (applied uint64) {
	applied = p.raft.AppliedIndex(p.id)
	return
}

// CommittedIndex returns the current index of the applied raft log in the raft store partition.
func (p *partition) CommittedIndex() (applied uint64) {
	applied = p.raft.CommittedIndex(p.id)
	return
}

// Submit submits command data to raft log.
func (p *partition) Submit(cmd []byte) (resp interface{}, err error) {
	if !p.IsRaftLeader() {
		err = raft.ErrNotLeader
		return
	}
	future := p.raft.Submit(nil, p.id, cmd)
	resp, err = future.Response()
	return
}

func (p *partition) SubmitWithCtx(ctx context.Context, cmd []byte) (resp interface{}, err error) {
	if !p.IsRaftLeader() {
		err = raft.ErrNotLeader
		return
	}
	future := p.raft.Submit(ctx, p.id, cmd)
	resp, err = future.Response()
	return
}

// Truncate truncates the raft log
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
