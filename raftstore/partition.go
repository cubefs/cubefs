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
	"fmt"
	"os"
	"path"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
)

// PartitionStatus is a type alias of raft.Status for better readability
type PartitionStatus = raft.Status

// PartitionFsm wraps necessary methods include both FSM implementation
// and data storage operation for raft store partition.
// It extends from raft StateMachine and Store.
type PartitionFsm = raft.StateMachine

// Partition wraps necessary methods for raft store partition operation.
// Partition is a shard for multi-raft in RaftStore. RaftStore is based on multi-raft which
// manages multiple raft replication groups at same time through a single
// raft server instance and system resource.
type Partition interface {
	// Submit submits command data to raft log.
	Submit(cmd []byte) (resp interface{}, err error)

	// ChangeMember submits member change event and information to raft log.
	ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (resp interface{}, err error)

	// Stop removes the raft partition from raft server and shuts down this partition.
	Stop() error

	// Delete stops and deletes the partition.
	Delete() error

	// Status returns the current raft status.
	Status() *PartitionStatus

	// IsRestoring returns true if the partition is currently restoring from snapshot.
	// This is much faster than checking status().RestoringSnapshot.
	IsRestoring() bool

	// LeaderTerm returns the current term of leader in the raft group.
	// Returns (0, 0) if no leader is available.
	LeaderTerm() (leaderID, term uint64)

	// IsRaftLeader returns true if this node is the leader of the raft group it belongs to.
	IsRaftLeader() bool

	// AppliedIndex returns the current index of the applied raft log in the raft store partition.
	AppliedIndex() uint64

	// CommittedIndex returns the current index of the committed raft log in the raft store partition.
	CommittedIndex() uint64

	// Truncate truncates the raft log at the specified index.
	Truncate(index uint64)

	// TryToLeader attempts to make the specified node the leader of this partition.
	TryToLeader(nodeID uint64) error

	// IsOfflinePeer returns true if the majority of peers are offline.
	IsOfflinePeer() bool

	// CloseAndBackup closes the partition and backs up the WAL.
	CloseAndBackup() error
	Closed() bool
}

// partition implements the Partition interface
type partition struct {
	id      uint64
	raft    *raft.RaftServer
	walPath string
	config  *PartitionConfig
}

// ChangeMember submits member change event and information to raft log.
func (p *partition) ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (
	resp interface{}, err error,
) {
	if !p.IsRaftLeader() {
		return nil, raft.ErrNotLeader
	}

	future := p.raft.ChangeMember(p.id, changeType, peer, context)
	return future.Response()
}

// Stop removes the raft partition from raft server and shuts down this partition.
func (p *partition) Stop() error {
	return p.raft.RemoveRaft(p.id)
}

// TryToLeader attempts to make the specified node the leader of this partition.
func (p *partition) TryToLeader(nodeID uint64) error {
	future := p.raft.TryToLeader(nodeID)
	_, err := future.Response()
	return err
}

func (p *partition) Closed() bool {
	return p.raft.Closed(p.id)
}

// Delete stops and deletes the partition.
func (p *partition) Delete() error {
	if err := p.Stop(); err != nil {
		return fmt.Errorf("failed to stop partition: %w", err)
	}

	if err := os.RemoveAll(p.walPath); err != nil {
		return fmt.Errorf("failed to remove WAL path %s: %w", p.walPath, err)
	}

	return nil
}

// IsRestoring returns true if the partition is currently restoring from snapshot.
func (p *partition) IsRestoring() bool {
	return p.raft.IsRestoring(p.id)
}

// Status returns the current raft status.
func (p *partition) Status() *PartitionStatus {
	return p.raft.Status(p.id)
}

// LeaderTerm returns the current term of leader in the raft group.
func (p *partition) LeaderTerm() (leaderID, term uint64) {
	if p.raft == nil {
		return 0, 0
	}
	return p.raft.LeaderTerm(p.id)
}

// IsOfflinePeer returns true if the majority of peers are offline.
func (p *partition) IsOfflinePeer() bool {
	status := p.Status()
	active := 0
	sumPeers := 0
	for _, peer := range status.Replicas {
		if peer.Active {
			active++
		}
		sumPeers++
	}

	return active >= (int(sumPeers)/2 + 1)
}

// IsRaftLeader returns true if this node is the leader of the raft group it belongs to.
func (p *partition) IsRaftLeader() bool {
	return p.raft != nil && p.raft.IsLeader(p.id)
}

// AppliedIndex returns the current index of the applied raft log in the raft store partition.
func (p *partition) AppliedIndex() uint64 {
	return p.raft.AppliedIndex(p.id)
}

// CommittedIndex returns the current index of the committed raft log in the raft store partition.
func (p *partition) CommittedIndex() uint64 {
	return p.raft.CommittedIndex(p.id)
}

// Submit submits command data to raft log.
func (p *partition) Submit(cmd []byte) (resp interface{}, err error) {
	if !p.IsRaftLeader() {
		return nil, raft.ErrNotLeader
	}

	future := p.raft.Submit(p.id, cmd)
	return future.Response()
}

// Truncate truncates the raft log at the specified index.
func (p *partition) Truncate(index uint64) {
	if p.raft != nil {
		p.raft.Truncate(p.id, index)
	}
}

// Backup stops and rename the partition.
func (p *partition) CloseAndBackup() (err error) {
	if err = p.Stop(); err != nil {
		return
	}
	if p.config.WalPath != "" {
		err = fmt.Errorf("raft path(%s) can't be backup", p.walPath)
		return
	}
	dirPath, dirName := path.Split(p.walPath)
	backupPath := dirPath + "del_" + dirName
	err = os.Rename(p.walPath, backupPath)
	return
}

func newPartition(cfg *PartitionConfig, raft *raft.RaftServer, walPath string) Partition {
	return &partition{
		id:      cfg.ID,
		raft:    raft,
		walPath: walPath,
		config:  cfg,
	}
}
