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
	"context"
	"math"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/tiglabs/raft/util"

	"github.com/tiglabs/raft/storage/wal"

	"github.com/cubefs/cubefs/util/log"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

const (
	ExpiredPartitionPrefix = "expired_"
)

// PartitionStatus is a type alias of raft.Status
type PartitionStatus = raft.Status

// Partition wraps necessary methods for raft store partition operation.
// Partition is a shard for multi-raft in RaftSore. RaftStore is based on multi-raft which
// manages multiple raft replication groups at same time through a single
// raft server instance and system resource.
type Partition interface {
	// Start this partition instance
	Start() error

	// Submit submits command data to raft log.
	Submit(cmd []byte) (resp interface{}, err error)

	SubmitWithCtx(ctx context.Context, cmd []byte) (resp interface{}, err error)

	// ChangeMember submits member change event and information to raft log.
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

	// HardState 返回Raft实例已持久化的Commit、Term、Vote信息
	HardState() (hs proto.HardState, err error)

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

	RaftConfig() *raft.Config

	FlushWAL(wait bool) error

	SetWALFileSize(filesize int)

	GetWALFileSize() int

	SetWALFileCacheCapacity(capacity int)

	GetWALFileCacheCapacity() int
}

// Default implementation of the Partition interface.
type partition struct {
	id      uint64
	rc      *raft.RaftConfig
	raft    *raft.RaftServer
	walPath string
	config  *PartitionConfig
	ws      *wal.Storage
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

// HardState 返回当前分片准确且已持久化的Commit、Vote、Term信息
func (p *partition) HardState() (hs proto.HardState, err error) {
	if err = p.initWALStorage(); err != nil {
		return
	}
	hs, err = p.ws.InitialState()
	return
}

// LeaderTerm returns the current term of leader in the raft group.
func (p *partition) LeaderTerm() (leaderID, term uint64) {
	if p == nil || p.raft == nil {
		return
	}
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

func (p *partition) FlushWAL(wait bool) (err error) {
	if p.raft != nil {
		err = p.raft.Flush(p.id, wait)
	}
	return
}

func (p *partition) RaftConfig() *raft.Config {
	return p.raft.Config()
}

func (p *partition) Start() (err error) {
	if err = p.initWALStorage(); err != nil {
		return
	}
	var fi uint64
	if fi, err = p.ws.FirstIndex(); err != nil {
		return
	}
	var li uint64
	if li, err = p.ws.LastIndex(); err != nil {
		return
	}
	peers := make([]proto.Peer, 0)
	for _, peerAddress := range p.config.Peers {
		peers = append(peers, peerAddress.Peer)
	}
	var applied = p.config.GetStartIndex.Get(fi, li)
	p.rc = &raft.RaftConfig{
		ID:           p.config.ID,
		Peers:        peers,
		Leader:       p.config.Leader,
		Term:         p.config.Term,
		Storage:      listenStorage(p.ws, p.config.StorageListener),
		StateMachine: p.config.SM,
		Applied:      applied,
		Learners:     p.config.Learners,
		StrictHS:     p.config.StrictHS,
		StartCommit:  p.config.StartCommit,
		Mode:         p.config.Mode.toRaftConsistencyMode(),
	}
	if ln := p.config.StorageListener; ln != nil {
		var (
			lo, hi uint64
		)
		if hi, err = p.ws.LastIndex(); err != nil {
			return
		}
		lo = uint64(math.Min(float64(applied+1), float64(hi)))
		for lo <= hi {
			var entries []*proto.Entry
			if entries, _, err = p.ws.Entries(lo, hi, util.MB); err != nil {
				return
			}
			if len(entries) == 0 {
				break
			}
			for _, entry := range entries {
				ln.StoredEntry(entry)
			}
			lo = entries[len(entries)-1].Index + 1
		}
	}

	if err = p.raft.CreateRaft(p.rc); err != nil {
		return
	}
	return
}

func (p *partition) initWALStorage() (err error) {
	if p.ws == nil {
		var wc = &wal.Config{
			FileCacheCapacity: p.config.WALFileCacheCapacity,
			FileSize:          p.config.WALFileSize,
			ContinuityCheck:   p.config.WALContinuityCheck,
			ContinuityFix:     p.config.WALContinuityFix,
		}
		p.ws, err = wal.NewStorage(p.walPath, wc)
		if err != nil {
			return
		}
	}
	return
}

func (p *partition) SetWALFileSize(filesize int) {
	if p != nil && p.config != nil {
		p.config.WALFileSize = filesize
		if p.ws != nil {
			p.ws.SetFileSize(filesize)
		}
	}
}

func (p *partition) GetWALFileSize() (filesize int) {
	if p != nil {
		if p.ws != nil {
			filesize = p.ws.GetFileSize()
			return
		}
		if p.config != nil {
			filesize = p.config.WALFileSize
		}
	}
	return
}

func (p *partition) SetWALFileCacheCapacity(capacity int) {
	if p != nil && p.config != nil {
		p.config.WALFileCacheCapacity = capacity
		if p.ws != nil {
			p.ws.SetFileCacheCapacity(capacity)
		}
	}
}

func (p *partition) GetWALFileCacheCapacity() (capacity int) {
	if p != nil {
		if p.ws != nil {
			capacity = p.ws.GetFileCacheCapacity()
			return
		}
		if p.config != nil {
			capacity = p.config.WALFileCacheCapacity
		}
	}
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
