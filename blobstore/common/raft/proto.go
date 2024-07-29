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

package raft

import (
	"context"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

const reqIDKey = "req-id"

type (
	StateMachine interface {
		// Apply will notify the state machine to apply all proposal data
		// Note that the rets slice length should be equal to proposal data slice length
		Apply(cxt context.Context, pd []ProposalData, index uint64) (rets []interface{}, err error)
		LeaderChange(peerID uint64) error
		ApplyMemberChange(cc *Member, index uint64) error
		Snapshot() Snapshot
		ApplySnapshot(s Snapshot) error
	}
	Storage interface {
		Get(key []byte) (ValGetter, error)
		Iter(prefix []byte) Iterator
		NewBatch() Batch
		Write(b Batch) error
	}
	// Snapshot return state machine's snapshot data.
	// For load considerations, it's the responsibility of the state machine
	// to limit the snapshot transmitting speed
	Snapshot interface {
		// ReadBatch read batch data for snapshot transmit
		// io.EOF should be return when read end of snapshot
		// Note: it is the responsibility for the caller to close the Batch
		ReadBatch() (Batch, error)
		Index() uint64
		Close() error
	}
	AddressResolver interface {
		Resolve(ctx context.Context, nodeID uint64) (Addr, error)
	}
	Addr interface {
		String() string
	}
	KeyGetter interface {
		Key() []byte
		Close()
	}
	ValGetter interface {
		Value() []byte
		Close()
	}
	Iterator interface {
		SeekTo(key []byte)
		SeekForPrev(prev []byte) error
		ReadNext() (key KeyGetter, val ValGetter, err error)
		ReadPrev() (key KeyGetter, val ValGetter, err error)
		Close()
	}
	Batch interface {
		Put(key, value []byte)
		Delete(key []byte)
		DeleteRange(start []byte, end []byte)
		Data() []byte
		From(data []byte)
		Close()
	}
)

type (
	Stat struct {
		ID             uint64   `json:"id"`
		NodeID         uint64   `json:"node_id"`
		Term           uint64   `json:"term"`
		Vote           uint64   `json:"vote"`
		Commit         uint64   `json:"commit"`
		Leader         uint64   `json:"leader"`
		RaftState      string   `json:"raftState"`
		Applied        uint64   `json:"applied"`
		RaftApplied    uint64   `json:"raftApplied"`
		LeadTransferee uint64   `json:"transferee"`
		Nodes          []uint64 `json:"nodes"`
	}

	ProposalResponse struct {
		Data interface{}
	}

	proposalRequest struct {
		entryType raftpb.EntryType
		data      []byte
		// data      *ProposalData
	}
	proposalResult struct {
		reply interface{}
		err   error
	}
)
