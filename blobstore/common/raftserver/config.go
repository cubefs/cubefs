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
	"fmt"
	"path"
)

type Config struct {
	// NodeID is the identity of the local node. NodeID cannot be 0.
	// This parameter is required.
	NodeId     uint64 `json:"nodeId"`
	ListenPort int    `json:"listen_port"`
	WalDir     string `json:"raft_wal_dir"`
	WalSync    bool   `json:"raft_wal_sync"`

	// TickInterval is the interval of timer which check heartbeat and election timeout.
	// The default value is 2s.
	TickInterval int `json:"tick_interval"`
	// HeartbeatTick is the heartbeat interval. A leader sends heartbeat
	// message to maintain the leadership every heartbeat interval.
	// The default value is 1.
	HeartbeatTick int `json:"heartbeat_tick"`
	// ElectionTick is the election timeout. If a follower does not receive any message
	// from the leader of current term during ElectionTick, it will become candidate and start an election.
	// ElectionTick must be greater than HeartbeatTick.
	// We suggest to use ElectionTick = 5 * HeartbeatTick to avoid unnecessary leader switching.
	// The default value is 5.
	ElectionTick int `json:"election_tick"`

	// MaxSnapConcurrency limits the max number of snapshot concurrency.
	// the default value is 10.
	MaxSnapConcurrency int `json:"max_snapshots"`

	// SnapshotTimeout is the snapshot timeout in memory.
	// the default value is 10s
	SnapshotTimeout int `json:"snapshot_timeout"`

	ProposeTimeout int `json:"propose_timeout"`

	Members []Member `json:"-"`

	// Applied is the last applied index. It should only be set when restarting
	Applied uint64 `json:"-"`

	SM StateMachine `json:"-"`
}

func (cfg *Config) Verify() error {
	if cfg.NodeId == 0 {
		return fmt.Errorf("Invalid NodeId=%d", cfg.NodeId)
	}

	if cfg.ListenPort == 0 {
		return fmt.Errorf("Invalid ListenPort=%d", cfg.ListenPort)
	}

	if cfg.WalDir == "" {
		return fmt.Errorf("Invalid WalDir=%s", cfg.WalDir)
	}

	if cfg.SM == nil {
		return fmt.Errorf("StateMachine is nil")
	}

	cfg.WalDir = path.Clean(cfg.WalDir)

	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 2
	}

	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 1
	}

	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 5
	}

	if cfg.MaxSnapConcurrency <= 0 {
		cfg.MaxSnapConcurrency = 10
	}

	if cfg.SnapshotTimeout <= 0 {
		cfg.SnapshotTimeout = 10
	}

	if cfg.ProposeTimeout <= 0 {
		cfg.ProposeTimeout = 10
	}
	return nil
}
