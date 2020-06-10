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
	"errors"
)

var (
	ErrCompacted     = errors.New("requested index is unavailable due to compaction.")
	ErrRaftExists    = errors.New("raft already exists.")
	ErrRaftNotExists = errors.New("raft not exists.")
	ErrNotLeader     = errors.New("raft is not the leader.")
	ErrStopped       = errors.New("raft is already shutdown.")
	ErrSnapping      = errors.New("raft is doing snapshot.")
	ErrRetryLater    = errors.New("retry later")
	ErrPeersEmpty    = errors.New("peers nil or empty")
)

type FatalError struct {
	ID  uint64
	Err error
}

// AppPanicError is panic error when repl occurred fatal error.
// The server will recover this panic and stop the shard repl.
type AppPanicError string

func (pe *AppPanicError) Error() string {
	return "Occurred application logic panic error: " + string(*pe)
}
