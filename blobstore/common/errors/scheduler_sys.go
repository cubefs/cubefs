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

package errors

import (
	"errors"
)

const (
	CodeNotingTodo        = 700
	CodeDestReplicaBad    = 702
	CodeOrphanShard       = 703
	CodeIllegalTask       = 704
	CodeNoInspect         = 705
	CodeClusterIDNotMatch = 706
	CodeRequestLimited    = 707
)

// common
var (
	ErrRequestLimited = Error(CodeRequestLimited)
)

// scheduler
var (
	ErrNoSuchService   = errors.New("no such service")
	ErrIllegalTaskType = errors.New("illegal task type")
	ErrCanNotDropped   = errors.New("disk can not dropped")

	// error code
	ErrNothingTodo = Error(CodeNotingTodo)
	ErrNoInspect   = Error(CodeNoInspect)
)

// worker
var (
	ErrShardMayBeLost = errors.New("shard may be lost")
	// error code
	ErrOrphanShard    = Error(CodeOrphanShard)
	ErrIllegalTask    = Error(CodeIllegalTask)
	ErrDestReplicaBad = Error(CodeDestReplicaBad)
)

//
var (
	ErrClusterIDNotMatch = Error(CodeClusterIDNotMatch)
)
