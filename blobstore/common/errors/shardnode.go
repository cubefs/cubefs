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

const (
	CodeShardNodeNotLeader          = 1001
	CodeShardRangeMismatch          = 1002
	CodeShardDoesNotExist           = 1003
	CodeShardNodeDiskNotFound       = 1004
	CodeUnknownField                = 1005
	CodeShardRouteVersionNeedUpdate = 1006
	CodeShardNoLeader               = 1007
	CodeIllegalSlices               = 1008
	CodeBlobAlreadyExists           = 1009
	CodeUnsupport                   = 1010
)

// 10xx
var (
	ErrShardNodeNotLeader          = newError(CodeShardNodeNotLeader, "shard node is not leader")
	ErrShardRangeMismatch          = newError(CodeShardRangeMismatch, "shard range mismatch")
	ErrShardDoesNotExist           = newError(CodeShardDoesNotExist, "shard doest not exist")
	ErrShardNodeDiskNotFound       = newError(CodeShardNodeDiskNotFound, "shard disk not found")
	ErrUnknownField                = newError(CodeUnknownField, "unknown field")
	ErrShardRouteVersionNeedUpdate = newError(CodeShardRouteVersionNeedUpdate, "shard route version need update")
	ErrShardNoLeader               = newError(CodeShardNoLeader, "shard has no leader")
	ErrIllegalSlices               = newError(CodeIllegalSlices, "illegal slices")
	ErrBlobAlreadyExists           = newError(CodeBlobAlreadyExists, "blob already exists")
	ErrShardNodeUnsupport          = newError(CodeUnsupport, "unsupport shard node")
)
