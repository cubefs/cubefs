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

// 2xx
var (
	ErrShardNodeNotLeader          = newError(1001, "shard node is not leader")
	ErrShardRangeMismatch          = newError(1002, "shard range mismatch")
	ErrShardDoesNotExist           = newError(1003, "shard doest not exist")
	ErrShardNodeDiskNotFound       = newError(1004, "shard disk not found")
	ErrUnknownField                = newError(1005, "unknown field")
	ErrShardRouteVersionNeedUpdate = newError(1006, "shard route version need update")
)
