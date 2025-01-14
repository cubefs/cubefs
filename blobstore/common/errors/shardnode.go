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
	CodeShardConflicts              = 1011
	CodeKeySizeTooLarge             = 1012
	CodeValueSizeTooLarge           = 1013
	CodeKeyNotFound                 = 1014
	CodeBlobAlreadySealed           = 1015
	CodeBlobNameEmpty               = 1016
	CodeNoEnoughRaftMember          = 1017
	CodeIllegalUpdateUnit           = 1018
)

// 10xx
var (
	ErrShardNodeNotLeader          = Error(CodeShardNodeNotLeader)
	ErrShardRangeMismatch          = Error(CodeShardRangeMismatch)
	ErrShardDoesNotExist           = Error(CodeShardDoesNotExist)
	ErrShardNodeDiskNotFound       = Error(CodeShardNodeDiskNotFound)
	ErrUnknownField                = Error(CodeUnknownField)
	ErrShardRouteVersionNeedUpdate = Error(CodeShardRouteVersionNeedUpdate)
	ErrShardNoLeader               = Error(CodeShardNoLeader)
	ErrIllegalSlices               = Error(CodeIllegalSlices)
	ErrBlobAlreadyExists           = Error(CodeBlobAlreadyExists)
	ErrShardNodeUnsupport          = Error(CodeUnsupport)
	ErrShardConflicts              = Error(CodeShardConflicts)
	ErrKeySizeTooLarge             = Error(CodeKeySizeTooLarge)
	ErrValueSizeTooLarge           = Error(CodeValueSizeTooLarge)
	ErrKeyNotFound                 = Error(CodeKeyNotFound)
	ErrBlobAlreadySealed           = Error(CodeBlobAlreadySealed)
	ErrBlobNameEmpty               = Error(CodeBlobNameEmpty)
	ErrNoEnoughRaftMember          = Error(CodeNoEnoughRaftMember)
	ErrIllegalUpdateUnit           = Error(CodeIllegalUpdateUnit)
)
