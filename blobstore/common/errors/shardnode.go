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
	CodeShardNodeNotLeader          = 820
	CodeShardRangeMismatch          = 821
	CodeShardDoesNotExist           = 822
	CodeShardNodeDiskNotFound       = 823
	CodeUnknownField                = 824
	CodeShardRouteVersionNeedUpdate = 825
	CodeShardNoLeader               = 826
	CodeIllegalSlices               = 827
	CodeBlobAlreadyExists           = 828
	CodeUnsupport                   = 829
	CodeShardConflicts              = 830
	CodeKeySizeTooLarge             = 831
	CodeValueSizeTooLarge           = 832
	CodeKeyNotFound                 = 833
	CodeBlobAlreadySealed           = 834
	CodeBlobNameEmpty               = 835
	CodeNoEnoughRaftMember          = 836
	CodeIllegalUpdateUnit           = 837
	CodeItemIDEmpty                 = 838
	CodeIllegalLocationSize         = 839
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
	ErrItemIDEmpty                 = Error(CodeItemIDEmpty)
	ErrIllegalLocationSize         = Error(CodeIllegalLocationSize)
)
