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
	CodeCMUnexpect                   = 900
	CodeLockNotAllow                 = 902
	CodeUnlockNotAllow               = 903
	CodeVolumeNotExist               = 904
	CodeRaftPropose                  = 906
	CodeNoLeader                     = 907
	CodeRaftReadIndex                = 908
	CodeDuplicatedMemberInfo         = 910
	CodeCMDiskNotFound               = 911
	CodeInvalidDiskStatus            = 912
	CodeChangeDiskStatusNotAllow     = 913
	CodeConcurrentAllocVolumeUnit    = 914
	CodeAllocVolumeInvalidParams     = 916
	CodeNoAvailableVolume            = 917
	CodeOldVuidNotMatch              = 918
	CodeNewVuidNotMatch              = 919
	CodeNewDiskIDNotMatch            = 920
	CodeConfigArgument               = 921
	CodeInvalidClusterID             = 922
	CodeInvalidIDC                   = 923
	CodeVolumeUnitNotExist           = 924
	CodeRegisterServiceInvalidParams = 925
	CodeDiskAbnormalOrNotReadOnly    = 926
	CodeStatChunkFailed              = 927
	CodeInvalidCodeMode              = 928
	CodeRetainVolumeNotAlloc         = 929
	CodeDroppedDiskHasVolumeUnit     = 930
	CodeNotSupportIdle               = 931
	CodeDiskIsDropping               = 932
	CodeRejectDeleteSystemConfig     = 933
	CodeCMNodeNotFound               = 934
	CodeCMNodeIsDropping             = 935
	CodeCMNodeSetNotFound            = 936
)

var (
	ErrCMUnexpect                   = Error(CodeCMUnexpect)
	ErrLockNotAllow                 = Error(CodeLockNotAllow)
	ErrUnlockNotAllow               = Error(CodeUnlockNotAllow)
	ErrVolumeNotExist               = Error(CodeVolumeNotExist)
	ErrRaftPropose                  = Error(CodeRaftPropose)
	ErrNoLeader                     = Error(CodeNoLeader)
	ErrRaftReadIndex                = Error(CodeRaftReadIndex)
	ErrDuplicatedMemberInfo         = Error(CodeDuplicatedMemberInfo)
	ErrCMDiskNotFound               = Error(CodeCMDiskNotFound)
	ErrInvalidStatus                = Error(CodeInvalidDiskStatus)
	ErrChangeDiskStatusNotAllow     = Error(CodeChangeDiskStatusNotAllow)
	ErrConcurrentAllocVolumeUnit    = Error(CodeConcurrentAllocVolumeUnit)
	ErrNoAvailableVolume            = Error(CodeNoAvailableVolume)
	ErrAllocVolumeInvalidParams     = Error(CodeAllocVolumeInvalidParams)
	ErrOldVuidNotMatch              = Error(CodeOldVuidNotMatch)
	ErrNewVuidNotMatch              = Error(CodeNewVuidNotMatch)
	ErrNewDiskIDNotMatch            = Error(CodeNewDiskIDNotMatch)
	ErrConfigArgument               = Error(CodeConfigArgument)
	ErrInvalidClusterID             = Error(CodeInvalidClusterID)
	ErrInvalidIDC                   = Error(CodeInvalidIDC)
	ErrVolumeUnitNotExist           = Error(CodeVolumeUnitNotExist)
	ErrRegisterServiceInvalidParams = Error(CodeRegisterServiceInvalidParams)
	ErrDiskAbnormalOrNotReadOnly    = Error(CodeDiskAbnormalOrNotReadOnly)
	ErrStatChunkFailed              = Error(CodeStatChunkFailed)
	ErrInvalidCodeMode              = Error(CodeInvalidCodeMode)
	ErrRetainVolumeNotAlloc         = Error(CodeRetainVolumeNotAlloc)
	ErrDroppedDiskHasVolumeUnit     = Error(CodeDroppedDiskHasVolumeUnit)
	ErrNotSupportIdle               = Error(CodeNotSupportIdle)
	ErrDiskIsDropping               = Error(CodeDiskIsDropping)
	ErrRejectDelSysConfig           = Error(CodeRejectDeleteSystemConfig)
	ErrCMNodeNotFound               = Error(CodeCMNodeNotFound)
	ErrCMNodeIsDropping             = Error(CodeCMNodeIsDropping)
	ErrCMNodeSetNotFound            = Error(CodeCMNodeSetNotFound)
)
