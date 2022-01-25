package errors

const (
	CodeCMUnexpect                   = 900
	CodeActiveVolume                 = 901
	CodeLockNotAllow                 = 902
	CodeUnlockNotAllow               = 903
	CodeVolumeNotExist               = 904
	CodeVolumeStatusNotAcceptable    = 905
	CodeRaftPropose                  = 906
	CodeNoLeader                     = 907
	CodeRaftReadIndex                = 908
	CodeUpdateVolumeParamInvalid     = 909
	CodeDuplicatedMemberInfo         = 910
	CodeCMDiskNotFound               = 911
	CodeInvalidDiskStatus            = 912
	CodeChangeDiskStatusNotAllow     = 913
	CodeConcurrentAllocVolumeUnit    = 914
	CodeOverMaxVolumeThreshold       = 915
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
	CodeDiskAbnormal                 = 926
	CodeStatChunkFailed              = 927
	CodeInvalidCodeMode              = 928
	CodeRetainVolumeNotAlloc         = 929
	CodeDroppedDiskHasVolumeUnit     = 930
	CodeNotSupportIdle               = 931
)

var (
	ErrCMUnexpect                   = Error(CodeCMUnexpect)
	ErrActiveVolume                 = Error(CodeActiveVolume)
	ErrLockNotAllow                 = Error(CodeLockNotAllow)
	ErrUnlockNotAllow               = Error(CodeUnlockNotAllow)
	ErrVolumeNotExist               = Error(CodeVolumeNotExist)
	ErrVolumeStatusNotAcceptable    = Error(CodeVolumeStatusNotAcceptable)
	ErrRaftPropose                  = Error(CodeRaftPropose)
	ErrNoLeader                     = Error(CodeNoLeader)
	ErrRaftReadIndex                = Error(CodeRaftReadIndex)
	ErrUpdateVolumeParamInvalid     = Error(CodeUpdateVolumeParamInvalid)
	ErrDuplicatedMemberInfo         = Error(CodeDuplicatedMemberInfo)
	ErrCMDiskNotFound               = Error(CodeCMDiskNotFound)
	ErrInvalidStatus                = Error(CodeInvalidDiskStatus)
	ErrChangeDiskStatusNotAllow     = Error(CodeChangeDiskStatusNotAllow)
	ErrConcurrentAllocVolumeUnit    = Error(CodeConcurrentAllocVolumeUnit)
	ErrOverMaxVolumeThreshold       = Error(CodeOverMaxVolumeThreshold)
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
	ErrDiskAbnormal                 = Error(CodeDiskAbnormal)
	ErrStatChunkFailed              = Error(CodeStatChunkFailed)
	ErrInvalidCodeMode              = Error(CodeInvalidCodeMode)
	ErrRetainVolumeNotAlloc         = Error(CodeRetainVolumeNotAlloc)
	ErrDroppedDiskHasVolumeUnit     = Error(CodeDroppedDiskHasVolumeUnit)
	ErrNotSupportIdle               = Error(CodeNotSupportIdle)
)
