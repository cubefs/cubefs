package compact

const (
	CmpList             = "/compact/list"
	CmpInfo             = "/compact/info"
	CmpStop             = "/compact/stop"
	CmpInsertMp         = "/compact/insertMp"
	CmpInsertInode      = "/compact/insertInode"
	CmpStatus           = "/compact/status"
	CmpConcurrencySetLimit = "/compact/setLimit"
	CmpConcurrencyGetLimit = "/compact/getLimit"
)

const (
	CLIClusterKey = "cluster"
	CLIVolNameKey = "vol"
	CLIMpIdKey    = "mpId"
	CLIInodeIdKey = "inodeId"
	CLILimitSize  = "limit"
)

const (
	DefaultVolumeMaxCompactingMPNums = 5
	DefaultMpConcurrency             = 5
	DefaultCompactVolumeLoadDuration = 60
	MPDealCountEveryDay              = 100
	RetryDoGetMaxCnt                 = 3
	VolLastUpdateIntervalTime        = 30 * 60 // s
	InodeLimitSize                   = 10 * 1024 * 1024
)

const (
	ConfigKeyInodeCheckStep  = "inodeCheckStep"
	ConfigKeyInodeConcurrent = "inodeConcurrent"
	ConfigKeyMinEkLen        = "minEkLen"
	ConfigKeyMinInodeSize    = "minInodeSize"
	ConfigKeyMaxEkAvgSize    = "maxEkAvgSize"
)

const (
	DefaultInodeCheckStep  = 100
	DefaultInodeConcurrent = 10
	DefaultMinEkLen        = 2
	DefaultMinInodeSize    = 1  //MB
	DefaultMaxEkAvgSize    = 32 //MB
)

const (
	InodeOpenFailed         = "open file failed"
	InodeReadFailed         = "read data failed"
	InodeWriteFailed        = "write data failed"
	InodeReadAndWriteFailed = "read and write data failed"
	InodeMergeFailed        = "send meta merge failed"
)

const (
	InodeOpenFailedCode = iota + 1
	InodeReadFailedCode
	InodeWriteFailedCode
	InodeMergeFailedCode
)
