package normalextentcheck

import (
	"github.com/cubefs/cubefs/storage"
	"time"
)

const (
	DefaultCheckInterval              = time.Minute * 1440 // 24h
	DefaultSafeCleanInterval          = 604800 // 7 day
	DefaultTaskConcurrency            = 5
	DefaultInodeConcurrency           = 10
	DefaultInodeCntThresholdForSearch = 100000000
	DefaultMailToMember               = "lizhenzhen36@jd.com"
	DefaultAlarmErps                  = "lizhenzhen36"
	DefaultInodeEKMaxCountThreshold   = 200000
)

type DataPartitionView struct {
	VolName   string                    `json:"volName"`
	ID        uint64                    `json:"id"`
	Files     []storage.ExtentInfoBlock `json:"extents"`
	FileCount int                       `json:"fileCount"`
}

type ExtentInfo struct {
	DataPartitionID uint64
	ExtentID        uint64
}