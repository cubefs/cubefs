package blck

import (
	"github.com/cubefs/cubefs/storage"
	"go.uber.org/atomic"
	"time"
)

const (
	DefaultCheckInterval     = time.Minute * 1440 // 24h
	DefaultSafeCleanInterval = 2592000            // 30 day
	DefaultTaskConcurrency   = 5
	DefaultInodeConcurrency  = 10
	defaultParallelMPCount   = 20
	DefaultMailToMember      = "lizhenzhen36@jd.com"
	DefaultAlarmErps         = "lizhenzhen36"
)

var (
	parallelMpCnt = atomic.NewInt32(defaultParallelMPCount)
	parallelInodeCnt = atomic.NewInt32(DefaultInodeConcurrency)
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