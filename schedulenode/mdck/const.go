package mdck

import (
	"github.com/cubefs/cubefs/storage"
	"time"
)

const (
	DefaultCheckInterval     = time.Minute * 1440 // 24h
	DefaultSafeCleanInterval = 604800             // 7 day
	DefaultMpConcurrency     = 20
	DefaultInodeConcurrency  = 10
	DefaultMailToMember      = "lizhenzhen36@jd.com"
)

type TreeType int

const (
	DentryType TreeType = iota
	InodeType
	ExtendType
	MultipartType
	DelDentryType
	DelInodeType
	MaxType
)

func (t TreeType) String() string {
	switch t {
	case DentryType:
		return "Dentry tree"
	case InodeType:
		return "Inode tree"
	case ExtendType:
		return "Extend tree"
	case MultipartType:
		return "Multipart tree"
	case DelDentryType:
		return "DeletedDentry tree"
	case DelInodeType:
		return "DeletedInode tree"
	default:
		return "Unknown"
	}
}

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