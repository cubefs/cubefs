package proto

import (
	"time"
)

type SnapshotVerDelTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *SnapshotVerDelTask
}

type SnapshotVerDelTask struct {
	Id             string
	VolName        string
	VolVersionInfo *VolVersionInfo
}

type SnapshotVerDelTaskResponse struct {
	ID                 string
	LcNode             string
	StartTime          *time.Time
	EndTime            *time.Time
	UpdateTime         *time.Time
	Done               bool
	Status             uint8
	Result             string
	SnapshotVerDelTask *SnapshotVerDelTask
	SnapshotStatistics
}

type SnapshotStatistics struct {
	VolName         string
	VerSeq          uint64
	TotalInodeNum   int64
	FileNum         int64
	DirNum          int64
	ErrorSkippedNum int64
}
