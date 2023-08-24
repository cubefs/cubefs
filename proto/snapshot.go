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
	UpdateTime     int64
	VolVersionInfo *VolVersionInfo
}

type SnapshotVerDelTaskResponse struct {
	ID        string
	StartTime *time.Time
	EndTime   *time.Time
	Done      bool
	Status    uint8
	Result    string
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
