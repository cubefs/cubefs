package proto

import (
	"fmt"
	"time"
)

type VerInfo struct {
	VolName string
	VerSeq  uint64
}

func (vi *VerInfo) Key() string {
	return fmt.Sprintf("%s_%d", vi.VolName, vi.VerSeq)
}

//snapshot version delete
type SnapshotVerDelTask struct {
	VerInfo
}

type SnapshotVerDelTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *SnapshotVerDelTask
}

type SnapshotStatistics struct {
	VerInfo
	TotalInodeNum   int64
	FileNum         int64
	DirNum          int64
	ErrorSkippedNum int64
}

type SnapshotVerDelTaskResponse struct {
	ID string
	SnapshotStatistics
	StartTime *time.Time
	EndTime   *time.Time
	Done      bool
	Status    uint8
	Result    string
}
