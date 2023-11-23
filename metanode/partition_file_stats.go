package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"time"
)

type FileSizeRange uint32

const (
	Size1K   uint64 = 2 << 10
	Size1M   uint64 = 2 << 20
	Size16M         = 16 * Size1M
	Size32M         = 32 * Size1M
	Size64M         = 64 * Size1M
	Size128M        = 128 * Size1M
	Size256M        = 256 * Size1M
)

const (
	LessThan1K FileSizeRange = iota
	LessThan1M
	LessThan16M
	LessThan32M
	LessThan64M
	LessThan128M
	LessThan256M
	BiggerThan256M
	MaxRangeType
)

const (
	fileStatsCheckPeriod = time.Second * 30
)

func toString(fileSize FileSizeRange) string {
	switch fileSize {
	case LessThan1K:
		return "<1K"
	case LessThan1M:
		return "<1M"
	case LessThan16M:
		return "<16M"
	case LessThan32M:
		return "<32M"
	case LessThan64M:
		return "<64M"
	case LessThan128M:
		return "<128M"
	case LessThan256M:
		return "<256M"
	case BiggerThan256M:
		return ">256M"
	default:
		return "unknown"
	}
}

func (mp *metaPartition) setMetrics(fileRange []int64) {
	for i, val := range fileRange {
		labels := map[string]string{
			"partid":    fmt.Sprintf("%d", mp.config.PartitionId),
			"volName":   mp.config.VolName,
			"sizeRange": toString(FileSizeRange(i)),
		}
		exporter.NewGauge("fileStats").SetWithLabels(float64(val), labels)
	}
}

func (mp *metaPartition) fileStats(ino *Inode) {
	if !mp.manager.fileStatsEnable {
		return
	}
	fileRange := mp.fileRange
	if ino.NLink > 0 && proto.IsRegular(ino.Type) {
		if 0 <= ino.Size && ino.Size < Size1K {
			fileRange[LessThan1K] += 1
		} else if Size1K <= ino.Size && ino.Size < Size1M {
			fileRange[LessThan1M] += 1
		} else if Size1M <= ino.Size && ino.Size < Size16M {
			fileRange[LessThan16M] += 1
		} else if Size16M <= ino.Size && ino.Size < Size32M {
			fileRange[LessThan32M] += 1
		} else if Size32M <= ino.Size && ino.Size < Size64M {
			fileRange[LessThan64M] += 1
		} else if Size64M <= ino.Size && ino.Size < Size128M {
			fileRange[LessThan128M] += 1
		} else if Size128M <= ino.Size && ino.Size < Size256M {
			fileRange[LessThan256M] += 1
		} else {
			fileRange[BiggerThan256M] += 1
		}
	}
}

func (mp *metaPartition) startFileStats() {
	checkTicker := time.NewTicker(fileStatsCheckPeriod)
	go func(stopC chan bool) {
		lastEnable := false
		isLeader := false
		for {
			select {
			case <-stopC:
				// if this mp is closed, clear the metric
				if lastEnable {
					fileRange := make([]int64, MaxRangeType)
					mp.setMetrics(fileRange)
				}
				checkTicker.Stop()
				return
			case <-checkTicker.C:
				if !mp.manager.fileStatsEnable {
					// if fileStatsEnable change from true to false, clear the metric
					if lastEnable {
						fileRange := make([]int64, MaxRangeType)
						mp.setMetrics(fileRange)
					}
					lastEnable = false
					continue
				}

				lastEnable = true

				// Clear the metric if status change from leader to follower
				if _, isLeader = mp.IsLeader(); isLeader {
					mp.setMetrics(mp.fileRange)
				} else {
					fileRange := make([]int64, MaxRangeType)
					mp.setMetrics(fileRange)
				}
			}
		}
	}(mp.stopC)
}
