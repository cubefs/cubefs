package metanode

import (
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

type FileSizeRange uint32

type fileStatsConfig struct {
	fileStatsEnable   bool
	fileSizeRanges    []FileSizeRangeConfig
	fileRangeLabels   []string
	maxConfiguredSize uint64
}

type FileSizeRangeConfig struct {
	Threshold uint64 `json:"threshold"` // 区间分界点（单位：字节）
	Label     string `json:"label"`     // 区间显示名称（如"1K-1M"）
}

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
	fileStatsCheckPeriod = 2 * time.Minute
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
	if !mp.manager.fileStatsConfig.fileStatsEnable || !proto.IsRegular(ino.Type) || ino.NLink <= 0 {
		log.LogWarnf("fileStats is not enabled")
		return
	}
	log.LogWarnf("fileStats is enabled")

	fileStatsConfig := mp.manager.fileStatsConfig
	var index int
	if ino.Size >= fileStatsConfig.maxConfiguredSize {
		index = len(fileStatsConfig.fileRangeLabels) - 1
	} else {
		index = sort.Search(len(fileStatsConfig.fileSizeRanges), func(i int) bool {
			return ino.Size < fileStatsConfig.fileSizeRanges[i].Threshold
		})
	}

	atomic.AddInt64(&mp.fileRange[index], 1)
	// fileRange := mp.fileRange
	// if ino.NLink > 0 && proto.IsRegular(ino.Type) {
	// 	if ino.Size < Size1K {
	// 		fileRange[LessThan1K] += 1
	// 	} else if Size1K <= ino.Size && ino.Size < Size1M {
	// 		fileRange[LessThan1M] += 1
	// 	} else if Size1M <= ino.Size && ino.Size < Size16M {
	// 		fileRange[LessThan16M] += 1
	// 	} else if Size16M <= ino.Size && ino.Size < Size32M {
	// 		fileRange[LessThan32M] += 1
	// 	} else if Size32M <= ino.Size && ino.Size < Size64M {
	// 		fileRange[LessThan64M] += 1
	// 	} else if Size64M <= ino.Size && ino.Size < Size128M {
	// 		fileRange[LessThan128M] += 1
	// 	} else if Size128M <= ino.Size && ino.Size < Size256M {
	// 		fileRange[LessThan256M] += 1
	// 	} else {
	// 		fileRange[BiggerThan256M] += 1
	// 	}
	// }
}

func (m *metadataManager) updateFileStatsRanges(ranges []FileSizeRangeConfig) error {
	if len(ranges) == 0 {
		return errors.New("at least one threshold needs to be configured")
	}

	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].Threshold < ranges[j].Threshold
	})
	uniqueRanges := make([]FileSizeRangeConfig, 0)
	traversed := make(map[uint64]struct{})
	for _, r := range ranges {
		if _, exists := traversed[r.Threshold]; !exists {
			traversed[r.Threshold] = struct{}{}
			uniqueRanges = append(uniqueRanges, r)
		}
	}

	labels := make([]string, len(uniqueRanges)+1)
	maxSize := uint64(0)
	for i := 0; i < len(uniqueRanges); i++ {
		if uniqueRanges[i].Label == "" {
			return fmt.Errorf("%d must be configured with a label", uniqueRanges[i].Threshold)
		}
		labels[i] = uniqueRanges[i].Label
		if uniqueRanges[i].Threshold > maxSize {
			maxSize = uniqueRanges[i].Threshold
		}
	}
	labels[len(uniqueRanges)] = ">" + strconv.FormatUint(maxSize/1024/1024, 10) + "MB"

	m.mu.Lock()
	defer m.mu.Unlock()
	m.fileStatsConfig.fileSizeRanges = uniqueRanges
	m.fileStatsConfig.fileRangeLabels = labels
	m.fileStatsConfig.maxConfiguredSize = maxSize

	for _, p := range m.partitions {
		if mp, ok := p.(*metaPartition); ok {
			mp.fileRange = make([]int64, len(labels))
		}
	}

	return nil
}

func (m *metadataManager) initFileStatsConfig() {
	m.fileStatsConfig = &fileStatsConfig{
		fileStatsEnable: true,
		fileSizeRanges: []FileSizeRangeConfig{
			{Threshold: Size1K, Label: "<1K"},
			{Threshold: Size1M, Label: "1K-1M"},
			{Threshold: Size16M, Label: "1M-16M"},
			{Threshold: Size32M, Label: "16M-32M"},
			{Threshold: Size64M, Label: "32M-64M"},
			{Threshold: Size128M, Label: "64M-128M"},
			{Threshold: Size256M, Label: "128M-256M"},
		},
		fileRangeLabels: []string{
			"<1K", "1K-1M", "1M-16M", "16M-32M", "32M-64M", "64M-128M", "128M-256M", ">256M",
		},
		maxConfiguredSize: Size256M,
	}
}

// func (mp *metaPartition) startFileStats() {
// 	checkTicker := time.NewTicker(fileStatsCheckPeriod)
// 	go func(stopC chan bool) {
// 		lastEnable := false
// 		isLeader := false
// 		for {
// 			select {
// 			case <-stopC:
// 				// if this mp is closed, clear the metric
// 				if lastEnable {
// 					fileRange := make([]int64, MaxRangeType)
// 					mp.setMetrics(fileRange)
// 				}
// 				checkTicker.Stop()
// 				return
// 			case <-checkTicker.C:
// 				if !mp.manager.fileStatsEnable {
// 					// if fileStatsEnable change from true to false, clear the metric
// 					if lastEnable {
// 						fileRange := make([]int64, MaxRangeType)
// 						mp.setMetrics(fileRange)
// 					}
// 					lastEnable = false
// 					continue
// 				}

// 				lastEnable = true

// 				// mp.inodeTree.Ascend(func(i BtreeItem) bool {
// 				// 	ino := i.(*Inode)
// 				// 	mp.fileStats(ino)
// 				// 	return true
// 				// })

// 				// Clear the metric if status change from leader to follower
// 				if _, isLeader = mp.IsLeader(); isLeader {
// 					mp.setMetrics(mp.fileRange)
// 				} else {
// 					fileRange := make([]int64, MaxRangeType)
// 					mp.setMetrics(fileRange)
// 				}
// 			}
// 		}
// 	}(mp.stopC)
// }
