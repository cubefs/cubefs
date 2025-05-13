package metanode

import (
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type FileSizeRange uint32

type fileStatsConfig struct {
	fileStatsEnable bool
	thresholds      []uint64
	fileRangeLabels []string
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
	fileStatsCheckPeriod = 5 * time.Minute
)

func (mp *metaPartition) fileStats(ino *Inode) {
	conf := mp.manager.fileStatsConfig
	if !conf.fileStatsEnable || !proto.IsRegular(ino.Type) || ino.NLink <= 0 {
		return
	}

	if len(conf.thresholds) == 0 {
		log.LogErrorf("[fileStats] Empty file stats thresholds configuration")
		return
	}

	var index int
	maxConfiguredSize := conf.thresholds[len(conf.thresholds)-1]
	if ino.Size >= maxConfiguredSize {
		index = len(conf.fileRangeLabels) - 1
	} else {
		index = sort.Search(len(conf.thresholds), func(i int) bool {
			return ino.Size < conf.thresholds[i]
		})
	}

	if index < 0 || index > len(conf.fileRangeLabels)-1 {
		log.LogErrorf("[fileStats] index illegal, index: %d, file size: %d", index, ino.Size)
		return
	}

	atomic.AddInt64(&mp.fileRange[index], 1)
}

func generateSizeLabels(thresholds []uint64) []string {
	labels := make([]string, len(thresholds)+1)

	for i := 0; i < len(thresholds); i++ {
		var lower, upper string
		if i == 0 {
			lower = "0"
		} else {
			lower = formatSize(thresholds[i-1])
		}
		upper = formatSize(thresholds[i])
		labels[i] = fmt.Sprintf("%s-%s", lower, upper)
	}

	lastThreshold := formatSize(thresholds[len(thresholds)-1])
	labels[len(thresholds)] = fmt.Sprintf(">=%s", lastThreshold)
	return labels
}

func formatSize(bytes uint64) string {
	switch {
	case bytes >= 1<<30:
		return fmt.Sprintf("%dG", bytes/(1<<30))
	case bytes >= 1<<20:
		return fmt.Sprintf("%dM", bytes/(1<<20))
	case bytes >= 1<<10:
		return fmt.Sprintf("%dK", bytes/(1<<10))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

func (m *metadataManager) updateFileStatsConfig(thresholds []uint64) error {
	if len(thresholds) == 0 {
		return errors.New("at least one threshold needs to be configured")
	}

	sort.Slice(thresholds, func(i, j int) bool {
		return thresholds[i] < thresholds[j]
	})

	if reflect.DeepEqual(thresholds, m.fileStatsConfig.thresholds) {
		return nil
	}

	log.LogWarnf("[updateFileStatsConfig] thresholds changed, new thresholds: %v", thresholds)

	uniquethresholds := make([]uint64, 0)
	traversed := make(map[uint64]struct{})
	for _, t := range thresholds {
		if _, exists := traversed[t]; !exists {
			traversed[t] = struct{}{}
			uniquethresholds = append(uniquethresholds, t)
		}
	}

	labels := generateSizeLabels(uniquethresholds)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.fileStatsConfig.thresholds = uniquethresholds
	m.fileStatsConfig.fileRangeLabels = labels

	for _, p := range m.partitions {
		if mp, ok := p.(*metaPartition); ok {
			mp.fileRange = make([]int64, len(labels))
		}
	}

	for _, p := range m.partitions {
		if mp, ok := p.(*metaPartition); ok {
			mp.doFileStats()
		}
	}

	return nil
}

func (m *metadataManager) initFileStatsConfig() {
	m.fileStatsConfig = &fileStatsConfig{
		fileStatsEnable: true,
		thresholds: []uint64{
			Size1K,
			Size1M,
			Size16M,
			Size32M,
			Size64M,
			Size128M,
			Size256M,
		},
		fileRangeLabels: []string{
			"<1K", "1K-1M", "1M-16M", "16M-32M", "32M-64M", "64M-128M", "128M-256M", ">256M",
		},
	}
}
