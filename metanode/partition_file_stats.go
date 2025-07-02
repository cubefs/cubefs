package metanode

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

type FileSizeRange uint32

type fileStatsConfig struct {
	sync.RWMutex
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

func (m *metadataManager) updateFileStatsConfig(thresholds []uint64) bool {
	if len(thresholds) == 0 {
		return false
	}

	sort.Slice(thresholds, func(i, j int) bool {
		return thresholds[i] < thresholds[j]
	})

	if reflect.DeepEqual(thresholds, m.fileStatsConfig.thresholds) {
		return false
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

	m.fileStatsConfig.thresholds = uniquethresholds
	m.fileStatsConfig.fileRangeLabels = labels

	return true
}

func (m *metadataManager) initFileStatsConfig() {
	m.fileStatsConfig = &fileStatsConfig{
		fileStatsEnable: false,
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

func (m *metadataManager) GetFileStatsConfig() (thresholds []uint64, labels []string, enable bool) {
	m.fileStatsConfig.RLock()
	defer m.fileStatsConfig.RUnlock()
	return m.fileStatsConfig.thresholds,
		m.fileStatsConfig.fileRangeLabels,
		m.fileStatsConfig.fileStatsEnable
}
