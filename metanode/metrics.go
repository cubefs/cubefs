// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
)

// metrics constants
const (
	StatPeriod = time.Minute * time.Duration(1)

	// Metric names
	MetricMetaFailedPartition      = "meta_failed_partition"
	MetricMetaPartitionInodeCount  = "mpInodeCount"
	MetricMetaPartitionDentryCount = "mpDentryCount"
	MetricConnectionCount          = "connectionCnt"
	MetricFileStats                = "fileStats"
	RocksdbStats                   = "rocksdbStats"
	RocksdbDiskUsage               = "rocksdbDiskUsage"

	// Timeout for metrics collection
	MetricsCollectionTimeout = 30 * time.Second
	RocksdbDiskUsageHigh     = 0.8
)

// MetaNodeMetrics holds all metrics for the meta node
var (
	rocksdbStatsList = []string{
		"rocksdb.db.get.micros",
		"rocksdb.db.write.micros",
		"rocksdb.db.seek.micros",
		"rocksdb.db.write.stall",
		"rocksdb.db.flush.micros",
		"rocksdb.sst.read.micros",
		"rocksdb.bytes.per.read",
		"rocksdb.bytes.per.write",
	}
	statsP99Regexp = regexp.MustCompile(`P99 : (\d+\.\d+)`)
)

type MetaNodeMetrics struct {
	MetricConnectionCount          *exporter.Gauge
	MetricMetaFailedPartition      *exporter.Gauge
	MetricMetaPartitionInodeCount  *exporter.GaugeVec
	MetricMetaPartitionDentryCount *exporter.GaugeVec
	MetricFileStats                *exporter.GaugeVec
	RocksdbStats                   *exporter.GaugeVec
	RocksdbDiskUsage               *exporter.GaugeVec

	metricStopCh chan struct{}
	ctx          context.Context
	cancel       context.CancelFunc
}

// startStat initializes and starts the metrics collection
func (m *MetaNode) startStat() {
	ctx, cancel := context.WithCancel(context.Background())

	m.metrics = &MetaNodeMetrics{
		metricStopCh: make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,

		MetricConnectionCount:          exporter.NewGauge(MetricConnectionCount),
		MetricMetaFailedPartition:      exporter.NewGauge(MetricMetaFailedPartition),
		MetricMetaPartitionInodeCount:  exporter.NewGaugeVec(MetricMetaPartitionInodeCount, "", []string{"volName"}),
		MetricMetaPartitionDentryCount: exporter.NewGaugeVec(MetricMetaPartitionDentryCount, "", []string{"volName"}),
		MetricFileStats:                exporter.NewGaugeVec(MetricFileStats, "", []string{"volName", "sizeRange"}),
		RocksdbStats:                   exporter.NewGaugeVec(RocksdbStats, "", []string{"rocksdbDir", "key"}),
		RocksdbDiskUsage:               exporter.NewGaugeVec(RocksdbDiskUsage, "", []string{"rocksdbDir"}),
	}

	go m.collectPartitionMetrics()
}

// updatePartitionMetrics updates partition-related metrics
func (m *MetaNode) updatePartitionMetrics() error {
	// Reset metrics to avoid stale data
	m.metrics.MetricMetaPartitionInodeCount.Reset()
	m.metrics.MetricMetaPartitionDentryCount.Reset()

	// Use maps to aggregate data by volume
	volInodeCount := make(map[string]int)
	volDentryCount := make(map[string]int)

	manager, ok := m.metadataManager.(*metadataManager)
	if !ok {
		return fmt.Errorf("invalid metadata manager type")
	}

	// Collect data with minimal lock time
	partitions := m.collectPartitionData(manager)

	// Process collected data
	for _, p := range partitions {
		volName := p.volName
		if _, exists := volInodeCount[volName]; !exists {
			volInodeCount[volName] = 0
			volDentryCount[volName] = 0
		}
		volInodeCount[volName] += p.inodeCount
		volDentryCount[volName] += p.dentryCount
	}

	// Update metrics
	for volName, inodeCount := range volInodeCount {
		dentryCount := volDentryCount[volName]
		m.metrics.MetricMetaPartitionInodeCount.SetWithLabelValues(float64(inodeCount), volName)
		m.metrics.MetricMetaPartitionDentryCount.SetWithLabelValues(float64(dentryCount), volName)
	}

	return nil
}

// partitionData holds partition metrics data
type partitionData struct {
	volName     string
	inodeCount  int
	dentryCount int
}

// collectPartitionData collects partition data with minimal lock time
func (m *MetaNode) collectPartitionData(manager *metadataManager) []partitionData {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	partitions := make([]partitionData, 0, len(manager.partitions))

	for _, p := range manager.partitions {
		mp, ok := p.(*metaPartition)
		if !ok {
			continue
		}

		partitions = append(partitions, partitionData{
			volName:     mp.config.VolName,
			inodeCount:  mp.GetInodeTreeLen(),
			dentryCount: mp.GetDentryTreeLen(),
		})
	}

	return partitions
}

// collectPartitionMetrics runs the main metrics collection loop
func (m *MetaNode) collectPartitionMetrics() {
	ticker := time.NewTicker(StatPeriod)
	defer ticker.Stop()

	fileStatTicker := time.NewTicker(fileStatsCheckPeriod)
	defer fileStatTicker.Stop()

	for {
		select {
		case <-m.metrics.metricStopCh:
			return
		case <-m.metrics.ctx.Done():
			return
		case <-ticker.C:
			if err := m.updatePartitionMetrics(); err != nil {
				// Log error but continue
				continue
			}
			m.metrics.MetricConnectionCount.Set(float64(m.connectionCnt))
		case <-fileStatTicker.C:
			m.updateFileStatsMetrics()
			if m.rocksdbEnableStats {
				m.updateRocksdbStatsMetrics()
			}
			m.updateRocksdbDiskUsageMetrics()
		}
	}
}

// updateFileStatsMetrics updates file statistics metrics
func (m *MetaNode) updateFileStatsMetrics() {
	m.metrics.MetricFileStats.Reset()
	volFileRange := make(map[string][]int64)

	manager, ok := m.metadataManager.(*metadataManager)
	if !ok {
		return
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	_, labels, _ := manager.GetFileStatsConfig()

	numRanges := len(labels)
	for _, p := range manager.partitions {
		mp, ok := p.(*metaPartition)
		if !ok {
			continue
		}
		fileRange := mp.getFileRange()
		volName := mp.config.VolName
		if _, exists := volFileRange[volName]; !exists {
			volFileRange[volName] = make([]int64, numRanges)
		}
		validLength := util.Min(len(fileRange), numRanges)
		for i := 0; i < validLength; i++ {
			volFileRange[volName][i] += fileRange[i]
		}
	}

	for volName, ranges := range volFileRange {
		for i, val := range ranges {
			sizeRange := labels[i]
			m.metrics.MetricFileStats.SetWithLabelValues(float64(val), volName, sizeRange)
		}
	}
}

func (m *MetaNode) updateRocksdbStatsMetrics() {
	m.metrics.RocksdbStats.Reset()

	manager, ok := m.metadataManager.(*metadataManager)
	if !ok {
		return
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	for _, dbPath := range m.rocksDirs {
		db, err := m.rocksdbManager.OpenRocksdb(dbPath, 0)
		if err != nil {
			continue
		}
		stats := db.GetStatistics()
		statsP99 := getStatsP99(stats, rocksdbStatsList)
		for key, val := range statsP99 {
			m.metrics.RocksdbStats.SetWithLabelValues(val, dbPath, key)
		}
		m.rocksdbManager.CloseRocksdb(db)
	}
}

// getStatsP99 extracts P99 statistics from RocksDB stats string
func getStatsP99(stats string, statsList []string) map[string]float64 {
	statsMap := make(map[string]float64, len(statsList))
	lines := strings.Split(stats, "\n")

	for _, item := range statsList {
		for _, line := range lines {
			if strings.HasPrefix(line, item) {
				statsP99 := statsP99Regexp.FindStringSubmatch(line)
				if statsP99 == nil || len(statsP99) < 2 {
					break
				}
				val, err := strconv.ParseFloat(statsP99[1], 64)
				if err != nil {
					break
				}
				statsMap[item] = val
				break
			}
		}
	}

	return statsMap
}

func (m *MetaNode) updateRocksdbDiskUsageMetrics() {
	m.metrics.RocksdbDiskUsage.Reset()
	for _, disk := range m.disks {
		if !disk.IsRocksDBDisk {
			continue
		}
		availableTotal := disk.Total - float64(disk.ReservedSpace)
		if availableTotal <= 0 {
			m.metrics.RocksdbDiskUsage.SetWithLabelValues(1.0, disk.Path)
			continue
		}
		usage := disk.Used / availableTotal
		if usage > 1 {
			usage = 1
		}
		if usage > RocksdbDiskUsageHigh {
			m.metrics.RocksdbDiskUsage.SetWithLabelValues(usage, disk.Path)
		}
	}
}

// stopStat stops the metrics collection
func (m *MetaNode) stopStat() {
	if m.metrics != nil {
		m.metrics.cancel()
		close(m.metrics.metricStopCh)
	}
}
