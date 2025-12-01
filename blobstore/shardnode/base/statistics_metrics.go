// Copyright 2025 The CubeFS Authors.
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

package base

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
)

// ErrorStats error stats
type ErrorStats struct {
	lock        sync.Mutex
	errMap      map[string]uint64
	totalErrCnt uint64
}

// ErrorPercent error percent
type ErrorPercent struct {
	err     string
	percent float64
	errCnt  uint64
}

// NewErrorStats returns error stats
func NewErrorStats() *ErrorStats {
	es := ErrorStats{
		errMap: make(map[string]uint64),
	}
	return &es
}

// AddFail add fail statistics
func (es *ErrorStats) AddFail(err error) {
	es.lock.Lock()
	defer es.lock.Unlock()
	es.totalErrCnt++

	errStr := errStrFormat(err)
	if _, ok := es.errMap[errStr]; !ok {
		es.errMap[errStr] = 0
	}
	es.errMap[errStr]++
}

// Stats returns stats
func (es *ErrorStats) Stats() (statsResult []ErrorPercent, totalErrCnt uint64) {
	es.lock.Lock()
	defer es.lock.Unlock()

	var totalCnt uint64
	for _, cnt := range es.errMap {
		totalCnt += cnt
	}

	for err, cnt := range es.errMap {
		percent := ErrorPercent{
			err:     err,
			percent: float64(cnt) / float64(totalCnt),
			errCnt:  cnt,
		}
		statsResult = append(statsResult, percent)
	}

	sort.Slice(statsResult, func(i, j int) bool {
		return statsResult[i].percent > statsResult[j].percent
	})

	return statsResult, es.totalErrCnt
}

// FormatPrint format print message
func FormatPrint(statsInfos []ErrorPercent) (res []string) {
	for _, info := range statsInfos {
		res = append(res, fmt.Sprintf("%s: %0.2f%%[%d]", info.err, info.percent*100, info.errCnt))
	}
	return
}

func errStrFormat(err error) string {
	if err == nil || len(err.Error()) == 0 {
		return ""
	}

	strSlice := strings.Split(err.Error(), ":")
	return strings.TrimSpace(strSlice[len(strSlice)-1])
}

const (
	Namespace                = "blobstore"
	Subsystem                = "shardnode"
	ShardRepair              = "shard_repair"
	ChunkMissMigrateAbnormal = "chunk_miss_migrate"
)

type AbnormalReporter struct {
	lock             sync.RWMutex
	abnormalReporter *prometheus.GaugeVec
	reportedVuids    map[proto.Vuid]struct{}
}

// NewAbnormalReporter returns abnormal reporter
func NewAbnormalReporter(clusterID proto.ClusterID, taskType string, abnormalKind string) *AbnormalReporter {
	abnormalReporter := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: Subsystem,
		Name:      "abnormal_task",
		Help:      "abnormal task",
		ConstLabels: map[string]string{
			"cluster_id": fmt.Sprintf("%d", clusterID),
			"task_type":  taskType,
			"kind":       abnormalKind,
		},
	}, []string{"diskID", "vuid"})
	if err := prometheus.Register(abnormalReporter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			abnormalReporter = are.ExistingCollector.(*prometheus.GaugeVec)
			return &AbnormalReporter{
				abnormalReporter: abnormalReporter,
				reportedVuids:    make(map[proto.Vuid]struct{}),
			}
		}
		panic(err)
	}
	return &AbnormalReporter{
		abnormalReporter: abnormalReporter,
		reportedVuids:    make(map[proto.Vuid]struct{}),
	}
}

// ReportAbnormal report abnormal task
func (abr *AbnormalReporter) ReportAbnormal(diskID proto.DiskID, vuid proto.Vuid) {
	abr.abnormalReporter.WithLabelValues(
		fmt.Sprintf("%d", diskID),
		fmt.Sprintf("%d", vuid),
	).Set(1)
}

// CancelAbnormal cancel abnormal report
func (abr *AbnormalReporter) CancelAbnormal(diskID proto.DiskID, vuid proto.Vuid) {
	abr.abnormalReporter.WithLabelValues(
		fmt.Sprintf("%d", diskID),
		fmt.Sprintf("%d", vuid),
	).Set(0)
}

// IsVuidReported check if vuid abnormal is reported
func (abr *AbnormalReporter) IsVuidReported(vuid proto.Vuid) bool {
	abr.lock.RLock()
	defer abr.lock.RUnlock()
	_, ok := abr.reportedVuids[vuid]
	return ok
}

// SetVuidReported set vuid reported
func (abr *AbnormalReporter) SetVuidReported(vuid proto.Vuid) {
	abr.lock.Lock()
	abr.reportedVuids[vuid] = struct{}{}
	abr.lock.Unlock()
}

var (
	shardRaftStatsName = "shard_raft_stats"
	shardMetaStatsName = "shard_metadata_stats"

	shardRaftStatsLabels = []string{"disk_id", "shard_id", "suid", "item"}
	shardMetaStatsLabels = []string{"shard_id", "item"}

	diskRocksdbStatsName   = "disk_rocksdb_stats"
	diskHealthName         = "disk_health"
	diskRocksdbStatsLabels = []string{"path", "db_name", "item"}
	diskHealthLabels       = []string{"path"}

	deleteBlobTaskName  = "shard_node_delete_blob_task"
	repairSliceTaskName = "shard_node_repair_slice_task"

	blobTaskLabels = []string{"shard_id", "task_status"}
)

type (
	gaugeVecReporter struct {
		gaugeVec *prometheus.GaugeVec
	}

	counterVecReporter struct {
		counterVec *prometheus.CounterVec
	}

	ShardRaftStatsReporter struct {
		*gaugeVecReporter
	}

	ShardMetaStatsReporter struct {
		*gaugeVecReporter
	}

	DiskRocksdbStatsReporter struct {
		*gaugeVecReporter
	}

	DiskHealthReporter struct {
		*gaugeVecReporter
	}

	MessageTaskReporter struct {
		*counterVecReporter
	}
)

func NewDeleteBlobTaskReporter(clusterID proto.ClusterID) *MessageTaskReporter {
	reporter := newCounterVecReporter(
		clusterID,
		deleteBlobTaskName,
		"delete blob task",
		blobTaskLabels,
	)
	return &MessageTaskReporter{
		counterVecReporter: reporter,
	}
}

func NewRepairSliceTaskReporter(clusterID proto.ClusterID) *MessageTaskReporter {
	reporter := newCounterVecReporter(
		clusterID,
		repairSliceTaskName,
		"repair slice task",
		blobTaskLabels,
	)
	return &MessageTaskReporter{
		counterVecReporter: reporter,
	}
}

func (r *MessageTaskReporter) ReportSuccess(shardID proto.ShardID) {
	r.counterVec.WithLabelValues(
		shardID.ToString(),
		"success",
	).Inc()
}

func (r *MessageTaskReporter) ReportFailed(shardID proto.ShardID) {
	r.counterVec.WithLabelValues(
		shardID.ToString(),
		"failed",
	).Inc()
}

func NewDiskRocksdbStatusReporter(clusterID proto.ClusterID) *DiskRocksdbStatsReporter {
	reporter := newGuageVecReporter(
		clusterID,
		diskRocksdbStatsName,
		"disk rocksdb stats",
		diskRocksdbStatsLabels,
	)
	return &DiskRocksdbStatsReporter{
		gaugeVecReporter: reporter,
	}
}

func (r *DiskRocksdbStatsReporter) Report(dbName string, diskPath string, stats kvstore.Stats) {
	metrics := []struct {
		item  string
		value float64
	}{
		{"used", float64(stats.Used)},
		{"block_cache_usage", float64(stats.MemoryUsage.BlockCacheUsage)},
		{"index_and_filter_usage", float64(stats.MemoryUsage.IndexAndFilterUsage)},
		{"memtable_usage", float64(stats.MemoryUsage.MemtableUsage)},
		{"block_pinned_usage", float64(stats.MemoryUsage.BlockPinnedUsage)},
		{"total_memory_usage", float64(stats.MemoryUsage.Total)},
		{"level0_file_num", float64(stats.Level0FileNum)},
		{"write_slowdown", boolToFloat64(stats.WriteSlowdown)},
		{"write_stop", boolToFloat64(stats.WriteStop)},
		{"running_flush", float64(stats.RunningFlush)},
		{"pending_flush", boolToFloat64(stats.PendingFlush)},
		{"running_compaction", float64(stats.RunningCompaction)},
		{"pending_compaction", boolToFloat64(stats.PendingCompaction)},
		{"background_errors", float64(stats.BackgroundErrors)},
	}

	for _, m := range metrics {
		r.gaugeVec.WithLabelValues(
			diskPath,
			dbName,
			m.item).Set(m.value)
	}
}

func NewDiskHealthReporter(clusterID proto.ClusterID) *DiskHealthReporter {
	reporter := newGuageVecReporter(
		clusterID,
		diskHealthName,
		"disk health",
		diskHealthLabels,
	)
	return &DiskHealthReporter{
		gaugeVecReporter: reporter,
	}
}

func (r *DiskHealthReporter) ReportHealthy(diskPath string) {
	r.gaugeVec.WithLabelValues(
		diskPath,
	).Set(0)
}

func (r *DiskHealthReporter) ReportUnhealthy(diskPath string) {
	r.gaugeVec.WithLabelValues(
		diskPath,
	).Set(1)
}

func NewShardMetaStatsReporter(clusterID proto.ClusterID) *ShardMetaStatsReporter {
	reporter := newGuageVecReporter(
		clusterID,
		shardMetaStatsName,
		"shard meta stats",
		shardMetaStatsLabels,
	)
	return &ShardMetaStatsReporter{
		gaugeVecReporter: reporter,
	}
}

func (r *ShardMetaStatsReporter) Report(metaStats snproto.ShardMetaStats) {
	shardIDStr := metaStats.ShardID.ToString()
	metrics := []struct {
		item  string
		value float64
	}{
		{"item_count", float64(metaStats.ItemCount)},
		{"item_size", float64(metaStats.ItemSize)},
		{"blob_count", float64(metaStats.BlobCount)},
		{"blob_size", float64(metaStats.BlobSize)},
	}

	for _, m := range metrics {
		r.gaugeVec.WithLabelValues(
			shardIDStr,
			m.item).Set(m.value)
	}
}

func NewRaftStatsReporter(clusterID proto.ClusterID) *ShardRaftStatsReporter {
	reporter := newGuageVecReporter(
		clusterID,
		shardRaftStatsName,
		"shard raft stats",
		shardRaftStatsLabels,
	)
	return &ShardRaftStatsReporter{
		gaugeVecReporter: reporter,
	}
}

func (r *ShardRaftStatsReporter) Report(shardStats snapi.ShardStats) {
	suid := shardStats.Suid
	diskIDStr := fmt.Sprintf("%d", shardStats.RaftStat.NodeID)
	suidStr := suid.ToString()
	shardIDStr := suid.ShardID().ToString()

	metrics := []struct {
		name  string
		value float64
	}{
		{"leader", float64(shardStats.LeaderDiskID)},
		{"applied", float64(shardStats.RaftStat.Applied)},
		{"commit", float64(shardStats.RaftStat.Commit)},
	}

	for _, m := range metrics {
		r.gaugeVec.WithLabelValues(
			diskIDStr,
			shardIDStr,
			suidStr,
			m.name,
		).Set(m.value)
	}
}

func newGuageVecReporter(clusterID proto.ClusterID, name, help string, lables []string) *gaugeVecReporter {
	constLabels := map[string]string{
		"cluster_id": clusterID.ToString(),
	}
	gaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   Namespace,
		Subsystem:   Subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: constLabels,
	}, lables)
	if err := prometheus.Register(gaugeVec); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			gaugeVec = are.ExistingCollector.(*prometheus.GaugeVec)
			return &gaugeVecReporter{
				gaugeVec: gaugeVec,
			}
		}
		panic(err)
	}
	return &gaugeVecReporter{
		gaugeVec: gaugeVec,
	}
}

func newCounterVecReporter(clusterID proto.ClusterID, name, help string, lables []string) *counterVecReporter {
	constLabels := map[string]string{
		"cluster_id": clusterID.ToString(),
	}
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   Namespace,
		Subsystem:   Subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: constLabels,
	}, lables)
	if err := prometheus.Register(counterVec); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counterVec = are.ExistingCollector.(*prometheus.CounterVec)
			return &counterVecReporter{
				counterVec: counterVec,
			}
		}
		panic(err)
	}
	return &counterVecReporter{
		counterVec: counterVec,
	}
}

func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
