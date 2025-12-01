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
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	dto "github.com/prometheus/client_model/go"
)

func TestErrorStats(t *testing.T) {
	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	err3 := errors.New("error 3")

	es := NewErrorStats()
	for range [3]struct{}{} {
		es.AddFail(err1)
	}
	for range [5]struct{}{} {
		es.AddFail(err2)
	}
	for range [2]struct{}{} {
		es.AddFail(err3)
	}

	infos, _ := es.Stats()
	res := FormatPrint(infos)

	t.Log(res)

	p, err := json.MarshalIndent(&res, "", "\t")
	t.Logf("%v -> %s", err, p)

	es2 := NewErrorStats()
	infos, _ = es2.Stats()
	p, err = json.MarshalIndent(&infos, "", "\t")
	t.Logf("%v -> %s", err, p)
}

func TestErrStrFormat(t *testing.T) {
	err1 := errors.New("Post http://127.0.0.1:xxx/xxx: EOF")
	err2 := errors.New("fake error")
	var err3 error

	require.Equal(t, "EOF", errStrFormat(err1))
	require.Equal(t, "fake error", errStrFormat(err2))
	require.Equal(t, "", errStrFormat(err3))
}

func TestAbnormalReport(t *testing.T) {
	rp := NewAbnormalReporter(proto.ClusterID(1), "", "")
	diskID := proto.DiskID(1)
	vuid := proto.Vuid(100)
	rp.ReportAbnormal(diskID, vuid)
	rp.CancelAbnormal(diskID, vuid)

	rp.SetVuidReported(vuid)
	require.True(t, rp.IsVuidReported(vuid))
	require.False(t, rp.IsVuidReported(vuid+1))
}

func TestNewDeleteBlobTaskReporter(t *testing.T) {
	clusterID := proto.ClusterID(1)
	reporter := NewDeleteBlobTaskReporter(clusterID)
	require.NotNil(t, reporter)
	require.NotNil(t, reporter.counterVecReporter)
	require.NotNil(t, reporter.counterVecReporter.counterVec)
}

func TestBlobTaskReporter_ReportSuccess(t *testing.T) {
	clusterID := proto.ClusterID(1)
	reporter := NewDeleteBlobTaskReporter(clusterID)
	shardID := proto.ShardID(100)

	// Report success multiple times
	reporter.ReportSuccess(shardID)
	reporter.ReportSuccess(shardID)
	reporter.ReportSuccess(shardID)

	// Verify metric value
	metric, err := reporter.counterVecReporter.counterVec.GetMetricWithLabelValues(
		shardID.ToString(),
		"success",
	)
	require.NoError(t, err)
	require.NotNil(t, metric)

	var m dto.Metric
	err = metric.Write(&m)
	require.NoError(t, err)
	require.NotNil(t, m.Counter)
	require.Equal(t, float64(3), *m.Counter.Value)
}

func TestBlobTaskReporter_ReportFailed(t *testing.T) {
	clusterID := proto.ClusterID(1)
	reporter := NewDeleteBlobTaskReporter(clusterID)
	shardID := proto.ShardID(200)

	// Report failed multiple times
	reporter.ReportFailed(shardID)
	reporter.ReportFailed(shardID)

	// Verify metric value
	metric, err := reporter.counterVecReporter.counterVec.GetMetricWithLabelValues(
		shardID.ToString(),
		"failed",
	)
	require.NoError(t, err)
	require.NotNil(t, metric)

	var m dto.Metric
	err = metric.Write(&m)
	require.NoError(t, err)
	require.NotNil(t, m.Counter)
	require.Equal(t, float64(2), *m.Counter.Value)
}

func TestNewRepairShardTaskReporter(t *testing.T) {
	clusterID := proto.ClusterID(2)
	reporter := NewRepairSliceTaskReporter(clusterID)
	require.NotNil(t, reporter)
	require.NotNil(t, reporter.counterVecReporter)
	require.NotNil(t, reporter.counterVecReporter.counterVec)
}

func TestNewDiskRocksdbStatusReporter(t *testing.T) {
	clusterID := proto.ClusterID(3)
	reporter := NewDiskRocksdbStatusReporter(clusterID)
	require.NotNil(t, reporter)
	require.NotNil(t, reporter.gaugeVecReporter)
	require.NotNil(t, reporter.gaugeVecReporter.gaugeVec)
}

func TestDiskRocksdbStatusReporter_Report(t *testing.T) {
	clusterID := proto.ClusterID(3)
	reporter := NewDiskRocksdbStatusReporter(clusterID)

	dbName := "test_db"
	path := "test_path"

	stats := kvstore.Stats{
		Used: 1024,
		MemoryUsage: kvstore.MemoryUsage{
			BlockCacheUsage:     512,
			IndexAndFilterUsage: 256,
			MemtableUsage:       128,
			BlockPinnedUsage:    64,
		},
	}

	reporter.Report(dbName, path, stats)

	// Verify metrics are set correctly
	// Label definition order: idc, rack, host, disk_id, db_name, item
	metrics := []string{"used", "block_cache_usage", "index_and_filter_usage", "memtable_usage", "block_pinned_usage"}
	values := []float64{1024, 512, 256, 128, 64}

	for i, metricName := range metrics {
		// GetMetricWithLabelValues must match the label definition order
		metric, err := reporter.gaugeVecReporter.gaugeVec.GetMetricWithLabelValues(
			path,
			dbName,
			metricName,
		)
		require.NoError(t, err)
		require.NotNil(t, metric)

		var m dto.Metric
		err = metric.Write(&m)
		require.NoError(t, err)
		require.NotNil(t, m.Gauge)
		require.Equal(t, values[i], *m.Gauge.Value)
	}
}

func TestNewShardMetaStatusReporter(t *testing.T) {
	clusterID := proto.ClusterID(4)
	reporter := NewShardMetaStatsReporter(clusterID)
	require.NotNil(t, reporter)
	require.NotNil(t, reporter.gaugeVecReporter)
	require.NotNil(t, reporter.gaugeVecReporter.gaugeVec)
}

func TestShardMetaStatusReporter_Report(t *testing.T) {
	clusterID := proto.ClusterID(4)
	reporter := NewShardMetaStatsReporter(clusterID)

	metaStats := snproto.ShardMetaStats{
		ShardID:   proto.ShardID(500),
		ItemCount: 1000,
		ItemSize:  2000,
		BlobCount: 3000,
		BlobSize:  4000,
	}

	reporter.Report(metaStats)

	// Verify metrics are set correctly
	metrics := []struct {
		name  string
		value float64
	}{
		{"item_count", 1000},
		{"item_size", 2000},
		{"blob_count", 3000},
		{"blob_size", 4000},
	}

	for _, m := range metrics {
		metric, err := reporter.gaugeVecReporter.gaugeVec.GetMetricWithLabelValues(
			metaStats.ShardID.ToString(),
			m.name,
		)
		require.NoError(t, err)
		require.NotNil(t, metric)

		var dtoMetric dto.Metric
		err = metric.Write(&dtoMetric)
		require.NoError(t, err)
		require.NotNil(t, dtoMetric.Gauge)
		require.Equal(t, m.value, *dtoMetric.Gauge.Value)
	}
}

func TestNewRaftStatusReporter(t *testing.T) {
	clusterID := proto.ClusterID(5)
	reporter := NewRaftStatsReporter(clusterID)
	require.NotNil(t, reporter)
	require.NotNil(t, reporter.gaugeVecReporter)
	require.NotNil(t, reporter.gaugeVecReporter.gaugeVec)
}

func TestShardRaftStatusReporter_Report(t *testing.T) {
	clusterID := proto.ClusterID(5)
	reporter := NewRaftStatsReporter(clusterID)

	suid := proto.EncodeSuid(proto.ShardID(600), 1, 1)
	shardStats := snapi.ShardStats{
		Suid:         suid,
		LeaderDiskID: proto.DiskID(100),
		RaftStat: raft.Stat{
			NodeID:  100,
			Applied: 1000,
			Commit:  2000,
		},
	}
	reporter.Report(shardStats)

	// Verify metrics are set correctly
	diskIDStr := "100"
	shardIDStr := shardStats.Suid.ShardID().ToString()
	suidStr := shardStats.Suid.ToString()

	metrics := []struct {
		name  string
		value float64
	}{
		{"leader", float64(shardStats.LeaderDiskID)},
		{"applied", float64(shardStats.RaftStat.Applied)},
		{"commit", float64(shardStats.RaftStat.Commit)},
	}

	for _, m := range metrics {
		metric, err := reporter.gaugeVecReporter.gaugeVec.GetMetricWithLabelValues(
			diskIDStr,
			shardIDStr,
			suidStr,
			m.name,
		)
		require.NoError(t, err)
		require.NotNil(t, metric)

		var dtoMetric dto.Metric
		err = metric.Write(&dtoMetric)
		require.NoError(t, err)
		require.NotNil(t, dtoMetric.Gauge)
		require.Equal(t, m.value, *dtoMetric.Gauge.Value)
	}
}

func TestNewDiskHealthReporter(t *testing.T) {
	clusterID := proto.ClusterID(6)
	reporter := NewDiskHealthReporter(clusterID)
	require.NotNil(t, reporter)
	require.NotNil(t, reporter.gaugeVecReporter)
	require.NotNil(t, reporter.gaugeVecReporter.gaugeVec)
}

func TestDiskHealthReporter_ReportHealthy(t *testing.T) {
	clusterID := proto.ClusterID(6)
	reporter := NewDiskHealthReporter(clusterID)
	reporter.ReportHealthy("test_path")
	require.NotNil(t, reporter.gaugeVecReporter.gaugeVec)

	metric, err := reporter.gaugeVecReporter.gaugeVec.GetMetricWithLabelValues("test_path")
	require.NoError(t, err)
	require.NotNil(t, metric)

	var m dto.Metric
	err = metric.Write(&m)
	require.NoError(t, err)
	require.NotNil(t, m.Gauge)
	require.Equal(t, float64(0), *m.Gauge.Value)
}

func TestDiskHealthReporter_ReportUnhealthy(t *testing.T) {
	clusterID := proto.ClusterID(6)
	reporter := NewDiskHealthReporter(clusterID)
	reporter.ReportUnhealthy("test_path")
	require.NotNil(t, reporter.gaugeVecReporter.gaugeVec)

	metric, err := reporter.gaugeVecReporter.gaugeVec.GetMetricWithLabelValues("test_path")
	require.NoError(t, err)
	require.NotNil(t, metric)

	var m dto.Metric
	err = metric.Write(&m)
	require.NoError(t, err)
	require.NotNil(t, m.Gauge)
	require.Equal(t, float64(1), *m.Gauge.Value)
}

func TestReporterAlreadyRegistered(t *testing.T) {
	// Test that creating multiple reporters with the same clusterID doesn't panic
	clusterID := proto.ClusterID(10)

	// Create first reporter
	reporter1 := NewDeleteBlobTaskReporter(clusterID)
	require.NotNil(t, reporter1)

	// Create second reporter with same clusterID (should reuse existing metric)
	reporter2 := NewDeleteBlobTaskReporter(clusterID)
	require.NotNil(t, reporter2)

	// Both should work
	shardID := proto.ShardID(1000)
	reporter1.ReportSuccess(shardID)
	reporter2.ReportSuccess(shardID)

	// Verify both increments are counted
	metric, err := reporter1.counterVecReporter.counterVec.GetMetricWithLabelValues(
		shardID.ToString(),
		"success",
	)
	require.NoError(t, err)
	require.NotNil(t, metric)

	var m dto.Metric
	err = metric.Write(&m)
	require.NoError(t, err)
	require.NotNil(t, m.Counter)
	require.Equal(t, float64(2), *m.Counter.Value)
}
