// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Package master requires the same build environment as production (e.g. RocksDB headers for CGO).

package master

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// newTestGaugeVec builds a GaugeVec without touching the global exporter enable flag or default registry.
func newTestGaugeVec(t *testing.T, name string, labels []string) *exporter.GaugeVec {
	t.Helper()
	v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name}, labels)
	return &exporter.GaugeVec{GaugeVec: v}
}

func TestMonitorMetrics_nodeStatTagLabel(t *testing.T) {
	t.Parallel()
	require.Equal(t, "null", nodeStatTagLabel(""))
	require.Equal(t, "ssd", nodeStatTagLabel("ssd"))
	require.Equal(t, "pool-tag", nodeStatTagLabel("pool-tag"))
	require.Equal(t, "  ", nodeStatTagLabel("  "))
}

func TestNewMonitorMetrics_initializesMaps(t *testing.T) {
	t.Parallel()
	mm := newMonitorMetrics(nil)
	require.NotNil(t, mm)
	require.Nil(t, mm.cluster)
	require.NotNil(t, mm.volNames)
	require.NotNil(t, mm.badDisks)
	require.NotNil(t, mm.flashNodesBadDisks)
	require.NotNil(t, mm.inconsistentMps)
	require.NotNil(t, mm.replicaCntMap)
	require.NotNil(t, mm.lcId)
	require.False(t, mm.lastLeaderResetTime.IsZero())
}

func TestShouldPeriodicResetLeaderMetrics(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	enabled := &clusterConfig{EnableLeaderMetricsReset: true}
	disabled := &clusterConfig{EnableLeaderMetricsReset: false}

	t.Run("nil_config", func(t *testing.T) {
		t.Parallel()
		require.False(t, shouldPeriodicResetLeaderMetrics(nil, now.Add(-leaderMetricsPeriodicResetInterval-time.Minute), now))
	})

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()
		require.False(t, shouldPeriodicResetLeaderMetrics(disabled, now.Add(-leaderMetricsPeriodicResetInterval-time.Minute), now))
	})

	t.Run("within_interval", func(t *testing.T) {
		t.Parallel()
		require.False(t, shouldPeriodicResetLeaderMetrics(enabled, now.Add(-leaderMetricsPeriodicResetInterval+time.Second), now))
	})

	t.Run("past_interval", func(t *testing.T) {
		t.Parallel()
		require.True(t, shouldPeriodicResetLeaderMetrics(enabled, now.Add(-leaderMetricsPeriodicResetInterval-time.Second), now))
	})

	t.Run("exact_interval_not_due", func(t *testing.T) {
		t.Parallel()
		require.False(t, shouldPeriodicResetLeaderMetrics(enabled, now.Add(-leaderMetricsPeriodicResetInterval), now))
	})
}

// TestLeaderMetricsPeriodicResetBlock mirrors the leader branch in statMetrics without
// starting the ticker loop (same condition + reset side effects).
func TestLeaderMetricsPeriodicResetBlock_resetsWhenDue(t *testing.T) {
	mm := newMonitorMetricsForLeaderResetTest(t)
	mm.cluster = &Cluster{cfg: &clusterConfig{EnableLeaderMetricsReset: true}}

	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	mm.lastLeaderResetTime = now.Add(-leaderMetricsPeriodicResetInterval - time.Minute)
	mm.volTotalSpace.SetWithLabelValues(10, "stale-vol")

	if shouldPeriodicResetLeaderMetrics(mm.cluster.cfg, mm.lastLeaderResetTime, now) {
		mm.resetAllLeaderMetrics()
		mm.lastLeaderResetTime = now
	}

	require.Equal(t, now, mm.lastLeaderResetTime)
	require.Equal(t, 0.0, gaugeVecValue(t, mm.volTotalSpace, "stale-vol"))
}

func TestCheckHostSelection_returnsTrue(t *testing.T) {
	t.Parallel()
	mm := newMonitorMetrics(&Cluster{})
	vol := &Vol{}
	require.True(t, mm.checkHostSelection(TypeDataPartition, vol, 0, proto.RackAwareNone))
	require.True(t, mm.checkHostSelection(TypeMetaPartition, vol, 0, proto.RackAwareNone))
	require.True(t, mm.checkHostSelection(TypeRocksdbPartition, vol, 1, proto.RackAwareNone))
}

func TestWarningMetrics_reset_clearsState(t *testing.T) {
	t.Parallel()
	c := &Cluster{Name: "test-warn-reset"}
	wm := newWarningMetrics(c)

	wm.dpMutex.Lock()
	wm.dpNoLeaderInfo[10] = NoLeaderPartInfo{ReportTime: 1, Replicas: 3}
	wm.dpMutex.Unlock()

	wm.mpMutex.Lock()
	wm.mpNoLeaderInfo[20] = NoLeaderPartInfo{ReportTime: 2, Replicas: 2}
	wm.mpMutex.Unlock()

	wm.dpMissingReplicaMutex.Lock()
	wm.dpMissingReplicaInfo["5"] = addrSet{addrs: map[string]voidType{"h1": voidVal}, replicaAlive: "1", replicaNum: "3"}
	wm.dpMissingReplicaMutex.Unlock()

	wm.mpMissingReplicaMutex.Lock()
	wm.mpMissingReplicaInfo["6"] = addrSet{addrs: map[string]voidType{"h2": voidVal}}
	wm.mpMissingReplicaMutex.Unlock()

	wm.reset()

	require.Empty(t, wm.dpNoLeaderInfo)
	require.Empty(t, wm.mpNoLeaderInfo)
	require.Empty(t, wm.dpMissingReplicaInfo)
	require.Empty(t, wm.mpMissingReplicaInfo)
}

func TestWarnMissingDp_wrongCluster_noop(t *testing.T) {
	t.Parallel()
	wm := newWarningMetrics(&Cluster{Name: "a"})
	wm.WarnMissingDp("b", "addr", 1, true)
	wm.dpMissingReplicaMutex.Lock()
	n := len(wm.dpMissingReplicaInfo)
	wm.dpMissingReplicaMutex.Unlock()
	require.Zero(t, n)
}

func TestWarnMissingDp_reportFalse_missingEntry_noPanic(t *testing.T) {
	t.Parallel()
	wm := newWarningMetrics(&Cluster{Name: "c"})
	require.NotPanics(t, func() {
		wm.WarnMissingDp("c", "addr", 99, false)
	})
}

func TestWarnMissingDp_reportTrue_recordsAddr(t *testing.T) {
	t.Parallel()
	wm := newWarningMetrics(&Cluster{Name: "d"})
	wm.WarnMissingDp("d", "10.0.0.1", 7, true)
	id := "7"
	wm.dpMissingReplicaMutex.Lock()
	as, ok := wm.dpMissingReplicaInfo[id]
	n := len(as.addrs)
	wm.dpMissingReplicaMutex.Unlock()
	require.True(t, ok)
	require.Equal(t, 1, n)
	_, has := as.addrs["10.0.0.1"]
	require.True(t, has)
}

func TestWarnDpNoLeader_wrongCluster_noop(t *testing.T) {
	t.Parallel()
	wm := newWarningMetrics(&Cluster{Name: "e"})
	wm.WarnDpNoLeader("other", 1, 3, true)
	wm.dpMutex.Lock()
	n := len(wm.dpNoLeaderInfo)
	wm.dpMutex.Unlock()
	require.Zero(t, n)
}

func TestWarnDpNoLeader_reportTrue_thenFalse_clears(t *testing.T) {
	t.Parallel()
	wm := newWarningMetrics(&Cluster{Name: "f"})
	wm.WarnDpNoLeader("f", 100, 3, true)
	wm.WarnDpNoLeader("f", 100, 3, false)
	wm.dpMutex.Lock()
	_, ok := wm.dpNoLeaderInfo[100]
	wm.dpMutex.Unlock()
	require.False(t, ok)
}

func TestDeleteS3LcVolMetric_resetsGaugeVecs(t *testing.T) {
	mm := &monitorMetrics{
		lcVolStatus:       newTestGaugeVec(t, "test_lc_vol_status", []string{"id"}),
		lcVolScanned:      newTestGaugeVec(t, "test_lc_vol_scanned", []string{"id", "type"}),
		lcVolExpired:      newTestGaugeVec(t, "test_lc_vol_expired", []string{"id", "type"}),
		lcVolMigrateBytes: newTestGaugeVec(t, "test_lc_vol_migrate_bytes", []string{"id", "type"}),
		lcVolError:        newTestGaugeVec(t, "test_lc_vol_error", []string{"id", "type"}),
	}
	mm.lcVolStatus.SetWithLabelValues(1, "vol1")
	mm.lcVolScanned.SetWithLabelValues(2, "vol1", "file")
	mm.deleteS3LcVolMetric("vol1")
	require.NotPanics(t, func() {
		mm.lcVolStatus.SetWithLabelValues(0, "vol1")
	})
}

func TestClearDiskErrMetrics_noPanic(t *testing.T) {
	mm := &monitorMetrics{
		badDisks:  map[string]string{"/data/disk1": "127.0.0.1"},
		diskError: newTestGaugeVec(t, "test_disk_error", []string{"addr", "path"}),
	}
	mm.diskError.SetWithLabelValues(1, "127.0.0.1", "/data/disk1")
	require.NotPanics(t, func() { mm.clearDiskErrMetrics() })
	require.Contains(t, mm.badDisks, "/data/disk1")
}

func TestClearFlashNodesDiskErrMetrics_noPanic(t *testing.T) {
	mm := &monitorMetrics{
		flashNodesBadDisks:  map[string]string{"/flash/d0": "10.1.1.1"},
		flashNodesDiskError: newTestGaugeVec(t, "test_flash_disk_error", []string{"addr", "path"}),
	}
	mm.flashNodesDiskError.SetWithLabelValues(1, "10.1.1.1", "/flash/d0")
	require.NotPanics(t, func() { mm.clearFlashNodesDiskErrMetrics() })
	require.Contains(t, mm.flashNodesBadDisks, "/flash/d0")
}

func gaugeVecValue(t *testing.T, gv *exporter.GaugeVec, lvs ...string) float64 {
	t.Helper()
	m, err := gv.GaugeVec.GetMetricWithLabelValues(lvs...)
	require.NoError(t, err)
	var pb dto.Metric
	require.NoError(t, m.Write(&pb))
	return pb.GetGauge().GetValue()
}

func nodeStatMetaAddrs(t *testing.T, gv *exporter.GaugeVec) map[string]struct{} {
	t.Helper()
	ch := make(chan prometheus.Metric, 256)
	go func() {
		gv.Collect(ch)
		close(ch)
	}()

	addrs := make(map[string]struct{})
	for m := range ch {
		var pb dto.Metric
		require.NoError(t, m.Write(&pb))
		var nodeType, addr string
		for _, lp := range pb.Label {
			switch lp.GetName() {
			case "type":
				nodeType = lp.GetValue()
			case "addr":
				addr = lp.GetValue()
			}
		}
		if nodeType == MetricRoleMetaNode && addr != "" {
			addrs[addr] = struct{}{}
		}
	}
	return addrs
}

func monitorMetricsVolTestCluster(t *testing.T) *Cluster {
	t.Helper()
	c := clusterStatTestCluster(t, "vol-metrics")
	c.poolMutex.Lock()
	if c.storagePools == nil {
		c.storagePools = make(map[uint8]*StoragePool)
	}
	c.storagePools[10] = &StoragePool{
		Id:           10,
		Name:         "ssd-pool",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
		Status:       proto.PoolStatusAvailable,
	}
	c.storagePools[11] = &StoragePool{
		Id:           11,
		Name:         "hdd-pool",
		StorageClass: uint8(proto.StorageClass_Replica_HDD),
		Status:       proto.PoolStatusAvailable,
	}
	c.poolMutex.Unlock()
	return c
}

func newMonitorMetricsForVolStatTest(t *testing.T, c *Cluster) *monitorMetrics {
	t.Helper()
	mm := newMonitorMetrics(c)
	mm.volStats = newTestGaugeVec(t, "test_vol_stats", []string{"volName", "type", "media"})
	mm.volTotalSpace = newTestGaugeVec(t, "test_vol_total_GB", []string{"volName"})
	mm.volUsedSpace = newTestGaugeVec(t, "test_vol_used_GB", []string{"volName"})
	mm.volUsage = newTestGaugeVec(t, "test_vol_usage_ratio", []string{"volName"})
	mm.volMetaCount = newTestGaugeVec(t, "test_vol_meta_count", []string{"volName", "type"})
	return mm
}

func TestSetVolMetrics_reportsPoolWritableDpByPool(t *testing.T) {
	c := monitorMetricsVolTestCluster(t)
	vol := clusterStatTestVol("rw-dp-vol", proto.VolStatusNormal, 100)
	vol.dataPartitions.setReadWriteCntByPoolId(map[uint8]int{
		10: 5,
		11: 12,
	})
	vol.StatByPool = []*proto.StatOfStorageClass{
		proto.NewStatOfStorageClassByPoolWithQuota(10, 100),
		proto.NewStatOfStorageClassByPoolWithQuota(11, 100),
	}

	c.volMutex.Lock()
	c.vols["rw-dp-vol"] = vol
	c.volMutex.Unlock()

	mm := newMonitorMetricsForVolStatTest(t, c)
	mm.setVolMetrics()

	require.Equal(t, float64(5), gaugeVecValue(t, mm.volStats, "rw-dp-vol", "pool_writable_dp", "ssd-pool"))
	require.Equal(t, float64(12), gaugeVecValue(t, mm.volStats, "rw-dp-vol", "pool_writable_dp", "hdd-pool"))
}

func TestSetVolMetrics_poolWritableDpUsesUnknownPoolNameWhenPoolMissing(t *testing.T) {
	c := monitorMetricsVolTestCluster(t)
	vol := clusterStatTestVol("unknown-pool-vol", proto.VolStatusNormal, 50)
	vol.dataPartitions.setReadWriteCntByPoolId(map[uint8]int{99: 3})

	c.volMutex.Lock()
	c.vols["unknown-pool-vol"] = vol
	c.volMutex.Unlock()

	mm := newMonitorMetricsForVolStatTest(t, c)
	mm.setVolMetrics()

	require.Equal(t, float64(3), gaugeVecValue(t, mm.volStats, "unknown-pool-vol", "pool_writable_dp", "UnknownPool-99"))
}

func TestSetVolMetrics_poolWritableDpPerVolume(t *testing.T) {
	c := monitorMetricsVolTestCluster(t)

	volA := clusterStatTestVol("vol-a", proto.VolStatusNormal, 100)
	volA.dataPartitions.setReadWriteCntByPoolId(map[uint8]int{10: 2})

	volB := clusterStatTestVol("vol-b", proto.VolStatusNormal, 100)
	volB.dataPartitions.setReadWriteCntByPoolId(map[uint8]int{10: 7})

	c.volMutex.Lock()
	c.vols["vol-a"] = volA
	c.vols["vol-b"] = volB
	c.volMutex.Unlock()

	mm := newMonitorMetricsForVolStatTest(t, c)
	mm.setVolMetrics()

	require.Equal(t, float64(2), gaugeVecValue(t, mm.volStats, "vol-a", "pool_writable_dp", "ssd-pool"))
	require.Equal(t, float64(7), gaugeVecValue(t, mm.volStats, "vol-b", "pool_writable_dp", "ssd-pool"))
}

func newMonitorMetricsForLeaderResetTest(t *testing.T) *monitorMetrics {
	t.Helper()
	mm := newMonitorMetrics(&Cluster{})

	mm.volTotalSpace = newTestGaugeVec(t, "test_reset_vol_total_GB", []string{"volName"})
	mm.volUsedSpace = newTestGaugeVec(t, "test_reset_vol_used_GB", []string{"volName"})
	mm.volUsage = newTestGaugeVec(t, "test_reset_vol_usage_ratio", []string{"volName"})
	mm.volStats = newTestGaugeVec(t, "test_reset_vol_stats", []string{"volName", "type", "media"})
	mm.volMetaCount = newTestGaugeVec(t, "test_reset_vol_meta_count", []string{"volName", "type"})
	mm.diskError = newTestGaugeVec(t, "test_reset_disk_error", []string{"addr", "path"})
	mm.flashNodesDiskError = newTestGaugeVec(t, "test_reset_flash_disk_error", []string{"addr", "path"})
	mm.metaEqualCheckFail = newTestGaugeVec(t, "test_reset_mp_inconsistent", []string{"volume", "mpId"})
	mm.lcVolStatus = newTestGaugeVec(t, "test_reset_lc_vol_status", []string{"id"})
	mm.lcVolScanned = newTestGaugeVec(t, "test_reset_lc_vol_scanned", []string{"id", "type"})
	mm.lcVolExpired = newTestGaugeVec(t, "test_reset_lc_vol_expired", []string{"id", "type"})
	mm.lcVolMigrateBytes = newTestGaugeVec(t, "test_reset_lc_vol_migrate_bytes", []string{"id", "type"})
	mm.lcVolError = newTestGaugeVec(t, "test_reset_lc_vol_error", []string{"id", "type"})
	mm.partitionCreate = newTestGaugeVec(t, "test_reset_partition_create_alarm", []string{"type", "racklevel", "media"})
	mm.dataNodeStat = newTestGaugeVec(t, "test_reset_dataNodes_stats", []string{"media", "type"})
	mm.nodeStat = newTestGaugeVec(t, "test_reset_node_stat", []string{"type", "addr", "stat", "zone", "set", "media", "writable", "alloc", "rack", "pool", "tag"})
	mm.diskLost = newTestGaugeVec(t, "test_reset_disk_lost", []string{"addr", "path"})
	mm.dpNoSamePeer = newTestGaugeVec(t, "test_reset_dp_no_same_peer", []string{"dpId"})
	mm.mpNoSamePeer = newTestGaugeVec(t, "test_reset_mp_no_same_peer", []string{"mpId"})
	mm.badDiskDecommissionTimeOverLimit = newTestGaugeVec(t, "test_reset_bad_disk_decommission_time_over_limit", []string{"addr", "path", "firstReportTime"})
	mm.diskDecommissionSuccess = newTestGaugeVec(t, "test_reset_disk_decommission_success", []string{"addr", "path"})
	mm.InactiveDataNodeInfo = newTestGaugeVec(t, "test_reset_inactive_dataNodes_info", []string{"clusterName", "addr"})
	mm.InactiveMetaNodeInfo = newTestGaugeVec(t, "test_reset_inactive_metaNodes_info", []string{"clusterName", "addr"})
	mm.InactiveMasterInfo = newTestGaugeVec(t, "test_reset_inactive_masters_info", []string{"clusterName", "addr"})
	mm.InactiveFlashNodeInfo = newTestGaugeVec(t, "test_reset_inactive_flashNodes_info", []string{"clusterName", "addr"})
	mm.ReplicaMissingDPCount = newTestGaugeVec(t, "test_reset_replica_missing_dp_count", []string{"replicaNum", "media"})
	mm.DpMissingLeaderCount = newTestGaugeVec(t, "test_reset_dp_missing_Leader_count", []string{"replicaNum", "media"})
	mm.MpRegionInfo = newTestGaugeVec(t, "test_reset_mp_region_info", []string{"volume", "type"})

	mm.dataNodesCount = exporter.NewGauge("test_reset_dataNodes_count")
	mm.metaNodesCount = exporter.NewGauge("test_reset_metaNodes_count")
	mm.lcNodesCount = exporter.NewGauge("test_reset_lc_nodes_count")
	mm.volCount = exporter.NewGauge("test_reset_vol_count")
	mm.dataNodesTotal = exporter.NewGauge("test_reset_dataNodes_total_GB")
	mm.dataNodesUsed = exporter.NewGauge("test_reset_dataNodes_used_GB")
	mm.dataNodeIncreased = exporter.NewGauge("test_reset_dataNodes_increased_GB")
	mm.metaNodesTotal = exporter.NewGauge("test_reset_metaNodes_total_GB")
	mm.metaNodesUsed = exporter.NewGauge("test_reset_metaNodes_used_GB")
	mm.metaNodesIncreased = exporter.NewGauge("test_reset_metaNodes_increased_GB")
	mm.badMpCount = exporter.NewGauge("test_reset_bad_mp_count")
	mm.badDpCount = exporter.NewGauge("test_reset_bad_dp_count")
	mm.dpUnableDecommissionCount = exporter.NewGauge("test_reset_dp_unable_decommission_count")
	mm.dataNodesInactive = exporter.NewGauge("test_reset_dataNodes_inactive")
	mm.metaNodesInactive = exporter.NewGauge("test_reset_metaNodes_inactive")
	mm.mastersInactive = exporter.NewGauge("test_reset_masters_inactive")
	mm.flashNodesInactive = exporter.NewGauge("test_reset_flashNodes_inactive")
	mm.dataNodesNotWritable = exporter.NewGauge("test_reset_dataNodes_not_writable")
	mm.dataNodesAllocable = exporter.NewGauge("test_reset_dataNodes_allocable")
	mm.metaNodesNotWritable = exporter.NewGauge("test_reset_metaNodes_not_writable")
	mm.metaNodesNotRocksdbWritable = exporter.NewGauge("test_reset_meta_rocksdb_not_writable")
	mm.MpMissingLeaderCount = exporter.NewGauge("test_reset_mp_missing_Leader_count")
	mm.MpMissingReplicaCount = exporter.NewGauge("test_reset_mp_missing_Replica_count")
	mm.MpFailedRecoveryCount = exporter.NewGauge("test_reset_mp_failed_recovery_count")
	mm.ssdNodeSetUnbalancedDPs = exporter.NewGauge("test_reset_ssd_nodeset_unbalanced_dp_count")
	mm.ssdRackConflictDPs = exporter.NewGauge("test_reset_ssd_rack_conflict_dp_count")
	mm.hddNodeSetUnbalancedDPs = exporter.NewGauge("test_reset_hdd_nodeset_unbalanced_dp_count")
	mm.hddRackConflictDPs = exporter.NewGauge("test_reset_hdd_rack_conflict_dp_count")

	return mm
}

func newMonitorMetricsForNodeStatTest(t *testing.T, c *Cluster) *monitorMetrics {
	t.Helper()
	mm := newMonitorMetrics(c)
	mm.nodeStat = newTestGaugeVec(t, "test_node_stat", []string{"type", "addr", "stat", "zone", "set", "media", "writable", "alloc", "rack", "pool", "tag"})
	mm.InactiveMetaNodeInfo = newTestGaugeVec(t, "test_inactive_meta", []string{"clusterName", "addr"})
	mm.metaNodesInactive = exporter.NewGauge("test_meta_inactive")
	return mm
}

func TestUpdateMetaNodesStat_withoutReset_keepsRemovedNodeSeries(t *testing.T) {
	cluster := clusterStatTestCluster(t, "node-stat-stale")
	addrA := "10.226.96.37:17210"
	addrB := "10.32.122.3:17210"
	zone := "stat-test-zone"

	cluster.metaNodes.Store(addrA, clusterStatWritableMetaNode(addrA, zone))
	cluster.metaNodes.Store(addrB, clusterStatWritableMetaNode(addrB, zone))

	mm := newMonitorMetricsForNodeStatTest(t, cluster)
	mm.updateMetaNodesStat()
	require.Contains(t, nodeStatMetaAddrs(t, mm.nodeStat), addrA)
	require.Contains(t, nodeStatMetaAddrs(t, mm.nodeStat), addrB)

	cluster.metaNodes.Delete(addrA)
	mm.updateMetaNodesStat()
	require.Contains(t, nodeStatMetaAddrs(t, mm.nodeStat), addrA, "removed node series remain until vec reset")
	require.Contains(t, nodeStatMetaAddrs(t, mm.nodeStat), addrB)
}

func TestUpdateMetaNodesStat_afterReset_dropsRemovedNodeSeries(t *testing.T) {
	cluster := clusterStatTestCluster(t, "node-stat-reset")
	addrA := "10.226.96.37:17210"
	addrB := "10.32.122.3:17210"
	zone := "stat-test-zone"

	cluster.metaNodes.Store(addrA, clusterStatWritableMetaNode(addrA, zone))
	cluster.metaNodes.Store(addrB, clusterStatWritableMetaNode(addrB, zone))

	mm := newMonitorMetricsForNodeStatTest(t, cluster)
	mm.updateMetaNodesStat()
	require.Contains(t, nodeStatMetaAddrs(t, mm.nodeStat), addrA)

	cluster.metaNodes.Delete(addrA)
	// resetAllLeaderMetrics clears nodeStat; full helper needs all gauge vecs initialized.
	mm.nodeStat.Reset()
	mm.updateMetaNodesStat()

	addrs := nodeStatMetaAddrs(t, mm.nodeStat)
	require.NotContains(t, addrs, addrA)
	require.Contains(t, addrs, addrB)
}

func TestResetAllLeaderMetricsClearsNodeStatSeries(t *testing.T) {
	mm := newMonitorMetricsForLeaderResetTest(t)
	mm.nodeStat.SetWithLabelValues(1, MetricRoleMetaNode, "10.0.0.1:17210", "alloc", "z", "1", "default", "true", "true", "r", "reg", "null")

	mm.resetAllLeaderMetrics()

	require.Empty(t, nodeStatMetaAddrs(t, mm.nodeStat))
}

func TestResetAllLeaderMetricsClearsFollowerUnsafeGaugeVecs(t *testing.T) {
	mm := newMonitorMetricsForLeaderResetTest(t)

	mm.volTotalSpace.SetWithLabelValues(10, "vol1")
	mm.volUsedSpace.SetWithLabelValues(5, "vol1")
	mm.volUsage.SetWithLabelValues(0.5, "vol1")
	mm.volMetaCount.SetWithLabelValues(3, "vol1", "dp")
	mm.volStats.SetWithLabelValues(8, "vol1", "pool_writable_dp", "ssd-pool")
	mm.InactiveDataNodeInfo.SetWithLabelValues(1, "cluster1", "dn1")
	mm.InactiveMetaNodeInfo.SetWithLabelValues(1, "cluster1", "mn1")
	mm.MpRegionInfo.SetWithLabelValues(1, "vol1", "lease_timeout")
	mm.ReplicaMissingDPCount.SetWithLabelValues(2, "3", "SSD")
	mm.DpMissingLeaderCount.SetWithLabelValues(4, "3", "SSD")
	mm.lcId["lc-vol1"] = struct{}{}
	mm.lcVolStatus.SetWithLabelValues(1, "lc-vol1")
	mm.lcVolScanned.SetWithLabelValues(6, "lc-vol1", "file")

	mm.resetAllLeaderMetrics()

	require.Zero(t, gaugeVecValue(t, mm.volTotalSpace, "vol1"))
	require.Zero(t, gaugeVecValue(t, mm.volUsedSpace, "vol1"))
	require.Zero(t, gaugeVecValue(t, mm.volUsage, "vol1"))
	require.Zero(t, gaugeVecValue(t, mm.volMetaCount, "vol1", "dp"))
	require.Zero(t, gaugeVecValue(t, mm.volStats, "vol1", "pool_writable_dp", "ssd-pool"))
	require.Zero(t, gaugeVecValue(t, mm.InactiveDataNodeInfo, "cluster1", "dn1"))
	require.Zero(t, gaugeVecValue(t, mm.InactiveMetaNodeInfo, "cluster1", "mn1"))
	require.Zero(t, gaugeVecValue(t, mm.MpRegionInfo, "vol1", "lease_timeout"))
	require.Zero(t, gaugeVecValue(t, mm.ReplicaMissingDPCount, "3", "SSD"))
	require.Zero(t, gaugeVecValue(t, mm.DpMissingLeaderCount, "3", "SSD"))
	require.Zero(t, gaugeVecValue(t, mm.lcVolStatus, "lc-vol1"))
	require.Zero(t, gaugeVecValue(t, mm.lcVolScanned, "lc-vol1", "file"))
	require.Empty(t, mm.lcId)
}
