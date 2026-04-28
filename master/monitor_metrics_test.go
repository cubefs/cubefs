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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/prometheus/client_golang/prometheus"
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
