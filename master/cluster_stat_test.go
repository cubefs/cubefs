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
//
// Master package tests require RocksDB CGO; run: source build/cgo_env.sh

package master

import (
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

// clusterStatTestCluster builds a minimal Cluster for stat-only paths without full master topology.
func clusterStatTestCluster(tb testing.TB, name string) *Cluster {
	topo := newTopology()
	zone := newZone("stat-test-zone", proto.MediaType_HDD)
	require.NoError(tb, topo.putZone(zone))

	c := &Cluster{
		Name: name,
		cfg:  newClusterConfig(),
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
	}
	c.t = topo
	c.dataNodeStatInfo = new(nodeStatInfo)
	c.metaNodeStatInfo = new(nodeStatInfo)
	c.zoneStatInfos = make(map[string]*proto.ZoneStat)
	c.dataStatsByMedia = make(map[string]*nodeStatInfo)
	return c
}

func clusterStatWritableDataNode(addr, zone string, media uint32) *DataNode {
	hb := strconv.Itoa(raftstore.DefaultHeartbeatPort)
	rp := strconv.Itoa(raftstore.DefaultReplicaPort)
	dn := newDataNode(addr, hb, rp, zone, "", "stat-test", media)
	dn.Total = 2000 * util.GB
	dn.Used = 100 * util.GB
	dn.AvailableSpace = dn.Total - dn.Used
	dn.PreReservedSpace = 0
	dn.isActive = true
	dn.RdOnly = false
	dn.DataPartitionCount = 0
	dn.AllDisks = []string{"/cfs/disk1"}
	dn.ReportTime = time.Now()
	return dn
}

func clusterStatWritableMetaNode(addr, zone string) *MetaNode {
	hb := strconv.Itoa(raftstore.DefaultHeartbeatPort)
	rp := strconv.Itoa(raftstore.DefaultReplicaPort)
	mn := newMetaNode(addr, hb, rp, zone, "", "stat-test")
	mn.Total = 10 * util.GB
	mn.Used = util.GB
	mn.IsActive = true
	mn.RdOnly = false
	mn.MetaPartitionCount = 0
	mn.MaxMemAvailWeight = defaultMetaNodeReservedMem * 4
	mn.NodeMemTotal = 100 * util.GB
	mn.NodeMemUsed = util.GB
	mn.Threshold = 0.75
	return mn
}

func clusterStatRocksdbWritableMetaNode(addr, zone string) *MetaNode {
	mn := clusterStatWritableMetaNode(addr, zone)
	mn.RocksdbDisks = []*proto.MetaNodeRocksdbInfo{
		{Status: proto.ReadWrite, Total: 10000, Used: 1000, KeyNum: 1},
	}
	mn.RocksdbRdOnly = false
	mn.RocksdbKeyNumMax = 0
	mn.RocksdbDiskThreshold = defaultRocksdbDiskThreshold
	return mn
}

func clusterStatTestVol(name string, status uint8, capacityGB uint64) *Vol {
	vol := &Vol{
		Name:             name,
		Status:           status,
		Capacity:         capacityGB,
		MetaPartitions:   make(map[uint64]*MetaPartition),
		dataPartitions:   newDataPartitionMap(name),
		mpsLock:          new(mpsLockManager),
		VolType:          proto.VolumeTypeHot,
		DefaultStoreMode: proto.StoreModeMem,
	}
	vol.MetaPartitions[1] = &MetaPartition{PartitionID: 1, InodeCount: 100}
	vol.MetaPartitions[2] = &MetaPartition{PartitionID: 2, InodeCount: 23}
	return vol
}

func TestNewVolStatInfo(t *testing.T) {
	t.Parallel()
	t.Run("used_ratio_three_decimals", func(t *testing.T) {
		t.Parallel()
		vs := newVolStatInfo("vol-a", 8000, 2000, 50)
		require.Equal(t, "vol-a", vs.Name)
		require.Equal(t, uint64(8000), vs.TotalSize)
		require.Equal(t, uint64(2000), vs.UsedSize)
		require.Equal(t, uint64(50), vs.InodeCount)
		want := strconv.FormatFloat(2000.0/8000.0, 'f', 3, 32)
		require.Equal(t, want, vs.UsedRatio)
	})
	t.Run("full_allocation", func(t *testing.T) {
		t.Parallel()
		vs := newVolStatInfo("vol-b", 100, 100, 0)
		require.Equal(t, "1.000", vs.UsedRatio)
	})
	t.Run("tiny_usage", func(t *testing.T) {
		t.Parallel()
		vs := newVolStatInfo("vol-c", 1_000_000, 1, 9)
		require.Equal(t, uint64(9), vs.InodeCount)
		require.Contains(t, vs.UsedRatio, "0.000")
	})
}

func TestNewZoneStatInfo(t *testing.T) {
	t.Parallel()
	zs := newZoneStatInfo()
	require.NotNil(t, zs)
	require.NotNil(t, zs.DataNodeStat)
	require.NotNil(t, zs.MetaNodeStat)
	require.EqualValues(t, 0, zs.DataNodeStat.TotalNodes)
	require.EqualValues(t, 0, zs.MetaNodeStat.TotalNodes)
}

func TestFixedPoint(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		x     float64
		scale int
		want  float64
	}{
		{"two_decimals_round_down", 1.234, 2, 1.23},
		{"two_decimals_round_up", 1.235, 2, 1.24},
		{"three_decimals", 3.1415926, 3, 3.142},
		{"zero", 0, 4, 0},
		// Avoid *.5 rounding ties; use values slightly off half-integers for stable math.Round.
		{"negative", -1.004, 2, -1.0},
		{"large_scale", 2.5, 0, 3},
		{"boundary_half", 0.125, 2, 0.13},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := fixedPoint(tc.x, tc.scale)
			require.InDelta(t, tc.want, got, 1e-9, "fixedPoint(%v,%d)", tc.x, tc.scale)
		})
	}
}

func TestUpdateDataNodeStatInfo(t *testing.T) {
	t.Run("no_data_nodes_early_return", func(t *testing.T) {
		c := clusterStatTestCluster(t, "dn-empty")
		prev := *c.dataNodeStatInfo
		c.updateDataNodeStatInfo()
		require.Equal(t, prev, *c.dataNodeStatInfo)
		require.Empty(t, c.dataStatsByMedia)
	})

	t.Run("aggregates_totals_media_and_avail", func(t *testing.T) {
		c := clusterStatTestCluster(t, "dn-agg")
		dnHDD := clusterStatWritableDataNode("10.0.0.1:17320", "stat-test-zone", proto.MediaType_HDD)
		dnSSD := clusterStatWritableDataNode("10.0.0.2:17320", "stat-test-zone", proto.MediaType_SSD)
		dnInactive := clusterStatWritableDataNode("10.0.0.3:17320", "stat-test-zone", proto.MediaType_HDD)
		dnInactive.isActive = false

		c.dataNodes.Store(dnHDD.Addr, dnHDD)
		c.dataNodes.Store(dnSSD.Addr, dnSSD)
		c.dataNodes.Store(dnInactive.Addr, dnInactive)

		c.dataNodeStatInfo.UsedGB = 50
		c.updateDataNodeStatInfo()

		totalBytes := dnHDD.Total + dnSSD.Total + dnInactive.Total
		usedBytes := dnHDD.Used + dnSSD.Used + dnInactive.Used
		availBytes := dnHDD.AvailableSpace + dnSSD.AvailableSpace // inactive omitted

		require.Equal(t, totalBytes/util.GB, c.dataNodeStatInfo.TotalGB)
		require.Equal(t, availBytes/util.GB, c.dataNodeStatInfo.AvailGB)
		require.Equal(t, usedBytes/util.GB, c.dataNodeStatInfo.UsedGB)

		newUsedGB := usedBytes / util.GB
		require.Equal(t, int64(newUsedGB)-50, c.dataNodeStatInfo.IncreasedGB)

		usedRate := float64(usedBytes) / float64(totalBytes)
		require.Equal(t, strconv.FormatFloat(usedRate, 'f', 3, 32), c.dataNodeStatInfo.UsedRatio)

		hddKey := proto.MediaTypeString(proto.MediaType_HDD)
		ssdKey := proto.MediaTypeString(proto.MediaType_SSD)
		require.Contains(t, c.dataStatsByMedia, hddKey)
		require.Contains(t, c.dataStatsByMedia, ssdKey)
		require.Greater(t, c.dataStatsByMedia[hddKey].TotalGB, uint64(0))
		require.Greater(t, c.dataStatsByMedia[ssdKey].TotalGB, uint64(0))
	})

	t.Run("high_used_rate_warn_no_panic", func(t *testing.T) {
		c := clusterStatTestCluster(t, "dn-warn")
		dn := clusterStatWritableDataNode("10.0.0.9:17320", "stat-test-zone", proto.MediaType_HDD)
		dn.Total = 100 * util.GB
		dn.Used = 95 * util.GB
		dn.AvailableSpace = dn.Total - dn.Used
		c.dataNodes.Store(dn.Addr, dn)
		require.NotPanics(t, func() { c.updateDataNodeStatInfo() })
		rate, err := strconv.ParseFloat(c.dataNodeStatInfo.UsedRatio, 64)
		require.NoError(t, err)
		require.Greater(t, rate, spaceAvailableRate)
	})
}

func TestUpdateMetaNodeStatInfo(t *testing.T) {
	t.Run("no_meta_nodes_early_return", func(t *testing.T) {
		c := clusterStatTestCluster(t, "mn-empty")
		prev := *c.metaNodeStatInfo
		c.updateMetaNodeStatInfo()
		require.Equal(t, prev, *c.metaNodeStatInfo)
	})

	t.Run("aggregates_and_inactive_skips_avail", func(t *testing.T) {
		require.NotNil(t, gConfig)

		c := clusterStatTestCluster(t, "mn-agg")
		mn1 := clusterStatWritableMetaNode("10.0.1.1:17210", "stat-test-zone")
		mn2 := clusterStatWritableMetaNode("10.0.1.2:17210", "stat-test-zone")
		mn2.IsActive = false
		mn2.MaxMemAvailWeight = 0

		c.metaNodes.Store(mn1.Addr, mn1)
		c.metaNodes.Store(mn2.Addr, mn2)

		c.metaNodeStatInfo.UsedGB = 1
		c.updateMetaNodeStatInfo()

		total := mn1.Total + mn2.Total
		used := mn1.Used + mn2.Used
		require.Equal(t, total/util.GB, c.metaNodeStatInfo.TotalGB)
		require.Equal(t, used/util.GB, c.metaNodeStatInfo.UsedGB)
		require.Equal(t, mn1.MaxMemAvailWeight/util.GB, c.metaNodeStatInfo.AvailGB)

		rate := float64(used) / float64(total)
		require.Equal(t, strconv.FormatFloat(rate, 'f', 3, 32), c.metaNodeStatInfo.UsedRatio)
	})

	t.Run("high_use_rate_warn_no_panic", func(t *testing.T) {
		require.NotNil(t, gConfig)

		c := clusterStatTestCluster(t, "mn-warn")
		mn := clusterStatWritableMetaNode("10.0.1.9:17210", "stat-test-zone")
		mn.Total = 100 * util.GB
		mn.Used = 92 * util.GB
		c.metaNodes.Store(mn.Addr, mn)
		require.NotPanics(t, func() { c.updateMetaNodeStatInfo() })
	})
}

func TestUpdateVolStatInfo(t *testing.T) {
	c := clusterStatTestCluster(t, "vol-stat")

	t.Run("skip_unavailable", func(t *testing.T) {
		v := clusterStatTestVol("unavail", proto.VolStatusInitializing, 10)
		c.volMutex.Lock()
		c.vols["unavail"] = v
		c.volMutex.Unlock()

		c.updateVolStatInfo()
		_, ok := c.volStatInfo.Load("unavail")
		require.False(t, ok)
	})

	t.Run("skip_zero_capacity", func(t *testing.T) {
		v := clusterStatTestVol("zerocap", proto.VolStatusNormal, 0)
		c.volMutex.Lock()
		c.vols["zerocap"] = v
		c.volMutex.Unlock()

		c.updateVolStatInfo()
		_, ok := c.volStatInfo.Load("zerocap")
		require.False(t, ok)
	})

	t.Run("stores_inode_aggregate", func(t *testing.T) {
		v := clusterStatTestVol("goodvol", proto.VolStatusNormal, 100)
		c.volMutex.Lock()
		c.vols["goodvol"] = v
		c.volMutex.Unlock()

		c.updateVolStatInfo()
		raw, ok := c.volStatInfo.Load("goodvol")
		require.True(t, ok)
		vs := raw.(*volStatInfo)
		require.Equal(t, "goodvol", vs.Name)
		require.Equal(t, uint64(100*util.GB), vs.TotalSize)
		require.Equal(t, uint64(123), vs.InodeCount)
		require.NotEmpty(t, vs.UsedRatio)
	})

	t.Run("skip_mark_delete", func(t *testing.T) {
		v := clusterStatTestVol("marked", proto.VolStatusMarkDelete, 20)
		c.volMutex.Lock()
		c.vols["marked"] = v
		c.volMutex.Unlock()

		c.updateVolStatInfo()
		_, ok := c.volStatInfo.Load("marked")
		require.False(t, ok)
	})

	// Cold volumes: totalUsedSpace -> ebsUsedSpace; used sums each MP's max replica dataSize.
	t.Run("cold_volume_used_via_mp_replica_data_size", func(t *testing.T) {
		v := clusterStatTestVol("coldvol", proto.VolStatusNormal, 80)
		v.VolType = proto.VolumeTypeCold
		v.MetaPartitions = map[uint64]*MetaPartition{
			1: {
				PartitionID: 1,
				InodeCount:  50,
				Replicas: []*MetaReplica{
					{Addr: "10.0.0.1:17210", dataSize: 3 * util.GB},
					{Addr: "10.0.0.2:17210", dataSize: 9 * util.GB},
				},
			},
			2: {
				PartitionID: 2,
				InodeCount:  7,
				Replicas: []*MetaReplica{
					{Addr: "10.0.0.3:17210", dataSize: 2 * util.GB},
				},
			},
		}
		c.volMutex.Lock()
		c.vols["coldvol"] = v
		c.volMutex.Unlock()

		c.updateVolStatInfo()
		raw, ok := c.volStatInfo.Load("coldvol")
		require.True(t, ok)
		vs := raw.(*volStatInfo)
		require.Equal(t, uint64(9*util.GB+2*util.GB), vs.UsedSize, "sum of per-MP max replica dataSize")
		require.Equal(t, uint64(57), vs.InodeCount)
		wantRatio := strconv.FormatFloat(float64(vs.UsedSize)/float64(80*util.GB), 'f', 3, 32)
		require.Equal(t, wantRatio, vs.UsedRatio)
	})
}

func TestUpdateZoneStatInfo(t *testing.T) {
	require.NotNil(t, gConfig)

	c := clusterStatTestCluster(t, "zone-stat")
	zone, err := c.t.getZone("stat-test-zone")
	require.NoError(t, err)

	dnW := clusterStatWritableDataNode("10.0.2.1:17320", zone.name, proto.MediaType_HDD)
	dnOff := clusterStatWritableDataNode("10.0.2.2:17320", zone.name, proto.MediaType_HDD)
	dnOff.isActive = false

	mnMem := clusterStatWritableMetaNode("10.0.2.11:17210", zone.name)
	mnRock := clusterStatRocksdbWritableMetaNode("10.0.2.12:17210", zone.name)
	mnInactive := clusterStatWritableMetaNode("10.0.2.13:17210", zone.name)
	mnInactive.IsActive = false

	zone.dataNodes.Store(dnW.Addr, dnW)
	zone.dataNodes.Store(dnOff.Addr, dnOff)
	zone.metaNodes.Store(mnMem.Addr, mnMem)
	zone.metaNodes.Store(mnRock.Addr, mnRock)
	zone.metaNodes.Store(mnInactive.Addr, mnInactive)

	c.updateZoneStatInfo()

	zs, ok := c.zoneStatInfos[zone.name]
	require.True(t, ok)
	require.NotNil(t, zs)

	require.EqualValues(t, 2, zs.DataNodeStat.TotalNodes)
	require.EqualValues(t, 1, zs.DataNodeStat.WritableNodes)
	require.Greater(t, zs.DataNodeStat.Total, 0.0)
	require.GreaterOrEqual(t, zs.DataNodeStat.Avail, 0.0)
	require.Greater(t, zs.DataNodeStat.UsedRatio, 0.0)

	require.EqualValues(t, 3, zs.MetaNodeStat.TotalNodes)
	require.GreaterOrEqual(t, zs.MetaNodeStat.WritableNodes, 1)
	require.GreaterOrEqual(t, zs.MetaNodeStat.RocksdbWritableNodes, 1)
}

func TestUpdateZoneStatInfo_empty_zone_totals_normalized(t *testing.T) {
	c := clusterStatTestCluster(t, "zone-empty")
	emptyZone := newZone("empty-zone", proto.MediaType_SSD)
	require.NoError(t, c.t.putZone(emptyZone))

	c.updateZoneStatInfo()

	zs := c.zoneStatInfos["empty-zone"]
	require.NotNil(t, zs)
	require.EqualValues(t, 1, zs.DataNodeStat.Total)
	require.EqualValues(t, 1, zs.MetaNodeStat.Total)
}

func TestUpdateStatInfo_full_path(t *testing.T) {
	require.NotNil(t, gConfig)

	c := clusterStatTestCluster(t, "full-stat")
	dn := clusterStatWritableDataNode("10.0.3.1:17320", "stat-test-zone", proto.MediaType_HDD)
	c.dataNodes.Store(dn.Addr, dn)

	mn := clusterStatWritableMetaNode("10.0.3.2:17210", "stat-test-zone")
	c.metaNodes.Store(mn.Addr, mn)

	c.volMutex.Lock()
	c.vols["stvol"] = clusterStatTestVol("stvol", proto.VolStatusNormal, 10)
	c.volMutex.Unlock()

	require.NotPanics(t, func() { c.updateStatInfo() })

	require.NotEmpty(t, c.dataNodeStatInfo.UsedRatio)
	require.NotEmpty(t, c.metaNodeStatInfo.UsedRatio)
	_, ok := c.volStatInfo.Load("stvol")
	require.True(t, ok)
	require.Contains(t, c.zoneStatInfos, "stat-test-zone")
}

// TestUpdateStatInfo_recoversOnNilTopology covers updateStatInfo defer recover: earlier steps succeed,
// then updateZoneStatInfo panics on nil topology and the panic is swallowed.
func TestUpdateStatInfo_recoversOnNilTopology(t *testing.T) {
	c := clusterStatTestCluster(t, "panic-stat")
	dn := clusterStatWritableDataNode("10.0.4.1:17320", "stat-test-zone", proto.MediaType_HDD)
	c.dataNodes.Store(dn.Addr, dn)
	c.t = nil

	require.NotPanics(t, func() { c.updateStatInfo() })
}

func TestUpdateStatInfo_idempotent_zone_map(t *testing.T) {
	require.NotNil(t, gConfig)

	c := clusterStatTestCluster(t, "idempotent")
	zone, err := c.t.getZone("stat-test-zone")
	require.NoError(t, err)
	zone.dataNodes.Store("z", clusterStatWritableDataNode("10.0.5.1:17320", zone.name, proto.MediaType_HDD))

	c.updateStatInfo()
	first := c.zoneStatInfos[zone.name]
	require.NotNil(t, first)

	c.updateStatInfo()
	second := c.zoneStatInfos[zone.name]
	require.NotNil(t, second)
	require.Equal(t, first.DataNodeStat.TotalNodes, second.DataNodeStat.TotalNodes)
}
