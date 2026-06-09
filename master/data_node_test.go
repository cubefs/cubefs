package master

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/require"
)

func TestDataNode(t *testing.T) {
	// /dataNode/add and /dataNode/response processed by mock data server
	var err error
	addr := "127.0.0.1:9096"
	func() {
		mockServerLock.Lock()
		defer mockServerLock.Unlock()
		mockDataServers = append(mockDataServers, addDataServer(addr, "test-add-zone", defaultMediaType))
	}()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getDataNodeInfo(addr, t)
	updateDisks(addr, t)
	decommissionDataNode(addr, t)
	for i := 0; i < 10; i++ { // decommission is async process
		_, err = server.cluster.dataNode(addr)
		if err == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		t.Errorf("decommission datanode [%v] failed", addr)
	}
	server.cluster.dataNodes.Delete(addr)
}

func getDataNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetDataNode, addr)
	process(reqURL, t)
}

func decommissionDataNode(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionDataNode, addr)
	process(reqURL, t)
}

func updateDisks(addr string, t *testing.T) {
	dn, err := server.cluster.dataNode(addr)
	require.NoError(t, err)

	dn.AllDisks = []string{"/data1"}
	allDisk := []string{"/data1", "/data2", "/data3"}
	badDisk := []string{"/data1"}
	updated, _ := dn.updateDisks(allDisk, badDisk)
	require.Equal(t, updated, true)
	require.Equal(t, allDisk, dn.AllDisks)
	require.Equal(t, badDisk, dn.BadDisks)
}

func TestDataNodeIsWriteAbleWithSizeNoLock(t *testing.T) {
	const (
		reqSize   = 10 * util.GB
		threshold = 1.0
		nodeAddr  = "10.52.134.101:17310"
	)

	tests := []struct {
		name string
		dn   *DataNode
		want bool
	}{
		{
			name: "strict underflow available less than preReserved",
			dn: &DataNode{
				Addr:             nodeAddr,
				isActive:         true,
				Total:            13 * util.TB,
				Used:             8 * util.TB,
				AvailableSpace:   2 * util.MB,
				PreReservedSpace: 6 * util.GB,
			},
			want: false,
		},
		{
			name: "equal available and preReserved both zero",
			dn: &DataNode{
				Addr:             nodeAddr,
				isActive:         true,
				Total:            100 * util.GB,
				Used:             100 * util.GB,
				AvailableSpace:   0,
				PreReservedSpace: 0,
			},
			want: false,
		},
		{
			name: "equal available and preReserved non-zero net zero",
			dn: &DataNode{
				Addr:             nodeAddr,
				isActive:         true,
				Total:            100 * util.GB,
				Used:             50 * util.GB,
				AvailableSpace:   6 * util.GB,
				PreReservedSpace: 6 * util.GB,
			},
			want: false,
		},
		{
			name: "remaining space after preReserved insufficient",
			dn: &DataNode{
				Addr:             nodeAddr,
				isActive:         true,
				Total:            100 * util.GB,
				Used:             95 * util.GB,
				AvailableSpace:   20 * util.GB,
				PreReservedSpace: 6 * util.GB,
			},
			want: false,
		},
		{
			name: "writable when available exceeds preReserved and size",
			dn: &DataNode{
				Addr:             nodeAddr,
				isActive:         true,
				Total:            100 * util.TB,
				Used:             10 * util.TB,
				AvailableSpace:   50 * util.TB,
				PreReservedSpace: 6 * util.GB,
			},
			want: true,
		},
		{
			name: "inactive node",
			dn: &DataNode{
				Addr:           nodeAddr,
				isActive:       false,
				AvailableSpace: 50 * util.TB,
			},
			want: false,
		},
		{
			name: "readonly node",
			dn: &DataNode{
				Addr:           nodeAddr,
				isActive:       true,
				RdOnly:         true,
				AvailableSpace: 50 * util.TB,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.dn.isWriteAbleWithSizeNoLock(reqSize, threshold))
		})
	}
}

func TestDataNodeIsWriteAbleWithSizeNoLockWarnOnlyOnStrictUnderflow(t *testing.T) {
	tmpDir := t.TempDir()
	const module = "dn_writable_warn"
	l, err := log.InitLog(tmpDir, module, log.WarnLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
	require.NoError(t, err)
	defer l.Close()

	const (
		reqSize   = 10 * util.GB
		threshold = 1.0
		nodeAddr  = "10.52.134.247:17310"
	)
	warnLogPath := func() string {
		log.LogFlush()
		return filepath.Join(tmpDir, module, module+log.WarnLogFileName)
	}
	readWarnLog := func() string {
		data, readErr := os.ReadFile(warnLogPath())
		require.NoError(t, readErr)
		return string(data)
	}

	t.Run("equal available and preReserved does not warn", func(t *testing.T) {
		dn := &DataNode{
			Addr:             nodeAddr,
			isActive:         true,
			Total:            100 * util.GB,
			Used:             100 * util.GB,
			AvailableSpace:   0,
			PreReservedSpace: 0,
		}
		require.False(t, dn.isWriteAbleWithSizeNoLock(reqSize, threshold))
		require.NotContains(t, readWarnLog(), "reject node")
	})

	t.Run("strict underflow warns", func(t *testing.T) {
		dn := &DataNode{
			Addr:             nodeAddr,
			isActive:         true,
			Total:            13 * util.TB,
			Used:             8 * util.TB,
			AvailableSpace:   2 * util.MB,
			PreReservedSpace: 6 * util.GB,
		}
		require.False(t, dn.isWriteAbleWithSizeNoLock(reqSize, threshold))
		require.Contains(t, readWarnLog(), "reject node")
		require.Contains(t, readWarnLog(), "available(2097152) < preReserved(6442450944)")
	})
}

func TestDataNodePartitionCntLimitedEx(t *testing.T) {
	const limit = uint64(100)

	t.Run("online under limit", func(t *testing.T) {
		dn := &DataNode{
			DataPartitionCount: 10,
			DpCntLimit:         limit,
			AllDisks:           []string{"/data1"},
		}
		require.True(t, dn.PartitionCntLimitedEx(1))
		require.True(t, dn.PartitionCntLimited())
	})

	t.Run("online over limit", func(t *testing.T) {
		dn := &DataNode{
			DataPartitionCount: 101,
			DpCntLimit:         limit,
			AllDisks:           []string{"/data1"},
		}
		require.False(t, dn.PartitionCntLimitedEx(1))
	})

	t.Run("ToBeOffline under limit", func(t *testing.T) {
		dn := &DataNode{
			DataPartitionCount: 10,
			DpCntLimit:         limit,
			ToBeOffline:        true,
			AllDisks:           []string{"/data1"},
		}
		require.False(t, dn.PartitionCntLimitedEx(1))
		require.True(t, dn.PartitionCntLimited())
	})

	t.Run("all disks decommissioned", func(t *testing.T) {
		dn := &DataNode{
			DataPartitionCount: 10,
			DpCntLimit:         limit,
			AllDisks:           []string{"/data1", "/data2"},
		}
		dn.DecommissionedDisks.Store("/data1", struct{}{})
		dn.DecommissionedDisks.Store("/data2", struct{}{})
		require.True(t, dn.IsOffline())
		require.False(t, dn.PartitionCntLimitedEx(1))
	})
}

func TestCalculateDpLimitByDiskCapacity(t *testing.T) {
	t.Run("SSD", func(t *testing.T) {
		cfg := newClusterConfig()
		cfg.DpLimitSsdBaseCount = 150
		cfg.DpLimitSsdFactor = 50 // 5.0 in tenths
		cluster := &Cluster{cfg: cfg}

		dn := &DataNode{
			AllDisks:  []string{"/data1", "/data2"},
			Total:     4096 * util.GB, // 4TB in bytes
			MediaType: proto.MediaType_SSD,
		}

		got := dn.calculateDpLimitByDiskCapacity(cluster)
		// expected = base*diskCount + (totalGB*factor)/(120*10) where factor is tenths
		want := uint64(150*2 + (4096*50)/(120*10))
		require.Equal(t, want, got)
	})

	t.Run("HDD", func(t *testing.T) {
		cfg := newClusterConfig()
		cfg.DpLimitHddBaseCount = 100
		cfg.DpLimitHddFactor = 20 // 2.0 in tenths
		cluster := &Cluster{cfg: cfg}

		dn := &DataNode{
			AllDisks:  []string{"/data1"},
			Total:     14336 * util.GB, // 14TB in bytes
			MediaType: proto.MediaType_HDD,
		}

		got := dn.calculateDpLimitByDiskCapacity(cluster)
		// expected = base*diskCount + (totalGB*factor)/(120*10)
		want := uint64(100*1 + (14336*20)/(120*10))
		require.Equal(t, want, got)
	})
}
