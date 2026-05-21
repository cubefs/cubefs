package master

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestTryDecommissionDiskCancelPreventsStaleCommit(t *testing.T) {
	cluster := &Cluster{
		partition: &mockPartition{isLeader: true},
	}
	disk := &DecommissionDisk{
		SrcAddr:             "10.0.0.1:17310",
		DiskPath:            "/disk1",
		DecommissionStatus:  markDecommission,
		DecommissionDpTotal: 1,
		DecommissionTerm:    1,
		Type:                ManualDecommission,
	}

	// Simulate TryDecommissionDisk having started and already holding an execution slot.
	execSeq, ok := disk.beginDecommissionAttempt()
	require.True(t, ok)

	err := disk.cancelDecommission(cluster, nil)
	require.NoError(t, err)
	require.EqualValues(t, DecommissionCancel, disk.GetDecommissionStatus())

	staleCommitted := disk.commitDecommissionAttempt(execSeq, func() {
		disk.SetDecommissionStatus(DecommissionRunning)
		disk.IgnoreDecommissionDps = []proto.IgnoreDecommissionDP{{PartitionID: 1}}
	})
	require.False(t, staleCommitted)
	require.EqualValues(t, DecommissionCancel, disk.GetDecommissionStatus())
	require.Empty(t, disk.IgnoreDecommissionDps)

	staleSuccessCommitted := disk.commitDecommissionAttempt(execSeq, func() {
		disk.markDecommissionSuccess()
	})
	require.False(t, staleSuccessCommitted)
	require.EqualValues(t, DecommissionCancel, disk.GetDecommissionStatus())
}

func TestDecommissionDiskMarkDecommissionInitializesState(t *testing.T) {
	disk := &DecommissionDisk{
		SrcAddr:             "10.0.0.1:17310",
		DiskPath:            "/disk1",
		DecommissionStatus:  DecommissionSuccess,
		DecommissionDpTotal: 3,
		DecommissionDpCount: 1,
		DecommissionTimes:   2,
		DstAddr:             "old-target",
	}

	disk.markDecommission("10.0.0.2:17310", true, 2)

	require.EqualValues(t, markDecommission, disk.GetDecommissionStatus())
	require.Equal(t, InvalidDecommissionDpCnt, disk.DecommissionDpTotal)
	require.Equal(t, 2, disk.DecommissionDpCount)
	require.True(t, disk.DecommissionRaftForce)
	require.Equal(t, "10.0.0.2:17310", disk.DstAddr)
	require.EqualValues(t, 0, disk.DecommissionTimes)
	require.NotZero(t, disk.DecommissionTerm)
}

func TestDecommissionDiskMarkDecommissionFromPausePreservesPlan(t *testing.T) {
	disk := &DecommissionDisk{
		SrcAddr:                  "10.0.0.1:17310",
		DiskPath:                 "/disk1",
		DecommissionStatus:       DecommissionPause,
		DecommissionDpTotal:      5,
		DecommissionDpCount:      2,
		DecommissionRaftForce:    true,
		DstAddr:                  "10.0.0.2:17310",
		DecommissionTimes:        3,
		DecommissionCompleteTime: 99,
	}

	disk.markDecommission("10.0.0.3:17310", false, 9)

	require.EqualValues(t, markDecommission, disk.GetDecommissionStatus())
	require.Equal(t, 5, disk.DecommissionDpTotal)
	require.Equal(t, 2, disk.DecommissionDpCount)
	require.True(t, disk.DecommissionRaftForce)
	require.Equal(t, "10.0.0.2:17310", disk.DstAddr)
	require.EqualValues(t, 3, disk.DecommissionTimes)
	require.NotZero(t, disk.DecommissionTerm)
}

func TestDecommissionDiskUpdateStatusNoRemainingPartitions(t *testing.T) {
	cluster := &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
		partition: &mockPartition{isLeader: true},
	}
	disk := &DecommissionDisk{
		SrcAddr:             "10.0.0.1:17310",
		DiskPath:            "/disk1",
		DecommissionStatus:  DecommissionRunning,
		DecommissionDpTotal: 2,
		DecommissionTerm:    1,
	}

	status, progress := disk.updateDecommissionStatus(cluster, false, false)

	require.EqualValues(t, DecommissionSuccess, status)
	require.Equal(t, float64(1), progress)
	require.EqualValues(t, DecommissionRunning, disk.GetDecommissionStatus())
}

func TestDecommissionDiskUpdateStatusCalculatesPartialProgress(t *testing.T) {
	const (
		nodeAddr = "10.0.0.1:17310"
		diskPath = "/disk1"
		term     = uint64(7)
	)
	cluster := newClusterWithDecommissionPartitions("vol-disk-progress",
		&DataPartition{
			PartitionID:             1,
			DecommissionStatus:      DecommissionRunning,
			DecommissionSrcAddr:     nodeAddr,
			DecommissionSrcDiskPath: diskPath,
			DecommissionTerm:        term,
		},
		&DataPartition{
			PartitionID:             2,
			DecommissionStatus:      DecommissionPrepare,
			DecommissionSrcAddr:     nodeAddr,
			DecommissionSrcDiskPath: diskPath,
			DecommissionTerm:        term,
		},
	)
	disk := &DecommissionDisk{
		SrcAddr:             nodeAddr,
		DiskPath:            diskPath,
		DecommissionStatus:  DecommissionRunning,
		DecommissionDpTotal: 4,
		DecommissionTerm:    term,
		IgnoreDecommissionDps: []proto.IgnoreDecommissionDP{
			{PartitionID: 3, ErrMsg: proto.ErrDecommissionDiskErrDPFirst.Error()},
		},
	}

	status, progress := disk.updateDecommissionStatus(cluster, false, false)

	require.EqualValues(t, DecommissionRunning, status)
	require.Equal(t, 0.25, progress)
	require.EqualValues(t, DecommissionRunning, disk.GetDecommissionStatus())
}

func TestDataNodeUpdateDecommissionStatusAggregatesDiskProgress(t *testing.T) {
	const (
		nodeAddr = "10.0.0.1:17310"
		diskPath = "/disk1"
		term     = uint64(8)
	)
	cluster := newClusterWithDecommissionPartitions("vol-node-progress",
		&DataPartition{
			PartitionID:             10,
			DecommissionStatus:      DecommissionRunning,
			DecommissionSrcAddr:     nodeAddr,
			DecommissionSrcDiskPath: diskPath,
			DecommissionTerm:        term,
		},
		&DataPartition{
			PartitionID:             11,
			DecommissionStatus:      DecommissionRunning,
			DecommissionSrcAddr:     nodeAddr,
			DecommissionSrcDiskPath: diskPath,
			DecommissionTerm:        term,
		},
	)
	disk := &DecommissionDisk{
		SrcAddr:             nodeAddr,
		DiskPath:            diskPath,
		DecommissionStatus:  DecommissionRunning,
		DecommissionDpTotal: 4,
		DecommissionTerm:    term,
	}
	cluster.DecommissionDisks.Store(disk.GenerateKey(), disk)
	dataNode := &DataNode{
		Addr:                 nodeAddr,
		DecommissionDiskList: []string{diskPath, "/already-success"},
	}
	dataNode.SetDecommissionStatus(DecommissionRunning)

	status, progress := dataNode.updateDecommissionStatus(cluster, false, false)

	require.EqualValues(t, DecommissionRunning, status)
	require.Equal(t, 0.75, progress)
	require.EqualValues(t, DecommissionRunning, dataNode.GetDecommissionStatus())
}

func TestTryDecommissionDataNodeNoPartitionsMarksSuccess(t *testing.T) {
	cluster := &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
		partition: &mockPartition{isLeader: true},
	}
	dataNode := &DataNode{Addr: "10.0.0.1:17310"}
	dataNode.SetDecommissionStatus(markDecommission)

	cluster.TryDecommissionDataNode(dataNode)

	require.EqualValues(t, DecommissionSuccess, dataNode.GetDecommissionStatus())
	require.NotZero(t, dataNode.DecommissionCompleteTime)
	require.False(t, dataNode.ToBeOffline)
	require.False(t, dataNode.RdOnly)
}

func TestTryDecommissionDataNodeRestoresPausedDiskList(t *testing.T) {
	const (
		nodeAddr = "10.0.0.1:17310"
		diskPath = "/disk1"
	)
	cluster := &Cluster{
		partition: &mockPartition{isLeader: true},
	}
	disk := &DecommissionDisk{
		SrcAddr:            nodeAddr,
		DiskPath:           diskPath,
		DecommissionStatus: DecommissionPause,
	}
	cluster.DecommissionDisks.Store(disk.GenerateKey(), disk)
	dataNode := &DataNode{
		Addr:                 nodeAddr,
		DecommissionDiskList: []string{diskPath},
		DecommissionLimit:    0,
	}
	dataNode.SetDecommissionStatus(DecommissionPause)

	cluster.TryDecommissionDataNode(dataNode)

	require.EqualValues(t, markDecommission, disk.GetDecommissionStatus())
	require.EqualValues(t, DecommissionRunning, dataNode.GetDecommissionStatus())
	require.True(t, dataNode.ToBeOffline)
	require.True(t, dataNode.RdOnly)
}

func newClusterWithDecommissionPartitions(volName string, partitions ...*DataPartition) *Cluster {
	vol := &Vol{Name: volName, dataPartitions: newDataPartitionMap(volName)}
	for _, dp := range partitions {
		dp.VolName = volName
		vol.dataPartitions.put(dp)
	}
	return &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{volName: vol},
		},
		partition: &mockPartition{isLeader: true},
	}
}
