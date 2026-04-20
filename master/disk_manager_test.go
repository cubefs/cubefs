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
