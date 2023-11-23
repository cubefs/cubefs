package datanode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStats(t *testing.T) {
	zone := "zoneA"
	s := NewStats(zone)
	require.Equal(t, zone, s.Zone, "NewStats() error")

	// Test AddConnection
	s.AddConnection()
	require.Equal(t, int64(1), s.GetConnectionCount(), "AddConnection() error")

	// Test RemoveConnection
	s.RemoveConnection()
	require.Equal(t, int64(0), s.GetConnectionCount(), "RemoveConnection() error")

	// Test updateMetrics
	total := uint64(100)
	used := uint64(50)
	available := total - used
	createdPartitionWeights := uint64(30)
	remainWeightsForCreatePartition := uint64(20)
	maxWeightsForCreatePartition := uint64(10)
	dataPartitionCnt := uint64(5)
	s.updateMetrics(total, used, available, createdPartitionWeights, remainWeightsForCreatePartition, maxWeightsForCreatePartition, dataPartitionCnt)
	require.Equal(t, total, s.Total, "updateMetrics() error")
	require.Equal(t, used, s.Used, "updateMetrics() error")
	require.Equal(t, available, s.Available, "updateMetrics() error")
	require.Equal(t, createdPartitionWeights, s.TotalPartitionSize, "updateMetrics() error")
	require.Equal(t, remainWeightsForCreatePartition, s.RemainingCapacityToCreatePartition, "updateMetrics() error")
	require.Equal(t, maxWeightsForCreatePartition, s.MaxCapacityToCreatePartition, "updateMetrics() error")
	require.Equal(t, dataPartitionCnt, s.CreatedPartitionCnt, "updateMetrics() error")

	// Test updateMetricLackPartitionsInMem
	lackPartitionsInMem := uint64(3)
	s.updateMetricLackPartitionsInMem(lackPartitionsInMem)
	require.Equal(t, lackPartitionsInMem, s.LackPartitionsInMem, "updateMetricLackPartitionsInMem() error")

	// Test updateMetricLackPartitionsInDisk
	lackPartitionsInDisk := uint64(2)
	s.updateMetricLackPartitionsInDisk(lackPartitionsInDisk)
	require.Equal(t, lackPartitionsInDisk, s.LackPartitionsInDisk, "updateMetricLackPartitionsInDisk() error")
}
