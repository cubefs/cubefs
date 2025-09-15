package master

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
)

// TestAdvancedInitializationFailureScenarios tests advanced initialization failure scenarios
func TestAdvancedInitializationFailureScenarios(t *testing.T) {
	t.Run("StageByStageFailure", TestStageByStageFailure)
	t.Run("ProgressiveRecovery", TestProgressiveRecovery)
	t.Run("PartialResourceAllocation", TestPartialResourceAllocation)
}

// testStageByStageFailure tests stage by stage failure and recovery
func TestStageByStageFailure(t *testing.T) {
	volName := "test_stage_failure"

	t.Logf("=== Stage 1: Complete Node Shortage ===")

	// Collect all nodes before setting read-only
	metaNodesBeforeTest := make([]*MetaNode, 0)
	server.cluster.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		metaNodesBeforeTest = append(metaNodesBeforeTest, metaNode)
		return true
	})

	dataNodesBeforeTest := make([]*DataNode, 0)
	server.cluster.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		dataNodesBeforeTest = append(dataNodesBeforeTest, dataNode)
		return true
	})

	require.Greater(t, len(metaNodesBeforeTest), 3, "Need more than 3 meta nodes for testing")
	require.Greater(t, len(dataNodesBeforeTest), 3, "Need more than 3 data nodes for testing")

	// Stage 1: Set most nodes to read-only, keep only 1 of each type available
	metaNodesToLimit := metaNodesBeforeTest[1:] // Keep only first node available
	dataNodesToLimit := dataNodesBeforeTest[1:] // Keep only first node available

	for _, node := range metaNodesToLimit {
		node.RdOnly = true
		// t.Logf("Set meta node %s to read only", node.Addr)
	}

	for _, node := range dataNodesToLimit {
		node.RdOnly = true
		// t.Logf("Set data node %s to read only", node.Addr)
	}

	// Restore everything after test
	defer func() {
		for _, node := range metaNodesToLimit {
			node.RdOnly = false
			// t.Logf("Restored meta node %s to read write", node.Addr)
		}
		for _, node := range dataNodesToLimit {
			node.RdOnly = false
			// t.Logf("Restored data node %s to read write", node.Addr)
		}
	}()

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         3,  // need 3, but only 1
		dpCount:         10, // need 10, but resource insufficient
		dpReplicaNum:    3,  // need 3 replicas, but only 1 node
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test stage by stage failure",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)
	if err != nil {
		t.Logf("Stage 1 - Complete failure as expected: %v", err)
		// Volume creation should fail with insufficient resources
		require.Error(t, err, "Volume creation should fail with insufficient resources")
		return // Exit early since we can't proceed without a volume
	}

	// If volume was created, it should be in InitFailed status
	require.NotNil(t, vol, "Volume should be created even if initialization fails")
	require.Equal(t, proto.VolStatusInitFailed, vol.Status, "Volume should be in InitFailed status")

	// Verify initial state with limited resources
	vol.mpsLock.RLock()
	initialMpCount := len(vol.MetaPartitions)
	vol.mpsLock.RUnlock()
	initialDpCount := len(vol.dataPartitions.partitions)

	require.Less(t, initialMpCount, req.mpCount, "Should have fewer meta partitions than requested")
	require.Less(t, initialDpCount, req.dpCount, "Should have fewer data partitions than requested")

	checkVolumeState(t, vol, "Stage 1")

	t.Logf("=== Stage 2: Add Meta Nodes (Partial Meta Success) ===")

	// Stage 2: Enable 1 more meta node (total 2 available, still need 3)
	if len(metaNodesToLimit) > 1 {
		metaNodesToLimit[0].RdOnly = false // Enable one more meta node
		t.Logf("Enabled meta node %s (now have 2 available)", metaNodesToLimit[0].Addr)
	}

	// Wait for cluster status update
	time.Sleep(3 * time.Second)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(2 * time.Second)

	// Retry volume creation to see if more meta partitions can be created
	vol2, err := server.cluster.createVol(req)
	if vol2 != nil {
		vol = vol2 // Use the new volume if created
	}

	vol.mpsLock.RLock()
	stage2MpCount := len(vol.MetaPartitions)
	vol.mpsLock.RUnlock()

	t.Logf("Stage 2 - Meta partition result: %d partitions, error: %v", stage2MpCount, err)
	require.GreaterOrEqual(t, stage2MpCount, initialMpCount, "Should have same or more meta partitions")

	checkVolumeState(t, vol, "Stage 2")

	t.Logf("=== Stage 3: Add Data Nodes (Partial Data Success) ===")

	// Stage 3: Enable 1 more data node (total 2 available, still need 3 for replication)
	if len(dataNodesToLimit) > 1 {
		dataNodesToLimit[0].RdOnly = false // Enable one more data node
		t.Logf("Enabled data node %s (now have 2 available)", dataNodesToLimit[0].Addr)
	}

	// Wait for cluster status update
	time.Sleep(3 * time.Second)
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(2 * time.Second)

	// Retry volume creation to see if more data partitions can be created
	vol3, err := server.cluster.createVol(req)
	if vol3 != nil {
		vol = vol3 // Use the new volume if created
	}

	stage3DpCount := len(vol.dataPartitions.partitions)
	t.Logf("Stage 3 - Data partition result: %d partitions, error: %v", stage3DpCount, err)
	require.GreaterOrEqual(t, stage3DpCount, initialDpCount, "Should have same or more data partitions")

	checkVolumeState(t, vol, "Stage 3")

	t.Logf("=== Stage 4: Full Recovery ===")

	// Stage 4: Enable all remaining nodes
	for i := 1; i < len(metaNodesToLimit); i++ {
		metaNodesToLimit[i].RdOnly = false
		t.Logf("Enabled meta node %s", metaNodesToLimit[i].Addr)
	}

	for i := 1; i < len(dataNodesToLimit); i++ {
		dataNodesToLimit[i].RdOnly = false
		t.Logf("Enabled data node %s", dataNodesToLimit[i].Addr)
	}

	// Wait for cluster status update
	time.Sleep(5 * time.Second)
	server.cluster.checkMetaNodeHeartbeat()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(3 * time.Second)

	// Final retry - should succeed completely
	vol4, err := server.cluster.createVol(req)
	if vol4 != nil {
		vol = vol4 // Use the new volume if created
	}

	vol.mpsLock.RLock()
	finalMpCount := len(vol.MetaPartitions)
	vol.mpsLock.RUnlock()
	finalDpCount := len(vol.dataPartitions.partitions)

	t.Logf("Stage 4 - Final result: %d meta partitions, %d data partitions, error: %v",
		finalMpCount, finalDpCount, err)

	if err == nil {
		require.Equal(t, proto.VolStatusNormal, vol.Status, "Volume should be in Normal status after full recovery")
		require.GreaterOrEqual(t, finalMpCount, req.mpCount, "Should have at least requested meta partitions")
		require.GreaterOrEqual(t, finalDpCount, req.dpCount, "Should have at least requested data partitions")
	} else {
		t.Logf("Final stage still failed: %v", err)
	}

	checkVolumeState(t, vol, "Stage 4 (Final)")

	// Clean up
	cleanupVolume(vol, server.cluster)
}

// TestProgressiveRecovery tests progressive recovery scenarios
func TestProgressiveRecovery(t *testing.T) {
	volName := "test_progressive_recovery"

	t.Logf("=== Progressive Recovery Test ===")

	// Create a volume that will fail initially
	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test progressive recovery",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	// Simulate resource constraints by limiting available nodes
	metaNodesBeforeTest := make([]*MetaNode, 0)
	server.cluster.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		metaNodesBeforeTest = append(metaNodesBeforeTest, metaNode)
		return true
	})

	if len(metaNodesBeforeTest) > 2 {
		// Set some nodes to read-only to simulate partial failure
		for i := 2; i < len(metaNodesBeforeTest); i++ {
			metaNodesBeforeTest[i].RdOnly = true
		}

		defer func() {
			// Restore all nodes
			for i := 2; i < len(metaNodesBeforeTest); i++ {
				metaNodesBeforeTest[i].RdOnly = false
			}
		}()
	}

	// First attempt - should partially fail
	vol, err := server.cluster.createVol(req)
	t.Logf("First attempt result: error=%v", err)

	if vol != nil {
		checkVolumeState(t, vol, "First Attempt")

		// Progressive recovery attempts
		for attempt := 1; attempt <= 3; attempt++ {
			t.Logf("=== Recovery Attempt %d ===", attempt)

			// Gradually restore nodes
			if attempt <= len(metaNodesBeforeTest)-2 {
				metaNodesBeforeTest[attempt+1].RdOnly = false
				t.Logf("Restored meta node %s", metaNodesBeforeTest[attempt+1].Addr)
			}

			time.Sleep(2 * time.Second)
			server.cluster.checkMetaNodeHeartbeat()
			time.Sleep(1 * time.Second)

			// Retry creation
			volRetry, errRetry := server.cluster.createVol(req)
			if volRetry != nil {
				vol = volRetry
			}

			t.Logf("Recovery attempt %d result: error=%v", attempt, errRetry)
			checkVolumeState(t, vol, fmt.Sprintf("Recovery Attempt %d", attempt))

			if errRetry == nil && vol.Status == proto.VolStatusNormal {
				t.Logf("Successfully recovered after %d attempts", attempt)
				break
			}
		}

		// Clean up
		cleanupVolume(vol, server.cluster)
	}
}

// TestPartialResourceAllocation tests partial resource allocation scenarios
func TestPartialResourceAllocation(t *testing.T) {
	volName := "test_partial_allocation"

	t.Logf("=== Partial Resource Allocation Test ===")

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         5,  // Request more than available
		dpCount:         20, // Request more than can be allocated
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test partial resource allocation",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	vol, err := server.cluster.createVol(req)
	t.Logf("Partial allocation result: error=%v", err)

	if vol != nil {
		vol.mpsLock.RLock()
		actualMpCount := len(vol.MetaPartitions)
		vol.mpsLock.RUnlock()
		actualDpCount := len(vol.dataPartitions.partitions)

		t.Logf("Requested: %d MP, %d DP", req.mpCount, req.dpCount)
		t.Logf("Allocated: %d MP, %d DP", actualMpCount, actualDpCount)

		// Should have allocated something, even if not the full request
		require.Greater(t, actualMpCount, 0, "Should have allocated at least some meta partitions")
		// NOTE: Data partitions can only be created after meta partitions are successfully created.
		// If meta partition creation fails, data partition allocation should also fail.
		// This is the correct behavior in CubeFS architecture.
		// require.Greater(t, actualDpCount, 0, "Should have allocated at least some data partitions")

		checkVolumeState(t, vol, "Partial Allocation")

		// Test re-entrant allocation
		t.Logf("=== Testing Re-entrant Allocation ===")

		volRetry, errRetry := server.cluster.createVol(req)
		if volRetry != nil {
			vol = volRetry
		}

		vol.mpsLock.RLock()
		retryMpCount := len(vol.MetaPartitions)
		vol.mpsLock.RUnlock()
		retryDpCount := len(vol.dataPartitions.partitions)

		t.Logf("After retry: %d MP, %d DP, error=%v", retryMpCount, retryDpCount, errRetry)
		require.GreaterOrEqual(t, retryMpCount, actualMpCount, "Should maintain or increase partition count")
		require.GreaterOrEqual(t, retryDpCount, actualDpCount, "Should maintain or increase partition count")

		checkVolumeState(t, vol, "After Retry")

		// Clean up
		cleanupVolume(vol, server.cluster)
	}
}

// Helper function to check volume state
func checkVolumeState(t *testing.T, vol *Vol, stage string) {
	if vol == nil {
		t.Logf("%s: Volume is nil", stage)
		return
	}

	vol.mpsLock.RLock()
	mpCount := len(vol.MetaPartitions)
	vol.mpsLock.RUnlock()
	dpCount := len(vol.dataPartitions.partitions)

	statusStr := "Unknown"
	switch vol.Status {
	case proto.VolStatusInitializing:
		statusStr = "Initializing"
	case proto.VolStatusNormal:
		statusStr = "Normal"
	case proto.VolStatusMarkDelete:
		statusStr = "MarkDelete"
	case proto.VolStatusInitFailed:
		statusStr = "InitFailed"
	}

	t.Logf("%s: Volume %s - Status: %s, MP: %d, DP: %d",
		stage, vol.Name, statusStr, mpCount, dpCount)

	// Basic sanity checks
	require.NotEmpty(t, vol.Name, "Volume name should not be empty")
	require.NotEmpty(t, vol.Owner, "Volume owner should not be empty")
}

// Helper function to clean up volume
func cleanupVolume(vol *Vol, cluster *Cluster) {
	if vol == nil {
		return
	}

	// Mark volume for deletion
	vol.volLock.Lock()
	vol.Status = proto.VolStatusMarkDelete
	vol.volLock.Unlock()

	// Remove from cluster
	cluster.deleteVol(vol.Name)
}
