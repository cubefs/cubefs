package master

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
)

// TestCreateVolAdvanced tests advanced volume creation scenarios
// TestCreateVol_RemoteCacheOnlyForNotSSDInvariant documents that new volumes keep remoteCacheOnlyForNotSSD true
// after the CLI/API flag removal (commit refactor defaulting this field to true).
func TestCreateVol_RemoteCacheOnlyForNotSSDInvariant(t *testing.T) {
	req := createDefaultReq("test_rc_only_not_ssd_invariant", "cfs_test_user")
	require.True(t, req.remoteCacheOnlyForNotSSD)
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)
	vol, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol)
	require.True(t, vol.remoteCacheOnlyForNotSSD)
	cleanUpVol(vol)
}

func TestCreateVolAdvanced(t *testing.T) {
	t.Run("SameNameCreate", testSameNameCreate)
	t.Run("InitFailedCleanup", testInitFailedCleanup)
	t.Run("ConcurrentCreation", testConcurrentVolumeCreation)
	t.Run("ConcurrentSameName", testConcurrentSameNameCreation)
	t.Run("ResourceShortageRetry", testResourceShortageRetry)
}

// testSameNameCreate tests creating volume with same name
func testSameNameCreate(t *testing.T) {
	req := createDefaultReq("test_same_name_create", "cfs_test_user")
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// Create the first volume
	vol1, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol1)

	// Try to create the same volume again
	vol2, err := server.cluster.createVol(req)

	// Should fail with duplicate error
	require.Error(t, err)
	require.Nil(t, vol2)
	require.Contains(t, err.Error(), "duplicate vol", "Should fail with duplicate vol error")
	t.Logf("Volume creation correctly failed with duplicate error: %v", err)

	req.mpCount = 5
	vol3, err := server.cluster.createVol(req)
	require.Error(t, err)
	require.Nil(t, vol3)
	require.Contains(t, err.Error(), "duplicate vol", "Should fail with duplicate vol error")
	t.Logf("Volume creation correctly failed with duplicate error: %v", err)

	// Clean up
	cleanUpVol(vol1)
}

// testInitFailedCleanup tests cleanup of InitFailed volumes
func testInitFailedCleanup(t *testing.T) {
	req := createDefaultReq("test_init_failed_cleanup", "cfs_test_user")
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// Create volume that might fail
	vol, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol)

	vol.Status = proto.VolStatusInitFailed
	server.cluster.syncUpdateVol(vol)

	vol.createTime = time.Now().Unix() - 30*60 // 30 minutes ago
	vol.checkInitFailed(server.cluster)

	require.True(t, vol.Status == proto.VolStatusMarkDelete || vol == nil)
	if vol != nil {
		cleanUpVol(vol)
	}
}

// testConcurrentVolumeCreation tests concurrent creation of different volumes
func testConcurrentVolumeCreation(t *testing.T) {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	var volumes []*Vol

	numVolumes := 5

	for i := 0; i < numVolumes; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req := createDefaultReq(fmt.Sprintf("test_concurrent_vol_%d", id), "cfs_test_user")
			vol, _ := server.cluster.createVol(req)

			mutex.Lock()
			volumes = append(volumes, vol)
			mutex.Unlock()
		}(i)
	}

	wg.Wait()

	successCount := 0
	for _, vol := range volumes {
		if vol != nil {
			successCount++
		}
	}

	t.Logf("Concurrent creation results: %d/%d succeeded", successCount, numVolumes)
	require.Equal(t, numVolumes, successCount, "All volumes should be created successfully")

	// Clean up
	for _, vol := range volumes {
		cleanUpVol(vol)
	}
}

// testConcurrentSameNameCreation tests concurrent creation of same-name volumes
func testConcurrentSameNameCreation(t *testing.T) {
	volName := "test_concurrent_same_name"
	var wg sync.WaitGroup
	var mutex sync.Mutex
	var volumes []*Vol

	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req := createDefaultReq(volName, "cfs_test_user")
			vol, _ := server.cluster.createVol(req)

			mutex.Lock()
			volumes = append(volumes, vol)
			mutex.Unlock()
		}(i)
	}

	wg.Wait()

	successCount := 0
	for _, vol := range volumes {
		if vol != nil {
			successCount++
		}
	}

	t.Logf("Concurrent same-name creation results: %d/%d succeeded", successCount, numGoroutines)

	// At least one should succeed
	require.Greater(t, successCount, 0, "At least one volume creation should succeed")

	// All successful creations should return the same volume
	var firstVol *Vol
	for _, vol := range volumes {
		if vol != nil {
			if firstVol == nil {
				firstVol = vol
			} else {
				require.Equal(t, firstVol.ID, vol.ID, "All volumes should have the same ID")
				require.Equal(t, firstVol.Name, vol.Name, "All volumes should have the same name")
			}
		}
	}
	require.NotNil(t, firstVol, "Should have at least one volume")
	t.Logf("All %d successful creations returned the same volume (ID: %d)", len(volumes), firstVol.ID)

	// Clean up
	for _, vol := range volumes {
		cleanUpVol(vol)
	}
}

// testResourceShortageRetry tests retry mechanism under resource shortage
func testResourceShortageRetry(t *testing.T) {
	req := createDefaultReq("test_resource_shortage_retry", "cfs_test_user")
	req.mpCount = 10
	req.dpCount = 20

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// First attempt
	vol1, err1 := server.cluster.createVol(req)
	t.Logf("First attempt: error=%v", err1)

	require.NoError(t, err1)
	require.NotNil(t, vol1)
	t.Logf("First attempt created volume with status: %d", vol1.Status)

	// If it failed, try again
	if vol1.Status == proto.VolStatusInitFailed {
		t.Logf("Volume failed, retrying...")

		// Wait a bit for resources to potentially become available
		time.Sleep(2 * time.Second)

		// Retry
		vol2, err2 := server.cluster.createVol(req)
		t.Logf("Retry attempt: error=%v", err2)

		if vol2 != nil {
			t.Logf("Retry created volume with status: %d", vol2.Status)
			vol1 = vol2 // Use the retry result
		}
	}

	// Check final state
	vol1.mpsLock.RLock()
	mpCount := len(vol1.MetaPartitions)
	vol1.mpsLock.RUnlock()
	dpCount := len(vol1.dataPartitions.partitions)

	t.Logf("Final state: %d meta partitions, %d data partitions", mpCount, dpCount)

	// Should have created at least some partitions
	require.Greater(t, mpCount, 0, "Should have at least some meta partitions")
	// NOTE: Data partitions are only created after meta partitions succeed
	if mpCount > 0 {
		require.Greater(t, dpCount, 0, "Should have at least some data partitions when meta partitions exist")
	}

	// Clean up
	cleanUpVol(vol1)
}

// TestVolumeCreationEdgeCases tests edge cases in volume creation
func TestVolumeCreationEdgeCases(t *testing.T) {
	t.Run("DifferentOwnerSameName", testDifferentOwnerSameName)
	t.Run("InitializingStatusHandling", testInitializingStatusHandling)
}

// testDifferentOwnerSameName tests creating volumes with same name but different owners
func testDifferentOwnerSameName(t *testing.T) {
	volName := "test_different_owner"

	// Create volume with first owner
	req1 := createDefaultReq(volName, "owner1")
	err := server.checkCreateVolReq(req1)
	require.NoError(t, err)

	vol1, err1 := server.cluster.createVol(req1)
	require.NoError(t, err1)
	require.NotNil(t, vol1)

	// Try to create volume with same name but different owner
	req2 := createDefaultReq(volName, "owner2")
	err = server.checkCreateVolReq(req2)
	require.NoError(t, err)

	vol2, err2 := server.cluster.createVol(req2)

	// Should fail with duplicate volume error
	require.Error(t, err2)
	require.Nil(t, vol2)
	t.Logf("Different owner creation failed as expected: %v", err2)

	// Clean up
	cleanUpVol(vol1)
}

// TestVolumeInitializationFailureAndRetry tests the volume initialization failure and retry logic
func TestVolumeInitializationFailureAndRetry(t *testing.T) {
	t.Run("PartialMetaPartitionFailure", testPartialMetaPartitionFailure)
	t.Run("PartialDataPartitionFailure", testPartialDataPartitionFailure)
	t.Run("CompleteInitializationFailure", testCompleteInitializationFailure)
	t.Run("RetryAfterFailure", testRetryAfterFailure)
}

// testPartialMetaPartitionFailure tests partial meta partition creation failure
func testPartialMetaPartitionFailure(t *testing.T) {
	volName := "test_partial_meta_failure"

	// Simulate limited meta nodes by setting some to read-only
	metaNodes := make([]*MetaNode, 0)
	server.cluster.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		metaNodes = append(metaNodes, metaNode)
		return true
	})

	if len(metaNodes) > 2 {
		// Set most meta nodes to read-only, keep only 1-2 available
		for i := 2; i < len(metaNodes); i++ {
			metaNodes[i].RdOnly = true
		}

		defer func() {
			// Restore all meta nodes
			for i := 2; i < len(metaNodes); i++ {
				metaNodes[i].RdOnly = false
			}
		}()
	}

	req := createDefaultReq(volName, "test_user")
	req.mpCount = 5
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)

	require.NotNil(t, vol)
	t.Logf("error: %v", err)
	vol.mpsLock.RLock()
	actualMpCount := len(vol.MetaPartitions)
	vol.mpsLock.RUnlock()

	t.Logf("Requested %d meta partitions, got %d, status: %d",
		req.mpCount, actualMpCount, vol.Status)

	// Should have fewer meta partitions than requested
	require.Less(t, actualMpCount, req.mpCount,
		"Should have fewer meta partitions due to limited resources")

	// Volume might be in InitFailed status
	require.Equal(t, proto.VolStatusInitFailed, vol.Status)

	// Clean up
	cleanUpVol(vol)
}

// testPartialDataPartitionFailure tests partial data partition creation failure
func testPartialDataPartitionFailure(t *testing.T) {
	volName := "test_partial_data_failure"

	// Simulate limited data nodes
	dataNodes := make([]*DataNode, 0)
	server.cluster.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		dataNodes = append(dataNodes, dataNode)
		return true
	})

	if len(dataNodes) > 2 {
		// Set most data nodes to read-only
		for i := 2; i < len(dataNodes); i++ {
			dataNodes[i].RdOnly = true
		}

		defer func() {
			// Restore all data nodes
			for i := 2; i < len(dataNodes); i++ {
				dataNodes[i].RdOnly = false
			}
		}()
	}

	req := createDefaultReq(volName, "test_user")
	req.dpCount = 20
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)
	vol, err := server.cluster.createVol(req)
	require.NotNil(t, vol)
	t.Logf("error: %v", err)
	actualDpCount := len(vol.dataPartitions.partitions)

	t.Logf("Requested %d data partitions, got %d, status: %d",
		req.dpCount, actualDpCount, vol.Status)

	// Should have fewer data partitions than requested
	require.Less(t, actualDpCount, req.dpCount,
		"Should have fewer data partitions due to limited resources")

	// Clean up
	cleanUpVol(vol)
}

// testCompleteInitializationFailure tests complete initialization failure
func testCompleteInitializationFailure(t *testing.T) {
	volName := "test_complete_init_failure"

	// Set all nodes to read-only to simulate complete failure
	metaNodes := make([]*MetaNode, 0)
	server.cluster.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		metaNodes = append(metaNodes, metaNode)
		return true
	})

	dataNodes := make([]*DataNode, 0)
	server.cluster.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		dataNodes = append(dataNodes, dataNode)
		return true
	})

	// Keep only minimal nodes available
	if len(metaNodes) > 1 {
		for i := 1; i < len(metaNodes); i++ {
			metaNodes[i].RdOnly = true
		}
	}

	if len(dataNodes) > 1 {
		for i := 1; i < len(dataNodes); i++ {
			dataNodes[i].RdOnly = true
		}
	}

	defer func() {
		// Restore all nodes
		for i := 1; i < len(metaNodes); i++ {
			metaNodes[i].RdOnly = false
		}
		for i := 1; i < len(dataNodes); i++ {
			dataNodes[i].RdOnly = false
		}
	}()

	req := createDefaultReq(volName, "test_user")
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)

	require.NotNil(t, vol)
	t.Logf("error: %v", err)
	require.Equal(t, proto.VolStatusInitFailed, vol.Status)

	// Clean up
	cleanUpVol(vol)
}

// testRetryAfterFailure tests retry mechanism after initialization failure
func testRetryAfterFailure(t *testing.T) {
	volName := "test_retry_after_failure"

	// First, create a scenario that will fail
	metaNodes := make([]*MetaNode, 0)
	server.cluster.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		metaNodes = append(metaNodes, metaNode)
		return true
	})

	// Limit resources initially
	if len(metaNodes) > 2 {
		for i := 2; i < len(metaNodes); i++ {
			metaNodes[i].RdOnly = true
		}
	}

	defer func() {
		for i := 2; i < len(metaNodes); i++ {
			metaNodes[i].RdOnly = false
		}
	}()

	req := createDefaultReq(volName, "test_user")
	req.mpCount = 5
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// First attempt - should fail or be incomplete
	vol1, err1 := server.cluster.createVol(req)
	t.Logf("First attempt: error=%v", err1)

	require.NotNil(t, vol1)
	t.Logf("First attempt created volume with status: %d", vol1.Status)

	vol1.mpsLock.RLock()
	initialMpCount := len(vol1.MetaPartitions)
	vol1.mpsLock.RUnlock()

	// Now restore resources
	for i := 2; i < len(metaNodes); i++ {
		metaNodes[i].RdOnly = false
	}

	// Wait for cluster to update
	time.Sleep(2 * time.Second)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(1 * time.Second)

	// Retry - should succeed or improve
	vol2, err2 := server.cluster.createVol(req)
	t.Logf("Retry attempt: error=%v", err2)

	require.NotNil(t, vol2)
	t.Logf("Retry created volume with status: %d", vol2.Status)

	vol2.mpsLock.RLock()
	retryMpCount := len(vol2.MetaPartitions)
	vol2.mpsLock.RUnlock()

	t.Logf("Initial MP count: %d, Retry MP count: %d",
		initialMpCount, retryMpCount)

	// Retry should have same or more partitions
	require.Greater(t, retryMpCount, initialMpCount,
		"Retry should maintain or improve partition count")

	// Clean up
	cleanUpVol(vol2)
}

// TestAdvancedInitializationFailureScenarios tests advanced initialization failure scenarios
func TestAdvancedInitializationFailureScenarios(t *testing.T) {
	t.Run("StageByStageFailure", TestStageByStageFailure)
	t.Run("ProgressiveRecovery", TestProgressiveRecovery)
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

	req := createDefaultReq(volName, "test_user")
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)
	require.NotNil(t, vol)
	t.Logf("error: %v", err)
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
	require.NotNil(t, vol2)
	vol2.mpsLock.RLock()
	stage2MpCount := len(vol2.MetaPartitions)
	vol2.mpsLock.RUnlock()
	t.Logf("Stage 2 - Meta partition result: %d partitions, error: %v", stage2MpCount, err)
	require.GreaterOrEqual(t, stage2MpCount, initialMpCount)
	require.Equal(t, proto.VolStatusInitFailed, vol2.Status)
	checkVolumeState(t, vol2, "Stage 2")

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
	require.NotNil(t, vol3)

	stage3DpCount := len(vol3.dataPartitions.partitions)
	t.Logf("Stage 3 - Data partition result: %d partitions, error: %v", stage3DpCount, err)
	require.GreaterOrEqual(t, stage3DpCount, initialDpCount, "Should have same or more data partitions")
	require.Equal(t, proto.VolStatusInitFailed, vol3.Status)

	checkVolumeState(t, vol3, "Stage 3")

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
	require.NotNil(t, vol4)

	vol4.mpsLock.RLock()
	finalMpCount := len(vol4.MetaPartitions)
	vol4.mpsLock.RUnlock()
	finalDpCount := len(vol4.dataPartitions.partitions)

	t.Logf("Stage 4 - Final result: %d meta partitions, %d data partitions, error: %v",
		finalMpCount, finalDpCount, err)

	require.NoError(t, err)
	require.Equal(t, proto.VolStatusNormal, vol4.Status)
	require.GreaterOrEqual(t, finalMpCount, req.mpCount)
	require.GreaterOrEqual(t, finalDpCount, req.dpCount)

	checkVolumeState(t, vol4, "Stage 4 (Final)")

	// Clean up
	cleanUpVol(vol4)
}

// TestProgressiveRecovery tests progressive recovery scenarios
func TestProgressiveRecovery(t *testing.T) {
	volName := "test_progressive_recovery"

	t.Logf("=== Progressive Recovery Test ===")

	// Create a volume that will fail initially
	req := createDefaultReq(volName, "test_user")
	// Simulate resource constraints by limiting available nodes
	metaNodesBeforeTest := make([]*MetaNode, 0)
	server.cluster.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		metaNodesBeforeTest = append(metaNodesBeforeTest, metaNode)
		return true
	})

	metaNodesToLimit := metaNodesBeforeTest[1:]
	for _, node := range metaNodesToLimit {
		node.RdOnly = true
	}

	defer func() {
		for _, node := range metaNodesToLimit {
			node.RdOnly = false
		}
	}()

	// First attempt - should partially fail
	vol, err := server.cluster.createVol(req)
	t.Logf("First attempt result: error=%v", err)

	require.NotNil(t, vol)
	checkVolumeState(t, vol, "First Attempt")

	// Progressive recovery attempts
	for attempt := 0; attempt < len(metaNodesToLimit); attempt++ {
		t.Logf("=== Recovery Attempt %d ===", attempt)
		// Gradually restore nodes
		metaNodesToLimit[attempt].RdOnly = false
		t.Logf("Restored meta node %s", metaNodesToLimit[attempt].Addr)

		time.Sleep(2 * time.Second)
		server.cluster.checkMetaNodeHeartbeat()
		time.Sleep(1 * time.Second)

		t.Logf("vol status: %d", vol.Status)

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

	require.Equal(t, proto.VolStatusNormal, vol.Status)
	// Clean up
	cleanUpVol(vol)
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

// testInitializingStatusHandling tests handling of Initializing status
func testInitializingStatusHandling(t *testing.T) {
	req := createDefaultReq("test_initializing_status", "cfs_test_user")
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol)

	vol.Status = proto.VolStatusInitializing
	server.cluster.syncUpdateVol(vol)

	require.Equal(t, proto.VolStatusInitializing, vol.Status)

	started := make(chan struct{})
	done := make(chan struct{})

	go func() {
		close(started)
		vol2, err2 := server.cluster.createVol(req)
		require.NoError(t, err2)
		require.NotNil(t, vol2)

		require.Equal(t, vol.ID, vol2.ID)
		require.Equal(t, vol.Name, vol2.Name)
		close(done)
	}()

	<-started
	time.Sleep(1 * time.Second)
	vol.Status = proto.VolStatusNormal
	server.cluster.syncUpdateVol(vol)

	<-done

	// clean up
	cleanUpVol(vol)
}

func createDefaultReq(name string, owner string) *createVolReq {
	return &createVolReq{
		name:                     name,
		owner:                    owner,
		dpSize:                   11,
		mpCount:                  3,
		dpCount:                  10,
		dpReplicaNum:             3,
		capacity:                 100,
		followerRead:             false,
		authenticate:             false,
		crossZone:                false,
		zoneName:                 testZone2,
		description:              "",
		volType:                  proto.VolumeTypeHot,
		qosLimitArgs:             &qosArgs{},
		volStorageClass:          defaultVolStorageClass,
		storeMode:                proto.StoreModeMem,
		defaultPoolId:            defaultPoolId,
		accessTimeValidInterval:  proto.MinAccessTimeValidInterval,
		remoteCacheReadTimeout:   proto.ReadDeadlineTime,
		remoteCacheOnlyForNotSSD: true,
		allowedPools:             []uint8{defaultPoolId},
	}
}

func cleanUpVol(vol *Vol) {
	if vol == nil {
		return
	}
	vol.volLock.Lock()
	vol.Status = proto.VolStatusMarkDelete
	vol.volLock.Unlock()
	server.cluster.syncUpdateVol(vol)
	server.cluster.deleteVol(vol.Name)
}
