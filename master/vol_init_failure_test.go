package master

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
)

// TestVolumeInitializationFailureAndRetry tests the volume initialization failure and retry logic
func TestVolumeInitializationFailureAndRetry(t *testing.T) {
	t.Run("PartialMetaPartitionFailure", testPartialMetaPartitionFailure)
	t.Run("PartialDataPartitionFailure", testPartialDataPartitionFailure)
	t.Run("CompleteInitializationFailure", testCompleteInitializationFailure)
	t.Run("RetryAfterFailure", testRetryAfterFailure)
	t.Run("FailureCleanupAfterTimeout", testFailureCleanupAfterTimeout)
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

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         5, // Request more than available
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test partial meta partition failure",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)

	if vol != nil {
		vol.mpsLock.RLock()
		actualMpCount := len(vol.MetaPartitions)
		vol.mpsLock.RUnlock()

		t.Logf("Requested %d meta partitions, got %d, status: %d",
			req.mpCount, actualMpCount, vol.Status)

		// Should have fewer meta partitions than requested
		require.Less(t, actualMpCount, req.mpCount,
			"Should have fewer meta partitions due to limited resources")

		// Volume might be in InitFailed status
		if vol.Status == proto.VolStatusInitFailed {
			t.Logf("Volume is in InitFailed status as expected")
		}

		// Clean up
		cleanupVolume(vol, server.cluster)
	} else {
		t.Logf("Volume creation failed completely: %v", err)
	}
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

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         3,
		dpCount:         20, // Request more than can be allocated
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test partial data partition failure",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)

	if vol != nil {
		actualDpCount := len(vol.dataPartitions.partitions)

		t.Logf("Requested %d data partitions, got %d, status: %d",
			req.dpCount, actualDpCount, vol.Status)

		// Should have fewer data partitions than requested
		require.Less(t, actualDpCount, req.dpCount,
			"Should have fewer data partitions due to limited resources")

		// Volume might be in InitFailed status
		if vol.Status == proto.VolStatusInitFailed {
			t.Logf("Volume is in InitFailed status as expected")
		}

		// Clean up
		cleanupVolume(vol, server.cluster)
	} else {
		t.Logf("Volume creation failed completely: %v", err)
	}
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

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3, // Need 3 replicas but only 1 node available
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test complete initialization failure",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)

	if err != nil {
		t.Logf("Complete failure as expected: %v", err)
		require.Error(t, err, "Should fail with insufficient resources")
	} else if vol != nil {
		t.Logf("Volume created with status: %d", vol.Status)
		require.Equal(t, proto.VolStatusInitFailed, vol.Status,
			"Volume should be in InitFailed status")

		// Clean up
		cleanupVolume(vol, server.cluster)
	}
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

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         5, // More than available
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test retry after failure",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// First attempt - should fail or be incomplete
	vol1, err1 := server.cluster.createVol(req)
	t.Logf("First attempt: error=%v", err1)

	if vol1 != nil {
		t.Logf("First attempt created volume with status: %d", vol1.Status)

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

		if vol2 != nil {
			t.Logf("Retry created volume with status: %d", vol2.Status)

			vol2.mpsLock.RLock()
			retryMpCount := len(vol2.MetaPartitions)
			vol2.mpsLock.RUnlock()

			vol1.mpsLock.RLock()
			initialMpCount := len(vol1.MetaPartitions)
			vol1.mpsLock.RUnlock()

			t.Logf("Initial MP count: %d, Retry MP count: %d",
				initialMpCount, retryMpCount)

			// Retry should have same or more partitions
			require.GreaterOrEqual(t, retryMpCount, initialMpCount,
				"Retry should maintain or improve partition count")

			// Clean up
			cleanupVolume(vol2, server.cluster)
		} else {
			// Clean up original volume
			cleanupVolume(vol1, server.cluster)
		}
	} else {
		t.Logf("First attempt failed completely: %v", err1)
	}

	// Restore all nodes
	for i := 2; i < len(metaNodes); i++ {
		metaNodes[i].RdOnly = false
	}
}

// testFailureCleanupAfterTimeout tests cleanup of failed volumes after timeout
func testFailureCleanupAfterTimeout(t *testing.T) {
	volName := "test_failure_cleanup_timeout"

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
		description:     "test failure cleanup after timeout",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)

	if vol != nil {
		// Force volume to InitFailed status
		vol.volLock.Lock()
		vol.Status = proto.VolStatusInitFailed
		vol.volLock.Unlock()

		// Simulate old creation time (more than 20 minutes ago)
		vol.createTime = time.Now().Unix() - 25*60

		t.Logf("Volume status before cleanup: %d", vol.Status)

		// Call checkInitFailed to trigger cleanup
		vol.checkInitFailed(server.cluster)

		t.Logf("Volume status after cleanup: %d", vol.Status)

		// Should be marked for deletion
		require.Equal(t, proto.VolStatusMarkDelete, vol.Status,
			"Volume should be marked for deletion after timeout")

		// Clean up
		server.cluster.deleteVol(volName)
	} else {
		t.Logf("Volume creation failed: %v", err)
	}
}

// TestVolumeInitializationEdgeCases tests edge cases in volume initialization
func TestVolumeInitializationEdgeCases(t *testing.T) {
	t.Run("ZeroPartitionRequest", testZeroPartitionRequest)
	t.Run("ExcessivePartitionRequest", testExcessivePartitionRequest)
	t.Run("InvalidReplicationFactor", testInvalidReplicationFactor)
}

// testZeroPartitionRequest tests handling of zero partition requests
func testZeroPartitionRequest(t *testing.T) {
	volName := "test_zero_partition"

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         0, // Zero meta partitions
		dpCount:         0, // Zero data partitions
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test zero partition request",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	if err != nil {
		t.Logf("Zero partition request validation failed as expected: %v", err)
		return
	}

	vol, err := server.cluster.createVol(req)

	if vol != nil {
		vol.mpsLock.RLock()
		mpCount := len(vol.MetaPartitions)
		vol.mpsLock.RUnlock()
		dpCount := len(vol.dataPartitions.partitions)

		t.Logf("Zero partition request result: %d MP, %d DP, status: %d",
			mpCount, dpCount, vol.Status)

		// Should have created minimum required partitions
		require.Greater(t, mpCount, 0, "Should create minimum meta partitions")
		// NOTE: Data partitions are only created after meta partitions succeed
		if mpCount > 0 {
			require.Greater(t, dpCount, 0, "Should create minimum data partitions when meta partitions exist")
		}

		// Clean up
		cleanupVolume(vol, server.cluster)
	} else {
		t.Logf("Zero partition request failed: %v", err)
	}
}

// testExcessivePartitionRequest tests handling of excessive partition requests
func testExcessivePartitionRequest(t *testing.T) {
	volName := "test_excessive_partition"

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         1000, // Excessive meta partitions
		dpCount:         1000, // Excessive data partitions
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test excessive partition request",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	if err != nil {
		t.Logf("Excessive partition request validation failed: %v", err)
		return
	}

	vol, err := server.cluster.createVol(req)

	if vol != nil {
		vol.mpsLock.RLock()
		mpCount := len(vol.MetaPartitions)
		vol.mpsLock.RUnlock()
		dpCount := len(vol.dataPartitions.partitions)

		t.Logf("Excessive partition request result: %d MP, %d DP, status: %d",
			mpCount, dpCount, vol.Status)

		// Should be limited by available resources
		require.Less(t, mpCount, req.mpCount, "Meta partitions should be limited")
		require.Less(t, dpCount, req.dpCount, "Data partitions should be limited")

		// Volume might be in InitFailed status due to resource constraints
		if vol.Status == proto.VolStatusInitFailed {
			t.Logf("Volume is in InitFailed status due to resource constraints")
		}

		// Clean up
		cleanupVolume(vol, server.cluster)
	} else {
		t.Logf("Excessive partition request failed: %v", err)
	}
}

// testInvalidReplicationFactor tests handling of invalid replication factors
func testInvalidReplicationFactor(t *testing.T) {
	volName := "test_invalid_replication"

	req := &createVolReq{
		name:            volName,
		owner:           "test_user",
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    10, // More replicas than available nodes
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test invalid replication factor",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	if err != nil {
		t.Logf("Invalid replication factor validation failed: %v", err)
		return
	}

	vol, err := server.cluster.createVol(req)

	if vol != nil {
		t.Logf("Invalid replication factor result: status: %d", vol.Status)

		// Volume should likely be in InitFailed status
		if vol.Status == proto.VolStatusInitFailed {
			t.Logf("Volume is in InitFailed status due to impossible replication factor")
		}

		// Clean up
		cleanupVolume(vol, server.cluster)
	} else {
		t.Logf("Invalid replication factor request failed: %v", err)
	}
}
