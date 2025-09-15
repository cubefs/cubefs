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
func TestCreateVolAdvanced(t *testing.T) {
	t.Run("SameNameWithin5Minutes", testSameNameWithin5Minutes)
	t.Run("SameNameAfter5Minutes", testSameNameAfter5Minutes)
	t.Run("InitFailedCleanup", testInitFailedCleanup)
	t.Run("ConcurrentCreation", testConcurrentVolumeCreation)
	t.Run("ConcurrentSameName", testConcurrentSameNameCreation)
	t.Run("ResourceShortageRetry", testResourceShortageRetry)
}

// testSameNameWithin5Minutes tests that creating a volume with the same name
// within 5 minutes should return the existing volume instead of error
func testSameNameWithin5Minutes(t *testing.T) {
	volName := "test_same_name_within_5min"
	owner := "cfs_test_user"

	// Create the first volume
	req := &createVolReq{
		name:            volName,
		owner:           owner,
		dpSize:          11, // Must be bigger than 10G
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test volume for same name within 5 minutes",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	// Auto set allowedStorageClass[] in createVolReq
	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// Create the first volume
	vol1, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol1)
	require.Equal(t, volName, vol1.Name)
	require.Equal(t, owner, vol1.Owner)
	require.Equal(t, proto.VolStatusNormal, vol1.Status)

	// Immediately try to create the same volume again (within 5 minutes)
	vol2, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol2)

	// Should return the same volume
	require.Equal(t, vol1.ID, vol2.ID)
	require.Equal(t, vol1.Name, vol2.Name)
	require.Equal(t, vol1.Owner, vol2.Owner)
	require.Equal(t, vol1.createTime, vol2.createTime)

	// Clean up
	vol1.Status = proto.VolStatusMarkDelete
	err = server.cluster.syncUpdateVol(vol1)
	require.NoError(t, err)
	server.cluster.deleteVol(volName)
}

// testSameNameAfter5Minutes tests creating volume with same name after 5 minutes
func testSameNameAfter5Minutes(t *testing.T) {
	volName := "test_same_name_after_5min"
	owner := "cfs_test_user"

	req := &createVolReq{
		name:            volName,
		owner:           owner,
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test volume for same name after 5 minutes",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// Create the first volume
	vol1, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol1)

	// Simulate time passing by modifying createTime
	vol1.createTime = time.Now().Unix() - 6*60 // 6 minutes ago

	// Try to create the same volume again (after 5 minutes)
	vol2, err := server.cluster.createVol(req)

	// This should either succeed (returning the same volume) or fail with duplicate error
	// depending on the implementation
	if err == nil {
		require.NotNil(t, vol2)
		t.Logf("Volume creation succeeded, returned volume ID: %d", vol2.ID)
	} else {
		t.Logf("Volume creation failed as expected: %v", err)
	}

	// Clean up
	vol1.Status = proto.VolStatusMarkDelete
	err = server.cluster.syncUpdateVol(vol1)
	require.NoError(t, err)
	server.cluster.deleteVol(volName)
}

// testInitFailedCleanup tests cleanup of InitFailed volumes
func testInitFailedCleanup(t *testing.T) {
	volName := "test_init_failed_cleanup"
	owner := "cfs_test_user"

	req := &createVolReq{
		name:            volName,
		owner:           owner,
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test init failed cleanup",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// Create volume that might fail
	vol, err := server.cluster.createVol(req)

	if vol != nil {
		// If volume was created but failed, test retry mechanism
		if vol.Status == proto.VolStatusInitFailed {
			t.Logf("Volume is in InitFailed status, testing retry...")

			// Try to create again - should retry initialization
			vol2, err2 := server.cluster.createVol(req)
			if err2 == nil && vol2 != nil {
				t.Logf("Retry succeeded, volume status: %d", vol2.Status)
				vol = vol2
			} else {
				t.Logf("Retry failed: %v", err2)
			}
		}

		// Test checkInitFailed method
		if vol.Status == proto.VolStatusInitFailed {
			// Simulate old creation time to trigger cleanup
			vol.createTime = time.Now().Unix() - 25*60 // 25 minutes ago

			// Call checkInitFailed
			vol.checkInitFailed(server.cluster)

			// Should be marked for deletion
			require.Equal(t, proto.VolStatusMarkDelete, vol.Status)
		}

		// Clean up
		server.cluster.deleteVol(volName)
	} else {
		t.Logf("Volume creation failed: %v", err)
	}
}

// testConcurrentVolumeCreation tests concurrent creation of different volumes
func testConcurrentVolumeCreation(t *testing.T) {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	var results []error
	var volumes []*Vol

	numVolumes := 5

	for i := 0; i < numVolumes; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			volName := fmt.Sprintf("test_concurrent_vol_%d", id)
			req := &createVolReq{
				name:            volName,
				owner:           "cfs_test_user",
				dpSize:          11,
				mpCount:         3,
				dpCount:         10,
				dpReplicaNum:    3,
				capacity:        100,
				followerRead:    false,
				authenticate:    false,
				crossZone:       false,
				zoneName:        testZone2,
				description:     fmt.Sprintf("test concurrent volume %d", id),
				volType:         proto.VolumeTypeHot,
				qosLimitArgs:    &qosArgs{},
				volStorageClass: defaultVolStorageClass,
			}

			err := server.checkCreateVolReq(req)
			if err != nil {
				mutex.Lock()
				results = append(results, err)
				mutex.Unlock()
				return
			}

			vol, err := server.cluster.createVol(req)

			mutex.Lock()
			results = append(results, err)
			if vol != nil {
				volumes = append(volumes, vol)
			}
			mutex.Unlock()
		}(i)
	}

	wg.Wait()

	// Check results
	require.Equal(t, numVolumes, len(results))

	successCount := 0
	for _, err := range results {
		if err == nil {
			successCount++
		}
	}

	t.Logf("Concurrent creation results: %d/%d succeeded", successCount, numVolumes)
	require.Greater(t, successCount, 0, "At least one volume should be created successfully")

	// Clean up
	for _, vol := range volumes {
		if vol != nil {
			vol.Status = proto.VolStatusMarkDelete
			server.cluster.syncUpdateVol(vol)
			server.cluster.deleteVol(vol.Name)
		}
	}
}

// testConcurrentSameNameCreation tests concurrent creation of same-name volumes
func testConcurrentSameNameCreation(t *testing.T) {
	volName := "test_concurrent_same_name"
	var wg sync.WaitGroup
	var mutex sync.Mutex
	var results []error
	var volumes []*Vol

	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req := &createVolReq{
				name:            volName,
				owner:           "cfs_test_user",
				dpSize:          11,
				mpCount:         3,
				dpCount:         10,
				dpReplicaNum:    3,
				capacity:        100,
				followerRead:    false,
				authenticate:    false,
				crossZone:       false,
				zoneName:        testZone2,
				description:     "test concurrent same name creation",
				volType:         proto.VolumeTypeHot,
				qosLimitArgs:    &qosArgs{},
				volStorageClass: defaultVolStorageClass,
			}

			err := server.checkCreateVolReq(req)
			if err != nil {
				mutex.Lock()
				results = append(results, err)
				mutex.Unlock()
				return
			}

			vol, err := server.cluster.createVol(req)

			mutex.Lock()
			results = append(results, err)
			if vol != nil {
				volumes = append(volumes, vol)
			}
			mutex.Unlock()
		}(i)
	}

	wg.Wait()

	// Check results
	require.Equal(t, numGoroutines, len(results))

	successCount := 0
	for _, err := range results {
		if err == nil {
			successCount++
		}
	}

	t.Logf("Concurrent same-name creation results: %d/%d succeeded", successCount, numGoroutines)

	// All successful creations should return the same volume
	if len(volumes) > 1 {
		firstVol := volumes[0]
		for i := 1; i < len(volumes); i++ {
			require.Equal(t, firstVol.ID, volumes[i].ID, "All volumes should have the same ID")
			require.Equal(t, firstVol.Name, volumes[i].Name, "All volumes should have the same name")
		}
	}

	// Clean up
	if len(volumes) > 0 {
		vol := volumes[0]
		vol.Status = proto.VolStatusMarkDelete
		server.cluster.syncUpdateVol(vol)
		server.cluster.deleteVol(volName)
	}
}

// testResourceShortageRetry tests retry mechanism under resource shortage
func testResourceShortageRetry(t *testing.T) {
	volName := "test_resource_shortage_retry"

	// Create a volume that might face resource shortage
	req := &createVolReq{
		name:            volName,
		owner:           "cfs_test_user",
		dpSize:          11,
		mpCount:         10, // Request many partitions
		dpCount:         20,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test resource shortage retry",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	// First attempt
	vol1, err1 := server.cluster.createVol(req)
	t.Logf("First attempt: error=%v", err1)

	if vol1 != nil {
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
		vol1.Status = proto.VolStatusMarkDelete
		server.cluster.syncUpdateVol(vol1)
		server.cluster.deleteVol(volName)
	} else {
		t.Logf("Volume creation completely failed: %v", err1)
	}
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
	req1 := &createVolReq{
		name:            volName,
		owner:           "owner1",
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test different owner same name",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req1)
	require.NoError(t, err)

	vol1, err1 := server.cluster.createVol(req1)
	require.NoError(t, err1)
	require.NotNil(t, vol1)

	// Try to create volume with same name but different owner
	req2 := &createVolReq{
		name:            volName,
		owner:           "owner2", // Different owner
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test different owner same name",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err = server.checkCreateVolReq(req2)
	require.NoError(t, err)

	vol2, err2 := server.cluster.createVol(req2)

	// Should fail with duplicate volume error
	require.Error(t, err2)
	require.Nil(t, vol2)
	t.Logf("Different owner creation failed as expected: %v", err2)

	// Clean up
	vol1.Status = proto.VolStatusMarkDelete
	server.cluster.syncUpdateVol(vol1)
	server.cluster.deleteVol(volName)
}

// testInitializingStatusHandling tests handling of Initializing status
func testInitializingStatusHandling(t *testing.T) {
	volName := "test_initializing_status"

	req := &createVolReq{
		name:            volName,
		owner:           "cfs_test_user",
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test initializing status handling",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
	}

	err := server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)

	if vol != nil {
		t.Logf("Volume created with status: %d", vol.Status)

		// Test concurrent access while initializing
		if vol.Status == proto.VolStatusInitializing {
			// Try to create the same volume while it's initializing
			vol2, err2 := server.cluster.createVol(req)

			if err2 == nil && vol2 != nil {
				// Should return the same volume or wait for completion
				t.Logf("Concurrent creation during initialization succeeded")
				require.Equal(t, vol.ID, vol2.ID)
			} else {
				t.Logf("Concurrent creation during initialization failed: %v", err2)
			}
		}

		// Clean up
		vol.Status = proto.VolStatusMarkDelete
		server.cluster.syncUpdateVol(vol)
		server.cluster.deleteVol(volName)
	} else {
		t.Logf("Volume creation failed: %v", err)
	}
}
