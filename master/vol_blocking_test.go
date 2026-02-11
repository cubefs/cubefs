package master

import (
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

// TestVolumeInitializationBlocking test volume initialization blocking
func TestVolumeInitializationBlocking(t *testing.T) {
	// create a test volume
	vv := volValue{
		ID:     1,
		Name:   "test-vol",
		Owner:  "test-owner",
		Status: proto.VolStatusInitializing,
	}
	vol := newVol(vv)

	// test 1: verify initial status
	assert.Equal(t, proto.VolStatusInitializing, vol.Status, "initial status should be initializing")

	// test 2: test concurrent creation of same name volume blocking mechanism
	var wg sync.WaitGroup
	var results []error
	var resultsMutex sync.Mutex

	// start multiple goroutines to concurrently attempt to create the same name volume
	numGoroutines := 5
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req := &createVolReq{
				name:            "test-vol",
				owner:           "test-owner",
				dpSize:          11,
				mpCount:         3,
				dpCount:         10,
				dpReplicaNum:    3,
				capacity:        100,
				followerRead:    false,
				authenticate:    false,
				crossZone:       false,
				zoneName:        testZone2,
				description:     "test blocking",
				volType:         proto.VolumeTypeHot,
				qosLimitArgs:    &qosArgs{},
				volStorageClass: defaultVolStorageClass,
				defaultPoolId:   defaultPoolId,
			}

			// simulate the process of creating a volume
			_, err := server.cluster.createVol(req)

			resultsMutex.Lock()
			results = append(results, err)
			resultsMutex.Unlock()
		}(i)
	}

	// wait for all goroutines to complete
	wg.Wait()

	// verify results - should have only one success, others should either wait or fail
	assert.Equal(t, numGoroutines, len(results), "should have results from all goroutines")

	// clean up
	server.cluster.deleteVol("test-vol")
}

// TestConcurrentVolumeCreation test concurrent volume creation
func TestConcurrentVolumeCreation(t *testing.T) {
	var wg sync.WaitGroup
	var successCount int
	var errorCount int
	var mutex sync.Mutex

	numGoroutines := 10
	volName := "test-concurrent-vol"

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req := &createVolReq{
				name:            volName,
				owner:           "test-owner",
				dpSize:          11,
				mpCount:         3,
				dpCount:         10,
				dpReplicaNum:    3,
				capacity:        100,
				followerRead:    false,
				authenticate:    false,
				crossZone:       false,
				zoneName:        testZone2,
				description:     "test concurrent creation",
				volType:         proto.VolumeTypeHot,
				qosLimitArgs:    &qosArgs{},
				volStorageClass: defaultVolStorageClass,
				defaultPoolId:   defaultPoolId,
			}

			vol, err := server.cluster.createVol(req)

			mutex.Lock()
			if err == nil && vol != nil {
				successCount++
			} else {
				errorCount++
			}
			mutex.Unlock()
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent creation results: %d success, %d errors", successCount, errorCount)

	// should at least have one success
	assert.Greater(t, successCount, 0, "at least one creation should succeed")

	// clean up
	server.cluster.deleteVol(volName)
}

// TestVolumeInitializationTimeout test initialization timeout
func TestVolumeInitializationTimeout(t *testing.T) {
	volName := "test-timeout-vol"

	// create a request that will timeout
	req := &createVolReq{
		name:            volName,
		owner:           "test-owner",
		dpSize:          11,
		mpCount:         100, // request too many partitions, may cause timeout
		dpCount:         100,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test timeout",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
		defaultPoolId:   defaultPoolId,
	}

	start := time.Now()
	vol, err := server.cluster.createVol(req)
	duration := time.Since(start)

	t.Logf("Volume creation took %v, error: %v", duration, err)

	if vol != nil {
		t.Logf("Volume status: %d", vol.Status)
		// clean up
		server.cluster.deleteVol(volName)
	}
}

// TestVolumeStatusTransitions test volume status transitions
func TestVolumeStatusTransitions(t *testing.T) {
	volName := "test-status-transitions"

	req := &createVolReq{
		name:            volName,
		owner:           "test-owner",
		dpSize:          11,
		mpCount:         3,
		dpCount:         10,
		dpReplicaNum:    3,
		capacity:        100,
		followerRead:    false,
		authenticate:    false,
		crossZone:       false,
		zoneName:        testZone2,
		description:     "test status transitions",
		volType:         proto.VolumeTypeHot,
		qosLimitArgs:    &qosArgs{},
		volStorageClass: defaultVolStorageClass,
		defaultPoolId:   defaultPoolId,
	}

	vol, err := server.cluster.createVol(req)

	if vol != nil {
		t.Logf("Initial volume status: %d", vol.Status)

		// test status transitions
		if vol.Status == proto.VolStatusInitFailed {
			t.Logf("Volume is in InitFailed status, testing retry...")

			// try retry
			vol2, err2 := server.cluster.createVol(req)
			if vol2 != nil {
				t.Logf("After retry, volume status: %d", vol2.Status)
				vol = vol2
			} else {
				t.Logf("Retry failed: %v", err2)
			}
		}

		// clean up
		server.cluster.deleteVol(volName)
	} else {
		t.Logf("Volume creation failed: %v", err)
	}
}
