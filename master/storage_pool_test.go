// Copyright 2023 The CubeFS Authors.
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

package master

import (
	"fmt"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateStoragePool tests creating storage pools
func TestCreateStoragePool(t *testing.T) {
	c := server.cluster

	// Test case 1: Create a valid SSD pool
	poolInfo1 := &proto.StoragePoolInfo{
		Id:           10,
		Name:         "test-ssd-pool",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err := c.createStoragePool(poolInfo1)
	require.NoError(t, err, "should create SSD pool successfully")

	// Verify pool was created
	pool, err := c.getStoragePool(10)
	require.NoError(t, err)
	assert.Equal(t, uint8(10), pool.Id)
	assert.Equal(t, "test-ssd-pool", pool.Name)
	assert.Equal(t, uint8(proto.StorageClass_Replica_SSD), pool.StorageClass)
	assert.Equal(t, proto.PoolStatusAvailable, pool.Status)

	// Test case 2: Create a valid HDD pool
	poolInfo2 := &proto.StoragePoolInfo{
		Id:           11,
		Name:         "test-hdd-pool",
		StorageClass: uint8(proto.StorageClass_Replica_HDD),
	}
	err = c.createStoragePool(poolInfo2)
	require.NoError(t, err, "should create HDD pool successfully")

	// Test case 3: Create a valid EC pool with CId and ECAddr
	poolInfo3 := &proto.StoragePoolInfo{
		Id:           12,
		Name:         "test-ec-pool",
		StorageClass: uint8(proto.StorageClass_BlobStore),
		CId:          1,
		ECAddr:       "127.0.0.1:8500",
	}
	err = c.createStoragePool(poolInfo3)
	require.NoError(t, err, "should create EC pool successfully")

	// Verify EC pool
	pool, err = c.getStoragePool(12)
	require.NoError(t, err)
	assert.Equal(t, uint8(12), pool.Id)
	assert.Equal(t, "test-ec-pool", pool.Name)
	assert.Equal(t, uint8(proto.StorageClass_BlobStore), pool.StorageClass)
	assert.Equal(t, 1, pool.CId)
	assert.Equal(t, "127.0.0.1:8500", pool.ECAddr)

	// Test case 4: Try to create pool with duplicate ID
	err = c.createStoragePool(poolInfo1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Test case 5: Try to create pool with duplicate name
	poolInfo4 := &proto.StoragePoolInfo{
		Id:           13,
		Name:         "test-ssd-pool", // duplicate name
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err = c.createStoragePool(poolInfo4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Test case 6: Try to create EC pool without CId and ECAddr
	poolInfo5 := &proto.StoragePoolInfo{
		Id:           14,
		Name:         "test-ec-pool-invalid",
		StorageClass: uint8(proto.StorageClass_BlobStore),
		// Missing CId and ECAddr
	}
	err = c.createStoragePool(poolInfo5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires CId and ECAddr")

	// Test case 7: Try to create pool with invalid storage class
	poolInfo6 := &proto.StoragePoolInfo{
		Id:           15,
		Name:         "test-invalid-pool",
		StorageClass: 255, // invalid storage class
	}
	err = c.createStoragePool(poolInfo6)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid storage class")
}

// TestGetStoragePool tests getting storage pools
func TestGetStoragePool(t *testing.T) {
	c := server.cluster

	// Test case 1: Get existing pool
	pool, err := c.getStoragePool(proto.DefaultSSDPoolId)
	require.NoError(t, err)
	assert.NotNil(t, pool)
	assert.Equal(t, proto.DefaultSSDPoolId, pool.Id)

	// Test case 2: Get non-existent pool
	pool, err = c.getStoragePool(255)
	require.Error(t, err)
	assert.Nil(t, pool)
	assert.Contains(t, err.Error(), "not found")
}

// TestListStoragePools tests listing all storage pools
func TestListStoragePools(t *testing.T) {
	c := server.cluster

	// Create some test pools
	poolInfo1 := &proto.StoragePoolInfo{
		Id:           20,
		Name:         "list-test-pool-1",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err := c.createStoragePool(poolInfo1)
	require.NoError(t, err)

	poolInfo2 := &proto.StoragePoolInfo{
		Id:           21,
		Name:         "list-test-pool-2",
		StorageClass: uint8(proto.StorageClass_Replica_HDD),
	}
	err = c.createStoragePool(poolInfo2)
	require.NoError(t, err)

	// List all pools
	pools := c.listStoragePools()
	assert.GreaterOrEqual(t, len(pools), 2, "should have at least 2 pools")

	// Verify pools are sorted by ID
	for i := 1; i < len(pools); i++ {
		assert.LessOrEqual(t, pools[i-1].Id, pools[i].Id, "pools should be sorted by ID")
	}

	// Verify we can find our test pools
	found1 := false
	found2 := false
	for _, pool := range pools {
		if pool.Id == 20 {
			found1 = true
			assert.Equal(t, "list-test-pool-1", pool.Name)
		}
		if pool.Id == 21 {
			found2 = true
			assert.Equal(t, "list-test-pool-2", pool.Name)
		}
	}
	assert.True(t, found1, "should find pool 20")
	assert.True(t, found2, "should find pool 21")
}

// TestUpdateStoragePool tests updating storage pools
func TestUpdateStoragePool(t *testing.T) {
	c := server.cluster

	// Create a test pool
	poolInfo := &proto.StoragePoolInfo{
		Id:           30,
		Name:         "update-test-pool",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err := c.createStoragePool(poolInfo)
	require.NoError(t, err)

	// Test case 1: Update pool name
	updateInfo1 := &proto.StoragePoolInfo{
		Name: "updated-pool-name",
	}
	err = c.updateStoragePool(30, updateInfo1)
	require.NoError(t, err)

	pool, err := c.getStoragePool(30)
	require.NoError(t, err)
	assert.Equal(t, "updated-pool-name", pool.Name)

	// Test case 2: Update EC pool CId and ECAddr
	ecPoolInfo := &proto.StoragePoolInfo{
		Id:           31,
		Name:         "update-ec-pool",
		StorageClass: uint8(proto.StorageClass_BlobStore),
		CId:          1,
		ECAddr:       "127.0.0.1:8500",
	}
	err = c.createStoragePool(ecPoolInfo)
	require.NoError(t, err)

	updateInfo2 := &proto.StoragePoolInfo{
		CId:    2,
		ECAddr: "127.0.0.1:8501",
	}
	err = c.updateStoragePool(31, updateInfo2)
	require.NoError(t, err)

	pool, err = c.getStoragePool(31)
	require.NoError(t, err)
	assert.Equal(t, 2, pool.CId)
	assert.Equal(t, "127.0.0.1:8501", pool.ECAddr)

	// Test case 3: Update non-existent pool
	updateInfo3 := &proto.StoragePoolInfo{
		Name: "non-existent",
	}
	err = c.updateStoragePool(255, updateInfo3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test case 4: Try to update with duplicate name
	poolInfo2 := &proto.StoragePoolInfo{
		Id:           32,
		Name:         "another-pool",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err = c.createStoragePool(poolInfo2)
	require.NoError(t, err)

	updateInfo4 := &proto.StoragePoolInfo{
		Name: "updated-pool-name", // duplicate name from pool 30
	}
	err = c.updateStoragePool(32, updateInfo4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Test case 5: Try to update storage class (not allowed)
	updateInfo5 := &proto.StoragePoolInfo{
		StorageClass: uint8(proto.StorageClass_Replica_HDD),
	}
	err = c.updateStoragePool(30, updateInfo5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage class update is not supported")

	// Test case 6: Try to set CId/ECAddr on non-EC pool
	updateInfo6 := &proto.StoragePoolInfo{
		CId:    1,
		ECAddr: "127.0.0.1:8500",
	}
	err = c.updateStoragePool(30, updateInfo6)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-EC pool does not support CId and ECAddr")
}

// TestGetStoragePoolCount tests getting storage pool count
func TestGetStoragePoolCount(t *testing.T) {
	c := server.cluster

	initialCount := c.getStoragePoolCount()

	// Create a test pool
	poolInfo := &proto.StoragePoolInfo{
		Id:           40,
		Name:         "count-test-pool",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err := c.createStoragePool(poolInfo)
	require.NoError(t, err)

	// Verify count increased
	newCount := c.getStoragePoolCount()
	assert.Equal(t, initialCount+1, newCount)
}

// TestGetPoolNameById tests getting pool name by ID
func TestGetPoolNameById(t *testing.T) {
	c := server.cluster

	// Test case 1: Get name of existing pool
	poolInfo := &proto.StoragePoolInfo{
		Id:           50,
		Name:         "name-test-pool",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err := c.createStoragePool(poolInfo)
	require.NoError(t, err)

	name := c.getPoolNameById(50)
	assert.Equal(t, "name-test-pool", name)

	// Test case 2: Get name of non-existent pool
	name = c.getPoolNameById(255)
	assert.Contains(t, name, "UnknownPool-255")
}

// TestStoragePoolConcurrency tests concurrent operations on storage pools
func TestStoragePoolConcurrency(t *testing.T) {
	c := server.cluster

	// Create multiple pools concurrently
	const numPools = 10
	errors := make(chan error, numPools)

	for i := 0; i < numPools; i++ {
		go func(id uint8) {
			poolInfo := &proto.StoragePoolInfo{
				Id:           60 + id,
				Name:         fmt.Sprintf("concurrent-pool-%d", id),
				StorageClass: uint8(proto.StorageClass_Replica_SSD),
			}
			err := c.createStoragePool(poolInfo)
			errors <- err
		}(uint8(i))
	}

	// Collect results
	var successCount int
	for i := 0; i < numPools; i++ {
		err := <-errors
		if err == nil {
			successCount++
		}
	}

	// All pools should be created successfully
	assert.Equal(t, numPools, successCount)

	// Verify all pools exist
	for i := 0; i < numPools; i++ {
		pool, err := c.getStoragePool(60 + uint8(i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("concurrent-pool-%d", i), pool.Name)
	}
}

// TestDefaultStoragePools tests default storage pools
func TestDefaultStoragePools(t *testing.T) {
	c := server.cluster

	// Verify default pools exist
	ssdPool, err := c.getStoragePool(proto.DefaultSSDPoolId)
	require.NoError(t, err)
	assert.Equal(t, proto.DefaultSSDPoolName, ssdPool.Name)
	assert.Equal(t, uint8(proto.StorageClass_Replica_SSD), ssdPool.StorageClass)

	hddPool, err := c.getStoragePool(proto.DefaultHDDPoolId)
	require.NoError(t, err)
	assert.Equal(t, proto.DefaultHDDPoolName, hddPool.Name)
	assert.Equal(t, uint8(proto.StorageClass_Replica_HDD), hddPool.StorageClass)

	ecPool, err := c.getStoragePool(proto.DefaultECPoolId)
	require.NoError(t, err)
	assert.Equal(t, proto.DefaultECPoolName, ecPool.Name)
	assert.Equal(t, uint8(proto.StorageClass_BlobStore), ecPool.StorageClass)
}

// TestVolumeWithPool tests volume creation and operations with pools
func TestVolumeWithPool(t *testing.T) {
	c := server.cluster

	// Create a test pool
	poolInfo := &proto.StoragePoolInfo{
		Id:           70,
		Name:         "vol-test-pool",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err := c.createStoragePool(poolInfo)
	require.NoError(t, err)

	// Create volume with defaultPoolId
	req := createDefaultReq("test_vol_with_pool", "cfs_test_user")
	req.defaultPoolId = 70
	err = server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol)
	assert.Equal(t, uint8(70), vol.defaultPoolId)

	// Clean up
	cleanUpVol(vol)
}

// TestVolumeWithAllowedPools tests volume creation with allowed pools
func TestVolumeWithAllowedPools(t *testing.T) {
	c := server.cluster

	// Create test pools
	poolInfo1 := &proto.StoragePoolInfo{
		Id:           71,
		Name:         "allowed-pool-1",
		StorageClass: uint8(proto.StorageClass_Replica_SSD),
	}
	err := c.createStoragePool(poolInfo1)
	require.NoError(t, err)

	poolInfo2 := &proto.StoragePoolInfo{
		Id:           72,
		Name:         "allowed-pool-2",
		StorageClass: uint8(proto.StorageClass_Replica_HDD),
	}
	err = c.createStoragePool(poolInfo2)
	require.NoError(t, err)

	// Create volume with allowed pools
	req := createDefaultReq("test_vol_with_allowed_pools", "cfs_test_user")
	req.defaultPoolId = 71
	req.allowedPools = []uint8{71, 72}
	err = server.checkCreateVolReq(req)
	require.NoError(t, err)

	vol, err := server.cluster.createVol(req)
	require.NoError(t, err)
	require.NotNil(t, vol)
	assert.Equal(t, uint8(71), vol.defaultPoolId)
	assert.Contains(t, vol.allowedPools, uint8(71))
	assert.Contains(t, vol.allowedPools, uint8(72))

	// Clean up
	cleanUpVol(vol)
}
