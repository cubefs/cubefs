// Copyright 2018 The CubeFS Authors.
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

package metanode

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util"
)

func TestExtend_Bytes(t *testing.T) {
	var err error
	const numSamples = 100

	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	extends := make([]*Extend, numSamples)
	for i := 0; i < numSamples; i++ {
		extend := NewExtend(random.Uint64())
		extend.Put([]byte("msg"), []byte(util.RandomString(16, util.Numeric|util.LowerLetter|util.UpperLetter)), 0)
		extends[i] = extend
	}

	outputs := make([][]byte, numSamples)
	for i := 0; i < numSamples; i++ {
		if outputs[i], err = extends[i].Bytes(); err != nil {
			t.Fatalf("encode extend to bytes fail cause: %v", err)
		}
	}

	// validate result
	for i := 0; i < numSamples; i++ {
		var e *Extend
		if e, err = NewExtendFromBytes(outputs[i]); err != nil {
			t.Fatalf("decode bytes to extend fail cause: %v", err)
		}
		if !reflect.DeepEqual(e, extends[i]) {
			t.Fatalf("result mismatch")
		}
	}
}

// TestExtend_NewExtend tests the NewExtend constructor
func TestExtend_NewExtend(t *testing.T) {
	inode := uint64(12345)
	extend := NewExtend(inode)

	if extend == nil {
		t.Fatal("NewExtend returned nil")
	}

	if extend.GetInode() != inode {
		t.Errorf("Expected inode %d, got %d", inode, extend.GetInode())
	}

	if extend.dataMap == nil {
		t.Error("dataMap should be initialized")
	}

	if len(extend.dataMap) != 0 {
		t.Error("dataMap should be empty")
	}
}

// TestExtend_NewExtendWithQuota tests the NewExtendWithQuota constructor
func TestExtend_NewExtendWithQuota(t *testing.T) {
	inode := uint64(67890)
	extend := NewExtendWithQuota(inode)

	if extend == nil {
		t.Fatal("NewExtendWithQuota returned nil")
	}

	if extend.GetInode() != inode {
		t.Errorf("Expected inode %d, got %d", inode, extend.GetInode())
	}

	if extend.dataMap != nil {
		t.Error("dataMap should be nil for quota-only extend")
	}
}

// TestExtend_Put tests the Put method
func TestExtend_Put(t *testing.T) {
	extend := NewExtend(12345)

	key := []byte("test_key")
	value := []byte("test_value")

	extend.Put(key, value, 0)

	retrievedValue, exists := extend.Get(key)
	if !exists {
		t.Error("Key should exist after Put")
	}

	if !reflect.DeepEqual(retrievedValue, value) {
		t.Errorf("Expected value %v, got %v", value, retrievedValue)
	}
}

// TestExtend_PutWithVersion tests Put with version sequence
func TestExtend_PutWithVersion(t *testing.T) {
	extend := NewExtend(12345)

	key := []byte("versioned_key")
	value := []byte("versioned_value")
	version := uint64(100)

	extend.Put(key, value, version)

	if extend.getVersion() != version {
		t.Errorf("Expected version %d, got %d", version, extend.getVersion())
	}
}

// TestExtend_Get tests the Get method
func TestExtend_Get(t *testing.T) {
	extend := NewExtend(12345)

	// Test getting non-existent key
	value, exists := extend.Get([]byte("non_existent"))
	if exists {
		t.Error("Non-existent key should not exist")
	}
	if value != nil {
		t.Error("Non-existent key should return nil value")
	}

	// Test getting existing key
	key := []byte("existing_key")
	expectedValue := []byte("existing_value")
	extend.Put(key, expectedValue, 0)

	value, exists = extend.Get(key)
	if !exists {
		t.Error("Existing key should exist")
	}
	if !reflect.DeepEqual(value, expectedValue) {
		t.Errorf("Expected value %v, got %v", expectedValue, value)
	}
}

// TestExtend_Remove tests the Remove method
func TestExtend_Remove(t *testing.T) {
	extend := NewExtend(12345)

	key := []byte("removable_key")
	value := []byte("removable_value")

	// Put and verify
	extend.Put(key, value, 0)
	if _, exists := extend.Get(key); !exists {
		t.Error("Key should exist after Put")
	}

	// Remove and verify
	extend.Remove(key)
	if _, exists := extend.Get(key); exists {
		t.Error("Key should not exist after Remove")
	}
}

// TestExtend_Range tests the Range method
func TestExtend_Range(t *testing.T) {
	extend := NewExtend(12345)

	// Add multiple key-value pairs
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for k, v := range testData {
		extend.Put([]byte(k), v, 0)
	}

	// Test range iteration
	collectedData := make(map[string][]byte)
	extend.Range(func(key, value []byte) bool {
		collectedData[string(key)] = value
		return true
	})

	if !reflect.DeepEqual(collectedData, testData) {
		t.Errorf("Expected %v, got %v", testData, collectedData)
	}
}

// TestExtend_RangeEarlyExit tests Range with early exit
func TestExtend_RangeEarlyExit(t *testing.T) {
	extend := NewExtend(12345)

	// Add multiple key-value pairs
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		extend.Put(key, value, 0)
	}

	// Test early exit after 2 iterations
	count := 0
	extend.Range(func(key, value []byte) bool {
		count++
		return count < 2
	})

	if count != 2 {
		t.Errorf("Expected 2 iterations, got %d", count)
	}
}

// TestExtend_Merge tests the Merge method
func TestExtend_Merge(t *testing.T) {
	extend1 := NewExtend(12345)
	extend2 := NewExtend(67890)

	// Add data to extend1
	extend1.Put([]byte("key1"), []byte("value1"), 0)
	extend1.Put([]byte("key2"), []byte("value1"), 0)

	// Add data to extend2
	extend2.Put([]byte("key2"), []byte("value2"), 0)
	extend2.Put([]byte("key3"), []byte("value3"), 0)

	// Merge extend2 into extend1 with override
	extend1.Merge(extend2, true)

	// Verify merged data
	expectedData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"), // overridden
		"key3": []byte("value3"),
	}

	collectedData := make(map[string][]byte)
	extend1.Range(func(key, value []byte) bool {
		collectedData[string(key)] = value
		return true
	})

	if !reflect.DeepEqual(collectedData, expectedData) {
		t.Errorf("Expected %v, got %v", expectedData, collectedData)
	}
}

// TestExtend_MergeWithoutOverride tests Merge without override
func TestExtend_MergeWithoutOverride(t *testing.T) {
	extend1 := NewExtend(12345)
	extend2 := NewExtend(67890)

	// Add data to extend1
	extend1.Put([]byte("key1"), []byte("value1"), 0)
	extend1.Put([]byte("key2"), []byte("value1"), 0)

	// Add data to extend2
	extend2.Put([]byte("key2"), []byte("value2"), 0)
	extend2.Put([]byte("key3"), []byte("value3"), 0)

	// Merge extend2 into extend1 without override
	extend1.Merge(extend2, false)

	// Verify merged data (key2 should not be overridden)
	expectedData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value1"), // not overridden
		"key3": []byte("value3"),
	}

	collectedData := make(map[string][]byte)
	extend1.Range(func(key, value []byte) bool {
		collectedData[string(key)] = value
		return true
	})

	if !reflect.DeepEqual(collectedData, expectedData) {
		t.Errorf("Expected %v, got %v", expectedData, collectedData)
	}
}

// TestExtend_Copy tests the Copy method
func TestExtend_Copy(t *testing.T) {
	original := NewExtend(12345)
	original.Put([]byte("key1"), []byte("value1"), 0)
	original.Put([]byte("key2"), []byte("value2"), 0)
	original.Quota = []byte("quota_data")

	// Create a copy
	copied := original.Copy().(*Extend)

	// Verify basic properties
	if copied.GetInode() != original.GetInode() {
		t.Error("Inode should be copied")
	}

	// Verify data is copied
	collectedData := make(map[string][]byte)
	copied.Range(func(key, value []byte) bool {
		collectedData[string(key)] = value
		return true
	})

	expectedData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	if !reflect.DeepEqual(collectedData, expectedData) {
		t.Errorf("Expected %v, got %v", expectedData, collectedData)
	}

	// Verify quota is copied
	if !reflect.DeepEqual(copied.Quota, original.Quota) {
		t.Error("Quota should be copied")
	}

	// Verify independence - modify original
	original.Put([]byte("key3"), []byte("value3"), 0)
	if _, exists := copied.Get([]byte("key3")); exists {
		t.Error("Copy should be independent of original")
	}
}

// TestExtend_Less tests the Less method for btree.Item interface
func TestExtend_Less(t *testing.T) {
	extend1 := NewExtend(100)
	extend2 := NewExtend(200)
	extend3 := NewExtend(100)

	// Test with different inodes
	if !extend1.Less(extend2) {
		t.Error("extend1 should be less than extend2")
	}

	if extend2.Less(extend1) {
		t.Error("extend2 should not be less than extend1")
	}

	// Test with same inodes
	if extend1.Less(extend3) {
		t.Error("extend1 should not be less than extend3 (same inode)")
	}
}

// TestExtend_GetInode tests the GetInode method
func TestExtend_GetInode(t *testing.T) {
	inode := uint64(98765)
	extend := NewExtend(inode)

	if extend.GetInode() != inode {
		t.Errorf("Expected inode %d, got %d", inode, extend.GetInode())
	}
}

// TestExtend_GetMinVerNilMultiSnap tests GetMinVer when multiSnap is nil
func TestExtend_GetMinVerNilMultiSnap(t *testing.T) {
	extend := NewExtend(12345)
	if extend.multiSnap != nil {
		t.Fatal("new extend should have nil multiSnap")
	}
	if got := extend.GetMinVer(); got != 0 {
		t.Errorf("GetMinVer with nil multiSnap: want 0, got %d", got)
	}
}

// TestExtend_QuotaHandling tests quota handling
func TestExtend_QuotaHandling(t *testing.T) {
	extend := NewExtendWithQuota(12345)
	quotaData := []byte("quota_information")
	extend.Quota = quotaData

	// Test serialization and deserialization with quota
	data, err := extend.Bytes()
	if err != nil {
		t.Fatalf("Failed to serialize extend with quota: %v", err)
	}

	deserialized, err := NewExtendFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to deserialize extend with quota: %v", err)
	}

	if !reflect.DeepEqual(deserialized.Quota, quotaData) {
		t.Errorf("Expected quota %v, got %v", quotaData, deserialized.Quota)
	}
}

// TestExtend_EmptyDataMap tests behavior with empty dataMap
func TestExtend_EmptyDataMap(t *testing.T) {
	extend := NewExtend(12345)

	// Test serialization of empty extend
	data, err := extend.Bytes()
	if err != nil {
		t.Fatalf("Failed to serialize empty extend: %v", err)
	}

	deserialized, err := NewExtendFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to deserialize empty extend: %v", err)
	}

	if deserialized.GetInode() != extend.GetInode() {
		t.Error("Inode should be preserved")
	}

	// Test range on empty extend
	count := 0
	deserialized.Range(func(key, value []byte) bool {
		count++
		return true
	})

	if count != 0 {
		t.Error("Empty extend should have no key-value pairs")
	}
}

// TestExtend_ConcurrentAccess tests concurrent access to Extend
func TestExtend_ConcurrentAccess(t *testing.T) {
	extend := NewExtend(12345)

	// Test concurrent Put operations
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := []byte(fmt.Sprintf("key%d", id))
			value := []byte(fmt.Sprintf("value%d", id))
			extend.Put(key, value, 0)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all keys exist
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))

		value, exists := extend.Get(key)
		if !exists {
			t.Errorf("Key %s should exist", key)
		}
		if !reflect.DeepEqual(value, expectedValue) {
			t.Errorf("Expected value %v, got %v", expectedValue, value)
		}
	}
}

// TestExtend_NewExtendFromBytes_InvalidData tests deserialization with invalid data
func TestExtend_NewExtendFromBytes_InvalidData(t *testing.T) {
	// Test with empty data
	_, err := NewExtendFromBytes([]byte{})
	if err == nil {
		t.Error("Should return error for empty data")
	}

	// Test with invalid data
	invalidData := []byte("invalid_data")
	_, err = NewExtendFromBytes(invalidData)
	if err == nil {
		t.Error("Should return error for invalid data")
	}
}
