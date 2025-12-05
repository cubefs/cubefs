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
	"sync"
	"testing"
	"time"
)

func TestUniqOpLegal(t *testing.T) {
	checker := newUniqChecker()
	for i := 1; i <= 10; i++ {
		if !checker.legalIn(uint64(i), 1) {
			t.Errorf("failed")
		}
	}
	if checker.legalIn(1, 2) {
		t.Errorf("failed, %v", checker.op)
	}
}

func TestUniqOpQueue(t *testing.T) {
	q := newUniqOpQueue()
	for i := uint64(0); i < 10000; i++ {
		q.append(&uniqOp{
			uniqid: i,
		})
	}

	cnt := uint64(0)
	q.scan(func(op *uniqOp) bool {
		if op.uniqid != cnt {
			t.Fatalf("op queue scan failed")
		}
		cnt++
		return true
	})
	if cnt != 10000 {
		t.Fatalf("scan failed %v", cnt)
	}

	op := q.index(4567)
	if op.uniqid != 4567 {
		t.Fatalf("q.index 4567 failed")
	}

	q.truncate(4567)

	op = q.index(0)
	if op.uniqid != 4568 {
		t.Fatalf("q.index 4568 failed")
	}

	if q.len() != 10000-4567-1 {
		t.Fatalf("op queue trancate failed")
	}

	q.scan(func(op *uniqOp) bool {
		if op.uniqid != 4568 {
			t.Fatalf("op queue trancate scan failed")
		}
		return false
	})

	clone := q.clone()
	for i := uint64(10000); i < 20000; i++ {
		q.append(&uniqOp{uniqid: i})
	}

	if q.len()-clone.len() != 10000 || q.index(1234).uniqid != clone.index(1234).uniqid {
		t.Fatalf("op queue clone failed")
	}

	q.reset()
	if q.len() != 0 || len(q.cur.s) != 0 || len(q.ss) != 1 {
		t.Fatalf("op queue trancate failed")
	}
}

func TestUniqOpClone(t *testing.T) {
	checker := newUniqChecker()
	for i := 1; i <= 10000; i++ {
		checker.legalIn(uint64(i), 1)
	}

	checker1 := checker.clone()
	if len(checker1.op) != 0 || checker.inQue.len() != checker1.inQue.len() {
		t.Errorf("failed")
	}

	i := 0
	checker.inQue.scan(func(op *uniqOp) bool {
		if op.uniqid != checker1.inQue.index(i).uniqid || op.atime != checker1.inQue.index(i).atime {
			t.Errorf("failed")
			return false
		}
		i++
		return true
	})
}

func TestUniqOpMarshal(t *testing.T) {
	checker := newUniqChecker()
	for i := 1; i <= 10000; i++ {
		checker.legalIn(uint64(i), 1)
	}

	bts, _, _ := checker.Marshal(checkerVersionV1)
	checker1 := newUniqChecker()
	checker1.UnMarshal(bts)

	if len(checker.op) != len(checker1.op) || checker.inQue.len() != checker1.inQue.len() {
		t.Errorf("failed")
	}

	i := 0
	checker.inQue.scan(func(v *uniqOp) bool {
		if v.uniqid != checker1.inQue.index(i).uniqid || v.atime != checker1.inQue.index(i).atime {
			t.Errorf("failed, id(%v, %v), atime(%v, %v)", v.uniqid, checker1.inQue.index(i).uniqid, v.atime, checker1.inQue.index(i).atime)
			return false
		}

		if _, ok := checker1.op[v.uniqid]; !ok {
			t.Errorf("failed, %v, %v", checker.op[v.uniqid], checker1.op[v.uniqid])
			return false
		}
		i++
		return true
	})
}

// TestUniqOpEdgeCases tests edge cases and boundary conditions
func TestUniqOpEdgeCases(t *testing.T) {
	checker := newUniqChecker()

	// Test zero uniqid (should always be legal)
	if !checker.legalIn(0, 1) {
		t.Error("Zero uniqid should always be legal")
	}

	// Test duplicate zero uniqid
	if !checker.legalIn(0, 2) {
		t.Error("Duplicate zero uniqid should still be legal")
	}

	// Test very large uniqid
	largeID := uint64(1<<63 - 1)
	if !checker.legalIn(largeID, 1) {
		t.Error("Large uniqid should be legal")
	}

	// Test duplicate large uniqid
	if checker.legalIn(largeID, 2) {
		t.Error("Duplicate large uniqid should not be legal")
	}
}

// TestUniqOpConcurrentAccess tests concurrent access to uniqChecker
func TestUniqOpConcurrentAccess(t *testing.T) {
	checker := newUniqChecker()
	var wg sync.WaitGroup
	numGoroutines := 100
	idsPerGoroutine := 10

	// Test concurrent legalIn calls
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(startID int) {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				id := uint64(startID*idsPerGoroutine + j + 1)
				checker.legalIn(id, id)
			}
		}(i)
	}

	wg.Wait()

	// Verify all IDs were processed
	expectedCount := numGoroutines * idsPerGoroutine
	if len(checker.op) != expectedCount {
		t.Errorf("Expected %d operations, got %d", expectedCount, len(checker.op))
	}
}

// TestUniqOpEvictionLogic tests the eviction mechanism
func TestUniqOpEvictionLogic(t *testing.T) {
	checker := newUniqChecker()

	// Add operations beyond keepOps limit
	for i := 1; i <= 2000; i++ {
		checker.legalIn(uint64(i), uint64(i))
	}

	// Test evictIndex - should return valid indices since we exceed keepOps (1024)
	left, idx, op := checker.evictIndex()
	if left < 0 {
		t.Errorf("evictIndex should return valid left count, got %d", left)
	}

	// For recent operations, idx will be -1 and op will be nil because
	// no operations are old enough to evict (all are recent)
	if idx == -1 && op == nil {
		t.Log("No operations old enough to evict (expected for recent operations)")
	} else {
		t.Errorf("Expected idx=-1 and op=nil for recent operations, got idx=%d, op=%v", idx, op)
	}

	// Test that evictIndex returns the correct queue length
	expectedLeft := checker.inQue.len()
	if left != expectedLeft {
		t.Errorf("Expected left count %d, got %d", expectedLeft, left)
	}

	// Test doEvict with a valid operation ID
	if op != nil {
		originalLen := checker.inQue.len()
		checker.doEvict(op.uniqid)
		newLen := checker.inQue.len()
		if newLen >= originalLen {
			t.Error("doEvict should reduce queue length")
		}
	}
}

// TestUniqOpEvictionWithOldOperations tests eviction with old operations
func TestUniqOpEvictionWithOldOperations(t *testing.T) {
	checker := newUniqChecker()

	// Create operations with old timestamps (older than opKeepTime = 300 seconds)
	oldTime := time.Now().Unix() - 400 // 400 seconds ago

	// Manually add operations with old timestamps
	for i := 1; i <= 1500; i++ {
		checker.Lock()
		op := &uniqOp{uniqid: uint64(i), atime: oldTime, applyId: uint64(i)}
		checker.op[uint64(i)] = op
		checker.inQue.append(op)
		checker.Unlock()
	}

	// Test evictIndex with old operations
	left, idx, op := checker.evictIndex()
	if left < 0 {
		t.Errorf("evictIndex should return valid left count, got %d", left)
	}
	if idx < 0 {
		t.Errorf("evictIndex should return valid index, got %d", idx)
	}

	if op == nil {
		t.Error("evictIndex should return an operation to evict for old operations")
	}

	// Test doEvict
	if op != nil {
		originalLen := checker.inQue.len()
		checker.doEvict(op.uniqid)
		newLen := checker.inQue.len()
		if newLen >= originalLen {
			t.Error("doEvict should reduce queue length")
		}
	}
}

// TestUniqOpMarshalUnmarshalEdgeCases tests edge cases in marshaling
func TestUniqOpMarshalUnmarshalEdgeCases(t *testing.T) {
	// Test empty checker
	checker := newUniqChecker()
	buf, crc, err := checker.Marshal(checkerVersionV1)
	if err != nil {
		t.Errorf("Marshal empty checker failed: %v", err)
	}
	if len(buf) == 0 {
		t.Error("Marshal should return non-empty buffer")
	}
	if crc == 0 {
		t.Error("CRC should not be zero")
	}

	// Test unmarshal empty data
	checker2 := newUniqChecker()
	err = checker2.UnMarshal([]byte{})
	if err == nil {
		t.Error("UnMarshal empty data should return error")
	}

	// Test unmarshal invalid data
	err = checker2.UnMarshal([]byte{1, 2, 3})
	if err == nil {
		t.Error("UnMarshal invalid data should return error")
	}
}

// TestUniqOpQueueEdgeCases tests edge cases for uniqOpQueue
func TestUniqOpQueueEdgeCases(t *testing.T) {
	q := newUniqOpQueue()

	// Test index with invalid indices
	if q.index(-1) != nil {
		t.Error("index(-1) should return nil")
	}
	if q.index(1000) != nil {
		t.Error("index(1000) on empty queue should return nil")
	}

	// Test truncate with invalid indices
	q.truncate(-1)
	if q.len() != 0 {
		t.Error("truncate(-1) should not affect empty queue")
	}

	q.truncate(1000)
	if q.len() != 0 {
		t.Error("truncate(1000) on empty queue should not affect length")
	}

	// Add some operations
	for i := 0; i < 5; i++ {
		q.append(&uniqOp{uniqid: uint64(i), atime: time.Now().Unix()})
	}

	// Test truncate beyond queue length
	_ = q.len()
	q.truncate(10)
	if q.len() != 0 {
		t.Error("truncate beyond queue length should reset queue")
	}

	// Rebuild queue
	for i := 0; i < 5; i++ {
		q.append(&uniqOp{uniqid: uint64(i), atime: time.Now().Unix()})
	}

	// Test truncate at exact length
	q.truncate(4)
	if q.len() != 0 {
		t.Error("truncate at exact length should reset queue")
	}
}

// TestUniqOpQueueScan tests the scan functionality
func TestUniqOpQueueScan(t *testing.T) {
	q := newUniqOpQueue()

	// Test scan on empty queue
	scanCount := 0
	q.scan(func(op *uniqOp) bool {
		scanCount++
		return true
	})
	if scanCount != 0 {
		t.Error("Scan on empty queue should not call function")
	}

	// Add operations
	for i := 0; i < 10; i++ {
		q.append(&uniqOp{uniqid: uint64(i), atime: time.Now().Unix()})
	}

	// Test scan with early termination
	scanCount = 0
	q.scan(func(op *uniqOp) bool {
		scanCount++
		return scanCount < 5 // Stop after 5 iterations
	})
	if scanCount != 5 {
		t.Errorf("Scan should stop after 5 iterations, got %d", scanCount)
	}
}

// TestUniqOpQueueClone tests the clone functionality
func TestUniqOpQueueClone(t *testing.T) {
	q := newUniqOpQueue()

	// Test clone of empty queue
	clone := q.clone()
	if clone.len() != 0 {
		t.Error("Clone of empty queue should have length 0")
	}

	// Add operations
	for i := 0; i < 100; i++ {
		q.append(&uniqOp{uniqid: uint64(i), atime: time.Now().Unix()})
	}

	// Test clone of non-empty queue
	clone = q.clone()
	if clone.len() != q.len() {
		t.Error("Clone should have same length as original")
	}

	// Verify clone is independent
	for i := 0; i < 10; i++ {
		q.append(&uniqOp{uniqid: uint64(100 + i), atime: time.Now().Unix()})
	}

	if clone.len() == q.len() {
		t.Error("Clone should be independent of original")
	}
}

// TestUniqOpQueueReset tests the reset functionality
func TestUniqOpQueueReset(t *testing.T) {
	q := newUniqOpQueue()

	// Add operations
	for i := 0; i < 50; i++ {
		q.append(&uniqOp{uniqid: uint64(i), atime: time.Now().Unix()})
	}

	// Test reset
	q.reset()
	if q.len() != 0 {
		t.Error("Reset should set length to 0")
	}
	if len(q.ss) != 1 {
		t.Error("Reset should have exactly one slice")
	}
	if len(q.cur.s) != 0 {
		t.Error("Reset should have empty current slice")
	}
}

// TestUniqOpStats tests the getStats functionality
func TestUniqOpStats(t *testing.T) {
	checker := newUniqChecker()

	// Test stats on empty checker
	stats := checker.getStats()
	if stats["queue_length"].(int) != 0 {
		t.Error("Empty checker should have queue length 0")
	}
	if stats["map_size"].(int) != 0 {
		t.Error("Empty checker should have map size 0")
	}

	// Add some operations
	for i := 1; i <= 100; i++ {
		checker.legalIn(uint64(i), uint64(i))
	}

	// Test stats on non-empty checker
	stats = checker.getStats()
	if stats["queue_length"].(int) != 100 {
		t.Errorf("Checker should have queue length 100, got %d", stats["queue_length"].(int))
	}
	if stats["map_size"].(int) != 100 {
		t.Errorf("Checker should have map size 100, got %d", stats["map_size"].(int))
	}
}

// TestUniqOpCloneIndependence tests that clone is independent
func TestUniqOpCloneIndependence(t *testing.T) {
	checker := newUniqChecker()

	// Add operations to original
	for i := 1; i <= 50; i++ {
		checker.legalIn(uint64(i), uint64(i))
	}

	// Clone checker
	checkerClone := checker.clone()

	// Add operations to original
	for i := 51; i <= 100; i++ {
		checker.legalIn(uint64(i), uint64(i))
	}

	// Verify clone is independent
	if len(checkerClone.op) != 0 {
		t.Error("Clone should have empty op map")
	}
	if checkerClone.inQue.len() != 50 {
		t.Error("Clone should have 50 operations in queue")
	}
	if checker.inQue.len() != 100 {
		t.Error("Original should have 100 operations in queue")
	}
}

// TestUniqOpMarshalUnmarshalRoundTrip tests round-trip marshaling
func TestUniqOpMarshalUnmarshalRoundTrip(t *testing.T) {
	checker := newUniqChecker()

	// Add operations
	for i := 1; i <= 100; i++ {
		checker.legalIn(uint64(i), uint64(i))
	}

	// Marshal
	buf, crc, err := checker.Marshal(checkerVersionV1)
	if err != nil {
		t.Errorf("Marshal failed: %v", err)
	}

	// Unmarshal
	checker2 := newUniqChecker()
	err = checker2.UnMarshal(buf)
	if err != nil {
		t.Errorf("Unmarshal failed: %v", err)
	}

	// Verify round-trip
	if len(checker.op) != len(checker2.op) {
		t.Error("Round-trip should preserve op map size")
	}
	if checker.inQue.len() != checker2.inQue.len() {
		t.Error("Round-trip should preserve queue length")
	}

	// Verify CRC
	buf2, crc2, err := checker2.Marshal(checkerVersionV1)
	if err != nil {
		t.Errorf("Second marshal failed: %v", err)
	}
	if crc != crc2 {
		t.Error("CRC should be consistent across round-trip")
	}
	if len(buf) != len(buf2) {
		t.Error("Buffer length should be consistent across round-trip")
	}
}

// TestUniqOpLargeScale tests large-scale operations
func TestUniqOpLargeScale(t *testing.T) {
	checker := newUniqChecker()

	// Add large number of operations
	numOps := 10000
	for i := 1; i <= numOps; i++ {
		if !checker.legalIn(uint64(i), uint64(i)) {
			t.Errorf("Failed to add operation %d", i)
		}
	}

	// Verify all operations were added
	if len(checker.op) != numOps {
		t.Errorf("Expected %d operations, got %d", numOps, len(checker.op))
	}
	if checker.inQue.len() != numOps {
		t.Errorf("Expected queue length %d, got %d", numOps, checker.inQue.len())
	}

	// Test duplicate operations
	for i := 1; i <= 100; i++ {
		if checker.legalIn(uint64(i), uint64(i+numOps)) {
			t.Errorf("Duplicate operation %d should not be legal", i)
		}
	}
}

// TestUniqOpQueueLargeScale tests large-scale queue operations
func TestUniqOpQueueLargeScale(t *testing.T) {
	q := newUniqOpQueue()

	// Add large number of operations
	numOps := 50000
	for i := 0; i < numOps; i++ {
		q.append(&uniqOp{uniqid: uint64(i), atime: time.Now().Unix()})
	}

	// Verify length
	if q.len() != numOps {
		t.Errorf("Expected length %d, got %d", numOps, q.len())
	}

	// Test random access
	for i := 0; i < 100; i++ {
		idx := i * 500
		op := q.index(idx)
		if op == nil {
			t.Errorf("Failed to get operation at index %d", idx)
		} else if op.uniqid != uint64(idx) {
			t.Errorf("Expected uniqid %d at index %d, got %d", idx, idx, op.uniqid)
		}
	}

	// Test truncate
	truncateIdx := 10000
	q.truncate(truncateIdx)
	expectedLen := numOps - truncateIdx - 1
	if q.len() != expectedLen {
		t.Errorf("After truncate, expected length %d, got %d", expectedLen, q.len())
	}

	// Verify first operation after truncate
	firstOp := q.index(0)
	if firstOp == nil {
		t.Error("First operation after truncate should not be nil")
	} else if firstOp.uniqid != uint64(truncateIdx+1) {
		t.Errorf("Expected first uniqid %d after truncate, got %d", truncateIdx+1, firstOp.uniqid)
	}
}

// TestUniqOpConcurrentMarshal tests concurrent marshaling
func TestUniqOpConcurrentMarshal(t *testing.T) {
	checker := newUniqChecker()

	// Add operations
	for i := 1; i <= 1000; i++ {
		checker.legalIn(uint64(i), uint64(i))
	}

	// Test concurrent marshaling
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf, crc, err := checker.Marshal(checkerVersionV1)
			if err != nil {
				t.Errorf("Concurrent marshal failed: %v", err)
			}
			if len(buf) == 0 {
				t.Error("Marshal should return non-empty buffer")
			}
			if crc == 0 {
				t.Error("CRC should not be zero")
			}
		}()
	}

	wg.Wait()
}

// TestUniqOpConcurrentLegalIn tests concurrent legalIn operations
func TestUniqOpConcurrentLegalIn(t *testing.T) {
	checker := newUniqChecker()
	var wg sync.WaitGroup
	numGoroutines := 50
	idsPerGoroutine := 20

	// Test concurrent legalIn with overlapping IDs
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(startID int) {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				id := uint64(startID*idsPerGoroutine + j + 1)
				checker.legalIn(id, id)
			}
		}(i)
	}

	wg.Wait()

	// Verify all unique IDs were processed
	expectedCount := numGoroutines * idsPerGoroutine
	if len(checker.op) != expectedCount {
		t.Errorf("Expected %d operations, got %d", expectedCount, len(checker.op))
	}

	// Test that duplicates are properly rejected
	duplicateCount := 0
	for i := 1; i <= 100; i++ {
		if checker.legalIn(uint64(i), uint64(i+expectedCount)) {
			duplicateCount++
		}
	}
	if duplicateCount != 0 {
		t.Errorf("Expected 0 duplicate operations to be legal, got %d", duplicateCount)
	}
}

func TestDoEvictBasic(t *testing.T) {
	checker := newUniqChecker()
	for i := 1; i <= 10; i++ {
		checker.legalIn(uint64(i), 1)
	}
	if checker.inQue.len() != 10 {
		t.Fatalf("unexpected queue len: %d", checker.inQue.len())
	}
	if len(checker.op) != 10 {
		t.Fatalf("unexpected map len: %d", len(checker.op))
	}

	// evict until uniqid == 5 (inclusive)
	checker.doEvict(5)
	if checker.inQue.len() != 5 {
		t.Fatalf("after evict 5, unexpected queue len: %d", checker.inQue.len())
	}
	if checker.inQue.index(0) == nil || checker.inQue.index(0).uniqid != 6 {
		t.Fatalf("after evict 5, head uniqid should be 6, got: %v", checker.inQue.index(0))
	}
	for i := 1; i <= 5; i++ {
		if _, ok := checker.op[uint64(i)]; ok {
			t.Fatalf("uniqid %d should be evicted from map", i)
		}
	}
	for i := 6; i <= 10; i++ {
		if _, ok := checker.op[uint64(i)]; !ok {
			t.Fatalf("uniqid %d should remain in map", i)
		}
	}

	// evict a non-existent key should not change anything
	prevLen := checker.inQue.len()
	checker.doEvict(3)
	if checker.inQue.len() != prevLen {
		t.Fatalf("evict non-existent should not change queue len, got: %d", checker.inQue.len())
	}
	checker.doEvict(1000)
	if checker.inQue.len() != prevLen {
		t.Fatalf("evict non-existent(1000) should not change queue len, got: %d", checker.inQue.len())
	}
}

func TestDoEvictWithRebuild(t *testing.T) {
	checker := newUniqChecker()
	for i := 1; i <= 10; i++ {
		checker.legalIn(uint64(i), 1)
	}
	// force rebuild path
	checker.rtime = 0

	// evict through uniqid == 7 (inclusive), remain 8..10
	checker.doEvict(7)
	if checker.inQue.len() != 3 {
		t.Fatalf("after evict 7, unexpected queue len: %d", checker.inQue.len())
	}
	want := []uint64{8, 9, 10}
	for idx, id := range want {
		if checker.inQue.index(idx) == nil || checker.inQue.index(idx).uniqid != id {
			t.Fatalf("after evict 7, idx %d want %d got %v", idx, id, checker.inQue.index(idx))
		}
		if _, ok := checker.op[id]; !ok {
			t.Fatalf("after evict 7, map missing uniqid %d", id)
		}
	}
	if len(checker.op) != 3 {
		t.Fatalf("after evict 7, unexpected map len: %d", len(checker.op))
	}
}
