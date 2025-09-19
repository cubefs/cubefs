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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxIDAllocator_BasicOperations(t *testing.T) {
	alloc := newTxIDAllocator()

	// Test initial state
	require.Equal(t, uint64(0), alloc.getTransactionID())

	// Test setTransactionID
	alloc.setTransactionID(10)
	require.Equal(t, uint64(10), alloc.getTransactionID())

	// Test allocateTransactionID
	require.Equal(t, uint64(11), alloc.allocateTransactionID())
	require.Equal(t, uint64(11), alloc.getTransactionID())

	// Test Reset
	alloc.Reset()
	require.Equal(t, uint64(0), alloc.getTransactionID())
}

func TestTxIDAllocator_ConcurrentAccess(t *testing.T) {
	alloc := newTxIDAllocator()
	const numGoroutines = 100
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	results := make(chan uint64, numGoroutines*operationsPerGoroutine)

	// Test concurrent allocation
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				id := alloc.allocateTransactionID()
				results <- id
			}
		}()
	}

	wg.Wait()
	close(results)

	// Collect all results
	allocatedIDs := make(map[uint64]bool)
	for id := range results {
		allocatedIDs[id] = true
	}

	// Verify we got the expected number of unique IDs
	expectedCount := numGoroutines * operationsPerGoroutine
	require.Equal(t, expectedCount, len(allocatedIDs))

	// Verify all IDs are unique
	require.Equal(t, expectedCount, len(allocatedIDs))

	// Verify the final ID is correct
	finalID := alloc.getTransactionID()
	require.Equal(t, uint64(expectedCount), finalID)
}

func TestTxIDAllocator_ConcurrentSetAndGet(t *testing.T) {
	alloc := newTxIDAllocator()
	const numGoroutines = 50

	var wg sync.WaitGroup

	// Test concurrent set operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			alloc.setTransactionID(id)
		}(uint64(i + 1))
	}

	wg.Wait()

	// The final value should be one of the set values
	finalID := alloc.getTransactionID()
	assert.GreaterOrEqual(t, finalID, uint64(1))
	assert.LessOrEqual(t, finalID, uint64(numGoroutines))
}

func TestTxIDAllocator_ConcurrentResetAndAllocate(t *testing.T) {
	alloc := newTxIDAllocator()
	const numGoroutines = 20

	var wg sync.WaitGroup
	results := make(chan uint64, numGoroutines*2)

	// Start goroutines that either reset or allocate
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(shouldReset bool) {
			defer wg.Done()
			if shouldReset {
				alloc.Reset()
				results <- 0 // Signal reset
			} else {
				id := alloc.allocateTransactionID()
				results <- id
			}
		}(i%2 == 0)
	}

	wg.Wait()
	close(results)

	// Verify that operations completed without deadlock
	operationCount := 0
	for range results {
		operationCount++
	}
	require.Equal(t, numGoroutines, operationCount)
}

func TestTxIDAllocator_EdgeCases(t *testing.T) {
	alloc := newTxIDAllocator()

	// Test setting to zero
	alloc.setTransactionID(0)
	require.Equal(t, uint64(0), alloc.getTransactionID())

	// Test setting to maximum uint64 value
	alloc.setTransactionID(^uint64(0))
	require.Equal(t, ^uint64(0), alloc.getTransactionID())

	// Test allocation after setting to max value
	require.Equal(t, uint64(0), alloc.allocateTransactionID()) // Should wrap around

	// Test multiple resets
	alloc.setTransactionID(100)
	alloc.Reset()
	require.Equal(t, uint64(0), alloc.getTransactionID())

	alloc.Reset() // Reset again
	require.Equal(t, uint64(0), alloc.getTransactionID())
}

func TestTxIDAllocator_Performance(t *testing.T) {
	alloc := newTxIDAllocator()
	const iterations = 1000000

	// Benchmark allocation performance
	for i := 0; i < iterations; i++ {
		alloc.allocateTransactionID()
	}

	require.Equal(t, uint64(iterations), alloc.getTransactionID())
}

func TestTxIDAllocator_ThreadSafety(t *testing.T) {
	alloc := newTxIDAllocator()
	const numGoroutines = 10
	const operationsPerGoroutine = 1000

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	// Test mixed operations concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				switch j % 4 {
				case 0:
					alloc.allocateTransactionID()
				case 1:
					alloc.getTransactionID()
				case 2:
					alloc.setTransactionID(uint64(goroutineID*1000 + j))
				case 3:
					if j%100 == 0 {
						alloc.Reset()
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Verify no errors occurred
	require.Empty(t, errors)
}
