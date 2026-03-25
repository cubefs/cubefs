// Copyright 2024 The CubeFS Authors.
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

package meta

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDirtyInodeCacheMark verifies that a marked inode is reported as dirty.
func TestDirtyInodeCacheMark(t *testing.T) {
	dc := newDirtyInodeCache(DirtyInodeTTL, MaxDirtyInodeCache)

	var ino uint64 = 100
	assert.False(t, dc.isDirty(ino), "inode should not be dirty before mark")

	dc.mark(ino)
	assert.True(t, dc.isDirty(ino), "inode should be dirty after mark")
}

// TestDirtyInodeCacheUnmarkedInode verifies that an unmarked inode is not dirty.
func TestDirtyInodeCacheUnmarkedInode(t *testing.T) {
	dc := newDirtyInodeCache(DirtyInodeTTL, MaxDirtyInodeCache)

	dc.mark(100)
	assert.False(t, dc.isDirty(200), "unmarked inode should not be dirty")
	assert.False(t, dc.isDirty(0), "inode 0 should not be dirty")
}

// TestDirtyInodeCacheTTLExpiry verifies that a dirty entry expires after TTL.
// TTL uses second-level granularity (timeutil.GetCurrentTimeUnix), so we use
// a 2-second TTL and sleep for 3 seconds to ensure reliable expiry detection.
func TestDirtyInodeCacheTTLExpiry(t *testing.T) {
	ttl := 2 * time.Second
	dc := newDirtyInodeCache(ttl, MaxDirtyInodeCache)

	var ino uint64 = 42
	dc.mark(ino)
	assert.True(t, dc.isDirty(ino), "inode should be dirty immediately after mark")

	time.Sleep(4 * time.Second)
	assert.False(t, dc.isDirty(ino), "inode should not be dirty after TTL expires")
}

// TestDirtyInodeCacheMarkRefreshesTTL verifies that re-marking an inode resets its TTL.
// TTL uses second-level granularity (timeutil.GetCurrentTimeUnix), so we use a 2-second
// TTL with 1-second phase sleeps to ensure reliable boundary detection.
func TestDirtyInodeCacheMarkRefreshesTTL(t *testing.T) {
	ttl := 2 * time.Second
	dc := newDirtyInodeCache(ttl, MaxDirtyInodeCache)

	var ino uint64 = 77
	dc.mark(ino)

	// Sleep for 1s (< TTL), then re-mark to refresh the expiry
	time.Sleep(time.Second)
	dc.mark(ino)

	// Sleep for 1s — original entry would have expired,
	// but the refreshed one should still be valid (refreshed TTL = now+2s)
	time.Sleep(time.Second)
	assert.True(t, dc.isDirty(ino), "inode should still be dirty after TTL refresh")

	// Sleep until refreshed TTL also expires (need > 2s more)
	time.Sleep(4 * time.Second)
	assert.False(t, dc.isDirty(ino), "inode should not be dirty after refreshed TTL expires")
}

// TestDirtyInodeCacheCapacityEviction verifies that the oldest entry is evicted
// when the capacity limit is exceeded.
func TestDirtyInodeCacheCapacityEviction(t *testing.T) {
	maxElements := 5
	dc := newDirtyInodeCache(DirtyInodeTTL, maxElements)

	// Fill to capacity
	for i := uint64(1); i <= uint64(maxElements); i++ {
		dc.mark(i)
	}
	for i := uint64(1); i <= uint64(maxElements); i++ {
		assert.True(t, dc.isDirty(i), "inode %d should be dirty", i)
	}

	// Insert one more — should evict the LRU entry (inode 1, inserted first)
	dc.mark(uint64(maxElements + 1))

	assert.False(t, dc.isDirty(1), "oldest inode should be evicted after capacity exceeded")
	assert.True(t, dc.isDirty(uint64(maxElements+1)), "newly marked inode should be dirty")
}

// TestDirtyInodeCacheMultipleInodes verifies independent tracking of multiple inodes.
func TestDirtyInodeCacheMultipleInodes(t *testing.T) {
	dc := newDirtyInodeCache(DirtyInodeTTL, MaxDirtyInodeCache)

	inodes := []uint64{1, 2, 3, 100, 999}
	for _, ino := range inodes {
		dc.mark(ino)
	}

	for _, ino := range inodes {
		assert.True(t, dc.isDirty(ino), "inode %d should be dirty", ino)
	}
	assert.False(t, dc.isDirty(4), "unmarked inode 4 should not be dirty")
}

// TestDirtyInodeCacheConcurrent verifies concurrent safety of mark and isDirty.
func TestDirtyInodeCacheConcurrent(t *testing.T) {
	dc := newDirtyInodeCache(DirtyInodeTTL, MaxDirtyInodeCache)

	const goroutines = 20
	const inodesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			base := uint64(g * inodesPerGoroutine)
			for i := uint64(0); i < inodesPerGoroutine; i++ {
				dc.mark(base + i)
				dc.isDirty(base + i)
			}
		}()
	}
	wg.Wait()
	// No race condition or panic means the test passes
}

// TestDirtyInodeCacheNearReadCondition verifies the end-to-end condition used
// in sendToMetaPartitionLeader: any dirty inode in a batch suppresses nearRead.
func TestDirtyInodeCacheNearReadCondition(t *testing.T) {
	dc := newDirtyInodeCache(DirtyInodeTTL, MaxDirtyInodeCache)

	dc.mark(10)
	dc.mark(20)

	// Simulate the loop in sendToMetaPartitionLeader
	checkDirty := func(inodes []uint64) bool {
		for _, ino := range inodes {
			if dc.isDirty(ino) {
				return true
			}
		}
		return false
	}

	// Batch containing a dirty inode → nearRead should be skipped
	assert.True(t, checkDirty([]uint64{5, 10, 15}), "batch with dirty inode should suppress nearRead")

	// Batch with no dirty inodes → nearRead is allowed
	assert.False(t, checkDirty([]uint64{1, 2, 3}), "batch without dirty inodes should allow nearRead")

	// Empty batch → no nearRead suppression
	assert.False(t, checkDirty([]uint64{}), "empty batch should allow nearRead")
}
