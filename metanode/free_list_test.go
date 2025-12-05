package metanode

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreeList_BasicOperations(t *testing.T) {
	fl := newFreeList()

	require.Equal(t, 0, fl.Len())

	// Test Push
	fl.Push(1)
	require.Equal(t, 1, fl.Len())

	fl.Push(2)
	require.Equal(t, 2, fl.Len())

	// Test Pop
	require.Equal(t, uint64(1), fl.Pop())
	require.Equal(t, 1, fl.Len())

	require.Equal(t, uint64(2), fl.Pop())
	require.Equal(t, 0, fl.Len())

	// Test Remove
	fl.Push(3)
	fl.Push(4)
	require.Equal(t, 2, fl.Len())

	fl.Remove(3)
	require.Equal(t, 1, fl.Len())

	require.Equal(t, uint64(4), fl.Pop())
	require.Equal(t, 0, fl.Len())
}

func TestFreeList_EdgeCases(t *testing.T) {
	fl := newFreeList()

	// Test Pop on empty list
	require.Equal(t, uint64(0), fl.Pop())
	require.Equal(t, 0, fl.Len())

	// Test Push with invalid inode (0)
	fl.Push(0)
	require.Equal(t, 0, fl.Len(), "Invalid inode 0 should not be added")

	// Test duplicate Push (should be idempotent)
	fl.Push(1)
	fl.Push(1) // Duplicate
	require.Equal(t, 1, fl.Len(), "Duplicate push should not increase length")

	// Test Remove on non-existent inode
	fl.Remove(999)
	require.Equal(t, 1, fl.Len(), "Removing non-existent inode should not affect length")
}

func TestFreeList_FIFOOrder(t *testing.T) {
	fl := newFreeList()

	// Test FIFO order
	for i := 1; i <= 10; i++ {
		fl.Push(uint64(i))
	}

	require.Equal(t, 10, fl.Len())

	// Pop should return in FIFO order
	for i := 1; i <= 10; i++ {
		require.Equal(t, uint64(i), fl.Pop(), "Pop should maintain FIFO order")
	}

	require.Equal(t, 0, fl.Len())
}

func TestFreeList_RemoveFromDifferentPositions(t *testing.T) {
	fl := newFreeList()

	fl.Push(1)
	fl.Push(2)
	fl.Push(3)

	// Remove from middle
	fl.Remove(2)
	require.Equal(t, 2, fl.Len())
	require.Equal(t, uint64(1), fl.Pop())
	require.Equal(t, uint64(3), fl.Pop())

	// Test remove from front
	fl.Push(4)
	fl.Push(5)
	fl.Remove(4)
	require.Equal(t, uint64(5), fl.Pop())

	// Test remove from back
	fl.Push(6)
	fl.Push(7)
	fl.Remove(7)
	require.Equal(t, uint64(6), fl.Pop())
}

func TestFreeList_ConcurrentOperations(t *testing.T) {
	fl := newFreeList()
	var wg sync.WaitGroup
	numGoroutines := 10
	itemsPerGoroutine := 10

	// Concurrent Push
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(startID int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				id := uint64(startID*itemsPerGoroutine + j + 1)
				fl.Push(id)
			}
		}(i)
	}

	wg.Wait()
	require.Equal(t, numGoroutines*itemsPerGoroutine, fl.Len())

	// Concurrent Pop
	var mu sync.Mutex
	poppedItems := make(map[uint64]bool)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item := fl.Pop()
				if item == 0 {
					break
				}
				mu.Lock()
				poppedItems[item] = true
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	require.Equal(t, numGoroutines*itemsPerGoroutine, len(poppedItems))
}

func TestFreeList_MixedOperations(t *testing.T) {
	fl := newFreeList()

	// Mixed operations sequence
	fl.Push(1)
	fl.Push(2)
	fl.Push(3)
	require.Equal(t, 3, fl.Len())

	// Pop one
	require.Equal(t, uint64(1), fl.Pop())
	require.Equal(t, 2, fl.Len())

	// Remove one
	fl.Remove(3)
	require.Equal(t, 1, fl.Len())

	// Push more
	fl.Push(4)
	fl.Push(5)
	require.Equal(t, 3, fl.Len())

	// Pop all
	require.Equal(t, uint64(2), fl.Pop())
	require.Equal(t, uint64(4), fl.Pop())
	require.Equal(t, uint64(5), fl.Pop())
	require.Equal(t, 0, fl.Len())
}
