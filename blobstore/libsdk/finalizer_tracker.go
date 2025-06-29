package main

import (
	"runtime"
	"sync"
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

// FinalizerTracker manages the lifecycle tracking of any instances.
// NOTE: Only for testing purpose
type FinalizerTracker struct {
	mu           sync.Mutex
	finalizedMap map[uintptr]bool
}

// NewFinalizerTracker creates and returns a new instance of FinalizerTracker.
func NewFinalizerTracker() *FinalizerTracker {
	return &FinalizerTracker{
		finalizedMap: make(map[uintptr]bool),
	}
}

// Track sets up the finalizer for the given object and starts tracking it.
func (ft *FinalizerTracker) Track(obj *blobStorageWrapper) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Get handle from obj
	handle := ft.GetHandle(obj)

	log.Debug("Track: obj=0x%x, fHandle=%d\n", obj, handle)
	// Record initial state as not finalized
	ft.finalizedMap[handle] = false

	// Set finalizer
	runtime.SetFinalizer(obj, func(o *blobStorageWrapper) {
		log.Debug("Finalizer: obj=0x%x\n", o)
		ft.MarkFinalized(o)
	})
}

// MarkFinalized marks the given object as finalized in the tracking map.
func (ft *FinalizerTracker) MarkFinalized(obj *blobStorageWrapper) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	handle := ft.GetHandle(obj)
	ft.finalizedMap[handle] = true

	log.Debug("MarkFinalized: obj=0x%x, fHandle=%d\n", obj, handle)
}

// WasFinalized checks if the given object has been marked as finalized.
func (ft *FinalizerTracker) WasFinalized(handle uintptr) bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	wasFinalized, exists := ft.finalizedMap[handle]
	if !exists {
		return false
	}
	return wasFinalized
}

// GetHandle returns the handle of an object based on its pointer address.
func (ft *FinalizerTracker) GetHandle(obj *blobStorageWrapper) uintptr {
	return uintptr(unsafe.Pointer(obj))
}

// Reset clears all internal state.
func (ft *FinalizerTracker) Reset() {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	ft.finalizedMap = make(map[uintptr]bool)
}
