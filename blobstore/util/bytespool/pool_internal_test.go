package bytespool

import (
	"fmt"
	"sync"
	"testing"
)

// Lock this mutex to ensure that test cases that call doInit()
// do not run concurrently, avoiding data race on global pools.
var mu sync.Mutex

func TestPowerOfTwoAllocAndFree(t *testing.T) {
	sizes := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 16 << 20}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			mu.Lock()
			defer mu.Unlock()

			doInit()

			buf := Alloc(size)

			if cap(buf) != size {
				t.Fatalf("initial cap mismatch: want %d, got %d", size, cap(buf))
			}

			origPtrStr := fmt.Sprintf("%p", &buf[0])

			Free(buf)

			newBuf := Alloc(size)
			newPtrStr := fmt.Sprintf("%p", &newBuf[0])

			if origPtrStr != newPtrStr {
				t.Errorf("memory not reused: orig=%s, new=%s", origPtrStr, newPtrStr)
			}

			if cap(newBuf) != size {
				t.Errorf("realloc cap mismatch: want %d, got %d", size, cap(newBuf))
			}

			Free(newBuf)
		})
	}
}

func TestPowerOfTwoAllocAndFreePointer(t *testing.T) {
	sizes := []int{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 16 << 20}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			mu.Lock()
			defer mu.Unlock()

			doInit()

			bufPtr := AllocPointer(size)

			if cap(*bufPtr) != size {
				t.Fatalf("initial cap mismatch: want %d, got %d", size, cap(*bufPtr))
			}

			origDataPtrStr := fmt.Sprintf("%p", *bufPtr)
			origHeaderPtrStr := fmt.Sprintf("%p", bufPtr)

			FreePointer(bufPtr)

			newBufPtr := AllocPointer(size)
			newDataPtrStr := fmt.Sprintf("%p", *newBufPtr)
			newHeaderPtrStr := fmt.Sprintf("%p", newBufPtr)

			if origDataPtrStr != newDataPtrStr {
				t.Errorf("memory not reused: orig=%s, new=%s", origDataPtrStr, newDataPtrStr)
			}

			if origHeaderPtrStr != newHeaderPtrStr {
				t.Errorf("slice header not reused: orig=%s, new=%s", origHeaderPtrStr, newHeaderPtrStr)
			}

			if cap(*newBufPtr) != size {
				t.Errorf("realloc cap mismatch: want %d, got %d", size, cap(*newBufPtr))
			}

			FreePointer(newBufPtr)
		})
	}
}

func TestPowerOfTwoExpandFree(t *testing.T) {
	mu.Lock()
	defer mu.Unlock()

	doInit()

	size := 16
	buf := Alloc(size)

	if len(buf) != size {
		t.Errorf("expected len=%d, got %d", size, len(buf))
	}
	if cap(buf) != 16 {
		t.Errorf("expected cap=16, got %d", cap(buf))
	}

	newSize := 128
	buf = make([]byte, newSize)

	if len(buf) != newSize {
		t.Errorf("expected len=%d after expansion, got %d", newSize, len(buf))
	}

	if cap(buf) != newSize {
		t.Errorf("expected cap=%d after expansion, got %d", newSize, cap(buf))
	}
	origDataPtrStr := fmt.Sprintf("%p", &buf[0])

	Free(buf)

	newBuf := Alloc(newSize)
	newDataPtrStr := fmt.Sprintf("%p", &newBuf[0])

	if origDataPtrStr != newDataPtrStr {
		t.Errorf("memory not reused: orig=%s, new=%s", origDataPtrStr, newDataPtrStr)
	}

	if cap(newBuf) != newSize {
		t.Errorf("expected cap=%d after realloc, got %d", newSize, cap(newBuf))
	}

	Free(newBuf)
}
