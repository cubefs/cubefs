package metanode

import (
	"testing"
	"time"
)

func TestForbiddenMigrationList(t *testing.T) {
	expiration := time.Second * 5
	fmList := newForbiddenMigrationList(expiration)

	// Test Put and getAllForbiddenMigrationInodes methods
	fmList.Put(1)
	fmList.Put(2)
	allInos := fmList.getAllForbiddenMigrationInodes(2)
	expectedInos := []uint64{1, 2}
	if !equalSlices(allInos, expectedInos) {
		t.Errorf("getAllForbiddenMigrationInodes: expected %v, got %v", expectedInos, allInos)
	}

	// Test Delete and getAllForbiddenMigrationInodes methods
	fmList.Delete(1)
	allInos = fmList.getAllForbiddenMigrationInodes(2)
	expectedInos = []uint64{2}
	if !equalSlices(allInos, expectedInos) {
		t.Errorf("getAllForbiddenMigrationInodes: expected %v, got %v", expectedInos, allInos)
	}

	// Test getExpiredForbiddenMigrationInodes method
	time.Sleep(expiration) // Wait for expiration
	expiredInos := fmList.getExpiredForbiddenMigrationInodes(2)
	expectedExpiredInos := []uint64{2}
	if !equalSlices(expiredInos, expectedExpiredInos) {
		t.Errorf("getExpiredForbiddenMigrationInodes: expected %v, got %v", expectedExpiredInos, expiredInos)
	}
}

// Helper function to check if two slices are equal
func equalSlices(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
