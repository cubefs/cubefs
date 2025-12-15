package stream

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
)

// hasCreateNewEk returns true if any request has CreateNewEk set.
func hasCreateNewEk(reqs []*ExtentRequest) bool {
	for _, r := range reqs {
		if r != nil && r.ExtentKey == nil && r.CreateNewEk {
			return true
		}
	}
	return false
}

// TestPrepareWriteRequests_CreateNewEkOnSupersetCover
// Scenario: the request range fully covers the current ek and extends to the right
// (start == ekStart && end > ekEnd). Expect CreateNewEk=true to enforce flush and avoid cross-view.
func TestPrepareWriteRequests_CreateNewEkOnSupersetCover(t *testing.T) {
	cache := NewExtentCache(100)

	// Existing ek: fileOffset=1000, size=100 (committed)
	cache.Append(&proto.ExtentKey{
		FileOffset: 1000,
		Size:       100,
	}, true)

	// Request: start at 1000, length 200 (fully covers the 100 and extends right)
	offset := 1000
	size := 200
	data := make([]byte, size)

	reqs := cache.PrepareWriteRequests(offset, size, data)

	// Expect a CreateNewEk=true request to force flush and avoid using an inconsistent view.
	if !hasCreateNewEk(reqs) {
		t.Fatalf("expected CreateNewEk=true for superset cover (start==ekStart && end>ekEnd), but got requests=%v", reqs)
	}
}

// TestPrepareWriteRequests_CoverageMatrix
// Table-driven coverage of various overlap/coverage relations against a single ek,
// verifying whether CreateNewEk should be set.
func TestPrepareWriteRequests_CoverageMatrix(t *testing.T) {
	type tc struct {
		name         string
		ekStart      int
		ekSize       int
		reqStart     int
		reqSize      int
		expectCreate bool
	}
	cases := []tc{
		// A) Completely left of ek, no overlap: hole only, no CreateNewEk
		{name: "LeftNoOverlap", ekStart: 1000, ekSize: 100, reqStart: 800, reqSize: 100, expectCreate: false}, // [800,900) vs [1000,1100)
		// B) Left partial overlap (end within ek): hole + non-hole, not superset, no CreateNewEk
		{name: "LeftPartialOverlap", ekStart: 1000, ekSize: 100, reqStart: 900, reqSize: 150, expectCreate: false}, // [900,1050)
		// C) Cover to ek end (start<ekStart && end==ekEnd): current impl returns a hole and sets CreateNewEk
		{name: "LeftToExactEnd", ekStart: 1000, ekSize: 100, reqStart: 900, reqSize: 200, expectCreate: true}, // [900,1100)
		// D) Cross from left (start<ekStart && end>ekEnd): current impl returns a hole and sets CreateNewEk
		{name: "LeftCrossOver", ekStart: 1000, ekSize: 100, reqStart: 900, reqSize: 300, expectCreate: true}, // [900,1200)
		// E) Inner subset (start==ekStart && end<ekEnd): no CreateNewEk
		{name: "InnerSubset", ekStart: 1000, ekSize: 100, reqStart: 1000, reqSize: 50, expectCreate: false}, // [1000,1050)
		// F) Exact cover (start==ekStart && end==ekEnd): need CreateNewEk (covered by current impl)
		{name: "ExactCover", ekStart: 1000, ekSize: 100, reqStart: 1000, reqSize: 100, expectCreate: true}, // [1000,1100)
		// G) Superset (start==ekStart && end>ekEnd): should need CreateNewEk (guard missing impl)
		{name: "SupersetCover", ekStart: 1000, ekSize: 100, reqStart: 1000, reqSize: 150, expectCreate: true}, // [1000,1150)
		// H) Start inside (ekStart<start<ekEnd && end<=ekEnd): no CreateNewEk
		{name: "InnerStartWithin", ekStart: 1000, ekSize: 100, reqStart: 1020, reqSize: 30, expectCreate: false}, // [1020,1050)
		// I) Start inside and cross right boundary (ekStart<start<ekEnd && end>ekEnd): no CreateNewEk
		{name: "InnerStartCrossRight", ekStart: 1000, ekSize: 100, reqStart: 1050, reqSize: 100, expectCreate: false}, // [1050,1150)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cache := NewExtentCache(101)
			// Insert target ek (committed form to avoid temporary vs committed alternation side-effects)
			cache.Append(&proto.ExtentKey{
				FileOffset: uint64(c.ekStart),
				Size:       uint32(c.ekSize),
			}, true)

			data := make([]byte, c.reqSize)
			reqs := cache.PrepareWriteRequests(c.reqStart, c.reqSize, data)
			got := hasCreateNewEk(reqs)
			if got != c.expectCreate {
				t.Fatalf("case=%s: CreateNewEk mismatch: want %v, got %v; reqs=%v", c.name, c.expectCreate, got, reqs)
			}
		})
	}
}
