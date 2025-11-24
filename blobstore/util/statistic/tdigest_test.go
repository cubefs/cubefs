// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package statistic

import (
	"math"
	"math/rand"
	"testing"
)

func TestTDigestQuantileSequential(t *testing.T) {
	td := NewTDigest(100)
	const total = 10000

	for i := 0; i < total; i++ {
		td.Add(uint64(i))
	}

	// Test median
	median := td.Quantile(0.5)
	expected := uint64(5000)
	if absDiff(median, expected) > 50 {
		t.Errorf("median inaccurate: got %d, expected ~%d, diff %d",
			median, expected, absDiff(median, expected))
	}

	// Test p90
	p90 := td.Quantile(0.9)
	expectedP90 := uint64(9000)
	if absDiff(p90, expectedP90) > 100 {
		t.Errorf("p90 inaccurate: got %d, expected ~%d, diff %d",
			p90, expectedP90, absDiff(p90, expectedP90))
	}

	// Test edge cases
	if td.Quantile(0.0) != 0 {
		t.Errorf("min quantile should be 0, got %d", td.Quantile(0.0))
	}
	if td.Quantile(1.0) != total-1 {
		t.Errorf("max quantile should be %d, got %d", total-1, td.Quantile(1.0))
	}
}

// TestTDigestBoundaryValues tests behavior with extreme values
func TestTDigestBoundaryValues(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
	}{
		{
			name:   "small values",
			values: []uint64{1, 2, 3, 4, 5},
		},
		{
			name:   "large values",
			values: []uint64{math.MaxUint64 - 1000, math.MaxUint64 - 500, math.MaxUint64 - 100},
		},
		{
			name:   "mixed range",
			values: []uint64{100, 1000, 1000000, math.MaxUint64 / 2},
		},
		{
			name:   "single value",
			values: []uint64{42},
		},
		{
			name:   "duplicate values",
			values: []uint64{100, 100, 100, 200, 200},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := NewTDigest(100)

			for _, v := range tt.values {
				td.Add(v)
			}

			// Test edge quantiles
			min := td.Quantile(0.0)
			max := td.Quantile(1.0)

			// Find expected min and max
			expectedMin := tt.values[0]
			expectedMax := tt.values[0]
			for _, v := range tt.values {
				if v < expectedMin {
					expectedMin = v
				}
				if v > expectedMax {
					expectedMax = v
				}
			}

			if min != expectedMin {
				t.Errorf("min mismatch: got %d, want %d", min, expectedMin)
			}
			if max != expectedMax {
				t.Errorf("max mismatch: got %d, want %d", max, expectedMax)
			}

			// Test total weight
			if td.TotalWeight() != uint64(len(tt.values)) {
				t.Errorf("total weight mismatch: got %d, want %d",
					td.TotalWeight(), len(tt.values))
			}
		})
	}
}

// TestTDigestSnapshot tests snapshot and restore functionality
func TestTDigestSnapshot(t *testing.T) {
	td := NewTDigest(100)

	// Add some values
	for i := 0; i < 1000; i++ {
		td.Add(uint64(i * 10))
	}

	// Take snapshot
	snapshot := td.Snapshot()

	// Verify snapshot properties
	if snapshot.TotalWeight != 1000 {
		t.Errorf("snapshot weight: got %d, want 1000", snapshot.TotalWeight)
	}
	if snapshot.Min != 0 {
		t.Errorf("snapshot min: got %d, want 0", snapshot.Min)
	}
	if snapshot.Max != 9990 {
		t.Errorf("snapshot max: got %d, want 9990", snapshot.Max)
	}

	// Create new digest and load snapshot
	td2 := NewTDigest(100)
	td2.LoadFromSnapshot(snapshot)

	// Verify loaded digest produces same results
	for _, q := range []float64{0.25, 0.5, 0.75, 0.95} {
		original := td.Quantile(q)
		restored := td2.Quantile(q)
		if original != restored {
			t.Errorf("quantile %.2f mismatch after restore: original=%d, restored=%d",
				q, original, restored)
		}
	}
}

// TestTDigestCompression tests different compression factors
func TestTDigestCompression(t *testing.T) {
	compressions := []uint64{20, 50, 100, 200}
	const numValues = 10000

	values := make([]uint64, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = uint64(i)
	}

	for _, comp := range compressions {
		t.Run(string(rune(comp)), func(t *testing.T) {
			td := NewTDigest(comp)

			for _, v := range values {
				td.Add(v)
			}

			median := td.Quantile(0.5)
			expected := uint64(5000)

			// Higher compression should give better accuracy
			maxError := uint64(200)
			if comp >= 100 {
				maxError = 100
			}

			if absDiff(median, expected) > maxError {
				t.Errorf("compression=%d: median error too large: got %d, want ~%d",
					comp, median, expected)
			}

			t.Logf("compression=%d: median=%d (expected ~%d), centroids=%d",
				comp, median, expected, len(td.Snapshot().Centroids))
		})
	}
}

// TestTDigestConcurrency tests thread-safety with concurrent operations
func TestTDigestConcurrency(t *testing.T) {
	td := NewTDigest(100)
	const goroutines = 10
	const valuesPerGoroutine = 1000

	done := make(chan bool, goroutines)

	// Concurrent adds
	for g := 0; g < goroutines; g++ {
		go func(offset int) {
			for i := 0; i < valuesPerGoroutine; i++ {
				td.Add(uint64(offset*valuesPerGoroutine + i))
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for g := 0; g < goroutines; g++ {
		<-done
	}

	// Verify total weight
	expectedWeight := uint64(goroutines * valuesPerGoroutine)
	if td.TotalWeight() != expectedWeight {
		t.Errorf("concurrent add weight mismatch: got %d, want %d",
			td.TotalWeight(), expectedWeight)
	}

	// Concurrent queries
	for g := 0; g < goroutines; g++ {
		go func() {
			td.Quantile(0.5)
			td.Quantile(0.95)
			done <- true
		}()
	}

	// Wait for all query goroutines
	for g := 0; g < goroutines; g++ {
		<-done
	}
}

// Benchmark tests

func BenchmarkTDigest_Add(b *testing.B) {
	td := NewTDigest(100)
	values := generateTestValues(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		td.Add(values[i])
	}
}

func BenchmarkTDigest_Quantile(b *testing.B) {
	td := NewTDigest(100)
	for i := 0; i < 10000; i++ {
		td.Add(rand.Uint64())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		td.Quantile(0.5)
	}
}

func BenchmarkTDigest_AddAndQuery(b *testing.B) {
	td := NewTDigest(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		td.Add(uint64(i))
		if i%100 == 0 {
			td.Quantile(0.95)
		}
	}
}

func BenchmarkTDigest_Snapshot(b *testing.B) {
	td := NewTDigest(100)
	for i := 0; i < 10000; i++ {
		td.Add(uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = td.Snapshot()
	}
}

// Helper functions

func generateTestValues(count int) []uint64 {
	values := make([]uint64, count)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < count; i++ {
		values[i] = rng.Uint64()
	}
	return values
}
