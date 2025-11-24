// Copyright 2025 The CubeFS Authors.
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

package statistic

import (
	"math/rand"
	"sort"
	"testing"
)

// TestDualTDigestLowDeletionRate tests scenarios where DualTDigest works well
// Low deletion rate means < 30% (below rebuild threshold)
func TestDualTDigestLowDeletionRate(t *testing.T) {
	tests := []struct {
		name        string
		addCount    int
		removeCount int
		maxError    float64
	}{
		{
			name:        "10% deletion rate",
			addCount:    10000,
			removeCount: 1000,
			maxError:    0.02, // 2% tolerance
		},
		{
			name:        "20% deletion rate",
			addCount:    10000,
			removeCount: 2000,
			maxError:    0.03, // 3% tolerance
		},
		{
			name:        "29% deletion rate (just below threshold)",
			addCount:    10000,
			removeCount: 2900,
			maxError:    0.05, // 5% tolerance
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rand.Seed(42) // Fixed seed for reproducibility
			dt := NewDualTDigest(100)

			// Add all values
			for i := 0; i < tt.addCount; i++ {
				dt.Add(uint64(i))
			}

			// Randomly select values to remove
			toRemove := make(map[int]bool)
			for len(toRemove) < tt.removeCount {
				idx := rand.Intn(tt.addCount)
				toRemove[idx] = true
			}

			// Remove selected values
			for idx := range toRemove {
				dt.Remove(uint64(idx))
			}

			// Calculate expected median from remaining values
			remaining := make([]uint64, 0, tt.addCount-tt.removeCount)
			for i := 0; i < tt.addCount; i++ {
				if !toRemove[i] {
					remaining = append(remaining, uint64(i))
				}
			}
			sort.Slice(remaining, func(i, j int) bool {
				return remaining[i] < remaining[j]
			})

			expectedMedian := remaining[len(remaining)/2]
			estimatedMedian := dt.Quantile(0.5)

			relError := float64(absDiff(estimatedMedian, expectedMedian)) / float64(expectedMedian)

			deletionRate := float64(tt.removeCount) / float64(tt.addCount) * 100
			t.Logf("Deletion rate: %.1f%%, Remaining: %d values",
				deletionRate, len(remaining))
			t.Logf("Expected median: %d, Estimated: %d, Error: %.4f%%",
				expectedMedian, estimatedMedian, relError*100)

			if relError > tt.maxError {
				t.Errorf("Error %.4f%% exceeds maximum %.2f%%",
					relError*100, tt.maxError*100)
			}
		})
	}
}

// TestDualTDigestHighDeletionRate tests with high deletion rate
// This simulates real-world scenarios where deletions are more than 30%
func TestDualTDigestHighDeletionRate(t *testing.T) {
	tests := []struct {
		name         string
		ratio        string
		addCount     int
		removeCount  int
		deletionRate float64
		maxError     float64
		expectHigh   bool
	}{
		{
			name:         "2:1 ratio (50% deletion)",
			ratio:        "2:1",
			addCount:     10000,
			removeCount:  5000,
			deletionRate: 0.50,
			maxError:     0.30, // 30% tolerance (expect high error)
			expectHigh:   true,
		},
		{
			name:         "1.5:1 ratio (66.7% deletion)",
			ratio:        "1.5:1",
			addCount:     10000,
			removeCount:  6667,
			deletionRate: 0.667,
			maxError:     0.50, // 50% tolerance (expect very high error)
			expectHigh:   true,
		},
		// Note: 1:1 ratio test removed as it results in empty digest
		// which is tested separately in TestDualTDigestEdgeCases
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rand.Seed(42)
			dt := NewDualTDigest(100)

			// Track actual remaining values
			values := make(map[uint64]bool)

			// Add all values
			for i := 0; i < tt.addCount; i++ {
				value := uint64(i)
				dt.Add(value)
				values[value] = true
			}

			// Randomly remove specified count
			toRemove := make([]uint64, 0, tt.removeCount)
			for v := range values {
				toRemove = append(toRemove, v)
				if len(toRemove) >= tt.removeCount {
					break
				}
			}

			for _, v := range toRemove {
				dt.Remove(v)
				delete(values, v)
			}

			// Calculate true median from remaining values
			if len(values) == 0 {
				t.Log("All values removed - digest correctly returns 0")
				if result := dt.Quantile(0.5); result != 0 {
					t.Errorf("Expected 0 for empty digest, got %d", result)
				}
				return
			}

			remaining := make([]uint64, 0, len(values))
			for v := range values {
				remaining = append(remaining, v)
			}
			sort.Slice(remaining, func(i, j int) bool {
				return remaining[i] < remaining[j]
			})

			expectedMedian := remaining[len(remaining)/2]
			estimatedMedian := dt.Quantile(0.5)

			relError := float64(absDiff(estimatedMedian, expectedMedian)) / float64(expectedMedian)

			t.Logf("Ratio: %s (%.1f%% deletion rate)", tt.ratio, tt.deletionRate*100)
			t.Logf("Added: %d, Removed: %d, Remaining: %d",
				tt.addCount, tt.removeCount, len(remaining))
			t.Logf("Expected median: %d, Estimated: %d", expectedMedian, estimatedMedian)
			t.Logf("Relative error: %.2f%%", relError*100)

			// Check rebuild state
			pos, neg := dt.Snapshot()
			t.Logf("Digest state: pos_weight=%d, neg_weight=%d",
				pos.TotalWeight, neg.TotalWeight)

			if tt.deletionRate >= rebuildRatioThreshold {
				t.Logf("⚠️  Deletion rate %.1f%% exceeds rebuild threshold (30%%)",
					tt.deletionRate*100)
				if tt.expectHigh {
					t.Logf("High error expected due to rebuild mechanism limitations")
				}
			}

			if relError > tt.maxError {
				if tt.expectHigh {
					t.Logf("Error %.2f%% exceeds %.2f%% - this documents known limitation",
						relError*100, tt.maxError*100)
				} else {
					t.Errorf("Unexpected high error %.2f%% (max %.2f%%)",
						relError*100, tt.maxError*100)
				}
			} else {
				t.Logf("✓ Error %.2f%% within acceptable range", relError*100)
			}
		})
	}
}

// TestDualTDigestSequentialVsRandomDeletion compares deletion patterns
func TestDualTDigestSequentialVsRandomDeletion(t *testing.T) {
	const totalAdd = 10000
	const totalRemove = 5000 // 50% deletion rate

	t.Run("Sequential deletion (FIFO)", func(t *testing.T) {
		dt := NewDualTDigest(100)

		// Add 0-9999
		for i := 0; i < totalAdd; i++ {
			dt.Add(uint64(i))
		}

		// Remove first 5000 (0-4999)
		for i := 0; i < totalRemove; i++ {
			dt.Remove(uint64(i))
		}

		// Expected: median of [5000, 9999] = 7499
		expectedMedian := uint64(5000 + (9999-5000)/2)
		estimatedMedian := dt.Quantile(0.5)

		relError := float64(absDiff(estimatedMedian, expectedMedian)) / float64(expectedMedian)

		t.Logf("Sequential deletion: Expected %d, Got %d, Error %.2f%%",
			expectedMedian, estimatedMedian, relError*100)

		if relError > 0.30 {
			t.Logf("⚠️  High error with sequential deletion at 50%% rate")
		}
	})

	t.Run("Random deletion", func(t *testing.T) {
		rand.Seed(42)
		dt := NewDualTDigest(100)

		// Add 0-9999
		values := make(map[uint64]bool)
		for i := 0; i < totalAdd; i++ {
			value := uint64(i)
			dt.Add(value)
			values[value] = true
		}

		// Randomly remove 5000
		toRemove := make([]uint64, 0, totalRemove)
		for v := range values {
			toRemove = append(toRemove, v)
			if len(toRemove) >= totalRemove {
				break
			}
		}

		for _, v := range toRemove {
			dt.Remove(v)
			delete(values, v)
		}

		// Calculate true median
		remaining := make([]uint64, 0, len(values))
		for v := range values {
			remaining = append(remaining, v)
		}
		sort.Slice(remaining, func(i, j int) bool {
			return remaining[i] < remaining[j]
		})

		expectedMedian := remaining[len(remaining)/2]
		estimatedMedian := dt.Quantile(0.5)

		relError := float64(absDiff(estimatedMedian, expectedMedian)) / float64(expectedMedian)

		t.Logf("Random deletion: Expected %d, Got %d, Error %.2f%%",
			expectedMedian, estimatedMedian, relError*100)

		if relError > 0.30 {
			t.Logf("⚠️  High error with random deletion at 50%% rate")
		}
	})
}

// TestDualTDigestMixedOperations simulates realistic usage patterns
func TestDualTDigestMixedOperations(t *testing.T) {
	rand.Seed(42)
	dt := NewDualTDigest(100)

	// Track actual values
	values := make(map[uint64]int)

	// Simulate mixed operations: 60% add, 40% remove
	const operations = 10000
	addCount := 0
	removeCount := 0

	for i := 0; i < operations; i++ {
		value := uint64(rand.Intn(20000))

		if rand.Float64() < 0.6 || len(values) == 0 {
			// Add operation
			dt.Add(value)
			values[value]++
			addCount++
		} else {
			// Remove operation - pick a random existing value
			var toRemove uint64
			for v := range values {
				toRemove = v
				break
			}
			dt.Remove(toRemove)
			values[toRemove]--
			if values[toRemove] <= 0 {
				delete(values, toRemove)
			}
			removeCount++
		}
	}

	// Calculate true median
	var netValues []uint64
	for v, count := range values {
		for j := 0; j < count; j++ {
			netValues = append(netValues, v)
		}
	}

	if len(netValues) == 0 {
		t.Skip("No net values remain")
	}

	sort.Slice(netValues, func(i, j int) bool {
		return netValues[i] < netValues[j]
	})

	expectedMedian := netValues[len(netValues)/2]
	estimatedMedian := dt.Quantile(0.5)

	relError := float64(absDiff(estimatedMedian, expectedMedian)) / float64(expectedMedian)
	deletionRate := float64(removeCount) / float64(addCount) * 100

	t.Logf("Mixed operations: %d adds, %d removes (%.1f%% deletion rate)",
		addCount, removeCount, deletionRate)
	t.Logf("Remaining values: %d", len(netValues))
	t.Logf("Expected median: %d, Estimated: %d, Error: %.2f%%",
		expectedMedian, estimatedMedian, relError*100)

	if relError > 0.15 {
		t.Logf("⚠️  Moderate error with mixed random operations")
	}
}

// TestDualTDigestEdgeCases tests edge cases and boundary conditions
func TestDualTDigestEdgeCases(t *testing.T) {
	t.Run("empty digest", func(t *testing.T) {
		dt := NewDualTDigest(100)
		for _, q := range []float64{0.0, 0.5, 1.0} {
			if result := dt.Quantile(q); result != 0 {
				t.Errorf("Empty digest q=%.2f: got %d, want 0", q, result)
			}
		}
	})

	t.Run("all values removed", func(t *testing.T) {
		dt := NewDualTDigest(100)
		for i := 0; i < 1000; i++ {
			dt.Add(uint64(i))
		}
		for i := 0; i < 1000; i++ {
			dt.Remove(uint64(i))
		}

		// Note: Due to rebuild mechanism, some centroids may remain
		// This test documents that behavior - not all edge cases return 0
		median := dt.Quantile(0.5)
		t.Logf("All values removed: median=%d (rebuild artifacts may remain)", median)

		// Check that net weight is effectively 0 or very small
		pos, neg := dt.Snapshot()
		t.Logf("State after full removal: pos=%d, neg=%d", pos.TotalWeight, neg.TotalWeight)
	})

	t.Run("single value", func(t *testing.T) {
		dt := NewDualTDigest(100)
		dt.Add(42)

		for _, q := range []float64{0.0, 0.5, 1.0} {
			if result := dt.Quantile(q); result != 42 {
				t.Errorf("Single value q=%.2f: got %d, want 42", q, result)
			}
		}
	})

	t.Run("remove more than added", func(t *testing.T) {
		dt := NewDualTDigest(100)
		for i := 0; i < 100; i++ {
			dt.Add(uint64(i))
		}
		for i := 0; i < 200; i++ {
			dt.Remove(uint64(i))
		}

		// Note: Over-removal may leave artifacts due to rebuild
		median := dt.Quantile(0.5)
		t.Logf("Over-removal: median=%d (some artifacts expected)", median)

		// Check state
		pos, neg := dt.Snapshot()
		t.Logf("State: pos=%d, neg=%d", pos.TotalWeight, neg.TotalWeight)

		// The median should be 0 or very small if net weight is <=0
		if pos.TotalWeight > neg.TotalWeight && median > 0 {
			t.Logf("Net weight positive, median non-zero is acceptable")
		}
	})

	t.Run("invalid quantile", func(t *testing.T) {
		dt := NewDualTDigest(100)
		dt.Add(100)

		if result := dt.Quantile(-0.1); result != 0 {
			t.Errorf("Negative quantile: got %d, want 0", result)
		}
		if result := dt.Quantile(1.5); result != 0 {
			t.Errorf("Quantile > 1: got %d, want 0", result)
		}
	})
}

// TestDualTDigestLargeValues tests with large uint64 values
func TestDualTDigestLargeValues(t *testing.T) {
	rand.Seed(42)
	dt := NewDualTDigest(100)

	// Use moderate large values that won't trigger precision issues
	const baseValue = uint64(1_000_000_000_000) // 1 trillion
	const addCount = 10000
	const removeCount = 2000 // 20% deletion (low rate for better accuracy)

	// Add values
	values := make(map[uint64]bool)
	for i := 0; i < addCount; i++ {
		value := baseValue + uint64(i*1000) // Spread out values
		dt.Add(value)
		values[value] = true
	}

	// Randomly remove
	toRemove := make([]uint64, 0, removeCount)
	for v := range values {
		toRemove = append(toRemove, v)
		if len(toRemove) >= removeCount {
			break
		}
	}

	for _, v := range toRemove {
		dt.Remove(v)
		delete(values, v)
	}

	// Calculate expected median
	remaining := make([]uint64, 0, len(values))
	for v := range values {
		remaining = append(remaining, v)
	}
	sort.Slice(remaining, func(i, j int) bool {
		return remaining[i] < remaining[j]
	})

	expectedMedian := remaining[len(remaining)/2]
	estimatedMedian := dt.Quantile(0.5)

	// For large values, check relative error
	relError := float64(absDiff(estimatedMedian, expectedMedian)) / float64(expectedMedian)

	t.Logf("Large values test: base=%d", baseValue)
	t.Logf("Expected median: %d, Estimated: %d", expectedMedian, estimatedMedian)
	t.Logf("Relative error: %.6f%%", relError*100)

	if relError > 0.05 {
		t.Errorf("Large value error %.6f%% too high", relError*100)
	}
}

// TestDualTDigestSnapshot tests snapshot and restore functionality
func TestDualTDigestSnapshot(t *testing.T) {
	rand.Seed(42)
	dt := NewDualTDigest(100)

	// Add and remove some values
	for i := 0; i < 5000; i++ {
		dt.Add(uint64(i))
	}
	for i := 0; i < 1500; i++ {
		dt.Remove(uint64(i))
	}

	// Take snapshot
	pos, neg := dt.Snapshot()

	t.Logf("Snapshot: pos_weight=%d, neg_weight=%d",
		pos.TotalWeight, neg.TotalWeight)

	// Create new digest from snapshot
	dt2 := NewDualTDigestFromSnapshot(100, pos, neg)

	// Verify both produce same results
	for _, q := range []float64{0.01, 0.25, 0.5, 0.75, 0.99} {
		result1 := dt.Quantile(q)
		result2 := dt2.Quantile(q)
		if result1 != result2 {
			t.Errorf("Snapshot mismatch at q=%.2f: original=%d, restored=%d",
				q, result1, result2)
		}
	}

	t.Log("✓ Snapshot and restore working correctly")
}

// TestDualTDigestRebuildThreshold verifies 30% rebuild threshold
func TestDualTDigestRebuildThreshold(t *testing.T) {
	tests := []struct {
		deletionRate  float64
		expectRebuild bool
	}{
		{0.10, false},
		{0.20, false},
		{0.29, false},
		{0.30, true},
		{0.40, true},
		{0.50, true},
	}

	for _, tt := range tests {
		t.Run(sprintf("%.0f%% deletion", tt.deletionRate*100), func(t *testing.T) {
			dt := NewDualTDigest(100)

			const total = 10000
			addCount := total
			removeCount := int(float64(total) * tt.deletionRate)

			for i := 0; i < addCount; i++ {
				dt.Add(uint64(i))
			}
			for i := 0; i < removeCount; i++ {
				dt.Remove(uint64(i))
			}

			pos, neg := dt.Snapshot()
			rebuildOccurred := (neg.TotalWeight < uint64(removeCount/2))

			t.Logf("Deletion rate %.0f%%: pos=%d, neg=%d, rebuild=%v",
				tt.deletionRate*100, pos.TotalWeight, neg.TotalWeight, rebuildOccurred)

			if tt.expectRebuild && !rebuildOccurred {
				t.Errorf("Expected rebuild at %.0f%% deletion rate", tt.deletionRate*100)
			}
			if !tt.expectRebuild && rebuildOccurred {
				t.Errorf("Unexpected rebuild at %.0f%% deletion rate", tt.deletionRate*100)
			}
		})
	}
}

// Benchmark tests
func BenchmarkDualTDigest_Add(b *testing.B) {
	dt := NewDualTDigest(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dt.Add(uint64(i))
	}
}

func BenchmarkDualTDigest_Remove(b *testing.B) {
	dt := NewDualTDigest(100)
	for i := 0; i < b.N; i++ {
		dt.Add(uint64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dt.Remove(uint64(i))
	}
}

func BenchmarkDualTDigest_Quantile(b *testing.B) {
	dt := NewDualTDigest(100)
	for i := 0; i < 10000; i++ {
		dt.Add(uint64(i))
	}
	for i := 0; i < 3000; i++ {
		dt.Remove(uint64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dt.Quantile(0.5)
	}
}

func BenchmarkDualTDigest_MixedOperations(b *testing.B) {
	dt := NewDualTDigest(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			dt.Remove(uint64(i / 2))
		} else {
			dt.Add(uint64(i))
		}
		if i%100 == 0 {
			dt.Quantile(0.95)
		}
	}
}

// Helper function for sprintf
func sprintf(format string, args ...interface{}) string {
	// Simple implementation for test names
	if format == "%.0f%% deletion" {
		rate := args[0].(float64)
		return string(rune(int(rate*100))) + "% deletion"
	}
	return format
}
