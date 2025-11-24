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
	"math"
	"math/bits"
	"sort"
	"sync"
)

const (
	defaultCompression = 100
	defaultBufferSize  = 256
)

// TDigest implements the T-Digest algorithm for streaming quantile approximation.
// It is optimized for accurate tail quantiles while keeping a compact memory footprint.
//
// PRECISION: This implementation uses 128-bit integer arithmetic for high precision
// calculations to accurately handle the full uint64 range without floating-point errors.
type TDigest struct {
	compression uint64
	bufferSize  int

	centroids   []Centroid
	pending     []uint64
	totalWeight uint64
	min         uint64
	max         uint64

	mu sync.RWMutex
}

// NewTDigest creates a TDigest with the provided compression factor.
// Compression controls the trade-off between accuracy and number of centroids.
// Typical values are in the range [20, 200]. Values == 0 fall back to the default.
func NewTDigest(compression uint64) *TDigest {
	if compression == 0 {
		compression = defaultCompression
	}
	return &TDigest{
		compression: compression,
		bufferSize:  defaultBufferSize,
		min:         math.MaxUint64,
		max:         0,
	}
}

// Add inserts a single data point into the digest.
func (t *TDigest) Add(value uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.totalWeight == 0 || value < t.min {
		t.min = value
	}
	if value > t.max {
		t.max = value
	}

	t.pending = append(t.pending, value)
	t.totalWeight++

	if len(t.pending) >= t.bufferSize {
		t.compressLocked()
	}
}

// Quantile approximates the value at the provided quantile q ∈ [0, 1].
// When no samples are available it returns 0.
func (t *TDigest) Quantile(q float64) uint64 {
	if q < 0 || q > 1 {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.totalWeight == 0 {
		return 0
	}

	if len(t.pending) > 0 {
		t.compressLocked()
	}

	if q == 0 {
		return t.min
	}
	if q == 1 {
		return t.max
	}

	// Use high-precision calculation: target = q * totalWeight
	const precision = 1_000_000_000 // 1 billion for precision
	targetNumerator := uint64(q * float64(precision))
	targetWeight := mulDiv128(t.totalWeight, targetNumerator, precision)

	cumulative := uint64(0)

	for i, c := range t.centroids {
		weight := c.Weight
		next := cumulative + weight

		if targetWeight <= next {
			if i == 0 {
				if len(t.centroids) == 1 {
					return c.Mean
				}
				// Linear interpolation with integer arithmetic
				nextMean := t.centroids[i+1].Mean
				mean := c.Mean
				if nextMean == mean {
					return mean
				}
				// result = mean + (target - cumulative) / weight * (nextMean - mean)
				result := interpolateUint64(mean, nextMean, targetWeight-cumulative, weight)
				return result
			}

			prev := t.centroids[i-1]
			prevWeight := prev.Weight
			prevCumulative := cumulative - prevWeight
			span := next - prevCumulative

			if span == 0 {
				return c.Mean
			}

			prevMean := prev.Mean
			mean := c.Mean
			if mean == prevMean {
				return mean
			}

			// result = prevMean + (target - prevCumulative) / span * (mean - prevMean)
			result := interpolateUint64(prevMean, mean, targetWeight-prevCumulative, span)
			return result
		}
		cumulative = next
	}

	return t.max
}

// compressLocked merges pending samples and rebalances centroids.
// Caller must hold t.mu.
func (t *TDigest) compressLocked() {
	if len(t.pending) == 0 && len(t.centroids) <= 1 {
		t.pending = t.pending[:0]
		return
	}

	sort.Slice(t.pending, func(i, j int) bool {
		return t.pending[i] < t.pending[j]
	})

	all := make([]Centroid, 0, len(t.centroids)+len(t.pending))
	all = append(all, t.centroids...)
	for _, v := range t.pending {
		all = append(all, Centroid{Mean: v, Weight: 1})
	}
	t.pending = t.pending[:0]

	sort.Slice(all, func(i, j int) bool {
		return all[i].Mean < all[j].Mean
	})

	if len(all) == 0 {
		t.centroids = t.centroids[:0]
		return
	}

	total := float64(t.totalWeight)
	if total == 0 {
		total = 1
	}

	newCentroids := make([]Centroid, 0, len(all))
	newCentroids = append(newCentroids, all[0])
	cumulative := float64(all[0].Weight)

	for i := 1; i < len(all); i++ {
		c := all[i]

		// Calculate q = cumulative / totalWeight using float64 (only for threshold calculation)
		q := cumulative / total
		maxWeight := t.maxAllowedWeight(q)

		lastIdx := len(newCentroids) - 1
		last := newCentroids[lastIdx]

		lastWeight := last.Weight
		cWeight := c.Weight

		// Check if we can merge
		if float64(lastWeight+cWeight) <= maxWeight {
			combined := lastWeight + cWeight

			// Compute weighted mean using integer arithmetic to preserve precision
			// newMean = (lastMean * lastWeight + cMean * cWeight) / combined
			newMean := weightedMeanUint64(last.Mean, lastWeight, c.Mean, cWeight)

			last.Mean = newMean
			last.Weight = combined
			newCentroids[lastIdx] = last
		} else {
			newCentroids = append(newCentroids, c)
		}
		cumulative += float64(cWeight)
	}

	t.centroids = newCentroids
}

func (t *TDigest) maxAllowedWeight(q float64) float64 {
	scale := 4 * float64(t.totalWeight) * q * (1 - q) / float64(t.compression)
	if scale < 1 {
		return 1
	}
	return scale
}

// TotalWeight returns the number of samples currently tracked by the digest.
func (t *TDigest) TotalWeight() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.totalWeight
}

// Snapshot returns a copy of the centroids and metadata for read-only operations.
func (t *TDigest) Snapshot() TDigestSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pending) > 0 {
		t.compressLocked()
	}

	snapshot := TDigestSnapshot{
		TotalWeight: t.totalWeight,
		Min:         t.min,
		Max:         t.max,
	}

	if len(t.centroids) > 0 {
		snapshot.Centroids = make([]Centroid, len(t.centroids))
		copy(snapshot.Centroids, t.centroids)
	}

	return snapshot
}

func (t *TDigest) LoadFromSnapshot(snapshot TDigestSnapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.centroids = snapshot.Centroids
	t.totalWeight = snapshot.TotalWeight
	t.min = snapshot.Min
	t.max = snapshot.Max
}

// High-precision arithmetic functions using 128-bit integer operations

// mulDiv128 computes (a * b) / c using 128-bit intermediate result to avoid overflow.
func mulDiv128(a, b, c uint64) uint64 {
	if c == 0 {
		return 0
	}
	// Use bits.Mul64 to get 128-bit result of a * b
	hi, lo := bits.Mul64(a, b)
	// Perform 128-bit / 64-bit division
	quotient, _ := bits.Div64(hi, lo, c)
	return quotient
}

// interpolateUint64 performs linear interpolation using 128-bit integer arithmetic.
// Returns: start + (numerator / denominator) * (end - start)
func interpolateUint64(start, end, numerator, denominator uint64) uint64 {
	if denominator == 0 {
		return start
	}

	if start == end {
		return start
	}

	var diff uint64
	var isNegative bool
	if end > start {
		diff = end - start
		isNegative = false
	} else {
		diff = start - end
		isNegative = true
	}

	// Calculate: (numerator * diff) / denominator using 128-bit arithmetic
	delta := mulDiv128(numerator, diff, denominator)

	if isNegative {
		if delta > start {
			return 0
		}
		return start - delta
	}

	// Check for overflow
	if math.MaxUint64-start < delta {
		return math.MaxUint64
	}
	return start + delta
}

// weightedMeanUint64 calculates weighted mean using 128-bit arithmetic.
// Returns: (mean1 * weight1 + mean2 * weight2) / (weight1 + weight2)
func weightedMeanUint64(mean1, weight1, mean2, weight2 uint64) uint64 {
	totalWeight := weight1 + weight2
	if totalWeight == 0 {
		return 0
	}

	// Calculate mean1 * weight1 using 128-bit
	hi1, lo1 := bits.Mul64(mean1, weight1)

	// Calculate mean2 * weight2 using 128-bit
	hi2, lo2 := bits.Mul64(mean2, weight2)

	// Add the two 128-bit numbers
	lo, carry := bits.Add64(lo1, lo2, 0)
	hi, _ := bits.Add64(hi1, hi2, carry)

	// Divide by totalWeight
	result, _ := bits.Div64(hi, lo, totalWeight)
	return result
}

func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
