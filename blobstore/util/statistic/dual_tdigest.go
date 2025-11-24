package statistic

import (
	"math"
	"sort"
	"sync"
)

// DualTDigest maintains two TDigest instances (add and delete) to support logical deletions.
type DualTDigest struct {
	positive *TDigest
	negative *TDigest

	mu sync.RWMutex
}

const (
	rebuildRatioThreshold = 0.3
)

// NewDualTDigest creates a DualTDigest with given compression parameter.
// The delete digest uses half of the compression to keep its error tighter.
func NewDualTDigest(compression uint64) *DualTDigest {
	if compression == 0 {
		compression = defaultCompression
	}
	return &DualTDigest{
		positive: NewTDigest(compression),
		negative: NewTDigest(compression / 2),
	}
}

func NewDualTDigestFromSnapshot(compression uint64, positive, negative TDigestSnapshot) *DualTDigest {
	if compression == 0 {
		compression = defaultCompression
	}
	p := NewTDigest(compression)
	p.LoadFromSnapshot(positive)
	n := NewTDigest(compression / 2)
	n.LoadFromSnapshot(negative)
	return &DualTDigest{
		positive: p,
		negative: n,
	}
}

// Add inserts a data point into the positive digest.
func (d *DualTDigest) Add(value uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.positive.Add(value)
	d.maybeRebuildLocked()
}

// Remove records a deletion in the negative digest.
func (d *DualTDigest) Remove(value uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.negative.Add(value)
	d.maybeRebuildLocked()
}

// Quantile returns the approximate quantile in [0,1] of the net distribution.
func (d *DualTDigest) Quantile(q float64) uint64 {
	if q < 0 || q > 1 {
		return 0
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	posSnapshot := d.positive.Snapshot()
	posCentroids := posSnapshot.Centroids
	posWeightUint := posSnapshot.TotalWeight
	negSnapshot := d.negative.Snapshot()
	negCentroids := negSnapshot.Centroids
	negWeightUint := negSnapshot.TotalWeight

	if d.shouldRebuild(posWeightUint, negWeightUint) {
		d.rebuildFromCentroids(posCentroids, negCentroids)
		posSnapshot = d.positive.Snapshot()
		posCentroids = posSnapshot.Centroids
		posWeightUint = posSnapshot.TotalWeight
		negSnapshot = d.negative.Snapshot()
		negCentroids = negSnapshot.Centroids
		negWeightUint = negSnapshot.TotalWeight
	}

	// Check if all samples are removed
	if posWeightUint <= negWeightUint {
		return 0
	}

	// Calculate net weight using integer arithmetic
	netWeight := posWeightUint - negWeightUint
	if netWeight == 0 {
		return 0
	}

	if len(posCentroids) == 0 {
		return 0
	}

	if q == 0 {
		return d.minValue(posCentroids, negCentroids)
	}
	if q == 1 {
		return d.maxValue(posCentroids, negCentroids)
	}

	// Use high-precision calculation: target = q * netWeight
	const precision = 1_000_000_000
	targetNumerator := uint64(q * float64(precision))
	targetWeight := mulDiv128(netWeight, targetNumerator, precision)

	merged := mergeSignedCentroids(posCentroids, negCentroids)

	// Check if there are any positive weights in merged centroids
	hasPositive := false
	for _, sc := range merged {
		if sc.Weight > 0 {
			hasPositive = true
			break
		}
	}
	if !hasPositive {
		return 0
	}

	cumulative := int64(0) // Use signed integer for cumulative
	prevPosIndex := -1

	for i, sc := range merged {
		nextCum := cumulative + sc.Weight

		if sc.Weight > 0 && int64(targetWeight) <= nextCum {
			// Calculate boundaries
			leftBoundary := sc.Mean
			if prevPosIndex >= 0 {
				// Average of previous and current centroid means
				prevMean := merged[prevPosIndex].Mean
				leftBoundary = (prevMean + sc.Mean) / 2
			}

			rightBoundary := sc.Mean
			nextPos := findNextPositive(merged, i+1)
			if nextPos >= 0 {
				nextMean := merged[nextPos].Mean
				rightBoundary = (sc.Mean + nextMean) / 2
			}

			if rightBoundary <= leftBoundary {
				return sc.Mean
			}

			intervalWeight := sc.Weight
			if intervalWeight <= 0 {
				return sc.Mean
			}

			// Use integer interpolation
			// frac = (target - cumulative) / intervalWeight
			// result = leftBoundary + frac * (rightBoundary - leftBoundary)
			var numerator uint64
			if int64(targetWeight) > cumulative {
				numerator = uint64(int64(targetWeight) - cumulative)
			} else {
				numerator = 0
			}
			denominator := uint64(intervalWeight)

			result := interpolateUint64(leftBoundary, rightBoundary, numerator, denominator)
			return result
		}

		cumulative = nextCum
		if sc.Weight > 0 {
			prevPosIndex = i
		}
	}

	// If we didn't find the target quantile, check if cumulative is valid
	if cumulative <= 0 || prevPosIndex < 0 {
		return 0
	}

	// Return the last positive centroid's mean as fallback
	return merged[prevPosIndex].Mean
}

func (d *DualTDigest) minValue(pos, neg []Centroid) uint64 {
	merged := mergeSignedCentroids(pos, neg)
	for _, sc := range merged {
		if sc.Weight <= 0 {
			continue
		}
		return sc.Mean
	}
	return 0
}

func (d *DualTDigest) maxValue(pos, neg []Centroid) uint64 {
	merged := mergeSignedCentroids(pos, neg)
	for i := len(merged) - 1; i >= 0; i-- {
		if merged[i].Weight <= 0 {
			continue
		}
		return merged[i].Mean
	}
	return 0
}

// signedCentroid is a temporary structure for merging positive and negative centroids.
type signedCentroid struct {
	Mean   uint64
	Weight int64 // Signed integer: positive for additions, negative for deletions
}

// mergeSignedCentroids merges positive and negative centroids while preserving sign information.
func mergeSignedCentroids(pos, neg []Centroid) []signedCentroid {
	merged := make([]signedCentroid, 0, len(pos)+len(neg))

	// Add positive centroids
	for _, c := range pos {
		if c.Weight == 0 {
			continue
		}
		merged = append(merged, signedCentroid{Mean: c.Mean, Weight: int64(c.Weight)})
	}

	// Add negative centroids (with negative weight)
	for _, c := range neg {
		if c.Weight == 0 {
			continue
		}
		merged = append(merged, signedCentroid{Mean: c.Mean, Weight: -int64(c.Weight)})
	}

	// Sort by mean, with positive weights first for same mean
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].Mean == merged[j].Mean {
			return merged[i].Weight > merged[j].Weight
		}
		return merged[i].Mean < merged[j].Mean
	})

	return merged
}

func findNextPositive(merged []signedCentroid, start int) int {
	for i := start; i < len(merged); i++ {
		if merged[i].Weight > 0 {
			return i
		}
	}
	return -1
}

func (d *DualTDigest) maybeRebuildLocked() {
	posWeight := d.positive.TotalWeight()
	if posWeight == 0 {
		return
	}
	negWeight := d.negative.TotalWeight()
	if !d.shouldRebuild(posWeight, negWeight) {
		return
	}
	posSnapshot := d.positive.Snapshot()
	posCentroids := posSnapshot.Centroids

	negSnapshot := d.negative.Snapshot()
	negCentroids := negSnapshot.Centroids

	d.rebuildFromCentroids(posCentroids, negCentroids)
}

func (d *DualTDigest) shouldRebuild(posWeight, negWeight uint64) bool {
	if posWeight == 0 {
		return false
	}
	return float64(negWeight)/float64(posWeight) >= rebuildRatioThreshold
}

func (d *DualTDigest) rebuildFromCentroids(pos, neg []Centroid) {
	merged := mergeSignedCentroids(pos, neg)
	newPositive := NewTDigest(d.positive.compression)

	newPositive.mu.Lock()
	defer newPositive.mu.Unlock()
	newPositive.centroids = newPositive.centroids[:0]
	newPositive.pending = newPositive.pending[:0]
	newPositive.totalWeight = 0
	newPositive.min = math.MaxUint64
	newPositive.max = 0

	for _, sc := range merged {
		if sc.Weight <= 0 {
			continue
		}
		weight := uint64(sc.Weight)
		newPositive.centroids = append(newPositive.centroids, Centroid{
			Mean:   sc.Mean,
			Weight: weight,
		})
		newPositive.totalWeight += weight
		if newPositive.totalWeight == weight || sc.Mean < newPositive.min {
			newPositive.min = sc.Mean
		}
		if sc.Mean > newPositive.max {
			newPositive.max = sc.Mean
		}
	}

	if len(newPositive.centroids) == 0 {
		newPositive.min = math.MaxUint64
		newPositive.max = 0
	} else {
		newPositive.compressLocked()
	}

	d.positive = newPositive
	d.negative = NewTDigest(d.positive.compression / 2)
}

func (d *DualTDigest) Snapshot() (positive, negative TDigestSnapshot) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	positive = d.positive.Snapshot()
	negative = d.negative.Snapshot()
	return
}
