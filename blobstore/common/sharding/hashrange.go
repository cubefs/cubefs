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

package sharding

import (
	"fmt"
	"math"

	"github.com/spaolacci/murmur3"
)

const (
	minHashBoundary uint64 = 0
	maxHashBoundary uint64 = math.MaxUint64
)

type hashRange Range

func (h *hashRange) Belong(ci *CompareItem) bool {
	if ci.context == nil {
		hashValues := make([]uint64, len(ci.keys))
		for i := range ci.keys {
			hashValues[i] = Hash(ci.keys[i])
		}
		ci.context = hashValues
	}

	hashValues := ci.context.([]uint64)
	for i := range h.Subs {
		if hashValues[i] < h.Subs[i].Min ||
			hashValues[i] >= h.Subs[i].Max {
			return false
		}
	}
	return true
}

func (h *hashRange) Contain(r *Range) bool {
	for i := range r.Subs {
		if r.Subs[i].Min < h.Subs[i].Min ||
			r.Subs[i].Max > h.Subs[i].Max {
			return false
		}
	}
	return true
}

func (h *hashRange) Split(idx int) (ret [2]Range, err error) {
	sub := h.Subs[idx]

	if sub.Max == sub.Min {
		panic(fmt.Sprintf("invalid split range: %+v", h))
	}
	if sub.Max-sub.Min == 1 {
		// no more splittable
		err = ErrNoMoreSplit
		return
	}

	left := make([]SubRange, len(h.Subs))
	right := make([]SubRange, len(h.Subs))
	copy(left, h.Subs)
	copy(right, h.Subs)

	mid := sub.Min + ((sub.Max - sub.Min) >> 1)
	left[idx].Max = mid
	right[idx].Min = mid

	ret[0] = Range{
		Type: h.Type,
		Subs: left,
	}
	ret[1] = Range{
		Type: h.Type,
		Subs: right,
	}
	return
}

func (h *hashRange) IsEmpty() bool {
	for i := range h.Subs {
		if h.Subs[i].Min != 0 || h.Subs[i].Max != 0 {
			return false
		}
	}
	return true
}

func (h *hashRange) MaxBoundary() Boundary {
	values := make([]uint64, len(h.Subs))
	for i := range h.Subs {
		values[i] = h.Subs[i].Max
	}
	return &hashBoundary{hashValues: values}
}

type hashBoundary struct {
	hashValues []uint64
}

func (h *hashBoundary) Less(b Boundary) bool {
	hb := b.(*hashBoundary)
	for i := range hb.hashValues {
		if hb.hashValues[i] > h.hashValues[i] {
			return true
		}
	}

	return false
}

func (h *hashBoundary) String() string {
	return fmt.Sprintf("%v", h.hashValues)
}

/*----------------------------------------------util function----------------------------------------*/

// Hash calculate raw's hash value, return uint64 value as result
func Hash(raw []byte) uint64 {
	hash := murmur3.New64()
	hash.Write(raw)
	return hash.Sum64() % maxHashBoundary
}
