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
	"errors"
	"fmt"
)

const (
	Less   = -1
	Equal  = 0
	Larger = 1
)

var (
	ErrUnsupportedSplit     = errors.New("split is not supported")
	ErrNoMoreSplit          = errors.New("this range can not split anymore")
	ErrUnsupportedRangeType = errors.New("unsupported sharding range type: %s")
)

type (
	Boundary interface {
		Less(b Boundary) bool
		String() string
	}
)

// New return an empty Range with specified range type
func New(rt RangeType, subRangeCount int) *Range {
	switch rt {
	case RangeType_RangeTypeHash:
		subs := make([]SubRange, subRangeCount)
		for i := range subs {
			subs[i] = SubRange{
				Min: minHashBoundary,
				Max: maxHashBoundary,
			}
		}
		return &Range{
			Type: RangeType_RangeTypeHash,
			Subs: subs,
		}
	default:
		panic(fmt.Sprintf(ErrUnsupportedRangeType.Error(), rt))
	}
}

func InitShardingRange(RangeType RangeType, subRangeCount, shardCount int) []*Range {
	root := New(RangeType, subRangeCount)
	return initShardingRange(root, shardCount)
}

// NewCompareItem return compare item for range belong compare
// key can be integer type or bytes for different sharding range type
func NewCompareItem(rt RangeType, keys [][]byte) *CompareItem {
	return &CompareItem{rt: rt, keys: keys}
}

type CompareItem struct {
	rt      RangeType
	keys    [][]byte
	context interface{}
}

func (c *CompareItem) GetBoundary() Boundary {
	switch c.rt {
	case RangeType_RangeTypeHash:
		if c.context == nil {
			values := make([]uint64, len(c.keys))
			for i := range c.keys {
				values[i] = Hash(c.keys[i])
			}
			c.context = values
		}
		return &hashBoundary{hashValues: c.context.([]uint64)}
	default:
		panic("invalid range type")
	}
}

func (c *CompareItem) String() string {
	return fmt.Sprintf("{key:%v, context:%v}", c.keys, c.context)
}

func initShardingRange(root *Range, shardCount int) []*Range {
	if shardCount == 0 {
		return nil
	}
	if shardCount == 1 {
		return []*Range{root}
	}

	// default sharding range init with split index 0
	rs, err := root.Split(0)
	if err != nil {
		panic(fmt.Sprintf("split failed: %s", err))
	}

	if shardCount%2 > 0 {
		shardCount = (shardCount/2 + 1) * 2
	}
	lefts := initShardingRange(&rs[0], shardCount/2)
	rights := initShardingRange(&rs[1], shardCount/2)
	return append(lefts, rights...)
}
