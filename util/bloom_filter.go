// Copyright 2023 The CubeFS Authors.
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

package util

type BloomFilter struct {
	bits     []uint8
	hashFunc []func(data []byte) uint64
}

func NewBloomFilter(size uint64, hashFunc ...func(data []byte) uint64) *BloomFilter {
	return &BloomFilter{
		bits:     make([]uint8, size),
		hashFunc: hashFunc,
	}
}

func (bf *BloomFilter) Add(data []byte) {
	for _, hashFunc := range bf.hashFunc {
		hash := hashFunc(data)
		index := hash % uint64(len(bf.bits))
		bf.bits[index/8] |= 1 << (index % 8)
	}
}

func (bf *BloomFilter) Contains(data []byte) bool {
	for _, hashFunc := range bf.hashFunc {
		hash := hashFunc(data)
		index := hash % uint64(len(bf.bits))
		if bf.bits[index/8]&(1<<(index%8)) == 0 {
			return false
		}
	}
	return true
}
