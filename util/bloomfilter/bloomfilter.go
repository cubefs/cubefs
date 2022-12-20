package bloomfilter

import (
	"encoding/binary"

	"github.com/bits-and-blooms/bloom"
)

func AddUint64ToBloom(cacheBloom *bloom.BloomFilter, key uint64) {
	if cacheBloom == nil {
		return
	}
	n := make([]byte, 8)
	binary.BigEndian.PutUint64(n, key)
	cacheBloom.Add(n)
}

func CheckUint64Exist(cacheBloom *bloom.BloomFilter, key uint64) bool {
	if cacheBloom == nil {
		return false
	}
	n := make([]byte, 8)
	binary.BigEndian.PutUint64(n, key)
	return cacheBloom.Test(n)
}
