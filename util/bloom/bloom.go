package bloom

import (
	"encoding/binary"

	"github.com/bits-and-blooms/bloom/v3"
)

type BloomFilter struct {
	*bloom.BloomFilter
}

func New(m uint, k uint) *BloomFilter {
	return &BloomFilter{BloomFilter: bloom.New(m, k)}
}

func (b *BloomFilter) AddUint64(key uint64) {
	if b != nil {
		b.Add(uint64ToBytes(key))
	}
}

func (b *BloomFilter) TestUint64(key uint64) bool {
	if b == nil {
		return false
	}
	return b.Test(uint64ToBytes(key))
}

func uint64ToBytes(key uint64) (data []byte) {
	data = make([]byte, 8)
	binary.BigEndian.PutUint64(data[:], key)
	return
}
