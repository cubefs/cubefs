// Copyright 2022 The CubeFS Authors.
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

package ec

import (
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// Buffer one ec blob's reused buffer
// Manually manage the DataBuf in Ranged mode, do not Split it
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |   data      |    align bytes    |       partiy        | local |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |   DataBuf   |
//  |<--DataSize->|
//  - - - - - - - - - - - - - - - - - -
//  |            ECDataBuf            |
//  |<--         ECDataSize        -->|
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |                           ECBuf                               |
//  |<---                       ECSize                          --->|
type Buffer struct {
	tactic codemode.Tactic
	pool   *resourcepool.MemPool

	// DataBuf real data buffer
	DataBuf []byte
	// ECDataBuf ec data buffer to Split,
	// is nil if Buffer is Ranged mode
	ECDataBuf []byte
	// BufferSizes all sizes
	BufferSizes
}

// BufferSizes all sizes
// ECSize is sum of all ec shards size,
// not equal the capacity of DataBuf in Ranged mode
type BufferSizes struct {
	ShardSize  int // per shard size
	DataSize   int // real data size
	ECDataSize int // ec data size only with data
	ECSize     int // ec buffer size with partiy and local shards
	From       int // ranged from
	To         int // ranged to
}

func isOutOfRange(dataSize, from, to int) bool {
	return dataSize <= 0 || to < from ||
		from < 0 || from > dataSize ||
		to < 0 || to > dataSize
}

func newBuffer(dataSize, from, to int, tactic codemode.Tactic, pool *resourcepool.MemPool, hasParity bool) (*Buffer, error) {
	if isOutOfRange(dataSize, from, to) {
		return nil, ErrShortData
	}

	shardN := tactic.N
	if shardN <= 0 {
		return nil, ErrInvalidCodeMode
	}

	shardSize := (dataSize + shardN - 1) / shardN
	// align per shard with tactic MinShardSize
	if shardSize < tactic.MinShardSize {
		shardSize = tactic.MinShardSize
	}

	ecDataSize := shardSize * tactic.N
	ecSize := shardSize * (tactic.N + tactic.M + tactic.L)

	var (
		err error

		buf       []byte
		dataBuf   []byte
		ecDataBuf []byte
	)
	if pool != nil {
		size := ecSize
		if !hasParity {
			size = to - from
		}

		buf, err = pool.Get(size)
		if err == resourcepool.ErrNoSuitableSizeClass {
			log.Warn(err, "for", size, "try to alloc bytes")
			buf, err = pool.Alloc(size)
		}
		if err != nil {
			return nil, err
		}

		if !hasParity {
			dataBuf = buf[:size]
			ecDataBuf = nil
		} else {
			// zero the padding bytes of data section
			pool.Zero(buf[dataSize:ecDataSize])
			dataBuf = buf[:dataSize]
			ecDataBuf = buf[:ecDataSize]
		}
	}

	return &Buffer{
		tactic:    tactic,
		pool:      pool,
		DataBuf:   dataBuf,
		ECDataBuf: ecDataBuf,
		BufferSizes: BufferSizes{
			ShardSize:  shardSize,
			DataSize:   dataSize,
			ECDataSize: ecDataSize,
			ECSize:     ecSize,
			From:       from,
			To:         to,
		},
	}, nil
}

// NewBuffer new ec buffer with data size and ec mode
func NewBuffer(dataSize int, tactic codemode.Tactic, pool *resourcepool.MemPool) (*Buffer, error) {
	return newBuffer(dataSize, 0, dataSize, tactic, pool, true)
}

// NewRangeBuffer ranged buffer with least data size
func NewRangeBuffer(dataSize, from, to int, tactic codemode.Tactic, pool *resourcepool.MemPool) (*Buffer, error) {
	return newBuffer(dataSize, from, to, tactic, pool, false)
}

// GetBufferSizes calculate ec buffer sizes
func GetBufferSizes(dataSize int, tactic codemode.Tactic) (BufferSizes, error) {
	buf, err := NewBuffer(dataSize, tactic, nil)
	if err != nil {
		return BufferSizes{}, err
	}
	return buf.BufferSizes, nil
}

// Resize re-calculate ec buffer, alloc a new buffer if oversize
func (b *Buffer) Resize(dataSize int) error {
	if dataSize == b.BufferSizes.DataSize {
		return nil
	}

	sizes, err := GetBufferSizes(dataSize, b.tactic)
	if err != nil {
		return err
	}

	// buffer is enough
	if sizes.ECSize <= cap(b.DataBuf) {
		buf := b.DataBuf[:cap(b.DataBuf)]
		// zero the padding bytes of data section
		b.pool.Zero(buf[sizes.DataSize:sizes.ECDataSize])

		b.DataBuf = buf[:sizes.DataSize]
		b.ECDataBuf = buf[:sizes.ECDataSize]
		b.BufferSizes = sizes
		return nil
	}

	newb, err := NewBuffer(dataSize, b.tactic, b.pool)
	if err != nil {
		return err
	}

	b.Release()
	*b = *newb
	return nil
}

// Release recycles the buffer into pool
func (b *Buffer) Release() error {
	if b == nil {
		return nil
	}
	if b.DataBuf == nil {
		b.pool = nil
		return nil
	}

	buf := b.DataBuf
	b.DataBuf = nil
	b.ECDataBuf = nil

	pool := b.pool
	b.pool = nil
	if pool != nil {
		return pool.Put(buf)
	}

	return nil
}
