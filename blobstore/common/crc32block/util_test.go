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

package crc32block

import (
	crand "crypto/rand"
	"hash/crc32"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeSize(t *testing.T) {
	datas := []struct {
		blockLen   int64
		fsize      int64
		encodeSize int64
	}{
		{blockLen: 64 * 1024, fsize: 1, encodeSize: 5},
		{blockLen: 64 * 1024, fsize: 64*1024 - 4, encodeSize: 64 * 1024},
		{blockLen: 64 * 1024, fsize: 64 * 1024, encodeSize: 64*1024 + 8},
	}

	for _, dt := range datas {
		require.Equal(t, int64(dt.encodeSize), EncodeSize(dt.fsize, dt.blockLen))
		require.Equal(t, int64(dt.fsize), DecodeSize(dt.encodeSize, dt.blockLen))
	}
}

func TestPartialCodeSize(t *testing.T) {
	run := func(actual, stable int64) {
		totalSize, tail := PartialEncodeSize(actual, stable)
		decodeSize := PartialDecodeSize(totalSize, tail, stable)
		require.True(t, totalSize%_alignment == 0, totalSize)
		require.Equal(t, actual, decodeSize)
	}
	run(0, 0)
	run(0, 1)
	run(1, 0)
	run(1, 513)
	for range [100000]struct{}{} {
		run(rand.Int63n(16<<20), rand.Int63n(16<<20))
	}
}

func TestSetBlockSize(t *testing.T) {
	for _, size := range []int64{-100, -1, 0, baseBlockLen - 1, baseBlockLen + 1} {
		require.Panics(t, func() { SetBlockSize(size) })
	}

	defer func() {
		SetBlockSize(defaultCrc32BlockSize)
	}()

	datas := []struct {
		blockLen   int64
		fsize      int64
		encodeSize int64
	}{
		{blockLen: 1 << 12, fsize: 1, encodeSize: 5},
		{blockLen: 1 << 20, fsize: 64*1024 - 4, encodeSize: 64 * 1024},
		{blockLen: 64 * 1024, fsize: 64 * 1024, encodeSize: 64*1024 + 8},
	}
	for _, dt := range datas {
		SetBlockSize(dt.blockLen)
		require.Equal(t, dt.encodeSize, NewBodyEncoder(nil).CodeSize(dt.fsize))
		require.Equal(t, dt.fsize, NewBodyDecoder(nil).CodeSize(dt.encodeSize))
	}
}

func TestConstZeroCrc(t *testing.T) {
	require.Equal(t, uint32(0), ConstZeroCrc(-1))
	require.Equal(t, uint32(0), ConstZeroCrc(0))
	for range [100]struct{}{} {
		size := int(rand.Int31n(1 << 20))
		act := crc32.ChecksumIEEE(make([]byte, size))
		get1 := ConstZeroCrc(size)
		get2 := ConstZeroCrc(size)
		require.Equal(t, act, get1)
		require.Equal(t, act, get2)
	}
}

func TestIsZeroBuffer(t *testing.T) {
	require.True(t, IsZeroBuffer(nil))
	for _, size := range []int{1, 3, 1 << 10, 1 << 19} {
		buffer := make([]byte, size)
		require.True(t, IsZeroBuffer(buffer))
		buffer[0] = 'a'
		require.False(t, IsZeroBuffer(buffer))
		buffer[0] = 0
		buffer[len(buffer)-1] = 'z'
		require.False(t, IsZeroBuffer(buffer))
		buffer[len(buffer)-1] = 0
		require.True(t, IsZeroBuffer(buffer))
		crand.Read(buffer)
		require.False(t, IsZeroBuffer(buffer))
	}
}

func BenchmarkIsZeroBuffer(b *testing.B) {
	b.Run("1M-zero-all", func(b *testing.B) {
		buffer := make([]byte, 1<<20)
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			IsZeroBuffer(buffer)
		}
	})
	b.Run("1M-not-zero-first", func(b *testing.B) {
		buffer := make([]byte, 1<<20)
		buffer[7] = 1
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			IsZeroBuffer(buffer)
		}
	})
	b.Run("1M-not-zero-mid", func(b *testing.B) {
		buffer := make([]byte, 1<<20)
		buffer[1<<19] = 1
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			IsZeroBuffer(buffer)
		}
	})
	b.Run("1M-not-zero-last", func(b *testing.B) {
		buffer := make([]byte, 1<<20)
		buffer[(1<<20)-1] = 1
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			IsZeroBuffer(buffer)
		}
	})
}
