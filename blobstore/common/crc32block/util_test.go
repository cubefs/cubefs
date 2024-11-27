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
