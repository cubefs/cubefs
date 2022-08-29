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
	"errors"
	"io"
)

const (
	crc32Len     = 4
	baseBlockBit = 12
	baseBlockLen = (1 << baseBlockBit)
)

var (
	ErrInvalidBlock  = errors.New("crc32block: invalid block buffer")
	ErrMismatchedCrc = errors.New("crc32block: mismatched checksum")
	ErrReadOnClosed  = errors.New("crc32block: read on closed")
)

func isValidBlockLen(blockLen int64) bool {
	return blockLen > 0 && blockLen%baseBlockLen == 0
}

func blockPayload(blockLen int64) int64 {
	return blockLen - crc32Len
}

// SetBlockSize set default block size
func SetBlockSize(blockSize int64) {
	if !isValidBlockLen(blockSize) {
		panic(ErrInvalidBlock)
	}
	gBlockSize = blockSize
}

func EncodeSize(size int64, blockLen int64) int64 {
	if !isValidBlockLen(blockLen) {
		panic(ErrInvalidBlock)
	}
	payload := blockPayload(blockLen)
	blockCnt := (size + (payload - 1)) / payload
	return size + 4*blockCnt
}

func DecodeSize(totalSize int64, blockLen int64) int64 {
	if !isValidBlockLen(blockLen) {
		panic(ErrInvalidBlock)
	}
	blockCnt := (totalSize + (blockLen - 1)) / blockLen
	return totalSize - 4*blockCnt
}

func EncodeSizeWithDefualtBlock(size int64) int64 {
	return EncodeSize(size, defaultCrc32BlockSize)
}

func DecodeSizeWithDefualtBlock(size int64) int64 {
	return DecodeSize(size, defaultCrc32BlockSize)
}

func readFullOrToEnd(r io.Reader, buffer []byte) (n int, err error) {
	nn, size := 0, len(buffer)

	for n < size && err == nil {
		nn, err = r.Read(buffer[n:])
		n += nn
		if n != 0 && err == io.EOF {
			return n, nil
		}
	}

	return n, err
}
