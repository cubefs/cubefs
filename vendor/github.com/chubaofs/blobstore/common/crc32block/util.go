package crc32block

import (
	"errors"
	"io"
)

const (
	crc32Len            = 4
	baseBlockBit        = 12
	baseBlockLen        = (1 << baseBlockBit)
	baseBlockPayloadLen = baseBlockLen - crc32Len
)

var (
	ErrInvalidBlock  = errors.New("crc32block: invalid block buffer")
	ErrMismatchedCrc = errors.New("crc32block: mismatched checksum")
)

func isValidBlockLen(blockLen int64) bool {
	return blockLen%baseBlockLen == 0
}

func blockPayload(blockLen int64) int64 {
	return blockLen - crc32Len
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
