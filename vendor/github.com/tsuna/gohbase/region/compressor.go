// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/tsuna/gohbase/compression"
)

type compressor struct {
	compression.Codec
}

func min(x, y uint32) int {
	if x < y {
		return int(x)
	}
	return int(y)
}

func growBuffer(b []byte, sz int) []byte {
	l := len(b) + sz
	if l <= cap(b) {
		return b[:l]
	}
	return append(b, make([]byte, sz)...)
}

func (c *compressor) compressCellblocks(cbs net.Buffers, uncompressedLen uint32) []byte {
	b := newBuffer(4)

	// put uncompressed length
	binary.BigEndian.PutUint32(b, uncompressedLen)

	uncompressedBuffer := newBuffer(min(uncompressedLen, c.ChunkLen()))
	defer freeBuffer(uncompressedBuffer)

	var chunkLen uint32
	var lenOffset int
	for {
		n, err := cbs.Read(uncompressedBuffer)
		if n == 0 {
			break
		}

		// grow for chunk length
		lenOffset = len(b)
		b = growBuffer(b, 4)

		b, chunkLen = c.Encode(uncompressedBuffer[:n], b)

		// write the chunk length
		binary.BigEndian.PutUint32(b[lenOffset:], chunkLen)

		if err == io.EOF {
			break
		} else if err != nil {
			panic(err) // unexpected error
		}
	}
	return b
}

func readN(b []byte, n int) ([]byte, []byte, error) {
	if len(b) < n {
		return nil, nil, fmt.Errorf(
			"short read: want %d bytes, got %d", n, len(b))
	}
	return b[:n], b[n:], nil
}

func readUint32(b []byte) (uint32, []byte, error) {
	head, tail, err := readN(b, 4)
	if err != nil {
		return 0, nil, err
	}
	return binary.BigEndian.Uint32(head), tail, nil
}

// decompressCellblocks decodes block stream format of hadoop.
// The wire format is as follows:
//
//  <length of uncompressed block>
//    <length of compressed chunk><compressed chunk>
//    <length of compressed chunk><compressed chunk>
//    ...
//    <length of compressed chunk><compressed chunk>
//  <length of uncompressed block>
//    <length of compressed chunk><compressed chunk>
//    ...
//  ...
func (c *compressor) decompressCellblocks(b []byte) ([]byte, error) {
	var (
		err                  error
		out                  []byte
		compressedChunk      []byte
		compressedChunkLen   uint32
		uncompressedBlockLen uint32
		uncompressedChunkLen uint32
	)
	for len(b) > 0 {
		// read uncompressed block length
		uncompressedBlockLen, b, err = readUint32(b)
		if err != nil {
			return nil, fmt.Errorf("failed to read uncompressed block length: %w", err)
		}

		// read and decompress encoded chunks until whole block is read
		var uncompressedSoFar uint32
		for uncompressedSoFar < uncompressedBlockLen {
			compressedChunkLen, b, err = readUint32(b)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to read compressed chunk block length: %w", err)
			}

			compressedChunk, b, err = readN(b, int(compressedChunkLen))
			if err != nil {
				return nil, fmt.Errorf("failed to read compressed chunk: %w", err)
			}
			out, uncompressedChunkLen, err = c.Decode(compressedChunk, out)
			if err != nil {
				return nil, fmt.Errorf("failed to decode compressed chunk: %w", err)
			}
			uncompressedSoFar += uncompressedChunkLen
		}

		// check that uncompressed lengths add up
		if uncompressedSoFar > uncompressedBlockLen {
			return nil, fmt.Errorf(
				"uncompressed more than expected: expected %d, got %d so far",
				uncompressedBlockLen, uncompressedSoFar)
		}
	}
	return out, nil
}
