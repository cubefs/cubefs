// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"bytes"
	"errors"
	"net"
	"strconv"
	"testing"

	"github.com/tsuna/gohbase/test"
)

// mockCodec just takes the the source and appends it to destination
type mockCodec struct{}

func (mc mockCodec) Encode(src, dst []byte) ([]byte, uint32) {
	return append(dst, src...), uint32(len(src))
}

func (mc mockCodec) Decode(src, dst []byte) ([]byte, uint32, error) {
	return append(dst, src...), uint32(len(src)), nil
}

func (mc mockCodec) ChunkLen() uint32 {
	return 10
}

func (mc mockCodec) CellBlockCompressorClass() string {
	return "mock"
}

func TestCompressCellblocks(t *testing.T) {
	tests := []struct {
		cellblocks      net.Buffers
		uncompressedLen uint32
		out             []byte
	}{
		{
			cellblocks:      nil,
			uncompressedLen: 0,
			out:             []byte("\x00\x00\x00\x00"),
		},
		{ // 1 chunk
			cellblocks:      net.Buffers{[]byte("123"), []byte("456"), []byte("789")},
			uncompressedLen: 9,
			out:             []byte("\x00\x00\x00\t\x00\x00\x00\t123456789"),
		},
		{ // 1 chunk
			cellblocks:      net.Buffers{[]byte("12345"), []byte("67890")},
			uncompressedLen: 10,
			out:             []byte("\x00\x00\x00\n\x00\x00\x00\n1234567890"),
		},
		{ // 2 chunks
			cellblocks:      net.Buffers{[]byte("12345"), []byte("67890"), []byte("a")},
			uncompressedLen: 11,
			out:             []byte("\x00\x00\x00\v\x00\x00\x00\n1234567890\x00\x00\x00\x01a"),
		},
		{ // 2 chunks
			cellblocks:      net.Buffers{[]byte("1234567890a")},
			uncompressedLen: 11,
			out:             []byte("\x00\x00\x00\v\x00\x00\x00\n1234567890\x00\x00\x00\x01a"),
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			c := &compressor{Codec: mockCodec{}}
			out := c.compressCellblocks(tcase.cellblocks, tcase.uncompressedLen)

			if !bytes.Equal(tcase.out, out) {
				t.Errorf("expected out %q, got %q", tcase.out, out)
			}
		})
	}
}

func TestDecompressCellblocks(t *testing.T) {
	tests := []struct {
		in  []byte
		out []byte
		err error
	}{
		{
			in: []byte("\x00\x00\x00\x00"),
		},
		{ // 1 chunk
			in:  []byte("\x00\x00\x00\t\x00\x00\x00\t123456789"),
			out: []byte("123456789"),
		},
		{ // 2 chunks
			in:  []byte("\x00\x00\x00\v\x00\x00\x00\n1234567890\x00\x00\x00\x01a"),
			out: []byte("1234567890a"),
		},
		{
			in:  nil,
			out: nil,
		},
		{
			in: []byte("\x00\x00"),
			err: errors.New(
				"failed to read uncompressed block length: short read: want 4 bytes, got 2"),
		},
		{
			in: []byte("\x00\x00\x00\v\x00"),
			err: errors.New(
				"failed to read compressed chunk block length: short read: want 4 bytes, got 1"),
		},
		{
			in:  []byte("\x00\x00\x00\v\x00\x00\x00\n123"),
			err: errors.New("failed to read compressed chunk: short read: want 10 bytes, got 3"),
		},
		{
			in:  []byte("\x00\x00\x00\t\x00\x00\x00\n1234567890\x00\x00\x00\x01a"),
			err: errors.New("uncompressed more than expected: expected 9, got 10 so far"),
		},
		{ // uncompressed block length is larger than chunk's uncompressed length
			in: []byte("\x00\x00\x00\v\x00\x00\x00\t123456789"),
			err: errors.New(
				"failed to read compressed chunk block length: short read: want 4 bytes, got 0"),
		},
		{ // 2 blocks with 2 chunks each
			in: []byte("\x00\x00\x00\v\x00\x00\x00\n1234567890\x00\x00\x00\x01a" +
				"\x00\x00\x00\v\x00\x00\x00\n1234567890\x00\x00\x00\x01a"),
			out: []byte("1234567890a1234567890a"),
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			c := &compressor{Codec: mockCodec{}}
			out, err := c.decompressCellblocks(tcase.in)

			if !test.ErrEqual(tcase.err, err) {
				t.Errorf("expected err %q, got %q", tcase.err, err)
			}

			if !bytes.Equal(tcase.out, out) {
				t.Errorf("expected out %q, got %q", tcase.out, out)
			}
		})
	}
}
