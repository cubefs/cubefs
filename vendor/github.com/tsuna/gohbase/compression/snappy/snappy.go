// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package snappy

import (
	"github.com/golang/snappy"
)

// snapppyChunkLen is based on constants in org.apache.hadoop.io.compress.SnappyCodec
// it's buffer len (256*1024) minus snappy overhead.
const snappyChunkLen = 256*1024*5/6 - 32

type snappyCodec struct{}

func New() snappyCodec {
	return snappyCodec{}
}

func (sc snappyCodec) Encode(src, dst []byte) ([]byte, uint32) {
	chunk := snappy.Encode(dst[len(dst):cap(dst)], src)
	// append the chunk, this can be O(1) in case Encode had enough capacity
	return append(dst, chunk...), uint32(len(chunk))
}

func (sc snappyCodec) Decode(src, dst []byte) ([]byte, uint32, error) {
	chunk, err := snappy.Decode(dst[len(dst):cap(dst)], src)
	if err != nil {
		return nil, 0, err
	}
	return append(dst, chunk...), uint32(len(chunk)), nil
}

func (sc snappyCodec) ChunkLen() uint32 {
	return snappyChunkLen
}

func (sc snappyCodec) CellBlockCompressorClass() string {
	return "org.apache.hadoop.io.compress.SnappyCodec"
}
