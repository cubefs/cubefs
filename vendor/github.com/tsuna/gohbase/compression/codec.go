// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package compression

import (
	"github.com/tsuna/gohbase/compression/snappy"
)

// Codec is used to encode and decode chunks of hadoop's sequence file chunks.
type Codec interface {
	// Encode encodes uncompressed bytes from src and appends them to dst.
	Encode(src, dst []byte) ([]byte, uint32)
	// Decode decodes compressed bytes from src and appends them to dst.
	Decode(src, dst []byte) ([]byte, uint32, error)
	// ChunkLen returns the maximum size of chunk for particular codec.
	ChunkLen() uint32
	// CellBlockCompressorClass returns full Java class name of the compressor on the HBase side
	CellBlockCompressorClass() string
}

// New instantiates passed codec. Currently supported codes are:
// - snappy
func New(codec string) Codec {
	switch codec {
	case "snappy":
		return snappy.New()
	default:
		panic("uknown compression codec")
	}
}
