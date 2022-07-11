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

package storage

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"

	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
)

var (
	defaultSizeThreshold = 8          // 8 Byte
	maxSizeThreshold     = 128 * 1024 // 128 KiB
)

type tinyfileStorage struct {
	*storage
	size int
}

func NewTinyFileStg(underlying core.Storage, size int) core.Storage {
	if size <= 0 {
		size = defaultSizeThreshold
	}
	if size > maxSizeThreshold {
		size = maxSizeThreshold
	}
	return &tinyfileStorage{underlying.(*storage), size}
}

func (stg *tinyfileStorage) canInline(size uint32) bool {
	meta := stg.meta

	if meta.SupportInline() && size <= uint32(stg.size) {
		return true
	}

	return false
}

func (stg *tinyfileStorage) writeToMemory(b *core.Shard) (body []byte, err error) {
	body = make([]byte, b.Size)

	_, err = io.ReadFull(io.LimitReader(b.Body, int64(b.Size)), body)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, bloberr.ErrReaderError
		}
		return nil, err
	}

	b.Offset = 0                     // special offset
	b.Inline = true                  // inline data
	b.Crc = crc32.ChecksumIEEE(body) // calculate crc code

	return body, nil
}

func (stg *tinyfileStorage) Write(ctx context.Context, b *core.Shard) (err error) {
	if !stg.canInline(b.Size) {
		return stg.storage.Write(ctx, b)
	}

	buffer, err := stg.writeToMemory(b)
	if err != nil {
		return err
	}

	// write meta
	return stg.meta.Write(ctx, b.Bid, core.ShardMeta{
		Version: _shardVer[0],
		Size:    b.Size,
		Crc:     b.Crc,
		Offset:  b.Offset,
		Flag:    b.Flag,
		Inline:  true,
		Buffer:  buffer,
	})
}

func (stg *tinyfileStorage) NewRangeReader(ctx context.Context, b *core.Shard, from, to int64) (rc io.Reader, err error) {
	if !b.Inline {
		return stg.storage.NewRangeReader(ctx, b, from, to)
	}

	rc = bytes.NewReader(b.Buffer[from:to])
	return rc, nil
}
