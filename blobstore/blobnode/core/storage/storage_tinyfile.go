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

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
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

	b.Crc = crc32.ChecksumIEEE(body) // calculate crc code

	return body, nil
}

func (stg *tinyfileStorage) Write(ctx context.Context, b *core.Shard) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	var shardMeta core.ShardMeta
	var shardMetaErr error
	dataConfig := stg.data.GetConfig()

	// only verify user write data io from sdk or access, not verify background io
	// avoid to repair cannot write due to meta.Crc == this_call_data.Crc but meta.Crc != exists_old_data.Crc
	needVerifyBeforeWrite := bnapi.GetIoType(ctx) == bnapi.WriteIO &&
		dataConfig != nil &&
		dataConfig.EnablePutShardVerify

	if needVerifyBeforeWrite {
		shardMeta, shardMetaErr = stg.meta.Read(ctx, b.Bid)
	}

	var buffer []byte
	if b.NopData {
		b.Offset = 0
		b.Crc = crc32block.ConstZeroCrc(int(b.Size))
	} else if stg.canInline(b.Size) {
		if buffer, err = stg.writeToMemory(b); err != nil {
			return err
		}
		b.Offset = 0    // special offset
		b.Inline = true // inline data
	} else {
		// pre-check before writing for Chunk IO meta/data Path
		if needVerifyBeforeWrite && shardMetaErr == nil {
			bodyBytes, err := stg.writeToMemory(b)
			if err != nil {
				return err
			}
			if b.Crc == shardMeta.Crc {
				b.Offset = shardMeta.Offset
				b.Inline = shardMeta.Inline
				span.Debugf("bid %v already exists in meta shard_meta:%+v", b.Bid, shardMeta)
				return nil
			}

			// CRC mismatch and reconstruct shard body, fall through
			b.Body = bytes.NewReader(bodyBytes)
		}
		return stg.storage.Write(ctx, b)
	}

	// pre-check before writing for NopData and Inline IO Path
	if needVerifyBeforeWrite && shardMetaErr == nil {
		if b.Crc == shardMeta.Crc {
			span.Debugf("bid %v already exists in meta shard_meta:%+v", b.Bid, shardMeta)
			return nil
		}
	}

	// write meta
	return stg.meta.Write(ctx, b.Bid, core.ShardMeta{
		Version: _shardVer[0],
		Size:    b.Size,
		Crc:     b.Crc,
		Offset:  b.Offset,
		Flag:    b.Flag,
		NopData: b.NopData,
		Inline:  b.Inline,
		Buffer:  buffer,
	})
}

func (stg *tinyfileStorage) NewRangeReader(ctx context.Context, b *core.Shard, from, to int64) (rc io.ReadCloser, err error) {
	if b.NopData {
		return io.NopCloser(util.ZeroReader(int(to - from))), nil
	}

	if !b.Inline {
		return stg.storage.NewRangeReader(ctx, b, from, to)
	}

	r := bytes.NewReader(b.Buffer[from:to])
	rc = io.NopCloser(r)
	return rc, nil
}
