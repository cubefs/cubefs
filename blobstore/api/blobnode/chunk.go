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

package blobnode

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	ChunkStatusDefault  ChunkStatus = iota // 0
	ChunkStatusNormal                      // 1
	ChunkStatusReadOnly                    // 2
	ChunkStatusRelease                     // 3
	ChunkNumStatus                         // 4
)

const (
	ReleaseForUser    = "release for user"
	ReleaseForCompact = "release for compact"
)

// Chunk ID
// vuid + timestamp
const (
	chunkVuidLen      = 8
	chunkTimestmapLen = 8
	ChunkIdLength     = chunkVuidLen + chunkTimestmapLen
)

var InvalidChunkId ChunkId = [ChunkIdLength]byte{}

var (
	_vuidHexLen      = hex.EncodedLen(chunkVuidLen)
	_timestampHexLen = hex.EncodedLen(chunkTimestmapLen)

	// ${vuid_hex}-${tiemstamp_hex}
	// |-- 8 Bytes --|-- 1 Bytes --|-- 8 Bytes --|
	delimiter        = []byte("-")
	ChunkIdEncodeLen = _vuidHexLen + _timestampHexLen + len(delimiter)
)

type (
	ChunkId     [ChunkIdLength]byte
	ChunkStatus uint8
)

func (c ChunkId) UnixTime() uint64 {
	return binary.BigEndian.Uint64(c[chunkVuidLen:ChunkIdLength])
}

func (c ChunkId) VolumeUnitId() proto.Vuid {
	return proto.Vuid(binary.BigEndian.Uint64(c[:chunkVuidLen]))
}

func (c *ChunkId) Marshal() ([]byte, error) {
	buf := make([]byte, ChunkIdEncodeLen)
	var i int

	hex.Encode(buf[i:_vuidHexLen], c[:chunkVuidLen])
	i += _vuidHexLen

	copy(buf[i:i+len(delimiter)], delimiter)
	i += len(delimiter)

	hex.Encode(buf[i:], c[chunkVuidLen:ChunkIdLength])

	return buf, nil
}

func (c *ChunkId) Unmarshal(data []byte) error {
	if len(data) != ChunkIdEncodeLen {
		panic(errors.New("chunk buf size not match"))
	}

	var i int

	_, err := hex.Decode(c[:chunkVuidLen], data[i:_vuidHexLen])
	if err != nil {
		return err
	}

	i += _vuidHexLen
	i += len(delimiter)

	_, err = hex.Decode(c[chunkVuidLen:], data[i:])
	if err != nil {
		return err
	}

	return nil
}

func (c ChunkId) String() string {
	buf, _ := c.Marshal()
	return string(buf[:])
}

func (c ChunkId) MarshalJSON() ([]byte, error) {
	b := make([]byte, ChunkIdEncodeLen+2)
	b[0], b[ChunkIdEncodeLen+1] = '"', '"'

	buf, _ := c.Marshal()
	copy(b[1:], buf)

	return b, nil
}

func (c *ChunkId) UnmarshalJSON(data []byte) (err error) {
	if len(data) != ChunkIdEncodeLen+2 {
		return errors.New("failed unmarshal json")
	}

	return c.Unmarshal(data[1 : ChunkIdEncodeLen+1])
}

func EncodeChunk(id ChunkId) string {
	return id.String()
}

func NewChunkId(vuid proto.Vuid) (chunkId ChunkId) {
	binary.BigEndian.PutUint64(chunkId[:chunkVuidLen], uint64(vuid))
	binary.BigEndian.PutUint64(chunkId[chunkVuidLen:ChunkIdLength], uint64(time.Now().UnixNano()))
	return
}

func IsValidDiskID(id proto.DiskID) bool {
	return id != proto.InvalidDiskID
}

func IsValidChunkId(id ChunkId) bool {
	return id != InvalidChunkId
}

func IsValidChunkStatus(status ChunkStatus) bool {
	return status < ChunkNumStatus
}

func DecodeChunk(name string) (id ChunkId, err error) {
	buf := []byte(name)
	if len(buf) != ChunkIdEncodeLen {
		return InvalidChunkId, errors.New("invalid chunk name")
	}

	if err = id.Unmarshal(buf); err != nil {
		return InvalidChunkId, errors.New("chunk unmarshal failed")
	}

	return
}

type CreateChunkArgs struct {
	DiskID    proto.DiskID `json:"diskid"`
	Vuid      proto.Vuid   `json:"vuid"`
	ChunkSize int64        `json:"chunksize,omitempty"`
}

func (c *client) CreateChunk(ctx context.Context, host string, args *CreateChunkArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/create/diskid/%v/vuid/%v?chunksize=%v",
		host, args.DiskID, args.Vuid, args.ChunkSize)

	err = c.PostWith(ctx, urlStr, nil, nil)
	return
}

type StatChunkArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
}

func (c *client) StatChunk(ctx context.Context, host string, args *StatChunkArgs) (ci *ChunkInfo, err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/stat/diskid/%v/vuid/%v", host, args.DiskID, args.Vuid)
	ci = new(ChunkInfo)
	err = c.GetWith(ctx, urlStr, ci)
	return
}

type ChangeChunkStatusArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
	Force  bool         `json:"force,omitempty"`
}

type ChunkInspectArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
}

func (c *client) ReleaseChunk(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/release/diskid/%v/vuid/%v?force=%v", host, args.DiskID, args.Vuid, args.Force)
	err = c.PostWith(ctx, urlStr, nil, nil)
	return
}

func (c *client) SetChunkReadonly(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/readonly/diskid/%v/vuid/%v", host, args.DiskID, args.Vuid)

	err = c.PostWith(ctx, urlStr, nil, nil)
	return
}

func (c *client) SetChunkReadwrite(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/readwrite/diskid/%v/vuid/%v", host, args.DiskID, args.Vuid)
	err = c.PostWith(ctx, urlStr, nil, nil)
	return
}

type ListChunkArgs struct {
	DiskID proto.DiskID `json:"diskid"`
}

type ListChunkRet struct {
	ChunkInfos []*ChunkInfo `json:"chunk_infos"`
}

func (c *client) ListChunks(ctx context.Context, host string, args *ListChunkArgs) (ret []*ChunkInfo, err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/list/diskid/%v", host, args.DiskID)

	listRet := &ListChunkRet{}
	err = c.GetWith(ctx, urlStr, listRet)
	if err != nil {
		return nil, err
	}

	return listRet.ChunkInfos, nil
}

type CompactChunkArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
}

type DiskProbeArgs struct {
	Path string `json:"path"`
}

type BadShard struct {
	DiskID proto.DiskID
	Vuid   proto.Vuid
	Bid    proto.BlobID
}
