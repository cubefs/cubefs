// Copyright 2024 The CubeFS Authors.
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

package clustermgr

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type ChunkInfo struct {
	Id         ChunkID      `json:"id"`
	Vuid       proto.Vuid   `json:"vuid"`
	DiskID     proto.DiskID `json:"diskid"`
	Total      uint64       `json:"total"`  // ChunkSize
	Used       uint64       `json:"used"`   // user data size
	Free       uint64       `json:"free"`   // ChunkSize - Used
	Size       uint64       `json:"size"`   // Chunk File Size (logic size)
	Status     ChunkStatus  `json:"status"` // normal、readOnly
	Compacting bool         `json:"compacting"`
}

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
	ChunkIDLength     = chunkVuidLen + chunkTimestmapLen
)

var InvalidChunkID ChunkID = [ChunkIDLength]byte{}

var (
	_vuidHexLen      = hex.EncodedLen(chunkVuidLen)
	_timestampHexLen = hex.EncodedLen(chunkTimestmapLen)

	// ${vuid_hex}-${tiemstamp_hex}
	// |-- 8 Bytes --|-- 1 Bytes --|-- 8 Bytes --|
	delimiter        = []byte("-")
	ChunkIDEncodeLen = _vuidHexLen + _timestampHexLen + len(delimiter)
)

type (
	ChunkID     [ChunkIDLength]byte
	ChunkStatus uint8
)

func (c ChunkID) UnixTime() uint64 {
	return binary.BigEndian.Uint64(c[chunkVuidLen:ChunkIDLength])
}

func (c ChunkID) VolumeUnitId() proto.Vuid {
	return proto.Vuid(binary.BigEndian.Uint64(c[:chunkVuidLen]))
}

func (c *ChunkID) Marshal() ([]byte, error) {
	buf := make([]byte, ChunkIDEncodeLen)
	var i int

	hex.Encode(buf[i:_vuidHexLen], c[:chunkVuidLen])
	i += _vuidHexLen

	copy(buf[i:i+len(delimiter)], delimiter)
	i += len(delimiter)

	hex.Encode(buf[i:], c[chunkVuidLen:ChunkIDLength])

	return buf, nil
}

func (c *ChunkID) Unmarshal(data []byte) error {
	if len(data) != ChunkIDEncodeLen {
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

func (c ChunkID) String() string {
	buf, _ := c.Marshal()
	return string(buf[:])
}

func (c ChunkID) MarshalJSON() ([]byte, error) {
	b := make([]byte, ChunkIDEncodeLen+2)
	b[0], b[ChunkIDEncodeLen+1] = '"', '"'

	buf, _ := c.Marshal()
	copy(b[1:], buf)

	return b, nil
}

func (c *ChunkID) UnmarshalJSON(data []byte) (err error) {
	if len(data) != ChunkIDEncodeLen+2 {
		return errors.New("failed unmarshal json")
	}

	return c.Unmarshal(data[1 : ChunkIDEncodeLen+1])
}

func EncodeChunk(id ChunkID) string {
	return id.String()
}

func NewChunkID(vuid proto.Vuid) (chunkId ChunkID) {
	binary.BigEndian.PutUint64(chunkId[:chunkVuidLen], uint64(vuid))
	binary.BigEndian.PutUint64(chunkId[chunkVuidLen:ChunkIDLength], uint64(time.Now().UnixNano()))
	return
}

func DecodeChunk(name string) (id ChunkID, err error) {
	buf := []byte(name)
	if len(buf) != ChunkIDEncodeLen {
		return InvalidChunkID, errors.New("invalid chunk name")
	}

	if err = id.Unmarshal(buf); err != nil {
		return InvalidChunkID, errors.New("chunk unmarshal failed")
	}

	return
}

func (s *ChunkStatus) String() string {
	switch *s {
	case ChunkStatusDefault:
		return "default"
	case ChunkStatusNormal:
		return "normal"
	case ChunkStatusReadOnly:
		return "readOnly"
	case ChunkStatusRelease:
		return "release"
	default:
		return "unkown"
	}
}
