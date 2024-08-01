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

package proto

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
)

// BlobUnit is one piece of data in a location
//
// Bid is the blob id
// Vid is which volume the blob in
// Size is real size of the blob
type BlobUnit struct {
	Bid  BlobID
	Vid  Vid
	Size uint32
}

// Copy returns a new same Location
func (loc *Location) Copy() Location {
	dst := Location{
		ClusterID: loc.ClusterID,
		CodeMode:  loc.CodeMode,
		Size_:     loc.Size_,
		SliceSize: loc.SliceSize,
		Crc:       loc.Crc,
		Slices:    make([]Slice, len(loc.Slices)),
	}
	copy(dst.Slices, loc.Slices)
	return dst
}

// Encode transfer Location to slice byte
// Returns the buf created by me
//
//	(n) means max-n bytes
//	- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//	|  field  | crc | clusterid  | codemode |    size     |  blobsize  |
//	- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//	| n-bytes |  4  | uvarint(5) |    1     | uvarint(10) | uvarint(5) |
//	- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//	        25   +  (5){len(blobs)}   +   len(Slices) * 20
//	- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//	|  blobs  | minbid | vid | count |           ...                   |
//	- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//	| n-bytes |  (10)  | (5) |  (5)  | (20) | (20) |       ...         |
//	- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
func (loc *Location) Encode() []byte {
	if loc == nil {
		return nil
	}
	n := 25 + 5 + len(loc.Slices)*20
	buf := make([]byte, n)
	n = loc.Encode2(buf)
	return buf[:n]
}

// Encode2 transfer Location to the buf, the buf reuse by yourself
// Returns the number of bytes read
// If the buffer is too small, Encode2 will panic
func (loc *Location) Encode2(buf []byte) int {
	if loc == nil {
		return 0
	}

	n := 0
	binary.BigEndian.PutUint32(buf[n:], loc.Crc)
	n += 4
	n += binary.PutUvarint(buf[n:], uint64(loc.ClusterID))
	buf[n] = byte(loc.CodeMode)
	n++
	n += binary.PutUvarint(buf[n:], uint64(loc.Size_))
	n += binary.PutUvarint(buf[n:], uint64(loc.SliceSize))

	n += binary.PutUvarint(buf[n:], uint64(len(loc.Slices)))
	for _, blob := range loc.Slices {
		n += binary.PutUvarint(buf[n:], uint64(blob.MinSliceID))
		n += binary.PutUvarint(buf[n:], uint64(blob.Vid))
		n += binary.PutUvarint(buf[n:], uint64(blob.Count))
	}

	return n
}

// Decode parse location from buf
// Returns the number of bytes read
// Error is not nil when parsing failed
func (loc *Location) Decode(buf []byte) (int, error) {
	if loc == nil {
		return 0, fmt.Errorf("location receiver is nil")
	}

	location, n, err := DecodeLocation(buf)
	if err != nil {
		return n, err
	}

	*loc = location
	return n, nil
}

// ToString transfer location to hex string
func (loc *Location) ToString() string {
	return loc.HexString()
}

// HexString transfer location to hex string
func (loc *Location) HexString() string {
	return hex.EncodeToString(loc.Encode())
}

// Base64String transfer location to base64 string
func (loc *Location) Base64String() string {
	return base64.StdEncoding.EncodeToString(loc.Encode())
}

// Spread location blobs to slice
func (loc *Location) Spread() []BlobUnit {
	count := 0
	for _, blob := range loc.Slices {
		count += int(blob.Count)
	}

	blobs := make([]BlobUnit, 0, count)
	for _, blob := range loc.Slices {
		for offset := uint32(0); offset < blob.Count; offset++ {
			blobs = append(blobs, BlobUnit{
				Bid:  blob.MinSliceID + BlobID(offset),
				Vid:  blob.Vid,
				Size: loc.SliceSize,
			})
		}
	}
	if len(blobs) > 0 && loc.SliceSize > 0 {
		if lastSize := loc.Size_ % uint64(loc.SliceSize); lastSize > 0 {
			blobs[len(blobs)-1].Size = uint32(lastSize)
		}
	}
	return blobs
}

// DecodeLocation parse location from buf
// Returns Location and the number of bytes read
// Error is not nil when parsing failed
func DecodeLocation(buf []byte) (Location, int, error) {
	var (
		loc Location
		n   int

		val uint64
		nn  int
	)
	next := func() (uint64, int) {
		val, nn := binary.Uvarint(buf)
		if nn <= 0 {
			return 0, nn
		}
		n += nn
		buf = buf[nn:]
		return val, nn
	}

	if len(buf) < 4 {
		return loc, n, fmt.Errorf("bytes crc %d", len(buf))
	}
	loc.Crc = binary.BigEndian.Uint32(buf)
	n += 4
	buf = buf[4:]

	if val, nn = next(); nn <= 0 {
		return loc, n, fmt.Errorf("bytes cluster_id %d", nn)
	}
	loc.ClusterID = ClusterID(val)

	if len(buf) < 1 {
		return loc, n, fmt.Errorf("bytes codemode %d", len(buf))
	}
	loc.CodeMode = codemode.CodeMode(buf[0])
	n++
	buf = buf[1:]

	if val, nn = next(); nn <= 0 {
		return loc, n, fmt.Errorf("bytes size %d", nn)
	}
	loc.Size_ = val

	if val, nn = next(); nn <= 0 {
		return loc, n, fmt.Errorf("bytes blob_size %d", nn)
	}
	loc.SliceSize = uint32(val)

	if val, nn = next(); nn <= 0 {
		return loc, n, fmt.Errorf("bytes length blobs %d", nn)
	}
	length := int(val)

	if length > 0 {
		loc.Slices = make([]Slice, 0, length)
	}
	for index := 0; index < length; index++ {
		var blob Slice
		if val, nn = next(); nn <= 0 {
			return loc, n, fmt.Errorf("bytes %dth-blob min_bid %d", index, nn)
		}
		blob.MinSliceID = BlobID(val)

		if val, nn = next(); nn <= 0 {
			return loc, n, fmt.Errorf("bytes %dth-blob vid %d", index, nn)
		}
		blob.Vid = Vid(val)

		if val, nn = next(); nn <= 0 {
			return loc, n, fmt.Errorf("bytes %dth-blob count %d", index, nn)
		}
		blob.Count = uint32(val)

		loc.Slices = append(loc.Slices, blob)
	}

	return loc, n, nil
}

// DecodeLocationFrom decode location from hex string
func DecodeLocationFrom(s string) (Location, error) {
	return DecodeLocationFromHex(s)
}

// DecodeLocationFromHex decode location from hex string
func DecodeLocationFromHex(s string) (Location, error) {
	var loc Location
	src, err := hex.DecodeString(s)
	if err != nil {
		return loc, err
	}
	_, err = loc.Decode(src)
	if err != nil {
		return loc, err
	}
	return loc, nil
}

// DecodeLocationFromBase64 decode location from base64 string
func DecodeLocationFromBase64(s string) (Location, error) {
	var loc Location
	src, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return loc, err
	}
	_, err = loc.Decode(src)
	if err != nil {
		return loc, err
	}
	return loc, nil
}
