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

package access

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// HashAlgorithm hash.Hash algorithm when uploading data
type HashAlgorithm uint8

// defined hash algorithm
const (
	HashAlgDummy  HashAlgorithm = 1 << iota
	HashAlgCRC32                // crc32 with IEEE
	HashAlgMD5                  // md5
	HashAlgSHA1                 // sha1
	HashAlgSHA256               // sha256
)

const (
	// HashSize dummy hash size
	HashSize = 0

	// MaxLocationBlobs max blobs length in Location
	MaxLocationBlobs uint32 = 4
	// MaxDeleteLocations max locations of delete request
	MaxDeleteLocations int = 1024
	// MaxBlobSize max blob size for allocation
	MaxBlobSize uint32 = 1 << 25 // 32MB
)

type dummyHash struct{}

var _ hash.Hash = (*dummyHash)(nil)

// implements hash.Hash
func (d dummyHash) Write(p []byte) (n int, err error) { return len(p), nil }
func (d dummyHash) Sum(b []byte) []byte               { return []byte{} }
func (d dummyHash) Reset()                            {}
func (d dummyHash) Size() int                         { return 0 }
func (d dummyHash) BlockSize() int                    { return 0 }

// ToHasher returns a new hash.Hash computing checksum
// the value of algorithm should be one of HashAlg*
func (alg HashAlgorithm) ToHasher() hash.Hash {
	switch alg {
	case HashAlgCRC32:
		return crc32.NewIEEE()
	case HashAlgMD5:
		return md5.New()
	case HashAlgSHA1:
		return sha1.New()
	case HashAlgSHA256:
		return sha256.New()
	default:
		return dummyHash{}
	}
}

// ToHashSumMap returns a new HashSumMap, decode from rpc url argument
func (alg HashAlgorithm) ToHashSumMap() HashSumMap {
	h := make(HashSumMap)
	for _, a := range []HashAlgorithm{
		HashAlgDummy,
		HashAlgCRC32,
		HashAlgMD5,
		HashAlgSHA1,
		HashAlgSHA256,
	} {
		if alg&a == a {
			h[a] = nil
		}
	}
	return h
}

// HasherMap map hasher of HashAlgorithm
type HasherMap map[HashAlgorithm]hash.Hash

// ToHashAlgorithm returns HashAlgorithm
func (h HasherMap) ToHashAlgorithm() HashAlgorithm {
	alg := HashAlgorithm(0)
	for k := range h {
		alg |= k
	}
	return alg
}

// ToWriter returns io writer
func (h HasherMap) ToWriter() io.Writer {
	writers := make([]io.Writer, 0, len(h))
	for _, hasher := range h {
		writers = append(writers, hasher)
	}
	return io.MultiWriter(writers...)
}

// HashSumMap save checksum in rpc calls
type HashSumMap map[HashAlgorithm][]byte

// GetSum get checksum value and ok via HashAlgorithm
//   HashAlgDummy  returns nil, bool
//   HashAlgCRC32  returns uint32, bool
//   HashAlgMD5    returns string(32), bool
//   HashAlgSHA1   returns string(40), bool
//   HashAlgSHA256 returns string(64), bool
func (h HashSumMap) GetSum(key HashAlgorithm) (interface{}, bool) {
	b, ok := h[key]
	if !ok {
		return nil, false
	}

	switch key {
	case HashAlgCRC32:
		if len(b) != crc32.Size {
			return nil, false
		}
		return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24, true
	case HashAlgMD5:
		if len(b) != md5.Size {
			return nil, false
		}
		return hex.EncodeToString(b[:]), true
	case HashAlgSHA1:
		if len(b) != sha1.Size {
			return nil, false
		}
		return hex.EncodeToString(b[:]), true
	case HashAlgSHA256:
		if len(b) != sha256.Size {
			return nil, false
		}
		return hex.EncodeToString(b[:]), true
	default:
		if len(b) != HashSize {
			return nil, false
		}
		return nil, true
	}
}

// GetSumVal get checksum only value via HashAlgorithm
func (h HashSumMap) GetSumVal(key HashAlgorithm) interface{} {
	val, _ := h.GetSum(key)
	return val
}

// ToHashAlgorithm returns HashAlgorithm, encode to rpc url argument
func (h HashSumMap) ToHashAlgorithm() HashAlgorithm {
	alg := HashAlgorithm(0)
	for k := range h {
		alg |= k
	}
	return alg
}

// All returns readable checksum
func (h HashSumMap) All() map[string]interface{} {
	m := make(map[string]interface{})
	for a, name := range map[HashAlgorithm]string{
		HashAlgCRC32:  "crc32",
		HashAlgMD5:    "md5",
		HashAlgSHA1:   "sha1",
		HashAlgSHA256: "sha256",
	} {
		if val, ok := h.GetSum(a); ok {
			m[name] = val
		}
	}
	return m
}

// Location file location, 4 + 1 + 8 + 4 + 4 + len*16 bytes
// |                                        |
// |   ClusterID(4)    |    CodeMode(1)     |
// |                Size(8)                 |
// |   BlobSize(4)     |      Crc(4)        |
// |           len*SliceInfo(16)            |
//
// ClusterID which cluster file is in
// CodeMode is ec encode mode, see defined in "common/lib/codemode"
// Size is file size
// BlobSize is every blob's size but the last one which's size=(Size mod BlobSize)
// Crc is the checksum, change anything of the location, crc will mismatch
// Blobs all blob information
type Location struct {
	_         [0]byte
	ClusterID proto.ClusterID   `json:"cluster_id"`
	CodeMode  codemode.CodeMode `json:"code_mode"`
	Size      uint64            `json:"size"`
	BlobSize  uint32            `json:"blob_size"`
	Crc       uint32            `json:"crc"`
	Blobs     []SliceInfo       `json:"blobs"`
}

// SliceInfo blobs info, 8 + 4 + 4 bytes
//
// MinBid is the first blob id
// Vid is which volume all blobs in
// Count is num of consecutive blob ids, count=1 just has one blob
//
// blob ids = [MinBid, MinBid+count)
type SliceInfo struct {
	_      [0]byte
	MinBid proto.BlobID `json:"min_bid"`
	Vid    proto.Vid    `json:"vid"`
	Count  uint32       `json:"count"`
}

// Blob is one piece of data in a location
//
// Bid is the blob id
// Vid is which volume the blob in
// Size is real size of the blob
type Blob struct {
	Bid  proto.BlobID
	Vid  proto.Vid
	Size uint32
}

// Copy returns a new same Location
func (loc *Location) Copy() Location {
	dst := Location{
		ClusterID: loc.ClusterID,
		CodeMode:  loc.CodeMode,
		Size:      loc.Size,
		BlobSize:  loc.BlobSize,
		Crc:       loc.Crc,
		Blobs:     make([]SliceInfo, len(loc.Blobs)),
	}
	copy(dst.Blobs, loc.Blobs)
	return dst
}

// Encode transfer Location to slice byte
// Returns the buf created by me
//  (n) means max-n bytes
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |  field  | crc | clusterid  | codemode |    size     |  blobsize  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | n-bytes |  4  | uvarint(5) |    1     | uvarint(10) | uvarint(5) |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//          25   +  (5){len(blobs)}   +   len(Blobs) * 20
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |  blobs  | minbid | vid | count |           ...                   |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | n-bytes |  (10)  | (5) |  (5)  | (20) | (20) |       ...         |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
func (loc *Location) Encode() []byte {
	if loc == nil {
		return nil
	}
	n := 25 + 5 + len(loc.Blobs)*20
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
	n += binary.PutUvarint(buf[n:], uint64(loc.Size))
	n += binary.PutUvarint(buf[n:], uint64(loc.BlobSize))

	n += binary.PutUvarint(buf[n:], uint64(len(loc.Blobs)))
	for _, blob := range loc.Blobs {
		n += binary.PutUvarint(buf[n:], uint64(blob.MinBid))
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
func (loc *Location) Spread() []Blob {
	count := 0
	for _, blob := range loc.Blobs {
		count += int(blob.Count)
	}

	blobs := make([]Blob, 0, count)
	for _, blob := range loc.Blobs {
		for offset := uint32(0); offset < blob.Count; offset++ {
			blobs = append(blobs, Blob{
				Bid:  blob.MinBid + proto.BlobID(offset),
				Vid:  blob.Vid,
				Size: loc.BlobSize,
			})
		}
	}
	if len(blobs) > 0 && loc.BlobSize > 0 {
		if lastSize := loc.Size % uint64(loc.BlobSize); lastSize > 0 {
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
	loc.ClusterID = proto.ClusterID(val)

	if len(buf) < 1 {
		return loc, n, fmt.Errorf("bytes codemode %d", len(buf))
	}
	loc.CodeMode = codemode.CodeMode(buf[0])
	n++
	buf = buf[1:]

	if val, nn = next(); nn <= 0 {
		return loc, n, fmt.Errorf("bytes size %d", nn)
	}
	loc.Size = val

	if val, nn = next(); nn <= 0 {
		return loc, n, fmt.Errorf("bytes blob_size %d", nn)
	}
	loc.BlobSize = uint32(val)

	if val, nn = next(); nn <= 0 {
		return loc, n, fmt.Errorf("bytes length blobs %d", nn)
	}
	length := int(val)

	if length > 0 {
		loc.Blobs = make([]SliceInfo, 0, length)
	}
	for index := 0; index < length; index++ {
		var blob SliceInfo
		if val, nn = next(); nn <= 0 {
			return loc, n, fmt.Errorf("bytes %dth-blob min_bid %d", index, nn)
		}
		blob.MinBid = proto.BlobID(val)

		if val, nn = next(); nn <= 0 {
			return loc, n, fmt.Errorf("bytes %dth-blob vid %d", index, nn)
		}
		blob.Vid = proto.Vid(val)

		if val, nn = next(); nn <= 0 {
			return loc, n, fmt.Errorf("bytes %dth-blob count %d", index, nn)
		}
		blob.Count = uint32(val)

		loc.Blobs = append(loc.Blobs, blob)
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

// PutArgs for service /put
// Hashes means how to calculate check sum,
// HashAlgCRC32 | HashAlgMD5 equal 2 + 4 = 6
type PutArgs struct {
	Size   int64         `json:"size"`
	Hashes HashAlgorithm `json:"hashes,omitempty"`
	Body   io.Reader     `json:"-"`
}

// IsValid is valid put args
func (args *PutArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.Size > 0
}

// PutResp put response result
type PutResp struct {
	Location   Location   `json:"location"`
	HashSumMap HashSumMap `json:"hashsum"`
}

// PutAtArgs for service /putat
type PutAtArgs struct {
	ClusterID proto.ClusterID `json:"clusterid"`
	Vid       proto.Vid       `json:"volumeid"`
	Blobid    proto.BlobID    `json:"blobid"`
	Size      int64           `json:"size"`
	Hashes    HashAlgorithm   `json:"hashes,omitempty"`
	Token     string          `json:"token"`
	Body      io.Reader       `json:"-"`
}

// IsValid is valid putat args
func (args *PutAtArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.ClusterID > proto.ClusterID(0) &&
		args.Vid > proto.Vid(0) &&
		args.Blobid > proto.BlobID(0) &&
		args.Size > 0
}

// PutAtResp putat response result
type PutAtResp struct {
	HashSumMap HashSumMap `json:"hashsum"`
}

// AllocArgs for service /alloc
type AllocArgs struct {
	Size            uint64            `json:"size"`
	BlobSize        uint32            `json:"blob_size"`
	AssignClusterID proto.ClusterID   `json:"assign_cluster_id"`
	CodeMode        codemode.CodeMode `json:"code_mode"`
}

// IsValid is valid alloc args
func (args *AllocArgs) IsValid() bool {
	if args == nil {
		return false
	}
	if args.AssignClusterID > 0 {
		return args.Size > 0 && args.BlobSize > 0 && args.BlobSize <= MaxBlobSize &&
			args.CodeMode.IsValid()
	}
	return args.Size > 0 && args.BlobSize <= MaxBlobSize
}

// AllocResp alloc response result with tokens
// if size mod blobsize == 0, length of tokens equal length of location blobs
// otherwise additional token for the last blob uploading
type AllocResp struct {
	Location Location `json:"location"`
	Tokens   []string `json:"tokens"`
}

// GetArgs for service /get
type GetArgs struct {
	Location Location `json:"location"`
	Offset   uint64   `json:"offset"`
	ReadSize uint64   `json:"read_size"`
}

// IsValid is valid get args
func (args *GetArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.Offset+args.ReadSize <= args.Location.Size
}

// DeleteArgs for service /delete
type DeleteArgs struct {
	Locations []Location `json:"locations"`
}

// IsValid is valid delete args
func (args *DeleteArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return len(args.Locations) > 0 && len(args.Locations) <= MaxDeleteLocations
}

// DeleteResp delete response with failed locations
type DeleteResp struct {
	FailedLocations []Location `json:"failed_locations,omitempty"`
}

// DeleteBlobArgs for service /deleteblob
type DeleteBlobArgs struct {
	ClusterID proto.ClusterID `json:"clusterid"`
	Vid       proto.Vid       `json:"volumeid"`
	Blobid    proto.BlobID    `json:"blobid"`
	Size      int64           `json:"size"`
	Token     string          `json:"token"`
}

// IsValid is valid delete blob args
func (args *DeleteBlobArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.ClusterID > proto.ClusterID(0) &&
		args.Vid > proto.Vid(0) &&
		args.Blobid > proto.BlobID(0) &&
		args.Size > 0
}

// SignArgs for service /sign
// Locations are signed location getting from /alloc
// Location is to be signed location which merged by yourself
type SignArgs struct {
	Locations []Location `json:"locations"`
	Location  Location   `json:"location"`
}

// IsValid is valid sign args
func (args *SignArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return len(args.Locations) > 0
}

// SignResp sign response location with crc
type SignResp struct {
	Location Location `json:"location"`
}
