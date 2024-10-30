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
	"encoding/hex"
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
	MaxLocationBlobs int = 4
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
func (d dummyHash) Reset()                            { _ = struct{}{} }
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
//
//	HashAlgDummy  returns nil, bool
//	HashAlgCRC32  returns uint32, bool
//	HashAlgMD5    returns string(32), bool
//	HashAlgSHA1   returns string(40), bool
//	HashAlgSHA256 returns string(64), bool
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

// PutArgs for service /put
// Hashes means how to calculate check sum,
// HashAlgCRC32 | HashAlgMD5 equal 2 + 4 = 6
type PutArgs struct {
	Size   int64         `json:"size"`
	Hashes HashAlgorithm `json:"hashes,omitempty"`
	Body   io.Reader     `json:"-"`

	// GetBody defines an optional func to return a new copy of Body.
	// It is used for client requests when a redirect requires reading
	// the body more than once. Use of GetBody still requires setting Body.
	//
	// There force reset request.GetBody if it is setting.
	GetBody func() (io.ReadCloser, error) `json:"-"`
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
	Location   proto.Location `json:"location"`
	HashSumMap HashSumMap     `json:"hashsum"`
}

// PutAtArgs for service /putat
type PutAtArgs struct {
	ClusterID proto.ClusterID `json:"clusterid"`
	Vid       proto.Vid       `json:"volumeid"`
	BlobID    proto.BlobID    `json:"blobid"`
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
		args.BlobID > proto.BlobID(0) &&
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
	Location proto.Location `json:"location"`
	Tokens   []string       `json:"tokens"`
}

// GetArgs for service /get
type GetArgs struct {
	Location proto.Location `json:"location"`
	Offset   uint64         `json:"offset"`
	ReadSize uint64         `json:"read_size"`
	Writer   io.Writer      `json:"-"`
}

// IsValid is valid get args
func (args *GetArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.Offset <= args.Location.Size_ &&
		args.ReadSize <= args.Location.Size_ &&
		args.Offset+args.ReadSize <= args.Location.Size_
}

// DeleteArgs for service /delete
type DeleteArgs struct {
	Locations []proto.Location `json:"locations"`
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
	FailedLocations []proto.Location `json:"failed_locations,omitempty"`
}

// DeleteBlobArgs for service /deleteblob
type DeleteBlobArgs struct {
	ClusterID proto.ClusterID `json:"clusterid"`
	Vid       proto.Vid       `json:"volumeid"`
	BlobID    proto.BlobID    `json:"blobid"`
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
		args.BlobID > proto.BlobID(0) &&
		args.Size > 0
}

// SignArgs for service /sign
// Locations are signed location getting from /alloc
// Location is to be signed location which merged by yourself
type SignArgs struct {
	Locations []proto.Location `json:"locations"`
	Location  proto.Location   `json:"location"`
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
	Location proto.Location `json:"location"`
}

// Shardnode Blob

type GetShardMode int

const (
	GetShardModeRandom = GetShardMode(iota)
	GetShardModeLeader
)

type CreateBlobArgs struct {
	ClusterID proto.ClusterID
	CodeMode  codemode.CodeMode
	BlobName  []byte
	ShardKeys [][]byte
	Size      uint64
	SliceSize uint32
}

func (args *CreateBlobArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.Size != 0 && (len(args.BlobName) != 0 || len(args.ShardKeys) != 0)
}

type CreateBlobRet struct {
	Location proto.Location
}

type ListBlobArgs struct {
	ClusterID proto.ClusterID
	ShardID   proto.ShardID
	Mode      GetShardMode
	Prefix    []byte
	Marker    []byte
	Count     uint64
}

func (args *ListBlobArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.ClusterID != 0
}

type SealBlobArgs struct {
	ClusterID proto.ClusterID
	BlobName  []byte
	ShardKeys [][]byte
	Slices    []proto.Slice
}

func (args *SealBlobArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.ClusterID != 0 && len(args.Slices) != 0 && (len(args.BlobName) != 0 || len(args.ShardKeys) != 0)
}

type GetBlobArgs struct {
	ClusterID proto.ClusterID
	Mode      GetShardMode
	BlobName  []byte
	ShardKeys [][]byte

	Offset   uint64
	ReadSize uint64
	Writer   io.Writer
}

// IsValid is valid get args
func (args *GetBlobArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.ClusterID != 0 && (len(args.BlobName) != 0 || len(args.ShardKeys) != 0)
}

type DelBlobArgs struct {
	ClusterID proto.ClusterID
	BlobName  []byte
	ShardKeys [][]byte
}

func (args *DelBlobArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.ClusterID != 0 && (len(args.BlobName) != 0 || len(args.ShardKeys) != 0)
}

type AllocSliceArgs struct {
	ClusterID proto.ClusterID
	CodeMode  codemode.CodeMode
	BlobName  []byte
	ShardKeys [][]byte
	Size      uint64
	FailSlice proto.Slice
}

func (args *AllocSliceArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.ClusterID != 0 && args.CodeMode.IsValid() && args.Size != 0 && (len(args.BlobName) != 0 || len(args.ShardKeys) != 0)
}

type PutBlobArgs struct {
	CodeMode  codemode.CodeMode
	BlobName  []byte
	ShardKeys [][]byte
	NeedSeal  bool

	Size   uint64
	Hashes HashAlgorithm
	Body   io.Reader
}

func (args *PutBlobArgs) IsValid() bool {
	if args == nil {
		return false
	}
	return args.CodeMode != 0 && args.Size != 0 && (len(args.BlobName) != 0 || len(args.ShardKeys) != 0)
}

type GetShardCommonArgs struct {
	ClusterID proto.ClusterID
	ShardID   proto.ShardID
	Mode      GetShardMode
	Exclude   proto.DiskID
	BlobName  []byte
	ShardKeys [][]byte
}
