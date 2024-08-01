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

package access_test

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"hash/crc32"
	"math"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestHashAlgorithm(t *testing.T) {
	cases := []struct {
		alg    access.HashAlgorithm
		size   int
		sumLen int
	}{
		{0, 0, access.HashSize},
		{0b0, 1 << 10, access.HashSize},
		{access.HashAlgorithm(0), 1 << 14, access.HashSize},

		{1, 0, access.HashSize},
		{0b1, 1 << 10, access.HashSize},
		{access.HashAlgDummy, 1 << 14, access.HashSize},

		{2, 0, crc32.Size},
		{0b10, 1 << 10, crc32.Size},
		{access.HashAlgCRC32, 1 << 14, crc32.Size},

		{4, 0, md5.Size},
		{0b100, 1 << 10, md5.Size},
		{access.HashAlgMD5, 1 << 14, md5.Size},

		{8, 0, sha1.Size},
		{0b1000, 1 << 10, sha1.Size},
		{access.HashAlgSHA1, 1 << 14, sha1.Size},

		{16, 0, sha256.Size},
		{0b10000, 1 << 10, sha256.Size},
		{access.HashAlgSHA256, 1 << 14, sha256.Size},

		{100, 0, access.HashSize},
		{0b1111111, 1 << 10, access.HashSize},
		{access.HashAlgDummy, 1 << 14, access.HashSize},
	}
	for _, cs := range cases {
		buffer := make([]byte, cs.size)
		rand.Read(buffer)

		hasher := cs.alg.ToHasher()
		_, err := hasher.Write(buffer)
		require.NoError(t, err)
		require.Equal(t, cs.sumLen, len(hasher.Sum(nil)))
	}
}

func TestHasherMapWriter(t *testing.T) {
	hasherMap := access.HasherMap{
		access.HashAlgCRC32: access.HashAlgCRC32.ToHasher(),
		access.HashAlgMD5:   access.HashAlgMD5.ToHasher(),
		access.HashAlgSHA1:  access.HashAlgSHA1.ToHasher(),
	}
	writer := hasherMap.ToWriter()
	writer.Write([]byte("foo x bar"))
	require.Equal(t, []byte{0x5a, 0xc7, 0x36, 0x25}, hasherMap[access.HashAlgCRC32].Sum(nil))
	require.Equal(t, []byte{
		0x7c, 0xb4, 0x1e, 0x1a, 0x38, 0xad, 0x8b, 0xd9,
		0x5a, 0x49, 0x2d, 0xb6, 0x29, 0x30, 0x62, 0x8a,
	}, hasherMap[access.HashAlgMD5].Sum(nil))
	require.Equal(t, []byte{
		0x75, 0xe8, 0xe5, 0x0, 0x5b, 0xaa, 0x5c, 0xd, 0x40, 0x44, 0x1c, 0x42,
		0xe5, 0xf6, 0xb4, 0x9a, 0xf, 0x80, 0x80, 0x11,
	}, hasherMap[access.HashAlgSHA1].Sum(nil))
}

func TestHashSumMapGetSum(t *testing.T) {
	crc32Hash := crc32.NewIEEE()
	crc32Hash.Write([]byte("crc32"))
	bytesCRC32 := crc32Hash.Sum(nil)
	bytesMD5 := md5.Sum([]byte("md5"))
	bytesSHA1 := sha1.Sum([]byte("sha1"))
	bytesSHA256 := sha256.Sum256([]byte("sha256"))

	cases := []struct {
		key string
		m   access.HashSumMap
		alg access.HashAlgorithm
		val interface{}
		ok  bool
	}{
		{
			"dummy",
			access.HashSumMap{},
			access.HashAlgDummy, nil, false,
		},
		{
			"dummy",
			access.HashSumMap{access.HashAlgDummy: []byte{0x11}},
			access.HashAlgDummy, nil, false,
		},
		{
			"dummy",
			access.HashSumMap{access.HashAlgDummy: []byte{}},
			access.HashAlgDummy, nil, true,
		},

		{
			"crc32",
			access.HashSumMap{access.HashAlgCRC32: []byte{}},
			access.HashAlgCRC32, nil, false,
		},
		{
			"crc32",
			access.HashSumMap{access.HashAlgCRC32: []byte{}},
			access.HashAlgCRC32, nil, false,
		},
		{
			"crc32",
			access.HashSumMap{access.HashAlgCRC32: bytesCRC32},
			access.HashAlgCRC32, crc32.ChecksumIEEE([]byte("crc32")), true,
		},

		{
			"md5",
			access.HashSumMap{access.HashAlgMD5: []byte{}},
			access.HashAlgMD5, nil, false,
		},
		{
			"md5",
			access.HashSumMap{access.HashAlgMD5: []byte{0x11}},
			access.HashAlgMD5, nil, false,
		},
		{
			"md5",
			access.HashSumMap{access.HashAlgMD5: bytesMD5[:]},
			access.HashAlgMD5, "1bc29b36f623ba82aaf6724fd3b16718", true,
		},

		{
			"sha1",
			access.HashSumMap{access.HashAlgSHA1: []byte{}},
			access.HashAlgSHA1, nil, false,
		},
		{
			"sha1",
			access.HashSumMap{access.HashAlgSHA1: []byte{0x00, 0xff}},
			access.HashAlgSHA1, nil, false,
		},
		{
			"sha1",
			access.HashSumMap{access.HashAlgSHA1: bytesSHA1[:]},
			access.HashAlgSHA1, "415ab40ae9b7cc4e66d6769cb2c08106e8293b48", true,
		},

		{
			"sha256",
			access.HashSumMap{},
			access.HashAlgSHA256, nil, false,
		},
		{
			"sha256",
			access.HashSumMap{access.HashAlgSHA256: nil},
			access.HashAlgSHA256, nil, false,
		},
		{
			"sha256",
			access.HashSumMap{access.HashAlgSHA256: bytesSHA256[:]},
			access.HashAlgSHA256,
			"5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e", true,
		},

		{
			"multi",
			access.HashSumMap{access.HashAlgSHA1: bytesSHA1[:]},
			access.HashAlgSHA256, nil, false,
		},
		{
			"multi",
			access.HashSumMap{access.HashAlgSHA1: bytesSHA1[:]},
			access.HashAlgMD5, nil, false,
		},
	}

	for _, cs := range cases {
		val, ok := cs.m.GetSum(cs.alg)
		require.Equal(t, cs.ok, ok, cs.key)
		require.Equal(t, cs.val, val, cs.key)

		val = cs.m.GetSumVal(cs.alg)
		require.Equal(t, cs.val, val, cs.key)

		hasherMap := make(access.HasherMap)
		for alg := range cs.m {
			hasherMap[alg] = nil
		}
		require.Equal(t, cs.m.ToHashAlgorithm(), hasherMap.ToHashAlgorithm())

		cs.m.All()
	}
}

func TestHashAlgorithm2HashSumMap(t *testing.T) {
	cases := []struct {
		m access.HashSumMap
		h access.HashAlgorithm
	}{
		{access.HashSumMap{}, 0},
		{access.HashSumMap{
			access.HashAlgDummy: nil,
		}, 0b1},
		{access.HashSumMap{
			access.HashAlgDummy:  nil,
			access.HashAlgCRC32:  nil,
			access.HashAlgMD5:    nil,
			access.HashAlgSHA1:   nil,
			access.HashAlgSHA256: nil,
		}, 0b11111},
		{access.HashSumMap{
			access.HashAlgDummy:  nil,
			access.HashAlgCRC32:  nil,
			access.HashAlgMD5:    nil,
			access.HashAlgSHA1:   nil,
			access.HashAlgSHA256: nil,
		}, 0b00011111},
		{access.HashSumMap{
			access.HashAlgDummy:  nil,
			access.HashAlgSHA1:   nil,
			access.HashAlgSHA256: nil,
		}, 0b11001},
	}
	for _, cs := range cases {
		h := cs.m.ToHashAlgorithm()
		require.Equal(t, cs.h, h)
		m := h.ToHashSumMap()
		require.Equal(t, cs.m, m)
	}
}

func TestPutArgs(t *testing.T) {
	cases := []struct {
		size  int64
		valid bool
	}{
		{-(1 << 20), false},
		{-1, false},
		{0, false},
		{1, true},
		{100, true},
		{1 << 32, true},
	}
	for _, cs := range cases {
		args := access.PutArgs{Size: cs.size}
		require.Equal(t, cs.valid, args.IsValid())
	}
}

func TestPutAtArgs(t *testing.T) {
	cases := []struct {
		cid   proto.ClusterID
		vid   proto.Vid
		bid   proto.BlobID
		size  int64
		valid bool
	}{
		{0, 0, 0, -1, false},
		{0, 0, 0, 0, false},
		{0, 0, 0, 1, false},
		{1, 0, 0, 1, false},
		{1, 1, 0, 1, false},
		{1, 1, 1, 1, true},
	}
	for _, cs := range cases {
		args := access.PutAtArgs{
			ClusterID: cs.cid,
			Vid:       cs.vid,
			BlobID:    cs.bid,
			Size:      cs.size,
		}
		require.Equal(t, cs.valid, args.IsValid())
	}
}

func TestAllocArgs(t *testing.T) {
	cases := []struct {
		size     uint64
		cid      proto.ClusterID
		blobSize uint32
		cm       codemode.CodeMode
		valid    bool
	}{
		{0, 0, 0, 0, false},
		{0, 0, 0, 1, false},
		{1, 0, 0, 0, true},
		{1, 1, 0, 0, false},
		{1, 1, 1, 0, false},
		{1, 1, 1, 1, true},
		{1, 1, 1, 3, true},
		{1, 1, 1, 0xff, false},
		{1, 1, 0, 2, false},
		{1, 0, access.MaxBlobSize, 0, true},
		{1, 1, access.MaxBlobSize, 2, true},
		{1, 0, access.MaxBlobSize + 1, 0, false},
		{1, 1, access.MaxBlobSize + 1, 2, false},
	}
	for _, cs := range cases {
		args := access.AllocArgs{
			Size:            cs.size,
			BlobSize:        cs.blobSize,
			CodeMode:        cs.cm,
			AssignClusterID: cs.cid,
		}
		require.Equal(t, cs.valid, args.IsValid())
	}
}

func TestGetArgs(t *testing.T) {
	for _, cs := range []struct {
		size, offset, readSize uint64
		valid                  bool
	}{
		{0, 0, 0, true},
		{1024, 10, 1014, true},
		{1024, math.MaxUint64, 0, false},
		{1024, math.MaxUint64 - 10, 20, false},
		{1024, 0, math.MaxUint64 - 10, false},
		{1024, 20, math.MaxUint64 - 10, false},
	} {
		args := access.GetArgs{
			Offset:   cs.offset,
			ReadSize: cs.readSize,
		}
		args.Location.Size_ = cs.size
		require.Equal(t, cs.valid, args.IsValid())
	}
}

func TestDeleteArgs(t *testing.T) {
	args := access.DeleteArgs{}
	require.False(t, args.IsValid())
	require.False(t, (*access.DeleteArgs)(nil).IsValid())
	args.Locations = []proto.Location{{}}
	require.True(t, args.IsValid())
	args.Locations = make([]proto.Location, access.MaxDeleteLocations)
	require.True(t, args.IsValid())
	args.Locations = make([]proto.Location, access.MaxDeleteLocations+1)
	require.False(t, args.IsValid())
}

func TestDeleteBlobArgs(t *testing.T) {
	args := access.DeleteBlobArgs{}
	require.False(t, args.IsValid())
	require.False(t, (*access.DeleteBlobArgs)(nil).IsValid())
	args.ClusterID = 1
	require.False(t, args.IsValid())
	args.BlobID = 1
	require.False(t, args.IsValid())
	args.Vid = 1
	require.False(t, args.IsValid())
	args.Size = 1
	require.True(t, args.IsValid())
}

func TestSignArgs(t *testing.T) {
	args := access.SignArgs{}
	require.False(t, args.IsValid())
	require.False(t, (*access.DeleteArgs)(nil).IsValid())
	args.Locations = []proto.Location{{}}
	require.True(t, args.IsValid())
}

func init() {
	mrand.Seed(time.Now().UnixNano())
}
