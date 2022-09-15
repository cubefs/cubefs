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

func TestLocationEncodeDecodeNil(t *testing.T) {
	var loc *access.Location
	require.Nil(t, loc.Encode())
	require.Equal(t, 0, loc.Encode2(nil))
	require.Equal(t, "", loc.ToString())
	require.Equal(t, "", loc.HexString())
	require.Equal(t, "", loc.Base64String())

	n, err := loc.Decode(nil)
	require.Error(t, err)
	require.Equal(t, 0, n)

	locx, n, err := access.DecodeLocation(nil)
	require.Error(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, access.Location{}, locx)

	locx, err = access.DecodeLocationFrom("")
	require.Error(t, err)
	require.Equal(t, access.Location{}, locx)

	locx, err = access.DecodeLocationFrom("xxx")
	require.Error(t, err)
	require.Equal(t, access.Location{}, locx)

	locx, err = access.DecodeLocationFromHex("xxx")
	require.Error(t, err)
	require.Equal(t, access.Location{}, locx)

	locx, err = access.DecodeLocationFromBase64("xxx")
	require.Error(t, err)
	require.Equal(t, access.Location{}, locx)
}

func TestLocationEncodeDecode(t *testing.T) {
	for ii := 0; ii < 100; ii++ {
		loc := &access.Location{
			ClusterID: proto.ClusterID(mrand.Uint32()),
			CodeMode:  codemode.CodeMode(mrand.Intn(0xff)),
			Size:      mrand.Uint64(),
			BlobSize:  mrand.Uint32(),
			Crc:       mrand.Uint32(),
		}

		num := mrand.Intn(5)
		for i := 0; i < num; i++ {
			loc.Blobs = append(loc.Blobs, access.SliceInfo{
				MinBid: proto.BlobID(mrand.Uint64()),
				Vid:    proto.Vid(mrand.Uint32()),
				Count:  mrand.Uint32(),
			})
		}

		buf := loc.Encode()
		bufx := make([]byte, len(buf))
		n := loc.Encode2(bufx)
		require.Equal(t, len(buf), n)
		require.Equal(t, buf, bufx)

		require.Panics(t, func() { loc.Encode2(nil) })
		require.Panics(t, func() { loc.Encode2(bufx[:3]) })
		require.Panics(t, func() { loc.Encode2(bufx[:n/2]) })
		require.Panics(t, func() { loc.Encode2(bufx[:n-1]) })

		locx := access.Location{}
		locx.Decode(bufx)
		require.Equal(t, loc.ToString(), locx.ToString())
		require.Equal(t, loc.HexString(), locx.HexString())
		require.Equal(t, loc.Base64String(), locx.Base64String())

		str := loc.ToString()
		locx, err := access.DecodeLocationFrom(str)
		require.NoError(t, err)
		require.Equal(t, *loc, locx)

		str = loc.HexString()
		locx, err = access.DecodeLocationFromHex(str)
		require.NoError(t, err)
		require.Equal(t, *loc, locx)

		str = loc.Base64String()
		locx, err = access.DecodeLocationFromBase64(str)
		require.NoError(t, err)
		require.Equal(t, *loc, locx)
	}
}

func TestLocationDecodeError(t *testing.T) {
	loc := &access.Location{
		ClusterID: proto.ClusterID(math.MaxUint32),
		CodeMode:  codemode.CodeMode(math.MaxInt8),
		Size:      math.MaxUint64,
		BlobSize:  math.MaxUint32,
		Crc:       math.MaxUint32,
	}

	buf := loc.Encode()
	require.Equal(t, 25+1, len(buf))
	for _, n := range []int{3, 8, 9, 19, 24} {
		_, _, err := access.DecodeLocation(buf[:n])
		require.Error(t, err)
		t.Log(err)
	}

	loc.Blobs = append(loc.Blobs, access.SliceInfo{
		MinBid: proto.BlobID(math.MaxUint64),
		Vid:    proto.Vid(math.MaxUint32),
		Count:  math.MaxUint32,
	})

	buf = loc.Encode()
	require.Equal(t, 25+1+20, len(buf))
	for _, n := range []int{25, 35, 40, 45} {
		_, _, err := access.DecodeLocation(buf[:n])
		require.Error(t, err)
		t.Log(err)
	}
}

func TestLocationSpread(t *testing.T) {
	{
		var loc access.Location
		blobs := loc.Spread()
		require.NotNil(t, blobs)
		require.Equal(t, 0, len(blobs))
	}
	{
		loc := &access.Location{
			Size:     10,
			BlobSize: 1 << 22,
			Blobs: []access.SliceInfo{{
				MinBid: 100,
				Vid:    4,
				Count:  1,
			}},
		}
		blobs := loc.Spread()
		require.Equal(t, 1, len(blobs))
		require.Equal(t, proto.BlobID(100), blobs[0].Bid)
		require.Equal(t, proto.Vid(4), blobs[0].Vid)
		require.Equal(t, uint32(10), blobs[0].Size)
	}
	{
		loc := &access.Location{
			Size:     (1 << 22) + 10,
			BlobSize: 1 << 22,
			Blobs: []access.SliceInfo{{
				MinBid: 100,
				Vid:    4,
				Count:  2,
			}},
		}
		blobs := loc.Spread()
		require.Equal(t, 2, len(blobs))
		require.Equal(t, proto.BlobID(100), blobs[0].Bid)
		require.Equal(t, proto.Vid(4), blobs[0].Vid)
		require.Equal(t, uint32(1<<22), blobs[0].Size)
		require.Equal(t, proto.BlobID(101), blobs[1].Bid)
		require.Equal(t, proto.Vid(4), blobs[1].Vid)
		require.Equal(t, uint32(10), blobs[1].Size)
	}
	{
		loc := &access.Location{
			Size:     1 << 23,
			BlobSize: 1 << 22,
			Blobs: []access.SliceInfo{{
				MinBid: 100,
				Vid:    4,
				Count:  1,
			}, {
				MinBid: 200,
				Vid:    4,
				Count:  1,
			}},
		}
		blobs := loc.Spread()
		require.Equal(t, 2, len(blobs))
		require.Equal(t, proto.BlobID(100), blobs[0].Bid)
		require.Equal(t, proto.Vid(4), blobs[0].Vid)
		require.Equal(t, uint32(1<<22), blobs[0].Size)
		require.Equal(t, proto.BlobID(200), blobs[1].Bid)
		require.Equal(t, proto.Vid(4), blobs[1].Vid)
		require.Equal(t, uint32(1<<22), blobs[1].Size)
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
	args := access.GetArgs{}
	require.True(t, args.IsValid())
}

func TestDeleteArgs(t *testing.T) {
	args := access.DeleteArgs{}
	require.False(t, args.IsValid())
	require.False(t, (*access.DeleteArgs)(nil).IsValid())
	args.Locations = []access.Location{{}}
	require.True(t, args.IsValid())
	args.Locations = make([]access.Location, access.MaxDeleteLocations)
	require.True(t, args.IsValid())
	args.Locations = make([]access.Location, access.MaxDeleteLocations+1)
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
	args.Locations = []access.Location{{}}
	require.True(t, args.IsValid())
}

func init() {
	mrand.Seed(time.Now().UnixNano())
}
