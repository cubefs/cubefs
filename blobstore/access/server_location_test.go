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
	"fmt"
	"hash/crc32"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

var (
	testMaxBlob = access.SliceInfo{
		MinBid: proto.BlobID(math.MaxUint64),
		Vid:    proto.Vid(math.MaxInt32),
		Count:  math.MaxUint32,
	}
	testMaxLoc = access.Location{
		ClusterID: proto.ClusterID(math.MaxUint32),
		CodeMode:  codemode.CodeMode(math.MaxInt8),
		Size:      math.MaxUint64,
		BlobSize:  math.MaxUint32,
		Crc:       math.MaxUint32,
	}

	testMinBlob = access.SliceInfo{}
	testMinLoc  = access.Location{}
)

func TestAccessServiceLocationCrc(t *testing.T) {
	{
		_, err := calcCrc(nil)
		require.Error(t, err)
	}
	{
		crc, err := calcCrc(&testMinLoc)
		require.NoError(t, err)
		require.Equal(t, uint32(0x8a7370cc), crc)
	}
	{
		crc, err := calcCrc(&testMaxLoc)
		require.NoError(t, err)
		require.Equal(t, uint32(0xda55150a), crc)
	}
	{
		loc := testMinLoc.Copy()
		loc.Size = 1 << 30

		err := fillCrc(&loc)
		require.NoError(t, err)
		require.Equal(t, uint32(0x9e17bc9e), loc.Crc)
	}
	{
		loc := testMinLoc.Copy()
		loc.Size = 1 << 30
		require.False(t, verifyCrc(&loc))

		loc.Crc = 0x9e17bc9e
		require.True(t, verifyCrc(&loc))
	}
}

func TestAccessServiceLocationSecret(t *testing.T) {
	secret := make([]byte, len(_crcMagicKey))
	copy(secret, _crcMagicKey[:])
	defer func() {
		copy(_crcMagicKey[:], secret)
	}()
	{
		initLocationSecret([]byte{0x34, 0x45, 0x18, 0x4f})
		crc, err := calcCrc(&testMinLoc)
		require.NoError(t, err)
		require.NotEqual(t, uint32(0x8a7370cc), crc)
		require.Equal(t, uint32(0xdbe8df90), crc)
	}
	{
		// init once
		initLocationSecret([]byte{0x1, 0x2, 0x3, 0x4})
		crc, err := calcCrc(&testMinLoc)
		require.NoError(t, err)
		require.Equal(t, uint32(0xdbe8df90), crc)
	}
}

func TestAccessServiceLocationSignCrc(t *testing.T) {
	loc := &access.Location{
		ClusterID: 1,
		CodeMode:  1,
		Size:      1023,
		BlobSize:  6,
		Crc:       0,
		Blobs: []access.SliceInfo{{
			MinBid: 11,
			Vid:    199,
			Count:  10,
		}},
	}
	fillCrc(loc)
	require.True(t, verifyCrc(loc))

	{
		loc1, loc2 := loc.Copy(), loc.Copy()
		require.NoError(t, signCrc(loc, []access.Location{loc1, loc2}))
	}
	{
		loc1, loc2 := loc.Copy(), loc.Copy()
		loc1.BlobSize = 100
		fillCrc(&loc1)
		require.Error(t, signCrc(loc, []access.Location{loc1, loc2}))
	}
	{
		loc1, loc2 := loc.Copy(), loc.Copy()
		loc2.Crc = 0
		require.Error(t, signCrc(loc, []access.Location{loc1, loc2}))
	}
	{
		loc1, loc2 := loc.Copy(), loc.Copy()
		loc2.ClusterID = 2
		fillCrc(&loc2)
		require.Error(t, signCrc(loc, []access.Location{loc1, loc2}))
	}
	{
		loc1, loc2 := loc.Copy(), loc.Copy()
		loc2.CodeMode = 100
		fillCrc(&loc2)
		require.Error(t, signCrc(loc, []access.Location{loc1, loc2}))
	}
	{
		loc1, loc2 := loc.Copy(), loc.Copy()
		loc1.Blobs = nil
		loc2.Blobs[0].Count = 5
		fillCrc(&loc1)
		fillCrc(&loc2)
		require.Error(t, signCrc(loc, []access.Location{loc1, loc2}))
	}
}

func calcCrcWithoutMagic(loc *access.Location) (uint32, error) {
	crcWriter := crc32.New(_crcTable)

	buf := bytespool.Alloc(1024)
	defer bytespool.Free(buf)

	n := loc.Encode2(buf)
	crcWriter.Write(buf[4:n])

	return crcWriter.Sum32(), nil
}

func benchmarkCrc(b *testing.B, key string,
	location access.Location, blob access.SliceInfo,
	run func(loc *access.Location) (uint32, error)) {
	cases := []int{0, 2, 4, 8, 16, 32}
	for _, l := range cases {
		b.ResetTimer()
		b.Run(fmt.Sprintf(key+"-%d", l), func(b *testing.B) {
			loc := location.Copy()
			loc.Blobs = make([]access.SliceInfo, l)
			for idx := range loc.Blobs {
				loc.Blobs[idx] = blob
			}
			b.ResetTimer()
			for ii := 0; ii <= b.N; ii++ {
				run(&loc)
			}
		})
	}
}

func BenchmarkAccessServerCrcWithMagicMin(b *testing.B) {
	benchmarkCrc(b, "min-with-magic", testMinLoc, testMinBlob, calcCrc)
}

func BenchmarkAccessServerCrcWithoutMagicMin(b *testing.B) {
	benchmarkCrc(b, "min-without-magic", testMinLoc, testMinBlob, calcCrcWithoutMagic)
}

func BenchmarkAccessServerCrcWithMagicMax(b *testing.B) {
	benchmarkCrc(b, "max-with-magic", testMaxLoc, testMaxBlob, calcCrc)
}

func BenchmarkAccessServerCrcWithoutMagicMax(b *testing.B) {
	benchmarkCrc(b, "max-without-magic", testMaxLoc, testMaxBlob, calcCrcWithoutMagic)
}
