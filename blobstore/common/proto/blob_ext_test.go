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
	"math"
	mrand "math/rand"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/stretchr/testify/require"
)

func TestLocationEncodeDecodeNil(t *testing.T) {
	var loc *Location
	require.Nil(t, loc.Encode())
	require.Equal(t, 0, loc.Encode2(nil))
	require.Equal(t, "", loc.ToString())
	require.Equal(t, "", loc.HexString())
	require.Equal(t, "", loc.Base64String())

	n, err := loc.Decode(nil)
	require.Error(t, err)
	require.Equal(t, 0, n)

	locx, n, err := DecodeLocation(nil)
	require.Error(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, Location{}, locx)

	locx, err = DecodeLocationFrom("")
	require.Error(t, err)
	require.Equal(t, Location{}, locx)

	locx, err = DecodeLocationFrom("xxx")
	require.Error(t, err)
	require.Equal(t, Location{}, locx)

	locx, err = DecodeLocationFromHex("xxx")
	require.Error(t, err)
	require.Equal(t, Location{}, locx)

	locx, err = DecodeLocationFromBase64("xxx")
	require.Error(t, err)
	require.Equal(t, Location{}, locx)
}

func TestLocationEncodeDecode(t *testing.T) {
	for ii := 0; ii < 100; ii++ {
		loc := &Location{
			ClusterID: ClusterID(mrand.Uint32()),
			CodeMode:  codemode.CodeMode(mrand.Intn(0xff)),
			Size_:     mrand.Uint64(),
			SliceSize: mrand.Uint32(),
			Crc:       mrand.Uint32(),
		}

		num := mrand.Intn(5)
		for i := 0; i < num; i++ {
			loc.Slices = append(loc.Slices, Slice{
				MinSliceID: BlobID(mrand.Uint64()),
				Vid:        Vid(mrand.Uint32()),
				Count:      mrand.Uint32(),
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

		locx := Location{}
		locx.Decode(bufx)
		require.Equal(t, loc.ToString(), locx.ToString())
		require.Equal(t, loc.HexString(), locx.HexString())
		require.Equal(t, loc.Base64String(), locx.Base64String())

		str := loc.ToString()
		locx, err := DecodeLocationFrom(str)
		require.NoError(t, err)
		require.Equal(t, *loc, locx)

		str = loc.HexString()
		locx, err = DecodeLocationFromHex(str)
		require.NoError(t, err)
		require.Equal(t, *loc, locx)

		str = loc.Base64String()
		locx, err = DecodeLocationFromBase64(str)
		require.NoError(t, err)
		require.Equal(t, *loc, locx)
	}
}

func TestLocationDecodeError(t *testing.T) {
	loc := &Location{
		ClusterID: ClusterID(math.MaxUint32),
		CodeMode:  codemode.CodeMode(math.MaxInt8),
		Size_:     math.MaxUint64,
		SliceSize: math.MaxUint32,
		Crc:       math.MaxUint32,
	}

	buf := loc.Encode()
	require.Equal(t, 25+1, len(buf))
	for _, n := range []int{3, 8, 9, 19, 24} {
		_, _, err := DecodeLocation(buf[:n])
		require.Error(t, err)
		t.Log(err)
	}

	loc.Slices = append(loc.Slices, Slice{
		MinSliceID: BlobID(math.MaxUint64),
		Vid:        Vid(math.MaxUint32),
		Count:      math.MaxUint32,
	})

	buf = loc.Encode()
	require.Equal(t, 25+1+20, len(buf))
	for _, n := range []int{25, 35, 40, 45} {
		_, _, err := DecodeLocation(buf[:n])
		require.Error(t, err)
		t.Log(err)
	}
}

func TestLocationSpread(t *testing.T) {
	{
		var loc Location
		blobs := loc.Spread()
		require.NotNil(t, blobs)
		require.Equal(t, 0, len(blobs))
	}
	{
		loc := &Location{
			Size_:     10,
			SliceSize: 1 << 22,
			Slices: []Slice{{
				MinSliceID: 100,
				Vid:        4,
				Count:      1,
			}},
		}
		blobs := loc.Spread()
		require.Equal(t, 1, len(blobs))
		require.Equal(t, BlobID(100), blobs[0].Bid)
		require.Equal(t, Vid(4), blobs[0].Vid)
		require.Equal(t, uint32(10), blobs[0].Size)
	}
	{
		loc := &Location{
			Size_:     (1 << 22) + 10,
			SliceSize: 1 << 22,
			Slices: []Slice{{
				MinSliceID: 100,
				Vid:        4,
				Count:      2,
			}},
		}
		blobs := loc.Spread()
		require.Equal(t, 2, len(blobs))
		require.Equal(t, BlobID(100), blobs[0].Bid)
		require.Equal(t, Vid(4), blobs[0].Vid)
		require.Equal(t, uint32(1<<22), blobs[0].Size)
		require.Equal(t, BlobID(101), blobs[1].Bid)
		require.Equal(t, Vid(4), blobs[1].Vid)
		require.Equal(t, uint32(10), blobs[1].Size)
	}
	{
		loc := &Location{
			Size_:     1 << 23,
			SliceSize: 1 << 22,
			Slices: []Slice{{
				MinSliceID: 100,
				Vid:        4,
				Count:      1,
			}, {
				MinSliceID: 200,
				Vid:        4,
				Count:      1,
			}},
		}
		blobs := loc.Spread()
		require.Equal(t, 2, len(blobs))
		require.Equal(t, BlobID(100), blobs[0].Bid)
		require.Equal(t, Vid(4), blobs[0].Vid)
		require.Equal(t, uint32(1<<22), blobs[0].Size)
		require.Equal(t, BlobID(200), blobs[1].Bid)
		require.Equal(t, Vid(4), blobs[1].Vid)
		require.Equal(t, uint32(1<<22), blobs[1].Size)
	}
}
