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

package clustermgr

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestChunkReportArgs(t *testing.T) {
	vuid1, _ := proto.NewVuid(1, 0, 1)
	vuid2, _ := proto.NewVuid(1, 1, 1)
	args := ReportChunkArgs{
		ChunkInfos: []blobnode.ChunkInfo{
			{
				Id:     blobnode.NewChunkId(vuid1),
				Vuid:   vuid1,
				DiskID: 1,
				Free:   1,
				Used:   1,
				Size:   1,
			},
			{
				Id:     blobnode.NewChunkId(vuid2),
				Vuid:   vuid2,
				DiskID: 2,
				Free:   2,
				Used:   2,
				Size:   2,
			},
		},
	}
	b, err := args.Encode()
	require.NoError(t, err)

	decodeArgs := &ReportChunkArgs{}
	err = decodeArgs.Decode(bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, len(args.ChunkInfos), len(decodeArgs.ChunkInfos))
	for i := 0; i < len(args.ChunkInfos); i++ {
		require.Equal(t, args.ChunkInfos[i].Id, decodeArgs.ChunkInfos[i].Id)
		require.Equal(t, args.ChunkInfos[i].Vuid, decodeArgs.ChunkInfos[i].Vuid)
		require.Equal(t, args.ChunkInfos[i].DiskID, decodeArgs.ChunkInfos[i].DiskID)
		require.Equal(t, args.ChunkInfos[i].Free, decodeArgs.ChunkInfos[i].Free)
		require.Equal(t, args.ChunkInfos[i].Used, decodeArgs.ChunkInfos[i].Used)
		require.Equal(t, args.ChunkInfos[i].Size, decodeArgs.ChunkInfos[i].Size)
	}
}

func TestEqual(t *testing.T) {
	baseVolume := VolumeInfo{
		Units:          []Unit{{Vuid: proto.Vuid(10000), DiskID: 1, Host: "127.0.0.1"}, {Vuid: proto.Vuid(10001), DiskID: 2, Host: "127.0.0.1"}},
		VolumeInfoBase: VolumeInfoBase{Vid: 1, HealthScore: 0, Status: 0},
	}

	// health score not match
	vol1 := VolumeInfo{
		Units:          []Unit{{Vuid: proto.Vuid(10000), DiskID: 1, Host: "127.0.0.1"}, {Vuid: proto.Vuid(10001), DiskID: 2, Host: "127.0.0.1"}},
		VolumeInfoBase: VolumeInfoBase{Vid: 1, HealthScore: -1, Status: 0},
	}
	// status not match
	vol2 := VolumeInfo{
		Units:          []Unit{{Vuid: proto.Vuid(10000), DiskID: 1, Host: "127.0.0.1"}, {Vuid: proto.Vuid(10001), DiskID: 2, Host: "127.0.0.1"}},
		VolumeInfoBase: VolumeInfoBase{Vid: 1, HealthScore: 0, Status: 1},
	}
	// diskID not match
	vol3 := VolumeInfo{
		Units:          []Unit{{Vuid: proto.Vuid(10000), DiskID: 99, Host: "127.0.0.1"}, {Vuid: proto.Vuid(10001), DiskID: 2, Host: "127.0.0.1"}},
		VolumeInfoBase: VolumeInfoBase{Vid: 1, HealthScore: 0, Status: 0},
	}
	// vuid not match
	vol4 := VolumeInfo{
		Units:          []Unit{{Vuid: proto.Vuid(20000), DiskID: 1, Host: "127.0.0.1"}, {Vuid: proto.Vuid(10001), DiskID: 2, Host: "127.0.0.1"}},
		VolumeInfoBase: VolumeInfoBase{Vid: 1, HealthScore: 0, Status: 0},
	}

	vol5 := VolumeInfo{
		Units:          []Unit{{Vuid: proto.Vuid(10000), DiskID: 1, Host: "127.0.0.1"}, {Vuid: proto.Vuid(10001), DiskID: 2, Host: "127.0.0.1"}},
		VolumeInfoBase: VolumeInfoBase{Vid: 1, HealthScore: 0, Status: 0},
	}

	testCases := []struct {
		vol   VolumeInfo
		equal bool
	}{
		{vol1, false},
		{vol2, false},
		{vol3, false},
		{vol4, false},
		{vol5, true},
	}
	for _, ca := range testCases {
		require.Equal(t, baseVolume.Equal(&ca.vol), ca.equal)
	}
}
