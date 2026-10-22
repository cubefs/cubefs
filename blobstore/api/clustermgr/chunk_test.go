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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestChunkReportArgs(t *testing.T) {
	vuid1, _ := proto.NewVuid(1, 0, 1)
	vuid2, _ := proto.NewVuid(1, 1, 1)
	args := ReportChunkArgs{
		ChunkInfos: []ChunkInfo{
			{
				Id:     NewChunkID(vuid1),
				Vuid:   vuid1,
				DiskID: 1,
				Free:   1,
				Used:   1,
				Size:   1,
			},
			{
				Id:     NewChunkID(vuid2),
				Vuid:   vuid2,
				DiskID: 2,
				Free:   2,
				Used:   2,
				Size:   2,
			},
		},
	}
	b := make([]byte, args.Size())
	err := args.Encode(b)
	require.NoError(t, err)

	decodeArgs := &ReportChunkArgs{}
	err = decodeArgs.Decode(b)
	require.NoError(t, err)
	require.Equal(t, len(args.ChunkInfos), len(decodeArgs.ChunkInfos))
	for i := 0; i < len(args.ChunkInfos); i++ {
		require.Equal(t, args.ChunkInfos[i].Vuid, decodeArgs.ChunkInfos[i].Vuid)
		require.Equal(t, args.ChunkInfos[i].Free, decodeArgs.ChunkInfos[i].Free)
		require.Equal(t, args.ChunkInfos[i].Used, decodeArgs.ChunkInfos[i].Used)
		require.Equal(t, args.ChunkInfos[i].Size, decodeArgs.ChunkInfos[i].Size)
	}
}

func TestChunkReportVerCodec(t *testing.T) {
	vuid, err := proto.NewVuid(1, 0, 1)
	require.NoError(t, err)
	args := ReportChunkArgs{ChunkInfos: []ChunkInfo{{
		Id:     NewChunkID(vuid),
		Vuid:   vuid,
		DiskID: 3,
		Total:  10,
		Used:   4,
		Free:   6,
		Size:   4,
	}}}
	b := make([]byte, args.Size())
	err = args.Encode(b)
	require.NoError(t, err)
	ver, ok := chunkReportVersion(binary.BigEndian.Uint32(b[:4]))
	require.True(t, ok)
	require.Equal(t, chunkReportVersion1, ver)

	decoded := &ReportChunkArgs{}
	require.NoError(t, decoded.Decode(b))
	require.Equal(t, []ChunkInfo{{Vuid: vuid, Total: 10, Used: 4, Free: 6, Size: 4}}, decoded.ChunkInfos)

	legacy := make([]byte, args.legacyChunkReportSize())
	err = args.encodeLegacyChunkReport(legacy)
	require.NoError(t, err)
	_, ok = chunkReportVersion(binary.BigEndian.Uint32(legacy[:4]))
	require.False(t, ok)
	decoded = &ReportChunkArgs{}
	require.NoError(t, decoded.Decode(legacy))
	require.Equal(t, args.ChunkInfos[0].Id, decoded.ChunkInfos[0].Id)
	require.Equal(t, args.ChunkInfos[0].DiskID, decoded.ChunkInfos[0].DiskID)
	require.Equal(t, args.ChunkInfos[0].Vuid, decoded.ChunkInfos[0].Vuid)
}

func TestChunkReportCodecError(t *testing.T) {
	vuid, err := proto.NewVuid(1, 0, 1)
	require.NoError(t, err)
	one := ReportChunkArgs{ChunkInfos: []ChunkInfo{{
		Id:         NewChunkID(vuid),
		Vuid:       vuid,
		DiskID:     proto.DiskID(^uint32(0)),
		Total:      ^uint64(0),
		Used:       1,
		Free:       2,
		Size:       3,
		Status:     ChunkStatusReadOnly,
		Compacting: true,
	}}}

	t.Run("encode", func(t *testing.T) {
		require.Error(t, (&ReportChunkArgs{}).Encode(nil))
		require.Error(t, (&ReportChunkArgs{}).encodeLegacyChunkReport(nil))
		require.Error(t, one.Encode(make([]byte, one.Size()-1)))
		require.Error(t, one.encodeLegacyChunkReport(make([]byte, one.legacyChunkReportSize()-1)))

		tooMany := ReportChunkArgs{ChunkInfos: make([]ChunkInfo, maxChunkReportCount+1)}
		require.Error(t, tooMany.Encode(nil))
		require.Error(t, tooMany.encodeLegacyChunkReport(nil))
	})

	t.Run("v1", func(t *testing.T) {
		buf := make([]byte, one.Size()+1)
		require.NoError(t, one.Encode(buf))
		decoded := &ReportChunkArgs{}
		require.Error(t, decoded.Decode(buf))
		require.NoError(t, decoded.Decode(buf[:one.Size()]))
		require.Equal(t, ChunkInfo{Vuid: vuid, Total: ^uint64(0), Used: 1, Free: 2, Size: 3}, decoded.ChunkInfos[0])

		require.Error(t, decoded.Decode(nil))
		require.Error(t, decoded.Decode([]byte{1, 2, 3}))

		magic := make([]byte, 4)
		binary.BigEndian.PutUint32(magic, chunkReportMagicBase+chunkReportVersion1)
		require.Error(t, decoded.Decode(magic))

		hdr := make([]byte, chunkReportHeaderSize)
		binary.BigEndian.PutUint32(hdr[:4], chunkReportMagicBase+chunkReportVersion1)
		require.Error(t, decoded.Decode(hdr))

		binary.BigEndian.PutUint32(hdr[4:], maxChunkReportCount+1)
		require.Error(t, decoded.Decode(hdr))
		binary.BigEndian.PutUint32(hdr[4:], maxChunkReportCount)
		require.Error(t, decoded.Decode(hdr))

		exact := buf[:one.Size()]
		require.Error(t, decoded.Decode(exact[:len(exact)-1]))

		binary.BigEndian.PutUint32(magic, chunkReportMagicBase+2)
		require.Error(t, decoded.Decode(magic))
		binary.BigEndian.PutUint32(magic, chunkReportMagicBase+chunkReportVersionSpan-1)
		require.Error(t, decoded.Decode(magic))
	})

	t.Run("legacy", func(t *testing.T) {
		legacy := make([]byte, one.legacyChunkReportSize())
		require.NoError(t, one.encodeLegacyChunkReport(legacy))
		decoded := &ReportChunkArgs{}
		require.NoError(t, decoded.Decode(append(append([]byte{}, legacy...), 0xff)))
		require.Equal(t, one.ChunkInfos[0], decoded.ChunkInfos[0])
		require.Error(t, decoded.Decode(legacy[:len(legacy)-1]))

		badID := append([]byte{}, legacy...)
		badID[4] = 'g'
		require.Error(t, decoded.Decode(badID))

		compacting := append([]byte{}, legacy...)
		compacting[len(compacting)-1] = 2
		require.NoError(t, decoded.Decode(compacting))
		require.True(t, decoded.ChunkInfos[0].Compacting)
		compacting[len(compacting)-1] = 0
		require.NoError(t, decoded.Decode(compacting))
		require.False(t, decoded.ChunkInfos[0].Compacting)

		require.NoError(t, decoded.Decode([]byte{0, 0, 0, 0, 0xff}))
		require.Empty(t, decoded.ChunkInfos)

		cnt := make([]byte, 4)
		binary.BigEndian.PutUint32(cnt, maxChunkReportCount+1)
		require.Error(t, decoded.Decode(cnt))
		binary.BigEndian.PutUint32(cnt, maxChunkReportCount)
		require.Error(t, decoded.Decode(cnt))

		binary.BigEndian.PutUint32(cnt, chunkReportMagicBase)
		require.Error(t, decoded.Decode(cnt))
		binary.BigEndian.PutUint32(cnt, chunkReportMagicBase+chunkReportVersionSpan)
		require.Error(t, decoded.Decode(cnt))
	})
}
