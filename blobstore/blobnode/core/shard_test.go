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

package core

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
)

func TestShard_writeHeader(t *testing.T) {
	// check buf
	s := Shard{}
	buf := make([]byte, 1)
	err := s.WriterHeader(buf)

	require.Error(t, err)
	require.Contains(t, err.Error(), "header size")

	s = Shard{
		Bid:  10,
		Vuid: 1023,
		Size: 1,
	}
	buf = make([]byte, _shardHeaderSize)
	err = s.WriterHeader(buf)
	require.NoError(t, err)

	// new shard struct
	ns := Shard{}
	err = ns.ParseHeader(buf)
	require.NoError(t, err)

	// bad shard header
	badbuf := make([]byte, len(buf))
	copy(badbuf, buf)
	badbuf[0] = 0xa
	ns_0 := Shard{}
	err = ns_0.ParseHeader(badbuf)
	require.Error(t, err)
}

func TestShard_writeFooter(t *testing.T) {
	// check buf
	s := Shard{}
	buf := make([]byte, 1)
	err := s.WriterFooter(buf)

	require.Error(t, err)
	require.Contains(t, err.Error(), "footer size")

	s = Shard{
		Bid:  10,
		Vuid: 1023,
		Size: 1,
	}
	buf = make([]byte, _shardFooterSize)
	err = s.WriterFooter(buf)
	require.NoError(t, err)

	// new shard struct
	ns := Shard{}
	err = ns.ParseFooter(buf)
	require.NoError(t, err)
}

func TestShardCopy(t *testing.T) {
	s1 := Shard{
		Bid:    1,
		Vuid:   2,
		Size:   3,
		Offset: 4,
		Crc:    5,
		Flag:   blobnode.ShardStatusNormal,
		From:   0,
		To:     3,
	}

	s2 := ShardCopy(&s1)

	require.Equal(t, s1.Bid, s2.Bid)
	require.Equal(t, s1.Vuid, s2.Vuid)
	require.Equal(t, s1.Offset, s2.Offset)
	require.Equal(t, s1.Size, s2.Size)
	require.Equal(t, s1.Crc, s2.Crc)
	require.Equal(t, s1.Flag, s2.Flag)
	require.Equal(t, s1.From, s2.From)
	require.Equal(t, s1.To, s2.To)
	require.Equal(t, s1.Body, s2.Body)
	require.Equal(t, s1.Writer, s2.Writer)
}

func TestShardMeta_Marshal(t *testing.T) {
	sm := &ShardMeta{
		Version: 0x1,
		Flag:    1,
		Offset:  1024,
		Size:    2048,
		Crc:     4096,
	}

	require.Equal(t, int(unsafe.Sizeof(ShardMeta{})) >= _ShardMetaSize, true)

	buf, err := sm.Marshal()
	require.NoError(t, err)
	require.Equal(t, _ShardMetaSize, len(buf))

	sm1 := &ShardMeta{}
	err = sm1.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, *sm, *sm1)

	buf[15] = 11
	err = sm1.Unmarshal(buf)
	require.NoError(t, err)
	require.NotEqual(t, *sm, *sm1)

	// inline
	sm.Buffer = make([]byte, 2048)
	sm.Inline = true
	copy(sm.Buffer, []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8})
	buf, err = sm.Marshal()
	require.NoError(t, err)
	require.Equal(t, int(_ShardMetaSize+sm.Size), int(len(buf)))

	sm1 = &ShardMeta{}
	err = sm1.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, true, sm1.Inline)
	require.Equal(t, int(1), int(sm1.Flag))
	require.Equal(t, sm.Buffer, sm1.Buffer)
}
