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

package wal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestLogIndex(t *testing.T) {
	var li logIndexSlice

	require.Equal(t, uint64(0), li.First())
	require.Equal(t, uint64(0), li.Last())
	_, err := li.Get(10)
	require.NotNil(t, err)

	for i := 0; i < 1000; i++ {
		entry := &pb.Entry{
			Index: uint64(i + 1),
			Term:  uint64(i + 1),
		}
		li = li.Append(uint32(i), entry)
	}
	require.Equal(t, 1000, li.Len())
	require.Equal(t, uint64(1), li.First())
	require.Equal(t, uint64(1000), li.Last())
	item, err := li.Get(800)
	require.Nil(t, err)
	require.Equal(t, indexItem{800, 800, 799}, item)
	_, err = li.Get(8000)
	require.NotNil(t, err)
	li, err = li.Truncate(900)
	require.Nil(t, err)
	require.Equal(t, 899, li.Len())
	require.Equal(t, uint64(899), li.Last())
	_, err = li.Truncate(900)
	require.NotNil(t, err)

	w := &bytes.Buffer{}
	err = li.Encode(w)
	require.Nil(t, err)
	require.Equal(t, uint64(17984), li.Size())

	indexes := decodeIndex(w.Bytes())
	require.Equal(t, li.Len(), indexes.Len())
	for i := 0; i < li.Len(); i++ {
		require.Equal(t, li[i], indexes[i])
	}
}
