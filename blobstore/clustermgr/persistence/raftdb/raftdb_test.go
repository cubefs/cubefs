// Copyright 2023 The CubeFS Authors.
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

package raftdb

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftDB(t *testing.T) {
	tmpDBPath := "/tmp/tmpraftdb" + strconv.Itoa(rand.Intn(1000000000))

	raftDB, err := OpenRaftDB(tmpDBPath, false)
	require.NoError(t, err)
	defer os.RemoveAll(tmpDBPath)

	putCases := []struct {
		key   string
		value []byte
	}{
		{
			key:   "raft-100-10000-1",
			value: []byte("raft-100-vid-10000-test1"),
		},
		{
			key:   "raft-100-10000-2",
			value: []byte("raft-101-vid-10000-test2"),
		},
		{
			key:   "raft-100-10000-11",
			value: []byte("raft-101-vid-10000-test3"),
		},
		{
			key:   "raft-101-10001-1",
			value: []byte("raft-101-vid-10001-test4"),
		},
		{
			key:   "raft-1000-1000-1",
			value: []byte("raft-1000-vid-1000-test5"),
		},
		{
			key:   "raft-100-nil-value",
			value: []byte{},
		},
	}

	for _, c := range putCases {
		err = raftDB.Put([]byte(c.key), c.value)
		require.NoError(t, err)
	}

	for i, c := range putCases {
		val, err := raftDB.Get([]byte(c.key))
		require.NoError(t, err)
		require.Equal(t, val, putCases[i].value)
	}

	raftDB.Close()
}
