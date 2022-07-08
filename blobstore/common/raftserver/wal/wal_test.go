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
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type getEntriesTest struct {
	lo         uint64
	hi         uint64
	matchError bool
	err        error
}

func genRandomBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func TestRaftWal(t *testing.T) {
	dir := "/tmp/raftwal-" + string(genRandomBytes(8))
	clear := func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	clear()
	defer clear()
	wal, err := OpenWal(dir, true)
	require.Nil(t, err)

	entries := make([]pb.Entry, 0, 100000)
	for i := 0; i < 100000; i++ {
		entry := pb.Entry{
			Term:  uint64((i + 1) % 1000),
			Index: uint64(i + 1),
			Type:  pb.EntryNormal,
			Data:  genRandomBytes(1024),
		}
		entries = append(entries, entry)
	}

	err = wal.SaveEntries(entries)
	require.Nil(t, err)

	err = wal.Truncate(1000)
	require.Nil(t, err)
	err = wal.Truncate(1000)
	require.Equal(t, raft.ErrCompacted, err)

	testCase := []getEntriesTest{
		{1, 1, true, raft.ErrUnavailable},
		{1000, 1, true, raft.ErrUnavailable},
		{1, 2, true, raft.ErrCompacted},
		{1000, 2000, true, raft.ErrCompacted},
		{1001, 100002, false, nil},
		{100001, 1000002, false, nil},
	}

	for _, tc := range testCase {
		_, err = wal.Entries(tc.lo, tc.hi, 64*1024*1024)
		if err != nil {
			if tc.matchError && err != tc.err {
				t.Fatal(err)
			}
		} else {
			t.Fatalf("need return error, dir(%s) testCase(%v)", dir, tc)
		}
	}

	entries, err = wal.Entries(1001, 100000, math.MaxUint64)
	require.Nil(t, err)
	for i, entry := range entries {
		require.Equal(t, uint64(i+1001), entry.Index)
	}

	_, err = wal.Term(999)
	require.Equal(t, err, raft.ErrCompacted)

	term, _ := wal.Term(1001)
	require.Equal(t, uint64(1), term)

	firstIndex := wal.FirstIndex()
	require.Equal(t, uint64(1001), firstIndex)

	lastIndex := wal.LastIndex()
	require.Equal(t, uint64(100000), lastIndex)

	entries = entries[0:0]
	for i := 0; i < 1000; i++ {
		entry := pb.Entry{
			Term:  uint64((i + 9990) % 1000),
			Index: uint64(i + 9990),
			Type:  pb.EntryNormal,
			Data:  genRandomBytes(1024),
		}
		entries = append(entries, entry)
	}
	err = wal.SaveEntries(entries)
	require.Nil(t, err)
	lastIndex = wal.LastIndex()
	require.Equal(t, uint64(10989), lastIndex)

	st := Snapshot{100, 1}
	err = wal.ApplySnapshot(st)
	require.Nil(t, err)

	firstIndex = wal.FirstIndex()
	require.Equal(t, uint64(101), firstIndex)

	lastIndex = wal.LastIndex()
	require.Equal(t, uint64(100), lastIndex)

	err = wal.SaveHardState(pb.HardState{
		Term:   3,
		Vote:   1,
		Commit: 100,
	})
	require.Nil(t, err)

	hs := wal.InitialState()
	require.Equal(t, uint64(3), hs.Term)
	require.Equal(t, uint64(1), hs.Vote)
	require.Equal(t, uint64(100), hs.Commit)

	wal.Close()
}
