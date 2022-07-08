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
	"errors"
	"os"
	"runtime"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func openLogStorage(dir string) *Wal {
	return &Wal{
		dir:         dir,
		nextFileSeq: 1,
		cache: newLogFileCache(logfileCacheNum,
			func(name logName) (*logFile, error) {
				lf, err := openLogFile(dir, name, false)
				if err != nil {
					return nil, err
				}
				runtime.SetFinalizer(lf, func(lf *logFile) {
					lf.Close()
				})
				return lf, nil
			}),
	}
}

func TestLogStorage(t *testing.T) {
	dir := "/tmp/raftwal/wal6378261765"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0o755)
	defer os.RemoveAll(dir)
	ls := openLogStorage(dir)
	err := ls.reload(1)
	require.Nil(t, err)
	names, _ := listLogFiles(dir)
	require.Equal(t, 1, len(names))
	ls.Close()

	ls = openLogStorage(dir)
	err = ls.reload(1)
	require.Nil(t, err)
	names, _ = listLogFiles(dir)
	require.Equal(t, 1, len(names))
}

func TestErrInvalid(t *testing.T) {
	require.True(t, isErrInvalid(os.ErrInvalid))
	require.True(t, isErrInvalid(os.NewSyscallError("test", syscall.EINVAL)))
	require.True(t, isErrInvalid(&os.PathError{Err: syscall.EINVAL}))
	require.False(t, isErrInvalid(errors.New("test")))
}

func TestLogStorageLastIndex(t *testing.T) {
	dir := "/tmp/raftwal-" + string(genRandomBytes(8))
	clear := func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	clear()
	defer clear()

	os.MkdirAll(dir, 0o755)

	ls := openLogStorage(dir)
	err := ls.reload(1)
	require.Nil(t, err)
	entries := make([]raftpb.Entry, 10)
	for i := 0; i < 10; i++ {
		entries[i] = raftpb.Entry{
			Term:  1,
			Index: uint64(i + 100),
			Type:  raftpb.EntryNormal,
		}
	}
	err = ls.saveEntries(entries)
	require.Nil(t, err)
	err = ls.rotate()
	require.Nil(t, err)
	lastIndex := ls.lastIndex()
	require.Equal(t, uint64(109), lastIndex)
	ls.Close()
}

func TestLogStorageTruncateFront(t *testing.T) {
	dir := "/tmp/raftwal-" + string(genRandomBytes(8))
	clear := func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	clear()
	defer clear()
	InitPath(dir)

	ls := openLogStorage(dir)
	err := ls.reload(1)
	require.Nil(t, err)
	entries := make([]raftpb.Entry, 10)
	for j := 0; j < 2; j++ {
		for i := 0; i < 10; i++ {
			entries[i] = raftpb.Entry{
				Term:  1,
				Index: uint64(i + 100 + 10*j),
				Type:  raftpb.EntryNormal,
			}
		}
		err = ls.SaveEntries(entries)
		require.Nil(t, err)
		err = ls.rotate()
		require.Nil(t, err)
	}
	err = ls.truncateFront(109)
	require.Nil(t, err)
	require.Equal(t, uint64(110), ls.logfiles[0].index)
	require.Equal(t, uint64(120), ls.logfiles[1].index)
	ls.Close()
}

func TestLogStoragetruncateBack(t *testing.T) {
	dir := "/tmp/raftwal-" + string(genRandomBytes(8))
	clear := func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	clear()
	defer clear()
	InitPath(dir)

	ls := openLogStorage(dir)
	err := ls.reload(100)
	require.Nil(t, err)
	entries := make([]raftpb.Entry, 10)
	for j := 0; j < 2; j++ {
		for i := 0; i < 10; i++ {
			entries[i] = raftpb.Entry{
				Term:  1,
				Index: uint64(i + 100 + 10*j),
				Type:  raftpb.EntryNormal,
			}
		}
		err = ls.SaveEntries(entries)
		require.Nil(t, err)
		err = ls.rotate()
		require.Nil(t, err)
	}
	err = ls.truncateBack(99)
	require.Nil(t, err)
	require.Equal(t, 1, len(ls.logfiles))
	require.Equal(t, uint64(99), ls.logfiles[0].index)

	for i := 0; i < 10; i++ {
		entries[i] = raftpb.Entry{
			Term:  1,
			Index: uint64(i + 99),
			Type:  raftpb.EntryNormal,
		}
	}
	err = ls.SaveEntries(entries)
	require.Nil(t, err)
	err = ls.truncateBack(100)
	require.Nil(t, err)
	require.Equal(t, 1, len(ls.logfiles))
	require.Equal(t, uint64(99), ls.logfiles[0].index)

	err = ls.saveEntry(&raftpb.Entry{
		Term:  0,
		Index: 1000,
		Type:  raftpb.EntryNormal,
	})
	require.NotNil(t, err)
	ls.Close()
}
