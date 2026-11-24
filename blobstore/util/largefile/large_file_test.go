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

package largefile

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLargeFile(t *testing.T) {
	tmpPath := os.TempDir() + "/largefile" + strconv.FormatInt(time.Now().Unix(), 16) + strconv.Itoa(rand.Intn(10000000))
	err := os.Mkdir(tmpPath, 0o755)
	require.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	l, err := OpenLargeFile(
		Config{
			Path:              tmpPath,
			FileChunkSizeBits: 10,
			Suffix:            ".log",
			Backup:            10,
		})
	require.NoError(t, err)
	defer l.Close()
	size, err := l.FsizeOf()
	require.NoError(t, err)
	require.Equal(t, int64(0), size)

	buf := make([]byte, 100)
	for i := 0; i < 100; i++ {
		for j := range buf {
			buf[j] = uint8(i)
		}
		n, err := l.Write(buf)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
	}

	dir, _ := os.Open(tmpPath)
	fis, _ := dir.Readdir(-1)
	for i := range fis {
		b, err := os.ReadFile(tmpPath + "/" + fis[i].Name())
		require.NoError(t, err)
		t.Log(fis[i].Name(), len(b))
	}

	off := int64(0)
	for i := 0; i < 100; i++ {
		n, err := l.ReadAt(buf, off)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		expectedBuf := make([]byte, 100)
		for j := range expectedBuf {
			expectedBuf[j] = uint8(i)
		}
		require.Equal(t, expectedBuf, buf)
		off += int64(len(buf))
	}
	_, err = l.ReadAt(buf, off)
	require.Equal(t, io.EOF, err)
}

func TestLargeFile_Rotate(t *testing.T) {
	tmpPath := os.TempDir() + "/largefile" + strconv.FormatInt(time.Now().Unix(), 16) + strconv.Itoa(rand.Intn(10000000))
	err := os.Mkdir(tmpPath, 0o755)
	require.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	l, err := OpenLargeFile(
		Config{
			Path:              tmpPath,
			FileChunkSizeBits: 10,
			Suffix:            ".log",
			Backup:            4,
		})
	require.NoError(t, err)
	defer l.Close()

	size, err := l.FsizeOf()
	require.NoError(t, err)
	require.Equal(t, int64(0), size)

	buf := make([]byte, 256)
	for i := 0; i < 20; i++ {
		n, err := l.Write(buf)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
	}
	size, err = l.FsizeOf()
	require.NoError(t, err)
	require.Equal(t, int64(5*1<<10), size)

	off := int64(0)
	for i := 0; i < 4; i++ {
		_, err := l.ReadAt(buf, off)
		require.Equal(t, io.EOF, err)
		off += int64(len(buf))
	}

	for i := 4; i < 20; i++ {
		n, err := l.ReadAt(buf, off)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		off += int64(len(buf))
	}
}

func newTestLargeFile(t *testing.T, chunkBits uint, backup int) *largeFile {
	t.Helper()
	dir := filepath.Join(os.TempDir(), "largefile_write_test_"+t.Name())
	require.NoError(t, os.MkdirAll(dir, 0o755))
	t.Cleanup(func() { os.RemoveAll(dir) })

	alf, err := OpenLargeFile(Config{
		Path:              dir,
		FileChunkSizeBits: chunkBits,
		Backup:            backup,
	})
	require.NoError(t, err)
	t.Cleanup(func() { alf.Close() })
	return alf.(*asyncLargeFile).lf.(*largeFile)
}

// TestWriteRollbackOffsetOnFailure verifies that a failed write does not
// burn the offset: repeated failures (e.g. ENOSPC on a full disk) would
// otherwise keep crossing chunk boundaries and rotate a series of empty
// files, evicting real logs from the backup window.
func TestWriteRollbackOffsetOnFailure(t *testing.T) {
	l := newTestLargeFile(t, 20, 3)

	line1 := []byte("first line\n")
	n, err := l.Write(line1)
	require.NoError(t, err)
	require.Equal(t, len(line1), n)
	off := atomic.LoadInt64(&l.off)

	// simulate a failing write by closing the cached file descriptor
	for _, fh := range l.fhs {
		if fh != nil {
			require.NoError(t, fh.f.Close())
		}
	}
	entriesBefore, err := os.ReadDir(l.Path)
	require.NoError(t, err)

	failed := []byte("second line\n")
	_, err = l.Write(failed)
	require.Error(t, err)
	require.Equal(t, off, atomic.LoadInt64(&l.off), "failed write must not burn the offset")

	// the offset is rolled back, so repeated failures do not cross chunk
	// boundaries and no empty file is rotated
	entriesAfter, err := os.ReadDir(l.Path)
	require.NoError(t, err)
	require.Len(t, entriesAfter, len(entriesBefore), "failed writes must not rotate new files")

	// recovery: drop the closed-fd cache so the file is reopened
	for i := range l.fhs {
		l.fhs[i] = nil
	}
	line3 := []byte("third line\n")
	n, err = l.Write(line3)
	require.NoError(t, err)
	require.Equal(t, len(line3), n)
	require.Equal(t, off+int64(len(line3)), atomic.LoadInt64(&l.off))

	// the failed line left no hole: the file holds line1 followed by line3
	entries, err := os.ReadDir(l.Path)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	data, err := os.ReadFile(filepath.Join(l.Path, entries[0].Name()))
	require.NoError(t, err)
	require.Equal(t, "first line\nthird line\n", string(data))
}

// TestCheckRotateEvictBeyondBackup is a regression guard that the backup
// eviction itself is untouched by the write-rollback change.
func TestCheckRotateEvictBeyondBackup(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "largefile_rotate_test_"+t.Name())
	require.NoError(t, os.MkdirAll(dir, 0o755))
	t.Cleanup(func() { os.RemoveAll(dir) })

	l := &largeFile{
		Config: Config{Path: dir, FileChunkSizeBits: 20, Backup: 2},
		fisM:   make(map[int64]os.FileInfo),
	}

	mkFile := func(idx, startOff int64, data string) {
		name := l.encodeFileName(idx, startOff)
		require.NoError(t, os.WriteFile(name, []byte(data), 0o666))
		fi, err := os.Stat(name)
		require.NoError(t, err)
		l.fis = append(l.fis, fi)
		l.fisM[idx] = fi
	}
	mkFile(0, 0, "real one\n")
	mkFile(1, 1<<20, "real two\n")
	mkFile(2, 2<<20, "real three\n")
	mkFile(3, 3<<20, "real four\n")
	atomic.StoreInt64(&l.latestIdx, 3)

	require.NoError(t, l.checkRotate())

	// the oldest two files are evicted, keeping the backup window
	require.Len(t, l.fis, 2)
	_, err := os.Stat(filepath.Join(dir, l.fis[0].Name()))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, l.fis[1].Name()))
	require.NoError(t, err)
}
