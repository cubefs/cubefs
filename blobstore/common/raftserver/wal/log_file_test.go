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
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestLogFile(t *testing.T) {
	dir := "/tmp/wal62387348"
	os.RemoveAll(dir)
	os.Mkdir(dir, 0o755)
	lf, err := createLogFile(dir, logName{1, 1000})
	require.Nil(t, err)
	for index := 1; index <= 1000; index++ {
		entry := &pb.Entry{
			Term:  uint64(index),
			Index: uint64(index),
			Type:  pb.EntryNormal,
		}
		err = lf.Save(entry)
		require.Nil(t, err)
	}
	err = lf.Flush()
	require.Nil(t, err)
	err = lf.Sync()
	require.Nil(t, err)

	require.Equal(t, logName{1, 1000}, lf.Name())
	require.Equal(t, uint64(1), lf.Seq())
	require.Equal(t, uint64(1), lf.FirstIndex())
	require.Equal(t, uint64(1000), lf.LastIndex())
	require.Equal(t, 1000, lf.Len())

	entry, err := lf.Get(900)
	require.Nil(t, err)
	require.Equal(t, uint64(900), entry.Index)
	term, err := lf.Term(800)
	require.Nil(t, err)
	require.Equal(t, uint64(800), term)

	err = lf.Truncate(900)
	require.Nil(t, err)
	require.Equal(t, uint64(899), lf.LastIndex())
	require.Equal(t, 899, lf.Len())

	err = lf.FinishWrite()
	require.Nil(t, err)
	lf.Close()

	lf, err = openLogFile(dir, logName{1, 1000}, false)
	require.Nil(t, err)
	lf.Close()
	lf, err = openLogFile(dir, logName{1, 1000}, true)
	require.Nil(t, err)
	lf.Close()
}

func TestReadIndex(t *testing.T) {
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

	// create log file
	lf, err := createLogFile(dir, logName{10, 10})
	require.Nil(t, err)

	require.NotNil(t, lf.loadIndex())
	rec := footer{
		indexOffset: 0,
	}
	copy(rec.magic[:], magic)
	w := lf.w
	binary.BigEndian.PutUint64(w.b, rec.Size()+1)
	w.buf.Write(w.b)
	w.buf.Write([]byte{byte(FooterType)})
	rec.Encode(w.buf)
	binary.BigEndian.PutUint32(w.b, 12345678)
	w.buf.Write(w.b[0:4])
	w.off += int64(recordSize(&rec))
	w.Flush()
	require.NotNil(t, lf.loadIndex())
	lf.Close()

	lf, err = createLogFile(dir, logName{10, 10})
	require.Nil(t, err)
	lf.w.Write(EntryType, &rec)
	lf.w.Flush()
	require.NotNil(t, lf.loadIndex())
	lf.Close()

	lf, err = createLogFile(dir, logName{10, 10})
	require.Nil(t, err)
	rec.magic[0] = 3
	w = lf.w
	binary.BigEndian.PutUint64(w.b, rec.Size()+1)
	w.buf.Write(w.b)
	crc := crc32.NewIEEE()
	mw := io.MultiWriter(w.buf, crc)
	mw.Write([]byte{byte(FooterType)})
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf, rec.indexOffset)
	copy(buf[8:], rec.magic[:])
	mw.Write(buf)
	binary.BigEndian.PutUint32(w.b, crc.Sum32())
	w.buf.Write(w.b[0:4])
	w.off += int64(recordSize(&rec))
	lf.w.Flush()
	require.NotNil(t, lf.loadIndex())
	lf.Close()

	lf, err = createLogFile(dir, logName{10, 10})
	require.Nil(t, err)
	w = lf.w
	entry := &Entry{
		Entry: pb.Entry{
			Term:  uint64(10),
			Index: uint64(10),
			Type:  pb.EntryNormal,
		},
	}
	w.Write(EntryType, entry)
	rec.indexOffset = uint64(w.off)
	w.Write(FooterType, &rec)
	w.Flush()
	require.NotNil(t, lf.loadIndex())
	lf.Close()

	lf, err = createLogFile(dir, logName{10, 10})
	require.Nil(t, err)
	w = lf.w
	entry = &Entry{
		Entry: pb.Entry{
			Term:  uint64(10),
			Index: uint64(10),
			Type:  pb.EntryNormal,
		},
	}
	w.Write(EntryType, entry)
	b := w.buf.Bytes()
	b[len(b)-1] = 10
	rec.indexOffset = 0
	w.Write(FooterType, &rec)
	w.Flush()
	lf.loadIndex()
	require.NotNil(t, lf.loadIndex())
	lf.Close()
}

func TestRebuildIndex(t *testing.T) {
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

	// create log file
	lf, err := createLogFile(dir, logName{20, 20})
	require.Nil(t, err)

	w := lf.w
	rec := footer{
		indexOffset: 0,
	}
	copy(rec.magic[:], magic)
	w.Write(EntryType, &rec)
	w.Flush()

	w.file.Seek(0, io.SeekStart)
	offset, err := lf.recoverIndex()
	require.Nil(t, err)
	require.Equal(t, 0, int(offset))
	lf.Close()
}
