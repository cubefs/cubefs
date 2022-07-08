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
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordReaderRead(t *testing.T) {
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

	f, err := os.OpenFile(path.Join(dir, "test"), os.O_CREATE|os.O_RDWR, 0o644)
	require.Nil(t, err)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, 64)
	f.Write(b)
	f.Seek(0, io.SeekStart)
	rr := &recordReader{
		br:         bufio.NewReaderSize(f, defaultReadBufferedSize),
		sr:         f,
		filename:   f.Name(),
		typeLenBuf: make([]byte, 8), // 8 byte dataLen
	}

	_, _, err = rr.Read()
	require.NotNil(t, err)

	f.Seek(8, io.SeekStart)
	f.Write(genRandomBytes(60))
	rr.br.Reset(f)
	f.Seek(0, io.SeekStart)
	_, _, err = rr.Read()
	require.NotNil(t, err)

	f.Seek(8+60, io.SeekStart)
	f.Write(genRandomBytes(4))
	f.Seek(0, io.SeekStart)
	rr.br.Reset(f)
	_, _, err = rr.Read()
	require.NotNil(t, err)

	f.Seek(8+64, io.SeekStart)
	f.Write(genRandomBytes(4))
	f.Seek(0, io.SeekStart)
	rr.br.Reset(f)
	_, _, err = rr.Read()
	require.NotNil(t, err)

	f.Close()
}

func TestRecordReaderReadAt(t *testing.T) {
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

	f, err := os.OpenFile(path.Join(dir, "test"), os.O_CREATE|os.O_RDWR, 0o644)
	require.Nil(t, err)

	rr := newRecordReader(f)
	_, err = rr.ReadAt(0)
	require.NotNil(t, err)

	f.Write(genRandomBytes(4))
	_, err = rr.ReadAt(0)
	require.NotNil(t, err)

	f.Seek(0, io.SeekStart)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, 64)
	f.Write(b)
	f.Write(genRandomBytes(64))
	_, err = rr.ReadAt(0)
	require.NotNil(t, err)

	f.Close()
}
