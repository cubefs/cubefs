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
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLargeFile(t *testing.T) {
	tmpPath := os.TempDir() + "/largefile" + strconv.FormatInt(time.Now().Unix(), 16) + strconv.Itoa(rand.Intn(10000000))
	err := os.Mkdir(tmpPath, 0o755)
	assert.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	l, err := OpenLargeFile(
		Config{
			Path:              tmpPath,
			FileChunkSizeBits: 10,
			Suffix:            ".log",
			Backup:            10,
		})
	assert.NoError(t, err)
	defer l.Close()
	size, err := l.FsizeOf()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)

	buf := make([]byte, 100)
	for i := 0; i < 100; i++ {
		for j := range buf {
			buf[j] = uint8(i)
		}
		n, err := l.Write(buf)
		assert.NoError(t, err)
		assert.Equal(t, len(buf), n)
	}

	dir, _ := os.Open(tmpPath)
	fis, _ := dir.Readdir(-1)
	for i := range fis {
		b, err := ioutil.ReadFile(tmpPath + "/" + fis[i].Name())
		assert.NoError(t, err)
		t.Log(fis[i].Name(), len(b))
	}

	off := int64(0)
	for i := 0; i < 100; i++ {
		n, err := l.ReadAt(buf, off)
		assert.NoError(t, err)
		assert.Equal(t, len(buf), n)
		expectedBuf := make([]byte, 100)
		for j := range expectedBuf {
			expectedBuf[j] = uint8(i)
		}
		assert.Equal(t, expectedBuf, buf)
		off += int64(len(buf))
	}
	_, err = l.ReadAt(buf, off)
	assert.Equal(t, io.EOF, err)
}

func TestLargeFile_Rotate(t *testing.T) {
	tmpPath := os.TempDir() + "/largefile" + strconv.FormatInt(time.Now().Unix(), 16) + strconv.Itoa(rand.Intn(10000000))
	err := os.Mkdir(tmpPath, 0o755)
	assert.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	l, err := OpenLargeFile(
		Config{
			Path:              tmpPath,
			FileChunkSizeBits: 10,
			Suffix:            ".log",
			Backup:            4,
		})
	assert.NoError(t, err)
	defer l.Close()

	size, err := l.FsizeOf()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)

	buf := make([]byte, 256)
	for i := 0; i < 20; i++ {
		n, err := l.Write(buf)
		assert.NoError(t, err)
		assert.Equal(t, len(buf), n)
	}
	size, err = l.FsizeOf()
	assert.NoError(t, err)
	assert.Equal(t, int64(5*1<<10), size)

	off := int64(0)
	for i := 0; i < 4; i++ {
		_, err := l.ReadAt(buf, off)
		assert.Equal(t, io.EOF, err)
		off += int64(len(buf))
	}

	for i := 4; i < 20; i++ {
		n, err := l.ReadAt(buf, off)
		assert.NoError(t, err)
		assert.Equal(t, len(buf), n)
		off += int64(len(buf))
	}
}
