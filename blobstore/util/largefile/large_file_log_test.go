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
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLargeFileLog(t *testing.T) {
	tmpPath := os.TempDir() + "/largefilelog" + strconv.FormatInt(time.Now().Unix(), 16) + strconv.Itoa(rand.Intn(10000000))
	err := os.Mkdir(tmpPath, 0o755)
	require.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	cfg := Config{
		Path:              tmpPath,
		FileChunkSizeBits: 10,
		Suffix:            ".log",
		Backup:            8,
	}
	l, err := OpenLargeFileLog(cfg, false)
	require.NoError(t, err)
	buf := make([]byte, 100)
	for i := 0; i < 100; i++ {
		err := l.Log(buf)
		require.NoError(t, err)
	}
	l.Close()

	l, err = OpenLargeFileLog(cfg, false)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		err := l.Log(buf)
		require.NoError(t, err)
	}
	l.Close()

	l, err = OpenLargeFileLog(cfg, true)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		err := l.Log(buf)
		require.NoError(t, err)
	}
	l.Close()

	l, err = OpenLargeFileLog(cfg, true)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		err := l.Log(buf)
		require.NoError(t, err)
	}
	l.Close()
}

type countedLine struct {
	buf      []byte
	released *int32
}

func (c *countedLine) Buffer() []byte { return c.buf }
func (c *countedLine) Release()       { atomic.AddInt32(c.released, 1) }

func TestLargeLoggerOwned(t *testing.T) {
	l, err := OpenLargeFileLog(Config{
		Path:              t.TempDir(),
		FileChunkSizeBits: 12,
		Backup:            2,
		Suffix:            ".log",
	}, true)
	require.NoError(t, err)
	defer l.Close()

	var released int32
	line := &countedLine{buf: []byte("line"), released: &released}
	require.NoError(t, l.LogOwned(line))
	require.Equal(t, int32(1), atomic.LoadInt32(&released))
}

// TestLargeLoggerEmptyLine verifies an empty line is written as a bare newline
// instead of panicking on msg[len(msg)-1].
func TestLargeLoggerEmptyLine(t *testing.T) {
	l, err := OpenLargeFileLog(Config{
		Path:              t.TempDir(),
		FileChunkSizeBits: 12,
		Backup:            2,
		Suffix:            ".log",
	}, true)
	require.NoError(t, err)
	defer l.Close()

	require.NoError(t, l.Log(nil))
	require.NoError(t, l.Log([]byte{}))

	require.NoError(t, l.LogOwned(&pooledLine{}))
}
