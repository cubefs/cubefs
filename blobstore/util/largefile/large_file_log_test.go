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
