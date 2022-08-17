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

package qos

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/blobnode/base/limitio"
)

const (
	_mb = 1024 * 1024
)

var (
	bufferSize = 4 * 1024 * 1024
	buffer     = make([]byte, bufferSize)
)

func TestBpsReader_Read(t *testing.T) {
	{
		c := limitio.NewController(1 * _mb)
		rc := NewBpsReader(context.TODO(), bytes.NewReader(buffer), c)

		b := make([]byte, bufferSize)
		now := time.Now()
		n, err := io.ReadFull(rc, b)
		elapsed := time.Since(now).Seconds()
		require.NoError(t, err)
		require.True(t, math.Abs(4-elapsed) < 0.5)
		require.Equal(t, bufferSize, n)
		require.Equal(t, buffer, b)
	}
}

func TestBpsWriter_Write(t *testing.T) {
	{
		w := bytes.NewBuffer(nil)
		c := limitio.NewController(2 * _mb)
		wc := NewBpsWriter(context.TODO(), w, c)

		now := time.Now()
		n, err := io.Copy(wc, bytes.NewReader(buffer))
		elapsed := time.Since(now).Seconds()

		require.NoError(t, err)
		require.True(t, math.Abs(2-elapsed) < 0.8)
		require.Equal(t, bufferSize, int(n))
		require.Equal(t, buffer, w.Bytes())
	}
}

func TestBpsWriter_WriteAt(t *testing.T) {
	{
		workDir, err := ioutil.TempDir(os.TempDir(), "workDir")
		require.NoError(t, err)
		defer os.RemoveAll(workDir)

		path1 := filepath.Join(workDir, "path1")
		defer os.Remove(path1)

		w, err := os.OpenFile(path1, os.O_CREATE|os.O_RDWR, 0o666)
		require.NoError(t, err)
		c := limitio.NewController(4 * _mb)
		wc := NewBpsWriterAt(context.TODO(), w, c)

		now := time.Now()
		n, err := wc.WriteAt(_buffer, 0)
		elapsed := time.Since(now).Seconds()

		require.NoError(t, err)
		require.True(t, math.Abs(1-elapsed) < 0.8)
		require.Equal(t, bufferSize, int(n))
	}
}

func TestBPSWriter(t *testing.T) {
	{
		buffer := bytes.NewBuffer(nil)
		c := limitio.NewController(2 * _mb)
		w := NewBpsWriter(context.TODO(), buffer, c)

		now := time.Now()
		n, err := w.Write(_buffer)
		elapsed := time.Since(now).Seconds()
		require.True(t, math.Abs(2-elapsed) < 0.5)
		require.NoError(t, err)
		require.Equal(t, _bufferSize, int64(n))
		require.Equal(t, _buffer, buffer.Bytes())
	}
}
