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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

const (
	_mb = 1024 * 1024
)

var (
	bufferSize = 4 * 1024 * 1024
	buffer     = make([]byte, bufferSize)
)

func TestRateLimiter_Read(t *testing.T) {
	levelQos := &levelQos{bpsLimiter: rate.NewLimiter(rate.Limit(2*_mb), 4*_mb)}
	now := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	// consume initial tokens
	go func() {
		defer wg.Done()
		buffer1 := make([]byte, bufferSize)
		rc1 := &rateLimiter{
			ctx:      context.TODO(),
			reader:   bytes.NewReader(buffer1),
			levelQos: levelQos,
		}
		b := make([]byte, bufferSize)
		n, err := io.ReadFull(rc1, b)
		require.NoError(t, err)
		require.Equal(t, bufferSize, n)
	}()
	// actual test traffic limit
	go func() {
		defer wg.Done()
		buffer2 := make([]byte, bufferSize)
		rc2 := &rateLimiter{
			ctx:      context.TODO(),
			reader:   bytes.NewReader(buffer2),
			levelQos: levelQos,
		}
		b := make([]byte, bufferSize)
		n, err := io.ReadFull(rc2, b)
		require.NoError(t, err)
		require.Equal(t, bufferSize, n)
	}()
	wg.Wait()
	elapsed := time.Since(now).Seconds()
	require.True(t, math.Abs(2-elapsed) < 0.5)
}

func TestRateLimiter_Write(t *testing.T) {
	levelQos := &levelQos{bpsLimiter: rate.NewLimiter(rate.Limit(2*_mb), 4*_mb)}
	now := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	// consume initial tokens
	go func() {
		defer wg.Done()
		w := bytes.NewBuffer(nil)
		wc := &rateLimiter{
			ctx:      context.TODO(),
			writer:   w,
			levelQos: levelQos,
		}
		n, err := io.Copy(wc, bytes.NewReader(buffer))
		require.NoError(t, err)
		require.Equal(t, bufferSize, int(n))
		require.Equal(t, buffer, w.Bytes())
	}()
	// actual test traffic limit
	go func() {
		defer wg.Done()
		w := bytes.NewBuffer(nil)
		wc := &rateLimiter{
			ctx:      context.TODO(),
			writer:   w,
			levelQos: levelQos,
		}
		n, err := io.Copy(wc, bytes.NewReader(buffer))
		require.NoError(t, err)
		require.Equal(t, bufferSize, int(n))
		require.Equal(t, buffer, w.Bytes())
	}()
	wg.Wait()
	elapsed := time.Since(now).Seconds()
	require.True(t, math.Abs(2-elapsed) < 0.8)
}

func TestRateLimiter_Error(t *testing.T) {
	levelQos := &levelQos{bpsLimiter: rate.NewLimiter(rate.Limit(2*_mb), 3*_mb)}
	now := time.Now()
	w := bytes.NewBuffer(nil)
	wc := &rateLimiter{
		ctx:      context.TODO(),
		writer:   w,
		levelQos: levelQos,
	}
	n, err := io.Copy(wc, bytes.NewReader(buffer))
	elapsed := time.Since(now).Seconds()
	require.True(t, math.Abs(elapsed) < 0.1)
	require.Error(t, err)
	require.Equal(t, bufferSize, int(n))
	require.Equal(t, buffer, w.Bytes())
}

func TestRateLimiter_WriteAt(t *testing.T) {
	levelQos := &levelQos{bpsLimiter: rate.NewLimiter(rate.Limit(2*_mb), 4*_mb)}
	workDir, err := ioutil.TempDir(os.TempDir(), "workDir")
	require.NoError(t, err)
	defer os.RemoveAll(workDir)

	path1 := filepath.Join(workDir, "path1")
	defer os.Remove(path1)

	w, err := os.OpenFile(path1, os.O_CREATE|os.O_RDWR, 0o666)
	require.NoError(t, err)
	wc := &rateLimiter{
		ctx:      context.TODO(),
		writerAt: w,
		levelQos: levelQos,
	}

	now := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	// consume initial tokens
	go func() {
		defer wg.Done()
		n, err := wc.WriteAt(_buffer, 0)
		require.NoError(t, err)
		require.Equal(t, bufferSize, n)
	}()
	// actual test traffic limit
	go func() {
		defer wg.Done()
		n, err := wc.WriteAt(_buffer, 4*_mb)
		require.NoError(t, err)
		require.Equal(t, bufferSize, n)
	}()
	wg.Wait()

	elapsed := time.Since(now).Seconds()
	require.True(t, math.Abs(2-elapsed) < 0.1)
}
