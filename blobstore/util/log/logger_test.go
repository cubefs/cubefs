// Copyright 2025 The CubeFS Authors.
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

package log

import (
	"io"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util"
)

var tempBuffer = make([]byte, 64<<10)

func tempFilename() (string, string) {
	dir := path.Join(os.TempDir(), "log-async-"+util.Any2String(time.Now().Nanosecond()))
	path := path.Join(dir, "log.log")
	return dir, path
}

func TestAyncLoggerSync(t *testing.T) {
	dir, filename := tempFilename()
	defer os.RemoveAll(dir)

	require.Panics(t, func() {
		l := &AsyncLogger{Filename: filename, QueueSize: (1 << 20) + 1}
		l.Write(make([]byte, 1))
	})

	logger := &AsyncLogger{Filename: filename}
	var wg sync.WaitGroup
	wg.Add(1000)
	for range [1000]struct{}{} {
		go func() {
			logger.Write(make([]byte, 1<<10))
			wg.Done()
		}()
	}
	wg.Wait()
	buffer := NewAsyncBuffer()
	logger.AsyncWrite(buffer)
	logger.Rotate()
	logger.Flush()
	logger.Close()
}

func TestAyncLoggerAsync(t *testing.T) {
	dir, filename := tempFilename()
	defer os.RemoveAll(dir)

	{
		logger := &AsyncLogger{
			Filename:   filename,
			MaxSize:    10,
			MaxBackups: 3,
			QueueSize:  1,
		}
		var wg sync.WaitGroup
		wg.Add(1000)
		for range [1000]struct{}{} {
			go func() {
				logger.Write(tempBuffer)
				wg.Done()
			}()
		}
		wg.Wait()
		logger.Rotate()
		logger.Flush()
		logger.Close()
	}
	{
		logger := &AsyncLogger{
			Filename:   filename,
			MaxSize:    10,
			MaxBackups: 3,
			QueueSize:  1024,
		}
		for range [10000]struct{}{} {
			go func() {
				logger.Write(tempBuffer)
			}()
		}
		logger.Flush()
		for range [1000]struct{}{} {
			go func() {
				logger.Write(tempBuffer)
			}()
		}
		logger.Close()
		require.ErrorIs(t, logger.Flush(), io.ErrClosedPipe)
	}
}

func TestAyncLoggerTimeout(t *testing.T) {
	dir, filename := tempFilename()
	defer os.RemoveAll(dir)

	{
		logger := &AsyncLogger{
			Filename:     filename,
			MaxSize:      10,
			MaxBackups:   3,
			QueueSize:    4,
			WriteTimeout: util.Duration{Duration: 100 * time.Nanosecond},
		}
		for range [10000]struct{}{} {
			go func() {
				logger.Write(tempBuffer)
			}()
		}
		logger.Flush()
		for range [1000]struct{}{} {
			go func() {
				logger.Write(tempBuffer)
			}()
		}
		logger.Close()
		for range [1000]struct{}{} {
			go func() {
				logger.Write(tempBuffer)
			}()
		}
		require.ErrorIs(t, logger.Flush(), io.ErrClosedPipe)
	}
}
