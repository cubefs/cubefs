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
	"crypto/rand"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
)

var (
	_bufferSize int64 = 4 * 1024 * 1024
	_buffer           = make([]byte, _bufferSize)
)

func init() {
	runtime.GOMAXPROCS(8)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	rand.Read(_buffer)
}

func TestQoSControllerReader(t *testing.T) {
	{
		ctx := context.Background()
		ioStat, _ := iostat.StatInit("", 0, true)
		iom := &flow.IOFlowStat{}
		iom[0] = ioStat
		input := Threshold{
			DiskBandwidth: 10 * 1024 * 1024,
			DiskIOPS:      100,

			ParaConfig: ParaConfig{
				Bandwidth: 2 << 20,
				Iops:      512,
			},
		}
		diskIO := flow.NewDiskViewer(iom)
		c := NewLevelQos(&input, diskIO)
		r1 := c.Reader(ctx, ioStat.Reader(bytes.NewReader(_buffer)))
		r2 := c.Reader(ctx, ioStat.Reader(bytes.NewReader(_buffer)))
		defer c.Close()

		b1 := make([]byte, _bufferSize)
		b2 := make([]byte, _bufferSize)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			n, err := io.ReadFull(r1, b1)
			assert.NoError(t, err)
			assert.Equal(t, _bufferSize, int64(n))
			wg.Done()
		}()
		go func() {
			time.Sleep(1 * time.Second)
			n, err := io.ReadFull(r2, b2)
			assert.NoError(t, err)
			assert.Equal(t, _bufferSize, int64(n))
			wg.Done()
		}()
		now := time.Now()
		wg.Wait()
		elapsed := time.Since(now).Seconds()
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.Equal(t, _buffer, b1)
		assert.Equal(t, _buffer, b2)
	}
}

func TestQoSControllerReaderAt(t *testing.T) {
	{
		ctx := context.Background()
		ioStat, _ := iostat.StatInit("", 0, true)
		iom := &flow.IOFlowStat{}
		iom[0] = ioStat
		input := Threshold{
			DiskBandwidth: 10 * 1024 * 1024,
			DiskIOPS:      100,

			ParaConfig: ParaConfig{
				Bandwidth: 2 << 20,
				Iops:      512,
			},
		}
		diskIO := flow.NewDiskViewer(iom)
		c := NewLevelQos(&input, diskIO)
		r1 := c.ReaderAt(ctx, ioStat.ReaderAt(bytes.NewReader(_buffer)))
		r2 := c.ReaderAt(ctx, ioStat.ReaderAt(bytes.NewReader(_buffer)))
		sc1 := io.NewSectionReader(r1, 0, _bufferSize)
		sc2 := io.NewSectionReader(r2, 0, _bufferSize)
		defer c.Close()

		b1 := make([]byte, _bufferSize)
		b2 := make([]byte, _bufferSize)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			n, err := io.ReadFull(sc1, b1)
			assert.NoError(t, err)
			assert.Equal(t, _bufferSize, int64(n))
			wg.Done()
		}()
		go func() {
			time.Sleep(1 * time.Second)
			n, err := io.ReadFull(sc2, b2)
			assert.NoError(t, err)
			assert.Equal(t, _bufferSize, int64(n))
			wg.Done()
		}()
		now := time.Now()
		wg.Wait()
		elapsed := time.Since(now).Seconds()
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.Equal(t, _buffer, b1)
		assert.Equal(t, _buffer, b2)
	}
}

func TestBPSControllerWriter(t *testing.T) {
	{
		ctx := context.Background()
		ioStat, _ := iostat.StatInit("", 0, true)
		iom := &flow.IOFlowStat{}
		iom[0] = ioStat
		input := Threshold{
			DiskBandwidth: 10 * 1024 * 1024,
			DiskIOPS:      100,

			ParaConfig: ParaConfig{
				Bandwidth: 2 << 20,
				Iops:      512,
			},
		}
		diskIO := flow.NewDiskViewer(iom)
		h := NewLevelQos(&input, diskIO)
		defer h.Close()
		b1 := new(bytes.Buffer)
		b2 := new(bytes.Buffer)
		w1 := h.Writer(ctx, ioStat.Writer(b1))
		w2 := h.Writer(ctx, ioStat.Writer(b2))

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			n, err := io.Copy(w1, bytes.NewReader(_buffer))
			assert.NoError(t, err)
			assert.Equal(t, _bufferSize, n)
			wg.Done()
		}()
		go func() {
			time.Sleep(1 * time.Second)
			n, err := io.Copy(w2, bytes.NewReader(_buffer))
			assert.NoError(t, err)
			assert.Equal(t, _bufferSize, n)
			wg.Done()
		}()

		now := time.Now()
		wg.Wait()
		elapsed := time.Since(now).Seconds()
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.Equal(t, _buffer, b1.Bytes())
		assert.Equal(t, _buffer, b2.Bytes())
	}
}

func TestBPSControllerWriterAt(t *testing.T) {
	ctx := context.Background()
	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	iom[0] = ioStat
	input := Threshold{
		DiskBandwidth: 10 * 1024 * 1024,
		DiskIOPS:      100,

		ParaConfig: ParaConfig{
			Bandwidth: 2 << 20,
			Iops:      512,
		},
	}
	diskIO := flow.NewDiskViewer(iom)
	c := NewLevelQos(&input, diskIO)
	defer c.Close()

	workDir, err := ioutil.TempDir(os.TempDir(), "workDir")
	assert.NoError(t, err)
	defer os.RemoveAll(workDir)
	path1 := filepath.Join(workDir, "path1")
	path2 := filepath.Join(workDir, "path2")
	defer os.Remove(path1)
	defer os.Remove(path2)

	b1, err := os.OpenFile(path1, os.O_CREATE|os.O_RDWR, 0o666)
	assert.NoError(t, err)
	b2, err := os.OpenFile(path2, os.O_CREATE|os.O_RDWR, 0o666)
	assert.NoError(t, err)
	w1 := c.WriterAt(ctx, ioStat.WriterAt(b1))
	w2 := c.WriterAt(ctx, ioStat.WriterAt(b2))
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		n, err := w1.WriteAt(_buffer, 0)
		assert.NoError(t, err)
		assert.Equal(t, _bufferSize, int64(n))
		wg.Done()
	}()
	go func() {
		time.Sleep(1 * time.Second)
		n, err := w2.WriteAt(_buffer, 0)
		assert.NoError(t, err)
		assert.Equal(t, _bufferSize, int64(n))
		wg.Done()
	}()
	now := time.Now()
	wg.Wait()
	elapsed := time.Since(now).Seconds()
	assert.True(t, math.Abs(4-elapsed) < 0.8)
}
