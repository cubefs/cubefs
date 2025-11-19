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
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	ErrWriteTimeout = errors.New("log: async write timeout")

	bufferPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

	_ AsyncWriter = (*AsyncLogger)(nil)
)

// AsyncWriter no-copy buffer async writer.
type AsyncWriter interface {
	Write(p []byte) (n int, err error)
	// AsyncWrite should release async buffer.
	AsyncWrite(buf AsyncBuffer) (n int, err error)
}

// AsyncBuffer reuseable buffer for asynchronous write.
type AsyncBuffer interface {
	Buffer() *bytes.Buffer
	Release()
}

func NewAsyncBuffer() AsyncBuffer {
	return &asyncBuffer{buf: bufferPool.New().(*bytes.Buffer)}
}

type asyncBuffer struct{ buf *bytes.Buffer }

func (ab *asyncBuffer) Buffer() *bytes.Buffer { return ab.buf }
func (ab *asyncBuffer) Release() {
	if ab.buf != nil {
		bufferPool.Put(ab.buf)
		ab.buf = nil
	}
}

// AsyncLogger wraps an io.Writer and performs writes asynchronously.
// It uses a buffered channel to queue write operations and a goroutine
// to process them in the background.
// But QueueSize == 0, it is a synchronous logger.
//
// AsyncLogger must not be copied after first use. It contains sync.Once,
// sync.WaitGroup, and channels that cannot be safely copied.
type AsyncLogger struct {
	// configures of lumberjack.Logger
	Filename   string `json:"filename" yaml:"filename"`
	MaxSize    int    `json:"maxsize" yaml:"maxsize"`
	MaxAge     int    `json:"maxage" yaml:"maxage"`
	MaxBackups int    `json:"maxbackups" yaml:"maxbackups"`
	LocalTime  bool   `json:"localtime" yaml:"localtime"`
	Compress   bool   `json:"compress" yaml:"compress"`

	// QueueSize is the size of the async write queue. Default is 0.
	// If it is 0, synchronous writing is used (no async wrapper).
	QueueSize int `json:"queuesize" yaml:"queuesize"`

	// WriteTimeout is the timeout for async writes. Default is 0, means no timeout.
	WriteTimeout util.Duration `json:"writetimeout" yaml:"writetimeout"`

	logger *lumberjack.Logger

	initOnce  sync.Once
	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
	flushCh   chan chan struct{}
	queue     chan AsyncBuffer
}

func (al *AsyncLogger) init() {
	al.initOnce.Do(func() {
		if al.QueueSize > 1<<20 {
			panic(fmt.Sprintf("log: large queue size %d for logger", al.QueueSize))
		}
		if al.QueueSize > 0 {
			al.queue = make(chan AsyncBuffer, al.QueueSize)
		}

		al.logger = &lumberjack.Logger{
			Filename:   al.Filename,
			MaxSize:    al.MaxSize,
			MaxAge:     al.MaxAge,
			MaxBackups: al.MaxBackups,
			LocalTime:  al.LocalTime,
			Compress:   al.Compress,
		}

		al.closeCh = make(chan struct{})
		al.flushCh = make(chan chan struct{})

		al.wg.Add(1)
		go al.run()
	})
}

func (al *AsyncLogger) run() {
	defer al.wg.Done()
	for {
		select {
		case buffer := <-al.queue:
			al.logger.Write(buffer.Buffer().Bytes())
			buffer.Release()
		case <-al.closeCh: // close
			for {
				select {
				case buffer := <-al.queue:
					al.logger.Write(buffer.Buffer().Bytes())
					buffer.Release()
				default:
					return
				}
			}
		case done := <-al.flushCh: // flush
			flushed := false
			for !flushed {
				select {
				case buffer := <-al.queue:
					al.logger.Write(buffer.Buffer().Bytes())
					buffer.Release()
				default:
					close(done)
					flushed = true
				}
			}
		}
	}
}

func (al *AsyncLogger) Flush() error {
	al.init()
	done := make(chan struct{})
	select {
	case al.flushCh <- done:
		<-done
		return nil
	case <-al.closeCh:
		return io.ErrClosedPipe
	}
}

func (al *AsyncLogger) Rotate() error {
	al.init()
	return al.logger.Rotate()
}

func (al *AsyncLogger) Close() error {
	al.init()
	al.closeOnce.Do(func() { close(al.closeCh) })
	al.wg.Wait()
	return al.logger.Close()
}

// Write queues the data for asynchronous writing.
// It never blocks; if the queue is full, it will drop the write.
func (al *AsyncLogger) Write(p []byte) (n int, err error) {
	al.init()
	if al.queue == nil {
		return al.logger.Write(p)
	}

	buffer := NewAsyncBuffer()
	buffer.Buffer().Write(p) // copy
	return al.AsyncWrite(buffer)
}

func (al *AsyncLogger) AsyncWrite(buffer AsyncBuffer) (n int, err error) {
	al.init()
	if al.queue == nil {
		n, err = al.logger.Write(buffer.Buffer().Bytes())
		buffer.Release()
		return
	}

	var timerCh <-chan time.Time
	if al.WriteTimeout.Duration > 0 {
		timer := util.TimerAcquire(al.WriteTimeout.Duration)
		timerCh = timer.C
		defer func() { util.TimerRelease(timer) }()
	}
	l := buffer.Buffer().Len()
	select {
	case al.queue <- buffer:
		return l, nil
	case <-timerCh:
		buffer.Release()
		return 0, ErrWriteTimeout
	case <-al.closeCh:
		buffer.Release()
		return 0, io.ErrClosedPipe
	}
}
