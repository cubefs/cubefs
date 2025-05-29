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
	"context"
	"io"
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const _limited = "limited"

type qosLimiter interface {
	ReserveN(t time.Time, n int) *rate.Reservation
	UpdateQosBpsLimiter()
	UpdateQosConcurrency()
}

type rateLimiter struct {
	readerAt io.ReaderAt
	reader   io.Reader
	writer   io.Writer
	writerAt io.WriterAt
	ctx      context.Context

	ctrl qosLimiter
}

func (l *rateLimiter) Read(p []byte) (n int, err error) {
	err = l.doWithLimit(len(p))
	if err != nil {
		return
	}

	return l.reader.Read(p)
}

func (l *rateLimiter) readAt(p []byte, off int64) (n int, err error) {
	err = l.doWithLimit(len(p))
	if err != nil {
		return
	}

	return l.readerAt.ReadAt(p, off)
}

func (l *rateLimiter) ReadAt(p []byte, off int64) (readn int, err error) {
	select {
	case <-l.ctx.Done():
		return 0, l.ctx.Err()
	default:
	}

	var nn int
	for readn < len(p) && err == nil {
		nn, err = l.readAt(p[readn:], off)
		off += int64(nn)
		readn += nn
	}
	return
}

func (l *rateLimiter) Write(p []byte) (n int, err error) {
	err = l.doWithLimit(len(p))
	if err != nil {
		return
	}

	return l.writer.Write(p)
}

func (l *rateLimiter) WriteAt(p []byte, off int64) (n int, err error) {
	err = l.doWithLimit(len(p))
	if err != nil {
		return
	}

	return l.writerAt.WriteAt(p, off)
}

func (l *rateLimiter) doWithLimit(n int) (err error) {
	l.ctrl.UpdateQosBpsLimiter()
	l.ctrl.UpdateQosConcurrency()

	return l.doWithSingleLimit(n)
}

func (l *rateLimiter) doWithSingleLimit(n int) (err error) {
	now := time.Now()
	reserve := l.ctrl.ReserveN(now, n)
	if !reserve.OK() {
		return errors.ErrSizeOverBurst
	}
	delay := reserve.DelayFrom(now)
	if delay == 0 {
		return
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	span := trace.SpanFromContextSafe(l.ctx)
	span.SetTag(_limited, delay.Milliseconds())
	select {
	case <-t.C:
		return
	case <-l.ctx.Done():
		reserve.Cancel()
		err = l.ctx.Err()
		return
	}
}
