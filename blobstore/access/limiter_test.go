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

package access

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/limit"
)

type (
	limitReader struct {
		size int
		read int
	}
	limitWriter struct{}
)

func (r *limitReader) Read(p []byte) (n int, err error) {
	if r.read >= r.size {
		return 0, io.EOF
	}

	b := make([]byte, 1<<20)
	n = copy(p, b)
	r.read += n
	return
}

func (w *limitWriter) Write(p []byte) (n int, err error) {
	b := make([]byte, 1<<20)
	n = 0
	for len(p) > 0 {
		nn := copy(p, b)
		n += nn
		p = p[nn:]
	}
	return
}

func TestAccessLimitReader(t *testing.T) {
	ctx, cancel := context.WithCancel(ctxWithName("TestAccessLimitReader")())
	r := &Reader{
		ctx:        ctx,
		rate:       rate.NewLimiter(rate.Limit(1<<20), 1<<20),
		underlying: &limitReader{size: 1 << 24},
	}
	errCh := make(chan error)
	go func() {
		buf := make([]byte, 1<<24)
		_, err := io.ReadFull(r, buf)
		errCh <- err
	}()

	time.Sleep(time.Second)
	cancel()

	err := <-errCh
	require.Error(t, err)
}

func TestAccessLimitWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(ctxWithName("TestAccessLimitWriter")())
	w := &Writer{
		ctx:        ctx,
		rate:       rate.NewLimiter(rate.Limit(1<<20), 1<<20),
		underlying: &limitWriter{},
	}
	errCh := make(chan error)
	go func() {
		_, err := w.Write(make([]byte, 1<<24))
		errCh <- err
	}()

	time.Sleep(time.Second)
	cancel()

	err := <-errCh
	require.Error(t, err)
}

func TestAccessLimiterBase(t *testing.T) {
	nameGet := "get"
	namePut := "put"
	cfg := LimitConfig{
		NameRps: map[string]int{
			nameGet: 1,
			namePut: 1,
		},
		ReaderMBps: 1,
		WriterMBps: 1,
	}
	l := NewLimiter(cfg)

	{
		for range [100]struct{}{} {
			err := l.Acquire("")
			assert.NoError(t, err)
		}
		l.Release("")
	}
	{
		err := l.Acquire(nameGet)
		assert.NoError(t, err)
		err = l.Acquire(nameGet)
		assert.Equal(t, limit.ErrLimited, err)
		l.Release(nameGet)
		err = l.Acquire(nameGet)
		assert.NoError(t, err)
		l.Release(nameGet)
	}
	{
		err := l.Acquire(nameGet)
		assert.NoError(t, err)
		err = l.Acquire(namePut)
		assert.NoError(t, err)

		err = l.Acquire(nameGet)
		assert.Equal(t, limit.ErrLimited, err)
		err = l.Acquire(namePut)
		assert.Equal(t, limit.ErrLimited, err)

		l.Release(nameGet)
		l.Release(namePut)
	}

	{
		ctx := ctxWithName("TestAccessLimiterBase")()
		rbuff := &limitReader{size: 1 << 8}
		r := l.Reader(ctx, rbuff)

		wbuff := &limitWriter{}
		w := l.Writer(ctx, wbuff)
		buf := make([]byte, 1<<20)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			io.CopyBuffer(w, r, buf)
			wg.Done()
		}()
		wg.Wait()

		span := trace.SpanFromContextSafe(ctx)
		tags := span.Tags()
		require.Empty(t, tags, span.TraceID(), tags)
	}
	{
		ctx, cancel := context.WithCancel(ctxWithName("TestAccessLimiterBase")())
		rbuff := &limitReader{size: 1 << 30}
		r := l.Reader(ctx, rbuff)

		wbuff := &limitWriter{}
		w := l.Writer(ctx, wbuff)

		var wg sync.WaitGroup
		wg.Add(1)
		closeCh := make(chan struct{})
		go func() {
			defer wg.Done()

			for {
				select {
				case <-closeCh:
					return
				default:
				}

				_, err := io.CopyN(w, r, 1<<20)
				if err != nil {
					return
				}
			}
		}()

		time.Sleep(time.Second)
		cancel()
		close(closeCh)
		wg.Wait()

		span := trace.SpanFromContextSafe(ctx)
		tags := span.Tags()
		require.NotEmpty(t, tags, span.TraceID(), tags)
	}
}

func TestAccessLimiterNoop(t *testing.T) {
	l := NewLimiter(LimitConfig{
		NameRps:    nil,
		ReaderMBps: 0,
		WriterMBps: 0,
	})

	name := "noop"
	err := l.Acquire(name)
	assert.NoError(t, err)
	err = l.Acquire(name)
	assert.NoError(t, err)
	l.Release(name)
	l.Release(name)

	ctx := ctxWithName("TestAccessLimiterNoop")()
	rbuff := &limitReader{size: 1 << 10}
	r := l.Reader(ctx, rbuff)

	wbuff := &limitWriter{}
	w := l.Writer(ctx, wbuff)
	buf := make([]byte, 1<<5)
	ch := make(chan struct{})
	go func() {
		io.CopyBuffer(w, r, buf)
		close(ch)
	}()

	<-ch
	span := trace.SpanFromContextSafe(ctx)
	tags := span.Tags()
	require.Empty(t, tags, span.TraceID(), tags)
}

func TestAccessLimiterStatus(t *testing.T) {
	{
		l := NewLimiter(LimitConfig{
			NameRps:    nil,
			ReaderMBps: 0,
			WriterMBps: 0,
		})
		for range [100]struct{}{} {
			l.Acquire("foo")
		}
		t.Logf("%+v\n", l.Status())
	}
	{
		ctx := ctxWithName("TestAccessLimiterStatus")()
		name := "foo"
		l := NewLimiter(LimitConfig{
			NameRps:    map[string]int{name: 10},
			ReaderMBps: 2,
			WriterMBps: 10,
		})

		ch := make(chan struct{})
		for range [7]struct{}{} {
			go func() {
				l.Acquire(name)
				<-ch
				l.Release(name)
			}()
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			rbuff := &limitReader{size: 1 << 24}
			r := l.Reader(ctx, rbuff)
			io.CopyBuffer(&limitWriter{}, r, make([]byte, 1<<20))
			wg.Done()
		}()

		w := l.Writer(ctx, &limitWriter{})
		wg.Add(8)
		for range [8]struct{}{} {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ch:
						return
					default:
					}
					w.Write(make([]byte, 1<<20))
				}
			}()
		}

		time.Sleep(time.Second * 3)
		t.Logf("%+v\n", l.Status())

		close(ch)
		wg.Wait()
	}
}
