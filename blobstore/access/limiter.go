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
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
)

const (
	_tagLimitedR = "limitedr"
	_tagLimitedW = "limitedw"
)

// Limiter rps and bps limiter
type Limiter interface {
	// Acquire acquire with one request per second
	Acquire(name string) error
	// Release release of one request per second
	Release(name string)

	// Reader return io.Reader with bandwidth rate limit
	Reader(ctx context.Context, r io.Reader) io.Reader
	// Writer return io.Writer with bandwidth rate limit
	Writer(ctx context.Context, w io.Writer) io.Writer

	// Status returns running status
	// TODO: calculate rate limit wait concurrent
	Status() Status
}

// LimitConfig configuration of limiter
type LimitConfig struct {
	NameRps    map[string]int `json:"name_rps"`    // request with name n/s
	ReaderMBps int            `json:"reader_mbps"` // read with MB/s
	WriterMBps int            `json:"writer_mbps"` // write with MB/s
}

// Status running status
type Status struct {
	Config    LimitConfig    `json:"config"`     // configuration status
	Running   map[string]int `json:"running"`    // running request
	ReadWait  int            `json:"read_wait"`  // wait reading duration
	WriteWait int            `json:"write_wait"` // wait writing duration
}

// Reader limited reader
type Reader struct {
	ctx        context.Context
	rate       *rate.Limiter
	underlying io.Reader
}

var _ io.Reader = &Reader{}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.underlying.Read(p)

	now := time.Now()
	reserve := r.rate.ReserveN(now, n)

	// Wait if necessary
	delay := reserve.DelayFrom(now)
	if delay == 0 {
		return
	}

	span := trace.SpanFromContextSafe(r.ctx)
	if !reserve.OK() {
		span.Warnf("reader exceeds limiter n:%d, burst:%d", n, r.rate.Burst())
		return
	}
	t := time.NewTimer(delay)
	defer t.Stop()

	// for access PUT request is Read from client
	span.SetTag(_tagLimitedW, delay.Milliseconds())

	select {
	case <-t.C:
		// We can proceed.
		return
	case <-r.ctx.Done():
		// Context was canceled before we could proceed.  Cancel the
		// reservation, which may permit other events to proceed sooner.
		reserve.Cancel()
		err = r.ctx.Err()
		return
	}
}

// Writer limited writer
type Writer struct {
	ctx        context.Context
	rate       *rate.Limiter
	underlying io.Writer
}

var _ io.Writer = &Writer{}

func (w *Writer) Write(p []byte) (n int, err error) {
	n, err = w.underlying.Write(p)

	now := time.Now()
	reserve := w.rate.ReserveN(now, n)

	// Wait if necessary
	delay := reserve.DelayFrom(now)
	if delay == 0 {
		return
	}

	span := trace.SpanFromContextSafe(w.ctx)
	if !reserve.OK() {
		span.Warnf("writer exceeds limiter n:%d, burst:%d", n, w.rate.Burst())
		return
	}
	t := time.NewTimer(delay)
	defer t.Stop()

	// for access GET request is Write to client
	span.SetTag(_tagLimitedR, delay.Milliseconds())

	select {
	case <-t.C:
		// We can proceed.
		return
	case <-w.ctx.Done():
		// Context was canceled before we could proceed.  Cancel the
		// reservation, which may permit other events to proceed sooner.
		reserve.Cancel()
		err = w.ctx.Err()
		return
	}
}

type limiter struct {
	config     LimitConfig
	limiters   map[string]limit.Limiter
	rateReader *rate.Limiter
	rateWriter *rate.Limiter
}

// NewLimiter returns a Limiter
func NewLimiter(cfg LimitConfig) Limiter {
	mb := 1 << 20
	lim := &limiter{
		config:   cfg,
		limiters: make(map[string]limit.Limiter, len(cfg.NameRps)),
	}

	for name, rps := range cfg.NameRps {
		if rps > 0 {
			lim.limiters[name] = count.New(rps)
		}
	}

	if cfg.ReaderMBps > 0 {
		lim.rateReader = rate.NewLimiter(rate.Limit(cfg.ReaderMBps*mb), 2*cfg.ReaderMBps*mb)
	}
	if cfg.WriterMBps > 0 {
		lim.rateWriter = rate.NewLimiter(rate.Limit(cfg.WriterMBps*mb), 2*cfg.WriterMBps*mb)
	}

	return lim
}

func (lim *limiter) Acquire(name string) error {
	if l := lim.limiters[name]; l != nil {
		return l.Acquire()
	}
	return nil
}

func (lim *limiter) Release(name string) {
	if l := lim.limiters[name]; l != nil {
		l.Release()
	}
}

func (lim *limiter) Reader(ctx context.Context, r io.Reader) io.Reader {
	if lim.rateReader != nil {
		return &Reader{
			ctx:        ctx,
			rate:       lim.rateReader,
			underlying: r,
		}
	}
	return r
}

func (lim *limiter) Writer(ctx context.Context, w io.Writer) io.Writer {
	if lim.rateWriter != nil {
		return &Writer{
			ctx:        ctx,
			rate:       lim.rateWriter,
			underlying: w,
		}
	}
	return w
}

func (lim *limiter) Status() Status {
	st := Status{
		Config: lim.config,
	}

	st.Running = make(map[string]int, len(lim.limiters))
	for name, nl := range lim.limiters {
		st.Running[name] = nl.Running()
	}

	st.ReadWait = rateWait(lim.rateReader)
	st.WriteWait = rateWait(lim.rateWriter)

	return st
}

// rateWait get duration of waiting half of limit
func rateWait(r *rate.Limiter) int {
	if r == nil {
		return 0
	}
	now := time.Now()
	reserve := r.ReserveN(now, int(r.Limit())/2)
	duration := reserve.DelayFrom(now)
	reserve.Cancel()
	return int(duration.Milliseconds())
}
