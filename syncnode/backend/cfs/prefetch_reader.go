// Copyright 2026 The CubeFS Authors.
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

//go:build linux

package cfs

import (
	"context"
	"errors"
	"io"
	"sync"
)

// prefetchReader wraps a random-access fetch function with N worker
// goroutines that fetch fixed-size chunks ahead of the consumer, and
// presents the result as an in-order io.ReadCloser.
//
// Mirrors tool/cfs-sync/storage/prefetch_reader.go. Re-implemented here
// rather than imported because that package is not a public dependency of
// syncnode — the backend should own its read path so cfs-sync can evolve
// independently.
type prefetchReader struct {
	fetch       fetchFunc
	startOff    int64
	size        int64
	chunkSize   int
	parallelism int

	bufPool *sync.Pool

	cur         *prefetchChunk
	curPos      int
	nextSeq     int64
	pending     map[int64]*prefetchChunk
	terminalErr error

	totalChunks int64

	in       chan *prefetchChunk
	dispatch chan int64

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

type fetchFunc func(p []byte, off int64) (int, error)

type prefetchChunk struct {
	seq    int64
	data   []byte
	bufPtr *[]byte
	err    error
}

func newPrefetchReader(fetch fetchFunc, startOff, size int64, chunkSize, parallelism int) *prefetchReader {
	if chunkSize <= 0 {
		chunkSize = 4 * 1024 * 1024
	}
	if parallelism <= 0 {
		parallelism = 4
	}
	if size < 0 {
		size = 0
	}

	totalChunks := (size + int64(chunkSize) - 1) / int64(chunkSize)
	if int64(parallelism) > totalChunks && totalChunks > 0 {
		parallelism = int(totalChunks)
	}
	if parallelism == 0 && totalChunks == 0 {
		ctx, cancel := context.WithCancel(context.Background())
		return &prefetchReader{
			fetch:       fetch,
			startOff:    startOff,
			size:        size,
			chunkSize:   chunkSize,
			parallelism: 0,
			totalChunks: 0,
			ctx:         ctx,
			cancel:      cancel,
		}
	}

	pr := &prefetchReader{
		fetch:       fetch,
		startOff:    startOff,
		size:        size,
		chunkSize:   chunkSize,
		parallelism: parallelism,
		bufPool: &sync.Pool{
			New: func() interface{} { b := make([]byte, chunkSize); return &b },
		},
		pending:     make(map[int64]*prefetchChunk),
		totalChunks: totalChunks,
		in:          make(chan *prefetchChunk, parallelism),
		dispatch:    make(chan int64, parallelism+1),
	}
	pr.ctx, pr.cancel = context.WithCancel(context.Background())

	for i := 0; i < parallelism; i++ {
		pr.wg.Add(1)
		go pr.workerLoop()
	}
	for seq := int64(0); seq < int64(parallelism) && seq < totalChunks; seq++ {
		pr.dispatch <- seq
	}
	return pr
}

func (pr *prefetchReader) workerLoop() {
	defer pr.wg.Done()
	for {
		select {
		case <-pr.ctx.Done():
			return
		case seq, ok := <-pr.dispatch:
			if !ok {
				return
			}
			pr.serveOne(seq)
		}
	}
}

func (pr *prefetchReader) serveOne(seq int64) {
	off := int64(seq) * int64(pr.chunkSize)
	if off >= pr.size {
		pr.sendOrAbandon(&prefetchChunk{seq: seq, err: io.EOF})
		return
	}
	want := pr.chunkSize
	if off+int64(want) > pr.size {
		want = int(pr.size - off)
	}

	bufPtr := pr.bufPool.Get().(*[]byte)
	chunk := &prefetchChunk{seq: seq, bufPtr: bufPtr}

	filled := 0
	for filled < want {
		n, err := pr.fetch((*bufPtr)[filled:want], pr.startOff+off+int64(filled))
		filled += n
		if err == nil {
			if n == 0 {
				chunk.err = errShortFetchLoop
				break
			}
			continue
		}
		if err == io.EOF {
			break
		}
		chunk.err = err
		break
	}
	chunk.data = (*bufPtr)[:filled]
	if filled == 0 && chunk.err == nil {
		chunk.err = io.EOF
		pr.bufPool.Put(bufPtr)
		chunk.bufPtr = nil
	}
	pr.sendOrAbandon(chunk)
}

var errShortFetchLoop = errors.New("prefetchReader: fetch returned 0 bytes with nil error")

func (pr *prefetchReader) sendOrAbandon(chunk *prefetchChunk) {
	select {
	case pr.in <- chunk:
	case <-pr.ctx.Done():
		if chunk.bufPtr != nil {
			pr.bufPool.Put(chunk.bufPtr)
			chunk.bufPtr = nil
		}
	}
}

func (pr *prefetchReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if pr.terminalErr != nil {
		return 0, pr.terminalErr
	}
	if pr.size == 0 {
		return 0, io.EOF
	}
	if pr.cur != nil && pr.curPos < len(pr.cur.data) {
		n := copy(p, pr.cur.data[pr.curPos:])
		pr.curPos += n
		if pr.curPos >= len(pr.cur.data) {
			pr.recycle(pr.cur)
			pr.cur = nil
			pr.curPos = 0
			pr.scheduleAhead()
		}
		return n, nil
	}
	if pr.nextSeq >= pr.totalChunks {
		pr.terminalErr = io.EOF
		return 0, io.EOF
	}
	c, err := pr.acquireSeq(pr.nextSeq)
	if err != nil {
		pr.terminalErr = err
		return 0, err
	}
	pr.cur = c
	pr.curPos = 0
	pr.nextSeq++
	return pr.Read(p)
}

func (pr *prefetchReader) acquireSeq(seq int64) (*prefetchChunk, error) {
	if c, ok := pr.pending[seq]; ok {
		delete(pr.pending, seq)
		return pr.unwrapChunk(c)
	}
	for {
		select {
		case <-pr.ctx.Done():
			return nil, io.ErrClosedPipe
		case c := <-pr.in:
			if c.seq == seq {
				return pr.unwrapChunk(c)
			}
			pr.pending[c.seq] = c
		}
	}
}

func (pr *prefetchReader) unwrapChunk(c *prefetchChunk) (*prefetchChunk, error) {
	if c.err != nil && c.err != io.EOF {
		if c.bufPtr != nil {
			pr.bufPool.Put(c.bufPtr)
			c.bufPtr = nil
		}
		return nil, c.err
	}
	if len(c.data) == 0 {
		if c.bufPtr != nil {
			pr.bufPool.Put(c.bufPtr)
			c.bufPtr = nil
		}
		return nil, io.EOF
	}
	return c, nil
}

func (pr *prefetchReader) recycle(c *prefetchChunk) {
	if c == nil || c.bufPtr == nil {
		return
	}
	pr.bufPool.Put(c.bufPtr)
	c.bufPtr = nil
	c.data = nil
}

func (pr *prefetchReader) scheduleAhead() {
	next := pr.nextSeq + int64(pr.parallelism) - 1
	if next < 0 || next >= pr.totalChunks {
		return
	}
	select {
	case pr.dispatch <- next:
	case <-pr.ctx.Done():
	}
}

func (pr *prefetchReader) Close() error {
	pr.closeOnce.Do(func() {
		pr.cancel()
		if pr.in != nil {
			go func() {
				for {
					select {
					case c := <-pr.in:
						if c != nil && c.bufPtr != nil {
							pr.bufPool.Put(c.bufPtr)
							c.bufPtr = nil
						}
					default:
						return
					}
				}
			}()
		}
		pr.wg.Wait()
		if pr.in != nil {
			for {
				select {
				case c := <-pr.in:
					if c != nil && c.bufPtr != nil {
						pr.bufPool.Put(c.bufPtr)
						c.bufPtr = nil
					}
				default:
					goto doneDrain
				}
			}
		doneDrain:
		}
		for _, c := range pr.pending {
			pr.recycle(c)
		}
		pr.pending = nil
		pr.recycle(pr.cur)
		pr.cur = nil
	})
	if pr.terminalErr != nil && !errors.Is(pr.terminalErr, io.EOF) {
		return pr.terminalErr
	}
	return nil
}
