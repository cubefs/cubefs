// Copyright 2009 The Go Authors. All rights reserved.
// Modified work copyright 2018 The tiglabs Authors.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bufalloc

import (
	"bytes"
	"errors"
	"io"
)

const minRead = 512

var (
	ErrTooLarge = errors.New("bufalloc.Buffer: too large.")
)

type ibuffer struct {
	off int
	buf []byte
}

func makeSlice(n int) []byte {
	defer func() {
		if recover() != nil {
			panic(ErrTooLarge)
		}
	}()
	return make([]byte, n)
}

func (b *ibuffer) Bytes() []byte { return b.buf[b.off:] }

func (b *ibuffer) String() string {
	if b == nil {
		return "<nil>"
	}
	return string(b.buf[b.off:])
}

func (b *ibuffer) Len() int { return len(b.buf) - b.off }

func (b *ibuffer) Cap() int { return cap(b.buf) }

func (b *ibuffer) Reset() { b.Truncate(0) }

func (b *ibuffer) Truncate(n int) {
	switch {
	case n < 0 || n > b.Len():
		panic("bufalloc.Buffer: truncation out of range")
	case n == 0:
		b.off = 0
	}
	b.buf = b.buf[0 : b.off+n]
}

func (b *ibuffer) grow(n int) int {
	if b.buf == nil {
		b.buf = makeSlice(n)
		return 0
	}

	m := b.Len()
	if m == 0 && b.off != 0 {
		b.Truncate(0)
	}
	if len(b.buf)+n > cap(b.buf) {
		var buf []byte
		if m+n <= cap(b.buf)/2 {
			copy(b.buf[:], b.buf[b.off:])
			buf = b.buf[:m]
		} else {
			buf = makeSlice(2*cap(b.buf) + n)
			copy(buf, b.buf[b.off:])
		}
		b.buf = buf
		b.off = 0
	}
	b.buf = b.buf[0 : b.off+m+n]
	return b.off + m
}

func (b *ibuffer) Alloc(n int) []byte {
	if n < 0 {
		panic("bufalloc.Buffer: negative count")
	}
	m := b.grow(n)
	return b.buf[m:]
}

func (b *ibuffer) Grow(n int) {
	if n < 0 {
		panic("bufalloc.Buffer: negative count")
	}
	m := b.grow(n)
	b.buf = b.buf[0:m]
}

func (b *ibuffer) Write(p []byte) (n int, err error) {
	m := b.grow(len(p))
	return copy(b.buf[m:], p), nil
}

func (b *ibuffer) ReadFrom(r io.Reader) (n int64, err error) {
	if b.off >= len(b.buf) {
		b.Truncate(0)
	}
	for {
		if free := cap(b.buf) - len(b.buf); free < minRead {
			// not enough space at end
			newBuf := b.buf
			if b.off+free < minRead {
				newBuf = makeSlice(2*cap(b.buf) + minRead)
			}
			copy(newBuf, b.buf[b.off:])
			b.buf = newBuf[:len(b.buf)-b.off]
			b.off = 0
		}
		m, e := r.Read(b.buf[len(b.buf):cap(b.buf)])
		b.buf = b.buf[0 : len(b.buf)+m]
		n += int64(m)
		if e == io.EOF {
			break
		}
		if e != nil {
			return n, e
		}
	}
	return n, nil // err is EOF, so return nil explicitly
}

func (b *ibuffer) WriteTo(w io.Writer) (n int64, err error) {
	if b.off < len(b.buf) {
		nBytes := b.Len()
		m, e := w.Write(b.buf[b.off:])
		if m > nBytes {
			panic("bufalloc.Buffer: invalid Write count")
		}
		b.off += m
		n = int64(m)
		if e != nil {
			return n, e
		}
		if m != nBytes {
			return n, io.ErrShortWrite
		}
	}
	b.Truncate(0)
	return
}

func (b *ibuffer) WriteByte(c byte) error {
	m := b.grow(1)
	b.buf[m] = c
	return nil
}

func (b *ibuffer) Read(p []byte) (n int, err error) {
	if b.off >= len(b.buf) {
		b.Truncate(0)
		if len(p) == 0 {
			return
		}
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.off:])
	b.off += n
	return
}

func (b *ibuffer) Next(n int) []byte {
	m := b.Len()
	if n > m {
		n = m
	}
	data := b.buf[b.off : b.off+n]
	b.off += n
	return data
}

func (b *ibuffer) ReadByte() (c byte, err error) {
	if b.off >= len(b.buf) {
		b.Truncate(0)
		return 0, io.EOF
	}
	c = b.buf[b.off]
	b.off++
	return c, nil
}

func (b *ibuffer) ReadBytes(delim byte) (line []byte, err error) {
	slice, err := b.readSlice(delim)
	line = append(line, slice...)
	return
}

func (b *ibuffer) readSlice(delim byte) (line []byte, err error) {
	i := bytes.IndexByte(b.buf[b.off:], delim)
	end := b.off + i + 1
	if i < 0 {
		end = len(b.buf)
		err = io.EOF
	}
	line = b.buf[b.off:end]
	b.off = end
	return line, err
}
