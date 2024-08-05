// Copyright 2024 The CubeFS Authors.
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

package rpc2

import (
	"bytes"
	"io"
	"sort"
	"sync"
)

func (h *Header) newIfNil() {
	if h.M == nil {
		h.M = make(map[string]string)
	}
}

func (h *Header) Add(key, val string) {
	h.Set(key, val)
}

func (h *Header) Set(key, val string) {
	h.newIfNil()
	h.M[key] = val
}

func (h *Header) Del(key string) {
	h.newIfNil()
	delete(h.M, key)
}

func (h *Header) Get(key string) string {
	h.newIfNil()
	return h.M[key]
}

func (h *Header) Clone() Header {
	var nh Header
	nh.M = make(map[string]string, len(h.M))
	for key, val := range h.M {
		nh.Add(key, val)
	}
	return nh
}

func (h *Header) Merge(other Header) {
	h.newIfNil()
	for key, val := range other.M {
		h.M[key] = val
	}
}

func (h *Header) ToFixedHeader() FixedHeader {
	fh := FixedHeader{}
	for key, val := range h.M {
		fh.Set(key, val)
	}
	return fh
}

func (fh *FixedHeader) newIfNil() {
	if fh.M == nil {
		fh.M = make(map[string]FixedHeaderValue)
	}
}

func (fh *FixedHeader) Add(key, val string) {
	fh.Set(key, val)
}

func (fh *FixedHeader) Set(key, val string) {
	fh.newIfNil()
	if v, exist := fh.M[key]; exist {
		v.Value = val
		fh.M[key] = v
	} else {
		fh.M[key] = FixedHeaderValue{Len: int32(len(val)), Value: val}
	}
}

func (fh *FixedHeader) Del(key string) {
	fh.newIfNil()
	delete(fh.M, key)
}

func (fh *FixedHeader) Get(key string) string {
	fh.newIfNil()
	return fh.M[key].Value
}

func (fh *FixedHeader) SetLen(key string, l int) {
	fh.newIfNil()
	fh.M[key] = FixedHeaderValue{Len: int32(l)}
}

func (fh *FixedHeader) ToHeader() Header {
	h := Header{
		M: make(map[string]string, len(fh.M)),
	}
	for k, v := range fh.M {
		h.M[k] = v.GetValue()
	}
	return h
}

func (fh *FixedHeader) MergeHeader(h Header) {
	for key, val := range h.M {
		fh.Set(key, val)
	}
}

func (fh *FixedHeader) AllSize() (n int) {
	for _, v := range fh.M {
		n += int(v.GetLen())
	}
	return
}

func (fh *FixedHeader) keys() []string {
	keys := make([]string, 0, len(fh.M))
	for key := range fh.M {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (fh *FixedHeader) Reader() io.Reader {
	if len(fh.M) == 0 {
		return NoBody
	}
	buff := make([]byte, fh.AllSize())
	off := 0
	for _, key := range fh.keys() {
		val := fh.M[key]
		copy(buff[off:off+int(val.Len)], []byte(val.Value))
		off += int(val.Len)
	}
	return bytes.NewReader(buff)
}

func (fh *FixedHeader) ReadFrom(r io.Reader) (int64, error) {
	buff := make([]byte, fh.AllSize())
	n, err := io.ReadFull(r, buff)
	if err != nil {
		return int64(n), err
	}
	off := 0
	for _, key := range fh.keys() {
		val := fh.M[key]
		val.Value = string(buff[off : off+int(val.Len)])
		fh.M[key] = val
		off += int(val.Len)
	}
	return int64(n), nil
}

type trailerReader struct {
	r io.Reader

	once    sync.Once
	Fn      func() error
	Trailer FixedHeader
}

func (t *trailerReader) Read(p []byte) (n int, err error) {
	t.once.Do(func() {
		if t.Fn != nil {
			err = t.Fn()
		}
		t.r = t.Trailer.Reader()
	})
	if err != nil {
		return
	}
	return t.r.Read(p)
}
