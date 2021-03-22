// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package snappy_test

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"unsafe"

	"github.com/tsuna/gohbase/compression"
	"github.com/tsuna/gohbase/compression/snappy"
	"github.com/tsuna/gohbase/test"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		src       []byte
		dst       []byte
		out       []byte
		sz        uint32
		sameAsDst bool
	}{
		{
			dst: nil,
			src: []byte("test"),
			out: []byte("\x04\ftest"),
			sz:  6,
		},
		{
			src: nil,
			out: []byte("\x00"),
			sz:  1,
		},
		{
			src: []byte("test"),
			dst: []byte("something here already"),
			out: []byte("something here already\x04\ftest"),
			sz:  6,
		},
		{
			src:       []byte("test"),
			dst:       make([]byte, 4, 10),
			out:       []byte("\x00\x00\x00\x00\x04\ftest"),
			sz:        6,
			sameAsDst: true,
		},
		{
			src:       []byte("test"),
			dst:       make([]byte, 4, 11),
			out:       []byte("\x00\x00\x00\x00\x04\ftest"),
			sz:        6,
			sameAsDst: true,
		},
		{
			src:       []byte("test"),
			dst:       make([]byte, 4, 9),
			out:       []byte("\x00\x00\x00\x00\x04\ftest"),
			sz:        6,
			sameAsDst: false,
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var codec compression.Codec
			codec = snappy.New()

			dst := make([]byte, len(tcase.dst), cap(tcase.dst))
			copy(dst, tcase.dst)
			dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))

			out, sz := codec.Encode(tcase.src, dst)

			if tcase.sz != sz {
				t.Errorf("expected size %d, got %d", tcase.sz, sz)
			}

			if l := len(tcase.dst) + int(sz); l != len(out) {
				t.Errorf("expected length %d, got %d", l, len(out))
			}

			if !bytes.Equal(tcase.out, out) {
				t.Errorf("expected out %q, got %q", tcase.out, out)
			}

			outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
			if tcase.sameAsDst && dstHeader.Data != outHeader.Data {
				t.Errorf("expected dst %v to be reused, got %v", dstHeader, outHeader)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	tests := []struct {
		src       []byte
		dst       []byte
		out       []byte
		sz        uint32
		err       error
		sameAsDst bool
	}{
		{
			src: []byte("\x04\ftest"),
			out: []byte("test"),
			sz:  4,
		},
		{
			src: []byte("\x00"),
			out: nil,
			sz:  0,
		},
		{
			src: []byte("\x04\ftest"),
			dst: []byte("something here already"),
			out: []byte("something here alreadytest"),
			sz:  4,
		},
		{
			src:       []byte("\x04\ftest"),
			dst:       make([]byte, 4, 8),
			out:       []byte("\x00\x00\x00\x00test"),
			sz:        4,
			sameAsDst: true,
		},
		{
			src:       []byte("\x04\ftest"),
			dst:       make([]byte, 4, 9),
			out:       []byte("\x00\x00\x00\x00test"),
			sz:        4,
			sameAsDst: true,
		},
		{
			src:       []byte("\x04\ftest"),
			dst:       make([]byte, 4, 7),
			out:       []byte("\x00\x00\x00\x00test"),
			sz:        4,
			sameAsDst: false,
		},
		{
			src: []byte("test"),
			err: errors.New("snappy: corrupt input"),
		},
		{
			src: []byte("\x04\ftes"),
			err: errors.New("snappy: corrupt input"),
		},
		{
			src: []byte("\x04\ftestasdfasdfasdf"),
			err: errors.New("snappy: corrupt input"),
		},
	}

	for i, tcase := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var codec compression.Codec
			codec = snappy.New()

			dst := make([]byte, len(tcase.dst), cap(tcase.dst))
			copy(dst, tcase.dst)
			dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))

			out, sz, err := codec.Decode(tcase.src, dst)

			if !test.ErrEqual(tcase.err, err) {
				t.Errorf("expected error %v, got %v", tcase.err, err)
			}

			if tcase.sz != sz {
				t.Errorf("expected size %d, got %d", tcase.sz, sz)
			}

			if l := len(tcase.dst) + int(sz); l != len(out) {
				t.Errorf("expected length %d, got %d", l, len(out))
			}

			if !bytes.Equal(tcase.out, out) {
				t.Errorf("expected out %q, got %q", tcase.out, out)
			}

			outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
			if tcase.sameAsDst && dstHeader.Data != outHeader.Data {
				t.Errorf("expected dst %v to be reused, got %v", dstHeader, outHeader)
			}
		})
	}
}
