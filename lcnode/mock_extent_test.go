// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package lcnode

import (
	"bytes"
	"context"
	"crypto/md5"
	"io"

	"github.com/cubefs/cubefs/proto"
)

type MockExtentClient struct{}

func NewMockExtentClient() *MockExtentClient {
	return &MockExtentClient{}
}

func (*MockExtentClient) OpenStream(uint64, bool, bool, string) error { return nil }
func (*MockExtentClient) CloseStream(uint64) error                    { return nil }

// Read returns requested bytes so migrate/readFromExtentClient loops make progress.
func (*MockExtentClient) Read(_ uint64, data []byte, _ int, size int, _ uint8, _ bool) (int, error) {
	if size <= 0 {
		return 0, nil
	}
	n := size
	if n > len(data) {
		n = len(data)
	}
	return n, nil
}

func (*MockExtentClient) Write(_ uint64, _ int, data []byte, _ int, _ func() error, _ uint8, _ uint32, _ bool, _ bool) (int, error) {
	return len(data), nil
}

func (*MockExtentClient) Flush(uint64) error { return nil }
func (*MockExtentClient) Close() error       { return nil }

type MockEbsClient struct{}

func NewMockEbsClient() *MockEbsClient {
	return &MockEbsClient{}
}

func (*MockEbsClient) Put(_ context.Context, _ string, r io.Reader, _ uint64) ([]proto.ObjExtentKey, [][]byte, error) {
	h := md5.New()
	if _, err := io.Copy(h, r); err != nil {
		return nil, nil, err
	}
	sum := h.Sum(nil)
	return []proto.ObjExtentKey{{}}, [][]byte{sum}, nil
}

func (*MockEbsClient) Get(context.Context, string, uint64, uint64, proto.ObjExtentKey) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
