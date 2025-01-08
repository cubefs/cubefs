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

package iouring

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/cubefs/cubefs/blobstore/blobnode/sys"
)

const defaultMaxSize = 2 << 30

var ErrOffsetExceedMaxSize = errors.New("offset exceed max size")

func NewMockEngine(cfg Config) (Engine, error) {
	f, err := os.OpenFile(cfg.FilePath, os.O_RDWR|syscall.O_DIRECT, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open temp file failed: %s", err)
	}
	if err := sys.PreAllocate(f.Fd(), 0, defaultMaxSize); err != nil {
		return nil, err
	}

	return &mockEngine{f: f}, nil
}

type mockEngine struct {
	f *os.File
}

func (m *mockEngine) Read(data []byte, off uint64, size int) error {
	if off+uint64(size) > defaultMaxSize {
		return ErrOffsetExceedMaxSize
	}

	n, err := m.f.ReadAt(data, int64(off))
	if err != nil {
		return err
	}
	if n != size {
		return io.ErrUnexpectedEOF
	}

	return nil
}

func (m *mockEngine) Write(data []byte, off uint64, size int) error {
	if off+uint64(size) > defaultMaxSize {
		return ErrOffsetExceedMaxSize
	}

	n, err := m.f.WriteAt(data, int64(off))
	if err != nil {
		return err
	}
	if n != size {
		return io.ErrShortWrite
	}

	return nil
}

func (m *mockEngine) Close() error {
	if err := m.f.Close(); err != nil {
		return err
	}

	return nil
}
