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

package largefile

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type mockLargeFile struct {
	writeCount  int
	readAtCount int
	fsize       int64
	rotated     bool
	closed      bool
}

func (m *mockLargeFile) Write(b []byte) (int, error) {
	m.writeCount++
	return len(b), nil
}

func (m *mockLargeFile) ReadAt(b []byte, off int64) (int, error) {
	m.readAtCount++
	return len(b), nil
}

func (m *mockLargeFile) FsizeOf() (int64, error) {
	return m.fsize, nil
}

func (m *mockLargeFile) Rotate() error {
	m.rotated = true
	return nil
}

func (m *mockLargeFile) Close() error {
	m.closed = true
	return nil
}

func TestAsyncLargeFile(t *testing.T) {
	workerNum := 4
	lf := &mockLargeFile{}
	a := newAsyncLargeFile(AsyncConfig{
		WorkerPoolNum: workerNum,
		lf:            lf,
	})

	// test Write
	n, err := a.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Errorf("Write failed: n=%d, err=%v", n, err)
	}
	// test ReadAt
	n, err = a.ReadAt([]byte("world"), 10)
	if err != nil || n != 5 {
		t.Errorf("ReadAt failed: n=%d, err=%v", n, err)
	}

	// test FsizeOf
	lf.fsize = 12345
	fsize, err := a.FsizeOf()
	if err != nil || fsize != 12345 {
		t.Errorf("FsizeOf failed: fsize=%d, err=%v", fsize, err)
	}

	// test Rotate
	err = a.Rotate()
	if err != nil || !lf.rotated {
		t.Errorf("Rotate failed: err=%v, rotated=%v", err, lf.rotated)
	}

	// test Close
	err = a.Close()
	require.NoError(t, err)

	// Write and ReadAt benchmark concurrently test
	lf2 := &mockLargeFile{}
	a2 := newAsyncLargeFile(AsyncConfig{
		WorkerPoolNum: workerNum,
		lf:            lf2,
	})

	writeN := 100
	readN := 100
	doneCh := make(chan struct{})
	go func() {
		for i := 0; i < writeN; i++ {
			_, err := a2.Write([]byte("abc"))
			if err != nil {
				t.Errorf("concurrent Write error: %v", err)
			}
		}
		doneCh <- struct{}{}
	}()
	go func() {
		for i := 0; i < readN; i++ {
			_, err := a2.ReadAt([]byte("xyz"), int64(i))
			if err != nil {
				t.Errorf("concurrent ReadAt error: %v", err)
			}
		}
		doneCh <- struct{}{}
	}()
	<-doneCh
	<-doneCh

	// wait for all worker done
	a2.Close()

	if lf2.writeCount == 0 || lf2.readAtCount == 0 {
		t.Errorf("Write/ReadAt has not been called: writeCount=%d, readAtCount=%d", lf2.writeCount, lf2.readAtCount)
	}
}
