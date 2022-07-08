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

package raftserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.etcd.io/etcd/raft/v3"
)

type testSnapshot struct {
	name  string
	index uint64
}

func (s *testSnapshot) Name() string {
	return s.name
}

func (s *testSnapshot) Index() uint64 {
	return s.index
}

func (s *testSnapshot) Read() ([]byte, error) {
	return nil, nil
}

func (s *testSnapshot) Close() {
}

func TestSnapshotter(t *testing.T) {
	shotter := newSnapshotter(10, time.Second)

	for i := 0; i < 10; i++ {
		snap := &snapshot{
			st: &testSnapshot{
				name: fmt.Sprintf("testsnapshot%d", i),
			},
		}
		assert.Nil(t, shotter.Set(snap))
	}

	for i := 10; i < 20; i++ {
		snap := &snapshot{
			st: &testSnapshot{
				name: fmt.Sprintf("testsnapshot%d", i),
			},
		}
		assert.Equal(t, shotter.Set(snap), raft.ErrSnapshotTemporarilyUnavailable)
	}

	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("testsnapshot%d", i)
		assert.NotNil(t, shotter.Get(name))
		shotter.Delete(name)
		assert.Nil(t, shotter.Get(name))
	}
	for i := 10; i < 20; i++ {
		assert.Nil(t, shotter.Get(fmt.Sprintf("testsnapshot%d", i)))
	}

	for i := 0; i < 10; i++ {
		snap := &snapshot{
			st: &testSnapshot{
				name: fmt.Sprintf("testsnapshot%d", i),
			},
		}
		assert.Nil(t, shotter.Set(snap))
	}
	shotter.deleteAll()
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("testsnapshot%d", i)
		assert.Nil(t, shotter.Get(name))
	}

	for i := 0; i < 10; i++ {
		snap := &snapshot{
			st: &testSnapshot{
				name: fmt.Sprintf("testsnapshot%d", i),
			},
		}
		assert.Nil(t, shotter.Set(snap))
	}
	time.Sleep(time.Second)
	shotter.Stop()

	buffer := &bytes.Buffer{}
	st := newApplySnapshot(buffer)
	_, err := st.Read()
	assert.NotNil(t, err)

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 10)
	buffer.Write(b)
	buffer.Write([]byte("123456789"))
	_, err = st.Read()
	assert.NotNil(t, err)

	buffer.Reset()
	binary.BigEndian.PutUint32(b, 10)
	buffer.Write(b)
	buffer.Write([]byte("0123456789"))
	_, err = st.Read()
	assert.NotNil(t, err)

	buffer.Reset()
	crc := crc32.NewIEEE()
	mw := io.MultiWriter(buffer, crc)
	binary.BigEndian.PutUint32(b, 10)
	buffer.Write(b)
	mw.Write([]byte("0123456789"))
	checksum := crc.Sum32()
	binary.BigEndian.PutUint32(b, checksum)
	buffer.Write(b)
	body, err := st.Read()
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(body, []byte("0123456789")))
}
