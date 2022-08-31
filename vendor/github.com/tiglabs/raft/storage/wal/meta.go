// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path"

	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util/bufalloc"
)

type truncateMeta struct {
	truncIndex uint64
	truncTerm  uint64
}

func (m truncateMeta) Size() uint64 {
	return 16
}

func (m truncateMeta) Encode(b []byte) {
	binary.BigEndian.PutUint64(b, m.truncIndex)
	binary.BigEndian.PutUint64(b[8:], m.truncTerm)
}

func (m *truncateMeta) Decode(b []byte) {
	m.truncIndex = binary.BigEndian.Uint64(b)
	m.truncTerm = binary.BigEndian.Uint64(b[8:])
}

// Used to read and store Hard State and Truncate Mete information.
// Data storage in this file will be read and modified by using mmap.
//
// The size of the data file is:
// Hard State Size (24 bytes) + Truncate Meta Size (16 bytes) = 40 bytes.
//
// The first 24 bytes [0, 24) of the file are Hard Sate information.
// The last 16 bytes [24, 40) are Truncate Meta information.
//
// Schematic diagram of data storage distribution:
//
// | Hard State | Truncate Meta |
// 0           23               39
//
type metaFile struct {
	f           *os.File
	truncOffset int64
	maybeDirty  bool
}

func openMetaFile(dir string) (mf *metaFile, hs proto.HardState, meta truncateMeta, err error) {
	f, err := os.OpenFile(path.Join(dir, "META"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}

	mf = &metaFile{
		f:           f,
		truncOffset: int64(hs.Size()),
	}

	hs, meta, err = mf.load()
	return mf, hs, meta, err
}

func (mf *metaFile) load() (hs proto.HardState, meta truncateMeta, err error) {
	// load hardstate
	hs_size := int(hs.Size())
	buffer := bufalloc.AllocBuffer(hs_size)
	defer bufalloc.FreeBuffer(buffer)

	buf := buffer.Alloc(hs_size)
	n, err := mf.f.Read(buf)
	if err != nil {
		if err == io.EOF {
			err = nil
			return
		}
		return
	}
	if n != hs_size {
		err = NewCorruptError("META", 0, "wrong hardstate data size")
		return
	}
	hs.Decode(buf)

	// load trunc meta
	buffer.Reset()
	mt_size := int(meta.Size())
	buf = buffer.Alloc(mt_size)
	n, err = mf.f.Read(buf)
	if err != nil {
		if err == io.EOF {
			err = nil
			return
		}
		return
	}
	if n != mt_size {
		err = NewCorruptError("META", 0, "wrong truncmeta data size")
		return
	}
	meta.Decode(buf)
	return
}

func (mf *metaFile) Close() error {
	return mf.f.Close()
}

func (mf *metaFile) SaveTruncateMeta(meta truncateMeta) error {
	mt_size := int(meta.Size())
	buffer := bufalloc.AllocBuffer(mt_size)
	defer bufalloc.FreeBuffer(buffer)

	b := buffer.Alloc(mt_size)
	meta.Encode(b)
	_, err := mf.f.WriteAt(b, mf.truncOffset)
	mf.maybeDirty = true
	return err
}

func (mf *metaFile) SaveHardState(hs proto.HardState) error {
	hs_size := int(hs.Size())
	buffer := bufalloc.AllocBuffer(hs_size)
	defer bufalloc.FreeBuffer(buffer)

	b := buffer.Alloc(hs_size)
	hs.Encode(b)
	_, err := mf.f.WriteAt(b, 0)
	mf.maybeDirty = true
	return err
}

func (mf *metaFile) Sync() (err error) {
	if mf.maybeDirty {
		if err = mf.f.Sync(); err != nil {
			return
		}
		mf.maybeDirty = false
	}
	return
}
