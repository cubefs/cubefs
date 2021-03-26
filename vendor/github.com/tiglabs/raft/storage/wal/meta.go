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
	"os"
	"path"

	"github.com/edsrzf/mmap-go"

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
	mm          mmap.MMap
	truncOffset int64
}

func openMetaFile(dir string) (mf *metaFile, hs proto.HardState, meta truncateMeta, err error) {
	var f *os.File
	if f, err = os.OpenFile(path.Join(dir, "META"), os.O_RDWR|os.O_CREATE, 0600); err != nil {
		return
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	var metaSize = hs.Size() + meta.Size()
	var info os.FileInfo
	if info, err = f.Stat(); err != nil {
		return
	}
	if info.Size() != int64(metaSize) {
		if err = f.Truncate(int64(metaSize)); err != nil {
			return
		}
	}

	var mm mmap.MMap
	if mm, err = mmap.Map(f, mmap.RDWR, 0); err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = mm.Unmap()
		}
	}()

	mf = &metaFile{
		f:           f,
		mm:          mm,
		truncOffset: int64(hs.Size()),
	}

	hs, meta = mf.load()
	return mf, hs, meta, nil
}

func (mf *metaFile) load() (hs proto.HardState, meta truncateMeta) {
	// Load Hard State data and decode.
	hs.Decode(mf.mm[0:hs.Size()])

	// Load Truncate Meta data and decode.
	meta.Decode(mf.mm[hs.Size() : hs.Size()+meta.Size()])

	return
}

func (mf *metaFile) Close() (err error) {
	if err = mf.mm.Unmap(); err != nil {
		return
	}
	return mf.f.Close()
}

func (mf *metaFile) SaveTruncateMeta(meta truncateMeta) {
	// Encode truncate meta and write to file.
	var metaSize = int(meta.Size())
	buffer := bufalloc.AllocBuffer(metaSize)
	defer bufalloc.FreeBuffer(buffer)

	var b = buffer.Alloc(metaSize)
	meta.Encode(b)
	copy(mf.mm[mf.truncOffset:mf.truncOffset+int64(meta.Size())], b)
}

func (mf *metaFile) SaveHardState(hs proto.HardState) {
	// Encode hard state and write to file.
	hsSize := int(hs.Size())
	buffer := bufalloc.AllocBuffer(hsSize)
	defer bufalloc.FreeBuffer(buffer)

	b := buffer.Alloc(hsSize)
	hs.Encode(b)
	copy(mf.mm[0:mf.truncOffset], b)
}

func (mf *metaFile) Sync() error {
	return mf.mm.Flush()
}
